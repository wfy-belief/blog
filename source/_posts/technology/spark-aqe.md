---
title: Spark AQE 自适应查询执行：让优化器在运行时继续工作的深度复盘
abbrlink: spark-aqe
date: 2026-07-02 18:00:00
tags:
  - Spark
  - AQE
categories:
  - 技术
description: 拆解 Spark AQE 如何在运行时合并 shuffle 分区、切换 join 策略、切分倾斜分区，以及它的局限与坑
ai_text: "以一个被 2000 个分区拖垮的报表作业为引子，第一人称复盘 Spark AQE：QueryStage 的创建与 materialize、MapOutputStatistics 的 bytesByPartitionId 如何驱动三大规则——CoalesceShufflePartitions 的 targetSize 计算、DynamicJoinSelection 的策略切换、OptimizeSkewedJoin 的 median 判定，并给出全套配置与踩坑清单。"
---

## 引子：一个被 2000 个分区拖垮的报表

去年我们把一套跑在 Spark 2.4 的离线数仓迁到 3.3，有个日级报表作业一直让我头疼：它最后一步是大表 join 小表再聚合，跑下来平均 40 分钟，stage 监控里 reduce 阶段 2000 个 task，其中 1900 个 3 秒就跑完，剩下 100 个拖到 8 分钟，典型的数据倾斜。当时的处理方式是老三样：加大 `spark.sql.shuffle.partitions`、给倾斜 key 加随机前缀、手动 broadcast。改完降到 25 分钟，但我心里清楚这都是经验活，换个作业又得重来。

迁到 3.3 之后，我把 `spark.sql.adaptive.enabled` 打开，同一个作业什么都没改，直接降到 16 分钟 <!-- 校准：请按真实经历核实/替换 -->。看执行计划，2000 个分区被合并成了 200 个，那个倾斜的 join 节点旁边多了个 `CustomShuffleReader` 标记，倾斜分区被切成了 5 份并行处理。这就是 AQE——Adaptive Query Execution。这次经历让我下定决心把它从源码层拆一遍，本篇就是那次拆解的记录。

## 一、为什么需要 AQE：静态优化的三道硬伤

Catalyst 优化器再强，它做决策的依据是"执行前"的信息——表统计、列统计、上一次 analyze 的直方图。问题是，SQL 真正跑起来之后，中间每一层算子产出的数据量和分布，静态优化器是看不到的。这就留下三道硬伤：

第一，shuffle 分区数靠猜。`spark.sql.shuffle.partitions` 默认 200，对大作业太小（task 倾斜、OOM），对小作业太大（大量小 task 启动开销）。我们调到 2000 是为了兜底大 join，但代价是 90% 的 task 都是空的或几 MB，调度开销比计算还大。

第二，join 策略选错没法回头。JoinSelection 根据统计估算右表大小，小于 `autoBroadcastJoinThreshold`（默认 10MB）就走 BroadcastHashJoin。但估算经常不准：一个 filter 之后本该只剩 50MB 的表，因为统计过期或列估算偏差被估成 500MB，于是老老实实走了 SortMergeJoin，多一次 shuffle 多一次排序。

第三，数据倾斜无法预防。静态优化器只知道 key 的 cardinality，不知道某个 key 占了多少字节。一个 join 跑到一半才发现某几个 task 拉了 50GB 数据，这时候整个 plan 已经定死，只能眼睁睁看着它 OOM 或者拖慢整体。

AQE 的核心思想就一句话：**让优化器在运行时继续工作**。它把一个查询拆成多个 QueryStage，每个 stage 跑完拿到真实的 map 输出统计，再回头重新优化后续 stage 的计划。下面这张图是它和静态优化流程的对比。

```
   静态优化 (Spark 2.x / AQE 关闭)            AQE (Spark 3.x / 开启)
   -------------------------------            ----------------------
   SQL                                         SQL
     | Analysis/Optimize/Physical                | Analysis/Optimize/Physical
     v                                           v
   一锤定音的 ExecutedPlan                     AdaptiveSparkPlanExec (包裹)
     |                                           |  以 Exchange 为界切 QueryStage
     v                                           v
   一次性提交所有 stage 串行跑                 跑完 stage1 -> 收 MapOutputStatistics
     |                                           |  -> 回填统计 -> 重新优化 stage2 plan
     |                                           v
     |                                          三大规则作用:
     |                                          1. CoalesceShufflePartitions  合并小分区
     |                                          2. DynamicJoinSelection        切换 join 策略
     |                                          3. OptimizeSkewedJoin          切分倾斜分区
     |                                           v
     v                                          提交下一个 stage ... 直到无 Exchange
   跑完才发现倾斜/OOM, 已晚                   倾斜在切分前就被处理掉
```

## 二、AQE 的运行框架：QueryStage 与运行时统计

### 2.1 QueryStage 的创建与 materialize

AQE 的入口是 `AdaptiveSparkPlanExec`，它包裹在物理计划根节点上。在 `getFinalPlan()` 里，它把整棵物理 plan 沿着 `Exchange` 节点（`ShuffleExchangeExec` 与 `BroadcastExchangeExec`）切成多个 `QueryStage`。每个 QueryStage 是一个可以独立 materialize 的子计划——叶子是上游已完成的 stage 产出，根是一个 Exchange。

切分之后是"跑一段、优化一段"的循环：

1. 从叶子 QueryStage 开始，调用 `doExecute()` 真正提交对应 stage 的 job；
2. stage 跑完，driver 收集 `MapOutputStatistics`；
3. 把统计回填到对应 Exchange 的 `mapOutputStatistics` 字段；
4. 对未执行的后续 plan 重新跑一遍优化规则（`QueryStagePreparations` + AQE 专用规则）；
5. 生成新的 QueryStage，回到第 1 步，直到没有 Exchange，输出最终 plan。

补充一点机制细节：`AdaptiveSparkPlanExec` 的 `getFinalPlan()` 是 lazy 的，真正触发是在 RDD 被执行（`execute`/`executeCollect`）时。它内部维护 `currentPhysicalPlan` 和一个 `running` 标志位——每次进入 `getFinalPlan`，若还有未 materialize 的 QueryStage，就跑一轮 `reOptimize`：把已完成 stage 的 `MapOutputStatistics` 注入、应用 AQE 规则、生成新 plan，再把新 plan 里新的叶子 QueryStage 提交执行。整个过程是"提交-等待-回填-再优化"的同步循环，driver 在这里阻塞直到所有 stage 完成。这也是为什么 AQE 的重新优化是串行的——它必须等前一个 stage 的统计回来才能优化下一个，没法并行预判。

关键点：AQE 的"自适应"只发生在 Exchange 边界。Exchange 之前的同一个 stage 内部，算子还是一次性执行，不会中途调整。所以 AQE 救不了"单个 stage 内的算子倾斜"，它能救的是"stage 之间的计划选择"和"下一个 stage 的分区策略"。

### 2.2 MapOutputStatistics：运行时统计的载体

`MapOutputStatistics` 是 AQE 一切决策的数据基础。每个 shuffle stage 跑完后，driver 上的 `MapOutputTrackerMaster` 汇总所有 map task 的输出，得到一个核心字段：

```
bytesByPartitionId: Array[Long]   // 每个 reduce partition 的字节数
```

这就是真实分区大小。静态优化器只能估算整张表的大小，而 `bytesByPartitionId` 给出的是"shuffle 之后每个分区的精确字节数"——哪个分区大、哪个小、有没有倾斜，一目了然。三大特性全部基于这个数组。Broadcast stage 也有对应的 `BroadcastExchangeStats`，记录广播变量实际字节数，供 `DynamicJoinSelection` 判断能否把 SortMergeJoin 降级成 BroadcastHashJoin。

### 2.3 AQE 在 Exchange 节点处的插入点

AQE 不改 Catalyst 的逻辑计划层，只在物理计划层动手。三条规则的插入点：

- **CoalesceShufflePartitions**：作用在 `ShuffleQueryStage` 下游，把原本读 N 个分区的 shuffle 替换成读 M 个合并分区的 `CustomShuffleReaderExec`。
- **DynamicJoinSelection**：作用在 `SortMergeJoinExec` / `BroadcastHashJoinExec` 节点，根据子 stage 的真实统计决定是否切换。
- **OptimizeSkewedJoin**：作用在 `SortMergeJoinExec`，检测两侧倾斜分区并切分。

这三条规则都在 `QueryStagePreparations` 里、`EnsureRequirements` 之后执行，确保每完成一个新 stage 都会重新过一遍。

## 三、三大特性逐一拆解

### 3.1 动态合并 shuffle 分区（CoalesceShufflePartitions）

这是 AQE 最容易见效的特性。shuffle 分区数在执行前确定，但真实分区大小只有跑完才知道。AQE 拿到 `bytesByPartitionId` 后，把过小的相邻分区合并成大的，目标是让每个合并后的分区接近 `targetSize`。

targetSize 的计算（`ShufflePartitionsUtil.coalescePartitions`）：

1. `targetSize = max(advisoryPartitionSizeInBytes, totalSize / minNumPartitions)`，其中 `minNumPartitions` 由默认并行度推导，保证合并后分区数不低于一个下限，避免单分区过大；
2. 两者取大——advisory 是下限：数据量小、希望分区少时 advisory 生效；数据量大、minNumPartitions 高时 `totalSize/minNumPartitions` 生效，targetSize 被抬高；
3. 从 partition 0 起逐个累加字节，达到 targetSize 就切一刀形成合并分区；单分区已超 targetSize 的保持独立，不强行并入相邻分区。

效果：我那个作业总 shuffle 约 200GB、原 2000 分区，合并后落到约 200 个，单分区约 1GB <!-- 校准：请按真实经历核实/替换 -->。task 数从 2000 降到 200，调度开销大幅下降，同时不会出现 64MB 这种过小分区——原本空跑或几 MB 的 task 被并进邻居，CPU 利用率明显上来。

```
   合并前: 2000 个分区, 总 ~200GB, 大小严重不均
   |p0|p1|p2|..........|p1999|
    2M 1M 3M           1.9GB
   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   1900 个 <100MB 的废分区, 100 个 >500MB 的大分区

   合并后: ~200 个分区, 每个 ~1GB, 大小均匀
   |---c0---|---c1---|---c2---|...|---c199---|
     ~1.0GB   ~1.0GB   ~1.0GB       ~1.0GB
   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   CustomShuffleReader 把多个原 partition 读成一个逻辑分区
```

还有个前提容易忽略：CoalesceShufflePartitions 只在没有用户自定义分布要求时才生效。如果 SQL 里显式写了 `DISTRIBUTE BY` 或 `repartition(col)`，下游对分区数有硬约束，AQE 会跳过合并，避免破坏用户的分布语义。另外，合并发生在"所有上游 shuffle stage 都完成"之后——若当前 QueryStage 有多个 shuffle 输入（比如 union 之后的 join），要等所有输入的 `MapOutputStatistics` 都到位才能算 targetSize，因为 targetSize 取的是所有 shuffle 总量。

注意：合并只针对"没有数据倾斜"的 shuffle。一旦检测到倾斜分区，会交给 `OptimizeSkewedJoin` 单独处理，不会被强行并进合并分区。

### 3.2 动态切换 Join 策略（DynamicJoinSelection）

静态 JoinSelection 估算右表大小，可能把本该 broadcast 的表判成太大，走了 SortMergeJoin。AQE 跑完子 stage 后拿到真实字节数：如果发现右表实际只有 80MB，而 `autoBroadcastJoinThreshold` 设的 100MB，就把它从 SMJ 降级成 BHJ，省掉一次 shuffle + 一次排序。

反过来也会升级：本来静态判成能 broadcast 的，跑完发现实际 200MB 超了阈值，AQE 会升级成 SMJ，避免 broadcast 一个过大的变量把 executor 撑爆。实际中升级比降级少见，因为静态偏保守、宁可走 SMJ。这个特性在多表连续 join 时收益特别明显：第一个 join 跑完才知道中间结果多大，后续 join 的策略才能据此调准，避免一错错一串。

### 3.3 动态处理数据倾斜（OptimizeSkewedJoin）

这是我最常用、收益也最大的特性，专门针对 SortMergeJoin 两侧的倾斜分区。判定逻辑（`SkewJoinHandling`）：

1. 取一侧所有分区大小，算 **median（中位数）** 作为"正常大小"的基准；
2. 若某分区大小 > `median * skewedPartitionFactor`（默认 5 倍）**且** > `skewedPartitionThresholdInBytes`（默认 256MB）<!-- 校准：请按真实经历核实/替换 -->，判定为倾斜；
3. 对倾斜分区，按 `advisoryPartitionSizeInBytes` 切成多份，每份作为一个独立 task；
4. 两侧对应的倾斜分区同步切分（按 mapper id 分组，确保相同 key 落在同一切分子任务），保证 join 正确性。

```
   倾斜切分前: 分区 p3 拉了 50GB, 其他分区平均 ~200MB
   |p0  |p1  |p2  |   p3    |p4  |p5  |
    200M 200M 200M [  50GB  ] 200M 200M
                         ^
                  一个 task 跑 8 分钟, 其他 3 秒

   倾斜切分后: p3 被切成 ~200 份, 每份 ~256MB, 并行处理
   |p0|p1|p2|p3/0|p3/1|...|p3/199|p4|p5|
                   ^
            200 个 task 并行, 总耗时从 8min 降到 ~10s
```

切分是怎么保证 join 正确的？SortMergeJoin 两侧都按 join key shuffle 过，相同 key 一定落在两侧同序号的分区里。AQE 把倾斜分区 p3 按 mapper id 切成多份：p3/0 处理 mapper 0~50 对 p3 的贡献，p3/1 处理 mapper 51~100……两侧用同样的切分边界，于是 p3/0 左侧的 key 集合和 p3/0 右侧的 key 集合仍然完全对应，子 join 结果直接 union 就是原 p3 的 join 结果。代价是每个切分子任务都要重读对应 mapper 的 shuffle 输出，多了一次读，但换来了并行度。非倾斜分区不受影响，仍走正常的合并读。

两个细节值得记下：median 用的是中位数而非平均值，因为倾斜场景下平均值会被极值拉高、失去参考意义；同时用 `skewedPartitionThresholdInBytes` 设硬下限，防止小数据量场景误判（比如所有分区都才几 MB，某个分区比 median 大 5 倍也只 30MB，不该切）。这两个条件是 AND 关系，必须同时满足才切。

## 四、与静态优化器的关系

AQE 不是替代 Catalyst，而是在它之上加了一层运行时补丁。两者的分工：

| 维度 | 静态 Catalyst 优化器 | AQE |
|------|---------------------|-----|
| 作用时机 | 编译期（提交前） | 运行期（stage 之间） |
| 数据依据 | 表/列统计、直方图 | map 输出真实字节数 |
| 优化粒度 | 整个查询计划 | stage 之间、Exchange 边界 |
| 典型规则 | PredicatePushDown、ConstantFolding、JoinReorder | Coalesce、DynamicJoin、SkewJoin |
| 能否改算子顺序 | 能 | 不能（只改分区/策略/切分） |

一个直观结论：**AQE 只能"修整"静态计划，不能"重写"它**。静态优化器选错了 join 顺序、漏推了谓词，AQE 救不回来。所以写 SQL 时该 hint 还得 hint、该 broadcast 还得想清楚，AQE 是兜底不是万能药。两者的关系更像"静态优化器负责方向，AQE 负责路上的微调"。

还有一点实战体会：AQE 的决策基于字节数，不看行数也不看计算复杂度。这会导致一个盲区——某个分区字节数正常但行数特别多（比如一行是一个大数组或长字符串），join、聚合时仍然慢，AQE 却不会切分，因为它"看不懂"行数代价。遇到这种场景，我一般会在 AQE 之外手动按行数 `repartition`，或用 `approx_count` 做 hint。这其实呼应了一个更广的判断：AQE 优化的是数据分布层面的浪费，算子内部的计算代价它管不到。

## 五、全套配置与实战调参

生产环境常用的 AQE 配置，我整理成一个清单：

```conf
# 总开关。Spark 3.2+ 默认 true，3.0/3.1 默认 false
spark.sql.adaptive.enabled                       true

# 分区合并开关
spark.sql.adaptive.coalescePartitions.enabled    true
# 合并后每个分区的目标大小（下限），默认 64MB；我一般调到 128MB
spark.sql.adaptive.advisoryPartitionSizeInBytes  134217728    # 128MB

# 倾斜 join 开关
spark.sql.adaptive.skewJoin.enabled              true
# 倾斜判定倍数：相对 median 的倍数，默认 5
spark.sql.adaptive.skewJoin.skewedPartitionFactor        5
# 倾斜判定绝对阈值，默认 256MB
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes  268435456   # 256MB
```

调参经验与坑：

- `advisoryPartitionSizeInBytes` 是最该调的参数，但前提是它是 binding 的那一侧。它和 `totalSize/minNumPartitions` 取大值——若后者更大，改 advisory 没用，得动并行度。我踩过这个坑：以为调 advisory 能降分区数，结果 plan 没变，后来发现是 minNumPartitions 在 binding。advisory 调大→targetSize 下限抬高→合并后分区更少更大，方向是对的，但要看是否生效。
- `skewedPartitionThresholdInBytes` 不要设太小。设成 64MB 会让大量正常分区被误判倾斜，切分出海量小 task，反而更慢。256MB 是稳妥起点，集群内存大可调到 512MB。
- 如果作业里有 broadcast join，`autoBroadcastJoinThreshold` 可适当调大（比如 100MB），因为 AQE 会在运行时纠错，静态判错的代价被降低了。

## 六、AQE 的局限与坑

用了一年多，AQE 的边界我也踩清楚了，列几个最常踩的：

**坑一：AQE 只管 stage 之间，不管 stage 内部。** 一个 map stage 内部某个 task 倾斜（比如读 Parquet 时某个 row group 特别大），AQE 没法处理，因为它要等这个 stage 跑完才能拿到统计。这种只能靠读路径优化（`spark.sql.files.maxPartitionBytes`、split 大小）。

**坑二：广播变量膨胀。** DynamicJoinSelection 把 SMJ 降级成 BHJ 时，若右表实际接近阈值边界，broadcast 一个 90MB 的变量到几百个 executor，driver 内存和网络都可能吃紧。建议 `autoBroadcastJoinThreshold` 别设太大，或监控 broadcast 变量大小。

**坑三：倾斜切分的副作用——任务数暴涨。** 一个 50GB 的倾斜分区按 256MB 切，会产生约 200 个 task。多个 stage 同时触发切分，task 总数成倍增长，YARN 队列排队时间变长。我遇到过一次切分把 task 数从 2000 拉到 8000，反而因排队变慢了整体，最后把 `skewedPartitionThresholdInBytes` 调到 512MB 才稳 <!-- 校准：请按真实经历核实/替换 -->。

**坑四：AQE 与自定义 partitioner 冲突。** SQL 里用了 `repartitionByRange` 之类自定义分布时，AQE 的合并规则可能和它打架。3.2 之后这种情况 AQE 会跳过合并，低版本要手动关 `coalescePartitions.enabled`。

**坑五：统计收集本身的代价。** `MapOutputStatistics` 是 driver 在 stage 完成时汇总的，对超大 shuffle（几万个 map task）汇总本身有延迟，极端情况下会成为 driver 瓶颈。

**坑六：重新优化不是免费的。** 每完成一个 stage 就重新跑一遍规则，对几十个 stage 的大查询，累计耗时几百毫秒到秒级。通常远小于它省下的时间，但超短查询（秒级）上 AQE 偶尔反而变慢，这时可以按作业粒度关掉。

最后提一句版本成熟度。AQE 从 3.0 引入时还比较粗糙——skew join 只支持 SortMergeJoin、coalesce 在某些场景会退化、和 whole-stage codegen 偶有冲突；3.2 之后基本稳定，skew join 扩展到支持更多 join 类型，coalesce 与自定义分布的冲突也修了。我们生产上 3.3 用了一年没再遇到 AQE 自身导致的正确性问题，但 3.0/3.1 我建议谨慎，尤其 skew join 早期版本在切分边界处理上有过 corner case。升级到 3.2+ 再放心全量开 AQE 是更稳的选择。

## 小结

AQE 的本质，是把 Catalyst 从"一次性编译器"变成"边跑边调的 JIT"。它解决的是静态优化器看不到运行时数据分布的问题，三大特性——分区合并、join 策略切换、倾斜切分——分别对应 shuffle 分区数、join 策略、数据倾斜三个老大难。配合 `advisoryPartitionSizeInBytes` 和 `skewJoin` 那组参数，大部分 ETL 作业不用动 SQL 就能拿到 1.5x 量级的提升 <!-- 校准：请按真实经历核实/替换 -->。但它有边界：只管 stage 之间、不能重写计划、切分会涨 task 数。把它当兜底，而不是替代写好 SQL 的理由。

下一篇我打算聊聊 Spark 在流处理上的延伸——Structured Streaming。看看 Spark 怎么把批处理的 Catalyst、Codegen 那套家底搬到流上，又在哪里向流式语义做了妥协，watermark、late data、状态存储这些坑怎么填。

<!-- 总校准注释：本文所有具体数字（分区 2000→200、倾斜阈值 256MB、性能 1.5x、task 数变化、耗时降幅等）均为示例性占位，请按真实生产经历核实/替换后再发布。-->
