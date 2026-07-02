---
title: Spark 性能调优全景：把一个 4 小时任务压到 40 分钟的复盘
abbrlink: spark-performance-tuning
date: 2026-07-02 19:00:00
tags:
  - Spark
  - 性能调优
categories:
  - 技术
description: 从资源分配、序列化、广播、GC、partition 到 join 策略，分层复盘一次 Spark ETL 的性能治理。
ai_text: "本文以一次离线 ETL 任务从 4 小时压到 40 分钟的复盘为线索，系统梳理 Spark 性能调优全景：executor 资源木桶、堆内/堆外/overhead 内存切分、Kryo 序列化、广播与 broadcast hash join、G1 GC 调优、partition、数据本地性降级阶梯、join 策略、小文件治理与开发规范，给出可复用的分层定位方法论。"
---

## 引子：4 小时跑到 40 分钟的那次复盘

去年接手一个离线数仓的 Spark 任务，跑一张大宽表 ETL，每天增量约 800GB，整个 DAG 跑下来要 4 小时出头<!-- 校准：请按真实经历核实/替换 -->。SLA 卡得死，下游报表等它出数，几次延迟直接被投诉。组里之前调过几轮，无非是把 executor memory 从 8G 加到 16G、parallelism 拉到 500 之类，没本质改善。

我接手后没马上动参数，而是先把 Spark UI 的 Stage 时间线、Task 的 GC 时间、Shuffle Read/Write、Skew 情况全部拉出来看了一遍。结论是：瓶颈不在内存大小，而在三个地方——executor 资源配比失衡导致 HDFS client 线程打满、Shuffle partition 过多引发小文件风暴、GC 占比高达 35% 把 Task 拖住。逐一治理后，整个任务压到 40 分钟<!-- 校准：请按真实经历核实/替换 -->。这篇文章把那次调优全过程拆开讲，顺带把 Spark 性能调优的方法论沉淀下来。

调优方法论我坚持一条：**先定位瓶颈层级，再动参数。** 任何一个参数变更，都必须先有一个可量化的指标证明它需要改，改完之后这个指标能验证效果；如果只能说"好像快了"，那基本是无效调优。Spark 的性能问题可以归到资源分配、序列化、Shuffle 与 partition、内存与 GC、数据本地性、Join 策略这几大类，每一类都有典型的现象和定位手段：

| 层级 | 典型瓶颈现象 | 主要观察指标 |
|------|------|------|
| 资源分配 | Stage 拖尾、HDFS 读排队 | Spark UI Task 时间线、executor 用率 |
| 序列化 | Shuffle 慢、cache 体积大 | Shuffle Write 字节、Storage 占用 |
| Shuffle/partition | 小文件风暴、task 饿死 | partition 数、单 task 处理量 |
| 内存与 GC | Task GC 红条、Spill | Task GC 时间、Spill(Memory/Disk) |
| 数据本地性 | task 排队等本地节点 | Locality Level 分布 |
| Join 策略 | 大表 shuffle 倾斜、BNLJ 出现 | EXPLAIN 物理计划 |

排查时从现象往层级压，再从参数往指标验。下面按这个顺序展开。

## 一、资源分配：executor 数量/cores/memory 的木桶理论

这是被问得最多也最容易配错的一层。很多人觉得"加机器就行"，但 executor 的三个维度——cores、memory、数量——是木桶关系，一根短板就拖垮整体。

```
        Executor 资源木桶：三轴不能有一根短板
   ┌──────────────────────────────────────────────┐
   │   cores(并发)      memory(吞吐)    数量(并行)  │
   │      │                │               │      │
   │   5 cores          20G heap       50 execs    │
   │      │                │               │      │
   │ 偏少→CPU闲置      偏小→Spill/OOM  偏少→拖尾    │
   │ 偏多→HDFS线程争用  偏大→GC停顿长  偏多→调度开销 │
   └──────────────────────────────────────────────┘
```

几个我踩过的坑：

**cores 不是越多越好。** 早期我照着网上教程把 `--executor-cores 8` 甚至更高，结果发现 HDFS client 的线程池成了瓶颈。每个 executor 内部共享一组 HDFS client（`dfs.client.read.thread.pool.size` 默认 10），cores 太多时并发读请求排队，反而比 4 cores 慢。我的经验值是 HDFS 读密集场景 cores 给 4–5，纯计算可以到 5–6<!-- 校准：请按真实经历核实/替换 -->。另外 cores 太多还会放大 task 之间在 sort/shuffle 阶段的竞争。

**memory 不是越大越好。** 堆一大，Full GC 一把就几秒甚至十几秒，直接把 Stage 时间线打出毛刺。我一般单 executor 堆控制在 16–24G，配合 G1 GC；超过 32G 还要考虑压缩指针失效的问题。

**executor 数量要和 partition 数量匹配。** 一个粗略公式：`executor 数 ≈ total cores / cores per executor`，partition 数 ≈ `executor 数 × cores × 2~3`。partition 太少 task 饿死，太多调度开销大。

那次任务最终调成 5 cores × 20GB × 50 个 executor<!-- 校准：请按真实经历核实/替换 -->，整体并行度提上来，HDFS 读也不再排队。

补充一点关于 driver 的资源。cluster 模式下 driver 也是个独立 container，`--driver-memory` 给小了 collect 大结果或 broadcast 大表时会 OOM，给大了又浪费。我的经验是常规 ETL 给 4–8G，带 collect 到 driver 的任务按 collect 量 × 1.5 留余量。另外**动态资源分配**（`spark.dynamicAllocation.enabled=true`）在长跑任务和流处理里很有用，它能根据 task 积压自动增减 executor，配合 external shuffle service 才能安全回收 executor——YARN 上要确保 `spark.shuffle.service.enabled=true`，否则回收会丢 shuffle 文件。但短任务别开，冷启动拉 executor 的开销比省下的还多。

一个完整的 spark-submit 模板：

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name dwd_etl_wide_table \
  --queue production \
  --num-executors 50 \
  --executor-cores 5 \
  --executor-memory 20G \
  --executor-memory-overhead 4G \
  --driver-memory 8G \
  --driver-cores 4 \
  --conf spark.sql.shuffle.partitions=800 \
  --conf spark.default.parallelism=800 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrationRequired=true \
  --conf spark.sql.broadcastTimeout=1200 \
  --conf spark.locality.wait=6s \
  --conf spark.locality.wait.process=3s \
  --conf spark.locality.wait.node=2s \
  --conf spark.sql.autoBroadcastJoinThreshold=104857600 \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC" \
  --class com.xxx.DwdEtlMain \
  /opt/apps/dwd-etl-1.0.jar
```

这里有个细节：`executor-memory-overhead` 是堆外内存，默认是 executor-memory 的 10%，但开了堆外 shuffle 或 Python worker 时往往不够，要单独提。

## 二、Executor 内存切分：堆内、堆外、overhead

很多人把 memory 理解成"一个堆"，其实 Spark executor 的内存空间是三块：

```
   ┌─────────────────────────────────────────────────┐
   │            executor-memory (堆内 JVM Heap)        │
   │  ┌────────┬──────────────┬────────────────────┐  │
   │  │Reserved│  User Memory │  Unified(Exec/Stor)│  │
   │  └────────┴──────────────┴────────────────────┘  │
   ├─────────────────────────────────────────────────┤
   │         memoryOverhead (堆外, 默认 10%)            │
   │   PySpark worker / 堆外 shuffle / NIO direct     │
   ├─────────────────────────────────────────────────┤
   │  spark.memory.offHeap.size (启用 offHeap 时)      │
   └─────────────────────────────────────────────────┘
```

- 堆内：受 `spark.memory.fraction`、`storageFraction` 控制（前一篇讲过，不重复）。
- overhead：默认 `max(executor-memory × 0.1, 384MB)`，开 Kryo 堆外或 Python 时要加到 4G 左右，否则容易 `Container killed by YARN for exceeding memory limits`。
- offHeap：`spark.memory.offHeap.enabled=true` + `spark.memory.offHeap.size`，绕过 JVM 堆，避免 GC 扫描大对象。代价是要手动管理，UDF 里别乱用。

那次任务我把 overhead 从默认 2G 提到 4G，`Container killed` 的告警就消失了<!-- 校准：请按真实经历核实/替换 -->。

这里再强调一下堆内两块的动态借用：Unified 里的 Storage 和 Execution 是可以互相借的——Shuffle 不紧张时 Storage 可以多占去缓存 RDD，Execution 紧张时又能把 Storage 挤出去。但有个铁律：**Execution 内存不足时可以强制回收 Storage 借走的，Storage 不足却不能抢 Execution 的**，只会把缓存块逐出（evict）。所以一次 OOM 复盘的根因往往是缓存了太多 broadcast/RDD 把 Execution 挤到边角，Shuffle 一来就借不到钱。判断方法是看 Spark UI Storage 页面占用是否逼近 Storage 上限，是的话先 `unpersist` 掉用不上的缓存。

## 三、序列化：Kryo 几乎是无脑选

Spark 默认用 Java Serializer，兼容性好但慢、体积大。Kryo 速度快 5–10 倍，体积小 3–5 倍<!-- 校准：请按真实经历核实/替换 -->。两者对比：

| 维度 | Java Serializer | Kryo |
|------|------|------|
| 序列化速度 | 慢，走反射 | 快，基于字节码生成 |
| 体积 | 大，带完整类描述 | 小，需注册类名 |
| 兼容性 | 任意 Serializable | 需注册，部分类要自定义 |
| 适用 | 兼容旧代码/闭包 | 生产首选 |

生产环境我一律切 Kryo：

```scala
val conf = new SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.kryo.registrationRequired", "true")
conf.registerKryoClasses(Array(
  classOf[com.xxx.DwdRecord],
  classOf[com.xxx.DimensionKey],
  classOf[Array[com.xxx.DwdRecord]],
  classOf[scala.Tuple2[_, _]],
  classOf[org.roaringbitmap.RoaringBitmap]
))
```

两个关键点：一是 `registrationRequired=true` 强制注册，否则遇到没注册的类 Kryo 会回退到全类名写入，性能反而更差；二是注册顺序要稳定，类注册 ID 一旦变化会导致已序列化数据反序列化失败（Streaming/checkpoint 场景要注意）。RDD cache、Shuffle、Broadcast 全程都受益于 Kryo。

## 四、广播变量与 broadcast hash join

广播变量是把小数据在 driver 端打包后通过 BitTorrent 协议分发到每个 executor，避免每个 task 重复拉取。两个核心用途：

1. **业务侧广播**：维度表、配置字典、IP 库、RoaringBitmap 之类。阈值要控制，单个 broadcast 别超过 2GB（Spark 单个块上限），我一般控制在 500MB 以内。
2. **引擎侧 broadcast hash join**：Spark SQL 在小表小于 `spark.sql.autoBroadcastJoinThreshold`（默认 10MB）时自动走 broadcast hash join，把小表广播到所有 executor，大表走 map 端 join，省掉整个 shuffle。

那次任务有个 fact 表 join 维度表的场景，维度表 80MB<!-- 校准：请按真实经历核实/替换 -->，默认阈值 10MB 走不了 broadcast，走了 sort merge join，shuffle write 几百 GB。我把阈值调到 100MB：

```bash
--conf spark.sql.autoBroadcastJoinThreshold=104857600
```

整个 stage 的 shuffle 直接消失，时间从 25 分钟降到 6 分钟<!-- 校准：请按真实经历核实/替换 -->。代价是 driver 端要先把小表拉过来，`spark.sql.broadcastTimeout` 默认 300s，大表 broadcast 要适当调大。但维度表如果不能装进 driver 内存或频繁更新，别硬广播，会 OOM。

广播分发的底层是 BitTorrent 协议：driver 把变量切成 4MB 的块，executor 之间互相拉块，而不是每个都从 driver 拉，所以几百个 executor 也不会把 driver 打爆。判断广播是否生效，看 Stage 详情里的 "Broadcast" 列和 `accumulator`。另外 AQE（自适应执行）开起来后，Spark 会根据运行时统计动态调整 broadcast，比静态阈值更稳——它能在运行时发现某个被估成大表的输入其实只有几十 MB，临时改走 BHJ。

## 五、GC 调优：从 ParallelGC 到 G1

那次任务 GC 时间占比 35%，Task 时间线一片红<!-- 校准：请按真实经历核实/替换 -->。默认 executor 用 ParallelGC，大堆下 Full GC 停顿几秒，直接卡死所有 task。我切到 G1：

```
   调优前 (ParallelGC, 20GB 堆):
   |==#####=====#####=====#####=====#####====|
       ^    ^    ^    ^    ^    ^    ^
      FGC  FGC  FGC  FGC  FGC  FGC  FGC
   GC 占比 35%, 单次 Full GC ≈ 1.2s, Stage 抖动严重

   调优后 (G1, 20GB 堆):
   |·--·--·--·--·--·--·--·--·--·--·--·--·--·|
    YGC YGC YGC Mix Mix Mix Mix YGC YGC
   GC 占比 8%, 单次 Mixed GC ≈ 80ms, 无长停顿
```

G1 参数：

```
-XX:+UseG1GC
-XX:G1HeapRegionSize=16m
-XX:InitiatingHeapOccupancyPercent=35
-XX:MaxGCPauseMillis=200
-XX:+ParallelRefProcEnabled
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps
```

几个经验值：

- `G1HeapRegionSize`：堆 16–32G 给 16m，堆大于 32G 给 32m。region 太小 GC 频繁，太大浪费。
- `InitiatingHeapOccupancyPercent`：默认 45，内存压力大时降到 35，让 mixed GC 提前启动避免 Full GC。
- `MaxGCPauseMillis`：别设太小，100ms 以下 G1 会频繁回收反而拖累吞吐，我给 200ms。
- `ParallelRefProcEnabled`：一定要开，否则 reference 处理是单线程，慢得离谱。

切 G1 后 GC 占比从 35% 降到 8%<!-- 校准：请按真实经历核实/替换 -->。

调 G1 不能只设参数，还得会读 GC 日志。关键看几个信号：`Pause` 后跟的 `young/mixed/full` 区分回收类型，`G1EvacuationPause` 里 `to-space exhausted` 说明 survivor 装不下、要扩 region 或降并行度，连续出现 `Full GC` 基本是 IHOP 没触发 mixed GC、堆碎片化严重。我一般把 GC 日志开起来后用 `jstat -gcutil` 或 GCEasy 之类工具画成图，看 young/mixed/full 的频率和停顿分布。还有一个常被忽略的点：executor 堆里大量短生命周期对象来自 shuffle 的序列化 buffer 和 Spark 内部的 `TaskMemoryManager` 页表，开 Kryo + offHeap 能直接砍掉这部分对象创建，从根上减轻 GC 压力——所以序列化、内存、GC 三者其实是联动的。

但要提醒：G1 不是银弹，堆小于 8G 时 ParallelGC 吞吐反而更高；堆超 32G 又对延迟敏感，可以试 ZGC/Shenandoah。

## 六、partition 数量：spark.sql.shuffle.partitions

这参数默认 200，绝大多数场景都偏小。partition 太少 → 单 task 数据大、内存压力大、并行度低；太多 → 调度开销大、小文件多。经验值是单 partition 数据量 100–500MB<!-- 校准：请按真实经历核实/替换 -->。

| 参数 | 默认 | 建议 | 说明 |
|------|------|------|------|
| `spark.sql.shuffle.partitions` | 200 | 800–2000 | SQL shuffle 后分区数 |
| `spark.default.parallelism` | 依赖 | cores × 2~3 | RDD 并行度 |
| `spark.sql.adaptive.coalescePartitions.enabled` | false | true | AQE 自动合并小分区 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | 128MB | 合并后目标分区大小 |
| `spark.sql.adaptive.skewJoin.enabled` | false | true | 倾斜 join 自动拆分 |

我那次直接把 shuffle partitions 拉到 800，再开 AQE 自动合并，既保证并行度又避免小文件。

## 七、数据本地性与 locality.wait 降级阶梯

Spark 调度会优先把 task 调度到数据所在节点（PROCESS_LOCAL），等不到就降级：PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY。每级等待 `spark.locality.wait`（默认 3s）。

大集群里默认 3s 太长，task 排队等本地性反而拖慢。我一般缩短成阶梯递减：

```
--conf spark.locality.wait=6s
--conf spark.locality.wait.process=3s
--conf spark.locality.wait.node=2s
```

让 task 快速降级到 ANY，避免饥饿。但反过来说，如果是 HDFS cache 命中场景，本地性收益大，可以适当拉长。这个参数要看数据源特性调，不能一刀切。

还有一个本地性失效的常见坑：**Shuffle 之后第一个 stage 没有数据本地性可言**，因为 shuffle 输出的数据落在哪个 executor 是随机的，下游 task 只能按 `ANY` 调度。所以看到某 stage 全是 `ANY` 别急着调 `locality.wait`，那不是它的锅。真正能吃到本地性的是直接读 HDFS/外部数据源的首个 stage，以及开了 external shuffle service 后复用同节点 shuffle 文件的情况。

## 八、Join 策略选择：别让引擎替你做所有决定

Spark SQL 的 join 策略优先级：BroadcastHashJoin > SortMergeJoin > ShuffledHashJoin > BroadcastNestedLoopJoin > CartesianProduct。生产里 90% 是 SMJ 和 BHJ。几个判断点：

- 小表 < broadcast 阈值 → BHJ，最省。
- 两表都大，有等值条件 → SMJ，稳定。
- 一表大但分布均匀、另一表中等大小（> 阈值但能进 executor 内存）→ 手动 broadcast 或 ShuffledHashJoin。
- 非等值 join → 只能 BNLJ 或 Cartesian，能 avoid 就 avoid。

那次任务里有个用户行为表 join 商品维表，维表 80MB，我把阈值调到 100MB 走 BHJ；另一个场景是两个超大 fact 表 join，走 SMJ 但开了 `spark.sql.adaptive.skewJoin.enabled=true` 处理倾斜，倾斜 key 自动拆分，从 OOM 边缘救回来<!-- 校准：请按真实经历核实/替换 -->。

skew join 的拆分原理值得说一句：AQE 在 shuffle 阶段结束后会统计每个 partition 的数据量，发现某个 partition 远超中位数（默认 5 倍以上）就判定为倾斜，把这个 partition 按 `spark.sql.adaptive.skewJoin.skewedPartitionFactor` 拆成多个小 partition，与小表对应分区做多次 join 后再 union。这比开发侧手动 `salt + explode` 优雅得多，但前提是 join 两侧都得走 shuffle——也就是说 BHJ 是不会触发 skew 拆分的，这是很多人开了 AQE 仍倾斜的原因。

## 九、小文件治理与推测执行

**小文件问题**在 Spark SQL 输出尤其严重。shuffle partitions 太多、动态分区写入都会产生海量小文件，拖累下游 NameNode 和读取。治理手段：

1. **AQE 合并**：`spark.sql.adaptive.coalescePartitions.enabled=true`，shuffle 后自动合并。
2. **写入前 repartition**：`df.repartition(N)` 或 `coalesce(N)` 控制输出文件数，动态分区写入按分区键 repartition。动态分区写入有个隐藏的放大效应——每个 task 会为它接触到的每个分区单独落一个文件，task 多、分区多时文件数是 `task 数 × 分区数` 量级，很容易单 job 产出几十万小文件。治理上要么按分区键 `distribute by` 让每个分区落到少数 task，要么用 `spark.sql.sources.partitionOverwriteMode=dynamic` 配合控制。
3. **Hive 端 compaction**：对 ACID 表定期跑 `ALTER TABLE ... COMPACT`，合并小文件。
4. **ANALYZE 统计**：`ANALYZE TABLE t COMPUTE STATISTICS`，让 Cost Based Optimizer 拿到准确统计，避免错误走 broadcast 或 broadcast 超阈值。

**推测执行**（speculative execution）要谨慎。默认关闭，开起来后慢 task 会被复制重跑，长尾有奇效，但有两个坑：一是输出到外部存储（数据库、HBase）的 task 开推测执行会重复写，二是慢 task 可能是数据倾斜不是机器问题，复制一份还是慢。我的规矩是只对纯计算、幂等输出的任务开：

```
--conf spark.speculation=true
--conf spark.speculation.quantile=0.9
--conf spark.speculation.multiplier=2
```

## 十、与开发规范结合

调优不是 DBA 一个人能扛的，得落到开发规范里。我给团队定了几条铁律：

- **写 SQL 先看执行计划**，`EXPLAIN` 里出现 CartesianProduct 或 BroadcastNestedLoop 的一律打回。
- **join 前过滤**，谓词下推要养成习惯，别让引擎扫一堆再扔。
- **避免 group by 后立刻 join 同一 key**，倾斜会放大。
- **cache 要有命名并显式 unpersist**，`df.persist(StorageLevel.MEMORY_AND_DISK)` 配合 `unpersist`，别让 storage 内存被占死。
- **UDF 慎用**，能用原生表达式就用，UDF 会打破 Catalyst 优化。
- **广播维度表用 `SparkContext.broadcast` 而不是 join**，跨多个 transformation 复用。
- **上线前看一眼 Task 分布**，最长 task 与中位数比值超过 3 基本就是倾斜，先治倾斜再谈别的，否则加再多资源也只是喂给那条长尾。
- **大表写入用列式 + 合理分区**，Parquet 配合 `repartition` 控制文件数，既能压缩又能减少下游扫描量。

规范一旦形成肌肉记忆，很多性能问题在开发阶段就被掐掉了，不用等到上线再救火。

## 小结

这次 4 小时到 40 分钟的调优，本质上是在十个层面分别找短板：资源配比、内存切分、序列化、广播、GC、partition、本地性、join 策略、小文件、开发规范。没有银弹，只有分层定位 + 量化度量。每一项变更都对应 Spark UI 上一个可见的指标变化，这才叫调优，否则就是瞎改。

写到这里，资源调优这一层基本覆盖了。但实际部署形态本身也会极大影响性能和资源利用率——同样的任务跑在 YARN 上和跑在 K8s 上，弹性扩缩、资源隔离、调度延迟、本地存储、shuffle service 完全是两套故事。下一篇我就来拆 Spark on K8s vs YARN 的选型与踩坑，看看云原生到底是噱头还是真香。

<!-- 总校准：本文所有具体数字（任务时长 4h→40min、executor 5core×20g×50、HDFS client 线程默认 10、GC 占比 35%→8%、单次 Full GC 1.2s/Mixed GC 80ms、维度表 80MB、shuffle partitions 800、各经验阈值等）均为示例性复盘数据，请按真实生产经历核实或替换后再发布。-->
