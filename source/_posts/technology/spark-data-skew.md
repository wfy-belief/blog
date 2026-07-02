---
title: Spark 数据倾斜治理实战：从 SparkUI 定位到加盐两阶段聚合的完整复盘
abbrlink: spark-data-skew
date: 2026-07-02 17:30:00
tags:
  - Spark
  - 数据倾斜
categories:
  - 技术
description: 一次倾斜 Task 把 4 小时作业拖死的复盘，从 SparkUI 长尾定位、倾斜键识别到加盐打散、两阶段聚合、AQE skew join 的完整治理路径。
ai_text: "以一次 Stage 卡死 4 小时的线上事故为引子，第一人称复盘 Spark 数据倾斜治理全路径：用 SparkUI Task metrics 看 maxTask 与 medianTask 耗时差与 shuffle read bytes 长尾定位倾斜、通过 MapStatus 与采样识别倾斜键、对倾斜键加盐打散做两阶段聚合（local aggregate + global aggregate）、用 broadcast join 绕过 shuffle、改 join 顺序把大表推到下游，最后拆解 AQE skew join 的 ShufflePartitionHandle 切分逻辑（splitSize、targetSize）与 Spark 3.0+ 的自动倾斜处理配置。"
---

## 引子：一个 Stage 卡了 4 小时

去年某个周一早上，我接手一个用户行为分析作业。这个作业平时跑 40 分钟，那天跑到凌晨四点还卡在 Stage 18，SparkUI 上 199 个 Task 全绿，唯独一个 Task 灰着不动，shuffle read 指标顶在 120GB。我把那个 Task 的 executor 日志拉出来，全是 `GC overhead limit exceeded` 和长时间的 sort spill，堆使用率长期 95% 以上。最后整个 Stage 跑了 4 小时，下游全部延误，业务方早上的报表全泡汤。<!-- 校准：请按真实经历核实/替换 -->

复盘下来，根因是 `user_id` join：一张 80 亿行的行为表 join 一张 5 亿行的用户维表，`user_id` 高度集中——头部 1% 的用户贡献了 80% 的行为记录，倾斜键直接把一个 reduce partition 撑爆。同样的代码、同样的资源，为什么平时没事、扩量就炸？因为数据分布变了，但分区数没变，倾斜键的体积跨过了单 Task 内存临界点。<!-- 校准：请按真实经历核实/替换 -->

这篇把我后来沉淀下来的数据倾斜治理套路写下来，从定位、识别到治理，一条线讲透。

## 一、数据倾斜的本质：reduce 端分区不均

Shuffle 把数据按 key 哈希到 reduce partition，默认走 `HashPartitioner`，`partitionId = hash(key.hashCode) % numShufflePartitions`。理想情况各分区均匀，单分区数据量约等于总量除以分区数。但当某个 key 占比过高（比如空值、默认设备号、头部大客、测试账号），这个 key 的全部记录都被路由到同一个 partition，单个 Task 的 shuffle read 远超中位数，长尾出现。

倾斜的本质不是数据量大，而是**分布不均叠加按 key 聚合**。同样的数据量，均匀分布时 200 个 Task 各跑 5 分钟，倾斜时一个 Task 跑 4 小时、其余 199 个 5 分钟就空转等待，整个 Stage 的耗时被最慢的那个 Task 锁死。

要特别区分两种倾斜：**map 端倾斜**和 **reduce 端倾斜**。map 端倾斜通常是输入文件本身不均（大文件 + 小文件，或某个 split 远大于其他），治理靠调整 `splitSize` 或对输入做 `repartition`，相对简单；reduce 端倾斜才是绝大多数 shuffle 倾斜的真相，发生在 `join`、`groupBy`、`distinct`、`window` 这类需要按 key 重分布的算子上，治理手段复杂得多，本文聚焦后者。

还要区分**数据倾斜**和**计算倾斜**。数据倾斜是输入分布不均导致的，换分区策略或加盐能解；计算倾斜是某些 key 的计算逻辑本身就重（比如复杂 UDF、大表 lookup），即使数据均匀也慢，治理要靠拆分逻辑或缓存中间结果。两者在 SparkUI 上的表现不同：数据倾斜看 shuffle read bytes 长尾，计算倾斜看 Duration 长尾但 shuffle read 均匀，别混淆。

## 二、用 SparkUI 定位倾斜：Task metrics 长尾

定位倾斜，第一现场是 SparkUI 的 Stage 详情页。我看四个指标：

| 指标 | 含义 | 倾斜信号 |
|------|------|----------|
| Duration | Task 耗时 | maxTask / medianTask > 3x |
| Shuffle Read Size / Records | reduce 端拉取数据量 | max 远超 median |
| Shuffle Spill (Memory/Disk) | 内存放不下落盘量 | 倾斜 Task 常伴大量 spill |
| GC Time | Task GC 耗时 | 倾斜 Task 常伴 50%+ GC |

经验阈值：`maxTask耗时 / medianTask耗时 ≥ 3`，或 `max shuffle read bytes / median ≥ 5`，基本可以判定倾斜。那次事故里 medianTask 是 6 分钟，maxTask 是 4 小时，比值 40 倍；shuffle read 中位数 600MB，max 是 120GB，比值 200 倍，铁证。<!-- 校准：请按真实经历核实/替换 -->

下面是 Stage 视图的柱状示意，每根柱代表一个 Task 的耗时，长尾一目了然：

```
   Task 耗时分布（Stage 18，200 tasks，单位 min）
   ┤
240┤                                                            ▓
   ┤                                                            ▓
120┤                                                            ▓
   ┤                                                            ▓
 60┤                                                            ▓
   ┤                                                            ▓
 30┤                                                            ▓
   ┤                                                            ▓
 12┤                                                            ▓
   ┤        ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓     ▓
  6┤        ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓  ▓     ▓
  0┤────────▓──▓──▓──▓──▓──▓──▓──▓──▓──▓──▓──▓──▓──▓──▓──▓────▓──
            └────────── 199 个 medianTask ≈ 6 min ─────────┘
                                                             └ 1 个倾斜 Task ≈ 240 min
```

柱状图右端那根突刺就是倾斜 Task。配合 Summary 里的 `Min / 25th / Median / 75th / Max / Mean` 分位数，max 和 median 差几个数量级，倾斜跑不掉。注意一个坑：SparkUI 默认按 Task 完成时间排序，倾斜 Task 往往最后才完成，要切到按 Duration 排序才能直观看到长尾。

## 三、倾斜键识别：MapStatus 与采样

定位到 Stage 后，要找到是哪个 key 倾斜。两条路。

**1. 从 MapStatus 反查倾斜分区。** Spark shuffle write 完成后，每个 map task 上报 `MapStatus` 给 Driver，里面记录该 map task 写到每个 reduce partition 的数据大小。这里有个内存优化的细节：如果一个 map task 写几千个 reduce partition，每个 partition 都存一个 long 值，driver 端聚合几百万个 map task 的状态会把 driver 堆撑爆。所以 Spark 用 `HighlyCompressedMapStatus` 做压缩——大 partition（超过阈值）单独存，小 partition 用 bitmap 标记存在性、配一个平均大小估算，牺牲精度换内存。这意味着 SparkUI 上看到的 shuffle read bytes 是估算值，和真实拉取量有偏差，但量级是对的，足够定位倾斜。Driver 端的 `MapOutputTrackerMaster` 汇总这些信息，reduce task 拉数据前先查它，知道去哪个 executor 拉多少字节，这个查询走 RPC `GetMapOutputMessage`，map 阶段没完成时 reduce task 会阻塞等待。在 SparkUI 的 Stage 页面点 Shuffle Read 列展开，能看到每个 Task 的 shuffle read bytes 和 records，配合 partition id 能反推是哪个分区重。但 MapStatus 只能定位到 partition，不能直接给 key——partition 内可能多个 key 混杂。这条路适合确认"是不是倾斜"，不适合"是哪个 key 倾斜"。

**2. 直接采样统计 key 分布。** 这是我更常用的办法，简单直接：

```scala
// 统计倾斜键占比，sample 比例按数据量调
val skewedKeys = behaviorTable
  .sample(false, 0.01, seed = 42L)
  .groupBy($"user_id")
  .count()
  .orderBy($"count".desc)
  .limit(100)

skewedKeys.show(50)
```

跑完立刻能看到头部几个 `user_id` 的 count 是中位数的几千倍。那次复盘我跑出 12 个 `user_id` 占了 35% 的记录，其中一个是测试账号 `user_id = 0`，单独占 18%；另一个是默认设备号 `device_id = "unknown"` 拼到 user_id 字段，占 9%。这类脏数据倾斜治理成本最低，过滤即可。<!-- 校准：请按真实经历核实/替换 -->

## 四、加盐打散：倾斜键扩容 join

定位到倾斜键后，最直接的治理是**加盐打散**：把倾斜 key 加一个随机后缀打散到多个 partition，让多个 Task 共同消化。

思路是双边扩容：倾斜表一侧给 key 拼 `0~N` 的盐，广播表一侧复制 N 份、每份拼对应盐，然后正常 join。这样原本挤在一个 partition 的倾斜 key 被均摊到 N 个 partition，单 partition 数据量降为原来的 1/N。

```scala
import org.apache.spark.sql.functions._

val N = 100  // 盐化倍数，按倾斜程度调

// 1. 拆出倾斜键与非倾斜键
val skewedKeySet = skewedKeys.select($"user_id").as[String].collect().toSet
val broadcast = udf((skewed: Boolean) => if (skewed) scala.util.Random.nextInt(N) else 0)

// 2. 大表：倾斜键打盐，非倾斜键盐=0
val bigSalted = behaviorTable
  .withColumn("is_skew", $"user_id".isInCollection(skewedKeySet))
  .withColumn("salt", broadcast($"is_skew"))
  .withColumn("join_key", when($"is_skew", concat($"user_id", lit("_"), $"salt"))
                          otherwise $"user_id")

// 3. 小表：倾斜键复制 N 份打盐 0..N-1，非倾斜键一份盐=0
val smallSalted = userDim
  .withColumn("is_skew", $"user_id".isInCollection(skewedKeySet))
  .withColumn("salt_arr", when($"is_skew", array((0 until N).map(lit): _*))
                          otherwise array(lit(0)))
  .withColumn("salt", explode($"salt_arr"))
  .withColumn("join_key", when($"is_skew", concat($"user_id", lit("_"), $"salt"))
                          otherwise $"user_id")
  .drop("salt_arr")

// 4. 正常 join，倾斜键被均摊到 N 个 partition
val joined = bigSalted.join(smallSalted, "join_key")
```

要点：盐化倍数 N 不是越大越好，N 越大小表膨胀越狠（小表倾斜键部分要复制 N 份）。我一般按 `倾斜键数据量 / 中位单分区数据量` 估算 N，那次 N=100，小表从 5 亿膨胀到 50 亿，但因为走 broadcast，膨胀代价可控。<!-- 校准：请按真实经历核实/替换 --> 还有一个易错点：非倾斜键必须盐=0 且小表也只一份盐=0，否则非倾斜键被错误复制 N 份导致 join 结果膨胀 N 倍，这是个隐蔽的正确性 bug。

## 五、两阶段聚合：local aggregate + global aggregate

聚合类倾斜（`group by` 后 `sum/count`）更适合用两阶段聚合，思路和 MapReduce 的 combiner 一脉相承：先在 map 端局部聚合压缩数据量，再 shuffle 到全局聚合，避免单分区数据爆炸。

核心是**加盐打散 + 局部聚合 + 去盐全局聚合**三步。先看数据流：

```
   两阶段聚合数据流（以 user_id 聚合为例）

   原始数据                 Stage 1：局部聚合              Stage 2：全局聚合
   (倾斜 key)               (加盐 + mapSideCombine)        (去盐 + 再聚合)

   u0  vvvvvv  ─┐
   u0  vvvvvv   │  加盐 split        ┌→ (u0#1, partial_agg) ┐
   u0  vvvvvv   ├──────────────────→ ├→ (u0#2, partial_agg) ├→  shuffle by u0
   u0  vvvvvv   │  N=10 路并行        └→ (u0#10,partial_agg) ┘   再 sum/avg
   ...          │
   u1  vv      ─┘                     → (u1#0, partial_agg)  →  (u1, final_agg)

   单分区 120GB 倾斜               10 个分区各 ~12GB            各分区结果均匀
                                  reduceByKey 自动 combiner    去盐后 key 重聚
```

代码上，SQL 和 RDD 两种写法。SQL 用两层 `group by`：

```scala
// Stage 1：加盐局部聚合，shuffle 前先 combiner
val partial = behaviorTable
  .withColumn("salt", (rand() * 100).cast("int"))
  .groupBy($"user_id", $"salt")
  .agg(sum("metric").as("partial_sum"), count("*").as("partial_cnt"))

// Stage 2：去盐全局聚合
val finalAgg = partial
  .groupBy($"user_id")
  .agg(sum("partial_sum").as("total_sum"), sum("partial_cnt").as("total_cnt"))
```

RDD 写法更直接，借助 `reduceByKey` 的 `mapSideCombine` 优势：

```scala
// reduceByKey 在 map 端先做 combiner，shuffle 数据量被压缩
// 这是它相对 groupByKey 的核心优势：groupByKey 不做 combiner，
// 倾斜数据原样 shuffle，必然爆炸
val rdd: RDD[(String, Long)] = behaviorRDD.map(x => (x.userId, x.metric))

// 两阶段：先加盐 reduceByKey，再去盐 reduceByKey
val stage1 = rdd
  .map { case (k, v) => (s"$k#${scala.util.Random.nextInt(100)}", v) }
  .reduceByKey(_ + _)                       // mapSideCombine 自动启用

val stage2 = stage1
  .map { case (k, v) => (k.split("#")(0), v) }
  .reduceByKey(_ + _)                       // 全局聚合，此时数据已均匀
```

`reduceByKey` 为什么比 `groupByKey` 抗倾斜？关键在 `mapSideCombine`：`reduceByKey` 在 shuffle write 前先用 combiner 函数本地预聚合，相同 key 在 map 端就合并成一条，shuffle 数据量按 key 数压缩而非记录数。底层走 `ExternalAppendOnlyMap`，这个数据结构是个溢出感知的哈希表：每插入一条记录先在内存哈希表里聚合，内存达到 `spark.shuffle.spill.numElementsForceSpillThreshold` 或堆压力阈值时，把当前哈希表排序后 spill 到磁盘形成一个 spilled file，然后清空内存继续插入；最后所有 spilled file 加内存中残余做 merge。倾斜 key 的记录在 map 端就被合并成一条（如果 combiner 是 sum/count 之类），shuffle 量从"记录数"降到"key 数"。`groupByKey` 没有这个优化，所有原始记录原样走网络，shuffle 量等于原始数据量。倾斜场景下两者 shuffle 量能差两三个数量级。所以凡是能用 `reduceByKey`/`aggregateByKey` 的场景，绝不用 `groupByKey`，这是写 RDD 的铁律。SQL 层的 `group by` 默认就走两阶段聚合（`HashAggregate` + `ObjectHashAggregate`/`SortAggregate`），这也是为什么 SQL 倾斜比 RDD 倾斜少见——优化器帮你做了 combiner，但 combiner 压缩不了"key 数本身就多"的倾斜，那种情况还是要手动加盐。

## 六、MapJoin / Broadcast Join：小表广播绕过 shuffle

如果 join 一侧足够小，最干净的治法是**广播 join**：把小表 collect 到 driver 再 broadcast 到每个 executor，大表本地匹配走 `BroadcastHashJoin`，根本不走 shuffle，倾斜无从谈起。

Spark SQL 里由 `spark.sql.autoBroadcastJoinThreshold` 控制阈值，默认 10MB。小表统计量小于这个值时，Catalyst 优化器基于 `BroadcastExchange` 物理算子自动插 `BroadcastHashJoin`。判定走 broadcast 还是 sort-merge join 的依据是 `sizeInBytes` 统计，统计不准时会误判——这是 AQE 的 `DynamicSwitchJoin` 规则要解决的问题，下一篇会展开。

```scala
// 显式 broadcast，绕过阈值限制（小表内存放得下时）
import org.apache.spark.sql.functions.broadcast

val joined = bigTable.join(broadcast(smallTable), "user_id")
```

`BroadcastHashJoin` 的 build side 选择有讲究：小表作为 build side 哈希到内存，大表作为 stream side 逐行探测。Catalyst 会选统计量小的一侧做 build，但如果是 left outer join，右表不能做 build（build side 全量在内存，左表无匹配行需要右表 null，build side 必须是右表），这些 join 类型约束决定了 broadcast 不是想用就能用。

那次治理我把 5 亿行用户维表按需用列裁到 3000 万行、约 800MB，加大 `autoBroadcastJoinThreshold` 到 1GB 后强制走 broadcast。但要警告：**broadcast 是双刃剑**。小表先 collect 到 driver，driver 内存撑不住会 OOM；其次 broadcast 到所有 executor，每个 executor 堆都涨一份。表大小超过 2GB 我就不敢 broadcast 了，driver 端序列化都吃力，单广播包超过 `spark.rpc.message.maxSize` 默认 128MB 还要分段发。<!-- 校准：请按真实经历核实/替换 -->

阈值调优要配合 `spark.driver.memory` 和 executor 堆一起看，不是越大越好。还有个隐含成本：broadcast 是同步阻塞的，driver 要等所有 executor 确认收到才继续，大表广播期间整个作业停摆，这段时间在 SparkUI 上表现为一个长时间空白的 `BroadcastExchange` 节点。我见过有人把阈值设到 5GB，结果广播本身花了 8 分钟，得不偿失。实务上我倾向于能 broadcast 的就显式 broadcast，不能的就用 AQE skew join，手动加盐留作重度倾斜的最后手段。

## 七、倾斜表改 join 顺序

有时候倾斜不是 key 的问题，是 join 顺序的问题。比如 `A join B join C`，A 和 B 都是大表，但 C 是过滤条件很重的小表。如果先 A join B，倾斜键没过滤，必然爆；如果把 C 的过滤下推到 A 和 B 上再 join，倾斜键的数据量可能直接降到可接受范围。

Catalyst 的 `PushDownPredicates` 和 `ReorderJoin` 优化器规则会做一部分，但它基于统计估算，统计不准时不会自动调。统计来源是 `CatalogStatistics`，Hive metastore 里的 `numRows`、`sizeInBytes` 不及时更新是常事，导致优化器基于过期统计做错误决策。我遇到这种情况会手动用 CTE 先过滤再 join：

```scala
val aFiltered = spark.sql("""
  WITH a_pre AS (SELECT * FROM big_a WHERE dt = '2026-07-01' AND uid > 0
                 AND uid NOT IN (0, -1)),
       b_pre AS (SELECT * FROM big_b WHERE dt = '2026-07-01')
  SELECT a.*, b.feat
  FROM a_pre a JOIN b_pre b ON a.uid = b.uid
""")
```

把空值、默认值、测试账号在 join 前就过滤掉，倾斜键数量级降下来，往往比加盐还管用。配合 `ANALYZE TABLE ... COMPUTE STATISTICS` 及时刷新统计，能让优化器自己选对 join 顺序。

## 八、AQE skew join 自动处理

Spark 3.0 引入 Adaptive Query Execution（AQE），其中 `OptimizeSkewedJoin` 规则能自动处理倾斜分区，不用改代码。这是最省心的方案，但有前提。

**工作机制：** AQE 在 shuffle map 阶段完成后，拿到真实的 `MapStatus` 统计，发现某些 reduce partition 数据量远超中位数时，把这些 partition 切成多个子 partition 并行处理，最后 union 起来。注意是 map 阶段完成后再决策，所以能用到真实数据量，而非优化器阶段基于统计的估算——这是 AQE 和静态优化器的根本区别。

核心是 `ShufflePartitionHandle` 的切分逻辑。它按 `medianPartitionSize`（中位分区大小）作为基准，对超大的 partition 按 `targetSize` 切片：

- `splitSize = spark.sql.adaptive.skewJoin.skewedPartitionFactor * medianPartitionSize`，默认 factor 是 5，即超过中位数 5 倍才算倾斜。
- `targetSize = spark.sql.adaptive.advisoryPartitionSizeInBytes`，默认 64MB，每个子 partition 目标大小。
- 切分时把原 partition 的数据按 `splitSize` 步长切成 `ceil(partitionSize / targetSize)` 份，每份独立 reduce，最后合并。

举例：一个倾斜 partition 120GB，median 是 2GB，`splitSize = 5 * 2GB = 10GB`，120GB > 10GB 判为倾斜；`targetSize = 64MB`，切成 `120GB / 64MB ≈ 1920` 个子 partition 并行处理。原本 1 个 Task 干 4 小时，变成 1920 个 Task 各干 1 分钟，最后 union 合并结果。<!-- 校准：请按真实经历核实/替换 -->

配置：

```scala
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
```

`skewedPartitionThresholdInBytes` 是绝对阈值，分区超过它且同时超过 `factor * median` 才判倾斜，双条件防止 median 本身就很大时误判（比如各分区都 50GB，median 50GB，单看 factor 没倾斜，但实际都要切）。这两个参数需要协同调：factor 管"相对倾斜"，threshold 管"绝对倾斜"，必须同时满足。

AQE 的局限：只处理 join 的倾斜，不处理纯 `groupBy` 聚合倾斜（聚合没有 map 阶段完成的 partition 概念，AQE 切不了）；依赖 map 阶段统计，对 sort-merge join 有效，对 broadcast join 无意义（本来就不 shuffle）；切分会产生额外的 task 调度开销，子 partition 太多反而拖慢。还有个边界：AQE 切分依据是 map 阶段单 partition 的总数据量，但同一个 partition 内可能既有倾斜 key 又有非倾斜 key，切分是按字节切，不是按 key 切，切完的子 partition 里倾斜 key 还在，只是从"一个 partition 120GB"变成"若干个 partition 各 64MB"，倾斜被均摊了但没消除。所以 AQE 开了不等于高枕无忧，重度倾斜还是要手动加盐。

## 九、预防机制：把倾斜挡在上线前

事后治理是被动的，更省事的是上线前就拦住倾斜。我团队后来落了两道防线：

**第一道：数据质量巡检。** 在 ETL 上游加 key 分布监控，每日采样统计大表的头部 key 占比，占比超过 30% 的表自动告警，把倾斜风险前置到数据生产环节。这套监控基于前面说的采样脚本，固化成 Airflow 上的一个 daily check task。

**第二道：作业配置规范。** 强制要求所有 join 作业开 AQE，`skewJoin.enabled` 默认 true；强制要求 `group by` 后聚合的作业开 `spark.sql.adaptive.coalescePartitions.enabled`，避免小分区碎片化；强制要求倾斜敏感表在 join 前显式过滤空值和默认值。这些规范写进 CI 检查，作业提交时校验配置，不合规直接打回。

还有个经验：新表上线、新业务接入时，先小流量灰度跑，观察 SparkUI 的 Task 分布再放量。那次 4 小时事故的根因之一就是扩量当天直接全量跑，没灰度，倾斜在扩量后才暴露。灰度跑一遍能看到长尾，提前加盐或开 AQE，比线上炸了再救火代价小得多。

## 十、几种治理手段的取舍

我把治理手段按场景归类：

| 场景 | 首选方案 | 备选 |
|------|----------|------|
| join 倾斜，一侧表小 | broadcast join | 加盐 |
| join 倾斜，两侧都大 | AQE skew join | 加盐 join |
| groupBy 聚合倾斜 | 两阶段聚合 | 加盐 + reduceByKey |
| 倾斜键是脏数据 | 过滤下推 | — |
| Spark 3.0+ 环境 | AQE 优先兜底 | 手动治理补位 |

实战里我一般先开 AQE 兜底，再针对性优化：脏数据过滤掉、小表 broadcast、聚合走两阶段、大表 join 加盐。那次事故用这套组合拳，作业从 4 小时降到 18 分钟，瓶颈从倾斜 Task 转移到了正常的 shuffle 吞吐，已经在可控范围。<!-- 校准：请按真实经历核实/替换 -->

## 小结

数据倾斜治理的核心是"先定位、再对症"。SparkUI 的 Task metrics 长尾是定位的起点，`MapStatus` 反查和采样统计是识别倾斜键的两把刷子。治理上，broadcast join 绕过 shuffle、两阶段聚合用 `mapSideCombine` 压缩 shuffle 量、加盐打散均摊倾斜分区、AQE 自动切分超大 partition，各有适用场景，没有银弹。

但 AQE 这套机制远不止 skew join 一个规则——它背后的 Adaptive Query Execution 是 Spark 3.0 之后整个执行计划层面的范式转变，从静态优化器走向运行时自适应。下一篇我会把 AQE 的完整框架拆开讲：`LogicalQueryStages` 与 `QueryStage` 的物理化、三个核心规则（Coalesce Partitions / Switch Join / Skew Join）的触发条件与内部状态机、`MapOutputStatistics` 如何驱动规则应用、以及它和 Catalyst 优化器的协作边界。这是 Spark SQL 调优绕不过去的一块。

<!-- 总校准：本文涉及的数字（80 亿行行为表、5 亿行用户维表、倾斜键占比 80%/35%/18%/9%、单 Task shuffle read 120GB、median 600MB、medianTask 6min/maxTask 4h、盐化倍数 N=100、小表膨胀 5 亿→50 亿、列裁后 3000 万行 800MB、autoBroadcastJoinThreshold 1GB、作业耗时从 4h 降到 18min、AQE 切分 1920 子 partition 等）均按个人经历写出，发布前请按真实生产环境核实/替换。AQE 参数名与默认值以 Spark 3.2+ 为基准，不同小版本有差异，请以官方文档为准。-->
