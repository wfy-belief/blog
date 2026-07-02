---
title: RDD 抽象与 lineage 容错：从五大特性到迭代场景的血缘治理
abbrlink: spark-rdd-lineage
date: 2026-07-02 15:30:00
tags:
  - Spark
  - RDD
categories:
  - 技术
description: 从 RDD 五大特性到窄宽依赖、lineage 重算、persist/cache/checkpoint 取舍与迭代血缘治理
ai_text: "本文以第一人称复盘 Spark RDD 抽象：从五大特性、窄宽依赖的 stage 划分、分区与算子并行度，到 lineage 重算容错，再落到 persist/cache/checkpoint 三种固化手段取舍，最后剖析迭代场景血缘链过长的 OOM 与调度栈风险及周期性 checkpoint 治理。"
---

# RDD 抽象与 lineage 容错：从五大特性到迭代场景的血缘治理

## 一、引言：Spark 蹚过这么多坑，最终还是绕回 RDD

这几年我做的离线链路基本都跑在 Spark SQL 和 DataFrame 上，写原生 RDD 的机会越来越少。但每次线上出事——一个跑了三个小时的作业因为某个 Executor OOM 挂掉、整条链路从头重算；一个迭代算法跑到第 40 轮突然 `StackOverflowError`；一个 checkpoint 写盘把 NameNode RPC 打爆——最后都得回到 RDD 这一层来解释。

原因很朴素：**DataFrame / Dataset 的物理计划再聪明，落到执行层仍然是 RDD 的 transform 和 action**。Catalyst 优化掉的是冗余算子，Tungsten 加速的是单算子执行，但容错模型、shuffle 边界、缓存与重算的取舍，全都还压在 RDD 这套抽象上。不理解 RDD，遇到 Stage 重试、缓存丢失、lineage 风暴这类问题就只能瞎试参数。这篇文章把我过去几年在 Spark 上踩过的与 RDD 直接相关的坑复盘一遍，重点放在五大特性、依赖与 stage、lineage 重算、persist/checkpoint 的取舍，以及迭代场景里血缘过长这个容易被忽视的杀手。

## 二、五大特性：不只是"弹性分布式数据集"这句口号

RDD 的论文定义只有一句——只读、分区、可容错的抽象。但真正决定它行为的，是源码里那五条特性。我每次面试都会问，能答全的人不多。

| 特性 | 含义 | 由谁决定 |
|------|------|---------|
| partitions | 一组分区，是并行度的物理单元 | `getPartitions`，由 InputFormat / shuffle 决定 |
| compute | 每个分区的计算函数 | `compute(split, context)`，无需落盘即可重算 |
| dependencies | 对父 RDD 的依赖列表 | `getDependencies`，窄/宽依赖的分水岭 |
| partitioner | KV 类型 RDD 的分区器（可选） | HashPartitioner / RangePartitioner |
| preferredLocations | 每个分区的偏好位置（可选） | 数据本地性 hints |

这五条里，**dependencies 是容错的根，partitioner 是 shuffle 的根，compute 是重算的根**。其余两条服务于并行度和本地性。理解 RDD 不要从"弹性"这个营销词入手，要从这三条入手。

一个关键认知：**RDD 本身不存数据**。它只存"怎么算出来"的描述。真正有数据的是被 `persist` 或 `cache` 物化过的分区，以及 source（HDFS 文件等）。这是 lineage 容错能够成立的前提——只要描述在，数据随时可重算。

这里要多说一句 `preferredLocations`。它返回一个分区偏好跑在哪些 Executor 上（HDFS 块所在节点、`RDDCheckpointLocation` 等）。TaskScheduler 调度时会按 `PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY` 的层级降级等待，超时就跨层级。我曾排过一个作业 map 阶段比预期慢 3 倍，最后定位是上游 HDFS 文件被一次 `hdfs balancer` 重新分布后块位置变了，而 RDD 的 `preferredLocations` 还指向旧节点，导致大量 task 退化成 `ANY` 跨网络读。理解本地性这一层，对排"明明没倾斜但就是慢"的作业非常关键。

## 三、窄依赖 vs 宽依赖：stage 划分的唯一依据

DAGScheduler 切分 stage，依据只有一条：**遇到宽依赖就断开**。窄依赖可以 pipeline 进同一个 stage，宽依赖必须落 shuffle boundary。

```
   窄依赖 (map / filter / union)         宽依赖 (groupByKey / reduceByKey / join)

   父 RDD            子 RDD              父 RDD            子 RDD
  +------+         +------+            +------+---------+------+
  | P0   |--map--->| P0'  |            | P0   |  \  /   | P0'  |
  +------+         +------+            +------+   X     +------+
  | P1   |--map--->| P1'  |            | P1   |  /  \   | P1'  |
  +------+         +------+            +------+         +------+
  | P2   |--map--->| P2'  |            | P2   |----\    | P2'  |
  +------+         +------+            +------+         +------+
                                          shuffle boundary
                                       (按 key 重分布，必须落盘/网络洗牌)
   父子分区一对一/一对多                父分区被多个子分区消费
   失败只需重算对应父分区                失败需重算所有父分区
```

源码里这层抽象是 `Dependency[T]`，分两支：

- `NarrowDependency`（窄依赖）：抽象类，核心方法是 `getParents(partitionId): Seq[Int]`，返回这个子分区依赖哪些父分区。它的两个实现是 `OneToOneDependency`（map / filter，父分区号等于子分区号）和 `RangeDependency`（`union` 算子用，一段连续父分区映射到一段子分区）。窄依赖的好处是父分区数据可在本地管道消费，不落 shuffle。
- `ShuffleDependency`（宽依赖）：携带 `partitioner`、`serializer`、`mapSideCombine`、`shuffleId`、`keyOrdering` 等信息。一旦出现，DAGScheduler 就以此为界生成 `ShuffleMapStage`，子 stage 作为 `ResultStage` 或下一个 `ShuffleMapStage` 的输入。这里有个性能细节：`mapSideCombine` 默认对 `reduceByKey` 开、对 `groupByKey` 关。开了 map 端预聚合能把 map 输出字节数压一个数量级，但前提是聚合函数满足结合律。把 `groupByKey` 强行加 combiner 是没用的——它没有可结合的聚合函数，这也是为什么 `groupByKey` 永远是性能黑洞，能用 `reduceByKey` / `aggregateByKey` 就别用 `groupByKey`。

这里有个常被忽略的细节：**`coalesce` 默认是窄依赖，但 `shuffle=true` 时变宽**。我曾在压缩分区数时忘了加 `shuffle=true`，结果上游 2000 个分区里的数据被强行塞进下游 200 个分区，绝大多数分区数据分布严重不均，长尾 task 一堆。正确写法是 `repartition`（等价 `coalesce(n, shuffle=true)`），让数据真的走一次 shuffle 重分布。

还有一类容易被误判的——**`join` 不一定是宽依赖**。如果两张表已经同 partitioner 且同分区数，Spark 会走 `CoGroupedRDD` 的窄依赖路径（`PartitionwiseSampledRDD` 之类），不走 shuffle。这是 BroadcastHashJoin 之外另一条"免 shuffle join"的路，但前提是分区对齐，生产里得显式 `repartition` 好。

## 四、分区与算子：并行度的物理表达

分区数直接决定 task 数，task 数过多则调度开销大、JVM 启动占比高；过少则并行度低、长尾严重。我的经验阈值：

- `spark.default.parallelism`：给非 SQL RDD 作业用，一般设为 executor core 总数的 2~3 倍 <!-- 校准：请按真实经历核实/替换 -->。
- `spark.sql.shuffle.partitions`：给 DataFrame/SQL 用，默认 200 在任何正经规模都太小，我们生产设 2000 <!-- 校准：请按真实经历核实/替换 -->，再配合 AQE 自适应压缩。

算子分两类：`transform` 惰性、`action` 触发。下面是我常用的一个 RDD 算子链骨架，把过滤、映射、聚合、排序串起来：

```scala
val topUsers = sc.textFile("hdfs:///logs/2026-07-01/*")
  .filter(_.contains("ERROR"))
  .map(line => (line.split("\t")(1), 1))
  .reduceByKey(_ + _, 2000)        // 显式给分区数，避免默认值失控
  .sortByKey(ascending = false)    // 触发一次 RangePartitioner 的采样

topUsers.take(10).foreach(println)
```

这里 `sortByKey` 背后是 `OrderedRDDFunctions` 隐式增强 + `RangePartitioner`：它先对父 RDD 分区做水塘采样，估出分位边界，再按边界把 key 分到各分区，使每个分区内部有序、分区之间也有序。这就是所谓的 **OrderedRDD 语义**——`sortByKey` 之后，RDD 同时具备"分区有序"和"分区内有序"，后续若再做 `repartitionAndSortWithinPartitions`，可以把一次 shuffle 同时完成重分区和排序，省掉一轮额外 shuffle。这是 RDD 层比朴素 DataFrame 写法更精细的地方，点查或范围扫场景非常受用。

再补两个算子选择的实战心得。第一，`mapPartitions` 比 `map` 更适合"每分区建一次昂贵资源"的场景，比如每分区开一个数据库连接、加载一个模型——`map` 会在每条记录上重复建连，而 `mapPartitions` 在分区粒度复用。但代价是 `mapPartitions` 一次性把整个分区的迭代器交给你，如果你在内部把它 `toList` 全量物化，分区内存就会爆，正确做法是惰性 `Iterator` 转换。第二，`coalesce(n, shuffle=false)` 用来"压缩"分区数，但它是把多个相邻小分区合并成一个大分区，不会触发 shuffle，适合 map 后分区过碎的场景；如果分区分布不均，`shuffle=false` 的 coalesce 只会加剧倾斜，这时必须 `repartition`。

## 五、lineage 重算：容错的灵魂，也是双刃剑

RDD 不存数据，只存血缘。某个分区丢了，DAGScheduler 沿 dependencies 反向回溯到最近的物化点（cache 的 / checkpoint 的 / source），正向重算这一段。这就是 lineage 重算。

```
   source(HDFS) -> map -> filter -> persist(MEM_AND_DISK) -> join -> reduce
                                                          ^
                                                     task 在此失败
                                  <-------- 重算只需从此点正向 --------+
                                  (被 persist 物化的分区不重算)
```

重算路径的长度 = 失败分区到最近物化点的算子数。物化点越近，重算越便宜；没有物化点，就要回到 source 全算。我吃过一次大亏：一个 1.2 TB 的中间结果没 persist，下游 join 阶段某个 Executor 磁盘故障触发 Stage 重试，结果整条链路从读 HDFS 开始重算，多花了 47 分钟 <!-- 校准：请按真实经历核实/替换 -->。从那以后我定了规矩：**任何会被下游多次使用的中间 RDD，必须 persist**，且优先 `MEMORY_AND_DISK_SER`。

还有一类隐蔽的重算陷阱：**`cache` 后没触发 action 就直接用**。`persist` / `cache` 是惰性的，只标记不物化，必须有一个 action 强制计算该 RDD 才会把分区真正缓存进 BlockManager。我见过不少代码写成 `val rdd = src.map(...).cache(); rdd.map(...).count()`，看似缓存了，实际上第一个 action 触发的是下游 RDD 的计算，缓存的只是计算过程中顺手存下来的分区——如果下游是窄依赖链到 cache 点，这没问题；但如果中间还隔着宽依赖，cache 点的分区可能根本没被独立物化。最稳的写法是显式 `rdd.count()` 或 `rdd.first()` 把它"暖"起来，再交给后续算子。判断是否真缓存住，看 Spark UI 的 Storage 标签页，`Fraction Cached` 必须是 100%。

lineage 的危险面在迭代场景被放大，下面专门讲。

## 六、persist / cache / checkpoint：三种固化手段的取舍

这三者经常被混为一谈，其实定位完全不同。

**`cache()` = `persist(StorageLevel.MEMORY_ONLY)`**。只放内存，丢了不落盘，丢了就靠 lineage 重算。适合数据量小、重算便宜的 RDD。

**`persist(level)`**：按指定级别固化，但**不切断血缘**。即使数据在内存/磁盘里，一旦 Executor 挂了，对应 BlockManager 失联，缓存的分区没了，DAGScheduler 仍会沿 lineage 重算。也就是说，persist 是"加速"，不是"容错"。

**`checkpoint()`**：把 RDD 各分区写到 `setCheckpointDir` 指定的可靠目录（必须 HDFS / S3 等分布式 FS，不能本地），**写完后切断血缘**。后续该 RDD 的 `dependencies` 返回 `Nil`，`compute` 直接从 checkpoint 文件读，由内部的 `ReliableRDDCheckpointData` + `CheckpointRDD` 接管。这是真正的容错手段。

StorageLevel 取舍表：

| 级别 | 内存 | 磁盘 | 序列化 | 适用 |
|------|------|------|--------|------|
| MEMORY_ONLY | 是 | 否 | 否 | 数据量小、频繁用 |
| MEMORY_ONLY_SER | 是 | 否 | 是 | 内存紧、对象大 |
| MEMORY_AND_DISK | 是 | 是 | 否 | 略超内存 |
| MEMORY_AND_DISK_SER | 是 | 是 | 是 | 生产默认首选 |
| DISK_ONLY | 否 | 是 | 是 | 数据极大、重算极贵 |
| OFF_HEAP | 堆外 | - | 是 | GC 压力大 |

我的默认选型几乎都是 `MEMORY_AND_DISK_SER`：序列化省内存（对象能小 3~5 倍 <!-- 校准：请按真实经历核实/替换 -->），装不下自动落盘而不是直接丢，代价是反序列化 CPU 开销，但对绝大多数 ETL 不是瓶颈。序列化器我用 `org.apache.spark.serializer.KryoSerializer` 并 `kryo.register` 注册业务类，比默认 Java 序列化快一个量级且体积更小。

`OFF_HEAP` 是另一个值得单说的级别：数据放到堆外的 `Unsafe` 内存里，不进 JVM 堆、不参与 GC。在堆上有几十 GB 大对象、GC 停顿明显的作业里效果显著，但代价是 off-heap 内存受 `spark.memory.offHeapSize` 硬上限约束，超出会直接 OOM 而不是缓慢退化，调试也更难。我对它的态度是"GC 真成了瓶颈再上，别默认开"。

checkpoint 的标准用法：

```scala
sc.setCheckpointDir("hdfs:///spark/ckpt/lineage")  // 必须是分布式 FS

val features = rawEvents
  .map(parseEvent)
  .filter(_.isValid)
  .map(_.toFeatureVector)
  .persist(StorageLevel.MEMORY_AND_DISK_SER)

features.checkpoint()      // 只标记，不触发
features.count()           // 这次 action 会先算出 features，再额外起一个 job 写 checkpoint
```

`setCheckpointDir` 的机制要交代清楚：它把路径广播给所有 Executor，`ReliableRDDCheckpointData` 为每个分区写一个 `rdd-{id}-{partition}.bin` 文件。checkpoint 是**惰性**的——`checkpoint()` 只登记，真正写盘发生在下一个 action 触发该 RDD 计算之后，而且会**额外提交一个 job**专门写 checkpoint。这意味着 checkpoint 一次的成本 = 一次完整计算 + 一次写 HDFS。我测过 800 GB 的中间 RDD 写 checkpoint 大概 12 分钟 <!-- 校准：请按真实经历核实/替换 -->，期间 NameNode RPC QPS 会明显上涨，所以别在生产高峰对大 RDD 滥用。

checkpoint 与 persist 的配合有个推荐顺序：**先 persist 再 checkpoint**。如果直接 checkpoint 一个没缓存的 RDD，写 checkpoint 的那个额外 job 会从头把 RDD 算一遍；而如果先 persist 把它物化在内存，checkpoint job 直接读缓存写盘，省掉重复计算。写完 checkpoint 后再 `unpersist` 释放缓存，让后续重算直接走 checkpoint 文件。这套"persist → count → checkpoint → count → unpersist"是迭代场景的标配写法。

## 七、迭代计算：lineage 过长的 OOM 与调度栈风险

这是我最想讲的一节。GraphX 的 PageRank、MLlib 的迭代算法、自定义的梯度下降，都会形成"同一个 RDD 在循环里被反复派生"的模式。

朴素写法的陷阱：

```scala
var g = loadGraph().persist(StorageLevel.MEMORY_AND_DISK_SER)
g.count()
for (i <- 1 to 100) {
  g = g.pregel(msg)(vprog, sendMsg, mergeMsg)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  g.count()
}
```

每轮 `g` 都是一个新 RDD，父依赖指向上一轮的 `g`。100 轮后 lineage 链长 100 层。这会带来三个真实风险：

1. **重算爆炸**：某个 Executor 挂了，缓存的分区丢失，DAGScheduler 要沿 100 层血缘回溯。如果中间几轮没缓存住（`MEMORY_AND_DISK_SER` 装不下被 evict 到磁盘又恰巧磁盘满），回溯会一直退到 source，重算等价于把整个 100 轮迭代重跑。
2. **调度栈/递归深度**：`DAGScheduler.getMissingParentStages` 与 `RDD.dependencies` 是递归遍历的，lineage 极深时偶发 `StackOverflowError`。社区有相关 issue，生产里我在第 60+ 轮踩过 <!-- 校准：请按真实经历核实/替换 -->。
3. **内存引用堆积**：旧 `g` 如果没 `unpersist`，每轮的 BlockManager 都持有上轮缓存，迭代越多内存占用线性增长，最终 OOM。

治理套路是**周期性 checkpoint 切血缘 + 主动 unpersist 释放旧分区**：

```scala
sc.setCheckpointDir("hdfs:///spark/ckpt/iter")
var g = loadGraph().persist(StorageLevel.MEMORY_AND_DISK_SER)
g.count()

val ckptEvery = 10  // 校准：按血缘深度与重算成本权衡
for (i <- 1 to 100) {
  val prev = g
  g = prev.pregel(msg)(vprog, sendMsg, mergeMsg)
       .persist(StorageLevel.MEMORY_AND_DISK_SER)
  g.count()
  if (i % ckptEvery == 0) {
    g.checkpoint()
    g.count()              // 真正写盘并切断血缘
  }
  prev.unpersist(blocking = false)  // 释放上一轮缓存
}
```

`ckptEvery` 的取舍：太小则 checkpoint 写盘开销主导；太大则 lineage 仍长、重算风险高。我的经验值 8~15 轮一次 <!-- 校准：请按真实经历核实/替换 -->，看单轮算子成本和单轮数据量。配合 `spark.cleaner.referenceTracking=true`（默认开）能让被切断血缘的旧 RDD 尽快被 GC。

这里有一个真实事故复盘。我们一个图计算作业跑 80 轮迭代，最初没加 checkpoint，第 63 轮时一台 Executor 因宿主机内存压力被 YARN kill。本来期望 Spark 重试那个 task 就行，结果因为 lineage 没断，回溯要回到第 1 轮的 source，单 task 重算等价于重跑 60+ 轮，直接把作业拖到超时失败。加了周期性 checkpoint 后，最坏情况只回退到上一个 checkpoint 点（10 轮以内），单 task 重算控制在分钟级。这个对比让我彻底理解了"checkpoint 不是可选优化，是长血缘作业的必需品"。

还有一个反直觉的点：**checkpoint 写完不等于血缘立刻断**。血缘是在该 RDD 下一次被计算时才"看到"checkpoint 已完成并切换到 `CheckpointRDD` 读取。所以 checkpoint 之后必须紧跟一个 action，让框架真正走一次重算路径，这之后 `isCheckpointed` 才稳定返回 true、`getCheckpointFile` 才有值。生产里我会在 checkpoint 后显式跑一次 `count()` 验证，再继续下游。

## 八、小结

把这篇收个口：

1. RDD 的五条特性里，dependencies 决定容错边界、partitioner 决定 shuffle 边界、compute 决定重算路径。理解这三条，比背"弹性分布式数据集"有意义得多。
2. 窄依赖 pipeline、宽依赖断 stage，这是 DAGScheduler 唯一的切分规则。`OneToOneDependency` / `RangeDependency` 是窄依赖的两种实现，`ShuffleDependency` 携带 partitioner 触发 shuffle。`join` 在分区对齐时可以免 shuffle。
3. lineage 重算是 Spark 容错的灵魂，但代价是重算路径上每个算子都要重新执行。物化点（persist / checkpoint / source）越近，重算越便宜。
4. `persist` 加速不切断血缘、`cache` 是其特例、`checkpoint` 切断血缘写可靠存储。生产默认 `MEMORY_AND_DISK_SER`，迭代场景周期性 checkpoint + 主动 unpersist 是治血缘过长的标准套路。
5. 迭代场景的三个杀手：重算爆炸、调度栈溢出、内存引用堆积，根因都是血缘链太长且无切断点。

下一篇我会接着讲 Spark 的 Shuffle 机制——从 sort-based shuffle 的写入/索引结构，到 AQE 自适应分区、shuffle 服务与 External Shuffle Service 的取舍，把"宽依赖两侧到底发生了什么"这层彻底拆开。那是理解 Spark 性能与稳定性另一块更大的拼图。

<!-- 校准：请按真实经历核实/替换 -->
> 全文涉及的所有具体数字（分区数、内存倍数、重算耗时、checkpoint 写盘耗时、迭代轮数、ckptEvery 阈值等）均为示例性占位，请按你自己的集群规模、数据分布与真实经历核实替换。博客代表个人可追溯履历，数字必须落到自己身上。
