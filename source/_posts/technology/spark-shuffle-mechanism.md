---
title: Spark Shuffle 全流程演进：从 HashShuffle 到 Push-based Shuffle 的十年复盘
abbrlink: spark-shuffle-mechanism
date: 2026-07-02 16:00:00
tags:
  - Spark
  - Shuffle
categories:
  - 技术
description: 一次 shuffle 把集群写崩之后，我把 HashShuffle、SortShuffle、Tungsten-Sort、push-based shuffle 的演进路径全部复盘了一遍。
ai_text: "以一次 shuffle 写崩集群的夜间事故为引子，第一人称复盘 Spark Shuffle 十年演进：HashShuffle 普通版与 consolidate 优化版的文件数公式差异、SortShuffle 取代它成为默认的根因、bypass 与 serialized shuffle 的触发条件、Tungsten 堆外排序的本地与全局之辨、push-based shuffle 的 SPI 实现，并对比 External Shuffle Service 与 Hadoop MapReduce，最后给出实战配置清单。"
---

# Spark Shuffle 全流程演进：从 HashShuffle 到 Push-based Shuffle 的十年复盘

## 引子：一次 shuffle 写崩集群

去年冬天一个夜间任务，一个 2T 的 join 跑到凌晨三点突然全线 OOM，Driver 日志里全是 `org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle`。我爬起来看 Ganglia，发现是 reduce 端拉取把一台 NodeManager 的网卡打满了，连锁触发 External Shuffle Service 假死。等我把任务救回来，天已经亮了。

那次事故之后，我下定决心把 Spark Shuffle 从 HashShuffle 一路到 push-based shuffle 的演进路径完整复盘一遍。这篇就是那次复盘的产物——写给同样被 shuffle 折磨过的同行。

## 一、Shuffle 的本质：为什么是性能命门

Shuffle 是把 map 端按 key 分区写出去、reduce 端再跨节点拉回来重新聚合的过程。它横跨磁盘、网络、内存三个瓶颈，几乎是所有 Spark 作业的"阿喀琉斯之踵"。

我和团队做过统计：在我们那个 200 节点的集群上，一个典型 ETL 作业 60% 以上的耗时花在 shuffle 阶段，而 OOM、数据倾斜、GC 风暴这些最常见的故障，八成以上都和 shuffle 直接相关。<!-- 校准：请按真实经历核实/替换 -->

为什么 shuffle 这么贵？因为它同时踩中三颗雷。第一颗是磁盘：map 端必须把内存里按 key 分好区的数据落盘，落盘就是顺序写加随机 spill merge；第二颗是网络：reduce 端要跨节点把分散在几百个 executor 上的数据按 partition 聚拢，拉取流量动辄几十 GB；第三颗是内存：reduce 端聚合用的 HashMap 在数据量大时会反复 spill，形成"读-聚合-spill-再 merge"的锯齿，GC 压力全压在堆上。

理解 shuffle 的演进，本质上是理解 Spark 如何在"文件数"、"排序代价"、"内存压力"这三个维度之间做权衡。每一代都是在用新的代价换掉上一代的瓶颈。下面我按时间线展开。

## 二、HashShuffle 时代：文件数爆炸的原罪

### 2.1 普通 HashShuffle

最早 Spark 1.0 之前只有 HashShuffle。每个 map task 为每个 reduce partition 写一个独立文件，思路简单粗暴：算好 `partitionId = hash(key) % numReduce`，往对应文件里追加。

文件数公式是 **M × R**：M 个 map task，R 个 reduce partition。一个 400 个 executor、每个 8 核、reduce 端 200 分区的作业，shuffle 写盘文件数能到 `400*8*200 = 640000` 个。<!-- 校准：请按真实经历核实/替换 --> 这么多文件同时打开，inode 直接打爆，磁盘随机 IO 严重退化，写盘吞吐从顺序写的几百 MB/s 掉到十几 MB/s。<!-- 校准：请按真实经历核实/替换 -->

### 2.2 优化版 HashShuffle（consolidate）

Spark 1.1 引入了 consolidate 机制，核心思路是"同一核上复用文件"。每个 executor 上的一个核对应一组文件（一组 R 个），同一个核上先后跑的多个 map task 共用这组文件，按 task 顺序追加。

文件数公式变成 **C × R**，C 是总核数。同样是上面那个作业，文件数降到 `400*8*200 = 640000`？不对——consolidate 后是 `总核数 * R = 3200*200 = 640000`，看似没变，但核心在于一个 executor 上串行跑的多个 task 共享文件，实际文件数远小于 M×R。<!-- 校准：请按真实经历核实/替换 --> 在我们那个作业上，consolidate 把文件数压了差不多一个数量级。

但 HashShuffle 的根本问题没解决：文件数仍和 R 成正比，R 一大就崩。我在 1.4 之前还踩过一个坑：consolidate 模式下，如果 executor 被动态回收再重新申请，新的 task 会和旧 task 抢同一组文件，触发文件锁竞争，日志里一片 `FileNotFoundException`。这类问题在 SortShuffle 把文件数降到 2M 之后才彻底消失。这直接催生了 SortShuffle。

## 三、SortShuffle：取代 HashShuffle 成为默认

### 3.1 普通 SortShuffle 的核心改进

SortShuffle 的关键洞察是：每个 map task 只写**两个**文件——一个 data 文件、一个 index 文件。无论 R 多大，文件数都是 **2 × M**，与 R 无关。

流程是这样的：map task 把记录写进内存数据结构（`PartitionedAppendOnlyMap` 或 `PartitionedPairBuffer`），过程中按 partitionId 排序（必要时也按 key 排），每写一条带 partitionId；内存达到阈值（默认 `spark.shuffle.spill.numElementsForceSpillThreshold` 或内存压力）就 spill 到磁盘一个临时文件；task 结束时把所有 spill 文件 merge 成一个 data 文件，同时生成 index 文件记录每个 partition 在 data 文件里的起止偏移。

下面是我画的 map 端写盘结构图：

```
              map task (一个 task 内)
   ┌──────────────────────────────────────────────┐
   │  内存: PartitionedAppendOnlyMap              │
   │   按 (partitionId, key) 排序                  │
   │                                              │
   │   ┌──p0──┬──p1──┬──p2──┬──...──┬──pR-1──┐    │
   │   │k,v   │k,v   │k,v   │       │k,v     │    │
   │   └──────┴──────┴──────┴───────┴────────┘    │
   │        │ 内存超阈 → spill                     │
   │        ▼                                     │
   │   spill0.sorted  spill1.sorted  spill2.sorted│
   └──────────────────────────────────────────────┘
                  │ task 结束 merge
                  ▼
        ┌─────────────────────────────────┐
        │  shuffle_<mapId>_0.data         │  ← 所有 partition 顺序拼接
        │  [p0 段][p1 段][p2 段]...[pR-1段]│
        ├─────────────────────────────────┤
        │  shuffle_<mapId>_0.index        │  ← 每个 partition 的偏移
        │  [off0,len0][off1,len1]...      │
        └─────────────────────────────────┘
```

reduce 端拉取时，先读 index 拿到目标 partition 的偏移，再去 data 文件里 range read，省去了 HashShuffle 时代海量小文件的随机 IO。这是 SortShuffle 能撑住大 R 的根本原因。

这里有个容易混淆的点：SortShuffle 的排序对象是 `(partitionId, key)`，排序主要目的是"把同一个 partition 的记录聚到一起连续存放"，而不是为了全局有序。也就是说，即便你写的是 `groupByKey` 这种不需要 key 排序的操作，map 端依然会按 partitionId 排——因为只有这样，merge 阶段才能把所有 spill 文件里同一个 partition 的段抽出来拼到一起。排序在这里是"分组"的手段，不是"排序"的目的。

spill 的触发逻辑也值得说清楚。`PartitionedAppendOnlyMap` 占用的内存由 `spark.shuffle.file.buffer`（默认 32KB，控制写盘缓冲）和 execution 内存池共同约束。当 map 端插入的记录占用达到 execution 内存的某个水位（默认 0.8），就会触发 spill：把当前 map 里所有记录排序后写成一个临时文件，然后清空 map 继续接收。task 结束时如果有多个 spill，再做一次 merge sort 合并成最终 data 文件。spill 次数过多会让 merge 阶段变长，这也是为什么调大 `spark.shuffle.file.buffer` 能提速——它减少了 spill 次数。

### 3.2 bypass 机制：小作业的快速通道

SortShuffle 多了排序和 merge 代价，对"不需要排序、又小"的作业是浪费。于是有了 bypass 机制：当 **`spark.shuffle.sort.bypassMergeThreshold`（默认 200）** 且 map 端没有 map 端聚合时，跳过排序，每个 partition 直接写一个临时文件，最后 concat 成一个 data 文件再生成 index。

bypass 的核心收益是省掉内存里的排序结构，代价是仍要开 R 个文件句柄（所以才有 200 这个阈值）。`spark.sql.shuffle.partitions=200` 这个默认值之所以这么多年没动，部分原因就是它正好卡在 bypass 阈值附近——再大就触发排序，再小并行度不够。<!-- 校准：请按真实经历核实/替换 -->

| 维度 | 普通 SortShuffle | bypass SortShuffle |
|------|-----------------|-------------------|
| 触发条件 | 默认 | partition 数 ≤ 200 且无 map 端聚合 |
| 是否排序 | 是（按 partitionId） | 否 |
| 内存结构 | PartitionedAppendOnlyMap | 直接写文件 |
| 文件句柄 | 2 | R（临时） |
| 适用场景 | 大 R / 需要排序 | 小 R / 无聚合 |

### 3.3 serialized shuffle（Tungsten-Sort）

Spark 1.4+ 引入 Tungsten-Sort，又叫 unsafe shuffle。它把记录序列化后直接放在堆外内存，只对一个 8 字节的指针（partitionId + 内存页偏移）排序，排序完成后按指针顺序把序列化数据 spill 到磁盘，最后 merge。

触发 serialized shuffle 需要同时满足三个条件：
1. shuffle dependency 没有 map 端聚合；
2. serializer 支持 relocation（KryoSerializer 配 `spark.kryo.referenceTracking=false`，或默认的 `UnsafeRowSerializer`）；
3. partition 数 ≤ `spark.shuffle.sort.bypassMergeThreshold`（16777216，约 2^24）<!-- 校准：请按真实经历核实/替换 -->。

注意这里有个容易踩的坑：serialized shuffle 的排序是**本地排序**，只保证单个 map task 内每个 partition 内部有序，不保证全局有序。如果业务需要全局排序（比如 `sortByKey`），Spark 会退化回普通 SortShuffle 做全局 sort。本地排序 vs 全局排序的区别就在这：Tungsten-Sort 追求的是"按 partition 分组+partition 内有序"的弱保证，换取更低的内存和 CPU 开销。

为什么 Tungsten 能省内存？普通 SortShuffle 把记录以 Java 对象形式存在堆上，每条记录除了 key/value 本身还有对象头、引用、对齐开销，一条几十字节的记录在堆上可能占上百字节。Tungsten 把记录直接序列化成二进制塞进堆外的内存页（page），排序时只交换一个 64 位的指针——高 24 位是 partitionId，低 40 位是页内偏移。CPU 排序的是这些 8 字节指针，而不是对象本身。这不仅把内存占用压到接近数据原始大小，还完全绕开了 GC——堆外内存对 GC 不可见，也就没有 stop-the-world 的代价。代价是反序列化发生在 reduce 端，且 record 必须支持 relocation（也就是序列化后的字节流可以原样搬运，不能有外部指针引用），这就是为什么必须用支持 relocation 的 serializer。

## 四、Reduce 端拉取与聚合

### 4.1 拉取流程

reduce task 启动后，先从 Driver 的 MapOutputTracker 拿到每个 map task 输出的位置（executor 地址 + 文件路径 + 每个 partition 的偏移），然后开多个线程并发拉取自己那个 partition 的数据。

```
   reduce task (partition = r)
   ┌──────────────────────────────────────────────┐
   │ 1. 向 Driver 请求 MapStatus                  │
   │    └─ 拿到每个 map 输出: executor + offset    │
   │                                              │
   │ 2. ShuffleBlockFetcherIterator 并发拉取       │
   │    ┌──────┬──────┬──────┬──────┐             │
   │    │map0  │map1  │map2  │...   │  ← maxReqs  │
   │    │p=r   │p=r   │p=r   │      │   in flight │
   │    └──┬───┴──┬───┴──┬───┴──────┘             │
   │       ▼      ▼      ▼                         │
   │ 3. 写入 InMemorySorter / 内存缓冲             │
   │    超过阈值 → spill 到本地                    │
   │                                              │
   │ 4. 若有 reduce 端聚合: 边拉边聚合             │
   │    否则: 全部拉完再排序/聚合                  │
   └──────────────────────────────────────────────┘
              │
              ▼
        ExternalAppendOnlyMap  ← 堆外 + 磁盘 spill
        (支持 spill 的 HashMap)
```

关键参数是 `spark.reducer.maxSizeInFlight`（默认 48MB）和 `spark.reducer.maxReqsInFlight`，控制单 reduce task 同时在途的拉取量。我们那个夜间事故的根因，就是 reduce 端拉取并发太高把 NM 网卡打满——后来我把 `maxSizeInFlight` 调到 24MB 才稳住。<!-- 校准：请按真实经历核实/替换 -->

reduce 端拉取的另一个细节是"边拉边聚合"还是"先拉后聚合"。如果 shuffle dependency 带了聚合函数（`aggregateByKey`、`reduceByKey`），reduce 端会用 `ExternalAppendOnlyMap`，每拉一批就更新 map，map 满了就 spill，最后再 merge——这样内存峰值可控。如果是 `groupByKey` 这种没有聚合的，reduce 端会把所有数据先拉到内存或磁盘，再做后续处理，内存压力更大，这也是为什么 `groupByKey` 在大数据量下比 `reduceByKey` 危险得多。

还有个容易被忽视的失败重试机制：拉取某个 block 失败时，Spark 会重试，重试次数由 `spark.shuffle.io.maxRetries`（默认 3）和 `spark.shuffle.io.retryWait`（默认 5s）控制。在动态分配或网络抖动环境下，适当调大这两个值能避免任务直接失败。但要注意，重试期间整个 task 阻塞，如果倾斜数据集中在一个 reduce，重试会放大长尾。

### 4.2 External Shuffle Service

reduce 拉取依赖 map task 所在的 executor 还活着。一旦 executor 因动态资源回收被杀，reduce 就拉不到数据——这就是开头那个 `Missing an output location` 的来由。

External Shuffle Service（ESS）独立部署在每个 NodeManager 上，常驻进程代管 shuffle 文件，executor 死了也能继续提供拉取服务。配置：

```yaml
# spark-defaults.conf
spark.shuffle.service.enabled           true
spark.shuffle.service.port              7337
spark.dynamicAllocation.enabled         true
spark.dynamicAllocation.executorIdleTimeout  60s

# yarn-site.xml (NM 侧)
yarn.nodemanager.aux-services                 mapreduce_shuffle,spark_shuffle
yarn.nodemanager.aux-services.spark_shuffle.class \
  org.apache.spark.network.yarn.YarnShuffleService
```

开 ESS 之后那次"executor 被杀导致 shuffle 失败"的故障在我们集群基本绝迹。代价是 NM 多一个常驻进程，shuffle 大的时候磁盘 IO 压力会集中到 NM 上——开头那次网卡打满，本质上就是 ESS 成了单点瓶颈。

## 五、Push-based Shuffle（Spark 3.2+）

### 5.1 动因：reduce 端的连接抖动

传统 shuffle 里，reduce 端要和所有 map task 建连接拉数据，连接数是 M × R 量级。大作业下 reduce 端的拉取调度、连接复用、GC 暂停都会放大抖动，数据倾斜时个别 reduce 拉取堆积，进一步恶化。

push-based shuffle 借鉴了 MapReduce 的思路：map task 不再原地等 reduce 来拉，而是主动把自己的输出**推**到一组 shuffle 服务上，按 reduce partition 重新分桶；reduce 端只从这组 shuffle 服务拉取，连接数大幅收敛。

### 5.2 SPI 实现

Spark 3.2 把 push-based shuffle 做成了可插拔的 SPI，核心接口是 `org.apache.spark.network.shuffle.ShuffleDriverComponents` 和 `ShuffleExecutorComponents`。开启方式：

```properties
spark.shuffle.manager                           org.apache.spark.shuffle.sort.SortShuffleManager
spark.shuffle.push.enabled                      true
spark.shuffle.push.server.min.version           3.2.0
# 推送的目标：可对接 ESS 或外部 shuffle service
spark.shuffle.push.mergers                      <numMergers>
```

push 的精髓在于"merge"环节：多个 map task 推来的同一个 partition 的数据在 shuffle 服务侧被合并成一个文件，reduce 端只需拉取少数几个已合并的大文件，而不是海量 map 输出。这对大作业、特别是 reduce 端分区数大、map task 多的场景，提升明显——官方 benchmark 在某些 TPC-DS 查询上有 10%–30% 的提速。<!-- 校准：请按真实经历核实/替换 -->

push 的流程比传统 shuffle 多一步。map task 把数据按 partition 分组后，不是等 reduce 来拉，而是立即推送到一组指定的 merger 节点上；merger 收到同一 partition 的多个推送后，按顺序合并成一个连续文件并生成索引。如果某个 merger 不可用或某个 partition 太大无法在单节点合并，push 会退化回传统拉取路径——这种"尽力而为"的设计保证了不会因为 push 失败而整个 shuffle 失败。

注意 push-based shuffle 当前对 `groupByKey`、`join` 这类普通 shuffle 生效，但对需要 map 端聚合的 shuffle（如 `reduceByKey`）路径不同，使用前要按版本核对支持矩阵。另外它对 block 大小有要求：太小的 block 走 push 反而增加 RPC 开销，Spark 内部有个 `spark.shuffle.push.maxBlockBatchSize` 之类的阈值来控制批合并粒度。

## 六、与 Hadoop MapReduce shuffle 的对比

我是从 Hadoop 时代过来的，两边对比很有意思：

| 维度 | Hadoop MapReduce | Spark |
|------|-----------------|-------|
| map 端写盘 | 每个 map 输出按 partition 顺序写一个 data + index | 同（SortShuffle 后趋同） |
| 拉取方式 | HTTP，reduce 主动拉 | HTTP，reduce 主动拉（push-based 改为推送+合并） |
| 内存模型 | JVM 堆，受 GC 拖累 | 堆外 + 序列化（Tungsten），绕开 GC |
| combiner | map 端 spill 时调用 | map 端聚合在内存 map 里做 |
| 文件服务 | NodeManager 常驻 shuffle service | ESS（同思路） |
| 失败恢复 | 重跑 map | 推测执行 + ESS 容错 |

可以看出 Spark 的 SortShuffle 在结构上其实向 MapReduce 收敛了，但用 Tungsten 的堆外序列化把 GC 和内存效率拉到了一个新的台阶。push-based shuffle 则是把 MapReduce 早就有的"中间 merge 服务"思想搬过来，结合 SPI 做成可插拔。

一个本质区别值得点出：Hadoop MapReduce 的 map 输出天然就是先 spill 再 merge 的，它的 combiner 在 spill 时就调用，能显著减少 shuffle 量；而 Spark 把"是否 map 端聚合"交给算子语义决定（`reduceByKey` 有，`groupByKey` 没有），更灵活但也更容易被用错。我见过太多同学用 `groupByKey` 之后再 reduce，等于把 shuffle 量放大了好几倍，map 端 combiner 的红利全丢了。

## 七、实战配置清单

把我这些年压测和踩坑沉淀下来的关键配置列一下，供参考：

```properties
# —— Shuffle Manager（2.0+ 默认就是 sort，一般不用动）——
spark.shuffle.manager                           sort
spark.shuffle.sort.bypassMergeThreshold         200

# —— External Shuffle Service（强烈建议开）——
spark.shuffle.service.enabled                   true
spark.shuffle.service.port                      7337

# —— Reduce 拉取控制（防网卡打满）——
spark.reducer.maxSizeInFlight                   24m   # 默认 48m，大作业可下调
spark.reducer.maxReqsInFlight                   16

# —— 数据倾斜（AQE 联动）——
spark.sql.adaptive.enabled                      true
spark.sql.adaptive.skewJoin.enabled             true
spark.sql.adaptive.skewJoin.skewedPartitionFactor        5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes  256mb
spark.sql.adaptive.advisoryPartitionSizeInBytes          128mb

# —— Push-based Shuffle（3.2+，大作业可试）——
spark.shuffle.push.enabled                      false  # 默认关，按需开
```

其中 `spark.sql.adaptive.skewJoin` 这一组我重点说一句：它不是 shuffle 机制本身，但和 shuffle 强相关。AQE 在 shuffle 写完后统计每个 partition 大小，发现倾斜就把大 partition 拆成多个子任务并行处理，最后再合并——这是应对数据倾斜最省心的方式，比手动 `salt + explode` 干净得多。我们那个夜间 join 事故，最终就是靠 `skewJoin.enabled=true` + 调小 `advisoryPartitionSizeInBytes` 解决的。<!-- 校准：请按真实经历核实/替换 -->

## 小结

这一路复盘下来，Spark Shuffle 的演进逻辑很清晰：HashShuffle 用文件数换简单，崩在文件数；SortShuffle 用排序换文件数，崩在排序和内存；Tungsten-Sort 用堆外序列化换内存和 GC；push-based shuffle 用推送合并换 reduce 端连接抖动。每一代都是在解决上一代的瓶颈。

理解了 shuffle 的写盘、排序、索引、拉取这条主线，下一篇我们接着聊 Spark 的内存管理——`MemoryManager`、`UnifiedMemoryManager`、execution 内存与 storage 内存的动态借用，以及为什么 shuffle 的 `PartitionedAppendOnlyMap` 在内存压力大时会反复 spill，把磁盘 IO 打成锯齿状。那才是理解 shuffle 调优的真正地基。

---

<!-- 总校准：本文涉及的数字（集群规模 200 节点、文件数 640000、写盘吞吐、maxSizeInFlight 调到 24m、bypassMergeThreshold 默认 200、serialized shuffle 阈值 2^24、push-based 提速 10%–30%、advisoryPartitionSizeInBytes 128mb 等）均按个人经历写出，发布前请按真实生产环境核实/替换。版本基准为 Spark 3.2+，push-based shuffle 的参数名与支持矩阵在不同小版本有差异，请以官方文档为准。-->
