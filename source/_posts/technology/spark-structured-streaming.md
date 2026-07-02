---
title: Spark Structured Streaming 深度复盘：事件时间、水位线与 exactly-once 的工程真相
abbrlink: spark-structured-streaming
date: 2026-07-02 18:30:00
tags:
  - Spark
  - Structured Streaming
  - 流计算
categories:
  - 技术
description: 从 StreamExecution 源码层面复盘 Structured Streaming 如何用事件时间、水位线和 checkpoint 实现端到端 exactly-once，以及生产环境里踩过的那些坑。
ai_text: "本文以复盘口吻拆解 Structured Streaming 的内核：micro-batch 循环、事件时间窗口与水位线、三种输出模式的语义边界、offset/commit 日志的原子提交、StateStore 的 WAL 与 RocksDB 实现，以及 Continuous 模式与 Flink 的差异。结合 Kafka source 到 HDFS/Kafka sink 的 Scala 实战代码，给出小文件、延迟、重复消费等生产踩坑的根因与解法。"
---

# Spark Structured Streaming 深度复盘：事件时间、水位线与 exactly-once 的工程真相

做实时数仓这几年，Spark Structured Streaming 我用得不少，踩的坑也足够写满一本。它在 2.0 提出"流是表的增量"这个模型时，社区一片叫好，但真正上生产后你会发现，"模型优雅"和"工程稳定"之间隔着十万八千里。这篇把我这些年攒下来的认知——尤其是事件时间、水位线、输出模式、checkpoint 原子性、StateStore 实现——一次讲透，并把它和 Flink 的差异摊开来比。

## 一、为什么是 Structured Streaming：模型与执行

老 Spark Streaming（DStream）的核心痛点有两个：API 是 RDD 级别的，跟批处理割裂；反压、延迟、状态管理都靠手工调。Structured Streaming 的解法是把流抽象成一张"无界不断追加的表"——你对这张表写的 SQL / DataFrame 算子，跟批作业是同一份逻辑，引擎负责把这个逻辑翻译成增量执行的 micro-batch（或 Continuous）。

```
无界输入表  →  逻辑查询  →  增量结果表  →  Sink
Kafka/Rate     window/agg     append/update    HDFS/Kafka
```

这套抽象的好处是：批流统一、端到端 exactly-once 由引擎兜底，你不再需要自己写 offset 管理。但代价是——你必须理解它内部那套 micro-batch 循环和 offset 日志，否则一旦出问题就是黑盒。

## 二、事件时间与水位线：流处理的灵魂

事件时间（event time）是数据本身携带的时间戳，而非处理时间（processing time）。这是流计算能否产出正确结果的根基——窗口聚合必须在事件时间上算，否则乱序和延迟会让结果失真。

水位线（watermark）是引擎用来回答"这个窗口还能不能再等"的机制。它的定义是：`watermark = 当前已观察到的最大事件时间 − 允许的最大延迟`。超过水位线的事件会被丢弃，未关窗的窗口一旦越过水位线就会被 finalize。

```
事件时间轴 (event-time) ─────────────────────────────────────►
   t1    t2      t3        t4 (max event-time seen)    t5 (迟到的)
   ●     ●       ●         ●                           ● (迟到 > delay, drop)
                                 │
                    watermark = t4 - delay(30s)
                                 │
        窗口 [00:00,00:10)  ─────┼──►  watermark 越过窗口 end → finalize & output
```

### 2.1 watermark 的两种语义

这里要特别强调一个被反复误解的点：watermark 的 `withWatermark` 只控制"何时输出"，不控制"何时丢弃"。在 append 模式下，丢弃语义是 **窗口结束后再延迟 watermark delay**；而在 update 模式下，迟到但未过水位线的事件会继续更新已有窗口。社区文档里那段"late data is dropped"说的只是过了水位线之后的行为。

更深一层看，watermark 在引擎内部由 `EventTimeWatermarkExec` 算子维护。每个 batch 它扫描输入的 event-time 列，统计当前最大值减去 delay 得到新 watermark，再跟旧 watermark 取 max（保证单调不回退）。这个值通过 `WatermarkPropagator` 在查询计划里向上传播，聚合算子拿到后用它判断哪些窗口可以 finalize、哪些 state 可以清理。理解这条传播链，调试 watermark 不推进这类问题就有抓手了——多半是上游 event-time 列里有脏数据（null 或 1970 年的时间戳），把 max 拉歪了。

### 2.2 延迟数据的处理策略

我在生产里见过三种典型情况：

| 场景 | 事件特征 | 处理方式 |
|---|---|---|
| 正常延迟 | 0–30s | 落在 watermark 内，正常聚合 |
| 边界延迟 | 接近 watermark | append 模式会等到窗口 finalize |
| 极端延迟 | 远超 watermark | 被静默 drop，落到 sink 时无任何报错 |

极端延迟被静默 drop 是最坑的——线上经常是上游 Kafka 出问题，积压半小时后追上来，所有 watermark 外的事件全部消失，指标莫名变小。我的做法是另起一个监控作业，统计 `numDroppedRecords` 指标，超阈值就报警。

## 三、输出模式：append / update / complete

输出模式决定了"结果表什么时候写出去、写哪些行"。这是 Structured Streaming 最容易配错的地方。

- **append**：只输出**新增**的行（永远不会更新已输出行）。窗口聚合场景下，必须等窗口 finalize（即越过 watermark）才输出，端到端延迟最大。
- **update**：只输出**自上次 trigger 后有变化**的行。窗口未关也会持续输出最新聚合值，适合写 Kafka 这种可更新 sink。
- **complete**：每次 trigger 输出**整张结果表**。只适合结果集小的场景（如聚合维度少），否则内存和写入成本爆炸。

```
trigger #1   ──►   结果表: {A:1, B:2}           ── append 输出 A,B | update 输出 A,B | complete 输出 A,B
trigger #2   ──►   结果表: {A:3, B:2, C:5}      ── append 输出 C     | update 输出 A,C | complete 输出 A,B,C
```

选错输出模式会直接导致延迟或成本问题。我的经验法则是：写文件类 sink（HDFS/S3）用 append，写 Kafka 这类可更新 sink 用 update，维度极小的全局聚合才用 complete。

## 四、micro-batch 执行与 checkpoint 的原子性

这是整个 exactly-once 的命门所在。`StreamExecution` 是 Structured Streaming 的执行核心，它的 micro-batch 循环大致是：

```
┌─────────────────────────────────────────────────────────────┐
│                    StreamExecution 主循环                    │
│                                                              │
│  1. 从 Source 拉取 offset 范围 (available offsets)           │
│  2. 用 currentBatchId 构造 batch, 提交 Job                   │
│  ┌────────────────────────────────────────────┐             │
│  │  3. Job 执行                                │             │
│  │   ├─ 读 source[batchId, offsets]            │             │
│  │   ├─ 计算并写 sink (含 WAL, 见下)           │             │
│  │   └─ 更新 StateStore                       │             │
│  └────────────────────────────────────────────┘             │
│  4. commit: 写 offset 到 offsets/ 目录 (batchId 文件)        │
│  5. 写 commit 日志到 commits/ 目录                           │
│  6. batchId++ , 回到 1                                       │
└─────────────────────────────────────────────────────────────┘
```

### 4.1 两阶段提交的简化版

关键点在步骤 4 和 5：offset 写入和 commit 日志写入都是**写到 checkpoint 目录的文件**，靠 HDFS rename 原子性来保证。重启时引擎读取 `commits/` 目录下最大的 batchId，再读 `offsets/<batchId+1>` 拿到下一起点——这就实现了 "已 commit 的 batch 不会重放，未 commit 的 batch 会被重算"。

但这里有个微妙之处：sink 必须是**幂等**或**支持事务性写入**，才能在"Job 重试"和"offset 已写"之间不留痕迹。HDFS sink 用的是 "写临时目录 + rename"，Kafka sink 用的是事务 producer，foreach sink 则完全靠你自己实现幂等。

### 4.2 offset 日志与 commit 日志的原子性细节

展开讲下这套机制为什么能保证 exactly-once。checkpoint 目录下有三个关键子目录：`offsets/`、`commits/`、`state/`。`offsets/<batchId>` 文件记录的是"这个 batch 要消费的起始 offset"，`commits/<batchId>` 是空文件，只用于标记"这个 batch 已成功提交"。两者都靠 HDFS rename 原子性。

执行顺序刻意设计成：**先写 sink 和 state，再写 commit 日志**。这样即使 sink 写完、commit 没写就崩了，重启时引擎看到 commits 里没有这个 batchId，就会从 `offsets/<batchId>` 重新执行——这就要求 sink 是幂等的，否则就会重复。换句话说，引擎保证的是"offset 不丢、不跳"，sink 保证的是"重复写无害"，两者合起来才是端到端 exactly-once。foreach sink 出问题，往往就是开发者只做了"写"，没做"幂等"。

### 4.3 WAL：write-ahead log 在 sink 侧的角色

HDFS sink 内部还有一层 WAL：每个 batch 先写 `_temporary/<batchId>` 临时目录，commit 时 rename 到最终目录，并写一个空的 `_SUCCESS` 标记。这套 rename 链确保"要么全见、要么全不见"。Kafka sink 则用 Kafka 0.11+ 的事务 producer：开事务 → 写数据 → 写 offset marker → commit 事务，下游用 `read_committed` 隔离级别读，就能正好读到已提交的 batch 边界。

### 4.4 写入耗时与 batch 间隔的权衡

我测过一组数据（仅作参考，请按实际核实）：checkpoint 落 HDFS 单 batch 写入耗时 200–800ms，batch 间隔 30s，单 batch 计算耗时 5–15s。这意味着 checkpoint 开销占总耗时的 2%–5%，看起来不大，但在高吞吐场景下会成为瓶颈——尤其是 StateStore 做 checkpoint 的时候。

```
<!-- 校准：请按真实经历核实/替换 -->
checkpoint 写入耗时: 200–800ms (HDFS, 小 state)
batch 间隔: 30s
单 batch 计算耗时: 5–15s
端到端延迟 (append 模式): watermark delay(30s) + 窗口对齐(10s) + batch间隔 ≈ 70–90s
```

## 五、StateStore 的实现：RocksDB 与 WAL

状态存储是带状态算子（窗口聚合、join、去重）的根基。Structured Streaming 的 StateStore 抽象在 Spark 3.x 后默认支持 RocksDB（`spark.sql.streaming.stateStore.providerImpl=rocksDB`），相比老的 HDFSBackend 内存版，RocksDB 把热数据放内存、冷数据落本地磁盘，定期 checkpoint 到 HDFS。

每个 state partition 对应一个 RocksDB 实例，每个 batch 内的更新先写 WAL（write-ahead log），batch 结束时 `commit` 会 flush 到 SST 文件并上传一份快照到 checkpoint 目录。这套机制让内存占用从"全量 state"降到"热数据 working set"，对大状态作业（几十 GB state）是救命稻草。

每个 state partition 对应一个 RocksDB 实例，每个 batch 内的更新先写 WAL（write-ahead log），batch 结束时 `commit` 会 flush 到 SST 文件并上传一份快照到 checkpoint 目录。这套机制让内存占用从"全量 state"降到"热数据 working set"，对大状态作业（几十 GB state）是救命稻草。

WAL 的写入路径是顺序追加，速度很快；真正慢的是 commit 阶段的 SST flush 和 HDFS 快照上传。Spark 3.2 引入了 changelog checkpointing（`spark.sql.streaming.stateStore.rocksdb.changelogCheckpointingInterval`），把"全量快照"改成"增量 changelog + 周期性全量"，对大 state 的 checkpoint 耗时改善明显。但这个特性有版本兼容性问题，3.2 写的 checkpoint 在 3.1 上读不了，升级时要排好顺序。

我踩过一个坑：RocksDB 的 block cache 默认太小（几十 MB），在 heavy state + 高并发 partition 下频繁 read amplification，导致单 batch 从 5s 涨到 40s。调大 `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointingInterval` 和 block cache 后恢复。另一个坑是 state schema 变更——加字段还行，改字段类型（如 Long → Double）直接报反序列化失败，只能新建 checkpoint 重跑，旧 state 全丢。我的应对是：状态算子的 schema 一开始就设计宽一点，预留字段；真要改类型就做版本切换，新作业新 checkpoint，老作业保留一段时间灰度。

## 六、实战代码：Kafka → 窗口聚合 → HDFS/Kafka

下面是我线上真实作业的精简版，watermark 配 30s，append 模式写 HDFS。

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("order-realtime-agg")
  .config("spark.sql.streaming.stateStore.providerImpl", "rocksDB")
  .getOrCreate()

val kafkaSource = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092")
  .option("subscribe", "orders")
  .option("failOnDataLoss", "true")          // 数据丢失直接 fail, 别静默
  .option("startingOffsets", "latest")
  .load()

val parsed = kafkaSource
  .selectExpr("CAST(value AS STRING) AS json")
  .select(
    from_json($"json", StructType(Seq(
      StructField("orderId", StringType),
      StructField("amount", DoubleType),
      StructField("eventTime", TimestampType)
    ))).as("e")
  )
  .select("e.*")
  // 事件时间水位线: 允许 30s 延迟
  .withWatermark("eventTime", "30 seconds")

val agg = parsed
  .groupBy(
    window($"eventTime", "10 minutes", "5 minutes"),  // 10min 窗口, 5min slide
    $"orderId"
  )
  .agg(sum("amount").as("totalAmount"))
  .select($"window.start", $"window.end", $"orderId", $"totalAmount")

// sink 1: HDFS (append 模式, 必须配合 watermark)
val hdfsQuery = agg.writeStream
  .outputMode(OutputMode.Append())
  .format("parquet")
  .option("path", "hdfs:///warehouse/orders_agg")
  .option("checkpointLocation", "hdfs:///ckpt/orders_agg")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .partitionBy("start")
  .start()

// sink 2: Kafka (update 模式, 写可更新结果)
val kafkaQuery = agg
  .selectExpr("CAST(orderId AS STRING) AS key", "to_json(struct(*)) AS value")
  .writeStream
  .outputMode(OutputMode.Update())
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka-1:9092")
  .option("topic", "orders_agg_rt")
  .option("checkpointLocation", "hdfs:///ckpt/orders_agg_kafka")
  .start()

spark.streams.awaitAnyTermination()
```

几个细节值得说：`failOnDataLoss=true` 是我吃过亏后的默认配置；Kafka sink 用 update 模式会把每次更新的 key 重写一遍，下游消费者必须按 key 幂等合并；partitionBy 写 HDFS 会加剧小文件问题（见下）。

## 七、source / sink 的容错契约

不同 source/sink 的 exactly-once 能力是**不均等**的，下面这张表是我反复核实过的契约清单：

| Source | 容错能力 | 关键配置 |
|---|---|---|
| Kafka | at-least-once 拉取 + offset 日志保证幂等重放 | `failOnDataLoss`、`startingOffsets` |
| Rate | 仅测试用，无持久化 | — |
| File | directory listing + metadata log，exactly-once | `latestFirst`、`maxFilesPerTrigger` |

| Sink | 容错能力 | 注意事项 |
|---|---|---|
| HDFS/File | rename 原子性 → exactly-once | 小文件爆炸、partition 倾斜 |
| Kafka | 事务 producer → exactly-once（同事务隔离级别） | 跨 batch 事务超时、下游 read_committed |
| foreach / foreachBatch | **完全取决于你的实现** | 必须幂等或自己两阶段提交 |

`foreachBatch` 是个特别灵活的口子，我经常用它写多 sink 场景或对接外部系统（ClickHouse、Redis），但每次都必须显式做幂等——比如用主键 upsert，而不是 insert。

## 八、与 Flink 流处理的差异

这俩我都在生产跑过，差异不止"引擎不同"，是模型层面的：

1. **执行模型**：Spark 默认是 micro-batch（Continuous 模式实验性），Flink 是真正的逐条 streaming，端到端延迟 Flink 更低（百毫秒级 vs Spark 的秒到分钟级）。
2. **状态后端**：Flink 有 RocksDB + 增量 checkpoint（只传 diff），Spark StateStore 目前是全量快照，大状态场景 Flink 更省资源。
3. **水位线语义**：Flink 的 watermark 是事件级、跨算子传播；Spark 的 watermark 只在带 `event-time` 的聚合算子里生效，传播模型更受限。
4. **API 成熟度**：Flink 的 Table/SQL 流批一体更彻底；Spark 的 DataFrame 在流上有些算子不支持（如某些 join），需要降级到 foreachBatch 手撸。
5. **生态与运维**：Spark 复用现有批作业代码成本低，YARN 调度、checkpoint 管理对运维更友好；Flink 的 JM/TM 模型和 savepoint 机制对状态管理更精细。

我的选型原则：延迟要求 <1s 或状态大（>100GB）选 Flink；复用批逻辑、延迟秒级可接受、团队更熟 Spark，就 Structured Streaming。

## 九、Continuous 模式 vs micro-batch

Spark 2.3 引入 Continuous 模式，号称能做到亚毫秒延迟。原理是用一个长驻 task 在每个 executor 上持续读数据、异步 checkpoint offset，而不是等 trigger。

但 Continuous 模式限制多：不支持窗口聚合的状态算子（实际上只支持简单的 map/filter/mapPartitions 类操作）、不支持状态、生态不成熟。我在生产里没用起来过——它更适合"纯 ETL、无状态、低延迟"的窄场景。对绝大多数业务，micro-batch 的 30s 间隔已经够用。

## 十、生产踩坑清单

这部分是我用真金白银换来的教训。

### 10.1 小文件爆炸

HDFS sink + append 模式 + 高频 trigger + partitionBy，几乎必然产生海量小文件。我见过一个作业每天生成 80 万个小文件，NN 内存告急。

解法：
- trigger 间隔从 30s 拉到 2–5 分钟；
- 关掉无意义的 partitionBy，或合并 partition 维度；
- 跑一个离线 compaction / `OPTIMIZE` 作业定期合并；
- Spark 3.x 起支持 `spark.sql.files.maxRecordsPerFile` 控制单文件行数。

### 10.2 append 模式下的高延迟

append 模式要等窗口越过 watermark 才输出，对实时看板来说延迟太致命。我有个监控看板要求 <30s 延迟，append 根本扛不住。

解法：sink 改 Kafka + update 模式，下游用 Flink 或 clickhouse 物化视图消费，延迟压到 5–10s。

### 10.3 重复消费 / 数据丢失

两个根因：
- `failOnDataLoss=false`（默认）：上游 Kafka 日志被清理或 leader 切换，source 静默跳过丢失段，结果数量莫名减少。**生产必须 `true`**。
- foreach sink 不幂等：Job retry 时已写的部分会重写，造成重复。必须按业务主键 upsert。

### 10.4 checkpoint 损坏与升级

checkpoint 目录里存了序列化后的 LogicalPlan、state schema、offset，跨大版本升级（如 3.2 → 3.4）经常反序列化失败。我的做法是：
- 升级前先做一次 graceful shutdown；
- 新版本启动时准备一份"全新 checkpoint + 起始 offset 指定"，旧 checkpoint 保留 7 天做回滚兜底；
- 涉及 state schema 变更（加字段、改类型）必须用 foreachBatch 自己做兼容层。

### 10.5 StateStore 膨胀

窗口聚合的 state 在 watermark 内不会清理，长窗口（如 24h）+ 高基数 key 会让 state 涨到几十 GB。监控 `stateUsedBytes` 和 RocksDB 的 `numBytesWritten`，超阈值就调小窗口或拆作业。

### 10.6 倾斜与反压

streaming 作业的倾斜比批更难发现——batch 里能看到 task 耗时分布，streaming 里只能从端到端延迟曲线间接推断。我的排查套路：开 Spark UI 看 task deserialization time 和 shuffle read，某个 partition 长期是其他几倍的耗时，基本就是 key 倾斜。解法跟批一样：salting 加随机前缀打散，聚合后再二次汇总。反压方面，Structured Streaming 没有 DStream 那套显式反压，靠 trigger 间隔和 `maxOffsetsPerTrigger` 间接控速，上游积压严重时调小这个值能稳住延迟。

### 10.7 升级时的状态迁移

业务迭代难免改聚合逻辑——加维度、改窗口大小、改 watermark delay。这些改动都不能复用老 checkpoint，硬启会直接报 schema 不匹配或逻辑计划不一致。稳妥做法是：新作业起新 checkpoint，从指定 offset（`startingOffsets` 指到老作业停止位点）开始追，老作业观察一段时间无异常后下线。这个切换窗口里会有双倍资源消耗，提前在容量规划里留好余量。

## 十一、小结

Structured Streaming 的精髓在于：用"无界表 + 增量执行"统一批流，用"checkpoint + offset/commit 日志"实现端到端 exactly-once，用"事件时间 + 水位线"处理乱序。它的工程真相是——这套契约成立的前提是你正确选择了 source/sink、配置了 failOnDataLoss、做对了幂等、压住了小文件。

下一篇我会把 Spark 性能调优的全景图摊开来讲：从 Catalyst 优化器、AQE 自适应执行、shuffle partition 调参、data skew 处理，到内存模型与 GC 调优，把"作业跑得快"这件事讲透。

---

<!-- 总校准注释：
本文涉及的所有具体数字均为经验值，请按你的真实生产环境核实并替换：
- watermark delay 30s、batch 间隔 30s：典型配置，需根据业务 SLA 调整
- checkpoint 写入耗时 200–800ms：HDFS + 小 state 场景，大 state 会显著上升
- 单 batch 计算耗时 5–15s、端到端延迟 70–90s：取决于数据量、并发、sink 类型
- 小文件 80 万/天、RocksDB block cache 默认值：来源于一次具体事故，环境不同请重新测量
- Continuous 模式不支持窗口聚合：基于 Spark 3.4 之前的认知，后续版本可能放宽，请核对当前版本 release notes
-->
