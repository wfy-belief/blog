---
title: Spark Core 运行时架构拆解：从 SparkContext 到 Executor 的完整链路
abbrlink: spark-core-architecture
date: 2026-07-02 15:00:00
tags:
  - Spark
  - 架构
categories:
  - 技术
description: 从 SparkContext 到 DAGScheduler、TaskScheduler、Executor、Worker，逐层拆解 Spark 作业的提交、切分、派发与回传
ai_text: "我维护过一个约 300 台 Worker 的 Spark on YARN 集群，本文复盘 Spark Core 运行时架构的完整链路。从 SparkContext 初始化讲起，逐层拆解 DAGScheduler 的 getMissingParentStages 如何按 ShuffleDependency 切分 Stage、TaskSchedulerImpl 的 makeOffers 如何按本地性派发 Task、CoarseGrainedSchedulerBackend 的 RPC 握手、Executor 的内存模型与 launchTask、Heartbeat 回传，最后给出一次心跳超时事故与失败重试机制的真实排查复盘。"
---

## 引言

Spark 我用了很多年，从最早的 1.6 到现在线上长期跑的 3.3.1 <!-- 校准：请按真实经历核实/替换 -->。早期我以为 Spark 就是"RDD 算子写起来比 MR 简洁"，真正深入后才意识到，它真正的工程价值不在 RDD 这个编程抽象，而在那套把一个 action 翻译成跨数百台机器的 Task 调度的运行时——DAGScheduler、TaskScheduler、SchedulerBackend、Executor 这条链路。这条链路理解透了，作业为什么慢、为什么挂、为什么数据倾斜时 Stage 卡死，都有了判断依据。

我维护过一个约 300 台 Worker 的 Spark on YARN 集群 <!-- 校准：请按真实经历核实/替换 -->，日均跑 2 万+ 作业 <!-- 校准：请按真实经历核实/替换 -->。这篇文章把这条链路从源码层拆一遍，穿插我踩过的几个坑。

## 运行时角色总览

先把整体角色和它们的边界画清楚。一次 Spark 作业运行起来后，进程级别的角色其实不多：

```
                       +-----------------------+
   driver JVM          |   SparkContext        |
                      +-+---------------------+-+
                      |  DAGScheduler         |   逻辑层：切 Stage
                      |  TaskSchedulerImpl    |   调度层：派 Task
                      |  SchedulerBackend     |   通信层：RPC 到 Executor
                      +-----------+-----------+
                                  |  CoarseGrained RPC
            +---------------------+---------------------+
            |                     |                     |
       +----v----+           +----v----+           +----v----+
       |Executor0|           |Executor1|   ......   |ExecutorN|
       | Executor |          | Executor |           | Executor |
       |  Backend |          |  Backend |           |  Backend |
       +----+----+           +----+----+           +----+----+
            |  launchTask          |                     |
       +----v----+            +----v----+           +----v----+
       |Task 0/1 |            |Task 2/3 |           |Task ...  |
       +---------+            +---------+           +----------+
```

这里有一个关键区分：**Driver 不是 SparkContext**，Driver 是跑 SparkContext 的那个 JVM 进程；而 **Executor 是 Worker 上 fork 出来的子进程**，每个 Executor 内部用线程池跑 Task。在 YARN 模式下，Worker 这个概念被弱化成 NodeManager 上的一个 Container，所以下文我尽量用 Executor 而不是 Worker，避免和 YARN 的 NodeManager 混淆。

## SparkContext：一切的入口

`SparkContext` 是用户代码和运行时之间的唯一桥梁。我第一次看 `SparkContext` 初始化源码时，被它 new 出来的一长串组件吓到，后来才明白每个都有明确职责：

| 组件 | 职责 |
|---|---|
| `SparkEnv` | 持有所有 RPC、序列化、内存管理、广播变量等基础设施的容器 |
| `DAGScheduler` | 把 RDD 血缘切成 Stage，生成 TaskSet |
| `TaskScheduler`（实为 `TaskSchedulerImpl`） | 把 TaskSet 按 locality 派发给 Executor |
| `SchedulerBackend`（`CoarseGrainedSchedulerBackend` 等） | 维护与各 Executor 的 RPC 连接、资源Offer |
| `MapOutputTrackerMaster` | 记录每个 ShuffleMapStage 的输出位置，供下游 reduce task 拉取 |

`SparkContext` 初始化时，会根据 `spark.master` 决定用哪个 `SchedulerBackend`。yarn-cluster/yarn-client 都是 `CoarseGrainedSchedulerBackend`，local 模式是 `LocalSchedulerBackend`。这一点很关键——**coarse-grained（粗粒度）** 意味着 Executor 一旦申请就常驻，Task 在已有 Executor 上排队跑，不像 Mesos 细粒度模式那样每个 Task 现申请现释放。我们线上清一色 coarse-grained，因为 batch 场景下 Executor 启动开销要摊薄掉。

一个 action（比如 `collect`）最终走到 `SparkContext.runJob` → `DAGScheduler.runJob`，链路就从这里启动了。

## DAGScheduler：Stage 怎么切出来的

DAGScheduler 是我最喜欢读的一块源码，逻辑非常干净。它的核心问题是：**给定一个 RDD 血缘图，怎么切成一堆有依赖关系的 Stage？**

答案藏在 `getMissingParentStages` 里。这个方法从最终 RDD 出发，反向 BFS 遍历父依赖：

- 遇到 `NarrowDependency`（窄依赖，map/filter/union）：继续往上走，不切 Stage。
- 遇到 `ShuffleDependency`（宽依赖，shuffle 算子触发）：**在这里断开**，把上游封成一个 `ShuffleMapStage`，下游属于当前 Stage。
- 遇到已经缓存过的 RDD（`cache`/`persist` 且已物化）：直接跳过，不再往上递归。

```
   最终 RDD (ResultStage)
        |
   map  (narrow, 同 Stage 0)
        |
   groupByKey  (shuffle! 在此断开)
        |                     Stage 0 = [groupByKey 下游的 map]
   filter (narrow)            Stage 1 = [map -> groupByKey]  (ShuffleMapStage)
        |
   map  (narrow)
        |
  textFile (source)
```

切出来的 Stage 之间是树形依赖：Stage 0 依赖 Stage 1 的 shuffle 输出。`getMissingParentStages` 名字里的 "missing" 指"还没算完的父 Stage"——只有父 Stage 都 finished，当前 Stage 才能 submit。这就是为什么 shuffle 出问题时整条链路卡住：上游 Stage 不 finish，下游连提交都提交不了。

DAGScheduler 切完 Stage 后，调用 `submitStage`，对每个就绪的 Stage 调 `submitMissingTasks`，把 Stage 拆成一组 `Task`：

- `ShuffleMapStage` 拆成 N 个 `ShuffleMapTask`，N = 上游分区数。
- `ResultStage` 拆成 N 个 `ResultTask`，N = 最终 RDD 分区数。

每个 Task 处理一个 partition，这就是 Task 粒度的来源。**一个 Stage 的 Task 数 = 它对应 RDD 的分区数**，所以分区数（`parallelism`）直接决定了并行度。我踩过一次坑：一个 reduceByKey 作业跑了 4 小时，最后发现 `spark.default.parallelism` 没设，用了默认 200，而集群有 1000 个 core，并行度严重不足 <!-- 校准：请按真实经历核实/替换 -->。

需要补一个容易忽略的点：DAGScheduler 是**事件驱动**的，内部有一个 `DAGSchedulerEventProcessLoop`，所有动作（JobSubmitted、TaskCompleted、StageFailed）都先变成事件塞进队列，再由单独的事件线程串行处理。这样设计是为了把"切 Stage""处理 Task 完成回调""重试"这些有状态的操作收敛到单线程，避免并发竞争。我在排查一个 Job 莫名不推进的问题时，最后发现是事件队列堆积——上游几十个 Stage 同时完成，事件处理线程来不及消化，表现为 Driver CPU 打满但 Task 不派发。看 jstack 时盯着 `DAGSchedulerEventProcessLoop` 那个线程的栈就能判断。

### ShuffleMapStage 的"完成"语义

还有一个细节值得拎出来：`ShuffleMapStage` 的 finished 判定不是"所有 ShuffleMapTask 跑完"，而是"所有 partition 都有可用输出"。这两者在正常情况下等价，但配合推测执行（speculation）和 Task 失败重试时就不同。一个 partition 可能被重复执行多次，只有第一个成功的输出注册到 `MapOutputTrackerMaster` 才算数，后续重复的 Task 直接被丢弃。这就是为什么推测执行开启后，慢节点上的 Task 会被复制到别的 Executor，谁先完成用谁——`MapOutputTrackerMaster` 用 `AtomicReference` 保证注册的原子性。

## TaskSchedulerImpl：makeOffers 里的本地性

TaskSet 交给 `TaskSchedulerImpl` 后，进入调度层。这层的核心方法叫 `makeOffers`，名字来自 YARN/Mesos 的"资源 offer"概念，意思是"现在有一批 Executor 空出来了，给我看看能派哪些 Task 上去"。

`makeOffers` 的简化逻辑：

1. 收集所有 alive 的 Executor 及其空闲资源（core、memory）。
2. 对当前待调度的 TaskSet，按 **本地性级别**（`PROCESS_LOCAL` > `NODE_LOCAL` > `RACK_LOCAL` > `NO_PREF` > `ANY`）匹配。
3. 对每个 Executor，选出"本地性最好且资源够"的 Task 派发。

本地性这层很关键。`PROCESS_LOCAL` 表示 Task 要处理的数据就在这个 Executor 的同一个 JVM 内（比如 `cache` 过的 RDD partition），`NODE_LOCAL` 表示数据在本节点磁盘（比如 HDFS block 副本之一在本机）。Spark 会等待一段时间（`spark.locality.wait`，默认 3s）来"跳级"——宁可等 3 秒也要拿到更高级别的本地性，因为这个 Task 跑起来可能省几十秒的网络 IO。

`makeOffers` 的触发有两种：一是新 TaskSet 提交时主动调用；二是 Executor 通过心跳上报"我有空闲 core 了"时被动触发。后者就是为什么 Task 完成后下一个 Task 能很快被派下去——心跳既是存活检测，也是资源汇报。

### 本地性等待与降级

`spark.locality.wait` 的"跳级"逻辑我多解释一句，因为很多人误以为 Spark 一发现 NODE_LOCAL 就立刻派，其实不是。`TaskSetManager` 对每个 Task 维护一个"当前可接受的最差本地性"状态，初始是它最希望的级别（比如 PROCESS_LOCAL）。如果在 `locality.wait` 时间内没找到该级别的资源，就**降级**到下一档（NODE_LOCAL），再等一个 `locality.wait`，再降级。整个 TaskSet 的等待由 `TaskSetManager` 统一协调，`myLocalityLevels` 记录这个 TaskSet 实际可能用到的级别序列。

这套机制在数据本地性好的场景很有效，但在数据倾斜时反而是负担：如果某个 partition 的数据只落在少数几个节点上，大量 Task 争抢同一批 Executor，本地性降级又慢，就会出现"明明有空闲 Executor 却不派 Task"的假象。我遇到过一次，把 `spark.locality.wait` 从 3s 降到 1s 后吞吐反而上去了 <!-- 校准：请按真实经历核实/替换 -->，因为牺牲一点本地性换来更高的资源利用率。

### 推测执行

`TaskSchedulerImpl` 还内置推测执行：`TaskSetManager` 定期扫描在跑的 Task，如果某个 Task 运行时间超过中位数的 `speculation.multiplier` 倍（默认 2），就复制一个同样的 Task 派到别的 Executor 上，谁先完成用谁。它的实现是 `TaskSetManager.checkSpeculatableTasks`，由 `TaskSchedulerImpl` 的定时任务周期触发。推测执行对慢节点有效，但对数据倾斜无效——因为倾斜 Task 的"慢"是数据量大，复制到哪都慢，反而浪费资源。所以推测执行要配合 `spark.sql.adaptive.skewJoin` 这类倾斜处理一起用。

## CoarseGrainedSchedulerBackend：RPC 握手

`SchedulerBackend` 在 coarse-grained 模式下，真正和 Executor 通信的是 `CoarseGrainedSchedulerBackend`，它跑在 Driver 端，持有一个 `DriverEndpoint`，对应的 Executor 端有 `CoarseGrainedExecutorBackend`。两者通过 Spark 的 Netty RPC 通信。

Executor 启动后的第一件事是向 Driver 注册：

```
Executor 启动
   |
   v
CoarseGrainedExecutorBackend.onStart()
   |  RegisterExecutor(executorId, host, cores, logUrls)
   v
DriverEndpoint.receiveAndReply(RegisterExecutor)
   |  加入 executorDataMap，回复 RegisteredExecutor
   v
DriverEndpoint.makeOffers()  ->  触发 Task 派发
```

注册成功后，Driver 才会把这个 Executor 纳入资源池。这里有个我曾经排查过的坑：作业卡在"submitted"不动，最后发现是 Executor 注册时的 RPC 端口被防火墙拦了，Driver 一直收不到 RegisterExecutor，自然也就不会派发任何 Task。`spark.driver.host` 和 `spark.driver.port` 配错也会导致同样问题。

派发 Task 的 RPC 消息叫 `LaunchTask`。Driver 把序列化后的 Task 通过 `LaunchTask` 发给对应 Executor 的 `CoarseGrainedExecutorBackend`，后者反序列化后交给 `Executor.launchTask` 执行。

值得一提的是，`LaunchTask` 里发的是**整个 Task 的闭包 + 依赖的广播变量句柄**，序列化用 `closureSerializer`（默认 Java 序列化，Kryo 只用于数据）。闭包序列化是个隐蔽的性能点：如果闭包里不小心捕获了一个大对象（比如外部的 HashMap），每次派发 Task 都会序列化一份过去，网络和 GC 双重压力。我排查过一次 `org.apache.spark.SparkException: Job aborted due to stage failure`，根因就是闭包里捕获了一个 200MB 的查找表，改成 `broadcast` 后作业时间从 40 分钟降到 5 分钟 <!-- 校准：请按真实经历核实/替换 -->。

`CoarseGrainedSchedulerBackend` 还负责资源动态调整。开了 `spark.dynamicAllocation.enabled=true` 后，`ExecutorAllocationManager` 会根据 pending task 数动态请求或回收 Executor。请求通过 `DriverEndpoint` 向 YARN 申请 Container，回收时发 `RemoveExecutor` 给对应 Executor 的 backend。这套机制能让集群资源利用率从静态分配的 40% 提到 70% 左右 <!-- 校准：请按真实经历核实/替换 -->，代价是 Executor 频繁启停，shuffle 数据要靠 external shuffle service 保活才能跨 Executor 复用。

## Executor：launchTask 与 Heartbeat

Executor 是真正干活的进程，一个 JVM 内部维护一个 `TaskRunner` 线程池（大小 = `spark.executor.cores`）。

### 内存模型

讲 `launchTask` 之前先说 Executor 的内存划分，因为这是后面所有问题的根。一个 Executor 的堆内内存被 `SparkEnv` 在启动时切成几块：

| 区域 | 占比（默认） | 用途 |
|---|---|---|
| `ReservedMemory` | 固定 300MB | Spark 内部对象，不可调 |
| `User Memory` | `(1-0.6) × (heap-300MB)` | 用户代码里的数据结构、闭包对象 |
| `UnifiedMemory`（Storage + Execution）| `0.6 × (heap-300MB)` | 缓存块 + shuffle/sort 中间数据，两者软边界可互借 |

`spark.memory.fraction=0.6` 控制的就是 UnifiedMemory 占比。Execution 内存不够时可以借 Storage 的，反之亦然，但 Execution 不会被 Storage 挤到 OOM——这是软边界的保证。理解这块很重要：shuffle sort 用的内存就来自 Execution 区，数据倾斜时一个 Task 的 shuffle 数据撑爆 Execution，连带把 Storage 里的缓存挤掉，引发雪崩。

### launchTask 与 Heartbeat

`launchTask` 的核心逻辑：

1. 把反序列化后的 `Task` 包装成 `TaskRunner`。
2. 提交到线程池执行。
3. `TaskRunner.run()` 里：反序列化 Task 依赖（广播变量、闭包）→ 调 `Task.run()` → 拿到结果 → 序列化结果 → 通过 RPC 回传 `StatusUpdate` 给 Driver。

心跳机制是这块的重点。Executor 起了一个独立线程，定期（`spark.executor.heartbeatInterval`，默认 10s）向 Driver 发 `Heartbeat` 消息，内容是各 Task 的度量数据（峰值内存、shuffle 读写量等）。Driver 端 `TaskSchedulerImpl` 收到后更新 metrics，并回复 `HeartbeatResponse`。如果 Driver 发现某个 Executor 超过 `spark.network.timeout`（默认 120s）没心跳，就把它标记为 dead，它上面在跑的 Task 会被重新调度到别的 Executor（`TaskSchedulerImpl.executorLost`）。

这就是下面这个事故的根因来源。

## 一次心跳超时事故复盘

某天半夜一个重点 ETL 作业连续失败，报错是 `ExecutorLostFailure (executor 12 exited caused by one of the running tasks)`。看 Executor 日志，发现是 GC 时间过长：

```
Executor heartbeat timed out after 142s  <!-- 校准：请按真实经历核实/替换 -->
```

Executor 因为 Full GC 卡了将近 2 分钟，期间心跳线程也被冻住，Driver 等不到心跳，判定 Executor 失联，把上面 8 个 Task 全部重算。而那个 Stage 是 shuffle write 阶段，重算意味着上游整个 ShuffleMapStage 重跑，雪崩。

根因是 Executor 内存配小了，`spark.executor.memory=4g` 跑一个 3GB 的 shuffle 数据集，留给用户代码的堆太小。我把内存调到 8g、`spark.memory.fraction` 调到 0.6 <!-- 校准：请按真实经历核实/替换 -->，同时把 `spark.network.timeout` 从 120s 拉到 300s <!-- 校准：请按真实经历核实/替换 -->，给偶发 GC 留余地。这类问题排查的关键是看 Executor 日志里的 GC 段和 Driver 日志里 `Removing executor` 的时间戳是否对得上。

## 作业提交配置示例

下面是我们线上一个典型的 spark-submit 模板，针对内存密集型 ETL：

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name etl-daily-aggregate \
  --driver-memory 4g \
  --driver-cores 2 \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 50 \
  --conf spark.default.parallelism=800 \
  --conf spark.sql.shuffle.partitions=800 \
  --conf spark.memory.fraction=0.6 \
  --conf spark.network.timeout=300s \
  --conf spark.executor.heartbeatInterval=10s \
  --conf spark.locality.wait=5s \
  --conf spark.speculation=true \
  --conf spark.speculation.multiplier=2 \
  --class com.acitrus.EtlDaily \
  /opt/jobs/etl-daily-1.0.0.jar \
  --date 2026-07-01
```

几个参数的取舍说明：

- `num-executors × executor-cores = 200`，集群总 core 数约 1200，留余量给其他作业 <!-- 校准：请按真实经历核实/替换 -->。
- `parallelism = 800`，约 4 倍 core 数，保证每个 core 分到 4 个 Task，本地性切换更平滑。
- `speculation=true`：开启推测执行，对付偶尔的慢节点（坏盘导致的 NODE_LOCAL 拖尾）。代价是会有重复 Task，ETL 场景幂等可接受。

对应 `spark-defaults.conf` 片段：

```properties
spark.master                     yarn
spark.submit.deployMode          cluster
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs:///spark-logs
spark.history.fs.logDirectory    hdfs:///spark-logs
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max  256m
spark.rpc.message.maxSize        256
spark.driver.maxResultSize       4g
```

`KryoSerializer` 默认不开，但生产环境强烈建议开，序列化体积比 Java 自带的小 3-5 倍，shuffle 和广播都受益。`spark.rpc.message.maxSize` 调到 256MB 是因为有些大广播变量默认 128MB 会被截断。

## 结果回传与 Task 结束

Task 跑完后，结果走两条路径：

- **直接结果（direct result）**：小结果（< `spark.driver.maxResultSize` 的单 Task 上限，默认 1GB）通过 `StatusUpdate` RPC 直接回传 Driver，Driver 聚合后交给 `TaskSchedulerImpl`，再回调 `DAGScheduler`。
- **大结果（indirect result）**：超过 `spark.driver.maxResultSize` 直接抛异常，所以大结果要走 `take`/`collect` 时一定要谨慎，或者干脆落盘再读。

ShuffleMapTask 的"结果"不是数据本身，而是它的 shuffle 输出位置元数据，回传给 Driver 后注册到 `MapOutputTrackerMaster`。下游 reduce task 启动时，通过 `MapOutputTracker` 向 Master 拉取这些位置，再去对应 Executor 拉数据——这就是 shuffle 的 fetch 阶段。这条路径下一篇讲 shuffle 时会展开。

## 失败处理与重试

运行时链路里失败处理是绕不开的一环，因为生产环境失败才是常态。Spark 的失败分三个层级，每层都有自己的重试机制：

- **Task 级失败**：`TaskRunner` 捕获异常，通过 `StatusUpdate` 上报 `FAILED` 状态。`TaskSetManager` 按 `spark.task.maxFailures`（默认 4）重试，重试时优先选别的 Executor，并把失败次数记到 Task 的 `attempts` 里。超过上限，整个 TaskSet 标记失败。
- **Stage 级失败**：Task 超过重试上限或 `TaskScheduler` 主动 abort，`DAGScheduler` 收到 `TaskSetFailed` 事件，按 `spark.stage.maxConsecutiveAttempts`（默认 4）决定是否重提 Stage。注意 Stage 重提会重新计算所有父 Stage 的依赖——如果父 Stage 已缓存，就能省掉。
- **Job 级失败**：Stage 重试耗尽，整个 Job 标记失败，`SparkContext.runJob` 抛 `SparkException`。

还有一个特殊路径是**Executor 失联**。`TaskSchedulerImpl.executorLost` 会清理该 Executor 上所有在跑的 Task（标记为 `ExecutorLostFailure`，不计入 Task 重试次数），同时把它的 shuffle 输出从 `MapOutputTrackerMaster` 摘掉。这就是为什么 Executor 大批失联时，下游 reduce task 会因为 fetch 失败而连环重试——`MapOutputTrackerMaster` 找不到上游输出，只能等上游 ShuffleMapStage 重算。生产上外部 shuffle service（ESS）就是用来缓解这个的：Executor 死了，shuffle 数据由 ESS（常驻 NodeManager）继续提供，避免连锁重算。我们集群统一开了 ESS，Executor 失联引发的连锁失败事故降了大概 60% <!-- 校准：请按真实经历核实/替换 -->。

## 小结

这篇把 Spark Core 运行时从 `SparkContext` 一路拆到 `Executor.launchTask`，关键节点是：**DAGScheduler 切 Stage（依据 ShuffleDependency）、TaskSchedulerImpl 按 locality 派 Task、CoarseGrainedSchedulerBackend 走 RPC、Executor 跑 Task 并心跳回传**。理解这条链路后，调优的方向就从"乱试参数"变成了"定位瓶颈在哪一层"——是 Stage 切分不合理（DAGScheduler 层）、还是本地性差（TaskScheduler 层）、还是 GC 卡死心跳（Executor 层）。

但这里我刻意留了一个大坑没填：**ShuffleDependency 为什么能成为 Stage 边界？shuffle 那一坨数据到底怎么从上游 Executor 流到下游 Executor 的？** 这背后是 `ShuffleMapStage` 的 write、`MapOutputTracker` 的注册与 fetch、sort-based shuffle writer 的实现，以及数据倾斜时的工程对策。下一篇我就专门拆 Spark Shuffle，从 `SortShuffleManager` 到 `UnsafeShuffleWriter`，把这条最容易出性能问题的路径讲透。

<!-- 总校准提示：本文所有集群规模、版本号、性能数字、参数取值均为示例，请按真实生产经历核实/替换后再发布。 -->
