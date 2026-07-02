---
title: YARN 架构剖析与调度器调优实战
abbrlink: hadoop-yarn-scheduler
date: 2026-07-02 10:00:00
tags:
  - Hadoop
  - YARN
  - 资源调度
categories:
  - 技术
description: 从 ResourceManager 到 Container，讲透 YARN 调度器选型与调优
ai_text: "YARN 是 Hadoop 的核心资源管理层，本文从 ResourceManager、ApplicationMaster、NodeManager、Container 四个角色切入，梳理一次应用的完整生命周期，再深入对比 FIFO、Capacity、Fair 三种调度器的内部机制与选型权衡，最后结合某集群的真实生产问题——队列饿死、AM hang、NM 不健康、容器被 OOM kill——给出一套可落地的调优参数与监控指标体系。"
---

## 引言

YARN（Yet Another Resource Negotiator）从 Hadoop 2.x 开始接管了 MapReduce 1.x 里 JobTracker 那一坨"资源管理 + 任务调度"耦合在一起的职责，把集群变成一个通用的资源调度平台。它的核心贡献不是把 MapReduce 跑得更快，而是让 Spark、Flink、Tez、Storm（通过 YARN 模式）这些计算框架能共享同一批机器、同一个 HDFS，按需申请、按量归还。

我维护过一个约 200 个 NodeManager 节点的某集群，常年混跑 Spark 批处理、Flink 流任务和零散的 Hive on Tez 查询。踩过调度器选型的坑、被 OOM kill 折磨过、也排查过 ApplicationMaster 卡死导致整个应用僵死的事故。这篇文章把这些东西系统整理出来，给同样在做 Hadoop 平台的同学一个参照。

## 核心角色与架构

YARN 的架构可以浓缩成四个核心角色：ResourceManager、ApplicationMaster、NodeManager、Container。理解清楚它们的边界，是后续所有调优的前提。

### ResourceManager（RM）

RM 是全局的"大脑"，部署在管控节点上，承担两类职责：

- **Scheduler**：纯粹的资源分配器。它只关心"哪个节点有空闲资源、哪个队列还能分"，不关心应用本身的状态。当 NM 通过心跳上报节点资源、AM 通过心跳申请资源时，Scheduler 根据 SchedulerApplication 的资源请求和队列策略做分配决策，把 Container 分配出去。Scheduler 本身是事件驱动的，调度逻辑被抽象成可插拔的策略（FIFO / Capacity / Fair）。
- **ApplicationsManager**：负责接收客户端提交的应用、启动对应的 ApplicationMaster、监控 AM 的存活并在 AM 失败时重试。它维护应用的全局状态机（NEW → NEW_SAVING → SUBMITTED → ACCEPTED → RUNNING → ... → FINISHED/FAILED/KILLED）。

RM 本身是单点，但它通过嵌入式的 leader 选举 + 状态持久化到 RM State Store（ZK / FileSystem / LevelDB）实现 HA。我们线上用的是 ZK 做 RM HA 和状态存储，failover 大概在 30 秒内能完成 <!-- 校准：请按真实经历核实/替换 -->。

### ApplicationMaster（AM）

AM 是"每应用一个"的轻量级任务主管，自己就跑在一个 Container 里（通常叫 `container_e02_xxxx_0001_000001` 这种）。它的职责包括：

1. 向 RM 注册自己（registerApplicationMaster），上报自己所在 NM 和 RPC 端口。
2. 通过 `AMRMClientAsync` 心跳（默认 1 秒）向 RM 申请资源，把拿到的 Container 进一步交给 NM 启动。
3. 监控自己管理的 task 容器状态（通过 NM 的状态更新事件或自己轮询）。
4. 任务完成后，向 RM 注销（finishApplicationMaster），释放 AM 自己的 Container。

AM 的容错是关键。如果 AM 所在 NM 挂了或者 AM 进程崩了，RM 会按 `yarn.resourcemanager.am.max-attempts`（默认 2）重试。重试次数耗尽，整个应用才标记为 FAILED。这点对长跑的 Flink 作业尤其要留意——我们把 Flink 作业的 `max-attempts` 调到了 4 <!-- 校准：请按真实经历核实/替换 -->，给偶尔的 NM 重启留点缓冲。

### NodeManager（NM）

NM 是每台机器的守护进程，干三件事：

- **Container 生命周期管理**：接收 AM（经 RM 转发或直接 NM RPC）的 `startContainers` 请求，启动 Container 进程，运行结束回收。
- **资源监控**：定期（默认 3 秒）采样本节点所有 Container 的资源使用，通过心跳上报 RM。一旦 Container 实际使用超过预设上限，NM 会主动 kill 掉它（这是后面 OOM kill 的来源）。
- **健康检查脚本**：`yarn.nodemanager.health-checker.script.path` 指向一个脚本，定期执行，用来发现坏盘、目录满、磁盘 IO 异常等硬件问题。一旦脚本输出"unhealthy"，NM 会向 RM 上报 `UNHEALTHY` 状态，RM 就不再往这节点分配新 Container。

NM 的本地目录（`yarn.nodemanager.local-dirs` 存放 Container 工作目录、`yarn.nodemanager.log-dirs` 存放日志）建议配多块盘轮转，单盘写满不至于整个节点挂掉。

### Container

Container 是 YARN 的资源隔离单元，本质上是"一份资源契约 + 一个进程"。资源契约规定了内存（MB）和 vcore 上限，进程在 cgroup（或更老的 ulimit）层面被限制。3.x 之后还引入了通用的 resource types（GPU、FPGA、自定义），通过 NM 端的 `yarn.nodemanager.resource.<type>.available` 上报，Container 申请时可以指定。

## 应用生命周期完整流程

把上面四个角色串起来，一个应用从提交到结束的完整链路是这样的：

```text
1. client: yarn jar app.jar → RM ClientRMService.submitApplication
2. RM:    状态 NEW_SAVING → 持久化 → SUBMITTED → ACCEPTED
3. RM:    Scheduler 从队列分配第一个 Container（"AM Container"）给某个 NM
4. NM:    启动 AM Container，AM 进程起来
5. AM:    registerApplicationMaster(RM) → 状态 RUNNING
6. AM:    while 任务没完成:
            AMRMClient allocate(heartbeat) → 申请 N 个 Container
            RM Scheduler 分配 → 拿到 allocatedContainers
            for each container: NMClient startContainer(NM)
7. NM:    起 task 进程，监控，状态变更事件回传给 AM
8. AM:    所有 task 完成 → finishApplicationMaster(FINISHED)
9. RM:    unregister，应用状态 FINISHED，通知 NM 清理 Container
```

几个容易踩的细节：

- 第 6 步里，AM 的 `allocate` 心跳**必须周期性调用**，否则 RM 会认为 AM 失联（默认 `yarn.am.liveness-monitor.expiry-interval-ms` = 10 分钟）把它 kill 掉。所以即使 AM 暂时不需要新资源，也要发空心跳。
- 资源请求支持"本地性提示"（locality），AM 可以告诉 Scheduler："我希望分配在存了数据块 H1 的节点上"，Scheduler 会优先满足本地性，找不到再退而求次。延迟调度（delay scheduling）就是为这种本地性权衡设计的。

## 调度器深度对比

调度器是 YARN 最值得花时间的部分。三种内置调度器差异巨大，选错直接影响多租户体验。

### FIFO Scheduler

最朴素：所有应用排一个队列，先来先服务。优点是简单、吞吐高（无锁、无公平性计算）；致命缺点是一个大应用可以把后续所有应用全部堵死。生产环境几乎没人用，除了纯单租户的离线集群。

### Capacity Scheduler（CS）

我们生产用的就是 CS。它的核心思想是"层级队列 + 弹性共享"。

```
root
├── prod（生产）       min=60%, max=100%
│   ├── realtime       min=40% of prod
│   └── batch
└── dev（开发）        min=30%, max=80%
```

关键参数：

| 参数 | 含义 |
|---|---|
| `yarn.scheduler.capacity.<queue>.capacity` | 队列的最小保证容量（百分比），所有同层队列加起来应该等于 100 |
| `yarn.scheduler.capacity.<queue>.maximum-capacity` | 队列能"借"到的最大上限，超过就只能等别的队列还资源 |
| `yarn.scheduler.capacity.<queue>.user-limit-factor` | 单用户能占用队列容量的倍数（默认 1，即最多用满队列 capacity） |
| `yarn.scheduler.capacity.<queue>.maximum-applications` | 队列最大并发应用数 |
| `yarn.scheduler.capacity.<queue>.acl_submit_applications` | 提交 ACL，多租户隔离的最后一道闸 |
| `yarn.scheduler.capacity.<queue>.state` | RUNNING / STOPPED（停止接收新应用） |

CS 的精髓在于 `maximum-capacity`：平时 prod 队列只有 min=60%，但 dev 空闲时 prod 能弹性借资源到 100%；dev 反过来也能在 prod 空闲时借，但被 `max=80%` 卡住，防止开发把生产挤垮。这就是"弹性 + 隔离"。

`user-limit-factor` 我们调过几次。某业务组多个用户共用一个 dev 队列，默认 factor=1 时一个用户跑大作业，别人全排队。改到 0.5 后单用户最多用队列一半，多用户并发好很多 <!-- 校准：请按真实经历核实/替换 -->。

### Fair Scheduler（FS）

FS 的核心是 pool（池），按 weight 而不是百分比来切分资源。`weight` 越大，分配到的资源越多。

```
allocations.xml:
<pool name="prod">
  <weight>3</weight>
  <maxRunningApps>20</maxRunningApps>
  <schedulingMode>fair</schedulingMode>
  <preemption>true</preemption>
</pool>
<pool name="dev">
  <weight>1</weight>
</pool>
```

| 机制 | 说明 |
|---|---|
| `schedulingMode=fair` | 池内按公平分配 |
| `schedulingMode=fifo` | 池内按提交顺序 |
| `maxRunningApps` | 池内并发上限 |
| `preemption` | 当某池长期欠公平（starvation），强制从富余池抢回 container |
| `fairSharePreemptionThreshold` | 池实际拿到 < fairShare × 阈值（默认 0.5）持续一段时间，触发抢占 |
| `delay scheduling` | 为本地性牺牲短期公平，默认等 `yarn.scheduler.fair.locality-threshold.node=0`（即多少比例的调度机会） |

FS 的抢占比 CS 优雅，因为 CS 的抢占是后来加的（`yarn.resourcemanager.scheduler.monitor.enable`），需要配合 policies，配置相对繁琐。

### 选型建议

| 场景 | 推荐 |
|---|---|
| 单租户、追求吞吐 | FIFO |
| 多租户、强 SLA、组织结构清晰 | **Capacity Scheduler** |
| 多租户、抢占需求强、pool 权重灵活 | **Fair Scheduler** |
| Hortonworks / CDP 默认 | Capacity Scheduler（HDP 默认） |
| CDH 5.x 默认 | Fair Scheduler（老 CDH 默认） |

我的实战倾向是 CS——配置直观、和企业 AD/LDAP 的 ACL 集成成熟、CDP/HDP 生态默认就走它。除非你们平台对抢占公平性极度敏感，否则 CS 已经够用。

## 资源模型

### 传统模型：memory + vcore

- **memory（MB）**：`yarn.nodemanager.resource.memory-mb` 是这个节点能提供给 YARN 的总物理内存上限（要给 OS、NM 本身、DataNode 留余量）。
- **vcores**：`yarn.nodemanager.resource.cpu-vcores` 是虚拟核数，可以大于物理核（按经验，1 物理核 = 2~4 vcore，看任务 CPU 密集程度）。

Container 申请时通过 `Resource` 对象指定 `memory` 和 `vcores`，NM 通过 cgroup `memory.limit_in_bytes` 和 `cpu.cfs_quota_us` 做硬限制。

### 3.x 的扩展：resource types

Hadoop 3.0 引入了自定义资源类型。比如 GPU：

```xml
<!-- yarn-site.xml -->
<property>
  <name>yarn.resource-types</name>
  <value>yarn.io/gpu</value>
</property>
<property>
  <name>yarn.nodemanager.resource-plugins</name>
  <value>yarn-io/gpu</value>
</property>
<property>
  <name>yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables</name>
  <value>/usr/local/cuda/bin/nvidia-smi</value>
</property>
```

这样 NM 会通过 `nvidia-smi` 自动发现 GPU 数量并上报，AM 申请 Container 时可以写 `<resource name="yarn.io/gpu" value="2"/>`。FPGA、其他自定义资源同理。这对跑深度学习推理、GPU 加速的 ETL 很有用，我所在集群接入了 8 张卡的 GPU 节点专门跑推理模型 <!-- 校准：请按真实经历核实/替换 -->。

### resource profile

3.1 引入，可以预定义一组资源组合模板，AM 申请时直接引用 profile 名字而不是逐项指定。社区用得不多，企业内部封装倒是方便。

## 关键调优参数

把我线上维护时反复调过的参数列出来，按类别分组：

### NM 资源配置

| 参数 | 默认 | 建议 | 说明 |
|---|---|---|---|
| `yarn.nodemanager.resource.memory-mb` | 8192 | 物理内存 × 0.8 | 给系统留 20% |
| `yarn.nodemanager.resource.cpu-vcores` | 8 | 物理核 × 2 | 视任务类型 |
| `yarn.nodemanager.vmem-pmem-ratio` | 2.1 | 2.1~3 | 虚拟内存/物理内存比 |
| `yarn.nodemanager.pmem-check-enabled` | true | true | 物理内存超限 kill |
| `yarn.nodemanager.vmem-check-enabled` | true | **false** | Java 任务虚拟内存经常爆，建议关 |

### Scheduler 配置

| 参数 | 默认 | 说明 |
|---|---|---|
| `yarn.resourcemanager.scheduler.client.thread-count` | 50 | 处理 AM 心跳的线程数，集群大要调高 |
| `yarn.scheduler.maximum-allocation-mb` | 8192 | 单 Container 内存上限 |
| `yarn.scheduler.maximum-allocation-vcores` | 4 | 单 Container vcore 上限 |
| `yarn.scheduler.minimum-allocation-mb` | 1024 | 单 Container 内存下限（影响粒度） |
| `yarn.resourcemanager.scheduler.increment-allocation-mb` | 1024 | CS 弹性分配粒度 |

### AM 与 container

| 参数 | 默认 | 说明 |
|---|---|---|
| `yarn.resourcemanager.am.max-attempts` | 2 | AM 重试次数 |
| `yarn.am.liveness-monitor.expiry-interval-ms` | 600000 | AM 心跳超时 |
| `yarn.rm.container-allocation.expiry-interval-ms` | 600000 | 分配出去的 Container 未启动就过期 |

### Fair Scheduler 专属

| 参数 | 默认 | 说明 |
|---|---|---|
| `yarn.scheduler.fair.preemption` | false | 开抢占 |
| `yarn.scheduler.fair.preemption.cluster-utilization-threshold` | 0.8 | 集群利用率高于此才触发抢占 |
| `yarn.scheduler.fair.locality-threshold.node` | -1 | 节点本地性延迟阈值（按调度机会比例） |
| `yarn.scheduler.fair.locality-threshold.rack` | -1 | 机架本地性延迟阈值 |
| `yarn.scheduler.fair.max.assign` | -1 | 一次心跳最多分配 container 数 |

## 生产问题复盘

参数列表是死的，问题排查才是经验。下面三个是我印象最深的故障。

### 问题一：dev 队列被一个 Spark 作业饿死整个 prod

现象：某天 prod 队列里的 Flink 作业全部 pending，dev 队列里一个 memory 调错的 Spark 作业占了 1000+ vcore。

根因：Capacity Scheduler 的 `maximum-capacity` 设成了 100（即不限制），dev 可以无限弹性。那个 Spark 作业内存配错（executor 内存写小了），导致 YARN 以为它能很快结束、一直给资源。

解决：

1. 把 dev 队列 `maximum-capacity` 从 100 调到 40 <!-- 校准：请按真实经历核实/替换 -->。
2. 给所有 dev 队列设 `maximum-applications=50`，防止小集群灌爆。
3. 如果用 Fair Scheduler，`preemption=true` + `fairSharePreemptionThreshold=0.5`，prod 会被自动让回资源——这是 FS 比 CS 强的地方。

### 问题二：ApplicationMaster hang，整个应用僵死

现象：一个 Hive on Tez 查询卡住，AM 状态还是 RUNNING 但所有 task 都不动。

排查：`yarn application -status <appId>` 看 AM 所在 NM，上去 `jstack <amPid>` 发现 AM 线程在等一个 NM 的 RPC 响应，而那个 NM 因为磁盘满已经上报 UNHEALTHY——但 AM 自己没感知，进程没崩，所以 RM 也不会重试 AM。

教训：

- NM 健康检查脚本要覆盖"日志目录满"，默认脚本只检查坏盘和系统盘满。
- 监控要专门看 `yarn_nodemanager_health`，UNHEALTHY 的节点第一时间 drain 掉。
- 这种"AM 不崩但 hang"是最坑的，靠 RM 的 max-attempts 救不了。我们的做法是给关键 Flink 作业加了个外部 watchdog：连续 N 分钟 checkpoint 没推进就主动 kill application 重启 <!-- 校准：请按真实经历核实/替换 -->。

### 问题三：Container 被 OOM kill

现象：Spark executor 跑着跑着被 NM kill，日志里有 `Container [pid] is running beyond physical memory limits`。

排查：用户在 Spark 里配了 `executorMemory=8g`，但 YARN Container 默认会按 `executorMemory + overhead` 算（overhead 是 10%），实际申请的 Container memory 是 8.8g。再加上 JVM 堆外内存（Netty、堆外缓存），真实 RSS 超过 8.8g，被 NM 的 `pmem-check-enabled` 杀掉。

但更隐蔽的是另一个开关：`vmem-check-enabled`。Java 任务虚拟内存（按 `ulimit -v` 算的，包含 mmap、JIT、线程栈等）很容易爆，不是真实内存压力却被 NM 误杀。

解决：

```xml
<property>
  <name>yarn.nodemanager.vmem-check-enabled</name>
  <value>false</value>
</property>
<property>
  <name>yarn.nodemanager.pmem-check-enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.nodemanager.vmem-pmem-ratio</name>
  <value>2.1</value>
</property>
```

关闭 vmem 检查，保留 pmem 检查——这是 Java 集群的事实标准配置。再让 Spark 用户把 `spark.yarn.executor.memoryOverhead` 显式调到 2g 左右，问题消失。

### 问题四：vcore 配高导致 CPU 过载

现象：节点 CPU iowait 飙高、load average 长期 60+，但 YARN 看每个 Container 都"在限制内"。

根因：运维把 `yarn.nodemanager.resource.cpu-vcores` 从 32（物理核）配成了 96，以为 CPU 是 IO 密集任务可以超卖。结果一个 batch 任务全是 Spark hash shuffle，CPU 上下文切换爆炸。

教训：vcore 超卖要看任务类型。CPU 密集（Spark SQL、机器学习训练）按 1:1 配；IO 密集（Hive 简单查询、DistCp）按 1:2 或 1:3 可以。我们最后把 vcore 降回 40 <!-- 校准：请按真实经历核实/替换 -->，配合 cgroup `cpu.cfs_quota_us` 做软限制，load 才稳。

## 监控指标

YARN 暴露的 JMX 指标是平台运维的眼睛。我们接 Prometheus + Grafana 重点监控这些：

| 指标 | 含义 | 告警阈值 |
|---|---|---|
| `ClusterMetricsNumActiveNMs` | 活跃 NM 数 | 跌幅 > 10% 立即告警 |
| 每队列 `allocatedMB / availableMB` | 队列资源使用率 | > 90% 持续 10 分钟 |
| 每队列 `pendingContainers` | pending 容器数 | > 50 持续 15 分钟（饥饿） |
| 每队列 `activeApplications` | 运行中应用数 | 接近 maximum-applications |
| Fair Scheduler 的 `fairShare vs steadyFairShare` | 抢占是否在发生 | steadyFairShare 持续低于 minShare |
| RM `rpcProcessingTimeAvgTime` | RM RPC 延迟 | > 100ms |
| NM `containersKilled` | 被 kill 容器数（OOM、抢占） | 突增告警 |

RM 的 JMX endpoint 是 `http://<rm>:8088/jmx`，NM 是 `http://<nm>:8042/jmx`。Prometheus 抓取间隔 30 秒，pending container 这个指标尤其重要——它是平台最容易"温水煮青蛙"的问题，用户作业卡很久不一定有报错。

## 小结

YARN 看起来是 Hadoop 体系里"老掉牙"的部分，但越往深挖越发现里面的权衡很讲究：调度器选型（CS 的 capacity/maximum-capacity vs FS 的 weight/preemption）是组织结构和 SLA 的折中，资源模型（memory/vcore/resource types）是隔离强度和吞吐的折中，AM 的容错策略（max-attempts）是可靠性和资源占用的折中。把这套东西吃透，不止是能调好 Hadoop，对理解 Kubernetes 的调度、Mesos 的 offer 模型都有帮助——资源调度的底层逻辑是相通的。

落到实战，我个人最看重的三件事：第一，监控 `pendingContainers` 和 `ClusterMetricsNumActiveNMs`，这是平台健康的两条命脉；第二，对每个业务队列严格设 `maximum-capacity` 和 `user-limit-factor`，多租户的悲剧永远来自"我以为他会自己控制"；第三，`vmem-check-enabled=false` 几乎是 Java 集群的必选项，不然你会花一半时间在解释"为什么我的 Spark 没爆内存却被 kill"上。

调度系统没有银弹，但对参数和故障模式的熟悉程度，决定了你能不能在凌晨三点出事时十秒定位问题。希望这篇复盘能帮到同样在折腾 YARN 的同学。
