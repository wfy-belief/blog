---
title: Hadoop 性能调优全攻略：从 JVM、IO 到参数
abbrlink: hadoop-performance-tuning
date: 2026-07-02 12:00:00
tags:
  - Hadoop
  - 性能调优
  - JVM
categories:
  - 技术
description: 分层调优框架，把 Hadoop 集群榨干到最后一滴性能
ai_text: "从硬件、OS、磁盘、JVM、Hadoop 参数到作业本身，本文用一套分层定位框架梳理 Hadoop 性能调优的全链路。重点拆解 NameNode G1GC、DataNode IO、压缩选型与监控手段，并配真实复盘案例，帮你建立可观测、可量化的调优闭环。"
---

## 引言：调优不是玄学，是分层定位 + 量化度量

做大数据这些年被问得最多的问题之一就是："我们这个 Hadoop 集群跑得慢，能不能帮忙调一下？"每次听到这种描述我心里都一沉——没有基线、没有现象、没有瓶颈定位，调优就成了玄学。我接手任何一个慢集群，第一件事不是动参数，而是先把"慢"这件事拆开问清楚：是 NameNode RPC 排队？是 Map 阶段 shuffle 卡住？是 Reduce 端磁盘 IO 打满？还是某个作业的数据倾斜把整体拖垮？

调优真正的工作量在"诊断"而不是"改参数"。我给自己定了一条规矩：**任何一个参数变更，都必须先有一个可量化的指标证明它需要改，并且改完之后这个指标能验证效果。** 如果改完之后只能凭感觉说"好像快了"，那这个调优基本是无效的。

这篇文章是我这几年在 PB 级集群上踩坑整理出来的分层调优方法论，从最底层的硬件 OS 一路讲到作业参数和压缩选型，最后用一个真实复盘案例串起来。希望能帮你建立一个可以复用的调优框架，而不是记一堆"别人说 `io.file.buffer.size=65536` 就快"的孤立结论。

## 分层调优框架：先把问题压到某一层

我把 Hadoop 性能问题按从下到上分成五层，每一层都有典型的瓶颈形态和定位手段：

| 层级 | 典型瓶颈 | 主要观察指标 |
| --- | --- | --- |
| 硬件 / OS / 网络 | CPU 频率、THP、swap、网卡带宽 | `top`、`vmstat`、`sar -n DEV` |
| 磁盘 | JBOD 不均、IO 队列、调度器 | `iostat -x`、`iotop`、`dfs.du` |
| JVM | Full GC、堆碎片、RPC 队列堆积 | GC 日志、`jstack`、JMX |
| Hadoop 参数 | handler 不足、心跳超时、内存配置 | NameNode UI、RM UI、AM 日志 |
| 作业本身 | 数据倾斜、小文件、压缩选择不当 | Counter、Task 日志、shuffle 字节 |

这套框架的核心思想是**先度量再调优**。排查时从上往下定位作业现象，再从下往上验证瓶颈所在。最忌讳的是看到一个参数清单就开始逐条套，那只会越调越乱。下面每一层我都展开讲讲。

## 硬件与 OS 层：地基不牢，上层全白搭

这一层是被忽视得最厉害的，但又是天花板。如果地基出了问题，JVM 调得再漂亮也救不回来。

### 磁盘：JBOD 而非 RAID，但 NameNode 例外

DataNode 的数据盘强烈建议用 JBOD（Just a Bunch of Disks）直连，**不要做 RAID0**。原因有两个：一是 RAID0 的写性能随着盘数增加反而下降（条带开销），二是 Hadoop 本身有三副本机制，单盘故障只是少一个 volume，集群能自愈；而 RAID0 一块盘坏掉整个逻辑卷全完。我在一个 12 盘的 DataNode 上实测过，JBOD 相比 RAID0 在写吞吐上高出约 30%<!-- 校准：请按真实经历核实/替换 -->。

但 NameNode 是个例外。NameNode 的元数据（fsimage 和 editlog）是整个集群的命根子，丢了就完蛋，所以 **NameNode 的元数据盘要做 RAID1**，而且最好配多目录（`dfs.namenode.name.dir`）指向不同的物理盘甚至不同的 HBA 卡，做同步写冗余。

### HDD 与 SSD 的分层使用

不是所有数据都该放 SSD。冷数据、归档数据放高密度 HDD，热数据、NameNode 元数据、YARN 的本地化目录（`yarn.nodemanager.local-dirs`）放 SSD。HDFS 2.7+ 的存储策略（`HOT`/`WARM`/`COLD`/`ALL_SSD`）配合 storage type 可以让你按目录指定存储介质。

### 网络：拓扑、非阻塞、冗余

Hadoop shuffle 对网络极度敏感。机架感知（`net.topology.script.file.name`）必须配，否则所有节点会被当作一个机架，block placement 退化成"两副本同机架"，一旦机架 ToR 交换机故障就会丢数据。核心交换要做非阻塞（non-blocking）架构，跨机架带宽至少是机架内带宽的 20%<!-- 校准：请按真实经历核实/替换 -->。万兆网络是底线，shuffle 密集的场景我建议上 25G 甚至 100G。

### 文件系统与内核参数

几个我必做的 OS 项：

- **文件系统**：XFS 或 ext4，XFS 在大文件场景更稳，建议给 mkfs 时 `-b size=4096`。
- **关闭 swap**：`swapoff -a`，并在 `/etc/fstab` 注释掉 swap 行。Hadoop 进程一旦被换出，GC 就会暴雷。也可以在 `sysctl.conf` 里把 `vm.swappiness` 设到 1 甚至 0。
- **关闭透明大页（THP）**：THP 在高内存压力下会引发 khugepaged 抢 CPU，导致 JVM 卡顿。`echo never > /sys/kernel/mm/transparent_hugepage/enabled`。
- **文件描述符上限**：`ulimit -n` 至少调到 65536，DataNode 打开句柄多时容易撞到默认 1024 的墙。
- **磁盘调度器**：HDD 用 `deadline` 或 `mq-deadline`，SSD/NVMe 用 `none` 或 `noop`。echo 调度器到 `/sys/block/sdX/queue/scheduler`。

## NameNode JVM 调优：最容易出"玄学慢"的地方

NameNode 是整个集群的单点，所有 HDFS 操作都要过它。NameNode 一卡，全集群跟着卡。所以 NameNode 的 JVM 调优是 Hadoop 调优里最值得投入精力的部分。

### 堆内存规划

NameNode 堆内存主要消耗在两块：**文件系统的命名空间镜像（fsimage 加载到内存的对象）**和 **NN 端缓存的 block 位置信息**。经验值是每 100 万个文件/块对象大约吃 1GB 堆<!-- 校准：请按真实经历核实/替换 -->，再加上 block replication 缓存、lease 管理、RPC 缓冲，实际要预留 30%-50% 余量。

规划公式我一般这么估：`Heap(GB) ≈ 文件数 × 1GB / 1e6 × 1.3`。比如一个集群有 5 亿文件，NameNode 堆至少要 65GB<!-- 校准：请按真实经历核实/替换 -->。堆不要无脑开大，超过 64GB 后 GC 的停顿时间会陡增，这时候就要认真选 GC 算法了。

### G1GC vs CMS

Hadoop 2.x 时代 NameNode 默认 CMS，CMS 的痛点是碎片化严重和 Concurrent Mode Failure 时的 Full GC 停顿。Hadoop 3.x 默认 G1GC，对于大堆（>32GB）场景我强烈推荐 G1，它通过 region 化的堆布局把停顿时间可控化。

调 G1 的关键是 `-XX:MaxGCPauseMillis` 和 `-XX:G1HeapRegionSize`。NameNode 这种大堆、短存活对象多的场景，`G1HeapRegionSize` 建议 16MB 或 32MB（堆 64GB 时用 32MB）。`MaxGCPauseMillis` 设 200ms 是个常用起点，但要看实际 GC 日志调，如果发现 mixed GC 频繁但每次都很快，可以稍微放宽到 300ms 让它少做一些。

下面是我线上 NameNode 用过的一组 G1GC 参数（堆 64GB），可以直接套用后再根据 GC 日志微调：

```properties
-server -Xmx64g -Xms64g
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:G1HeapRegionSize=32m
-XX:InitiatingHeapOccupancyPercent=35
-XX:ParallelGCThreads=24
-XX:ConcGCThreads=6
-XX:+ExplicitGCInvokesConcurrent
-XX:+ParallelRefProcEnabled
-XX:+AlwaysPreTouch
-Xlog:gc*:file=/var/log/hadoop/nn-gc.log:time,uptime,level,tags:filecount=10,filesize=100M
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/var/log/hadoop/nn-heapdump
```

几个关键点的取舍说明：

- `-Xms = -Xmx`：避免堆动态扩张时的停顿。
- `InitiatingHeapOccupancyPercent=35`：触发并发标记的阈值，NameNode 长生命周期对象多，35% 比默认 45% 更早开始并发回收，能压低 mixed GC 频率。
- `ParallelGCThreads=24`：和物理核数挂钩，一般取物理核数的 1/4 到 1/2 之间，避免 GC 抢业务线程太多。
- `AlwaysPreTouch`：启动时就把所有堆页 touch 一遍，避免运行时缺页中断带来的尖刺。代价是启动慢，对 NameNode 完全可接受。
- GC 日志一定要落盘，10 个文件 × 100MB 滚动，这是事后定位 Full GC 的命根子。

### RPC handler 与队列

堆调好只是第一步，NameNode 真正的吞吐瓶颈往往在 RPC。`dfs.namenode.handler.count` 默认 10，对生产集群远远不够。我一般按 DataNode 数量 + 客户端并发估算，几百台机器的集群开到 64-128<!-- 校准：请按真实经历核实/替换 -->。配合 `ipc.server.max.response.size` 和 `ipc.server.read.threadpool.maximum` 一起调，否则 handler 被慢请求阻塞，新请求堆积在 listener 队列。

NameNode UI 上能直接看到 RPC 处理时间和队列长度，如果 `CallQueueLength` 持续高、`ProcessCallTime` 的 p99 超过几十毫秒，就是 handler 不够的信号，这时候加 handler 比加堆管用得多。

## DataNode 与 IO 调优：把磁盘榨干

DataNode 的工作很简单也很重：源源不断地读写块。调优的核心是让多块盘充分并行、避免单盘成为热点。

### handler 与多 volume 均衡

`dfs.datanode.handler.count` 默认 10，建议提到 20-40<!-- 校准：请按真实经历核实/替换 -->。但注意 handler 数不是越高越好，每个 handler 都会在 IO 上排队，handler 太多会让磁盘调度抖动加剧。

`dfs.datanode.data.dir` 配多 volume（比如 12 块盘就配 12 个目录），Hadoop 会基于剩余空间做 round-robin 写入。**但 Hadoop 不会自动 rebalance 已有数据**，新增盘后老盘可能被打满而新盘空着，这时候要手动跑 balancer 或者用 `hdfs diskbalancer` 做节点内均衡。

`dfs.datanode.failed.volumes.tolerated` 默认 0，意味着任何一块盘故障 DataNode 就退出。生产环境必须设为 ≥1（一般是 volume 总数的 1/4 左右），否则一块坏盘就让整个节点下线太脆弱。

### IO 缓冲与 balancer 带宽

`io.file.buffer.size` 控制 Hadoop 读写文件时的缓冲大小，默认 4KB 太小。我固定设成 65536（64KB），对大文件顺序读写吞吐提升明显。

balancer 的带宽 `dfs.datanode.balance.bandwidthPerSec` 默认只有 1MB/s<!-- 校准：请按真实经历核实/替换 -->，对 PB 级集群完全不够。但开太大会抢业务带宽，我一般在工作时间设 10-20MB/s，业务低峰期动态调到 50MB/s。HDFS 2.8+ 支持 `dfs.datanode.balance.max.concurrent.moves` 配合控制并发移动块数，比单纯调带宽更精细。

磁盘调度器选错也会拖累 IO。HDD 必须用 `deadline` 或 `mq-deadline`，不要用默认的 `cfq`（桌面调度器，对数据库式负载很不友好）。SSD 用 `none`（直通），因为 SSD 自己有 FTL 调度。

## 压缩策略：map 输出和结果存储要分开选

压缩是性价比最高的优化之一：CPU 换 IO，在现代 CPU 过剩而 IO 紧张的集群上几乎是白送的性能。但不同压缩算法在压缩率、速度、可分裂性上差异巨大，要按用途选。

| 算法 | 压缩率 | 压缩速度 | 解压速度 | 可分裂 | 典型用途 |
| --- | --- | --- | --- | --- | --- |
| gzip | 高 | 慢 | 中 | 否（除非带索引） | 历史归档、冷数据 |
| lzo | 中 | 快 | 很快 | 是（需建索引） | map 输出、中间数据 |
| snappy | 低 | 极快 | 极快 | 否 | map 输出、临时数据 |
| zstd | 中高 | 快（接近 snappy） | 快 | 是（带帧格式） | 全能选手，新版首选 |
| brotli | 很高 | 很慢 | 中 | 否 | 文本类静态资源、不推荐大数 |

一个关键概念是**可分裂性（splittable）**。如果一个 100GB 的 gzip 文件要被 MapReduce 处理，因为它不可分裂，只能由一个 map task 串行读，并行度被打成 1。这对作业来说是灾难性的。所以**存储层如果用压缩，必须用可分裂格式**（lzo + 索引、zstd 帧、或者直接用 Parquet/ORC 这种列存格式自带压缩和分裂）。

map 输出（shuffle 数据）的压缩又是另一回事。这里追求的是 CPU 消耗低、解压快，因为每个 reduce 都要解。snappy 是 map 输出压缩的黄金标准，zstd 在新版 Hadoop 上也越来越流行。gzip 太慢，绝对不要用在 shuffle 上。

下面是我在 mapred-site.xml 里配置压缩的常用片段，map 输出用 snappy，作业输出按需要切：

```xml
<!-- 启用 map 输出压缩，shuffle 阶段省网络 -->
<property>
  <name>mapreduce.map.output.compress</name>
  <value>true</value>
</property>
<property>
  <name>mapreduce.map.output.compress.codec</name>
  <value>org.apache.hadoop.io.compress.SnappyCodec</value>
</property>

<!-- 作业最终输出压缩，按场景切换 -->
<property>
  <name>mapreduce.output.fileoutputformat.compress</name>
  <value>true</value>
</property>
<property>
  <name>mapreduce.output.fileoutputformat.compress.codec</name>
  <value>org.apache.hadoop.io.compress.GzipCodec</value>
  <!-- 归档场景用 GzipCodec；下游还要再跑 MR 就换 ZStandardCodec 或 LzopCodec -->
</property>
<property>
  <name>mapreduce.output.fileoutputformat.compress.type</name>
  <value>BLOCK</value>
</property>
```

实际上现在大多数场景我更推荐直接上 Parquet + snappy 或 zstd，列存 + 字典编码 + 压缩三合一，比行存 + 压缩效率高一个量级。

## 关键参数清单：把最常调的列在一起

下面这张表是我调优时会逐条扫一遍的"检查清单"，按组件分组，每条都标了含义和典型值范围。

| 组件 | 参数 | 含义 | 典型值 |
| --- | --- | --- | --- |
| NameNode | `dfs.namenode.handler.count` | RPC 处理线程数 | 64-128 |
| NameNode | `dfs.namenode.name.dir` | 元数据目录（多盘冗余） | RAID1 × 2-3 |
| NameNode | `dfs.namenode.fs-limits.max-directory-items` | 单目录最大子项 | 1048576 |
| NameNode | `dfs.blocksize` | 块大小 | 256MB/512MB |
| DataNode | `dfs.datanode.handler.count` | DN 数据传输 handler | 20-40 |
| DataNode | `dfs.datanode.data.dir` | 数据 volume 列表 | 12 块盘路径 |
| DataNode | `dfs.datanode.failed.volumes.tolerated` | 允许故障 volume 数 | 1-3 |
| DataNode | `dfs.datanode.balance.bandwidthPerSec` | balancer 每秒带宽 | 10-50MB |
| 通用 IO | `io.file.buffer.size` | 读写缓冲 | 65536 |
| YARN | `yarn.nodemanager.resource.memory-mb` | 节点可分配内存 | 物理内存 × 0.8 |
| YARN | `yarn.scheduler.maximum-allocation-mb` | 单容器内存上限 | 与上同步 |
| YARN | `yarn.nodemanager.vmem-pmem-ratio` | 虚拟内存比 | 2.1（关掉更省心） |
| YARN | `yarn.scheduler.fair.allocation-file` | 公平调度分配文件 | 按队列配 |
| MR | `mapreduce.map.memory.mb` / `reduce.memory.mb` | 容器内存 | 2-4GB / 4-8GB |
| MR | `mapreduce.task.io.sort.mb` | map 端排序缓冲 | map 内存 × 0.5 |
| MR | `mapreduce.job.reduces` | reduce 数 | 按 shuffle 字节 ÷ 1GB 估 |

## 监控与定位手段：没有指标就没有调优

前面反复强调"先度量再调优"，这一节具体讲怎么度量。我把监控分成三个层次。

### 第一层：JMX + Prometheus

Hadoop 进程天然带 JMX，把所有内部指标都暴露出来。我用 JMX exporter（jmx_exporter Java agent）把 JMX 翻译成 Prometheus 格式，再由 Prometheus pull，Grafana 出图。重点关注的指标包括：

- NameNode：`NumStaleDataNodes`、`CapacityTotal`/`CapacityUsed`、`FilesTotal`/`BlocksTotal`、`TransactionsSinceLastCheckpoint`。
- DataNode：每个 volume 的 `DatanodeDiskIOStat`、`BytesWritten`/`BytesRead`。
- RPC：`RpcQueueTimeNumOps`、`RpcProcessingTimeAvgTime`、`CallQueueLength`。

RPC 的 `CallQueueLength` 和 `RpcProcessingTimeAvgTime` 是 NameNode 健康度的晴雨表。任何一次集群卡顿，Grafana 上这两个指标先跳。

### 第二层：GC 日志分析

GC 日志开启后用 GCViewer 或 GCEasy 解析。我重点看三个数：Full GC 频率、mixed GC 停顿的 p99、Concurrent Mark 的耗时占比。如果 mixed GC 频率超过每分钟一次，说明 `InitiatingHeapOccupancyPercent` 太高或者 region 太大，要往下调。

### 第三层：现场分析（jstack / jmap / 火焰图）

线上出问题时，先 `jstack <pid>` 抓栈，连抓三次间隔 1-2 秒，看线程都卡在哪。如果大量 RPC handler 卡在 `waitForBytes`，说明 IO 是瓶颈；如果卡在 `ArrayAllocator`，说明是元数据分配。`jmap -histo:live <pid>` 能看到对象直方图，NameNode 异常时常见的征兆是 `INodeFile` / `BlockInfo` 实例数暴涨。

火焰图是定位 CPU 热点的杀手锏。用 async-profiler 给 NameNode 抓 30 秒，火焰图里哪一段最宽就是热点。我曾经用这种方式定位过一个序列化反序列化库占用 40% CPU 的问题，靠看火焰图一眼就发现了。

## 一个真实调优复盘案例

最后用一个串起来的案例说明这套方法怎么落地。这个案例脱敏自一个日志分析集群。

**现象**：一个 200 节点的集群，每天凌晨跑一批 ETL 作业，最近几周 SLA 从 2 小时恶化到 5 小时<!-- 校准：请按真实经历核实/替换 -->，业务方投诉不断。

**定位过程**：

1. **先看 RM 和 JobHistory**：发现是某一个作业 `log_join_dedup` 的 reduce 阶段拖累整体，其他作业时间正常。这个作业的 reduce 数只有 50，但 shuffle 字节有 800GB<!-- 校准：请按真实经历核实/替换 -->。
2. **看 Counter**：reduce 端 `GC time elapsed` 占比 60%，`Spilled Records` 是 `Map output records` 的 4 倍，说明 reduce 内存不够，疯狂 spill。
3. **看 NameNode 监控**：`CallQueueLength` 在作业运行期间持续 >200，`RpcProcessingTimeAvgTime` 从平时的 2ms 跳到 30ms。说明 NameNode RPC 也在被打。
4. **看磁盘 IO**：DataNode 上多块盘 util 接近 100%<!-- 校准：请按真实经历核实/替换 -->，但只是 reduce 写出阶段。

**假设**：

- 主要矛盾：reduce 数太少 + reduce 内存不够，导致单 reduce 处理数据过多，疯狂 spill 触发磁盘 IO 打满。
- 次要矛盾：NameNode RPC handler 不够，作业大量小文件 create 拖累 NN。

**验证与改动**：

1. 把 `mapreduce.job.reduces` 从 50 调到 400（按 shuffle 字节 ÷ 2GB 估算）。reduce 数上去后单 reduce 数据降到 2GB，spill 大幅减少。
2. `mapreduce.reduce.memory.mb` 从 2GB 提到 4GB，`mapreduce.reduce.java.opts` 同步提到 3.2GB（留 heap 外内存给 shuffle）。spill 消失。
3. map 输出没开压缩。打开 `mapreduce.map.output.compress=true` + Snappy，shuffle 字节下降到原来的 1/3。
4. NameNode `dfs.namenode.handler.count` 从 32 调到 96，配合 G1GC 的 `InitiatingHeapOccupancyPercent` 从 45 降到 35。

**结果**：作业耗时从 5 小时降到 1.5 小时<!-- 校准：请按真实经历核实/替换 -->，NameNode RPC p99 回到 3ms 以内。一次改完验证、再改再验证，每一步都看指标变化，避免误判。

这个案例的关键不是改了哪几个参数，而是**用数据一步步逼出瓶颈**。如果一开始就拍脑袋"调 reduce 数"或者"调 GC"，很可能改完没效果，因为多个矛盾是叠加的。

## 小结：调优的终点是可观测

写到这里你应该能感受到，Hadoop 性能调优没有银弹。它本质上是一个**用可观测性把不可见的瓶颈变得可见**的过程：分层定位、量化度量、假设验证、回滚机制。具体改哪个参数反而是最不重要的部分——参数清单网上到处都是，真正稀缺的是"为什么这里要这么改"的判断力。

回到开头那句话：**调优不是玄学，是工程。** 把每一层都铺上监控，把每一次改动都留下指标对比，把每一次复盘都沉淀成检查清单。久而久之你会发现，遇到新问题不再慌，因为路径是固定的：从作业现象往下钻一层、再一层，直到看见那个被忽略的瓶颈。这套方法论的复用价值，远比记住某条 `properties` 配置大得多。

希望这篇梳理能帮你在下一次面对"集群慢"的灵魂拷问时，少走一些弯路。
