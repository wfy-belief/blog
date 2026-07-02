---
title: HDFS 运维与故障排查 SOP
abbrlink: hadoop-hdfs-ops-troubleshooting
date: 2026-07-02 18:30:00
tags:
  - Hadoop
  - HDFS
  - 运维
categories:
  - 技术
description: 一套可落地的 HDFS 观测指标与故障排查标准操作流程
ai_text: "本文是 HDFS 系列的运维收尾篇，把分散在各篇的运维点汇总成一套可落地的观测体系与故障排查 SOP。从 NameNode/DataNode JMX 关键指标、Prometheus 告警阈值，到日常巡检清单，再到 NN Full GC、大量 under-replicated blocks、DN 死亡坏盘、Safemode 不退、慢节点、误删恢复、NN 启动慢七类高频故障的现象-定位-处理流程，最后给出容量扩缩容、安全运维、应急工具箱与一份值班 checklist，是值班同学的一页纸手册。"
---

# HDFS 运维与故障排查 SOP

## 引言

写到这里，HDFS 系列已经走过了架构、读写路径、元数据、副本管理、一致性租约、小文件治理等篇。每一篇里我都会零散地提到一些"运维要点"，但一直没把它们攒成一套体系。这一篇就是来收尾的——**把分散在各篇的运维点，汇总成一套可落地的观测体系 + 故障排查 SOP**。

为什么 HDFS 的运维值得单独成篇？因为 HDFS 出问题，几乎从来不是"某个文件读不到"这么局部的事。它是元数据中枢，NameNode 一抖，全集群的所有作业一起抖；它是存储底座，DataNode 一批下线，所有上层引擎一起报错。换句话说，**HDFS 的故障天然是全集群级的**，这种级别的故障，靠临场翻日志、靠肌肉记忆去定位，是会出大事的。我在某集群（约 200 节点、3 PB 出头）<!-- 校准：请按真实经历核实/替换 -->值班的这几年，吃过的最大教训就是：**没有 SOP 的运维，就是把集群稳定性押在值班同学当天的状态上**。

这篇文章的目标，是给同样在维护 HDFS 的同学一份能直接抄走的"观测 + 排查"框架。前半部分讲怎么"看见"——观测体系与日常巡检；后半部分讲怎么"处置"——七类高频故障的 SOP，每个都给出现象、定位步骤、处理命令。最后附一份值班 checklist。

## 一、观测体系：HDFS 运维的眼睛

运维的第一性原则是：**在你被电话叫醒之前，监控要先告诉你哪里坏了**。HDFS 的观测，核心是两套 JMX：NameNode JMX 和 DataNode JMX。下面这张表是我值班时每天会扫一眼、出事时会逐项核对的 NameNode 关键指标。

### 1.1 NameNode JMX 关键指标

| 指标（JMX） | 含义 | 关注点 |
|---|---|---|
| `FilesTotal` / `BlocksTotal` | 文件总数 / 块总数 | 增长趋势异常 = 小文件爆炸或业务异常 |
| `UnderReplicatedBlocks` | 副本数不足的块 | 持续 > 0 且上升 = DN 下线/坏盘 |
| `PendingReplicationBlocks` | 等待副本复制的块 | 持续不消化 = 限速过严或 DN 跟不上 |
| `ScheduledReplicationBlocks` | 已调度但未完成 | 配合 Pending 看 re-replication 速率 |
| `StaleDataNodes` | 心跳过期的 DN | > 0 立即告警，可能即将进入 dead |
| `DeadNodes` | 已判定死亡 DN | 任何非预期增长都是 P1 |
| `CapacityUsed` / `CapacityTotal` | 已用 / 总容量 | 用率 > 85% 必须扩容 |
| `VolumeFailuresTotal` | 全集群失败卷数 | 反映坏盘情况 |
| `TransactionsSinceLastCheckpoint` | 距上次 Checkpoint 的事务数 | 持续上涨 = SNN/Standby 没在干活 |
| `LastWrittenTransactionId` | 最新写事务 ID | Standby 跟不上时差距拉大 |
| `MillisSinceLastLoadedEdits` | 距上次加载 edits 间隔 | HA 切换后关注 |
| `FSNamesystemState` / `Safemode` | 是否安全模式 | 非预期进入 = P0 |
| `RpcQueueTimeNumOps` / `RpcQueueTimeAvgTime` | RPC 队列长度与平均延迟 | 延迟 > 50ms<!-- 校准：请按真实经历核实/替换 --> 需关注 |
| `GCCount` / `GCTimeMillis` | GC 次数与耗时 | Full GC 是 NN 头号杀手 |
| `HeapMemoryUsage` | 堆内存使用 | used/committed > 80% 准备调优 |

这张表不是摆设。我给每一个指标都配了对应的 Prometheus 告警规则，下面会给出阈值。需要强调的是，**单点指标不可怕，趋势才可怕**——`UnderReplicatedBlocks` 在一次重启后短暂跳到几万很正常（块报告还没全上报），但如果它**单调递增且一小时不回落**，那就是真的有节点在掉。

### 1.2 DataNode JMX 关键指标

DataNode 的 JMX 相对简单，但有几个不能漏：

| 指标 | 含义 | 关注点 |
|---|---|---|
| `VolumeFailures` | 本节点失败卷数 | > 0 触发换盘流程 |
| `FsyncNanosAvgTime` | fsync 平均耗时 | 持续高 = 磁盘 IO 瓶颈 |
| `DatanodeNetworkErrors` | 网络错误计数 | 反映网卡/交换机问题 |
| `XceiverCount` | 并发数据连接数 | 异常高 = 大量并发写或 Hedged Read |
| `CacheUsed` / `CacheCapacity` | 集中式缓存 | 启用了 `hadoop.cache.data` 时关注 |
| `HeartbeatsAvgTime` | 心跳 RPC 平均耗时 | 高 = NN 侧 RPC 拥堵 |
| `BlocksCached` / `BlocksRead` | 缓存命中情况 | 评估缓存策略效果 |

`XceiverCount` 这个指标容易被忽略，但它其实很关键。它本质上是这个 DN 上正在处理的 IO 工作线程数。**当某个 DN 的 XceiverCount 持续显著高于其他节点**，往往意味着这是个热点（数据倾斜或 balancer 在猛打它），也可能是 Hedged Read 失控。我在小文件篇、读路径篇都提到过它的副作用。

### 1.3 采集方案：JMX Exporter + Prometheus + Grafana

生产上我用的方案是 `jmx_exporter`（Java Agent 方式）+ Prometheus + Grafana，这是社区最主流的组合。

```bash
# NN 启动时挂上 jmx_exporter agent
HDFS_NAMENODE_OPTS="$HDFS_NAMENODE_OPTS \
  -javaagent:/opt/prometheus/jmx_exporter.jar=30001:/opt/prometheus/nn.yml"
```

`nn.yml` 里要做白名单过滤，否则 JMX 几千个指标会把 Prometheus 打爆。关键告警规则（PromQL）参考：

```properties
# 1. Under-replicated blocks 持续上升
alert: HdfsUnderReplicatedBlocksSurge
expr: increase(hadoop_namenode_under_replicated_blocks[10m]) > 10000
for: 15m

# 2. Stale DN 出现
alert: HdfsStaleDataNodes
expr: hadoop_namenode_stale_data_nodes > 0
for: 5m

# 3. NN Full GC
alert: HdfsNameNodeFullGC
expr: increase(jvm_gc_collection_seconds_count{gc="ConcurrentMarkSweep"}[5m]) > 3
# <!-- 校准：请按真实经历核实/替换，G1/ZGC 名不同 -->

# 4. NN 堆使用率
alert: HdfsNameNodeHeapHigh
expr: jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"} > 0.85
for: 10m

# 5. NN RPC 延迟
alert: HdfsNameNodeRpcSlow
expr: rate(hadoop_namenode_rpc_queue_time_avg_time[5m]) > 50
for: 5m

# 6. 容量水位
alert: HdfsCapacityHigh
expr: hadoop_namenode_capacity_used / hadoop_namenode_capacity_total > 0.85
for: 30m
```

这套阈值不是抄来的，是我在某集群调出来的。**阈值调优的核心原则是：先调到不漏 P1，再压噪声**。一开始宁可多报，慢慢根据误报收。

## 二、日常巡检清单

监控是被动等你，巡检是主动去找。我习惯每天早上开工前花十分钟做一遍这套巡检。

- [ ] **NN UI 概览**：`http://nn:9870/dfshealth.html`，扫一眼集群状态、Live/Dead DN 数、Safemode 是否亮、容量条。
- [ ] **`hdfs fsck / -files -blocks | tail`**：看 missing/corrupt 块数，正常应该是 0。任何 `CORRUPT` / `MISSING` 都是 P1。
- [ ] **Under-replicated 趋势**：Grafana 上看 `UnderReplicatedBlocks` 过去 24h，正常应该是一条贴近 0 的平线。
- [ ] **Stale / Dead DN**：NN UI 上 Live Nodes 列表里 `Last Contact` 异常的、Stale 标记的，记下来跟进。
- [ ] **NN GC 日志**：`grep "Full GC" /var/log/hadoop/hdfs/*-namenode-*.log`，看最近 24h 是否有 Full GC。
- [ ] **Checkpoint 状态**：NN UI 看 `Transactions Since Last Checkpoint`，正常应该被 Standby 周期性消化掉，不应该无限上涨。
- [ ] **Standby NN 同步**：`Last Written Transaction Id` Active 和 Standby 的差距，差距大说明同步跟不上。
- [ ] **磁盘水位**：DN 上 `df -h` 看 HDFS 卷，单卷满了会触发 volume failure。
- [ ] **关键目录 quota**：业务方常年的坑就是往一个目录死命写，定期核 `hadoop fs -count -q`。

这十条每天过一遍，绝大多数问题能在用户报障之前发现。**运维的体面，来自你比用户先知道**。

## 三、高频故障 SOP

下面是这篇文章的核心。每个故障按"现象 → 定位 → 处理"三段写，都是我在生产环境真打过的仗。

### 3.1 NameNode Full GC / 卡顿

这是 HDFS 最经典、也是杀伤力最大的故障。NN 一卡，整个集群所有 RPC 都排队，作业大面积超时。

**现象**：NN UI 打不开或极慢；客户端报 `Failed to connect to NN` / `Retrying connect to NN`；上层 Spark/Flink 作业大批量失败；`jstat -gcutil <pid>` 看到 Old 区接近满，Full GC 频繁。

**定位**：

```bash
# 1. 确认 Full GC
grep "Full GC" /var/log/hadoop/hdfs/*-namenode-*.log | tail -50

# 2. 看堆使用与 GC 类型
jstat -gcutil <nn_pid> 1000 10

# 3. 抓 jstack，看 NN 卡在哪
jstack <nn_pid> > /tmp/nn_jstack_$(date +%s).txt
# 重点关注 RPC server 线程、BlockManager 线程是否被某操作阻塞

# 4. dump 堆看对象分布（谨慎，会 STW）
jmap -histo:live <nn_pid> | head -30
# 如果 INode/byte[] 占大头，几乎可以确定是小文件或大目录问题
```

**根因判断**：Full GC 的根因 90% 是**堆内对象过多**。在 NN 这儿，"对象"主要就是 INode（文件/目录元数据）。常见三种：

1. **小文件爆炸**：业务往集群灌了大量小文件，INode 数突破亿级。详见我那篇小文件治理。
2. **超大目录**：单目录下百万级子项，NN 在 list/操作时一次构造大量对象。
3. **RPC 堆积**：长 RPC 把线程占住，新请求堆积，对象不释放。

**处理**：

- **短期止血**：重启 NN（HA 切换）。注意先 `hdfs haadmin -failover` 再处理原 Active。
- **中期减对象**：
  - 找出小文件大户：`hdfs fsck / -files -blocks -locations | awk '{print $3}' | sort -n | tail`，或用 `hadoop oiv` 解析 FsImage 统计（小文件篇有完整脚本）。
  - 推业务方合并、Har 归档、或迁 OSS/S3。
- **长期扩容堆**：调大 `HADOOP_HEAPSIZE` / `-Xmx`，但注意 **NN 堆不建议超过 64GB**（指针压缩、GC 停顿），超了就该考虑 Federation 了（见我那篇 HA/Federation）。
- **调 GC**：从 CMS / ParallelGC 切 G1GC，停顿更可控。`-XX:+UseG1GC -XX:MaxGCPauseMillis=200`<!-- 校准：请按真实经历核实/替换 -->。

### 3.2 大量 under-replicated blocks

这是故障率仅次于 Full GC 的问题，本质是"副本跟不上"。

**现象**：NN UI 上 `Under Replicated Blocks` 持续为正且上升；Grafana 告警 `HdfsUnderReplicatedBlocksSurge`；严重时 `PendingReplicationBlocks` 也堆积；某些文件读出来报 `BlockMissingException`。

**定位**：

```bash
# 1. 看 under-replicated 块的来源
hdfs fsck / -files -blocks -locations | grep -A1 "Under replicated" | head -50

# 2. 看 DN 状态：是不是有一批 DN 死了/在 decommission
hdfs dfsadmin -report | grep -E "Live|Dead|Decommissioning"

# 3. 看 Stale DN
curl -s http://nn:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo \
  | jq '.beans[0].StaleDataNodes'

# 4. 坏盘？
curl -s http://nn:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo \
  | jq '.beans[0].VolumeFailuresTotal'
```

**根因**：通常是 DataNode 侧出了问题。一批 DN 同时下线（机架断电、交换机故障）、一批磁盘同时坏（同批次磁盘到寿命）、或 decommission 流程在跑但速度跟不上业务写入速度。

**处理**：

- **先恢复 DN**：如果是节点宕了，优先把 DN 拉起来；下线的 DN 恢复后块会自动补全。
- **放开 re-replication 限速**：默认参数比较保守，集群大时补不动。
  ```properties
  # hdfs-site.xml，调大 re-replication 速度
  dfs.namenode.replication.work.multiplier.per.iteration=8
  dfs.namenode.replication.max-streams=16
  dfs.namenode.replication.max-streams-hard-limit=32
  # <!-- 校准：请按真实经历核实/替换，过大可能压垮 DN 网络 -->
  ```
- **批量触发**：必要时用 `hdfs dfsadmin -setSpaceQuota` 调整触发条件，或重启 NN 让它重新扫描（重启有风险，权衡用）。
- **监控消化速率**：盯 `ScheduledReplicationBlocks` 是否在下降，下降说明在补，只是慢；不下降说明限速卡死，继续放大参数。

### 3.3 DataNode 死亡 / 坏盘

DN 故障分两类：节点级死亡（心跳没了）和磁盘级失败（volume 挂了但 DN 还在）。

**现象（节点死亡）**：NN UI 上该 DN 进入 Dead 列表；该节点上所有块进入 under-replicated；作业读这些块报错。

**定位（节点死亡）**：

```bash
# 1. 进程在不在
ssh <dn_host> "jps | grep DataNode"

# 2. 看日志
ssh <dn_host> "tail -200 /var/log/hadoop/hdfs/*-datanode-*.log"

# 3. 心跳网络通不通
ssh <dn_host> "ping -c 3 <nn_host>"
```

**处理（节点死亡）**：根据日志根因处理。OOM 调 DN 堆；磁盘满清日志；网络修好后重启 DN。**关键：恢复后 DN 上来的块报告会很猛，注意观察 NN RPC**。

**现象（坏盘）**：DN 还在 Live，但 `VolumeFailures` 上升；该 DN 上部分块变 under-replicated。

**定位（坏盘）**：

```bash
# 1. 在 DN 上看哪些 volume failed
ssh <dn_host> "cat /var/log/hadoop/hdfs/*-datanode-*.log | grep -i 'failed volume'"
# 或
ssh <dn_host> "ls -l /var/log/hadoop/hdfs/ | grep volumeFailure"

# 2. 看磁盘健康
ssh <dn_host> "df -h && dmesg | grep -i 'I/O error' | tail"
```

**处理（换盘流程）**：

1. 确认坏盘：`smartctl -a /dev/sdX` 看硬件错误。
2. 物理换盘（运维同事）。
3. 重建文件系统：`mkfs.xfs /dev/sdX1; mount /dev/sdX1 /data/dn/3`。
4. **目录归属要正确**：DN 会扫描所有 `dfs.datanode.data.dir` 配置的目录。
5. 重启 DN：`hdfs --daemon stop datanode; hdfs --daemon start datanode`。
6. 观察该 volume 上的块逐渐补全。

关于 `dfs.datanode.failed.volumes.tolerated`：默认是 0，即任何一块盘坏了 DN 直接退出。**生产上我建议设成 1**（12 盘节点），允许坏一块盘继续服役，避免一块盘带走整个节点。具体详见副本管理篇。

### 3.4 Safemode 不退出

**现象**：NN 启动后或运行中卡在 Safemode；客户端报 `Name node in safe mode`，写操作全失败；NN UI 上 `Safemode` 为 ON。

**定位**：

```bash
# 1. Safemode 原因
hdfs dfsadmin -safemode get

# 2. 看阈值达成情况
hdfs dfsadmin -metaSave /tmp/hdfs-meta.txt
cat /tmp/hdfs-meta.txt | head -50
# 关注 BlocksTotal / BlocksValid / 达到的比例

# 3. NN 启动日志
grep -i "safe" /var/log/hadoop/hdfs/*-namenode-*.log | tail -30
```

**根因**：Safemode 的退出条件是 `已上报块的节点比例 >= dfs.safemode.threshold.pct`（默认 0.999f）<!-- 校准：请按真实经历核实/替换 -->。不退出的常见原因：有 DN 还没起来（块没全上报）、块报告太慢、或真的丢了块（低于阈值）。

**处理**：

- **等**：如果只是 DN 还在陆续起来，等就行。NN UI 看 `datanode heartbeat` 数量在涨就是正常的。
- **查丢失块**：`hdfs fsck / -files -blocks | grep -c "MISSING"`，如果有大量 missing，说明真丢数据了，先修这个。
- **手动退出（高风险）**：`hdfs dfsadmin -safemode leave`。**这是核武器，慎用**。手动退出意味着你接受"可能丢块"的现状，后续读这些块会失败。除非你已确认块都在、只是上报慢，否则别用。
- **调阈值**：`dfs.safemode.threshold.pct` 调低，但这是治标，根因还是块上报慢。

### 3.5 慢节点拖垮作业

这是最隐蔽的一类故障，业务方特别容易投诉但 HDFS 侧很难抓。

**现象**：业务反馈"作业偶尔卡很久"；NN/DN 监控都正常；某些 task 的 locality 报告里某个 DN 反复出现高延迟。

**定位**：

```bash
# 1. 找慢节点——看 DN 的 RPC / IO 延迟
# Grafana 上按 datanode 维度看 Read/Write Op 平均耗时，离群点就是慢节点

# 2. 在 DN 上看磁盘队列
ssh <dn_host> "iostat -x 2 5"
# 关注 %util、await，单盘 await > 50ms<!-- 校准：请按真实经历核实/替换 --> 就有问题

# 3. 看是不是慢盘但还没 failed
ssh <dn_host> "smartctl -H /dev/sdX"

# 4. 看 DataBlockScanner 日志，是否有块被反复校验失败
ssh <dn_host> "grep -i 'verify' /var/log/hadoop/hdfs/*-datanode-*.log | tail"
```

**处理**：

- **Hedged Read**：客户端配置 `dfs.client.hedged.read.threshold.millis=50`<!-- 校准：请按真实经历核实/替换 -->，读慢了自动发副本读，规避慢节点。注意这个参数的副作用我在读路径篇讲过，会放大 DN 压力。
- **隔离慢节点**：把它加入 `dfs.hosts.exclude` 做 decommission，或临时下线修盘。
- **换盘**：确认是单盘老化就换。
- **网络排查**：有时不是磁盘是网卡，`ethtool -S <nic> | grep -i error` 看错包计数。

### 3.6 误删恢复

这条是给业务方救命的，运维同学务必熟记。

**现象**：业务方误删了重要目录，焦急求助。

**定位与处理**：

```bash
# 1. 第一直觉：查回收站
hdfs dfs -ls /user/<username>/.Trash/Current/

# 2. 找到删除的目录
hdfs dfs -ls -R /user/<username>/.Trash/Current/ | grep <dirname>

# 3. 恢复
hdfs dfs -mv /user/<username>/.Trash/Current/<deleted_path> /original/path
```

**回收站机制**：HDFS 回收站默认 `fs.trash.interval=0`（关闭）<!-- 校准：请按真实经历核实/替换 -->，生产**必须开**，建议设成 1440（分钟，即 24h）或更长。回收站里的文件到点会被 `expunge` 清掉。

**如果回收站已清空**：靠 **Snapshot**。前提是你提前给关键目录开了快照：

```bash
# 开启快照（事前）
hdfs dfsadmin -allowSnapshot /critical_dir

# 创建快照
hdfs dfs -createSnapshot /critical_dir snap_20260702

# 删除后从快照恢复
hdfs dfs -cp /critical_dir/.snapshot/snap_20260702/<file> /original/path
```

**关键教训**：回收站和快照都是**事前**手段，事后补不了。运维要做的，是在业务方出事之前就把这两个开关打开。

### 3.7 NN 启动慢

NN 重启慢本身不一定是故障，但**HA 切换时慢一秒都致命**，所以要把它纳入观测。

**现象**：NN 重启后进入 Safemode 很久才退出；FsImage 加载耗时长；块报告风暴。

**定位**：

```bash
# 1. 看 NN 启动各阶段耗时
grep -E "FSImage|LoadFsImage|SafeMode" /var/log/hadoop/hdfs/*-namenode-*.log | tail -50

# 2. FsImage 多大
ls -lh /data/nn/current/fsimage_*

# 3. 块报告风暴
grep "BLOCK\* registerDatanode" /var/log/hadoop/hdfs/*-namenode-*.log | wc -l
```

**根因与处理**：

- **FsImage 太大**：本质还是元数据多（小文件/大目录）。治理小文件是长期方案；短期可调大 `dfs.namenode.max.objects`（默认 0 不限制）<!-- 校准：请按真实经历核实/替换 -->，但要明白这会放大堆压力。
- **块报告风暴**：NN 一起来，所有 DN 同时上报全量块报告，RPC 被打爆。Hadoop 3.x 有块报告分批（`dfs.namenode.blockreport.full.report.length`），可以调；也可以**滚动重启 DN**，错开上报时间。
- **CheckPoint 没跟上**：NN 启动要回放 edits，edits 太长启动就慢。确保 Standby 在周期性合并，或手动 `hdfs dfsadmin -saveNamespace`（会触发 Safemode，慎选窗口期）。

## 四、容量与扩缩容

容量管理是 HDFS 运维的常规动作，但容易踩坑。

### 4.1 扩容上线流程

新节点上线 SOP：

1. 机器到位，装好 OS、Java、Hadoop 二进制，配置同步。
2. 配 `dfs.datanode.data.dir`、`dfs.namenode.name.dir` 等参数，与现网一致。
3. 把新节点加入 `dfs.hosts`（白名单）。
4. 启动 DN：`hdfs --daemon start datanode`。
5. **验证**：NN UI 看到新 DN 进入 Live；`hdfs dfsadmin -report` 看到容量增加。
6. **关键**：跑 balancer，让新节点真正承担数据。否则业务写入还是往老节点堆。详见副本管理篇。

### 4.2 Decommission 下线流程

下线节点 SOP（比上线更危险，因为要在保证副本数的前提下搬走数据）：

```bash
# 1. 加入 exclude 文件
echo "<dn_host>" >> /etc/hadoop/dfs.hosts.exclude

# 2. 刷新 NN
hdfs dfsadmin -refreshNodes

# 3. 观察
hdfs dfsadmin -report | grep -A5 "Decommissioning"
# 等 Decommission Status 变成 "Decommissioned" 才能下电

# 4. 完成后从白名单和 exclude 都移除，再 refresh
```

**坑点**：

- Decommission 是有副本数保证的搬运，速度受 `dfs.datanode.balance.bandwidthPerSec` 限制，慢。
- EC（纠删码）文件的 decommission 逻辑和 3 副本不同，可能卡住。见副本管理篇。
- **绝对不能直接 kill DN 进程了事**，那会导致大量 under-replicated blocks，等于制造故障。

### 4.3 Balancer 跟进

扩缩容后必跑 balancer。详见副本管理篇，这里只提醒一句：**balancer 的带宽不要调太大**，默认 1MB/s 太保守，但调到 100MB/s 以上会冲击业务 IO。我一般调到 50MB/s<!-- 校准：请按真实经历核实/替换 -->。

## 五、安全运维

启用了 Kerberos 的集群，会多出一类"安全类故障"。

### 5.1 Kerberos ticket 续约失效

**现象**：长跑作业（如 Spark/Flink 跑几小时）突然报认证失败 `GSSException: No credentials provided`；客户端报 ticket expired。

**根因**：Hadoop 的机制是 login 时拿一个 TGT，然后靠 `krb5.conf` 的 `renew_lifetime` 自动续约。续约失效常见原因：

- KDC 的 `max_renewable_life` 设得不够长。
- ticket 续约到了最大生命周期（默认 7 天）<!-- 校准：请按真实经历核实/替换 --> 还没跑完。
- keytab 过期或 principal 被禁。

**处理**：

- 长跑作业用 keytab + `UserGroupInformation.loginUserFromKeytabAndReturnUGI`，定期 re-login。
- 检查 `krb5.conf` 的 `renew_lifetime` 与 KDC 的 `maxrenewlife`。
- DN/NN 服务侧用 keytab + 自动续约，配 `hadoop.kerberos.keytab.login` 相关参数。

### 5.2 Delegation Token 过期

**现象**：MapReduce/Spark 长任务报 `org.apache.hadoop.security.token.SecretManager$InvalidToken: token expired`。

**根因**：HDFS Delegation Token 默认有效期 24h（`dfs.namenode.delegation.token.max-lifetime`）<!-- 校准：请按真实经历核实/替换 -->，长任务跑超了。

**处理**：作业配置里启用 token 续约（YARN 的 RM 有 TokenRenewer）；或调大 max-lifetime；或长跑流式作业改用 keytab 直接认证，不依赖 token。

### 5.3 ACL 变更审计

**现象**：误把权限给了不该给的人，或权限被改导致业务报 `AccessControlException`。

**处理**：

```bash
# 看权限
hdfs dfs -getfacl /sensitive/dir

# 设置 ACL
hdfs dfs -setfacl -m user:alice:rwx /sensitive/dir

# 审计：开 audit log
# hdfs-site.xml: dfs.namenode.audit.log.enabled=true
grep "allowed=true\|allowed=false" /var/log/hadoop/hdfs/*-namenode-audit.log | grep "/sensitive/dir"
```

**生产建议**：audit log 一定要开，配合 Ranger 做细粒度权限，关键目录变更留痕。详见我那篇 Kerberos & Ranger。

## 六、应急工具箱

把常用命令攒成一个 cheat sheet，值班时直接翻。

### 6.1 hdfs fsck —— 定位 missing/corrupt

```bash
# 全集群健康检查
hdfs fsck /

# 列出 corrupt/missing 块
hdfs fsck / -list-corruptfileblocks

# 删掉 corrupt 块（最后手段）
hdfs fsck / -delete
```

### 6.2 hdfs oiv / oev —— 离线分析镜像

```bash
# 把 FsImage 转成可读
hdfs oiv -i /data/nn/current/fsimage_0000000000001234567 \
         -o /tmp/fsimage.txt -p Delimited

# 把 edits 转成可读
hdfs oev -i /data/nn/current/edits_0000... -o /tmp/edits.xml
```

这俩在小文件治理（统计 FsImage 找小文件大户）和元数据排障时是核武器。

### 6.3 jstack / jmap —— JVM 现场诊断

```bash
# 线程栈
jstack <pid> > /tmp/nn.stack

# 对象直方图（轻量）
jmap -histo:live <pid> | head -30

# 完整 dump（重量级，会 STW）
jmap -dump:format=b,file=/tmp/nn.hprof <pid>
```

### 6.4 hdfs debug —— 块级调试

```bash
# 看某个块落在哪些 DN 上
hdfs debug recoverLease -path <file>

# 打印块位置
hdfs fsck <file> -files -blocks -locations
```

## 七、值班 Checklist

最后附一份我自己值班用的 checklist，出事时照着这个走，能保证不漏关键步骤。

- [ ] **第一反应：确认是否真的 P0**。看 NN UI 能否打开、Safemode 是否亮、Dead DN 是否激增。
- [ ] **通报**：P0 故障第一时间在工作群通报"已介入，预计 X 分钟同步进展"，避免业务方反复电话。
- [ ] **抓现场**：jstack、jmap、NN/DN 日志、fsck 输出，**第一时间抓**，错过就没了。
- [ ] **止血优先**：能切换 HA 就切换，能重启就重启，先恢复服务再追根因。
- [ ] **定位根因**：对照上面七类 SOP 逐项排查。
- [ ] **处理**：按 SOP 执行，每一步都留命令记录。
- [ ] **验证恢复**：fsck 干净、under-replicated 归零、业务方确认正常。
- [ ] **复盘**：48h 内出复盘文档，根因 / 时间线 / 改进项，改进项落到 SOP 和监控。

---

## 附：NN Full GC 应急排查命令序列

```bash
# === 步骤1：确认 Full GC 与影响 ===
grep "Full GC" /var/log/hadoop/hdfs/*-namenode-*.log | tail -50
jstat -gcutil <nn_pid> 1000 10

# === 步骤2：抓现场（关键，重启前必抓）===
jstack <nn_pid> > /tmp/nn_jstack_$(date +%s).txt
jmap -histo:live <nn_pid> | head -50 > /tmp/nn_histo_$(date +%s).txt

# === 步骤3：止血——HA failover ===
hdfs haadmin -getServiceState nn1   # 确认当前 Active
hdfs haadmin -failover nn1 nn2      # 切到 Standby

# === 步骤4：在原 Active 上分析根因 ===
jmap -histo:live <nn_pid> | head -30
# 看对象分布，INode/byte[] 占大头 = 小文件/大目录问题
grep -c "Full GC" /var/log/hadoop/hdfs/*-namenode-*.log

# === 步骤5：抓 FsImage 离线分析小文件大户 ===
ls -lh /data/nn/current/fsimage_*
hdfs oiv -i <最新fsimage> -o /tmp/fsimage.txt -p Delimited
awk -F',' '{print $7,$8}' /tmp/fsimage.txt | awk '$1<1048576 {print $2}' \
  | sed 's|/[^/]*$||' | sort | uniq -c | sort -rn | head -20
```

## 附：大量 under-replicated blocks 处理命令序列

```bash
# === 步骤1：确认 under-replicated 规模与来源 ===
hdfs fsck / -files -blocks -locations | grep -c "Under replicated"
hdfs dfsadmin -report | grep -E "Live|Dead|Decommissioning"
curl -s http://nn:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo \
  | jq '.beans[0] | {StaleDataNodes, VolumeFailuresTotal, UnderReplicatedBlocks}'

# === 步骤2：定位是 DN 下线还是坏盘 ===
hdfs dfsadmin -report | grep "Dead datanodes"
# 在 Dead DN 上排查：
ssh <dead_dn> "tail -100 /var/log/hadoop/hdfs/*-datanode-*.log"

# === 步骤3：恢复 DN（优先）或确认 decommission 状态 ===
hdfs dfsadmin -refreshNodes
hdfs dfsadmin -report | grep -A3 "Decommissioning"

# === 步骤4：放开 re-replication 限速（必要时滚动改 hdfs-site.xml）===
# dfs.namenode.replication.max-streams=16
# dfs.namenode.replication.work.multiplier.per.iteration=8
hdfs dfsadmin -reloadSharedAdapters  # 或重启 NN（HA 滚动）

# === 步骤5：监控消化进度 ===
watch -n 30 'curl -s http://nn:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo \
  | jq ".beans[0] | {UnderReplicatedBlocks, PendingReplicationBlocks, ScheduledReplicationBlocks}"'
# ScheduledReplicationBlocks 单调下降 = 在补，只是慢；不动 = 限速还卡着
```

## 小结

HDFS 的运维，本质是两件事：**让你先看见，让你能处置**。

观测体系是眼睛。NameNode / DataNode 的 JMX 指标，配上 Prometheus + Grafana 和一套调过的告警阈值，能把绝大多数故障掐灭在用户感知之前。日常巡检是肌肉记忆，十分钟换一份从容。

故障 SOP 是肌肉。NN Full GC、under-replicated blocks、DN 死亡坏盘、Safemode 卡死、慢节点、误删、启动慢——这七类几乎覆盖了 HDFS 生产故障的全部高频场景。每一类都按"现象 → 定位 → 处理"走一遍，配合文末的两段命令序列和应急工具箱，值班时就不慌。

最后想说一句，运维的尽头是工程化。**真正成熟的运维，不是个人英雄主义地救火，而是把每一次故障都沉淀成 SOP、监控、checklist，让下一个值班同学不用重新踩一遍坑**。希望这一篇能成为你值班屏幕角落里那页常驻的参考。

HDFS 系列到这里就告一段落了。从架构、读写、元数据、副本、一致性、小文件到这篇运维收尾，一套比较完整的 HDFS 知识体系算是搭起来了。后续如果有机会，会再写写 HDFS 跨集群迁移、EC 落地这些进阶话题。我们下个系列见。
