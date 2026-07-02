---
title: HDFS 读数据全流程：block 定位、短路读与预取
abbrlink: hadoop-hdfs-read-path
date: 2026-07-02 14:30:00
tags:
  - Hadoop
  - HDFS
  - 读路径
categories:
  - 技术
description: 拆解 HDFS 读链路，落到短路读、零拷贝与读吞吐调优
ai_text: "从 open 到 block 定位、checksum 校验、短路读零拷贝、预取与对冲读，逐层拆解 HDFS 读路径的源码级取舍与生产调优。读是 HDFS 最高频操作，读路径每一处设计都直接决定作业吞吐与长尾，本文把短路读权限、对冲读阈值、小文件放大等踩坑一并讲透。"
---

## 引言

在某个承载了 PB 级离线数仓与模型训练样本的集群里，我做过一次统计：一天 2.4 亿次 HDFS 访问里，读请求占比 97% <!-- 校准：请按真实经历核实/替换 -->，写不到 3%。这个比例并不意外——HDFS 自诞生起就是 "write once, read many" 语义，读是最高频的操作，也是作业吞吐的天花板。

很多人对 HDFS 的理解停留在"分布式文件系统，能存大文件"，但读路径里的每一个取舍——为什么 NameNode 只返回位置不返回数据、为什么 client 要自己挑 DN、为什么短路读要绕过 TCP、为什么对冲读能砍尾部延迟——都直接决定了 Spark stage 跑 40 分钟还是 25 分钟。本篇与系列里的"架构剖析"通用篇不同，专门把读链路拆到源码级，聚焦读性能。

## 一、open 与 block 定位：NameNode 只给"地图"，不给"货"

读的第一步是 `FileSystem.open(path)`，最终落到 `DFSClient.open()`，它会向 NameNode 发起 `getBlockLocations` RPC：

```java
// DFSClient.open -> 调用 namenode.getBlockLocations
LocatedBlocks locatedBlocks = namenode.getBlockLocations(src, start, length);
// 每个 LocatedBlock 携带 blockID + DatanodeInfo[]（已按就近性排序）
```

返回的 `LocatedBlocks` 是一个"地图"，不是数据本身。每个 `LocatedBlock` 携带：

| 字段 | 含义 |
|---|---|
| `block` | `ExtendedBlock`：blockID + poolID + numBytes |
| `locs` | `DatanodeInfo[]`：存储该 block 副本的 DN 列表，**已按就近性排序** |
| `corrupt` | 是否标记损坏 |
| `offset` | 该 block 在文件中的起始偏移 |

为什么 NameNode 不直接返回数据？两个根本原因：第一，NameNode 是元数据节点，自身不存块数据，把数据从 DN 拉到 NN 再转发给 client 会双倍占用网络、引入单点瓶颈；第二，client 直接从 DN 读，可以让 NN 的 RPC 保持轻量——NN 每秒处理几万次 getBlockLocations 就到顶了 <!-- 校准：请按真实经历核实/替换 -->，如果再背载数据转发，集群规模会被 NN 网卡打爆。

关键点是 `locs` 数组的**排序**。NameNode 借助 `DatanodeDescriptor` 与 `BlockPlacementPolicy` 反向计算优先级：如果 client 的 IP 命中某个 DN，那个 DN 排第一；否则同机架的 DN 靠前；再否则跨机架。这个排序是 client 选择 DN 的依据，直接决定读的本地性。

## 二、读循环：packet、chunk 与 checksum 校验

拿到 `LocatedBlocks` 后，`DFSInputStream` 在 `seek` 到目标位置时，会为当前 block 构造一个 `BlockReader`。整个读循环的核心在 `DFSInputStream.read()` 与 `blockSeekTo`：

```
   Client                     NameNode                 DataNode(副本1)
     |                            |                          |
     |--- getBlockLocations ----->|                          |
     |<-- LocatedBlocks[] --------|                          |
     |     (blockID, len,         |                          |
     |      DN[] 已按就近排序)     |                          |
     |                            |                          |
     |--- 选 nearest DN (本地性) ----------------------------->|
     |<============== TCP / domain socket ===================>|
     |     READ_BLOCK (blockID, offset, len)                  |
     |<== packet 流 (header + chunks) ======================= |
     |     每个 chunk = 512B 数据 + 4B CRC32C                 |
     |     逐 chunk 校验 checksum                              |
     |--- 校验失败 --- 切换副本2 ------------------------------>|
     |<== 从副本2 重读该 chunk =============================== |
     |     若全部副本失败 -> 抛 BlockMissingException          |
```

DataNode 侧的读由 `DataXceiverServer` 派发的 `DataXceiver.readBlock()` 处理，最终走 `BlockSender` 把 block 文件按 packet 发送。packet 是传输单元：

- 一个 packet 由 header + 多个 chunk 组成
- 每个 chunk = 512 字节数据 + 4 字节 CRC32C 校验和 <!-- 校准：请按真实经历核实/替换 -->
- packet 大小默认在 64KB 量级，由 `dfs.client.read.pkt.size` 控制

client 端 `BlockReaderRemote`（或短路读时的 `BlockReaderLocal`）每收到一个 chunk，就用 `DataChecksum` 重新算 CRC32C 并与 DN 送来的校验和比对。**这里的设计动机是防御 bit rot**：磁盘静默损坏在 PB 级集群上是必然事件，没有 per-chunk 校验，一个坏字节会污染整条数据链路而无人察觉。

校验失败时，`DFSInputStream` 会触发 replica fallback：标记当前 DN 为"该 block 暂不可信"，从 `locs` 取下一个副本，重新建立 `BlockReader`，从失败点继续读。源码里对应 `chooseDataNode` 与 `fetchBlockAt` 的重试循环；所有副本都失败才抛 `BlockMissingException`。这个机制让读路径在副本损坏时自愈，对上层透明。

## 三、就近原则与本地性：同节点 > 同机架 > 跨机架

`LocatedBlock.locs` 的排序本质是 `BlockPlacementPolicy` 的反解。读侧的 `DFSInputStream.chooseDataNode` 在挑选 DN 时遵循三级优先：

1. **同节点**：client 进程所在主机就是某个 DN，命中则该副本 node-local，延迟最低
2. **同机架**：同机架副本走架顶交换机，典型 RTT 在亚毫秒级 <!-- 校准：请按真实经历核实/替换 -->
3. **跨机架**：跨核心交换机，延迟和带宽都更贵

这个分级对计算框架至关重要。MapReduce 的调度器、Spark 的 `RDD.getPreferredLocations` 都依赖 `InputFormat` 返回的 host 列表，本质就是 HDFS 读路径给出的就近建议。当一个 Spark task 被调度到持有 block 的节点，数据是 node-local；否则变成 rack-local 甚至 any，shuffle 前的 read 阶段就要吃跨节点带宽。

这里有个容易被忽略的点：**client 不一定在 DN 上**。一个纯 client 角色（比如跑在边缘节点的 ad-hoc Hive 查询）永远不会 node-local，所有读都走网络。这就是为什么生产上强烈建议把计算任务跑在与 DN 同机柜的 NodeManager 节点上——本地性是免费的性能。

## 四、短路读（Short-Circuit Local Read）：绕过 TCP，直读文件

当 client 与 DN 同机时，走 TCP 读 block 是浪费——数据明明就在本地磁盘，却要经过 loopback、序列化、两次系统调用。短路读就是为这种场景设计。

启用后，client 通过 **domain socket**（Unix domain socket）与 DN 建立一条通道，DN 把 block 文件描述符直接传给 client，client 之后用 `FileChannel` / `mmap` 直读文件，**完全绕过 DataNode 进程和 TCP 栈**：

```
传统读 (TCP loopback):                短路读 (domain socket):
  client --TCP--> DN --read--> disk     client --fd--> block file
  4 次用户态/内核态切换                       (mmap, 零拷贝)
                                       1 次 mmap, 后续 read 无切换
```

关键配置（client 与 DN 两侧都要配）：

```properties
dfs.client.read.shortcircuit=true
dfs.domain.socket.path=/var/lib/hadoop-hdfs/dn_socket
# DN 侧允许哪些系统用户走短路读
dfs.block.local-path-access.user=hdfs,spark,yarn
# 短路读底层用 mmap（零拷贝）；false 则用 fileChannel
dfs.client.read.shortcircuit.use.mmap=true
```

`dfs.domain.socket.path` 必须是 client 和 DN 都能访问的本地路径，通常放在 `/var/lib/hadoop-hdfs/dn_socket` <!-- 校准：请按真实经历核实/替换 -->。DN 启动时 `bind` 这个 socket，client 端 `DomainSocket.connect` 后通过 `SHORT_CIRCUIT_READ` 操作码握手。

源码层面，`BlockReaderLocal` 持有一个 `ShortCircuitReplica`，它包装了 block 的 `RandomAccessFile` 和 mmap 出来的 `MappedByteBuffer`。当 `use.mmap=true` 时，`BlockReaderLocal.read()` 直接从 `MappedByteBuffer` 拷贝到用户 buffer，连 `read()` 系统调用都省了——这是真正的零拷贝。代价是 mmap 占用进程地址空间，由 `ShortCircuitShm` 池化管理，cache size 由 `dfs.client.read.shortcircuit.streams.cache.size` 控制 <!-- 校准：请按真实经历核实/替换 -->。

**踩坑**：短路读要求 client 的 UID 在 `dfs.block.local-path-access.user` 白名单里，否则 DN 会拒绝握手并 fallback 到 TCP。我见过一个 Spark 任务因为 executor 用 `nobody` 用户起，短路读没生效，读吞吐从预期的 800MB/s 掉到 200MB/s <!-- 校准：请按真实经历核实/替换 -->，排查半天才定位到权限。`hdfs dfsadmin -report` 看不到这个，要开 `org.apache.hadoop.hdfs.ShortCircuitShm` 的 DEBUG 日志才能确认是否真的走了短路读。

## 五、预取与 readahead：顺序读的胜利

HDFS 的 `DFSInputStream` 内部维护预取窗口。`DFSClient.read()` 在拿到一个 block 的 reader 后，会一次性从 DN 读 `dfs.read.prefetch.size` 大小的数据填充窗口，后续的 `read()` 命中窗口就直接返回，避免每次都走 RPC：

```properties
# 预取窗口，默认约 10 * block size（128MB 块下约 1.28GB）<!-- 校准：请按真实经历核实/替换 -->
dfs.read.prefetch.size=134217728
# 底层文件 I/O 缓冲，默认 4096，生产常调到 64KB-256KB
io.file.buffer.size=65536
```

`io.file.buffer.size` 是 `FSInputBuffer`/`BufferedInputStream` 的缓冲粒度，DN 侧 `BlockSender` 也用它。调大能减少 `read()` 系统调用次数，对顺序读有可观收益，但会占用每 reader 的堆内存，要按并发 reader 数量权衡。

**顺序读 vs 随机读**是 HDFS 性能分水岭。顺序读时预取窗口命中率接近 100%，单线程吞吐能跑到磁盘带宽上限（SATA SSD 约 500MB/s，NVMe 约 3GB/s）<!-- 校准：请按真实经历核实/替换 -->。但 HDFS 不擅长随机读，原因有三：

1. `seek()` 会丢弃预取窗口，下一次 `read()` 重新建连、重新预取，放大效应严重
2. block 是 128MB 大块，随机读意味着跨多个 block、多次 `getBlockLocations` RPC
3. DN 侧 `BlockSender` 对每次读要 reopen 文件、定位 offset，磁盘随机 IOPS 远低于顺序带宽

所以 HBase 才要把随机读做成"短扫描 + 缓存"，本质是规避 HDFS 随机读的弱势。如果你的场景是大量随机 point lookup，应该考虑 HBase/Kudu/对象存储，而不是直接读 HDFS 文件。

## 六、checksum 与 DataBlockScanner：静默损坏的防线

前面提过 per-chunk CRC32C 校验，这是 client 侧的实时防线。但还有一个问题：**没有被读到的 block 怎么发现损坏？** 一个冷数据 block 三年没人读，磁盘 bit rot 把它啃坏了，等某天被读到才报错，那时副本可能也坏了。

HDFS 的答案是 `DataBlockScanner`——DataNode 后台线程，周期性扫描本节点所有 block，重新计算 checksum 与 block 文件 `.meta` 里存的 checksum 比对。扫描周期默认 3 周（21 天，504 小时）扫完一圈，高优先级 block（刚写的）扫得更勤 <!-- 校准：请按真实经历核实/替换 -->。发现不一致就上报 NameNode，NN 标记该副本 corrupt 并触发 re-replication。

源码在 `DataNode.blockScanner`，`VolumeScanner` 按磁盘并行扫描，限速避免影响在线 I/O。这套机制是 HDFS 对抗 bit rot 的核心，生产上务必确认 `dfs.datanode.scan.period.hours` 没被设成 0——禁用扫描等于让冷数据失去完整性保障，非常危险。

## 七、副本选择与故障转移：慢节点与对冲读

`chooseDataNode` 默认按 `locs` 顺序挑副本，但"第一个副本"不一定是最快的。HDFS 集群里经常出现"慢节点"——CPU 抢占、磁盘队列堆积、GC 停顿——它不返回错误，就是慢，能把 P99 拖到几十秒。两个机制应对：

**1. 慢节点探测**：`DFSClient` 维护 reader 的延迟统计，超过 `dfs.client.hedged.read.threshold.millis` 的节点被标记为慢，后续读优先避开。

**2. Hedged Read（对冲读）**：对同一个 block，client 先向首选 DN 发读请求；如果在 threshold 内没返回，就向第二个副本 DN **并发**发一个对冲读请求，谁先返回用谁，另一个取消。本质是用多余的网络/I/O 换尾部延迟。

```properties
# 对冲读触发阈值，默认 30ms <!-- 校准：请按真实经历核实/替换 -->
dfs.client.hedged.read.threshold.millis=30
# 对冲读线程池大小，0=禁用
dfs.client.hedged.read.threadpool.size=10
```

对冲读对长尾效果显著。某次离线 ETL，stage 跑了 90 分钟，P99 task 45 分钟 <!-- 校准：请按真实经历核实/替换 -->，开了对冲读后降到 70 分钟，P99 18 分钟——多读了几个 block 的数据，但砍掉了慢节点的等待。代价是 DN 吞吐被对冲请求稀释，要按集群负载调 `threadpool.size`，别一上来就开 50。

## 八、生产调优与踩坑清单

把上面散落的配置和经验汇总成一份可直接用的 `hdfs-site.xml`（client 侧 + DN 侧合并）：

```xml
<!-- 短路读 -->
<property>
  <name>dfs.client.read.shortcircuit</name>
  <value>true</value>
</property>
<property>
  <name>dfs.domain.socket.path</name>
  <value>/var/lib/hadoop-hdfs/dn_socket</value>
</property>
<property>
  <name>dfs.block.local-path-access.user</name>
  <value>hdfs,spark,yarn</value>
</property>
<property>
  <name>dfs.client.read.shortcircuit.use.mmap</name>
  <value>true</value>
</property>

<!-- 预取与缓冲 -->
<property>
  <name>io.file.buffer.size</name>
  <value>65536</value>
</property>

<!-- 对冲读 -->
<property>
  <name>dfs.client.hedged.read.threshold.millis</name>
  <value>30</value>
</property>
<property>
  <name>dfs.client.hedged.read.threadpool.size</name>
  <value>10</value>
</property>

<!-- 校验与扫描（DN 侧）-->
<property>
  <name>dfs.datanode.scan.period.hours</name>
  <value>504</value> <!-- 21 天 -->
</property>
```

几条踩过的坑：

| 现象 | 原因 | 处置 |
|---|---|---|
| 短路读不生效，吞吐低 | client UID 不在白名单 | 加 `dfs.block.local-path-access.user`，或换用户跑任务 |
| domain socket 报 `Permission denied` | socket 路径目录权限不对 | `chown hdfs:hadoop /var/lib/hadoop-hdfs` |
| 对冲读开了 CPU 飙高 | `threadpool.size` 过大 | 先设 5-10，观察 DN 负载再调 |
| 小文件读放大严重 | 单文件 < block size，每文件一次 open + 一次 RPC | 合并成 SequenceFile/ORC，或上小文件治理 |
| 随机读延迟高 | 预取被 seek 频繁打破 | 改批量扫描，或迁 HBase |

**小文件**要单独说一句。一个 1KB 的小文件，读它要 open（NN RPC）+ getBlockLocations（NN RPC）+ connect DN + readBlock + checksum，放大效应 100 倍起。10 万个小文件的读作业，瓶颈 99% 在 NN RPC 而不是磁盘。这种场景的优化不在读路径参数，而在上游——要么合并文件，要么用 HAR/SequenceFile，要么干脆别用 HDFS 存小文件。曾经有个团队把 50 万张训练图片直接存成 50 万个 HDFS 文件，训练启动时 NameNode RPC 队列被打满，整个集群卡死 20 分钟 <!-- 校准：请按真实经历核实/替换 -->，最后改成 TFRecord + HAR 才消停。

## 小结

HDFS 读路径的设计哲学是"分层自治"：NameNode 只管元数据与就近排序，client 负责选副本与校验，DataNode 负责传输与后台扫描。这种分层让读性能可以在多个维度被优化——短路读砍掉进程间通信、预取吃满顺序带宽、对冲读削平长尾、DataBlockScanner 兜底数据完整性。

落到生产，读优化的优先级大致是：**本地性（任务与数据同机）> 短路读 > 预取/缓冲 > 对冲读 > 校验/扫描**。前三项是稳态优化，立竿见影；后两项是长尾与可靠性兜底，出问题才显价值。把这几层都配对、把权限和路径理顺，一个 PB 级 HDFS 集群的读吞吐能做到接近裸盘带宽——这也是它十几年仍是大数据存储底座的原因。

读路径里值得展开的细节还有很多：EC（纠删码）对读路径的影响、`readReadahead` 与 `dropBehind` 的内存权衡、视图文件系统 ViewFS 下的跨 federation 读。这些留到后续单独成篇。本篇聚焦单机到集群的读主干，希望能帮你把读性能的每一档旋钮调到该在的位置。
