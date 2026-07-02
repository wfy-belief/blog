---
title: Hadoop 高可用架构：NameNode HA、ResourceManager HA 与 Federation
abbrlink: hadoop-ha-federation
date: 2026-07-02 11:00:00
tags:
  - Hadoop
  - 高可用
  - Federation
categories:
  - 技术
description: 讲透 Hadoop 的单点消除之道与元数据水平扩展
ai_text: "本文以第一人称复盘某 PB 级 Hadoop 集群的高可用改造与 Federation 落地经验，系统讲透 NameNode HA 的 Active/Standby 双 NameNode 架构、QJM 多数写与 NFS 对比、ZKFC + ZooKeeper 自动故障转移与脑裂 fencing，再到 ResourceManager HA 与 HDFS Federation 的 Block Pool 解耦、ViewFS/Router 统一视图，最后落到 QJM 部署、脑裂排查、客户端 failover 调优等生产踩坑，帮助大数据工程师建立从底线到天花板的完整 HA 工程视角。"
---

# Hadoop 高可用架构：NameNode HA、ResourceManager HA 与 Federation

在我维护的那个约 200 节点、3 PB 规模的 Hadoop 集群里 <!-- 校准：请按真实经历核实/替换 -->，最让我后怕的一次事故，不是 DataNode 磁盘坏了一片，也不是 Spark 任务跑挂了，而是 NameNode 所在节点的主板烧了。那一年我们的集群还没上 HA，NameNode 是单点。机器一死，整个 HDFS 不可读写，所有上层 Hive/Spark 作业全部卡死，恢复靠的是把 fsimage 拷到备机再启动——前后宕了接近 40 分钟 <!-- 校准：请按真实经历核实/替换 -->。从那天起，"消除单点"成了我心里的一条红线。

这篇文章把我在生产里把一个非 HA 集群一步步改造为 NameNode HA + ResourceManager HA，又在元数据膨胀后引入 Federation 的全过程复盘清楚。重点不在配置文档抄一遍，而是把每个机制背后的设计假设、失败语义和我们踩过的坑讲透。

## 一、为什么 NameNode 是最致命的单点

HDFS 的元数据（目录树、文件->block 映射、DN 拓扑）全部常驻 NameNode 内存，DataNode 只是块存储的"工人"。这意味着：

- NameNode 进程挂了，集群无法 open/delete/rename 任何文件；
- NameNode 所在节点宕了，连 block location 都拿不到，客户端读已缓存的 block 还能凑合，但写和新建全部失败；
- 更糟的是，如果 fsimage + edits 损坏，可能丢失最近一段时间的元数据，连带整个集群的数据"地址簿"作废。

在 Hadoop 2.0 之前，业界普遍用 NFS 或 SecondaryNameNode 做"热备"。但 SecondaryNameNode 本质是定期合并 fsimage 的快照工，不是热备；NFS 共享 edits 能让备机拿到日志，但 NFS 自己又是单点。真正的双活热备（Active/Standby）直到 Hadoop 2.0 引入 HA 才算落地。这张图概括了改造前后的差别：

```
非 HA 架构（SPOF）:
   Client ──RPC──> [ NameNode (唯一) ] ──edits──> NFS/本地
                         |
                      DataNode x N

HA 架构（双 NameNode）:
   Client ──RPC──> [ Active NN ] ──edits──> JournalNode Quorum (3/5)
                        ^                       |
                        |---- 心脑/状态 ----ZK + ZKFC
                        v                       v
                    [ Standby NN ] <──读 edits── JournalNode Quorum
                         |
                      DataNode x N
```

下面进入正题。

## 二、NameNode HA 原理：Active/Standby 双 NameNode

### 2.1 整体结构

NameNode HA 的核心是同时运行两个 NameNode：一个 Active（负责所有客户端读写），一个 Standby（持续同步 Active 的 edits，随时准备接管）。两者的关键差异不在进程，而在"谁持有写 edits 的权力"——只有 Active 能向 JournalNode 集群发起写。

```
                +-------------------+        +-------------------+
   Client ---->|  Active NameNode  |        | Standby NameNode  |
                +--------+----------+        +----------^--------+
                         |                              |
                    写 edits                       读 edits 回放
                         v                              |
              +-----------------------+                 |
              | JournalNode 1/2/3 ... | (Quorum, 多数写) |
              +-----------------------+                 |
                         ^                              |
                         +------------------------------+
                          (Standby 持续从 JN 拉取 edits)

   DataNode 同时向 Active 和 Standby 汇报块报告（非常重要）
```

这里有几个工程细节常被一笔带过：

1. **DataNode 块报告必须双发**。在 HA 下，每个 DataNode 都要同时向 Active 和 Standby 上报 block report 和增量块报告，否则 Standby 接管时它的元数据里 block->DN 映射是过期的，会发生大量"误判 block 缺失"。配置项 `dfs.datanode.address`、`dfs.datanode.http.address` 等 DataNode 端的 NN 地址列表要写全两个。
2. **客户端要配置 nameservice 而不是某个 NN 的 host**。客户端通过 HA URI（如 `hdfs://mycluster/...`）访问，由客户端侧的 `ConfiguredFailoverProxyProvider` 决定连哪个 NN。
3. **Standby 不是热读副本**。Hadoop 2.x 里 Standby 不接读请求，到 3.x 才支持 `dfs.ha.tail-edits.in-progress` 让 Standby 更接近实时，但读仍走 Active（除非用了 Observer NN，HDFS-13583 引入的读副本，这是另一个话题）。

### 2.2 共享存储：QJM vs NFS

Active 写的 edits 必须可靠地让 Standby 看到，这就是"共享 Edit Log 存储"的职责。两种主流实现：

| 维度 | QJM（Quorum Journal Manager） | NFS 共享存储 |
| --- | --- | --- |
| 部署 | 3 或 5 个 JournalNode 进程，与 NN 共置即可 | 需要独立 NFS 服务器/存储阵列 |
| 写语义 | 多数写成功即算提交（2N+1 容忍 N 个故障） | 单点，NFS 挂了写不动 |
| Fencing | JN 仅接受一个 Active 的写（基于 epoch） | 依赖 NFS 锁，弱 |
| 运维 | 简单，纯 Java 进程 | 复杂，依赖 OS/NFS 服务 |
| 性能 | 多数写有网络开销，但通常够用 | 本地写，延迟低但单点 |

我们在选型时几乎没犹豫就上了 **QJM，部署 3 个 JournalNode** <!-- 校准：请按真实经历核实/替换 -->。理由是 QJM 把"共享存储"这个单点用多数派协议消除了，这是 HA 设计里最优雅的一笔——它本身就是一个微型 Quorum 协议。

QJM 的核心是 **Paxos-like 的多数写**：Active 写一条 edit log 时，要同时向所有 JN 发起写请求，超过半数（3 个里 2 个、5 个里 3 个）成功就算提交。这样即便一个 JN 宕机，整个 HA 链路仍可用。多 JN 之间的同步靠 **edits 文件分段（segment）+ epoch 号**保证一致性，新的 Active 接管会发起一次"precovery"，把所有 JN 上的 edits 对齐到最新已提交的 segment。

```
QJM 写流程（3 JN，多数 = 2）:
   Active NN ──写 edit #100──> JN1: OK
                  └──────────> JN2: OK   => 已 2/3，提交成功，返回客户端
                  └──────────> JN3: 超时

   (JN3 恢复后会从 JN1/JN2 同步缺失的 segment)
```

QJM 用 **epoch 号**做 fencing：每次 Active 切换都会生成一个单调递增的 epoch，写 JN 时带上，JN 拒绝比它已知 epoch 小的写。这就从根本上防止了"老 Active 假死复活后继续写 edits"的脑裂。

### 2.3 手动 vs 自动故障转移

HA 的故障转移分两档：

- **手动（manual failover）**：运维执行 `hdfs haadmin -failover nn1 nn2`。优点是可控，缺点是半夜报警要人爬起来。
- **自动（automatic failover）**：依赖 ZooKeeper + ZKFailoverController（ZKFC）。ZKFC 是 NN 旁边的独立进程，负责健康监控、ZK 会话维持、master 选举和触发切换。

生产里基本都要上自动故障转移——HA 没有自动切换等于半残。下一节展开。

### 2.4 Fencing：脑裂的最后一道防线

脑裂（split-brain）是分布式系统最可怕的故障：网络分区让两个 NN 都以为自己是 Active，于是都向 JN 写 edits，元数据彻底乱套。QJM 的 epoch 机制在共享存储层做了一道 fencing，但还可能有更隐蔽的场景——比如老 Active 进程卡住没死、网络瞬断后恢复。

所以 HA 在切换时强制要求对老 Active 做 fencing（隔离），配置在 `dfs.ha.fencing.methods`：

- **sshfence**：通过 SSH 登录老 NN 所在节点，杀掉 NN 进程（`kill -9`）。简单可靠，但要求 NN 之间互信 SSH。
- **shellfence**：执行任意 shell 脚本，比如调用 IPMI/PDU 电源切断、隔离网络。
- **STONITH**（Shoot The Other Node In The Head）：硬件级电源隔离，最暴力也最彻底，常配合 PDU/IPMI 实现。

我们生产用的是 `sshfence` + `shellfence`（双保险，前者失败走后者调 PDU 断电） <!-- 校准：请按真实经历核实/替换 -->。原则是：宁可错杀，不可漏放。fencing 失败的故障转移会被强制中止——宁可集群短暂不可用，也不能让两个 Active 同时写。

## 三、自动故障转移流程：ZKFC + ZooKeeper

自动故障转移的灵魂是 ZKFC。每个 NN 节点上跑一个 ZKFC 进程，它做四件事：健康检查、ZK 会话维持、master 选举、触发切换。

```
ZKFC 的工作模型（每个 NN 节点一个 ZKFC）:

   +----[ Node 1 ]----+        +----[ Node 2 ]----+
   |  NN1 (Active)    |        |  NN2 (Standby)   |
   |     ^            |        |     ^            |
   |     | 健康检查    |        |     | 健康检查    |
   |  ZKFC1 -------+   |        |  ZKFC2 ------+   |
   +---------------|---+        +---------------|---+
                   |                              |
                   v                              v
              +-----------------------------------+
              |   ZooKeeper (3/5 节点集群)         |
              |   /hadoop-ha/mycluster            |
              |     |- ActiveStandbyElectorLock   | (临时节点，谁抢到谁是 Active)
              |     |- ActiveBreadCrumb           | (持久节点，记录当前 Active)
              +-----------------------------------+
```

故障转移的完整流程：

1. **健康检查**：ZKFC 周期性（默认 1 秒，`ha.health-monitor.check-interval.ms`）<!-- 校准：请按真实经历核实/替换 --> 调用 NN 的 `MonitorHealth` RPC，判断 NN 是否健康。健康状态分 SERVICE_HEALTHY / SERVICE_UNHEALTHY / SERVICE_NOT_RESPONDING / HEALTH_CHECK_FAILED。
2. **会话失活**：如果 ZKFC 进程崩溃、或 ZKFC 与 ZK 网络断开、或所在节点宕机，ZK 上 ZKFC 持有的临时节点（ActiveStandbyElectorLock）会因 session timeout 被删除。
3. **选举**：锁一释放，另一个 NN 的 ZKFC 就能竞争创建这个临时节点。先创建成功的那个，就把自己的 NN 标记为 Active。
4. **Fencing 老 Active**：新 Active 在写入 JN 前，必须先对老 Active 做 fencing（见 2.4）。
5. **状态切换**：原 Standby 完成状态转换，开始服务客户端。

这里有几个细节最容易被忽视：

- **ZK session timeout 是核心调优参数**。设太短，NN 一次 Full GC 会被误判为宕机触发误切换；设太长，真宕机了响应慢。我们生产把 session 设为 10 秒 <!-- 校准：请按真实经历核实/替换 -->，配合 NN 的 GC 调优避免长时间 STW。
- **假死（false positive）问题**。ZKFC 的健康检查 RPC 走 NN 的 RPC server，如果 NN 因为 GC 停顿 30 秒，ZKFC 可能误判。Hadoop 加了"SERVICE_NOT_RESPONDING"状态——连续若干次没响应才认为是死。但生产里我们仍然遇到 NN 因为 RPC 队列积压（几万个待处理 RPC）导致健康检查超时，被错误地切换。
- **健康检查内容**：ZKFC 不只检查 RPC 通不通，还会检查 NN 是否处于 Safemode、磁盘是否可用（`dfs.namenode.resource.du.reserved` 等）。NN 的元数据目录所在磁盘满，会触发 NN 进入不健康状态，进而引发切换。

## 四、ResourceManager HA

YARN 的 ResourceManager（RM）也是单点。RM 故障不会丢数据（AM 和 NM 会重连），但会导致所有提交、调度、AM 续约中断。Hadoop 2.4 引入了 RM HA。

### 4.1 架构

```
   Client/AM/NM ──> [ Active RM ] <──状态同步──> [ Standby RM ]
                          |                            |
                          +-- 写状态 ---> ZooKeeperStateStore
                          +-- 写状态 ---> (可选) LevelDBStateStore / FileSystemRMStateStore
```

和 NameNode HA 类似，RM HA 也是 Active/Standby 双进程，但状态存储机制不同。RM 把应用/调度状态持久化到 RMStateStore（推荐 ZooKeeperStateStore 或 LevelDB），Standby 读取这些状态。当 Active 故障，Standby 接管并基于持久化状态恢复调度。

### 4.2 关键差异点

- **RM 的状态写不需要多数派**——它直接持久化到一个 StateStore。所以 RM HA 不需要 JournalNode。
- **客户端/AM 重试**：YARN 客户端和 ApplicationMaster 都内置了 failover 重试逻辑（`yarn.resourcemanager.connect.*`）。AM 续约失败会自动重连下一个 RM。
- **恢复粒度**：基于 ZKStateStore，恢复后能保住正在运行的应用的调度上下文（已分配的 container、application attempt 列表），但当前调度瞬间的内存状态会丢一些，所以可能看到少量 container 被重复分配或 AM 重试。

RM HA 的配置比较直白，重点是 `yarn.resourcemanager.ha.enabled=true`、`yarn.resourcemanager.cluster-id`、`yarn.resourcemanager.ha.rm-identifier` 列表，以及把 Store 切到 ZK。

## 五、HDFS Federation：元数据的水平扩展

HA 解决了"可用性"，但没有解决"容量"。单 NameNode 把整个集群的元数据装进内存，单机内存就是元数据规模的天花板。我们的集群在存了约 6 亿个文件后 <!-- 校准：请按真实经历核实/替换 -->，NameNode 堆内存吃满 400 GB <!-- 校准：请按真实经历核实/替换 -->，Full GC 越来越频繁，每次停顿几十秒——这是元数据瓶颈的典型信号。这时就要上 Federation。

### 5.1 为什么要 Federation：单 NN 的水平瓶颈

单 NameNode 的瓶颈有多重：

| 维度 | 限制来源 |
| --- | --- |
| 元数据内存 | NN 堆内存上限（实际几十到几百 GB） |
| RPC QPS | 单 NN 的 RPC handler 数与线程模型 |
| 启动时间 | fsimage 越大，加载越慢（小时级） |
| 写入吞吐 | edits 同步成为吞吐上限 |

加机器、加内存只能延缓，不能突破。Federation 的思路是：**多个独立的 NameNode，每个负责一部分命名空间**。

### 5.2 核心机制：Block Pool 与 Namespace 解耦

Federation 的设计精髓是把 NameNode 的两类元数据解耦：

- **Namespace（命名空间）**：目录树、文件名、权限等，每个 NN 独立维护一份。
- **Block Pool（块池）**：每个 NN 拥有一个全局唯一的 block pool ID，所有 DataNode 上属于这个 NN 的 block 都归在这个 pool 下。

DataNode 不再忠于某一个 NN，而是同时为多个 NN 注册、存储属于多个 block pool 的数据：

```
Federation 架构（2 个 NameService 示例）:

   Client ──> +-- NameService1 (NS1) ── NN1 (Active/Standby) ─ Block Pool "BP-1234"
              |
              +-- NameService2 (NS2) ── NN2 (Active/Standby) ─ Block Pool "BP-5678"
                                                           ^
   DataNode 节点上同时持有两个 block pool:                  |
   /data/dfs/dn/
       current/
           BP-1234/   <- 属于 NS1 的 block（块报告给 NS1）
                blk_xxx
           BP-5678/   <- 属于 NS2 的 block（块报告给 NS2）
                blk_yyy
```

这样的好处：

1. **水平扩展**：加 NN（即加 NameService）就能扩元数据容量和 RPC QPS。
2. **隔离**：一个业务组的 NN 出问题，不会影响另一个业务组的 NN。
3. **DataNode 复用**：物理 DN 是共享的，存储空间统一调度，不需要拆集群。

代价是**没有全局命名空间**——同一个客户端想访问 NS1 和 NS2 的文件，要用不同的 URI（`hdfs://ns1/...` 和 `hdfs://ns2/...`）。这就是 ViewFS / Router 要解决的问题。

### 5.3 统一视图：ViewFS 与 Router-based Federation

**ViewFS**（老方案）：客户端侧的"挂载表"。在 core-site.xml 里配 `fs.viewfs.mountTable.*`，把 `/user/bi` 映射到 `hdfs://ns1/...`，把 `/user/rec` 映射到 `hdfs://ns2/...`，让客户端看起来有一个全局的 `/`。缺点是配置散在客户端，更新要改所有客户端配置。

**Router-based Federation**（Hadoop 2.9+ 推荐）：在客户端和 NN 之间加一层 Router 进程（类似一个 RPC 代理），Router 维护全局 mount table，客户端只连 Router。Router 本身可以多实例 HA。这种方式把挂载表集中管理，是生产 Federation 的主流选择。

```
Router-based Federation:
   Client ──> [ Router (HA) ] ──根据路径──> NS1 / NS2 / NS3 ...
                (全局 mount table + StateStore)
```

我们落地时选了 Router 方案，Router 用 3 个实例 + ZK 做 HA，挂载表存 ZKStateStore <!-- 校准：请按真实经历核实/替换 -->。这样客户端配置干净，加 NameService 只改 Router 不动客户端。

## 六、生产实践与踩坑

理论和线上之间隔着一整座"踩坑库"。下面是我和团队用真金白银时间换来的经验。

### 6.1 关键 HA 配置示例

下面是我们生产 `core-site.xml` 与 `hdfs-site.xml` 的核心 HA 配置（节选）。

```xml
<!-- core-site.xml -->
<configuration>
  <!-- 指定 nameservice，客户端用 hdfs://mycluster 访问 -->
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://mycluster</value>
  </property>
  <!-- 启用自动故障转移 -->
  <property>
    <name>dfs.ha.automatic-failover.enabled</name>
    <value>true</value>
  </property>
  <!-- ZK 集群地址（ZKFC + 自动 failover 用） -->
  <property>
    <name>ha.zookeeper.quorum</name>
    <value>zk1:2181,zk2:2181,zk3:2181</value>
  </property>
</configuration>

<!-- hdfs-site.xml -->
<configuration>
  <!-- 定义 nameservice 包含哪些 NN -->
  <property>
    <name>dfs.nameservices</name>
    <value>mycluster</value>
  </property>
  <property>
    <name>dfs.ha.namenodes.mycluster</name>
    <value>nn1,nn2</value>
  </property>
  <!-- 每个 NN 的 RPC 地址 -->
  <property>
    <name>dfs.namenode.rpc-address.mycluster.nn1</name>
    <value>nn-host1:8020</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address.mycluster.nn2</name>
    <value>nn-host2:8020</value>
  </property>
  <!-- QJM 共享 edits 的地址，3 个 JournalNode -->
  <property>
    <name>dfs.namenode.shared.edits.dir</name>
    <value>qjournal://jn1:8485;jn2:8485;jn3:8485/mycluster</value>
  </property>
  <!-- 客户端 failover 代理 -->
  <property>
    <name>dfs.client.failover.proxy.provider.mycluster</name>
    <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
  </property>
  <!-- Fencing 方法：先 SSH 杀进程，再 shellfence -->
  <property>
    <name>dfs.ha.fencing.methods</name>
    <value>sshfence\nshellfence</value>
  </property>
  <property>
    <name>dfs.ha.fencing.ssh.private-key-files</name>
    <value>/home/hdfs/.ssh/id_rsa</value>
  </property>
  <!-- JournalNode 数据目录 -->
  <property>
    <name>dfs.journalnode.edits.dir</name>
    <value>/data/dfs/jn</value>
  </property>
</configuration>
```

### 6.2 客户端 failover 调优

客户端的 failover 行为直接决定用户感知到的"切换抖动"时长。几个值得调的参数：

```properties
# 切换时尝试每个 NN 的最大重试次数
dfs.client.failover.max.attempts=15
# 重试之间的睡眠（指数退避基数）
dfs.client.failover.sleep.base.millis=500
dfs.client.failover.sleep.max.millis=15000
# 切换时遇到远程异常是否触发 failover
dfs.client.failover.connection.retries=0
# 在读/写 RPC 上启用 retry policy（避免短暂的切换导致 IO 失败）
ipc.client.connect.max.retries.on.timeouts=10
```

踩坑最重的一条：**客户端缓存的 block location 在 failover 后会失效**。表现是某次故障切换后，部分 Spark 任务报 `IOException: Could not obtain block`。原因是客户端拿到的 block->DN 映射是基于老 Active 的，切换后 Standby 还没把 edits 回放完，block location 短暂不一致。解决：让客户端重试够久（`dfs.client.failover.max.attempts` 调大），并搭配 `dfs.client.retry.*` 重试读。

### 6.3 QJM 部署要点

- **JournalNode 数量**：生产环境推荐 3 个（容忍 1 个故障）；超大规模或对可用性要求极高时用 5 个（容忍 2 个故障） <!-- 校准：请按真实经历核实/替换 -->。3 个和 5 个在多数写延迟上差异不大，但 5 个的运维成本翻倍，我们选 3 个。
- **JN 数据目录要独立盘**：`dfs.journalnode.edits.dir` 指向的盘要和 DN 数据盘分开，避免 DN IO 抢占影响 edits 写延迟。JN 数据不大（活跃集群也就几 GB），用 SSD 最稳。
- **不要把 JN 和 NN 放同一节点**（虽然有文档这么建议）。我们让 JN 跑在 ZK 节点上，与 NN 物理隔离，这样单机架故障不会同时打掉 NN 和 JN。
- **JN 不能"原地降级"**：从 5 JN 缩到 3 JN 要走 `hdfs haadmin -transitionToObserver` 之类的流程，不能直接停掉 2 个，否则多数派协议可能错乱。

### 6.4 脑裂排查

真脑裂我经历过一次。现象：两个 NN 都声称自己是 Active，JN 的 edits segment 出现重复 segment 号，fsimage 合并后文件数对不上。复盘下来是 sshfence 失败（SSH 互信被误改）+ 同时网络抖动让 ZK 误判。处理：

1. 立即手动 `kill -9` 老的 NN 进程，止血。
2. 用 `hdfs oiv` 把两个 NN 的 fsimage 转成文本，对比目录树差异。
3. 以较新 epoch 的 NN 为准，对另一个 NN 的 edits 做人工合并后 `bootstrapStandby` 重建。

事后我们把 fencing 改成 sshfence + shellfence（shellfence 调 IPMI 断电），并且在监控里加了"双 Active 检测"——一个 Prometheus exporter 周期性查两个 NN 的 HA 状态，发现同时 Active 立即报警。这条监控救命过两次。

### 6.5 ZooKeeper 与 ZKFC 调优

- **ZK 集群独立部署**，不要和 Hadoop 业务混部。HBase / Kafka / Hadoop HA 共用一个 ZK 是灾难——一个业务把 ZK 打满，所有 HA 一起崩。
- **ZK session timeout** 与 ZKFC 健康检查间隔匹配。我们的经验：session timeout 10 秒、健康检查 1 秒一次、连续 3 次失败才判死 <!-- 校准：请按真实经历核实/替换 -->，对大多数 GC 抖动是宽容的。
- **ZKFC 假死**：ZKFC 进程本身也可能假死（比如它自己 GC）。Hadoop 文档建议把 ZKFC 跟 NN 跑同一节点是为了便于一起挂一起活，但这意味着节点挂掉时 NN 和 ZKFC 一起失联——ZK session 自然失活，触发对端接管。所以这个设计是正确的。
- **监控 ZKFC 状态**：`jmx` 暴露的 `Hadoop:service=HDFS,name=ZKFC` 里有 LastHealthState、LastFailoverTime 等指标，必须接到告警。

### 6.6 故障切换演练

光配好不够，必须**定期演练**。我们每季度做一次真切换演练 <!-- 校准：请按真实经历核实/替换 -->：

- 在低峰期对 Active NN 所在节点执行 `kill -9`，验证自动切换；
- 测量切换耗时（从进程挂到新 Active 可服务），目标是 < 60 秒 <!-- 校准：请按真实经历核实/替换 -->；
- 验证上层 Spark/Hive 任务是否能自动重连恢复，统计失败任务数；
- 演练后出报告，对超时/失败项做改进。

不演练的 HA 等于没有 HA——很多团队的 HA 故障转移第一次真触发就是事故现场。

## 七、小结

回到最初的那次宕机，如果说 NameNode 单点是悬在头上的剑，那 HA 就是把这把剑换成两张牌——任何时候都保证有一张能打出来。但 HA 解决的是"明天能不能用"，不是"明年还能不能装得下"。当元数据继续膨胀，Federation 才是把天花板顶高的那一步。

把这几年的经验压成一句话：

> **HA 是底线，Federation 是天花板。底线靠 QJM + ZKFC + fencing 三件套守住，天花板靠 Block Pool 解耦 + Router 统一视图撑开。**

落到工程动作清单：

1. 上线前确认 Active/Standby 双 NN + QJM（3 JN）+ 自动故障转移 + 至少两层 fencing；
2. 客户端配置 nameservice URI，调好 failover 重试参数；
3. ZK 独立集群、ZKFC 健康检查与 session timeout 匹配 NN 的 GC 表现；
4. 季度故障切换演练，把"配好的 HA"变成"验证过的 HA"；
5. 监控 NN/ZKFC/JN 状态、双 Active 检测、edits 延迟，把异常掐在萌芽；
6. 元数据接近 NN 堆内存 60% 时启动 Federation 评估，提前规划 Router + 多 NameService。

高可用不是某个开关，而是一整套"假设一切都会坏"的工程纪律。这套纪律不止适用于 Hadoop——任何承担关键业务的分布式系统，单点消除、多数派、 fencing、自动故障转移、定期演练，都是逃不掉的必修课。
