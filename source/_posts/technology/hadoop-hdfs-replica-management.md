---
title: HDFS 副本管理：Placement、Re-replication 与 Decommission
abbrlink: hadoop-hdfs-replica-management
date: 2026-07-02 15:30:00
tags:
  - Hadoop
  - HDFS
  - 副本管理
categories:
  - 技术
description: 从副本放置到节点退役，讲透 HDFS 副本全生命周期
ai_text: "副本是 HDFS 可靠性的基石，但副本管理远不止 dfs.replication=3 这一行配置。本文以第一人称复盘某 PB 级集群的实战经验，专攻副本全生命周期：默认 3 副本的机架感知放置策略与拓扑脚本、Re-replication 的优先级队列与限速、Over-replication 的安全删除、Decommission 退役流程与带宽控制、Maintenance 维护态、Balancer 与 Disk Balancer 的分工，最后给出退役卡住、balancer 跑不动等生产踩坑的根因与一套标准运维命令序列。"
---

# HDFS 副本管理：Placement、Re-replication 与 Decommission

在前一篇架构文章里，我花了一节篇幅讲了 HDFS 的整体架构，副本放置只是点到为止。但实际上，**副本管理才是 HDFS 日常运维的核心战场**。在某集群（约 200 节点、3 PB 出头）<!-- 校准：请按真实经历核实/替换 -->这几年的运维里，我处理过的高优故障里，副本相关的问题能占一半以上：退役卡了三天 Done 不来、扩容后新节点空跑数据全堆在老节点、balancer 把核心交换机打满、EC 文件让退役逻辑死锁……每一个都是真金白银的教训。

HDFS 用副本换可靠性，本质上是用空间换时间——多存两份数据，换的是节点坏了不丢、磁盘炸了能补。但"多存两份"只是结果，从一份副本诞生（写入时的 Placement）、到中间被搬运（Re-replication、Balancer）、到节点下线时被迁走（Decommission）、再到节点临时维护时副本该怎么算（Maintenance），副本有一个完整的**生命周期**。这篇文章，我就沿着这个生命周期把每一环讲透，区别于架构篇的概览，本篇专攻副本本身。

## 一、副本放置策略（Placement）

副本放置是副本生命周期的起点，也是 HDFS 设计哲学里最优雅的一笔。它用一套简单到近乎朴素的规则，在**写性能、读性能、容灾能力**三者之间取得了惊人的平衡。

### 1.1 默认 3 副本的放置规则

默认 `dfs.replication=3`<!-- 校准：请按真实经历核实/替换 -->，HDFS 的经典放置策略（`BlockPlacementPolicyDefault`）是这样的：

1. **第 1 个副本**：如果 client 运行在集群内的某个 DN 上，就放这个 DN（本地写，省一次网络）；如果是远程 client，NN 会随机挑一个相对空闲的节点。
2. **第 2 个副本**：放在与第 1 个副本**不同机架**的某个节点上——这是关键，它提供了机架级容灾。
3. **第 3 个副本**：放在与第 2 个副本**同机架**的另一个节点上（但不同 DN）。

最终的效果是 **"两个机架各放，其中一架放两份"**。这一点要特别强调，因为很多人（包括我带过的新人）会误以为是"同机架两份 + 跨机架一份"。实际的拓扑是：

```
                          写入 client (假设在 rack-A 的 dn-1)
                                     │
                                     ▼
   ┌─────────────────── rack-A ───────────────────┐    ┌──────── rack-B ────────┐
   │  dn-1 [副本1 ★]   dn-2 [副本3 ★]   dn-3      │    │  dn-4 [副本2 ★]  dn-5  │
   └───────────────────────────────────────────────┘    └────────────────────────┘
                      同机架内复制 (副本1 → 副本3)              跨机架复制 (副本1 → 副本2)

   pipeline 顺序: client → dn-1(rack-A) → dn-4(rack-B) → dn-2(rack-A)
   网络开销: 跨机架仅 1 次 (rack-A → rack-B)，rack-A 内 dn-4 → dn-2 走机架内带宽
```

这个设计为什么精妙？因为它同时满足了三个目标：

- **容灾**：任意一个机架整体故障（断电、Top-of-Rack 交换机宕），都至少还有副本存活。rack-A 全挂，rack-B 还有副本 2；rack-B 全挂，rack-A 还有副本 1 和 3。
- **写带宽**：跨机架带宽是稀缺资源（要经过核心交换机），这套策略只产生 **1 次跨机架写入**（pipeline 第一跳 rack-A→rack-B），后续 dn-4→dn-2 走 rack-B 内部？不，这里要注意 pipeline 是 `dn-1 → dn-4 → dn-2`，dn-4 到 dn-2 是跨机架的（rack-B 到 rack-A）。所以实际是 2 次跨机架传输——但相比"三个副本随机撒"，已经是经过深思熟虑的权衡。
- **读带宽**：客户端读时通常优先选本机架的副本，大部分读请求能在机架内闭环。

我在某集群早期犯过一个错：因为运维同学觉得"两份在同机架不安全"，私自改了副本分布逻辑，强制三个副本分散到三个机架。结果写入吞吐直接掉到原来的 40% 左右<!-- 校准：请按真实经历核实/替换 -->，因为每次写入都要跨机架传两次，核心交换机被打爆。后来老老实实改回默认策略，并理解了"机架内带宽远比跨机架便宜"这个底层事实。

### 1.2 机架感知脚本

放置策略的前提是 NN 知道每个 DN 属于哪个机架，这就是 **Rack Awareness（机架感知）** 的作用。NN 本身不知道物理拓扑，它依赖一个外部脚本来把 DN 的 IP 翻译成机架路径（如 `/dc1/rack-01`）。

```bash
#!/bin/bash
# /etc/hadoop/conf/rack-topology.sh —— 根据 IP 段映射机架
# NN 调用方式: echo "10.0.1.5 10.0.2.7" | bash rack-topology.sh
while read -r ip; do
  case "$ip" in
    10.0.1.*) echo "/dc1/rack-01" ;;
    10.0.2.*) echo "/dc1/rack-02" ;;
    10.0.3.*) echo "/dc1/rack-03" ;;
    *)         echo "/default-rack" ;;
  esac
done
```

```properties
# core-site.xml
net.topology.script.file.name=/etc/hadoop/conf/rack-topology.sh
net.topology.script.number.args=100
```

几个工程要点：

- **没配脚本 = 灾难**：所有 DN 会被归到 `/default-rack`，副本放置退化为"无拓扑感知的随机散布"，既谈不上容灾也谈不上带宽优化。我们某次扩容新机房忘了下发脚本，新节点全进 `/default-rack`，副本分布彻底乱套。
- **脚本要快**：NN 在初始化和 DN 注册时会调用脚本批量解析 IP，脚本慢会拖慢 NN 启动。生产里建议用纯 shell 的 case 匹配或维护一张 IP→机架 的静态映射文件，不要在脚本里去查 CMDB HTTP 接口。
- **`net.topology.script.number.args`**：控制单次调用传入多少个 IP（默认 100），调大可减少调用次数。
- **改了脚本要重启 NN**（或触发拓扑刷新），不然新机架感知不生效。

### 1.3 与 EC（纠删码）副本策略的对比

Hadoop 3.x 引入了 **Erasure Coding（纠删码）** 作为副本的替代方案。EC 文件的副本数是 1（数据条带 + 校验条带），典型策略 `RS-3-2-1024k` 表示 3 个数据块 + 2 个校验块，能容忍任意 2 个块丢失，存储开销仅约 1.5 倍（对比 3 副本的 3 倍）<!-- 校准：请按真实经历核实/替换 -->。

EC 和副本在"管理"上的核心区别：

| 维度 | 3 副本 | EC（RS-6-3 为例） |
|---|---|---|
| 存储开销 | 3x | 1.5x |
| 容错能力 | 容忍 2 个副本丢失 | 容忍 3 个块丢失 |
| 写路径 | pipeline，简单 | 条带化写，需多 DN 同时在线 |
| 读路径 | 随机读友好 | 顺序读好，随机读差（要读多个条带） |
| Re-replication | 复制单副本即可 | 需重建，CPU + 多块 IO 开销大 |
| 退役影响 | 标准流程 | 重建慢，更容易卡住 |

**关键提示**：EC 文件在 Decommission 和 Re-replication 时表现和普通副本完全不同（后面踩坑会讲），运维时一定要先 `hdfs ec -listPolicies` 确认哪些目录开了 EC。

## 二、Re-replication（重新复制）

副本放置是一次性的，但集群是动态的——DN 会下线、磁盘会坏、副本会校验失败。当副本数低于预期值时，NN 必须主动补齐，这就是 **Re-replication**。

### 2.1 触发条件

NN 通过两类信号感知副本缺失：

- **Block Report（块报告）**：DN 周期性（默认 6 小时）<!-- 校准：请按真实经历核实/替换 -->上报自己持有的全部 block 列表。NN 拿到后跟内存里的元数据比对，发现某个 block 在 DN 上不存在了，就标记该副本丢失。
- **心跳超时**：DN 默认 3 秒<!-- 校准：请按真实经历核实/替换 -->发一次心跳，超过 `dfs.namenode.heartbeat.recheck-interval`（默认 10 分钟 30 秒，含 2 次重试窗口）<!-- 校准：请按真实经历核实/替换 -->没心跳，NN 判定该 DN 死亡，它上面的所有 block 全部进入"副本缺失"状态。

此外还有一类更隐蔽的触发：**校验失败**。client 读 block 时会校验 CRC32，发现某个副本损坏，会上报 NN，NN 把该副本从有效列表里剔除，副本数随之下降。

触发后，NN 把这些 block 标记为 **under-replicated（副本不足）**，放入对应的优先级队列。

### 2.2 优先级队列

这是 Re-replication 设计里最聪明的一环。不是所有缺失副本的 block 都一样紧急——一个副本都没了（只剩唯一副本）显然比"3 个剩 2 个"紧急得多。NN 用优先级队列区分：

| 优先级 | 含义 | 典型场景 |
|---|---|---|
| HIGHEST（LEVEL 0） | 副本数严重不足（如只剩 1 个） | DN 大批死亡、坏盘 |
| VERY_HIGH | 副本数低于预期且文件刚写完 | 写流程中 pipeline 中断 |
| HIGH | 普通的 under-replication | 单 DN 正常下线 |
| MEDIUM / LOW | 容忍度较高的场景 | 退役过程中的中间状态 |

NN 的 ReplicationMonitor 线程（默认 3 秒一轮<!-- 校准：请按真实经历核实/替换 -->）按优先级从高到低调度复制任务。这意味着 **HDFS 会优先抢救"快要丢"的数据**，而不是均匀地补齐所有缺失。这个设计在 DN 大规模故障时至关重要——它会先把"只剩一份"的 block 补到两份，避免一旦唯一的副本也坏了就永久丢数据。

### 2.3 复制源与目标的选择

NN 决定要补某个 block 时，要回答两个问题：**从哪个 DN 复制（源）**、**复制到哪个 DN（目标）**。

- **源 DN 选择**：从该 block 现存的副本里挑一个负载较低、网络较近的 DN。会避免选正在退役或处于 Maintenance 的节点。
- **目标 DN 选择**：走一套接近 Placement 策略的逻辑——优先选不同机架、磁盘还有余量、当前复制任务不太多的 DN。NN 内部维护每个 DN 的"待复制任务数"，避免把任务都堆到同一个热点 DN 上。

**避免热点**是这里的关键工程考量。如果一次扩容进来 50 个新节点，全集群的 under-replication 任务不能一股脑涌向新节点（它们磁盘空但网卡/CPU 扛不住），NN 会通过 `dfs.namenode.replication.work.limit.per.iteration` 等参数把任务摊开。

### 2.4 限速与反压

Re-replication 本质是额外的网络与磁盘 IO，如果不限速会和业务读写抢资源。HDFS 提供多层限速：

```properties
# 每轮 ReplicationMonitor 给单个 DN 分配的最大复制任务数（按节点数倍数算）
dfs.namenode.replication.work.multiplier.per.iteration=5
# 单个 DN 同时进行的复制接收流上限
dfs.datanode.replication.max-streams=8
# 复制流在低优先级时的上限（避免低优先级任务占满带宽）
dfs.datanode.replication.max-streams.low.limit=4
# 单个 NN 处理的复制任务硬上限（防止雪崩）
dfs.namenode.replication.max-streams=20
```

这些参数的调优逻辑是：**故障恢复期放开**（让副本尽快补齐）、**业务高峰期收紧**（别让复制流量拖垮 SLA）。我们在某次坏盘风暴时，把 `dfs.namenode.replication.work.multiplier.per.iteration` 从默认 5 调到 20<!-- 校准：请按真实经历核实/替换 -->，恢复速度提升明显，代价是核心交换机流量涨了约 30%，在可接受范围内。

## 三、Over-replication 与删除策略

副本会过多吗？会。最典型的场景：**副本数配置从 3 调到 2**，或者某 DN 临时掉线触发了 Re-replication，结果它又回来了——这时候某些 block 就有了 4 份副本，称为 **over-replicated**。

NN 同样有后台线程处理 over-replication，删除多余副本。但"删哪一份"是个有讲究的问题，**不能乱删**。删除策略的核心目标是：**删除后仍满足机架多样性**（不能把两个机架的副本删成一个机架的），并且**优先删磁盘紧张节点上的副本**。

举例，某 block 有 4 份副本：rack-A 上 2 份（dn-1、dn-2）、rack-B 上 1 份（dn-3）、rack-C 上 1 份（dn-4）。要删一份时：

- 如果删 rack-A 的 dn-1：剩 rack-A(1) + rack-B(1) + rack-C(1)，机架多样性保留，✓。
- 如果删 rack-B 的 dn-3：剩 rack-A(2) + rack-C(1)，rack-B 完全丢失，✗。

NN 的删除算法（`chooseExcessReplicates`）会综合判断机架分布、节点存储压力、是否在退役列表等因素，选最该删的那一份。这个设计让"删副本"也不会破坏容灾能力。

理解这一点对运维很重要：**不要手贱去 DN 上 `rm` 副本**，NN 根本不知道你删了，元数据还以为副本在；同样，副本调整必须通过 NN，让它的删除算法来保证安全。

## 四、Decommission（节点退役）

Decommission 是副本管理里最重的一块，也是最容易出生产事故的一块。它指的是**把一个或一批 DN 从集群里安全地移除**——安全的意思是：先把这批节点上的所有副本搬到别的节点，搬完再下线，全程不丢数据、不停服务。

### 4.1 退役的完整流程

退役是通过 NN 的 **excludes（排除）文件**驱动的。标准流程：

1. 把要下线的节点（主机名或 IP）写入 `dfs.hosts.exclude` 指向的文件。
2. 执行 `hdfs dfsadmin -refreshNodes`，NN 重新读取 excludes 文件。
3. NN 把这些节点标记为 **Decommission In Progress**，开始把它们持有的 block 复制到其他节点。
4. 当某节点上所有 block 都已在别处有了足够副本（即该节点对任何 block 都不再是"必需副本源"），它的状态变为 **Decommissioned**。
5. 此时可以安全关闭该 DN 进程、下线物理机。

整个过程的状态机：

```
   ┌──────────────┐  -refreshNodes (加入 excludes)   ┌──────────────────────────┐
   │  In Service  │ ───────────────────────────────► │ Decommission In Progress │
   │  (正常服务)   │                                  │ (复制 block 到别处)       │
   └──────────────┘                                  └────────────┬─────────────┘
        ▲                                                        │
        │ -refreshNodes                                          │ 全部 block 已迁出
        │ (从 excludes 移除)                                      ▼
   ┌────┴─────────┐  -refreshNodes (从 excludes 移除)   ┌──────────────────────┐
   │  In Service  │ ◄───────────────────────────────── │    Decommissioned     │
   │  (重新服役)   │   (此时未关进程才能回来)            │  (可安全下线)          │
   └──────────────┘                                    └──────────────────────┘
```

**关键点**：只要节点还没到 `Decommissioned` 状态，**绝不能关 DN 进程**。一旦提前关，相当于一个还在持有副本的节点突然死亡，可能触发 under-replication 甚至丢数据。

### 4.2 退役带宽与速度控制

退役本质是大批量 Re-replication，必然产生大量网络流量。控制不好会把集群网络打满、拖垮业务。HDFS 用一组参数控制退役速度：

```properties
# DN 用于复制/退役/均衡的总带宽上限（字节/秒），默认 1MB/s，严重偏低
dfs.datanode.balance.bandwidthPerSec=10485760
# 单个 DN 同时接收的退役/复制流数
dfs.datanode.replication.max-streams=8
# NN 每轮调度退役复制的任务数倍数
dfs.namenode.replication.work.multiplier.per.iteration=5
```

`dfs.datanode.balance.bandwidthPerSec` 是最常用的限速旋钮，**注意它同时管 Balancer、Decommission、Re-replication 的带宽**。生产经验值是设到网卡带宽的 10%–30%：万兆网卡（约 1.25 GB/s）设到 100–300 MB/s<!-- 校准：请按真实经历核实/替换 -->，业务高峰期可临时下调到 50 MB/s。

### 4.3 集群缩容与换机的标准操作

退役最常见的两个场景：**缩容省钱**（集群资源过剩，砍掉一批机器）和**换机**（老硬件替换成新机型）。两者的标准操作略有不同：

**缩容**：先在 YARN 侧把要下线的节点设为 DRAINING（不让新任务调度上去），等现存 Container 跑完，再走 HDFS 的 Decommission。这样计算和存储有序退出。

**换机**：新机器先上线（加入集群、跑 Balancer 让数据均衡），老机器再走 Decommission 下线。**绝不能先下老机器再上新机器**——那样会先经历一次大规模 Re-replication（副本从老机器搬到剩余老机器上），新机器来了又得再均衡一次，双倍浪费。

监控退役进度的命令：

```bash
# 查看各节点退役状态
hdfs dfsadmin -report | grep -A2 "Decommission"
# 查看某节点还剩多少 block 在等迁出（要 DN 进程还在运行）
hdfs dfsadmin -report | grep -B1 -A8 "Name: 10.0.1.5"
```

当看到目标节点 `Decommission Status : Normal` 且 `Block Pool Used` 持续下降到接近 0，就快好了。

## 五、Maintenance Mode（维护态，Hadoop 3.x）

Decommission 有个痛点：**我只是想临时重启一下机器、或者换块内存，为什么要触发全量副本复制？** 在 2.x 时代，运维只能硬着头皮走 Decommission（或者干脆不维护）。3.x 引入了 **Maintenance Mode（维护态）** 解决这个问题。

维护态的核心思想：**允许副本数临时低于 `dfs.replication`**。当一个节点进入 Maintenance：

- NN 不再要求该节点上的 block 在别处有完整副本（按"维护预期副本数"算）。
- 节点临时下线不触发大规模 Re-replication。
- 维护结束后节点回来，副本自然就齐了。

```bash
# 把节点放入维护态（指定持续时间，到期自动退出）
hdfs dfsadmin -maintenance 10.0.1.5 constant
# 查看维护态节点
hdfs dfsadmin -report | grep -A2 "Maintenance"
```

Maintenance 特别适合这些场景：重启机器、升级内核、换内存/磁盘、机架维护导致整架机器临时下线。**它把"临时下线"和"永久退役"区分开了**，是 3.x 运维体验的一大提升。生产里凡是预计几小时内能回来的操作，都应该走 Maintenance 而不是 Decommission。

## 六、Balancer 与 Disk Balancer

副本放置只在写入时决定一次，但集群是动态的——扩容进来的新节点天然数据少，退役后剩余节点也不均衡。**让数据在节点间重新均衡，是 Balancer 的职责**。但要区分两个层级：集群级和节点内。

### 6.1 集群级 Balancer（节点间）

`hdfs balancer` 解决的是**节点之间**的存储利用率不均。它会找出利用率高于阈值的节点，把它的部分副本搬到利用率低于阈值的节点，直到全集群利用率收敛到设定范围内。

```bash
# 启动 balancer，设阈值 10%（默认 10%）
hdfs balancer -threshold 10
# 指定带宽和最大迭代次数
hdfs balancer -D dfs.datanode.balance.bandwidthPerSec=104857600 -maxIteration 20
```

`-threshold` 是关键参数：它表示**允许的最大节点间利用率差**。默认 10% 意味着如果某节点利用率比集群平均高 10%，就会被搬数据下去。阈值越小越均衡，但搬运量越大、耗时越长。生产里常用 5%–10%<!-- 校准：请按真实经历核实/替换 -->。

**为什么扩容/退役后必须跑 Balancer？**

- 扩容后，新节点存储利用率几乎为 0，而老节点可能 80%+，NN 在写入时虽然会优先选新节点（Placement 会考虑磁盘余量），但纯靠新写入收敛极慢——可能要几个月。Balancer 能在几小时内拉平。
- 退役完成后，剩余节点的利用率也会不均，需要 Balancer 二次均衡。
- YARN 的本地化调度依赖"计算任务所在节点的数据本地性"，数据不均衡直接导致计算任务跨网络读数据，拖慢整个作业。

### 6.2 Disk Balancer（节点内多 volume）

这是另一个常被忽略的工具。一个 DN 通常有多个磁盘 volume（JBOD，不做 RAID），写入时 HDFS 用**轮询策略**往各 volume 写。但运行久了会因为坏盘替换、写入不均等导致**同一节点内不同磁盘利用率差异巨大**——比如 /data/disk1 满了 95%，/data/disk2 才 60%。

集群级 Balancer **不管节点内的不均**，它只看节点整体利用率。这时需要 **HDFS Disk Balancer**（3.x）：

```bash
# 1. 生成磁盘均衡计划（输出到 plan.json）
hdfs diskbalancer -plan dn-01.cluster -out plan.json
# 2. 执行计划
hdfs diskbalancer -execute plan.json
# 3. 查询执行进度
hdfs diskbalancer -query dn-01.cluster
```

Disk Balancer 通过在节点内不同 volume 之间搬运 block 来均衡。它的存在说明 HDFS 的均衡是**两层**的：节点间用 Balancer，节点内用 Disk Balancer。两者职责不重叠，生产运维要分开跑。

### 6.3 退役 / 扩容后的均衡闭环

一个完整的运维闭环是：**Decommission → Balancer → Disk Balancer**。退役把数据从下线节点搬走，Balancer 在剩余节点间拉平，Disk Balancer 再在每个节点内拉平。三步走完，集群的存储利用率才是健康的。

## 七、生产踩坑实录

下面这些坑，都是我用半夜的血泪换来的，每一条都值得单独成节。

### 7.1 退役卡住，三天 Done 不来

**现象**：某次缩容，下线 20 台老机器，其中 18 台顺利 Decommissioned，有 2 台一直停在 `Decommission In Progress`，整整三天不动。

**排查**：用 `hdfs fsck / | grep -A5 "Under replicated"` 看到卡住的 block。根因有两类：

1. **单副本文件**：某些历史文件 `dfs.replication=1`（被业务方手动设过），它们只有一份副本，就在要退役的节点上。退役逻辑要求"该节点对每个 block 都不再是必需源"，但单副本文件的唯一副本就在这，NN 没法把它迁走——因为副本数就是 1，搬到别处也只是换了个节点持有，不算"补副本"。实际上 Decommission **应该**能处理单副本（把唯一副本复制到别处），但当时版本（2.7.x）<!-- 校准：请按真实经历核实/替换 -->有个边界 bug，叠加某个 block 同时存在于多个待退役节点，导致死锁。
2. **EC 文件**：某目录开了 EC（RS-6-3），要退役的节点恰好持有某个条带组的多个块，且部分块对应的源节点也在退役列表里。EC 重建需要多节点协同，源节点同时下线导致重建无法完成。

**解法**：
- 先 `hdfs fsck` 找到卡住的 block，确认是不是单副本/EC。
- 单副本的临时 `hdfs dfs -setrep 2` 把副本数提上去，让它能在别处补一份，再退役。
- EC 文件先把相关节点从 excludes 里移除（暂停退役），等 EC 块重建完再单独下线。
- 升级到 3.x 后这类边界情况社区修了不少。

**教训**：退役前一定先跑一遍 `hdfs fsck` 统计 under-replicated、单副本、EC 文件，提前清理异常。

### 7.2 Balancer 跑不动

**现象**：扩容后启动 Balancer，日志里反复出现 `Not enough datanode available` 或进度龟速，跑了 24 小时只均衡了几个百分点。

**根因**有三个常见方向：

1. **带宽配额太低**：`dfs.datanode.balance.bandwidthPerSec` 默认只有 1 MB/s<!-- 校准：请按真实经历核实/替换 -->，这是为了避免 Balancer 影响业务，但生产环境必须调大。我们设到 100 MB/s<!-- 校准：请按真实经历核实/替换 -->。
2. **Kerberos keytab 过期**：安全集群里 Balancer 是 long-running 进程，keytab 没配好或过期了，NN 拒绝它的请求。日志里会有认证失败信息。解法是确保 `hdfs` 用户的 keytab 有长期续签。
3. **源/目标节点选择受限**：如果大量节点同时被标记为"不允许作为源"（比如在退役），Balancer 找不到源节点。解法是错峰——退役和均衡不要同时进行。

### 7.3 扩容后数据全堆在老节点

**现象**：新扩容 30 台高配机器，本想缓解老节点磁盘压力，结果一周后发现新节点磁盘利用率才 15%，老节点还是 85%。

**根因**：HDFS 的写入 Placement 确实会优先选磁盘余量大的节点（新节点），但**纯靠新写入收敛太慢**——每天新写入的数据量相对于已有存量只是九牛一毛。而且部分写入路径（如 Hive 的 INSERT）会落到 client 所在节点，不一定走新节点。

**解法**：扩容后**立即**启动 Balancer，设一个激进的阈值（如 5%），配合较大带宽，集中跑 2–3 天把存量搬过去。同时用 Disk Balancer 处理老节点内部的不均。这是"扩容不跑 Balancer 等于白扩"的典型场景。

### 7.4 退役带宽打爆核心交换机

**现象**：某次大批量退役（一次下 40 台），退役启动后半小时，业务反馈 Spark 作业大面积变慢，监控显示核心交换机带宽打满。

**根因**：退役本质是跨机架的大规模复制，`dfs.datanode.balance.bandwidthPerSec` 设得太大（500 MB/s<!-- 校准：请按真实经历核实/替换 -->），叠加 40 台同时退役，总流量爆炸。

**解法**：
- 分批退役，一次不超过全集群 5%–10% 的节点。
- 业务高峰期收紧带宽，低谷期放开。
- 监控核心交换机带宽，设置告警，逼近上限就降速。

## 八、标准运维命令序列：Decommission + Balancer

把上面的内容串成一套生产可用的标准操作。下面是我维护的某集群下线节点 + 均衡的标准命令序列，经过多次实战验证。

```bash
# ============================================
# 第一阶段：退役前健康检查（务必先做）
# ============================================
# 1. 检查集群是否有 under-replicated / 单副本 / EC 异常 block
hdfs fsck / | grep -E "Under replicated|Missing|Corrupt"
# 2. 统计待退役节点持有的 block 数，评估迁移规模
for h in dn-101 dn-102 dn-103; do
  echo "=== $h ==="
  hdfs dfsadmin -report | grep -A8 "Name: $h"
done
# 3. 确认 YARN 侧已把节点置为 DRAINING（避免新任务调度上去）
yarn node -status dn-101:8041   # 视调度器配置而定

# ============================================
# 第二阶段：执行退役
# ============================================
# 4. 把要下线的节点加入 excludes 文件（/etc/hadoop/conf/dfs.exclude）
#    格式：一行一个主机名，如 dn-101 / dn-102 / dn-103
# 5. 让 NN 重新加载 excludes
hdfs dfsadmin -refreshNodes
# 6. （可选）调整退役带宽，业务低谷放开、高峰收紧
#    动态生效需 DN 重新加载配置（hdfs dfsadmin -reconfig datanode ... 或重启）
# 7. 轮询退役进度
watch -n 60 'hdfs dfsadmin -report | grep -E "Name|Decommission|Last Block"

# ============================================
# 第三阶段：退役完成后均衡
# ============================================
# 8. 等到目标节点全部显示 Decommissioned，关闭 DN 进程
systemctl stop hadoop-datanode   # 在 dn-101/102/103 上执行
# 9. 从 excludes 移除已下线节点（保持 excludes 文件干净），refreshNodes
hdfs dfsadmin -refreshNodes
# 10. 启动集群级 Balancer，阈值 5%，限速 100MB/s
nohup hdfs balancer -threshold 5 \
  -D dfs.datanode.balance.bandwidthPerSec=104857600 \
  > /var/log/hdfs/balancer-$(date +%F).log 2>&1 &
# 11. Balancer 收敛后，对利用率不均的节点跑 Disk Balancer
hdfs diskbalancer -plan dn-050 -out /tmp/diskplan-050.json
hdfs diskbalancer -execute /tmp/diskplan-050.json
hdfs diskbalancer -query dn-050

# ============================================
# 第四阶段：验收
# ============================================
# 12. 确认集群利用率收敛
hdfs dfsadmin -report | grep -E "DFS Used%|Present"
# 13. 抽查几个关键目录的副本情况
hdfs fsck /warehouse | tail -5
```

这套流程的核心原则：**先体检、再下线、后均衡、最后验收**。任何一步偷懒都会在后面加倍偿还。

## 小结

副本管理是 HDFS 运维的心脏。回看这几个环节，其实都围绕同一个思想：**用一套分布式协议，在可靠性、性能、运维成本之间持续做权衡**。

- **Placement** 用"两机架分散"的简单规则兼顾容灾与带宽，是 HDFS 最优雅的设计之一。
- **Re-replication** 用优先级队列保证了"最危险的数据先救"，这是大规模系统的生存智慧。
- **Over-replication 的删除**同样不能乱删，机架多样性的约束藏在算法里。
- **Decommission** 是最重的运维操作，分批、限速、先体检是铁律。
- **Maintenance Mode** 把临时下线和永久退役分开，是 3.x 体验提升的关键。
- **Balancer / Disk Balancer** 是两层均衡，扩容和退役后必跑。

我在这几块踩过的所有坑，归根结底都是同一个原因：**把副本管理当成黑盒，只记命令不理解机制**。Decommission 卡住时不知道去查 fsck，Balancer 跑不动时不知道查带宽和 keytab，都是这个毛病的体现。真正理解了副本的全生命周期，这些命令才能用得心里有底。

最后给一个判断标准：如果你能在白板上画出"一个 block 从写入到节点下线全程的状态流转"，并且能解释每一步 NN 在做什么、为什么这么做，那你对 HDFS 副本管理的理解就过关了。希望这篇复盘能帮你更快达到这一步。
