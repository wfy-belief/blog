---
title: HDFS 快照、加密区与配额：数据保护与多租户
abbrlink: hadoop-hdfs-snapshot-encryption-quota
date: 2026-07-02 17:30:00
tags:
  - Hadoop
  - HDFS
  - 治理
categories:
  - 技术
description: 用快照防误删、加密区护敏感数据、配额管多租户
ai_text: "企业级 HDFS 光能存不够，还得防得住误删、防得住泄露、管得住多租户。本文以实战复盘口吻，讲透三件事：快照基于 copy-on-write 的零拷贝原理与误删恢复、透明加密区端到端 EDEK 与 KMS 的协作机制及踩坑、Name/Space/Storage Type 三类配额的多租户用法。三者组合起来，才是安全可控的企业存储底座。"
---

## 引言

写过 HDFS 架构、写过读写路径、写过一致性，这一篇我想聊聊另一类问题：**数据治理**。在一个真正上了规模的生产集群上，"能存、能读、能写"只是入门，真正让人头疼的往往是这三件事——

- **误删**：某个同学的 `hdfs dfs -rm -r` 多敲了一个路径，PB 级数据秒级消失。回收站未必救得了跨任务的大批量删除。
- **泄露**：用户明细、财务数据、模型权重躺在 HDFS 上，谁都能 `cat`，合规审计过不了。
- **挤占**：A 业务一次性灌了几百 TB 把磁盘打满，B 业务的实时任务跟着写失败。

这三类问题分别对应 HDFS 的三个治理能力：**快照（Snapshot）防误删、透明加密区（Encryption Zone）防泄露、配额（Quota）防占用**。我在某集群上把这三件武器配齐，前后花了大概两个迭代 <!-- 校准：请按真实经历核实/替换 -->，踩过不少坑。本篇把原理和生产用法都讲透，区别于网上那种只贴命令的教程，我会重点讲清"为什么这么设计"以及"线上怎么用才不翻车"。

## 一、快照 Snapshot

### 1.1 原理：copy-on-write 的零拷贝只读副本

快照的核心机制是 **copy-on-write（写时复制）**，但 HDFS 的实现方式比很多人想象的更轻量——它**根本不拷贝数据**。

开启快照能力的目录，NameNode 会在其 inode 上挂一个 `SnapshotManager`。对一个目录打快照，NN 只是记录下"当前这个目录树下，每个文件/目录的 inode 列表和元数据状态"这样一个逻辑视图，对应的数据块（block）一个都不动。真正的开销发生在**后续修改时**：

- 当被快照引用的文件被 **删除或截断** 时，NN 不能真正释放这些 block，而是让它们继续被快照"锚住"，物理上保留。
- 当被快照引用的文件被 **重命名或追加** 时，NN 通过差异记录（snapshot diff）维护快照视图的不变性。

这就是为什么官方说快照"几乎不占空间"——它只占一份元数据，数据空间只有在源文件被改写时才"锁住"旧版本。

```
         开启 snapshot 的目录 /finance
                   │
   createSnapshot  │   t0: 快照 s0 (逻辑视图: A,B,C + 各自 block 列表)
   ───────────────►│
                   │
   用户删除文件 C   │   t1: 源目录只剩 A,B；但 s0 仍引用 C 的 block
                   │        → C 的 block 不释放，仍占物理空间
   ───────────────►│
                   │
   从 s0 恢复       │   通过 snapshot path /finance/.snapshot/s0/C 读回
```

关键结论：**快照不是备份，它和源数据共享 block，源盘坏了快照也救不了**。要防物理故障仍需异地副本/EC/ DistCp 到另一集群。快照防的是"逻辑误操作"——误删、误改、误覆盖。

### 1.2 基本操作

先对一个目录开启快照能力（注意是"能力"，不是打快照本身）：

```bash
# 允许某目录打快照
hdfs dfsadmin -allowSnapshot /finance

# 创建一个快照（名字可选，不指定则自动 s20260702-xxxx）
hdfs dfs -createSnapshot /finance finance_s_20260702

# 重命名快照
hdfs dfs -renameSnapshot /finance finance_s_20260702 finance_daily

# 删除快照（删除后，仅被该快照锚住的 block 才会真正释放）
hdfs dfs -deleteSnapshot /finance finance_daily

# 列出某目录所有快照
hdfs lsSnapshottableDir

# 禁用快照能力（前提是该目录已无快照）
hdfs dfsadmin -disallowSnapshot /finance
```

访问快照里的文件用 `.snapshot` 这个保留路径，比如 `/finance/.snapshot/finance_daily/report.parquet`。这个路径对普通 `hdfs dfs -cat`、`-get`、MapReduce/Spark 输入都透明可用。

### 1.3 误删恢复：先 diff 后恢复

生产里我用得最多的是 **`getSnapshotDiff`**。某天审计发现 `/finance` 下少了几个分区，先别急着恢复，先看清楚到底被改了什么：

```bash
hdfs snapshotDiff /finance finance_daily finance_s_20260702
# 输出 + 表示新增、- 表示删除、M 表示修改、R 表示重命名
```

拿到 diff 列表，就能精准 `hdfs dfs -cp /finance/.snapshot/finance_daily/被删文件 /finance/` 定点恢复，而不是整目录覆盖（整目录覆盖会丢掉快照之后正常写入的新数据）。

**定期快照策略**上，我们约定每个关键业务目录每天打一个快照、保留 7 天，每周打一个保留 4 周 <!-- 校准：请按真实经历核实/替换 -->，用 crontab + 一段 shell 调 `createSnapshot`/`deleteSnapshot` 滚动维护。不要手写脚本去遍历删旧快照，社区有现成的 `hdfs snapshotDiff` 配合 `ozone` 之类的工具，也可以简单点直接按命名规则 grep 删。

### 1.4 快照对删除的影响：空间不会真的释放

这是最容易踩的坑。很多人以为"我 `rm` 了 100TB，磁盘该腾出来了吧"——结果磁盘还是满的，因为某个快照还锚着这些 block。

```
NameNode 块回收逻辑（简化）:
  文件被 rm → 检查是否仍被任何 snapshot 引用
            ├─ 是 → block 进入 INVALIDATE 队列但保留(或只标记删除pending)
            └─ 否 → 通知 DN 物理删除
```

所以**快照策略要和容量规划一起做**：保留多少天的快照，就等于可能多锁住多少倍被改写的数据。对于写多改少、又有大量 append 的业务目录，快照保留期不能拉太长，否则 NN 端的 `BlocksMap` 和 DN 磁盘都会吃紧。

### 1.5 配合 DistCp 做增量同步

这是快照一个被低估的用法。跨集群同步数据时，传统 DistCp 要全量比对，慢且吃带宽。如果两端都开了快照，DistCp 支持基于 **snapdiff（快照差异）** 做增量同步：

```bash
# 源集群先打两个快照，间隔为同步周期
hdfs dfs -createSnapshot /src src_t0
# ... 一个周期后 ...
hdfs dfs -createSnapshot /src src_t1

# 用 diff 选项只同步变化部分到目标集群
hadoop distcp -update -diff src_t0 src_t1 \
  hdfs://srcCluster/src hdfs://dstCluster/dst
```

NN 直接吐出两个快照间的 inode 差异列表，DistCp 只拷贝增删改的部分。在 PB 级目录、每天只变几个分区的场景下，同步时间从小时级降到分钟级 <!-- 校准：请按真实经历核实/替换 -->。

## 二、透明加密区 Encryption Zone

### 2.1 原理：端到端的 EDEK + KMS

HDFS Transparent Encryption 叫"透明"，是因为**对上层应用完全无感**——同一个 `hdfs dfs -cat`，在加密区内外写法一模一样，加解密由 HDFS 在底层完成。但底层其实是一条精心设计的端到端链路。

先理清三个密钥的层级：

```
┌─────────────────────────────────────────────────────────────┐
│  层级         密钥                存放位置                   │
├─────────────────────────────────────────────────────────────┤
│  L1  Master Key (EZ Key)     KMS（Key Management Server）   │
│  L2  EDEK (加密的数据加密密钥)  NameNode 元数据中（已加密）  │
│  L3  DEK (真正的数据加密密钥)  仅在 client 内存中（明文）    │
└─────────────────────────────────────────────────────────────┘
```

- **EZ Key（Encryption Zone Key）**：每个加密区绑定一个，由 KMS 管理，永不离开 KMS 明文。
- **EDEK（Encrypted Data Encryption Key）**：创建加密区或新建文件时，KMS 用 EZ Key 加密一个随机生成的 DEK，把加密后的 EDEK **存在 NN 的文件元数据里**。NN 永远拿不到明文 DEK。
- **DEK（Data Encryption Key）**：真正用来加解密文件内容的对称密钥。client 读文件时，把 EDEK 发给 KMS 解密成 DEK，DEK 只在 client 进程内存里短暂存在，用来对落盘/读取的字节流做 AES 加解密。

整条链路的关键设计是：**DataNode 拿到的永远是密文**，NN 拿到的永远是密文（EDEK），只有持有合法 Kerberos 身份、且 KMS 授权的 client 才能解出 DEK。这保证了即便 DN 磁盘被拔走、NN 元数据被偷，攻击者也解不开数据。

```
  client 写文件到加密区:
    1. create /ez/secret.txt
    2. NN: 该路径在加密区 → 向 KMS 请求生成 EDEK (用 EZ Key 包装)
    3. KMS 返回 EDEK 给 NN，NN 存入文件元数据
    4. client 从 KMS 解 EDEK → DEK (仅内存)
    5. client 用 DEK 加密字节流 → DN 落盘密文

  client 读文件:
    1. open /ez/secret.txt
    2. NN 返回 EDEK + 块位置
    3. client 从 KMS 解 EDEK → DEK
    4. 从 DN 读密文块 → DEK 解密 → 上层拿到明文
```

### 2.2 基本操作

```bash
# 1. 在 KMS 创建一个 EZ 密钥（密钥名 = 加密区的身份）
hadoop key create finance_ezkey

# 列出 KMS 中的密钥
hadoop key list

# 2. 把目录设为加密区，绑定密钥
hdfs crypto -createEncryptionZone -path /finance -keyName finance_ezkey

# 列出所有加密区
hdfs crypto -listZones

# 查看某文件加密信息
hdfs crypto -getFileEncryptionInfo -path /finance/report.parquet
```

之后所有写进 `/finance` 的文件自动加密，读出来自动解密，对 Spark/Hive/Flink 完全透明。

### 2.3 与 Kerberos / Ranger 配合

加密区不是孤立的访问控制，它必须和 **Kerberos 认证 + Ranger 鉴权** 一起才构成完整闭环：

- **Kerberos** 解决"你是谁"——没有合法 TGT，连 KMS 都进不去，更别说解 EDEK。
- **Ranger** 解决"你能干什么"——即便你有 Kerberos 身份，Ranger 策略没授权你读 `/finance`，NN 直接拒绝 open，根本到不了 KMS 解密那一步。
- **加密区** 解决"绕过 HDFS 还能不能读"——哪怕有人把 DN 磁盘挂到别的机器上，没有 EZ Key 就解不开。

三者叠加：**谁有 key 谁能读**，而"谁有 key"又被 Kerberos + Ranger 严格约束。合规审计时，"敏感数据访问 = Ranger 审计日志 + KMS 解密日志"两份对账，几乎不可抵赖。

### 2.4 踩坑实录

**坑一：KMS 单点**。KMS 挂了，所有加密区的读写都会卡在"解 EDEK"那一步。生产上 KMS 必须 HA，至少 2 个实例前置负载均衡。我们初期只起了一个 KMS，一次 GC 停顿 8 秒 <!-- 校准：请按真实经历核实/替换 -->，整集群加密区读写集体超时，教训深刻。

**坑二：性能开销**。加解密是 CPU 密集型。client 侧默认用 AES-CTR，JDK 没开硬件加速时吞吐能掉 15%~30% <!-- 校准：请按真实经历核实/替换 -->。务必确认 JDK 启用了 AES-NI（`-XX:+UseAES -XX:+UseAESIntrinsics`），并在 KMS/client 节点观察 CPU。

**坑三：密钥轮换**。合规要求定期换 EZ Key，但**轮换 EZ Key 只影响新创建文件的 EDEK**，老文件的 EDEK 是用旧 key 包装的，KMS 必须同时保留新旧 key 才能读老文件。别以为 `rollover` 完就能把旧 key 删了——删了等于老文件全读不出来。

**坑四：加密区文件不能跨区 move**。`hdfs dfs -mv /finance/a.txt /public/` 会失败，因为源文件带 EDEK、目标目录不是同一个加密区，HDFS 不允许密钥"泄漏"到区外。必须先 `get` 到本地再 `put`，或用 DistCp 显式 re-encrypt。

## 三、配额 Quota

配额是 HDFS 多租户治理的根基。一个集群跑十几条业务线，没有配额就是"谁先灌满谁赢"。

### 3.1 三类配额

| 配额类型 | 限制对象 | 命令 | 典型场景 |
|---------|---------|------|---------|
| **Name Quota** | 目录下文件+子目录**个数** | `hdfs dfsadmin -setQuota 100000 /path` | 防小文件洪水 |
| **Space Quota** | 目录下总**字节数** | `hdfs dfsadmin -setSpaceQuota 500t /path` | 防容量挤占 |
| **Storage Type Quota**（3.x） | 按 **STORAGE_TYPE** 分别限字节 | `hdfs dfsadmin -setQuotaByStorageType -storageType DISK 500t /path` | 分层存储管控 |

查询当前配额与使用量：

```bash
hdfs dfs -count -q /finance
# 输出: NONE(剩余文件数) QUOTA(文件数上限) SPACE_REMAINING SPACE_QUOTA DIR_COUNT FILE_COUNT SPACE_USED PATH
```

### 3.2 Name Quota：防小文件

Name Quota 限制的是 inode 数量。NN 内存里每个文件/目录占约 150~300 字节元数据 <!-- 校准：请按真实经历核实/替换 -->，小文件过多会撑爆 NN 堆。给每个业务目录设一个 Name Quota 上限，等于在 NN 内存层面划红线：超了写入直接失败，逼着业务去做合并或走 HBase。

### 3.3 Space Quota：防容量挤占

Space Quota 限制的是逻辑字节数（副本数也算）。给 `/finance` 设 `500t`，意味着该目录下数据加起来不能超过 500TB（含三副本就是 500TB 物理占用 <!-- 校准：请按真实经历核实/替换 -->，注意 HDFS 配额计的是逻辑字节 × 副本数后的物理占用，具体口径以版本为准）。

### 3.4 Storage Type Quota：分层治理

3.x 引入存储类型后，可以按 DISK/SSD/ARCHIVE 分别限额。典型用法：热数据目录限 SSD 配额、冷数据目录限 ARCHIVE 配额，逼业务把冷数据走迁移策略落到归约存储。

```bash
# /hot 只能用 50TB SSD
hdfs dfsadmin -setQuotaByStorageType -storageType SSD 50t /hot

# /archive 只能用 1PB ARCHIVE（多为 EC 后的冷数据）
hdfs dfsadmin -setQuotaByStorageType -storageType ARCHIVE 1p /archive
```

### 3.5 多租户实践

我们的做法是**按业务线建独立 namespace 子目录 + 独立配额**：

```
/                          ← 集群根
├── /biz_finance           ← Name Quota 50w, Space Quota 500t
├── /biz_recommend         ← Name Quota 100w, Space Quota 1p
├── /biz_ai_model          ← 大文件少，但单文件大，Space Quota 300t
└── /tmp                   ← 强制 Space Quota 10t，定期清理
```

每个业务目录再配独立的 Ranger 策略、独立快照计划、必要时独立加密区。业务线之间天然隔离，A 业务再怎么失控也只撞自己的配额墙，不会拖垮 B。

### 3.6 踩坑

**坑一：配额超限导致写入失败**。Spark/Flink 任务写到一半撞 Space Quota，文件处于半截状态，重试又重新创建。务必在任务侧做好配额预检查，监控每个关键目录的 quota 使用率（我们接了 Prometheus，超 85% 告警 <!-- 校准：请按真实经历核实/替换 -->）。

**坑二：EC 文件的配额计算**。开了纠删码（Erasure Coding）的文件，Space Quota **按编码后大小算**，不是原始大小。比如 RS-6-3 把 6 块数据编码成 9 块（6 数据 + 3 校验），占用是原始的 1.5 倍而非 3 倍。设配额时要换算清楚，否则你以为能写 100TB，实际只能写约 66TB <!-- 校准：请按真实经历核实/替换 -->。

**坑三：快照占用的空间**。前面说过，被快照锚住的 block 不释放，这部分空间**也算在 Space Quota 里**。如果某目录快照保留期长、数据改写频繁，配额会被快照"吃掉"一大块，业务实际可用空间比配额数字小得多。监控时要看 `SPACE_USED` 的真实值，别只盯配额上限。

## 四、组合实战：一段命令序列

下面这段是为某业务目录 `/biz_finance` 同时配上快照、加密区、配额的完整序列，可作为生产上线 checklist：

```bash
# ===== 0. 前置：Kerberos 已启用、Ranger 策略已下发 =====

# ===== 1. 建目录 =====
hdfs dfs -mkdir -p /biz_finance

# ===== 2. 创建加密区密钥并绑定 =====
hadoop key create finance_ezkey
hdfs crypto -createEncryptionZone -path /biz_finance -keyName finance_ezkey

# ===== 3. 设配额 =====
hdfs dfsadmin -setQuota 500000 /biz_finance          # 50 万个文件/目录
hdfs dfsadmin -setSpaceQuota 500t /biz_finance       # 500TB 物理占用
hdfs dfsadmin -setQuotaByStorageType -storageType DISK 500t /biz_finance

# ===== 4. 开启快照能力并打第一个基线快照 =====
hdfs dfsadmin -allowSnapshot /biz_finance
hdfs dfs -createSnapshot /biz_finance baseline_$(date +%Y%m%d)

# ===== 5. 验证 =====
hdfs crypto -listZones
hdfs dfs -count -q /biz_finance
hdfs dfs -ls /biz_finance/.snapshot
```

注意顺序：**先建加密区再写数据**，已经存在的明文文件不会被自动加密；**配额在写数据前设好**，避免业务灌满才发现没限额；**快照基线在治理就绪后打**，作为后续 diff 的参照。

## 小结

这三件事看似各自独立，其实串成一条治理主线：

- **快照**给数据上了"撤销键"，逻辑误操作秒级回滚，但要记得它和源数据共盘，不防物理故障。
- **透明加密区**给数据上了"防读锁"，DN 被拔盘也解不开，但要保证 KMS 的 HA 与密钥的妥善保管。
- **配额**给集群上了"容量闸"，多租户各管一摊互不干扰，但要算清 EC 与快照对实际占用的影响。

三者组合起来，HDFS 才从一个"能存的文件系统"变成"安全可控的企业存储底座"。在我运维的某集群上，这套治理体系上线后，误删恢复从"翻备份要半天"变成"一条 `snapshotDiff` 几分钟定位" <!-- 校准：请按真实经历核实/替换 -->；敏感数据合规审计顺利通过；容量事故从月均两三起到基本归零 <!-- 校准：请按真实经历核实/替换 -->。这三件武器，值得每个 HDFS 运维者认真吃透。

—— 王飞宇，写于某次治理复盘之后。
