---
title: HDFS 存储策略与分层存储
abbrlink: hadoop-hdfs-storage-tiering
date: 2026-07-02 18:00:00
tags:
  - Hadoop
  - HDFS
  - 分层存储
categories:
  - 技术
description: 用 Storage Policy 与异构磁盘构建 HDFS 冷热分层
ai_text: "本文以第一人称复盘某 PB 级集群用 HDFS Storage Policy 与异构介质（DISK/SSD/ARCHIVE）构建冷热分层的实战经验。从 DataNode 存储类型标注、内置 Storage Policy 选型、Mover 数据迁移讲起，落到按时间分区做 HOT/COLD+EC 的分层方案、LAZY_PERSIST 内存写、与对象存储归档的配合，最后沉淀出介质配比、Storage Type Quota 与多类生产踩坑，给出可落地的分层决策框架。"
---

# HDFS 存储策略与分层存储：用异构介质给数据按温度落盘

## 引言

接手某集群做容量盘点时，一个数字让我至今印象深刻：全集群约 **3 PB** 的 HDFS 数据里，最近 30 天被读过的还不到 **12%**<!-- 校准：请按真实经历核实/替换 -->。也就是说，将近九成的数据是冷的——它们躺在和热数据一模一样的高转速企业级 HDD 上，甚至还有一部分被业务方"图快"写到了 SSD 上。存储成本和访问温度严重错配，这是几乎所有上了规模的 Hadoop 集群都会撞上的问题。

硬件层面，数据中心里冷数据天然该用大容量、低单 TB 成本的盘；热数据该用低延迟、高 IOPS 的盘。HDFS 从 2.x 开始就提供了完整的分层存储（Storage Tiering）能力——通过 Storage Type 描述异构介质、用 Storage Policy 声明数据的温度偏好、用 Mover 在介质间搬运数据。这篇复盘，我把这套机制在某集群从试点到全量铺开的工程实践系统写出来，重点讲清楚冷热分层怎么真正落地，而不是"配了 policy 就当分层用了"。

和此前写过的"湖仓演进""Hadoop 3.x 新特性"两篇侧重存储趋势不同，这篇**专攻 HDFS 内部的存储策略与分层存储机制本身**——Storage Type、Storage Policy、Mover、ARCHIVE 磁盘、LAZY_PERSIST，以及它们与 EC、对象存储归档如何配合。

## 一、Storage Type：让 DataNode 认得异构介质

### 1.1 四种存储类型

分层存储的第一步，是让 HDFS 知道每台 DataNode 上都挂了什么盘。HDFS 把存储介质抽象成四类 Storage Type：

| Storage Type | 典型介质 | 单 TB 成本 | IOPS/延迟 | 定位 |
|---|---|---|---|---|
| `DISK` | 企业级 HDD（如 2T/4T/8T 7.2K） | 中 | 中 | 默认通用存储 |
| `SSD` | NVMe / SATA SSD | 高 | 高 | 热数据、低延迟读 |
| `ARCHIVE` | 高密度 HDD（如 12T/16T SMR 或大容量企业盘） | 极低 | 低 | 冷数据归档 |
| `RAM_DISK` | DataNode 本机内存（tmpfs） | 极高 | 极高 | 临时数据加速写 |

需要注意的是，这里的 SSD/ARCHIVE **是逻辑标签，不是物理探测**。HDFS 不会去问盘的型号，它只看你把哪个目录标注成哪一类——你完全可以把一块慢盘标成 SSD（虽然没有任何意义），HDFS 也会照单全收。这给了我们很大的灵活性，但也意味着配错就是灾难。

### 1.2 在 `dfs.datanode.data.dir` 里标注介质

关键配置在 `hdfs-site.xml` 的 `dfs.datanode.data.dir`。每个目录前面用方括号标注 Storage Type，DataNode 启动时就会把这块盘登记成对应类型：

```xml
<property>
  <name>dfs.datanode.data.dir</name>
  <value>
    [DISK]/data/dn/disk1,
    [DISK]/data/dn/disk2,
    [DISK]/data/dn/disk3,
    [DISK]/data/dn/disk4,
    [SSD]/data/dn/ssd1,
    [SSD]/data/dn/ssd2,
    [ARCHIVE]/data/dn/arch1,
    [ARCHIVE]/data/dn/arch2
  </value>
</property>
```

上面这台 DataNode 就是一个典型的"混搭"机器：4 块 HDD 做热存储、2 块 SSD 承接低延迟读、2 块大容量盘做冷存储。生产里更常见的做法是**机型分层**而不是单机混搭——专门有一批"ARCHIVE 节点"全是大容量盘，一批"SSD 节点"全是 NVMe，这样硬件采购、机柜供电、故障域都更清晰。我倾向用后者，单机混搭主要用在中小集群或测试环境。

配置完重启 DataNode 后，用 `hdfs fsck` 或 NameNode JMX 可以看到每个 DataNode 报告上来的 `StorageReport`，里面有 `DfsUsed`/`Capacity`/`StorageType` 字段。这就是分层的基础数据。

## 二、Storage Policy：声明数据的温度偏好

### 2.1 内置的七种策略

有了介质类型，下一步是告诉 HDFS"这类数据应该落在哪类介质上"。这就是 Storage Policy。HDFS 内置了七种，覆盖了绝大多数场景：

| Policy | 副本放置策略（按副本序） | 适用场景 |
|---|---|---|
| `HOT`（默认） | 全部放 DISK | 通用热数据 |
| `WARM` | 第 1 副本 DISK，其余 ARCHIVE | 温数据，热读但可接受冷备 |
| `COLD` | 全部放 ARCHIVE | 冷数据归档 |
| `ALL_SSD` | 全部放 SSD | 低延迟读敏感 |
| `ONE_SSD` | 第 1 副本 SSD，其余 DISK | 折中：读快又省 SSD |
| `LAZY_PERSIST` | 写 RAM_DISK，异步落 DISK | 临时数据加速 |
| `Provided`（3.x） | 数据在外部存储（如对象存储） | 见湖仓篇，本文不展开 |

理解这几种策略的关键是**"首选介质"与"回退"**。比如 `ONE_SSD`，HDFS 在写第一个副本时会优先选 SSD；如果当时集群上所有 SSD 都写满了或没有 SSD 节点，它不会失败，而是回退到 DISK。这种回退是隐式的，也是后面要讲的踩坑重灾区——你以为数据落 SSD 了，其实可能因为 SSD 写满，悄悄落回了 DISK。

### 2.2 给目录/文件打 Policy

Storage Policy 是按目录或文件粒度设置的，配合数仓的分区目录结构用起来非常顺手。命名空间上设置 policy 是个轻量操作（只改 inode 的扩展属性），不影响数据本身：

```bash
# 看某个目录当前的 policy
hdfs storagepolicies -getStoragePolicy -path /warehouse/ods/events

# 给热分区打 HOT
hdfs storagepolicies -setStoragePolicy -path /warehouse/ods/events/dt=2026-07 -policy HOT

# 给历史分区打 COLD
hdfs storagepolicies -setStoragePolicy -path /warehouse/ods/events/dt=2025-01 -policy COLD

# 列出所有可用 policy
hdfs storagepolicies -listPolicies
```

这里有一个**新手最常误解的点**：`setStoragePolicy` 只是"贴标签"，**它本身不会搬运任何数据**。数据到底落在哪个介质，分两种情况：

1. **新写入的数据**：客户端写 Block 时，NameNode 根据目标目录/文件的 Policy 选择对应 Storage Type 的 DataNode，副本按 policy 的介质偏好放置。所以新数据会按 policy 落层。
2. **已经存在的数据**：已经写好的 Block 不会因为改了 policy 就自动搬家。它们留在原来的介质上，必须靠 **Mover** 这种离线工具主动迁移。

这条"贴标签不搬家"的特性，是我见过最多人踩的坑。下面专门讲 Mover。

## 三、Mover：介质间的数据搬运工

### 3.1 Mover 与 Balancer 的区别

HDFS 里有两个名字很像、容易混淆的工具，必须先区分清楚：

| 工具 | 目标 | 触发依据 | 调度粒度 |
|---|---|---|---|
| `Balancer` | DataNode **节点间**均衡磁盘利用率 | 各节点利用率与集群均值的偏差 | 按 DataNode |
| `Mover` | 数据按 **Storage Policy** 落到对应介质 | 目录/文件的 Storage Policy | 按 Block，跨介质/跨节点 |

一句话：**Balancer 解决"节点间不均"，Mover 解决"数据没在它该在的介质上"**。两者可以同时跑，但 Mover 优先级更高，因为它在纠正 policy 与实际介质的偏差。

### 3.2 跑一次 Mover

Mover 是一个独立的守护进程，跑在集群任一节点上即可（不要跑在 NameNode 上抢资源）：

```bash
# 对指定路径按 policy 做迁移（最常用）
hdfs mover -p /warehouse/ods/events/dt=2025-01

# 不指定路径，全集群扫描（生产慎用，开销大）
hdfs mover

# 控制并发与带宽，避免压垮在线业务
# 在 hdfs-site.xml 里调：
# dfs.datanode.balance.max.concurrent.moves  （单 DN 并发移动数，默认 5）
# dfs.balancer.mover.max.concurrent.movesPerMn  （每 NN 并发，默认 1000）
# dfs.mover.max.iterations  （一轮扫描迭代次数）
```

Mover 的工作流程是：扫描目标路径下所有 Block → 查每个 Block 的 policy → 比对当前副本所在介质与 policy 偏好 → 不一致就发起一次"复制到目标介质 → 删除老副本"的搬迁。这个过程会占用网络与磁盘 IO，所以在生产上一定要做两件事：

- **限带宽、限并发**：上面那几个参数按集群规模压一压，别开默认值。
- **错峰跑**：Mover 一般挂在夜间低峰，配合一个 cron 或 Airflow DAG 触发，跑完自动退出。

### 3.3 Mover 的"幂等"特性

Mover 是幂等的——同一路径反复跑，结果一致。这点在自动化调度里很重要。我们生产上有一个每天凌晨 2 点触发的 Mover DAG，针对前一天刚切到 COLD 的新历史分区跑一次，把上一节贴了标签但没搬的数据真正落到 ARCHIVE。这个流水线稳定跑了快两年。

## 四、冷热分层实战：按时间分区 + EC + ARCHIVE

讲完机制，落到真正的生产场景。这是某集群分层方案的骨架，也是最值得参考的部分。

### 4.1 按时间分区的温度曲线

数仓的 ODS/DWD 层几乎都是按天分区（`dt=YYYY-MM-DD`）。这类数据的访问温度有一条非常规律的曲线：

```
访问频率
  ▲
  │ ■■■
  │ ████■
  │ ███████■
  │ ███████████■■
  │ ██████████████████■■■■■■■■■■■
  │ ████████████████████████████████████████████████████
  └──────────────────────────────────────────────────────► 时间
   当天  T+1~7  T+8~30   T+31~90    T+90+（冷归档）
   HOT   HOT    HOT→WARM  COLD       COLD + EC
```

具体到我们的策略：当天到 T+7 的分区保持 `HOT`；T+8 到 T+30 视业务热度切到 `WARM` 或保持 `HOT`；超过 T+30 的历史分区全部切 `COLD`，并对 EC 友好的大文件启用 EC。这条曲线的阈值是按业务真实访问日志统计出来的，**不是拍脑袋**——我们埋点了所有 Hive/Spark 作业读取的分区路径，跑了一个月统计出每天的访问次数分布，再据此标定拐点。

### 4.2 ARCHIVE 节点的硬件选型

ARCHIVE 节点的核心诉求是**单 TB 成本最低**。我们选的机型是 12 块 16TB 大容量企业盘（部分机型用 SMR），单节点裸容量约 192TB<!-- 校准：请按真实经历核实/替换 -->，CPU/内存配得很省（几十核、128G），因为冷数据几乎没有计算压力。和热节点（NVMe + 高主频 CPU）相比，ARCHIVE 节点的单 TB 成本能压到热节点的 1/4 左右<!-- 校准：请按真实经历核实/替换 -->，这是分层存储最直接的成本收益。

ARCHIVE 节点在 `dfs.datanode.data.dir` 里全部标 `[ARCHIVE]`，DataNode 启动后向 NameNode 报告的就是纯 ARCHIVE 容量。这种"纯冷节点"的好处是故障域清晰：坏一台就是 192TB 冷数据副本少一份，影响面可控；坏处是机架规划要小心，下面讲。

### 4.3 COLD + EC 的机架要求

把冷数据切到 `COLD` policy 后，所有副本都要落 ARCHIVE 介质。但**纯 COLD 用三副本其实很亏**——冷数据三副本意味着 3 倍的归档盘成本，而冷数据的访问频率又低，三副本的容错收益在这里性价比很差。

这时候就要上 EC（Erasure Coding，纠删码）。Hadoop 3.x 的 EC 典型策略如 `RS-6-3`（6 个数据块 + 3 个校验块），存储开销仅 1.5 倍，比三副本省 50%<!-- 校准：请按真实经历核实/替换 -->。但 EC 有一个硬约束：**条带里的所有块必须分布在不同机架上**（每个块至少要能容忍丢一个机架）。这就要求 ARCHIVE 节点**至少跨 3 个机架**，每个机架有足够多的 ARCHIVE 节点。

我们早期试点时吃过亏：当时只有两个机架的 ARCHIVE 节点，EC 写入直接失败报"not enough racks"。后来补到 4 个机架才稳。所以**分层存储落地前，ARCHIVE 机架拓扑要提前规划**，别等 policy 配好才发现硬件不够。

### 4.4 与对象存储归档配合

再冷一层，我们做了"超冷归档"：超过 T+180 的数据，如果业务几乎不读，就下沉到对象存储（生产用的是 OSS<!-- 校准：请按真实经历核实/替换 -->）。这部分严格说已经超出 HDFS 内部分层范畴，但和分层是一个连续的曲线，必须一起规划。

落地的几种模式：

- **S3A/OSS 直挂**：Hadoop 3.x 的 S3A/OSS connector 让 Spark/Hive 能直接读对象存储路径。我们把超冷分区从 HDFS 用 `distcp` 搬到 OSS，Hive 里建一个外部表指向 OSS 路径，访问透明。
- **JindoFS / Ozone 一体化**：如果是阿里云生态，JindoFS 可以把 OSS 缓存到本地加速读，相当于给对象存储加了一层"热的本地缓存"。
- **HDFS Provided Storage**（3.x 新特性）：DataNode 不存数据本体，只存元数据，真正的块在 S3/OSS。这是 HDFS 与对象存储融合的原生方案，我们还在观望稳定性。

对象存储 + EC（OSS 自身的 EC 或低冗余模式）能进一步把超冷数据的单 TB 成本压到 HDFS ARCHIVE 的 1/3 以下<!-- 校准：请按真实经历核实/替换 -->。这条曲线再往下，就是真正的"磁带级"冷存储，业务侧只需要接受"读一次要等几十秒"的延迟。

## 五、LAZY_PERSIST：把临时数据写进内存

分层存储里还有一个容易被忽略的策略：`LAZY_PERSIST`。它的设计目标是**短期、临时、可丢的数据**——比如 Spark shuffle 的中间结果、Flink 的 checkpoint、某些实时计算的临时落盘。

`LAZY_PERSIST` 的工作原理：客户端写第一个副本时优先落到 DataNode 的 RAM_DISK（一般是 tmpfs 挂载的内存盘），异步地由 DataNode 后台线程 persist 到 DISK。如果还没 persist 完机器就挂了，数据丢失——但它本来就是临时数据，可接受。

启用三步：

```xml
<!-- 1. DataNode 配置内存盘路径，并允许异步落盘 -->
<property>
  <name>dfs.datanode.data.dir</name>
  <value>[RAM_DISK]/mnt/ram_disk,[DISK]/data/dn/disk1</value>
</property>
<property>
  <name>dfs.datanode.max.locked.memory</name>
  <value>1073741824</value> <!-- 1GB，锁在内存的上限 -->
</property>

<!-- 2. 客户端写时显式声明 LAZY_PERSIST（或对目录设 policy） -->
```

```bash
# 3. 给临时目录打 policy
hdfs storagepolicies -setStoragePolicy -path /tmp/spark_shuffle -policy LAZY_PERSIST
```

实际收益：我们在一个 Flink checkpoint 重度场景下，把 checkpoint 目录切到 `LAZY_PERSIST`，checkpoint 写入耗时从 800ms 降到 120ms 左右<!-- 校准：请按真实经历核实/替换 -->，端到端延迟稳定了很多。前提是 RAM_DISK 大小要规划好，别让临时数据把内存撑爆——一般给到单节点 8–16GB 的 tmpfs 就够用<!-- 校准：请按真实经历核实/替换 -->。

## 六、监控与调度：把分层跑成一条流水线

分层存储上线后，必须有配套的监控，否则很快退化成"配了 policy 但没人看"。我们盯的核心指标：

| 监控项 | 数据源 | 告警阈值（参考） |
|---|---|---|
| 各介质利用率（DISK/SSD/ARCHIVE） | NameNode JMX `StorageReport` | 任一介质 > 85% |
| 介质利用率标准差 | 同上 | 跨节点 σ > 15%（说明不均） |
| Policy 偏差 Block 数 | Mover 日志 / 自研扫描 | 偏差块 > 总块数 1% |
| Mover 任务耗时与吞吐 | Mover 进程日志 | 单轮 > 6 小时预警 |
| 冷数据命中率（按分区） | Hive/Spark 读取埋点 | 命中率异常下跌 |
| SSD 写满回退次数 | DataNode 日志 | 任意回退即告警 |

最后两项尤其关键。**冷数据命中率**能反向验证你的分层阈值是否合理——如果 T+30 的分区命中率突然飙高，说明业务在做回溯查询，COLD 切早了；反之 T+7 的分区几乎没人读，说明 HOT 留多了，可以提前切。**SSD 写满回退**则是前文提到的隐性回退，必须从 DataNode 日志里抓出来单独告警，否则你以为数据在 SSD，实际全在 HDD。

调度层面，我们有两条定时任务：

1. **每日 Mover DAG**：凌晨 2 点触发，扫描前一天的"新切 COLD 分区"，跑 Mover 落到 ARCHIVE。
2. **每周 Policy 调整 DAG**：每周日跑一次，按分区日期批量改 policy（比如 `dt < today-30` 的全切 COLD），再触发 Mover。

这两条 DAG 把分层存储从"手动运维"变成"自动流水线"，是能长期跑稳的关键。

## 七、生产踩坑

分层存储落地这几年，踩过的坑不少，挑几个最痛的。

### 7.1 设了 policy 但 Mover 没跑

最早一批切 COLD 的分区，我光打了 policy 没触发 Mover，过了两周去看 ARCHIVE 利用率纹丝不动，数据还全在 DISK 上。原因是前面讲的——**policy 是标签，不搬家**。后来所有 policy 变更都接了一条 Mover 任务，再没出过这个问题。教训：**policy 与 Mover 必须配套调度，缺一不可**。

### 7.2 ARCHIVE 盘配 EC 时机架不够

前文提到的：两个机架的 ARCHIVE 节点配 EC 直接写入失败。EC 的条带宽度（如 RS-6-3 需要 9 个不同节点/机架）是硬约束，机架数不够就写不进去。解决办法要么加机架，要么对这些目录暂时用三副本 COLD 顶一下，等机架扩容再切 EC。教训：**ARCHIVE 机架拓扑要在采购阶段就按 EC 要求规划**。

### 7.3 SSD 配比的两难

SSD 配少了，热数据争抢 SSD 导致回退频繁，等于没分层；配多了，SSD 利用率常年 20%，成本浪费。我们的经验是 **SSD 容量占全集群总容量的 5%–10%**<!-- 校准：请按真实经历核实/替换 -->，配合 `ONE_SSD` policy（只一份副本落 SSD）用，性价比最高。纯 `ALL_SSD` 只留给极少数低延迟读敏感的场景（比如某些 OLAP 加速层）。

### 7.4 没有 Storage Type Quota，SSD 被一个业务占满

某次一个业务方图省事，把一个高频写入的临时目录直接设成 `ALL_SSD`，三天写满了全集群 SSD，连带其他业务的 SSD 副本全部回退到 DISK，等于分层被打回原形。Hadoop 2.8+ 提供了 **Storage Type Quota**，可以按目录限制某类介质的用量：

```bash
# 限制 /warehouse/biz_a 在 SSD 上最多用 500GB
hdfs dfsadmin -setSpaceQuote -storageType SSD 500g /warehouse/biz_a
```

加上这条约束后，再也没出现过单业务挤爆 SSD 的情况。**强烈建议分层存储上线的同时，对 SSD/RAM_DISK 这类稀缺介质一律配 Quota**。

## 八、分层决策：介质配比与分层周期

最后沉淀一套可落地的决策框架。

**介质配比**（按容量）：某集群目前是 **DISK : ARCHIVE : SSD ≈ 7 : 2.5 : 0.5**<!-- 校准：请按真实经历核实/替换 -->。DISK 是主力，承担温热数据；ARCHIVE 承接冷数据，随业务量增长逐年提升占比；SSD 只给极少数低延迟场景。这个比例不是定死的，核心是按"温度曲线积分"算——把每类数据的访问频率乘以数据量积分，反推介质容量需求。

**分层周期**：以"天"为基本单位，典型阈值 T+7 / T+30 / T+90 / T+180。周期的标定必须基于真实的读取埋点，不要拍脑袋。我们每年会重跑一次温度曲线统计，业务模式变化了（比如从离线为主转向更多实时），曲线拐点也会变，对应调整 policy 阈值。

**成本核算口径**：分层存储的收益最终要算成钱。我们算的口径是"每 TB 有效数据的月成本"——把介质采购成本按 36 个月折旧，加上机房电费、机柜分摊，除以有效容量（EC 后实际存储量）。分层落地一年后，这个指标下降了约 38%<!-- 校准：请按真实经历核实/替换 -->，这是给老板汇报时最有说服力的数字。

## 小结

HDFS 分层存储的本质，是用 Storage Policy 这套"温度标签"把异构介质（DISK/SSD/ARCHIVE/RAM_DISK）和数据的访问温度对齐。它的机制并不复杂——DataNode 标注介质类型、目录打 Storage Policy、Mover 按 policy 搬运数据——但真正落地稳定，要做的远不止配置：

- 用 DataNode 的 `[ssd]/[archive]` 标注让集群认识异构介质，机型分层比单机混搭更易维护。
- 熟记七种内置 policy 的介质偏好与回退行为，尤其当心 SSD 写满的隐性回退。
- **policy 是标签，Mover 才搬家**——两者必须配套调度，否则分层形同虚设。
- 冷热分层的核心是按时间分区 + 温度曲线，ARCHIVE 节点配 EC 时机架拓扑要提前规划。
- LAZY_PERSIST 给临时数据加速，对象存储 + EC 承接超冷归档，构成完整温度梯度。
- 监控冷数据命中率与介质利用率，配 Storage Type Quota 保护稀缺介质，把分层跑成自动流水线。

分层存储不是一锤子买卖，而是一条随业务演化的曲线。把温度量出来、把介质配出来、把 Mover 跑起来、把 Quota 卡住——这四件事做扎实，HDFS 的存储成本和访问性能就能在一个合理的曲线上长期共存。这是某集群从"全热存储"走向"按温度落盘"这几年，我最想沉淀下来的方法论。
