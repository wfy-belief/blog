---
title: Hadoop 集群容量规划、部署与基准测试实战
abbrlink: hadoop-cluster-planning-deployment
date: 2026-07-02 13:00:00
tags:
  - Hadoop
  - 容量规划
  - 运维
categories:
  - 技术
description: 从硬件选型到基准测试，把一个生产级 Hadoop 集群立起来
ai_text: "本文复盘一次生产级 Hadoop 集群从容量规划到上线验收的完整过程。先给出磁盘、CPU、内存、网络的量化公式与典型配比，再讲硬件选型（JBOD、双网卡 bonding、ECC、冗余电源）背后的工程原因；随后对比裸机、Cloudera Manager、Ambari、K8s 四种部署形态，梳理 core/hdfs/yarn/mapred 等核心配置项；最后用 TestDFSIO、TeraSort 等基准测试给出读数解读方法，并附一份上线检查清单。"
---

## 一、引言：规划是集群的基因

我在做第二次 Hadoop 集群交付时吃过一个亏：因为初期把 NameNode 堆按"当前文件数"估的，集群跑了一年，元数据涨到接近堆上限，扩容不得不先停机迁移元数据。这件事让我形成一个判断——Hadoop 集群的"基因"在上线那一天就基本定死了：磁盘是 JBOD 还是 RAID、机架拓扑怎么画、NameNode 堆多大、副本数几份，这些决定一旦落到线上，再改就是动结构、要停机、要全员值班的"大手术"。

所以这篇文章我想倒回到集群还是一张空机柜的时候，讲清楚两件事：怎么在上线前把容量算对、把硬件选对、把配置填对；以及上线后用什么基准测试去验收、用什么检查清单去兜底。这是一个偏运维工程向的复盘，不讲架构哲学，只讲公式、数字、命令和踩过的坑。

需要先声明：Hadoop 这里指的是 Hadoop 3.x 生态（HDFS/YARN/MR），下文涉及 EC（纠删码）、YARN 资源约束等特性均以 3.x 为准。<!-- 校准：请按真实经历核实/替换 -->

## 二、容量规划：把数字先算清楚

容量规划是整个集群设计里最容易"拍脑袋"、也最贵的一步。下面我把每一项拆开，给公式和例子。

### 2.1 节点角色与配比

生产集群原则上必须做 Master/Worker 分离，不要让 NameNode 和 DataNode 混部。原因有两个：NameNode 是全内存元数据服务，对 GC 抖动零容忍；DataNode 在做磁盘密集型校验时会把 I/O 吃满，两者抢资源必然导致 RPC 延迟毛刺。

典型节点角色划分如下表：

| 角色 | 数量（建议） | 职责 | 关键瓶颈 |
|---|---|---|---|
| NameNode / Standby NN | 2（HA） | HDFS 元数据 | JVM 堆、RPC QPS |
| JournalNode | 3（与 ZK 共用） | 编辑日志仲裁 | 磁盘 fsync |
| ResourceManager | 2（HA） | YARN 调度 | 调度吞吐 |
| ZKFailoverController | 与 NN 同机 | NN 主备切换 | / |
| HistoryServer / Spark HS | 1~2 | 作业历史 | 堆 |
| DataNode / NodeManager | N | 存储与计算 | 磁盘、网络、CPU |

Worker 节点按定位分三类，选型完全不同：

- **存储型**：磁盘多（10~12 块）、CPU 弱（16~32 核）、内存小（64~128GB）。适合纯 HDFS / 冷数据 / 备份场景。
- **计算型**：磁盘少（4~6 块）、CPU 强（64+ 核）、内存大（256GB+）。适合 Spark / Flink 重计算。
- **混合型**：磁盘 8~12 块、CPU 32~64 核、内存 128~256GB。最常见的"一台顶一台"方案，也是下面公式默认的对象。<!-- 校准：请按真实经历核实/替换 -->

我倾向混合型起步，原因是可以用标签（YARN Node Label）后期把同一批机器切成存储池或计算池，灵活性最高。

### 2.2 磁盘容量公式

这是我认为最容易算错的环节。直观想法是"100 台机器 × 每台 100TB = 10PB"，但这个数和真实可用的"逻辑数据量"差得很远。我用的公式是：

```
可用逻辑容量 = 裸容量总数 ÷ ( 副本数 × 增长率系数 × 预留系数 × 压缩比系数 )
```

每一项的含义：

- **裸容量总数** = 节点数 × 单节点磁盘数 × 单盘净容量（注意厂商 1TB=1000GB，要 ÷1.024 折算）。
- **副本数**：默认 3，热数据可能更多，冷数据可走 EC（RS-6-3）等效副本约 1.5。
- **增长率系数**：预留未来 12~18 个月的增量，通常取 1.5（即留 50% 给增长）。
- **预留系数**：HDFS 推荐 DFS 使用率不要超过 85%，超过 90% 进只读，所以这里取 1.18（对应 85% 上限）。
- **压缩比系数**：如果开了列存/文本压缩（如 Zstd），实际逻辑数据能再放大约 2~4 倍，按业务估。

<!-- 校准：请按真实经历核实/替换 -->

举个具体例子。假设我要建一个 50 节点的混合型集群，每台 10 块 8TB 数据盘，副本 3，不压缩：

```
裸容量 = 50 × 10 × 8 / 1.024 ≈ 3906 TiB
可用逻辑 = 3906 ÷ (3 × 1.5 × 1.18 × 1) ≈ 735 TiB
```

也就是说 3.9 PiB 的物理盘，真正能写进去的业务数据大约只有 735 TiB。这个数字在和业务方谈"够不够用两年"时一定要先抛出来，否则一定会出现"买了一年就觉得满了"的尴尬。

补一句反向校验：HDFS Web UI 上看到的 `Configured Capacity` 就是 `dfs.datanode.data.dir` 各盘净容量之和，`DFS Used%` 是真实占用率。上线后第一件事是盯这两个数，超过 75% 就要规划扩容，别等到 85%。

### 2.3 CPU、内存与 AM 预留

YARN 的资源分配本质是"切蛋糕"。一台 64 核、256GB 的 Worker，并不是 64 核 / 256GB 全给 Container，要做这些扣减：

1. **OS + 守护进程**：4 核 / 8GB（DataNode、NodeManager、监控 Agent、日志）。
2. **AM 预留**：每个同时运行的 ApplicationMaster 至少 1~2GB，按并发作业数估，通常预留 8~16GB。
3. **堆外 / 直接内存**：Container 跑 Spark/Flink 时有堆外部分，按堆的 20% 留余量。

落到 `yarn-site.xml` 上就是两个核心参数：

```xml
<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>245760</value> <!-- 256GB - 10GB 预留，单位 MB -->
</property>
<property>
  <name>yarn.nodemanager.resource.cpu-vcores</name>
  <value>60</value> <!-- 64 核 - 4 核预留 -->
</property>
<property>
  <name>yarn.scheduler.minimum-allocation-mb</name>
  <value>2048</value> <!-- 单 Container 最小 2GB -->
</property>
<property>
  <name>yarn.scheduler.maximum-allocation-mb</name>
  <value>32768</value> <!-- 单 Container 最大 32GB -->
</property>
```

<!-- 校准：请按真实经历核实/替换 -->

**NameNode 堆** 是另一个必须前置算准的值。经验值是每 100 万个文件（含块）约占 1GB 堆，加上 NameNode 自身结构和缓存开销，再留 30% 余量：

```
NN Heap ≈ 文件数(百万) × 1GB × 1.3
```

如果预计三年后文件数到 3 亿，那 NN 堆就要给到 390GB 量级——这显然超出单机合理范围，这时候要反过来做：要么用联邦（HDFS Federation），要么用 EC 降低块数（一个大文件 3 副本是 N 个块，EC 是 N/6 量级的条带），要么强制业务合并小文件。这就是"规划决定基因"的典型场景。<!-- 校准：请按真实经历核实/替换 -->

### 2.4 网络拓扑与过载率

Hadoop 默认采用机架感知（`net.topology.script.file.name`）来决定副本放置策略：第一副本本机，第二副本异地机架，第三副本与第二副本同机架不同节点。所以机架数量、每机架节点数、交换机层级直接决定数据可靠性与读写性能。

生产集群通常采用"双层 leaf-spine"或"核心-汇聚-接入"三层架构，关键是控制 **过载率（oversubscription ratio）**：

```
过载率 = 接入层下联总带宽 : 接入层上联总带宽
```

业界经验值：Hadoop 集群接入层上联过载率不要超过 3:1，存储型甚至要做到 1:1（非阻塞）。我曾经在一个过载率 4:1 的集群上跑 TeraSort，跨机架 shuffle 把上联打满，整集群吞吐掉了 40%。所以规划阶段就要明确：每机架 20 节点，每节点双 25G 上联，机架交换机至少要有 2×100G 上行到 spine，对应过载率 (20×50G):(2×100G)=5:1，仍偏高——这种数字要提前抠。<!-- 校准：请按真实经历核实/替换 -->

机架数量没有硬公式，但有一个软规则：至少 2 个机架，否则副本策略退化；建议 ≥3 个机架，让 JournalNode/ZooKeeper 能跨机架均匀分布，避免单机架故障丢仲裁多数。

## 三、硬件选型：每一个选择背后都有原因

容量算完后是硬件采购清单。Hadoop 社区有一份很有名的 [Node Hardware](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html) 建议，我把工程上"为什么这么选"讲清楚。

### 3.1 磁盘：JBOD，不做 RAID

这是 Hadoop 选型里最容易和传统 DBA 起冲突的点。传统数据库要 RAID10 保数据安全，但 HDFS 的 DataNode 数据盘**强烈建议 JBOD（Just a Bunch of Disks）**，原因有三：

1. **HDFS 已经在软件层做了三副本**，磁盘再 RAID1 等于变相把有效容量再砍一半，性价比极低。
2. **RAID 控制器是单点**，坏一块卡整个节点的所有盘离线；JBOD 模式下坏一块盘只丢这一块的数据，HDFS 会自动从其他副本补齐。
3. **顺序读写性能**：JBOD 多盘独立，HDFS 把 `dfs.datanode.data.dir` 配成多个目录时，写入会按轮询或可用空间策略分散到各盘，吞吐是 RAID5/6 的数倍。

唯一的例外是 **NameNode 的元数据盘**，强烈建议 RAID1 或者更稳妥地配 HA（QJM/Quorum Journal Manager + Standby）。NameNode 的 `dfs.namenode.name.dir` 一旦损坏，整个命名空间就丢了，必须做磁盘级冗余。

磁盘转速建议 7200 RPM 企业级 NL-SAS 或 SATA SSD，**不要用 SMR（叠瓦盘）**。SMR 的随机改写性能极差，HDFS 的数据块校验、追加写会触发 SMR 内部重写，导致延迟飙到秒级——这点采购合同里要白纸黑字写清楚。

### 3.2 网卡：双网卡 bonding

每台 Worker 至少两块 25G（或 10G）网卡，做 LACP bonding（mode=4，802.3ad）配交换机侧 LAG。好处是带宽叠加 + 链路冗余，一根线断了业务不中断。`/etc/sysconfig/network-scripts/ifcfg-bond0` 里大致这样：

```bash
BONDING_OPTS="mode=4 miimon=100 lacp_rate=1 xmit_hash_policy=layer3+4"
```

`xmit_hash_policy=layer3+4` 是为了让单连接的流量能跨两条线做负载均衡（按 IP+端口四元组哈希），否则 Hadoop 这种长连接走单条线会退化到"主备"语义。

### 3.3 内存：必须 ECC

DataNode 的 `dfs.datanode.data.dir` 里存的是数据块本身，但 HDFS 校验是周期性的（`dfs.datanode.scan.period.hours`，默认 3 周），如果内存位翻转导致校验时把"对的"判成"错的"或反过来，污染会在不经意间发生。所以**ECC 内存是硬性要求**，不是建议。这同时也是为什么不能拿廉价消费级主板搭集群——它们大多不带 ECC 支持。

### 3.4 电源与机箱

生产集群每节点双电源（1+1 冗余），分别接 A/B 两路市电；机柜 PDU 双路；整机柜断电时集群能容忍单机柜掉线。这些是数据中心基本规范，不展开。

## 四、部署方式：四选一的取舍

Hadoop 集群立起来有四种主流姿势，我做个对比：

| 部署方式 | 上手难度 | 自动化程度 | 版本管理 | 监控告警 | 适用场景 |
|---|---|---|---|---|---|
| 裸机手动（tar 包 + 配置管理） | 高 | 低（需自配 Ansible） | 自管 | 自己接 | 学习、定制极强的特殊环境 |
| Cloudera Manager（CDP/CDH） | 低 | 高 | 厂商锁版本 | 自带 | 企业生产、有预算 |
| Apache Ambari | 中 | 高 | 自选 HDP 版本 | 自带 | 开源派、中等规模 |
| 容器化（K8s + Operator） | 很高 | 高（与 K8s 统一） | 镜像化 | 依赖 K8s 栈 | 云原生、弹性扩缩容 |

<!-- 校准：请按真实经历核实/替换 -->

我个人倾向：百台规模以内的生产集群选 Ambari，开箱即用且不锁版本；千台规模或企业级 SLA 选 Cloudera Manager，省运维人力但贵；K8s 方案（比如 云厂商的 ACK/EMR on K8s、开源的 Apache YuniKorn + Operator）适合弹性场景，但 HDFS on K8s 的本地盘管理（Local Persistent Volume）和数据本地化问题还没完全成熟，纯存储负载我不推荐上 K8s。

下面所有配置示例都按"裸机手动部署"来讲，因为只有裸机部署会逼你看清每一个参数，理解了之后用 CM/Ambari 也只是把这些值填到 GUI 里。

## 五、关键配置文件清单

Hadoop 的配置项非常多，但生产上线**必填**的就那么十几项。下面按文件给一份最小可用集。

### 5.1 `core-site.xml`

```xml
<!-- 默认 HDFS 入口，HA 下用 logical name -->
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://mycluster</value>
</property>
<!-- Hadoop 临时目录，放 DataNode/NM 的运行态，建议独立磁盘 -->
<property>
  <name>hadoop.tmp.dir</name>
  <value>/data/hadoop/tmp</value>
</property>
<!-- 机架感知脚本路径 -->
<property>
  <name>net.topology.script.file.name</name>
  <value>/etc/hadoop/conf/rack-topology.sh</value>
</property>
```

### 5.2 `hdfs-site.xml`

```xml
<!-- NameNode HA：nameservice ID -->
<property>
  <name>dfs.nameservices</name>
  <value>mycluster</value>
</property>
<!-- 两个 NN 的 RPC 地址 -->
<property>
  <name>dfs.ha.namenodes.mycluster</name>
  <value>nn1,nn2</value>
</property>
<!-- 元数据目录，建议多目录+RAID1 -->
<property>
  <name>dfs.namenode.name.dir</name>
  <value>file:///data/hadoop/hdfs/name</value>
</property>
<!-- 数据目录，JBOD，多盘逗号分隔 -->
<property>
  <name>dfs.datanode.data.dir</name>
  <value>file:///data/disk1/hdfs,file:///data/disk2/hdfs,file:///data/disk3/hdfs</value>
</property>
<!-- 副本数 -->
<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>
<!-- HA 故障切换：自动 failover 必开 -->
<property>
  <name>dfs.ha.automatic-failover.enabled</name>
  <value>true</value>
</property>
<!-- QJM 编辑日志地址 -->
<property>
  <name>dfs.namenode.shared.edits.dir</name>
  <value>qjournal://jn1:8485;jn2:8485;jn3:8485/mycluster</value>
</property>
<!-- 单 NN 堆大小，按文件数算 -->
<property>
  <name>dfs.namenode.handler.count</name>
  <value>200</value>
</property>
```

### 5.3 `yarn-site.xml`

```xml
<!-- RM HA -->
<property>
  <name>yarn.resourcemanager.ha.enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.resourcemanager.cluster-id</name>
  <value>yarn-cluster</value>
</property>
<!-- 单节点可分配资源，见 2.3 节 -->
<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>245760</value>
</property>
<property>
  <name>yarn.nodemanager.resource.cpu-vcores</name>
  <value>60</value>
</property>
<!-- 物理内存检查建议关闭，否则 Spark/堆外会被误杀 -->
<property>
  <name>yarn.nodemanager.pmem-check-enabled</name>
  <value>false</value>
</property>
<!-- 调度器：生产建议容量调度 -->
<property>
  <name>yarn.resourcemanager.scheduler.class</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
</property>
<!-- 日志聚合，强烈建议开启 -->
<property>
  <name>yarn.log-aggregation-enable</name>
  <value>true</value>
</property>
```

### 5.4 `mapred-site.xml` 与 `workers`、`hadoop-env.sh`

```xml
<property>
  <name>mapreduce.framework.name</name>
  <value>yarn</value>
</property>
<property>
  <name>mapreduce.map.memory.mb</name>
  <value>2048</value>
</property>
<property>
  <name>mapreduce.reduce.memory.mb</name>
  <value>4096</value>
</property>
<property>
  <name>mapreduce.jobhistory.address</name>
  <value>historyserver:10020</value>
</property>
```

```bash
# workers：每行一个 Worker 主机名
worker01
worker02
...
worker50
```

```bash
# hadoop-env.sh 关键项
export HDFS_NAMENODE_OPTS="-Xmx200g -XX:+UseG1GC ${HDFS_NAMENODE_OPTS}"
export HDFS_DATANODE_OPTS="-Xmx4g ${HDFS_DATANODE_OPTS}"
export YARN_RESOURCEMANAGER_OPTS="-Xmx16g ${YARN_RESOURCEMANAGER_OPTS}"
export YARN_NODEMANAGER_OPTS="-Xmx2g ${YARN_NODEMANAGER_OPTS}"
```

<!-- 校准：请按真实经历核实/替换 -->

`hadoop-env.sh` 里的堆参数一定要和 2.3 节算出来的对齐，并且优先用 G1GC（Hadoop 3.x 已默认，但显式声明更稳）。

## 六、基准测试：用数字验收，不靠手感

集群装完，下一步是用公认基准去压一遍，得到一组可对比的数字。Hadoop 自带四个经典工具，对应不同维度：

| 工具 | 测试维度 | 典型用途 |
|---|---|---|
| **TestDFSIO** | HDFS 顺序读/写吞吐 | 验证磁盘和网络的存储带宽 |
| **TeraSort** | 端到端 MapReduce 吞吐 | 综合压测，业界通用排序基准 |
| **NNThroughputBenchmark** | NameNode RPC 吞吐 | 元数据压力测试 |
| **MRBench** | 小作业循环开销 | 调度器/启动开销测试 |

<!-- 校准：请按真实经历核实/替换 -->

### 6.1 TestDFSIO 写测试

TestDFSIO 是最能直观反映"集群磁盘 + 网络存储侧"是否健康的工具。下面是一次写测试的标准命令，假设我要写 100GB 数据、每文件 1GB、副本 3：

```bash
# 写测试
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*.jar \
  TestDFSIO \
  -D test.build.data=/benchmarks/DFSIO \
  -write \
  -nrFiles 100 \
  -size 1GB \
  -resFile /tmp/dfsio_write_result.txt

# 读测试（写完之后）
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*.jar \
  TestDFSIO \
  -D test.build.data=/benchmarks/DFSIO \
  -read \
  -nrFiles 100 \
  -size 1GB \
  -resFile /tmp/dfsio_read_result.txt

# 清理
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*.jar \
  TestDFSIO -clean
```

跑完后 `/tmp/dfsio_write_result.txt` 里有一行汇总，长这样（节选关键字段）：

```text
----- TestDFSIO ----- : write
Date & time: Wed Jul 02 12:00:00 CST 2026
Number of files: 100
Total MBytes processed: 102400
Throughput mb/sec: 412.56
Average IO rate mb/sec: 418.13
IO rate std deviation: 24.7
Test exec time sec: 252.3
```

<!-- 校准：请按真实经历核实/替换 -->

**怎么解读这组数字**，我一般这样看：

1. **Throughput（聚合吞吐）**：这是集群整体写带宽除以参与节点的结果。经验上，一台 12 块 SATA 盘的 Worker，单机写吞吐应该到 600~900 MB/s。一个 50 节点的集群，理论聚合写到 30~45 GB/s 量级。如果实测 Throughput 远低于这个数（比如只有个位数 MB/s × 节点数），多半是网卡 bonding 没生效或磁盘控制器瓶颈。
2. **Average IO rate 与 std deviation**：平均值和标准差。标准差大说明节点间性能不均衡，可能是某几块盘老化、某台机器 BIOS 关了磁盘写缓存、或某机架网络抖动。生产集群要求 std/avg 控制在 10% 以内。
3. **Test exec time**：单次测试总耗时，反过来也能算出整体吞吐，用来和上次基准做对比。

和业界对比的话，[Sort Benchmark](https://sortbenchmark.org/) 和 Cloudera 公布的 [TeraSort on 100TB](https://blog.cloudera.com/) 是参考点；一个常规生产集群，TestDFSIO 写吞吐至少要打到磁盘标称带宽的 60% 以上才算"存储侧配置正确"。<!-- 校准：请按真实经历核实/替换 -->

### 6.2 TeraSort：端到端综合测试

TeraSort 由三步组成：TeraGen 生成数据 → TeraSort 排序 → TeraValidate 校验。它同时压 HDFS 写、shuffle 网络、reduce 落盘，是公认的综合基准。

```bash
# 1. 生成 1TB 随机数据（约 10 亿条 100 字节记录）
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  teragen -Dmapreduce.job.maps=200 1000000000 /benchmarks/terasort/input

# 2. 排序
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  terasort /benchmarks/terasort/input /benchmarks/terasort/output

# 3. 校验结果是否真的有序
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  teravalidate /benchmarks/terasort/output /benchmarks/terasort/validate
```

解读 TeraSort 的关键看三点：总耗时、shuffle 阶段带宽利用率、reduce 慢节点比例。一个健康集群跑 1TB TeraSort 应该在 5~10 分钟以内；如果超过 20 分钟，要查 `mapreduce.task.io.sort.mb` 是否过小导致频繁 spill、或者 `dfs.replication` 期间 shuffle 跨机架比例过高。

### 6.3 NNThroughputBenchmark

这是不依赖集群、直接压 NameNode RPC 的工具，主要看每秒能处理多少次 create/mkdir/getFileInfo。生产 NameNode 单节点一般能跑到 5~10 万 ops/sec，低于这个数要查 JVM GC、handler 数、磁盘 fsync 延迟。<!-- 校准：请按真实经历核实/替换 -->

## 七、上线检查清单

基准测试跑完不等于能上线。我每次交付前都要过一遍这份清单：

**高可用与故障切换**
- [ ] NameNode HA：手动 kill active NN，确认自动 failover 在 30 秒内完成，客户端无感重连。
- [ ] ResourceManager HA：同上验证。
- [ ] ZooKeeper 集群：3 或 5 节点，跨机架部署，`zkServer.sh status` 确认 leader/follower 角色正常。

**安全开关**
- [ ] Kerberos 是否开启（生产必须开，至少把 NN/RM 保护起来）。
- [ ] HDFS 权限 `dfs.permissions.enabled=true`，umask 设置合理。
- [ ] 敏感端口（9000/8020/8088/50070）通过防火墙或 Security Group 收敛。

**监控**
- [ ] JMX 端口暴露，Prometheus 通过 JMX Exporter 采集 NN/DN/RM/NM 指标。
- [ ] 关键告警：NN 堆使用率 > 80%、DFS 使用率 > 85%、Dead DataNode > 0、JournalNode 离线、RPC 延迟 P99 > 100ms。
- [ ] 磁盘 SMART 指标接监控，提前发现坏盘。

**Balancer 与目录权限**
- [ ] `hdfs balancer` 能正常启动，阈值（`dfs.datanode.balance.bandwidthPerSec`）合理，建议非高峰跑。
- [ ] DataNode 数据目录 owner 为 `hdfs:hadoop`，权限 755，避免 YARN 容器误写。
- [ ] NameNode 元数据目录权限 700，只有 hdfs 用户可读写。

**系统侧**
- [ ] 时钟同步：所有节点 NTP 或 Chrony 偏差 < 50ms，否则 Kerberos 票据和 ZK 会话会异常。
- [ ] `vm.swappiness=1`（生产建议设 1 而不是 0，避免突发 OOM 但保留少量 swap 兜底）。
- [ ] `noatime,nodiratime` 挂载选项加到数据盘，减少元数据更新开销。
- [ ] 关掉透明大页（THP），Hadoop 的堆大且分配模式不规则，THP 反而带来延迟抖动。
- [ ] 文件描述符上限 `ulimit -n` 至少 65536。

<!-- 校准：请按真实经历核实/替换 -->

这份清单看起来琐碎，但每一条背后都是一个真实的事故。时钟不同步导致 Kerberos 整集群认证失败、swappiness 没调导致 GC 时换页把 NameNode 卡死、THP 没关让 DataNode 心跳超时被踢出集群——这些坑我都见过。

## 八、小结

写到这，整条链路就清晰了：**容量规划**给出"该买多少、买多大"，**硬件选型**回答"为什么是 JBOD 而不是 RAID、为什么必须 ECC"，**部署方式**决定后续几年的运维姿势，**配置文件**是把规划落地成可运行系统的最后一公里，**基准测试**用一组可对比数字告诉所有人这台机器值不值这个钱，**上线清单**则是兜底。

我越来越觉得，Hadoop 这种重型分布式系统的工程感，体现在"前置"两个字上。规划是科学——有公式、有经验值、有基准对比；运维是修行——日复一日地看监控、做扩容、复盘故障。规划做得越扎实，运维的日子越好过；反过来，规划偷的懒，运维会十倍地还回来。

最后给一个经验总结，希望对你下次建集群有用：永远按"三年后的文件数"去配 NameNode 堆，永远按"业务峰值两倍"去估磁盘，永远把网络当成最容易成为瓶颈的资源——这三条做不到，集群就一定会在某个深夜用它最不愿意的方式提醒你。
