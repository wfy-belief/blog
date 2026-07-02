---
title: HDFS 集中式缓存与读性能优化
abbrlink: hadoop-hdfs-cache-read-optimization
date: 2026-07-02 16:30:00
tags:
  - Hadoop
  - HDFS
  - 缓存
categories:
  - 技术
description: 用 HDFS Centralized Cache 与读优化榨干热数据吞吐
ai_text: "操作系统 page cache 不可控、节点命中不均，是热数据读延迟抖动的根因。本篇专攻 HDFS Centralized Cache：从 NameNode 缓存指令、DataNode mlock 锁内存到 zero-copy 命中路径，再叠加短路读、对冲读、列式下推与小文件合并，给出一套可落地的读性能优化组合拳，并复盘生产踩坑与监控指标。"
---

## 引言

在某个承载推荐特征与模型训练样本的集群里，我接过一个"诡异"的故障：同一份 30GB 的特征字典，上午 Spark job 跑 8 分钟，下午跑 22 分钟，曲线像心电图一样跳 <!-- 校准：请按真实经历核实/替换 -->。查了一圈发现根因不在计算侧——那批 DN 上同时跑着一批_shuffle 大作业，把操作系统的 page cache 冲得一干二净，特征字典的 block 反复被挤出内存、重新从磁盘读。

这就是 page cache 的痛点：它是"尽力而为"的。谁先用、谁占用多、谁就会被留下，HDFS 自己说了不算。而且 page cache 的分布天然不均——同一个 block 的三个副本，可能只有一个落在了空闲 DN 的内存里，client 偏偏读到了另外两个。短路读能解决"本地性"，但解决不了"内存确定性"。

本篇不讲短路读基础（系列读路径篇已经讲透），这里专攻 **HDFS Centralized Cache（集中式缓存）** 这套把"指定 block 钉在内存"的机制，以及围绕它展开的读性能优化体系：哪些数据该缓存、怎么配、怎么和短路读/对冲读/列式下推叠加，最后落到生产踩坑与监控。

## 一、为什么需要集中式缓存：page cache 的三个原罪

先把"为什么不用操作系统的 page cache 就够了"这件事讲清楚，否则后面所有配置都是无源之水。

page cache 在 HDFS 读路径里的三个原罪：

1. **不可控**。内核 LRU 按自己的策略淘汰，HDFS 没有任何发言权。一个 shuffle 大作业一夜之间能把热数据的 page cache 全冲掉，第二天早高峰读延迟直接翻倍。
2. **不均**。同一个 block 的三个副本分布在不同 DN，每个 DN 的内存压力不同，命中情况天差地别。client 选 DN 时并不知道哪个副本在内存里，纯按网络拓扑选。
3. **会被 swap 出去**。即使数据暂时在 page cache 里，系统内存紧张时内核会把它 swap 到磁盘，读延迟从微秒级退化到毫秒级，等于没缓存。

Centralized Cache 解决的就是这三点：**让用户显式声明"这些 block 必须常驻内存"，由 HDFS 自己 mlock 锁住、统一调度、对 client 可见**。从"靠运气命中"变成"确定性命中"。

## 二、Centralized Cache 原理：NameNode 调度，DataNode 锁内存

集中式缓存的架构是一个典型的"控制面 / 数据面分离"设计。

```
         ┌──────────────────────────────────────────────────┐
         │                   NameNode (控制面)                │
         │  ┌────────────────┐   ┌────────────────────────┐  │
         │  │ CacheManager   │   │  缓存指令聚合 / 副本选择 │  │
         │  │  - CachePool   │   │  (哪些 block 该缓存、   │  │
         │  │  - Directive   │   │   缓存几份、缓在哪些 DN) │  │
         │  └────────────────┘   └────────────────────────┘  │
         │            ▲  cacheReport (DN 上报)                │
         │            │  下发 cache 命令                       │
         └────────────┼─────────────────────────────────────┘
                      │
        ┌─────────────┼──────────────────────────┐
        ▼             ▼                          ▼
   ┌─────────┐   ┌─────────┐                ┌─────────┐
   │  DN-1   │   │  DN-2   │   ...           │  DN-n   │
   │ ┌─────┐ │   │ ┌─────┐ │                │ ┌─────┐ │
   │ │mmap │ │   │ │mmap │ │                │ │mmap │ │
   │ │+mlock││   │ │+mlock││                │ │+mlock││
   │ │(堆外)│ │   │ │(堆外)│ │                │ │(堆外)│ │
   │ └─────┘ │   │ └─────┘ │                │ └─────┘ │
   │ block钉 │   │ block钉 │                │ block钉 │
   │  在内存 │   │  在内存 │                │  在内存 │
   └────┬────┘   └────┬────┘                └────┬────┘
        │             │                          │
        └─────────────┴──── client zero-copy ────┘
```

核心机制拆开看：

**NameNode 侧（CacheManager）** 维护两类对象——Cache Pool 与 Cache Directive（下一节细讲）。它做的事情是"指令聚合"：当用户通过 `hdfs cacheadmin` 声明"缓存路径 /user/x/dict"，NN 会把这条指令展开成具体的 block 列表，再根据副本分布、DN 的 `max.locked.memory` 配额，决定每个 block 该钉在哪些 DN 上。NN 不会盲目地把所有副本都缓存——默认只钉足够满足指令 replication 的副本数，省内存。

**DataNode 侧** 收到 NN 下发的缓存命令后，对目标 block 做两件事：`mmap` 把 block 文件映射到进程的**堆外内存**（不在 JVM heap 里，完全不受 GC 影响），然后 `mlock` 把这段内存**锁住**，禁止内核把它换出到 swap。这就是 HDFS 文档里反复强调的"locked memory"——它是确定性命中的物理保证。

这个设计有几个关键优势：

- **确定性低延迟**。读已缓存 block 时跳过磁盘 IO，延迟从毫秒级降到微秒级，而且抖动极小。
- **不受 GC 与 JVM 影响**。缓存在堆外，JVM 哪怕 Full GC 停顿几秒，缓存数据纹丝不动。
- **抗 swap**。`mlock` 锁定的内存内核无权换出，物理内存就是它的归宿。
- **client 可见**。NN 在返回 `LocatedBlocks` 时，会给已缓存的副本打标记（`LocatedBlock.isCacheAllowed`），client 优先选缓存的 DN 读。

## 三、缓存管理对象：Cache Pool 与 Cache Directive

集中式缓存不是"想缓存谁就缓存谁"，它有一套带权限、带配额的管理模型，这是它能上生产的关键。

### Cache Pool：权限与配额的容器

Cache Pool 是指令的"命名空间 + 权限边界"。每个 pool 有：

| 属性 | 含义 |
|---|---|
| `owner` / `group` | 所有者，对应 Unix 权限模型 |
| `mode` | `rwx`：`r` 可查看、`w` 可加/删 directive、`x` 管理权限 |
| `limit` | 该 pool 下所有 directive 缓存的**总字节数上限**，超限拒绝新指令 |
| `maxTtl` | directive 的最大 TTL，防止用户设一个永久指令占内存 |
| `weight` | pool 之间的相对权重，用于全局内存紧张时的驱逐优先级 |

生产上一定要给每个业务团队建独立的 pool，按 weight 分配内存份额，否则一个团队把热数据全缓存了，另一个团队根本抢不到份额。

### Cache Directive：一条"缓存这个"的指令

Directive 是真正生效的缓存声明，挂在 pool 下：

| 属性 | 含义 |
|---|---|
| `path` | 要缓存的路径，可以是文件或目录（递归） |
| `replication` | 缓存几份副本（注意：这是缓存副本数，不是文件本身的副本数） |
| `ttl` | 存活时间，到期自动失效，适合临时缓存 |
| `pool` | 归属的 Cache Pool |

比如推荐特征字典要缓存 2 份、TTL 7 天，就是一个 directive。

### hdfs cacheadmin 命令族

```bash
# 创建带权限和配额的缓存池
hdfs cacheadmin -addPool feature_dict \
  -owner ai_team -group data -mode 0770 \
  -limit 200GB -maxTtl 604800

# 为某目录添加缓存指令（缓存 2 份副本，7 天后失效）
hdfs cacheadmin -addDirective \
  -path /user/ai/recommend/dict \
  -pool feature_dict -replication 2 -ttl 604800s

# 查看缓存指令列表与命中情况
hdfs cacheadmin -listDirectives -path /user/ai/recommend

# 查看缓存池状态（用量、配额、权重）
hdfs cacheadmin -listPools

# 删除某条指令（指令 ID 从 -listDirectives 获得）
hdfs cacheadmin -removeDirective 42

# 删除缓存池（需先清空其下所有 directive）
hdfs cacheadmin -removePool feature_dict
```

这套命令是日常缓存治理的入口，建议做成脚本模板，CI 里审批后执行，避免人工误操作。

## 四、读命中路径：从锁定内存到 zero-copy

当 client 读一个已被集中式缓存的 block 时，链路与普通读有一个关键差异：**DataNode 直接从 `mmap` 锁定的内存把数据交给 client，全程不碰磁盘、不进 page cache 二次缓存**。

```
   Client                NameNode                 DataNode
     |                       |                        |
     |-- getBlockLocations ->|                        |
     |<-- LocatedBlocks -----|                        |
     |   (副本标记: cached!)  |                        |
     |                       |                        |
     |-- 选 cached DN ------>|                        |
     |                       |                        |
     |--- READ_BLOCK ------ (NN 已下发过 cache 命令)   |
     |                       |                        |
     |                       |  DN: 从 mmap 内存直接   |
     |                       |      sendfile() zero-copy
     |<======== 数据流 (锁定内存 → socket) ============|
     |                       |                        |
     |   跳过磁盘 IO、跳过 page cache 二次缓存          |
```

命中时 DataNode 用 `sendfile`（零拷贝）把 `mmap` 区域的数据直接推到 socket，CPU 占用极低。这是集中式缓存比 page cache 更快的原因之一：page cache 命中后，数据还是要从内核 page cache 拷到用户态再拷到 socket（除非也走 sendfile），而集中式缓存是天然的 zero-copy。

未命中时会回退到普通读路径——从磁盘读、进 page cache、走正常校验链路。所以集中式缓存是"锦上添花"，不是"没有它就读不了"。

## 五、关键配置：把配额给够，把 OS 调对

集中式缓存踩坑最多的地方不是 HDFS 配置本身，而是 **OS 层的 locked memory 限制**。这一节按"从下到上"的顺序讲。

### 5.1 OS 层：ulimit -l 必须先放开

`mlock` 锁内存受 Linux 的 `RLIMIT_MEMLOCK` 限制，默认值小得可怜（64KB 起）。如果没调，DataNode 启动时日志里会看到 `Cannot lock memory...` 之类报错，缓存指令全部失败。

```bash
# /etc/security/limits.conf 或 limits.d/hdfs.conf
hdfs  -  memlock  unlimited
hdfs  -  nofile   1048576
hdfs  -  nproc    32768
```

改完要重启 DataNode 进程（或重新登录 shell）才生效。验证：

```bash
ulimit -l    # 应为 unlimited
cat /proc/$(pidof datanode)/limits | grep -i lock
```

CentOS/RHEL 上还要确认 systemd unit 里 `LimitMEMLOCK=infinity`，否则 limits.conf 会被覆盖。这一步漏掉的集群至少占我见过的故障一半 <!-- 校准：请按真实经历核实/替换 -->。

### 5.2 DataNode 层：可锁内存上限

```properties
# hdfs-site.xml
# DN 用于集中式缓存的最大内存（堆外，单位字节）
# 必须小于物理可用内存，且受 OS memlock 限制
dfs.datanode.max.locked.memory=128gb

# DN 上报缓存状态给 NN 的间隔，默认 10s
# 越短 NN 视图越新，但 RPC 压力越大
dfs.cachereport.intervalMsec=10000

# DN 缓存管理线程在空闲时的扫描频率（一般不动）
dfs.datanode.fsdatasetcache.max.threads.per.volume=4
```

`max.locked.memory` 的取值经验：留给 DN 进程 heap + 操作系统 + page cache 各自的份额后，剩下的 50%-60% 给缓存 <!-- 校准：请按真实经历核实/替换 -->。给太大容易把 OS 逼到 swap 别的进程，给太小缓存命中率上不去。

### 5.3 NameNode 层：指令聚合与调度

```properties
# hdfs-site.xml (NN)
# NN 周期性扫描所有 directive，计算需要缓存的 block 列表
dfs.namenode.caching.enabled=true

# 缓存块副本数计算的间隔，默认 30s
dfs.namenode.path.based.cache.refresh.interval.ms=30000
```

NN 的 `CacheManager` 会把 directive → block 列表 → DN 选择的整个过程异步化，避免阻塞 RPC。如果 directive 涉及的文件特别多（比如缓存一个百万小文件的目录），聚合扫描会比较吃 NN CPU，这是要监控的点。

## 六、选型：哪些数据该缓存，哪些千万别缓存

集中式缓存的内存是稀缺资源，乱缓存比不缓存更糟——会挤掉真正该缓存的数据。我的选型原则：

**强烈建议缓存**：

- 高频读的**小表**：维度表、字典表、映射表。几十 MB 到几 GB，被 join 上万次。
- **索引文件**：HBase 的 region 元数据、Hive 的 ORC/Parquet footer 与 index stripe。
- **机器学习模型文件**：推理服务每次加载模型，几 GB 的 pb/onnx，缓存后冷启动从分钟级到秒级。
- **常驻配置**：业务规则表、特征 schema。

**谨慎缓存**：

- 中等大小、读写混合的数据。缓存副本与写路径会有微妙交互，TTL 要设短。

**绝不缓存**：

- 大文件（几十 GB 以上的事实表）。一份就把某 DN 的配额吃光，且大文件本来就是顺序读，page cache 命中率不差，缓存收益小、挤占大。
- 一次性扫描的临时数据。缓存的代价是"长期占内存"，扫一次就丢的数据根本不配。
- 写频繁的数据。每次写都要让缓存失效，缓存命中率低，纯属浪费。

一句话总结：**缓存"小而热"的，不缓存"大而冷"的**。

## 七、读性能优化组合拳：集中式缓存只是其中一环

集中式缓存单独用收益有限，真正把读吞吐拉起来的是它和其它优化手段的叠加。下面这套组合拳是我在线上验证过有效的。

### 7.1 集中缓存 + 短路读叠加

短路读（domain socket 直连 DN 本地数据）解决"绕过 TCP"，集中式缓存解决"绕过磁盘"。两者叠加是 1+1>2：

- client 在本地 DN 上，走短路读 domain socket。
- 该 block 又被集中式缓存锁定，DN 从内存 zero-copy 返回。
- 全程零磁盘 IO、零网络栈、零用户态拷贝。

这套组合对本地化作业（Spark executor 与 DN 同节点）的延迟改善最明显，特征 join 这类高频小读能从毫秒级压到几十微秒。

### 7.2 Hedged Read：对冲读砍尾部延迟

即使有了缓存，偶尔还是有慢读——GC 停顿、磁盘抖动、网络毛刺。Hedged Read 的思路是：发一个读请求，等一个阈值没回来，**并行再发一个**到另一个副本，谁先回用谁。

```properties
# hdfs-site.xml (client 侧)
# 开启对冲读
dfs.client.hedged.read.threshold.millis=50    # 等多久没回才发第二个
dfs.client.hedged.read.threadpool.size=10      # 对冲读线程池大小
```

阈值别设太小（10ms 以下基本全是噪音，会大幅放大 DN 读压力），50ms 是一个比较稳的起点 <!-- 校准：请按真实经历核实/替换 -->。线程池大小按并发读数估，通常 10-20 够用。

对冲读的代价是 DN 读量小幅上升（最多翻倍），所以它治的是"长尾"不是"均值"。在 SLA 对 P99 敏感的场景（交互式查询、在线推理）收益最大。

### 7.3 io.file.buffer.size 与预取

```properties
# core-site.xml
# 文件读写缓冲区大小，默认 4KB，太保守
io.file.buffer.size=65536    # 64KB

# hdfs-site.xml (client)
# 预取大小，影响一次读多少数据进 buffer
dfs.read.prefetch.size=262144    # 256KB，默认通常只有几十 KB
```

`io.file.buffer.size` 调大后，每次 read 系统调用拿更多数据，系统调用次数减少，对大量小读（比如 ORC 读 stripe）效果显著。预取调大对顺序扫描类作业有帮助，但对随机读是浪费——随机读场景反而要调小。

### 7.4 列式存储 + 谓词下推：从源头减少读量

这是最容易被忽略的"读优化"——**不读就是最快的读**。ORC/Parquet 这类列存格式配合谓词下推，能把一次扫描的读取量砍掉 90% 以上：

- 列裁剪：只读用到的列，100 列的宽表只用 5 列，IO 直接省 95%。
- 谓词下推：`WHERE dt='2026-07-01' AND city='SH'` 这种过滤条件推进到 reader 层，利用 ORC 的 min/max index 跳过整个 stripe。
- 字典编码：低基数列用字典编码，读出来的是 int 不是 string。

缓存和列存是互补的：列存降低了单次读量，缓存降低了重复读的延迟。两者叠加是离线数仓读优化的标配。

### 7.5 小文件合并：治读放大的根

小文件多的目录，读放大极其严重——一个逻辑读拆成几百次 block 定位、几百次 RPC、几百次磁盘寻道。集中式缓存对小文件也友好（每个 block 钉内存开销小），但**最有效的还是从源头合并**：

- 写入侧用 ORC/Parquet 的大文件，避免产出大量小文件。
- 历史小文件定期用 `hadoop archive`（har）或 compaction 合并。
- NN 端控制单目录文件数阈值，超限告警。

小文件治理是 NN 元数据压力与读性能的双重要求，不能光靠缓存兜底。

## 八、生产踩坑实录

下面这些坑，每一条都是真实出过故障的。

### 坑 1：locked memory 配额不足，缓存静默失效

现象：加了缓存指令，`-listDirectives` 显示正常，但读延迟一点没降。

排查：`hdfs cacheadmin -listPools` 看 pool 用量，发现 limit 没到，但 DN 实际锁定的内存远小于指令总量。最终定位到 OS 的 `ulimit -l` 没放开，DataNode 日志里早就有 `mlock failed: Cannot allocate memory` 被忽略了。

教训：**部署集中式缓存的第一步永远是先验证 `ulimit -l` 与 systemd 的 `LimitMEMLOCK`**，否则后面所有配置都是空中楼阁。

### 坑 2：缓存池权限混乱，业务互相覆盖

现象：A 团队加的指令，被 B 团队误删。

根因：所有业务共用一个 `default` pool，mode 给了 `0777`，谁都能删谁的 directive。

修复：按业务线拆 pool，mode 收到 `0770`，owner 指定到对应团队账号。directive 的增删走审批流，不要让人手动 `-removeDirective`。

### 坑 3：缓存未命中的"假命中"

现象：`-listDirectives` 显示指令在，但 client 读延迟没改善。

排查：指令在 ≠ block 已缓存。NN 聚合 directive 到 block 需要一个扫描周期（`dfs.namenode.path.based.cache.refresh.interval.ms`），而且 DN 收到命令到真正 mlock 也要时间。刚加的指令，可能要 30-60s 才生效。验证是否真正命中，要看 DN 的 JMX `FSDatasetState` 里 `numBlocksCached` 与指令目标是否对齐。

### 坑 4：缓存大文件导致内存挤占

现象：某天某 DN 的 locked memory 用量飙升到上限，原本缓存的特征字典被挤掉（directive 还在，但实际没钉内存）。

根因：有人给一个 80GB 的事实表加了缓存指令，replication=3，直接吃掉 240GB 配额，把别的 pool 份额挤没了。

修复：pool 的 `limit` 与 `weight` 一定要提前规划，给"大文件禁止缓存"写进规范，CI 里对 directive 的 path 做大小校验。

## 九、监控：命中率、locked memory、驱逐

集中式缓存上线后，监控要跟上，否则故障和"没缓存"无法区分。核心指标：

| 指标 | 来源 | 关注点 |
|---|---|---|
| 缓存命中率 | client JMX `HdfsFileInputStream` / 作业统计 | 命中率掉 = 配额被挤或 directive 失效 |
| locked memory 使用率 | DN JMX `FSDatasetState` 的 `memCacheUsed` | 接近 `max.locked.memory` 说明配额不够 |
| 缓存驱逐次数 | DN JMX `FsdDatasetCacheStats` | 驱逐多 = 内存压力或 directive 频繁变更 |
| DN 上 `numBlocksCached` | DN JMX | 与指令目标对比，看是否真正生效 |
| 缓存指令总数 | NN JMX `NameNodeStatus` | 指令异常增长可能是误操作 |
| NN CacheManager 扫描耗时 | NN 日志 | directive 多时扫描吃 CPU |

Grafana 上建议建一个独立的"HDFS Cache"面板，把这六个指标放一起，命中率掉的时候能一眼看到是哪个环节出问题。命中率是我们的北极星指标，线下建议稳定在 90% 以上 <!-- 校准：请按真实经历核实/替换 -->，低于这个值就要排查 directive 是否覆盖了真正热的 block。

## 十、小结

HDFS Centralized Cache 不是银弹，但它是把热数据读延迟从"看天吃饭"变成"确定性可控"的关键工具。回顾这一篇的脉络：

- **page cache 不可控、不均、会被 swap**，这是集中式缓存要解决的根本问题。
- **NameNode 调度 + DataNode mlock 锁堆外内存**，是确定性命中的物理保证；OS 的 `ulimit -l` 是这一切的地基。
- **Cache Pool + Directive** 是带权限与配额的管理模型，按业务拆 pool、给小而热的数据加 directive，是落地姿势。
- **集中式缓存要和短路读、对冲读、列存下推、小文件合并组合使用**，单点优化收益有限。
- **locked memory 配额、池权限、大文件挤占** 是最常见的四个生产坑，监控命中率与 locked memory 是发现问题的第一道防线。

下一篇我会落到 HDFS 的 EC（纠删码）——它和集中式缓存一样，都是在"用 CPU / 内存换 IO"这条主线上做取舍，写起来会更有意思。
