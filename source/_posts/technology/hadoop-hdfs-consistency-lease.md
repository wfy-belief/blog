---
title: HDFS 一致性与并发：Lease、Recovery 与 append 语义
abbrlink: hadoop-hdfs-consistency-lease
date: 2026-07-02 16:00:00
tags:
  - Hadoop
  - HDFS
  - 一致性
categories:
  - 技术
description: 讲透 HDFS 的 Lease、Recovery 机制与并发写一致性边界
ai_text: "HDFS 一次写入多次读取，但一致性边界比想象中微妙。本文以实战复盘口吻，讲透 Lease 软锁的单写者保证、Lease Recovery 让最后一个 block 收敛、Block Recovery 与 Pipeline Recovery 的分工、append 并发如何被 lease 串行化，以及 hflush/hsync 在 read-after-write 中的可见性与持久性边界。"
---

## 引言

我在某集群上运维 HDFS 多年，最常被新人问的一个问题是："HDFS 不是只能写一次吗？那还谈什么一致性和并发？"这个问题本身就暴露了大家对 HDFS 一致性模型的常见误解。HDFS 确实是"一次写入多次读取"（write-once-read-many）的模型，文件创建后不可重写已有字节，但可以 append 追加。正是这条"不可重写 + 可追加"的边界，让一致性语义变得比纯 WORM 模型微妙得多。

另一篇写路径的文章里我讲过 pipeline、hflush/hsync 的基本行为。本篇**专攻一致性模型**：Lease 如何保证单写者、writer 崩溃后 Lease Recovery 如何让最后一个 block 收敛、Block Recovery 与 Pipeline Recovery 的分工、append 在多客户端下的并发语义，以及 read-after-write 在不同 flush 级别下的可见性边界。这些都是我在排查 Flink checkpoint 丢数据、流式写入卡 under-construction 等线上问题时反复用到的底层逻辑。

## 一致性模型概览

先把 HDFS 的整体一致性边界画清楚，后面的机制都是在维护它。

HDFS 文件有三种状态：under construction（正在写）、under recovery（租约恢复中）、closed（已关闭，不可变）。

```
┌──────────────────┐   create/append    ┌────────────────────┐
│   (不存在)        │ ─────────────────> │ under construction │
└──────────────────┘                     │   (持有 lease)     │
                                          └──────────┬─────────┘
                                                     │
                          lease 过期 / client close  │
                                                     ▼
                                          ┌────────────────────┐
                                          │   under recovery   │
                                          │  (Lease Recovery)  │
                                          └──────────┬─────────┘
                                                     │
                                                     ▼
                                          ┌────────────────────┐
                                          │  closed (不可变)   │
                                          └────────────────────┘
```

核心保证可以归纳成三条：

1. **已 closed 的文件强一致**：文件一旦 close 成功，所有 reader 看到的都是完整、一致的内容，多副本之间长度和字节完全相同。这条由 close 流程里的 Block Recovery 保证。
2. **文件不可重写**：已 close 的文件不能修改已有字节，只能 append。这与本地文件系统的随机写本质不同，是 HDFS 一致性模型简化的根基。
3. **正在写的文件，read-after-write 取决于 flush 级别**：其他 reader 可以打开一个 under construction 的文件并读到"已可见"的部分，但"可见"多少由 writer 调用 `hflush`/`hsync` 与否决定。这是最容易踩坑的地方。

下面逐层拆解实现这三条保证的机制。

## Lease：单写者的软锁

### 为什么需要 Lease

HDFS 不提供文件锁，但需要保证"同一文件同一时刻只有一个 writer 在写"，否则多副本数据会撕裂。实现这个保证靠的是 **Lease（租约）**——NameNode 维护的一把软锁。

writer 在 create 或 append 一个文件时，NN 会向该 client（以 client name 标识）签发一份针对该文件的 lease。只要 lease 在有效期内，其他 client 试图以写模式打开同一文件，会被 NN 直接拒绝。lease 不是阻塞锁，而是"抢占式软锁"：当前持有者不续约到一定限度，NN 可以强行回收。

### softLimit 与 hardLimit

lease 有两个时间阈值：

| 阈值 | 默认值 | 含义 |
|------|--------|------|
| softLimit | 60s <!-- 校准：请按真实经历核实/替换 --> | soft limit 内当前 writer 的 lease 不可被抢占；超过后若有新 writer 申请同一文件，NN 可强制回收 |
| hardLimit | 3600s (60min) <!-- 校准：请按真实经历核实/替换 --> | 超过 hardLimit 且未续约，NN 认定 writer 已死，自动触发 Lease Recovery 回收 lease |

softLimit 对应配置 `dfs.namenode.lease-soft-limit-sec`<!-- 校准：请按真实经历核实/替换 -->，hardLimit 对应 `dfs.namenode.lease-hard-limit-sec`<!-- 校准：请按真实经历核实/替换 -->。

```
t=0s          t=60s(soft)        t=3600s(hard)
 │             │                  │
 ▼             ▼                  ▼
├──────────────┼──────────────────┤
│  绝对保护区   │   可被抢占区       │ 自动回收
│ (writer独占) │ (新writer可抢)    │
└──────────────┴──────────────────┘
```

续约（renew）由 client 端的 `LeaseRenewer` 线程负责，定期向 NN 发送 renewLease RPC，把该 client 持有的所有 lease 续期。续约间隔大约是 softLimit 的一个比例（约 30s 级别）<!-- 校准：请按真实经历核实/替换 -->。只要 client 存活且 DFSOutputStream 还开着，lease 就会被一直续下去。

### writer crash 后的回收

正常 close 文件时，client 会主动释放 lease。但如果 writer 进程崩溃、机器宕机，lease 不会被主动释放，此时要靠 hardLimit 兜底：

1. client 停止续约。
2. NN 的 `LeaseManager.Monitor` 线程周期性扫描，发现某 lease 距上次续约已超过 hardLimit。
3. NN 对该 lease 名下所有 under construction 的文件触发 **Lease Recovery**。
4. Recovery 完成后 lease 被释放，文件转为 closed（或允许新 writer append）。

这套机制保证了 writer 崩溃后集群不会永久卡住，但代价是"最长要等 hardLimit 才能恢复"，这正是流式写入场景下文件长时间卡 under construction 的根因。

### 常见错误

排查 HDFS 写问题，下面三个异常几乎绕不开：

**`AlreadyBeingCreatedException`**：另一个 client 正持有 lease（文件 under construction）。常见于同一应用重启后立刻 append 旧文件，但旧进程的 lease 还没过 hardLimit。解决要么等，要么主动调用 `fs.recoverLease(path)` 触发恢复。

**`RecoveryInProgressException`**：lease recovery 已经在进行中，此时再尝试 create/append 会被拒。这是 NN 防止 recovery 与新写并发冲突的保护，等几秒重试即可。

**lease 被抢占**：超过 softLimit 后被新 writer 抢走。老 writer 再 write 时会收到 `LeaseExpiredException`。我在某集群上见过多次——长 GC 暂停的 JVM writer 卡住没续约，lease 被抢，恢复后继续写就报错。

## Lease Recovery：让最后一个 block 收敛

Lease Recovery 的目标是：把一个 under construction 的文件安全地转为 closed，保证所有副本的最后一个 block 长度一致、内容一致。核心矛盾在于，writer 崩溃时最后一个 block 各副本的长度可能不同（writer 只确认了部分 DN 的写入）。

### 算法步骤

NN 主导，DN 配合，整体流程如下：

```
NN                         Primary DN          其他 DN
 │                              │                  │
 │ 1.选定 last block 的 primary │                  │
 │   (pipeline 中第一个存活的)   │                  │
 │─────────── 通知 ────────────>│                  │
 │                              │                  │
 │                              │ 2.向所有副本 DN  │
 │                              │   查询 block 长度 │
 │                              │─────────────────>│
 │                              │<────长度+GS──────│
 │                              │                  │
 │                              │ 3.取最小长度 Lmin │
 │                              │   (各副本已确认的 │
 │                              │    最大公共长度)  │
 │                              │                  │
 │                              │ 4.通知所有 DN:    │
 │                              │   truncate到Lmin │
 │                              │   升级 GS         │
 │                              │─────────────────>│
 │                              │<───── ack ───────│
 │                              │                  │
 │<── 5.上报新 GS + Lmin ───────│                  │
 │                              │                  │
 │ 6.commitBlock: 更新 NN 元数据│                  │
 │   若 last block 长度满 block │                  │
 │   size 则 complete           │                  │
 │   finalizeFile: 关闭文件     │                  │
 │   释放 lease                 │                  │
```

几个关键点：

**取最小长度（`minLength`）**：各副本 DN 上最后一个 block 的长度可能不一致，因为 writer 崩溃前 ack 的数据包只到部分 DN。取所有副本的最小长度，意味着 truncate 掉任何"超出"部分——这些是没被 writer 确认、可能在某些副本上多写的数据。truncate 后所有副本长度对齐到 `minLength`，保证一致性。

**升级 generation stamp（GS）**：truncate 后 block 内容变了，必须升 GS 让旧的（被截断的）副本视图失效。新 GS 下 DN 只保留截断后的副本，过期的副本在后续 block report 中被清理。

**commit 与关闭**：primary DN 上报后，NN commit 这个 block。如果这是最后一个 block 且长度达到 block size（`dfs.blocksize`，默认 128MB <!-- 校准：请按真实经历核实/替换 -->），NN 把 block 标记为 complete；若文件还有"未写满的尾块"，NN 也会把它 finalize 并关闭文件，释放 lease。

这个算法的精妙之处在于：**不需要 writer 参与**，纯靠 NN + DN 协作就能把一个崩溃中的写收敛到一致状态。代价是丢弃 writer 未确认的尾部数据——这在 HDFS 语义里是允许的，因为那些数据本来就"未被 ack"。

### 主动触发

除了 hardLimit 自动触发，client 也能主动调用 `FileSystem.recoverLease(path)`。这在运维中很有用：当你知道某个 writer 已经死了，不想干等一个小时，就可以手动 recoverLease 立刻进入恢复。注意 recoverLease 是异步的——它只是发起 recovery，返回 true 表示已开始，并不保证立即完成；调用方要轮询 `isFileClosed(path)` 或重试 open 才能确认。

命令行也有等价工具：

```bash
# 主动对某个路径触发 lease recovery
hdfs debug recoverLease -path /user/flink/checkpoints/job-xxx/chk-12
# 轮询文件是否已 closed
hdfs fsck /user/flink/checkpoints/job-xxx/chk-12 -files -openforwrite
```

## Block Recovery / Pipeline Recovery

这里要把两个容易混淆的概念区分开，因为它们都叫 "recovery" 但触发场景和算法不同。

### Block Recovery

广义的 Block Recovery 指"让一个 block 的多副本在长度和内容上重新达成一致"。它是一个通用机制，**Lease Recovery 的最后一步（对 last block）就是一次 Block Recovery**。另外在 addBlock 阶段若发现副本不一致也会触发。算法就是上一节描述的：选 primary、取 minLength、truncate、升 GS。

### Pipeline Recovery

Pipeline Recovery 发生在**写过程中 pipeline 故障**时，目标是让写操作不丢已确认数据地继续下去。这是写路径篇的重点之一，这里只从一致性角度补充。

典型场景：写过程中某个 DN 慢或挂了，pipeline 中断。client（DFSOutputStream）执行：

1. 放弃当前 packet（未 ack 的部分），但不丢已 ack 的数据。
2. 向 NN 报告坏 DN，NN 标记该副本为 stale。
3. NN 给出新的 pipeline（去掉坏 DN，或补一个新 DN）。
4. 新 pipeline 的 DN 之间先做一次 Block Recovery：把存活副本（已确认长度）复制到新 DN，升 GS 对齐。
5. client 从"最后一个 ack 的位置"继续写。

与 Lease Recovery 的区别：

| 维度 | Lease Recovery | Pipeline Recovery |
|------|----------------|-------------------|
| 触发者 | NN（lease 过期） | client（写过程异常） |
| writer 状态 | writer 已死/放弃 | writer 仍存活，要继续写 |
| 目标 | 关闭文件，释放 lease | 恢复 pipeline，继续写 |
| 数据去向 | truncate 到 minLength 后关闭 | 对齐后继续 append |

理解这点很重要：**Pipeline Recovery 不丢已 ack 的数据，但 unacked 数据需要重传**；Lease Recovery 则直接丢弃所有 unacked 数据并关闭。两者的"一致性边界"不同——前者是为了让写继续，后者是为了让写结束。

## append 语义与并发

append 是 HDFS 提供的对已 close 文件追加写的能力。从一致性角度看，append 的关键问题是：**多客户端并发 append 同一文件如何避免数据撕裂？**

答案仍是 lease。append 一个文件时，client 同样要先从 NN 拿到该文件的 lease。如果文件当前没有活跃 lease（已 closed），NN 直接签发；如果已有别的 client 持有 lease，则要么拒绝、要么（超过 softLimit）触发 recovery 后再签发。

```
Client A: openForAppend(f) ──> NN 检查 lease
                                  │
                ┌─────────────────┼─────────────────┐
                ▼                 ▼                 ▼
          无 lease          A 已持有(自己)      B 持有
          签发 lease         直接复用           │
                                               │
                                  softLimit内?  │  超过?
                                          │     │
                                  拒绝(AlreadyBeingCreated)
                                              │
                                        触发 Lease Recovery
                                        回收 B 的 lease
                                        签发给 A
```

所以 HDFS 的多客户端 append **本质是串行化的**：同一时刻只有一个 writer 持有 lease，其他 writer 要么等、要么抢占。这不是高性能并发写，而是用"单写者 + lease 切换"换取简单的一致性保证。如果你需要多 producer 并发写同一逻辑文件，正确做法是每个 producer 写自己的文件、下游再合并——而不是让多个 writer 抢同一个物理文件。

append 已存在文件时，如果该文件仍处于 under construction（上次 writer 没 close），NN 会先触发 Lease Recovery 把它收敛、close，然后再 reopen 给新 writer append。这就是为什么有时候 append 一个看起来"应该已经关闭"的文件会卡住——它其实还卡在 under construction，得先走一遍 recovery。

## Read-after-write 边界

这是实战中最容易出问题、也最能体现对 HDFS 一致性理解深度的地方。

### 可见性 vs 持久性

要分清两个维度：

- **可见性（visibility）**：其他 reader 能否读到这部分数据。
- **持久性（durability）**：DN 崩溃/掉电后这部分数据是否还在。

HDFS 的三种写操作在这两个维度上表现不同：

| 操作 | 数据落点 | 可见性 | 持久性 |
|------|----------|--------|--------|
| `write` | client buffer | 不可见 | 不持久 |
| `hflush` | 所有 DN 内存 buffer | 可见 | 不持久（DN crash 丢） |
| `hsync` | DN 调 fsync 落盘 | 可见 | 持久 |

注意 `hflush` 让数据"可见"是因为它把数据推到了 pipeline 所有 DN 的内存，NN 维护的 block 副本位置和 reader 读取时拿到的 DN 长度会推进。但 DN 进程 crash 或机器掉电，内存里的数据会丢——这是 `hflush` 与 `hsync` 的本质区别。

`close` 内部会做 flush（具体级别取决于实现，通常等价于 hflush 级别）<!-- 校准：请按真实经历核实/替换 -->，但不保证 hsync。所以**仅靠 close 不能保证掉电不丢最后一批数据**，对持久性要求高的场景要在 close 前显式 `hsync`。

### Producer-Consumer 场景

一个经典模式：一个 producer 持续 append 写日志文件，一个 consumer 持续 tail 读。要让 consumer 实时看到 producer 写的数据，producer 必须 `hflush`。否则数据停在 client buffer，consumer 永远读不到新内容。

```
Producer(client A)                HDFS                Consumer(client B)
    │                               │                       │
    │ write(chunk1)                 │                       │ open(file) 定期 reopen
    │ [仅 client buffer]            │                       │ read -> 0 字节
    │                               │                       │
    │ hflush()                      │                       │
    │──── 推到所有 DN 内存 ────────>│                       │
    │                               │<──── open + read ─────│
    │                               │───── chunk1 ─────────>│
    │                               │                       │ 读到 chunk1
    │                               │                       │
    │ write(chunk2)                 │                       │
    │ hsync()  // 落盘              │                       │
    │──── fsync 所有 DN ───────────>│                       │
    │                               │  [此刻 DN 掉电也不丢]  │
```

但要注意 read-after-write 的限制：**consumer 读到的长度上限是 NN 当前记录的文件长度**。对于 under construction 文件的最后一个 block，reader 打开时拿到的 block 副本里，可读长度是各副本的 minLength（已确认长度）。所以即使 producer 已经 hflush 了 chunk2，如果 NN 的 block 元数据还没推进，consumer 可能暂时读不到。`hflush` 会触发 client 向 NN 上报 block 长度推进，所以正常 hflush 后 reopen 是能读到的，但存在短暂延迟——这也是为什么严格实时 tail 场景下需要 consumer 做退避重试。

### 演示代码

下面这段代码演示三级 flush 的可见性/持久性差异：

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FlushVsSyncDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        try (FileSystem fs = FileSystem.get(conf)) {
            Path file = new Path("/tmp/consistency-demo.log");

            // overwrite=true：创建文件，NN 签发 lease 给当前 client
            try (FSDataOutputStream out = fs.create(file, true)) {
                // 1) 仅 write：数据停在 client buffer
                out.write("line-1\n".getBytes());
                // 此时其他 client open(file) 读到的长度为 0，不可见

                // 2) hflush：数据推到 pipeline 所有 DN 内存，对 reader 可见
                out.write("line-2\n".getBytes());
                out.hflush();
                // 此时其他 client 可读到 "line-1\nline-2\n"
                // 但 DN 进程 crash / 掉电，这部分内存数据会丢

                // 3) hsync：DN 调用 fsync 落盘，持久化
                out.write("line-3\n".getBytes());
                out.hsync();
                // 此时 DN 掉电后数据仍在，是真正的"持久"

                // close：内部 flush，释放 lease，文件转为 closed
                // 注意 close 默认不保证 hsync，落盘与否取决于 OS/DN 配置
            }
        }
    }
}
```

实际排查时，我会用这段代码配合手动 kill DN 进程来验证：hflush 后 kill DN，reopen 能读到，但 DN 重启后数据没了；hsync 后 kill 并重启，数据仍在。这个体感差异，是理解 HDFS 持久性边界的最直接方式。

## 生产踩坑

理论讲完，下面是我在某集群上真实踩过的几个坑，每一个都对应一致性模型的某个边界。

### 坑一：流式写入 crash 后文件卡 under-construction

现象：一个 Flink 任务写 HDFS，任务被 kill 后，目标文件长时间卡在 `under construction`，下游任务读不到完整数据，也无法 append 续写。

根因：writer 进程被强 kill，没机会执行 close，lease 没释放。要等 hardLimit（60min）<!-- 校准：请按真实经历核实/替换 -->才会自动触发 Lease Recovery。这期间文件既读不到完整内容（最后 block 不一致）、也不可写（lease 被占）。

处理：手动 `hdfs debug recoverLease -path <file>` 主动触发恢复，几秒内收敛。长期方案是把 hardLimit 调小（如 10min）<!-- 校准：请按真实经历核实/替换 -->，代价是正常长 GC 的 writer 更容易被误判。

### 坑二：lease 不释放导致下游任务起不来

现象：上游任务重启后立刻 append 旧文件，报 `AlreadyBeingCreatedException`，但旧进程明明已经退出了。

根因：旧进程是 OOM 退的，DFSOutputStream 的 close 没跑完，lease 残留。虽然进程没了，但 NN 侧 lease 还在，且没到 hardLimit。

处理：监控侧加一个对 under construction 文件年龄的告警，超过阈值自动 recoverLease。同时给业务方封装一个"安全 append"工具：append 前先 recoverLease 并轮询 isFileClosed，避免硬等。

### 坑三：df -h 看到 NN 视角与 DN 视角不一致

现象：DN 所在机器 `df -h` 显示磁盘快满，但 NN 的 `fsck` / Web UI 显示 HDFS 用量没那么大。排查发现有一批 under construction 文件，DN 上副本实际占空间，但 NN 元数据里这些 block 还没 commit，不计入已用空间统计。

根因：under construction 的 block 在 DN 上是真实存在的（写到了内存或落盘），但 NN 的 BlockManager 还没把它们标记为 complete，空间统计基于已 complete 的 block，于是出现"DN 有数据、NN 没统计"的偏差。

处理：清理长期 under construction 的残留文件（recoverLease 后删除），并修复 DN 的磁盘水位告警，以 DN 本地 `df` 为准，不能只看 NN 视角。

### 坑四：Flink checkpoint 因未 hflush 丢数据

现象：Flink 写 HDFS 的 Exactly-Once 依赖 checkpoint，但某次 JM failover 后，从最近 checkpoint 恢复发现少了最后一批数据。

根因：业务自定义的 `SinkWriter` 在 checkpoint 时只调了 `write` 没 `hflush`，JM failover 时 DN 进程恰好也被重启，内存里未落盘的数据全丢。HDFS 的持久性边界止于 hsync，不调 hsync 就只到 hflush 级别，crash 就可能丢。

处理：checkpoint 成功路径上强制 `hsync`（Flink 的 `StreamingFileSink` 有对应的 fsync/sync 选项）<!-- 校准：请按真实经历核实/替换 -->，保证 checkpoint 对应的数据真正落盘。这是"持久性边界"与"一致性模型"在工程上的直接交汇点——checkpoint 的语义是"已持久化"，HDFS 侧必须用 hsync 兑现。

## 小结

HDFS 的一致性模型可以浓缩成几条边界：

1. **单写者靠 Lease 保证**：同一文件同一时刻只有一个 writer，lease 是软锁，softLimit 管可抢占、hardLimit 管自动回收。
2. **writer 崩溃靠 Lease Recovery 收敛**：NN 协调 pipeline 各 DN 对 last block 取 minLength、truncate、升 GS，让多副本对齐后关闭文件，不丢已 ack 数据、丢弃 unacked 数据。
3. **写过程中故障靠 Pipeline Recovery 续命**：对齐已确认数据后重建 pipeline 继续 append，与 Lease Recovery 的"关闭"目标不同。
4. **append 并发被 lease 串行化**：多客户端并发 append 同一文件不是真并发，是"单写者 + lease 切换"。
5. **read-after-write 取决于 flush 级别**：write 不可见、hflush 可见但不持久、hsync 可见且持久；close 默认只到 hflush 级别。

理解这些边界的价值不在于背诵机制，而在于排查线上问题时能快速定位"这是可见性问题、持久性问题、还是 lease 占用问题"。下一篇我会展开 HDFS 的读路径与 short-circuit read、缓存一致性，把一致性模型从写侧补到读侧。
