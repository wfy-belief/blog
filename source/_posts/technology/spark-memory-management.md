---
title: Spark 内存管理：从统一内存模型到 Tungsten 页表，一次线上 OOM 的复盘
abbrlink: spark-memory-management
date: 2026-07-02 16:30:00
tags:
  - Spark
  - 内存管理
categories:
  - 技术
description: 复盘一次 Executor OOM，拆解 Spark 统一内存模型、堆外页表与 Tungsten 64bit 编码，给出内存调优路径。
ai_text: "本文以一次 Shuffle 阶段 OOM 为切入点，复盘 Spark 统一内存模型：reserved/user/storage/execution 四区划分、动态借用与回收规则、TaskMemoryManager 分页机制、Tungsten 堆外内存的 64bit 地址编码，最后给出 spark.memory.fraction、storageFraction、memoryOverhead、offHeap 等参数的调优建议与 SparkUI 指标读取方法。"
---

## 引子：一次 Shuffle OOM

去年冬天，我负责的一个离线任务在凌晨扩量后炸了。Executor 日志里反复出现 `Java heap space` 和 `Unable to acquire 100 bytes of memory, got 0`。Driver 端则是一堆 `SparkException: Task failed` 把整个 Stage 拖垮。任务数据量不过涨了 30%，按理不该 OOM。排查了两个多小时，最后发现根因是 Storage 把 Execution 的内存借走没还——缓存了一批 broadcast 小表和若干 RDD 分区，Shuffle 阶段 Execution 反而借不到钱。

这次复盘让我下决心把 Spark 内存模型从头到尾啃一遍。下面把我梳理出来的东西记录下来，免得下次再踩。

## 一、统一内存模型：四区划分

Spark 1.6 起引入统一内存管理（UnifiedMemoryManager），把 Executor 堆内内存切成几块。先看一张图：

```
+----------------------------------------------------------------+
|             Executor JVM Heap (spark.executor.memory)          |
|                                                                |
|  +-------------------+----------------------------------------+ |
|  | Reserved (300MB)  |        Usable = Heap - 300MB           | |
|  +-------------------+----------------------------------------+ |
|                       |                                        |
|           +-----------+-----------+                            |
|           |  User Memory (40%)   |   Unified/Spark Memory (60%)|
|           |  用户数据结构/UDF等   |                             |
|           |                       |  +-------------+----------+ |
|           |                       |  | Storage(50%)|Execution | |
|           |                       |  |cache/broadcast|shuffle | |
|           |                       |  |             |  (50%)   | |
|           +-----------+-----------+  +-------------+----------+ |
+----------------------------------------------------------------+
```

四块的含义：

| 区域 | 大小 | 用途 |
|------|------|------|
| Reserved | 固定 300MB <!-- 校准：请按真实经历核实/替换 --> | Spark 内部对象、JVM 自身开销 |
| User Memory | (Heap - 300MB) × (1 - spark.memory.fraction) | 用户数据结构、UDF 中的对象 |
| Storage Memory | Unified × spark.memory.storageFraction | cached RDD、broadcast 变量 |
| Execution Memory | Unified × (1 - spark.memory.storageFraction) | shuffle、join、sort、aggregation |

默认 `spark.memory.fraction = 0.6`，`spark.memory.storageFraction = 0.5` <!-- 校准：请按真实经历核实/替换 -->。以 4GB 堆为例：

- Reserved = 300MB
- Usable = 4096 - 300 = 3796MB
- User = 3796 × 0.4 ≈ 1518MB
- Unified = 3796 × 0.6 ≈ 2278MB
- Storage ≈ 1139MB，Execution ≈ 1139MB <!-- 校准：请按真实经历核实/替换 -->

这些数字看起来对，但真正关键的不是静态划分，而是它们之间能不能互相借。

## 二、内存池的借用与回收

`UnifiedMemoryManager` 内部维护两个池：`ExecutionMemoryPool` 和 `StorageMemoryPool`，都继承自 `MemoryPool`，但借用规则完全不对称——这是整个模型的精髓。

**Storage 借 Execution：** `StorageMemoryPool.acquireMemory` 请求 N 字节。当前池空闲不够时，去 `ExecutionMemoryPool` 借"空闲"部分。注意，只能借 Execution 当前没人用的空闲量，不能驱逐 Execution 已在用的内存。

**Execution 借 Storage：** `ExecutionMemoryPool.acquireMemory` 请求 N 字节。空闲不够时，它反过来要求 `StorageMemoryPool` 释放，而且是强制的——可以驱逐（evict）Storage 里缓存的 block，把它们 spill 到磁盘。Execution 受保护，绝不会被 Storage 驱逐。

换句话说：

- Execution 吃紧 → 可以把 Storage 的缓存块踢出去
- Storage 吃紧 → 只能干等，不能动 Execution

这就是为什么我那次 OOM 反过来了：broadcast 小表和 cached RDD 占的是 Storage，借走了 Execution 的空闲。Shuffle 阶段 Execution 要回内存时，理论上是能驱逐 Storage 的，但多个 Task 并发抢占、驱逐本身有 IO 延迟，回收速度跟不上申请速度，最终 `acquireMemory` 返回 0，Task 直接挂。

## 三、堆内 vs 堆外

上面讲的都是堆内。Spark 还支持堆外内存（off-heap），由 `spark.memory.offHeap.enabled` 开启：

```properties
spark.executor.memory                   4g
spark.memory.offHeap.enabled            true
spark.memory.offHeap.size               1073741824
```

堆外内存不进 JVM 堆，不受 GC 管控，分配/释放走 `sun.misc.Unsafe` 直接操作堆外地址。好处是消除 GC 抖动，坏处是必须手动管理，泄漏排查更难。堆外区域同样走统一内存模型：offHeap 总量按 `spark.memory.fraction` 和 `storageFraction` 切成 Execution/Storage 两块，逻辑和堆内一致，只是没有 Reserved 和 User。

还有一个容易忽略的 `spark.executor.memoryOverhead`。这是 YARN/K8s 给 Executor 进程额外分配的堆外额度，用于 Python worker、NIO direct buffer 等。默认值是 `max(executorMemory × 0.1, 384MB)` <!-- 校准：请按真实经历核实/替换 -->：

```
spark.executor.memoryOverhead = max(executor.memory × spark.executor.memoryOverheadFactor, 384MB)
# 默认 memoryOverheadFactor = 0.1
# 4GB executor → overhead = max(409.6MB, 384MB) ≈ 410MB
```

JVM 进程总占用 = `executor.memory + memoryOverhead`。很多人把它和 `spark.memory.offHeap.size` 搞混：前者是进程级容器额度，后者是 Spark 自管的堆外内存池，两者独立。

## 四、TaskMemoryManager：页式分配

每个 Task 有一个 `TaskMemoryManager`，负责把 Task 申请到的 Execution 内存按"页"分配给各算子。算子实现 `MemoryConsumer` 接口，向 `TaskMemoryManager` 申请内存页。

```
  Task 启动
     |
     v
  SortAggregateConsumer.acquireMemory(N)
     |
     v
  TaskMemoryManager.allocatePage(N, consumer)
     |
     v
  从 ExecutionMemoryPool 申请 N 字节 ---不够?---> 触发 spill
     |                                          |
     v                                          v
  分配 MemoryBlock(long[]/off-heap ptr)   consumer.spill() 写磁盘
     |                                          |
     v                                          v
  登记到 pageTable[pageNumber]            释放后重试申请
     |
     v
  返回 64bit encoded address 给算子
```

`TaskMemoryManager` 内部维护一个 `pageTable`（`MemoryBlock[]`），每页要么是堆内的 `long[]`，要么是堆外的一块裸内存。算子拿到的不是裸引用，而是一个 64 位的"编码地址"。

## 五、Tungsten 与 64bit 地址编码

这是整个内存管理里最 hack 的部分。Tungsten 把所有数据抽象成"行在页里"，用一个 64 位 long 当指针：

**堆内模式（on-heap）：**

```
| 13 bit page number | 51 bit offset in page |
   高位                            低位
```

- 高 13 位是 pageTable 的下标（页号），最多 8192 页
- 低 51 位是页内偏移

为什么堆内要这么编码？因为堆内 `long[]` 真实地址会被 GC 移动，不能直接存裸指针。只能存"页号 + 偏移"，用时去 pageTable 查到当前 `long[]` 引用再加偏移。**堆外模式（off-heap）：** 直接存 64 bit 绝对地址。堆外内存不被 GC 移动，地址稳定，根本不需要页号翻译，这也是堆外更快、`UnsafeRow` 操作更直接的原因。

读取代码大致长这样（`Platform` 封装在 `org.apache.spark.unsafe`）：

```scala
// on-heap 解码
val pageNumber   = (address >>> 51).toInt
val offsetInPage = address & ((1L << 51) - 1)
val page = taskMemoryManager.getPage(pageNumber) // long[]
val value = Platform.getLong(page, Platform.LONG_ARRAY_OFFSET + offsetInPage)

// off-heap 直接用裸地址
val value = Platform.getLong(null, address)
```

这种设计让 Tungsten 的 sort、aggregate 都能在 byte 级别操作，绕过 JVM 对象头开销，单行能从几十字节压到十几字节。

## 六、Spill 触发

当 `ExecutionMemoryPool.acquireMemory` 拿不够时，`TaskMemoryManager` 按注册顺序调用各 `MemoryConsumer` 的 `spill`，让它们把已占用的 Execution 内存吐出来，写到本地磁盘临时文件。触发链路：

1. 申请 Execution 内存，池子不够
2. 先尝试驱逐 Storage 的可驱逐 block
3. 还不够 → 触发本 Task 内已注册 consumer 的 spill
4. spill 释放的内存归回池子，再次尝试分配

注意 spill 是 Task 内的，不跨 Task。一个 Task 把自己 spill 到死也借不到别的 Task 的内存——这也是为什么 `spark.sql.shuffle.partitions` 调大能缓解 OOM：每个 Task 分到的数据更少，单 Task 压力更小。

## 七、OOM 排查与调优

回到开头那次故障，我的排查路径：

1. SparkUI → Executors 页，确认每个 Executor 的 `On Heap Memory Used` 峰值
2. Stage → 失败 Task 日志，定位是哪个算子、哪个阶段 OOM
3. Storage 页，确认 cached/broadcast 占了多少

读取 SQL 页内存指标可以用下面这段（基于 REST API）：

```python
import requests

app_id = "application_xxx_0001"
ui = "http://driver-host:4040"

# 各 Executor 内存占用
execs = requests.get(f"{ui}/api/v1/applications/{app_id}/executors").json()
for e in execs:
    used  = e["memoryUsed"]   # 当前已用字节
    total = e["maxMemory"]    # JVM 堆总量
    print(f"{e['id']}: {used/1e6:.1f}MB / {total/1e6:.1f}MB")

# 每个 Task 在 Execution 区的峰值
stages = requests.get(f"{ui}/api/v1/applications/{app_id}/stages").json()
for s in stages:
    for t in s.get("tasks", {}).values():
        peak = t.get("taskMetrics", {}).get("peakExecutionMemory", 0)
        print(s["stageId"], peak / 1e6, "MB")
```

`peakExecutionMemory` 是这个 Task 在 Execution 区域的峰值，是判断是否要加内存的核心指标。

调优经验：

| 现象 | 调整 |
|------|------|
| Shuffle/Sort 频繁 spill | 调大 `spark.memory.fraction`（如 0.7）压 User 区，或直接加 `executor.memory` |
| Storage 缓存挤压 Execution | 调小 `spark.memory.storageFraction`（如 0.3），或对缓存表用 `persist(DISK_ONLY)` |
| GC 抖动严重 | 开 `spark.memory.offHeap.enabled`，给 `offHeap.size` 1~2GB |
| Python 侧 OOM | 调大 `spark.executor.memoryOverhead`，或 `spark.python.worker.memory` |
| 单 Task 数据倾斜 | 调大 `spark.sql.shuffle.partitions`，或单独处理倾斜 key |

那次故障最后我做的：把 `spark.memory.storageFraction` 从 0.5 降到 0.3，broadcast 那张小表改用 `persist(DISK_ONLY)`，问题解决。

## 小结

Spark 内存管理的核心是"统一模型 + 不对称借用"：Execution 能抢 Storage，反过来不行。Tungsten 用 64bit 编码地址把堆内/堆外统一成页表抽象，让算子像操作数组一样操作行数据。OOM 排查的关键是分清到底哪一区不够——是 Execution 被 Storage 占了，还是 User 区被 UDF 对象吃光，抑或堆外 overhead 不够给 Python worker。

下一篇我打算写 Spark SQL 的 Catalyst 优化器，从逻辑计划到物理计划的转换里，内存预算是怎么下推到每个算子的——那是另一个值得专门拆解的话题。

<!-- 总校准：本文默认占比 0.6/0.5、reserved 300MB、overhead 384MB、4GB 各内存区大小均按 Spark 3.x 默认值给出，请按你的真实集群版本与经历核实替换。 -->
