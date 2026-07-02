---
title: MapReduce 原理与 Shuffle 深度优化
abbrlink: hadoop-mapreduce-shuffle
date: 2026-07-02 10:30:00
tags:
  - Hadoop
  - MapReduce
  - Shuffle
categories:
  - 技术
description: 逐阶段拆解 MapReduce 执行模型，深挖 Shuffle 与数据倾斜治理
ai_text: "本文以第一人称复盘口吻，逐阶段拆解 MapReduce 的执行模型：从 split 决定并行度，到 Map 端环形缓冲区 spill、merge，再到 Shuffle 拉取与 Reduce 端聚合。重点讲解 Combiner、压缩、环形缓冲调参、Reduce 慢启动等优化取舍，并专题剖析数据倾斜的定位与两阶段聚合、加随机盐、自定义 Partitioner、MapJoin 等治理手段，最后与 Spark Shuffle 做对比，帮助读者建立对所有大数据计算引擎的原理级理解。"
---

# MapReduce 原理与 Shuffle 深度优化

## 一、引言：为什么 2026 年我还要把 MapReduce 翻出来讲

我承认，现在没人会在生产环境新起一个 MR 任务去做流式 ETL 了。过去几年我做的新计算链路几乎都跑在 Spark 和 Flink 上，MR 看起来像上个时代的遗物。但我越来越觉得，**真正理解大数据计算引擎的捷径，反而是先把 MR 的执行模型啃透**。

原因很简单：Spark、Flink 这些引擎的执行抽象（stage 划分、shuffle、task 调度、推测执行）几乎都是从 MR 一脉相承下来的，只是换了实现策略。MR 的模型最直白、最不藏魔法，每一个阶段的成本（磁盘 IO、网络、序列化）都摆在台面上。一旦你真正搞懂了 MR 的 Shuffle——数据怎么从 map 端写到磁盘、怎么通过 HTTP 被拉到 reduce 端、怎么 merge——再去看 Spark 的 sort-based shuffle 或者 Tungsten 的 off-heap 排序，会非常顺。

这篇文章是我过去踩过坑之后的复盘，重点放在执行阶段全流程、Shuffle 优化参数取舍，以及最让人头疼的数据倾斜治理。所有参数我会给出选型建议，但涉及具体数值的地方，请按你自己的集群规模和数据分布去校准——我会在每个具体数字后面标注，提醒你别照搬。

## 二、执行阶段全流程：把每一步掰开看

我把 MR 的执行流程拆成六个阶段。先上一张我用了很多年的脑内模型：

| 阶段 | 发生位置 | 核心动作 | 关键开销 |
|------|---------|---------|---------|
| InputFormat / split | Client + AM | 切分输入，决定 map 并行度 | 主要是元信息计算 |
| Map | Map 容器 | 调用 mapper，写环形缓冲区 | CPU + 内存 |
| Spill & Merge | Map 容器 | 缓冲区写满后排序、落盘、合并 | 磁盘 IO |
| Shuffle（Copy） | Reduce 容器 | HTTP 拉取属于自己的 partition | 网络 + 磁盘 |
| Sort / Reduce | Reduce 容器 | 归并排序、分组、调用 reducer | CPU + 内存 |
| OutputFormat | Reduce 容器 | 写 HDFS | 网络 + 副本 IO |

下面逐阶段展开。

### 2.1 InputFormat 与 split：并行度的源头

`InputFormat` 干两件事：校验输入合法性、把输入切成 `InputSplit`。对最常见的 `FileInputFormat`，一个 split 默认对应一个 HDFS 块（默认 128MB <!-- 校准：请按真实经历核实/替换 -->），map task 的数量就约等于 split 数。

这里有个很多人忽略的点：**split 大小并不强等于 block 大小**。它由三个参数共同决定：

```properties
# split 最小值（按字节）
mapreduce.input.fileinputformat.split.minsize=0
# split 最大值
mapreduce.input.fileinputformat.split.maxsize=256000000
# 一个文件作为一个 split 的阈值
mapreduce.input.fileinputformat.split.minsize.per.node=0
```

实际 split 大小 = `max(minSize, min(maxSize, blockSize))`。所以想让小文件合并成更大的 split，调大 `minsize`；想让单个 split 更小、提高 map 并行度，调小 `maxsize`。

一个真实的坑：业务方每天往 HDFS 扔几千个几 MB 的小文件，结果 map task 数量是数据量的几十倍，光 JVM 启动就吃掉一半任务时长。这种情况下我会先用 `CombineFileInputFormat` 把小文件打包，把 map 数压下来。

### 2.2 Map 阶段：环形缓冲区是灵魂

这是整个 MR 我觉得最值得讲清楚的部分。mapper 的 `map()` 每输出一条 `(key, value)`，并不是直接写磁盘，而是写进一个**环形缓冲区**（circular buffer / kvbuffer）。

环形缓冲区的容量由 `mapreduce.task.io.sort.mb` 控制，默认 100MB <!-- 校准：请按真实经历核实/替换 -->。这个缓冲区同时承担两件事：

1. 写 **数据区**：序列化后的 key/value，以及每条记录的元信息（partition、key 起始偏移、value 起始偏移）。
2. 写 **索引区**（index）：每条记录一段定长的元信息，从缓冲区的另一头往反方向生长。

两个指针相向生长，碰头即满。当数据区使用率达到 `mapreduce.map.sort.spill.percent`（默认 0.80 <!-- 校准：请按真实经历核实/替换 -->）时，触发一次 **spill（溢写）**：后台线程把缓冲区里这部分数据按 `partition → key` 排序后写到 map 任务的本地磁盘（不是 HDFS），生成一个 spill 文件，同时生成一个 spill index 文件。

注意几个细节：

- **排序在 spill 之前**。也就是说，每个 spill 文件内部已经按 `(partition, key)` 有序。这是后续 merge 能用归并而不是重新排序的前提。
- **partition 在这里就分好了**。每条记录在写进缓冲区前，已经被 `Partitioner` 算出它属于第几个 reduce task，写进了元信息里。reduce 端拉数据时就是按 partition 号去对应 map 端文件里取。
- **Combiner（如果配了）在这里第一次有机会跑**。spill 之前如果配了 combiner 且 sorter 是排序类，框架会对排好序的数据跑一次 combiner，做本地预聚合，减小 spill 文件体积。这是 combiner 的第一个调用点。

如果一次 map 调用输出特别多，spill 会发生很多次，磁盘上会留下 N 个 spill 文件。这对 IO 是非常重的负担。我后面会讲怎么调参减少 spill。

### 2.3 Spill 合并：map 端的最后一步

mapper 跑完之后，框架会把所有 spill 文件 merge 成一个最终的 map 输出文件（`file.out` + `file.out.index`）。这次 merge 是**多路归并排序**，路数由 `mapreduce.task.io.sort.factor`（默认 10 <!-- 校准：请按真实经历核实/替换 -->）控制。

如果 spill 文件数超过 `min.num.spills.for.combine`（默认 3 <!-- 校准：请按真实经历核实/替换 -->）且配了 combiner，merge 时 combiner 会再跑一次——这是 combiner 的第二个调用点。注意 combiner 不保证只跑一次，所以它**必须是幂等/可重复聚合的**（sum、max/min 可以；mean、中位数不行）。

merge 完之后，map 端的工作就结束了。最终的输出文件按 partition 顺序排布，每个 partition 内部按 key 有序，等 reduce 来拉。

### 2.4 Shuffle：真正烧网络的一段

"Shuffle" 这个词在 MR 里其实是模糊的。广义的 Shuffle 指 map 输出到 reduce 输入之间的全部数据搬运；狭义地说，map 端的 spill+merge 叫 map-side shuffle，reduce 端的 copy+merge 才是 reduce-side shuffle。这里讲 reduce 端。

reduce task 启动后，它的 copy 阶段会通过 HTTP 向所有已完成的 map task 所在的 NodeManager 拉取"属于自己那个 partition"的数据。具体来说：

1. reduce task 的 `EventFetcher` 向 ApplicationMaster 询问哪些 map task 已完成、它们在哪些 host 上、对应 partition 的 offset 和长度。
2. 多个 `Fetcher` 线程并行地从各个 map host 拉数据。并发数由 `mapreduce.reduce.shuffle.parallelcopies`（默认 5 <!-- 校准：请按真实经历核实/替换 -->）控制。
3. 拉回来的数据先放内存缓冲区，放不下就落盘，最终所有来自不同 map 的数据要被 merge 成按 key 全局有序的流，喂给 reducer。

这一段是整个 MR 最吃网络和磁盘的环节，也是性能问题的重灾区。一个 reduce task 要和几百上千个 map host 通信，HTTP 连接数、磁盘随机读、跨机架带宽都会成为瓶颈。理解这一点，你才能理解为什么后面那些优化（压缩 map 输出、调并行拷贝数、慢启动）都集中在这里。

copy 阶段的失败重试由 `mapreduce.reduce.shuffle.maxfetchfailures` 控制；如果某个 map host 因为负载高一直拉不动，reduce 会卡在这里——这也是推测执行和慢启动需要权衡的地方。

### 2.5 Sort 与 GroupingComparator：reduce 端的归并

copy 拿回来的数据来自多个 map，每个 map 的输出在自己内部是有序的，所以这里做的是**多路归并**，路数同样是 `io.sort.factor`。归并完成后，reduce 端得到一个**全局按 key 有序**的数据流。

接下来是 grouping。reducer 收到的 key 是排序后的，但默认同一个 key 的多条 value 才会被分成一组调一次 `reduce()`。这个"判定两个 key 是否属于同一组"的逻辑由 `RawComparator` 控制，默认是 key 自己的 comparator。

这里有个高级技巧：**GroupingComparator**。当你的 key 是复合对象（比如 `(userId, timestamp)`），你希望按 `userId` 分组，但按 `(userId, timestamp)` 排序时，就需要自定义一个 GroupingComparator，只比较 key 的 userId 部分。这是 MR 里实现"二次排序"的标准手法，我后面在数据倾斜治理里也会用到类似思路。

### 2.6 OutputFormat：写出结果

reducer 输出最终通过 `OutputFormat` 写到 HDFS（通常是 `TextOutputFormat` 或 `SequenceFileOutputFormat`）。`RecordWriter` 负责 `(k,v) → byte[]` 的序列化和写文件。`FileOutputFormat` 的 `getRecordWriter` 会向 NameNode 申请 block 副本，每次写都要走三副本的网络流水线，所以如果 reduce 输出量很大，这里也是瓶颈。

至此一个 MR job 的完整生命周期走完。下面进入优化。

## 三、关键优化点：每个参数背后都是取舍

我把实战中真正有效的几个优化列出来，每个都给参数和为什么。

### 3.1 Combiner：本地预聚合，砍 shuffle 量

Combiner 的本质是在 map 端先做一次局部 reduce。对 WordCount、PV 统计这类场景，map 输出里同一个 key 可能出现成千上万次，如果在 map 端先 sum 一下，shuffle 的数据量能降一个数量级。

```java
job.setCombinerClass(IntSumReducer.class);
```

取舍：

- **必须是幂等聚合**：sum、min、max、count 可以；avg、median 不行（avg 会算错，因为两次 combine 的权重不同）。要做 avg，应该 combiner 输出 `(sum, count)`，reduce 端再除。
- **不保证一定执行**：spill 文件少于阈值时不跑 combiner，所以业务逻辑不能依赖 combiner。
- **适用的 key 分布**：key 越集中（同一个 key 在一个 map 内出现次数多），combiner 收益越大。如果 key 本身就高度离散，combiner 几乎没效果。

### 3.2 压缩 map 输出：用 CPU 换网络和磁盘

这是性价比最高的优化之一。map 输出本来就要走磁盘 + 网络，压缩能直接砍掉一半以上的字节数。

```properties
mapreduce.map.output.compress=true
mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
```

codec 的选择是个真问题：

| Codec | 压缩比 | 压缩速度 | 可分裂 | 适用场景 |
|-------|-------|---------|--------|---------|
| Snappy | 中 | 极快 | 否 | map 输出、shuffle 中间结果 |
| LZO | 中 | 快 | 是（需建索引） | 大文件存储 + 想并行读 |
| Gzip | 高 | 慢 | 否 | 归档冷数据 |
| Zstd | 高 | 快 | 否 | 较新集群，可替代 Snappy |

注意"可分裂性"：能不能让一个压缩文件被切成多个 split 并行读，取决于这种压缩格式是否支持随机定位到 block 边界。Snappy/Gzip 不行，整个文件只能被一个 map 读；LZO 加了索引后可以。map 输出压缩不涉及分裂问题（每个 map 的输出本来就是一个文件），所以 Snappy 完全合适。

reduce 输出也建议压缩（`mapreduce.output.fileoutputformat.compress=true`），codec 看下游消费方——如果下游还要再读，SequenceFile + Snappy 是稳妥选择。

### 3.3 调环形缓冲区：减少 spill 次数

回到 2.2。spill 次数多 = 磁盘写多 = 后续 merge 多。两个杠杆：

```properties
# 增大缓冲区，单次容纳更多数据。前提是容器内存够。
mapreduce.task.io.sort.mb=256
# 提高触发阈值，让缓冲区用到更满才 spill，单次 spill 数据更多、总 spill 次数更少。
mapreduce.map.sort.spill.percent=0.90
```

经验上，`sort.mb` 给到 map 容器内存的 40%~70% 比较合理（容器内存由 `mapreduce.map.memory.mb` 控制，比如 2GB <!-- 校准：请按真实经历核实/替换 -->）。给太大可能挤压 JVM heap 导致 OOM；给太小 spill 频繁。

`spill.percent` 不要拉到 0.95 以上——spill 是后台线程做的，如果 mapper 写得太快，缓冲区在 spill 完成前就被写满，会阻塞 map 调用，反而更慢。0.85~0.90 是我的舒适区。

### 3.4 Reduce 慢启动：别让 reduce 太早起

reduce task 默认在 map 完成 5% 时就开始启动、去 copy。问题是早期 copy 啥也拉不到，reduce 容器白白占着资源。

```properties
# 让大部分 map 完成后再启动 reduce
mapreduce.job.reduce.slowstart.completedmaps=0.8
```

调到 0.8 左右 <!-- 校准：请按真实经历核实/替换 -->，能显著降低集群同时运行的容器数，对资源紧张的生产集群尤其有用。但也不能调太接近 1，否则 map 全完成后 reduce 才启动，整体延迟反而增加。

### 3.5 推测执行与 JVM 重用

**推测执行（speculative execution）**：当一个 task 明显比同类 task 慢（比如慢于中位数的某个倍数），框架会起一个副本任务，谁先完成用谁的结果，另一个被 kill。对慢节点（straggler）很有效。

```properties
mapreduce.map.speculative=true
mapreduce.reduce.speculative=true
```

但推测执行有代价：双倍资源消耗。对 reduce 特别危险——如果 reduce 在跑重逻辑，推测副本会再吃一份内存和 CPU。所以**集群负载高时我会关掉 reduce 推测**，负载低时才开。

**JVM 重用**（仅 Hadoop 1 时代的 `mapred.job.reuse.jvm.num.tasks`，Hadoop 2 之后用 `mapreduce.job.jvm.numtasks`，默认 1 <!-- 校准：请按真实经历核实/替换 -->）：让一个 JVM 容器连续跑多个 task，省掉 JVM 启动开销。对大量小 task 的 job 效果明显，但要注意内存泄漏——一个 task 的 OOM 会拖累后续复用的 task。这个参数在新版本里已经被容器化逐渐取代，但面试和排查老集群还是会问到。

### 3.6 merge factor 与 io.sort.factor

`mapreduce.task.io.sort.factor` 同时影响 map 端 spill merge 和 reduce 端 copy merge 的路数。默认 10，调到 32~64 <!-- 校准：请按真实经历核实/替换 --> 通常能减少 merge 轮数。但每一路要占内存和文件句柄，太大反而压垮 NM。

## 四、数据倾斜专题：MR 里最难缠的问题

写到这里，我要重点讲数据倾斜。这是我在生产环境被坑过最多次的问题，也是面试时区分"会用 MR"和"懂 MR"的分水岭。

### 4.1 现象：长尾

数据倾斜的本质是**key 分布不均**，导致某个 partition 的数据量远超其他 partition，对应的 reduce task 耗时被拉成离谱的长尾。典型表现：

- job 整体进度卡在 99%，剩最后一个 reduce task 死活跑不完。
- 查 counter，某个 partition 的 `Reduce input records` 是其他 partition 的几十甚至几百倍。
- task 日志里看到某个 reduce 的 GC 频繁、copy 阶段拉到的数据量巨大。

我在一次用户行为分析里遇到过：某个大 V 用户的访问记录占了全量日志的 15%，按 `userId` 分组时这个 key 直接把一个 reduce 撑爆，任务跑了两小时，最后那个 reduce 占了一小时五十分钟。

### 4.2 定位手段

定位倾斜有三个抓手：

1. **Counter**：MR 自带一堆 counter。看 `MAP_OUTPUT_RECORDS` / `REDUCE_INPUT_RECORDS` 按 partition 的分布，哪个 partition 畸高一目了然。也可以在 reducer 里自定义 counter。
2. **采样**：对输入采样，按目标 key 做 `groupBy().count()`，观察 count 分布。可以用 Hive 的 `TABLESAMPLE` 或 Spark 的 `sample`。
3. **Task 日志**：去 ResourceManager / History Server 看每个 task 的耗时、shuffle bytes、GC 时间。耗时呈明显长尾基本就是倾斜。

### 4.3 治理手段

下面四种手段按场景选用，必要时组合。

**手段一：两阶段聚合（局部 + 全局）**

最经典。原始倾斜 key 直接进 reduce 会被打散不了，思路是先用一次 map-reduce 做局部聚合（用随机前缀打散），再做第二次全局聚合把随机前缀去掉。

伪代码（Java MR 风格）：

```java
// 第一阶段：给 key 加随机前缀，打散到多个 partition
// mapper1: (原始key, 1) -> ("prefix_" + random(N) + "_" + key, 1)
public static class SaltMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private final Random rnd = new Random();
    private int saltRange = 100; // 打散倍数 <!-- 校准：请按真实经历核实/替换 -->

    @Override
    protected void map(LongWritable offset, Text value, Context ctx)
            throws IOException, InterruptedException {
        // value 形如 "userId\t..."，按业务取 key
        String key = value.toString().split("\t")[0];
        int salt = rnd.nextInt(saltRange);
        ctx.write(new Text(salt + "_" + key), ONE);
    }
}

// reducer1: 对带盐的 key 做局部 sum
public static class LocalSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable out = new IntWritable();
    @Override
    protected void reduce(Text saltedKey, Iterable<IntWritable> vals, Context ctx)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : vals) sum += v.get();
        out.set(sum);
        ctx.write(saltedKey, out); // 输出 (salt_key, localCount)
    }
}

// 第二阶段：去掉盐，再按原始 key 做全局 sum
public static class StripSaltMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable offset, Text value, Context ctx)
            throws IOException, InterruptedException {
        // value 形如 "salt_key\tcount"
        String[] parts = value.toString().split("\t");
        String rawKey = parts[0].substring(parts[0].indexOf('_') + 1);
        ctx.write(new Text(rawKey), new IntWritable(Integer.parseInt(parts[1])));
    }
}
// reducer2: 标准 sum，此时每个原始 key 的数据已被平均分成 saltRange 份预聚合过，
// 数据量降一个数量级，倾斜被稀释。
```

核心思想：**用一次额外的 MR 把倾斜 key 的工作量摊薄到 N 个 reduce task，再用第二次 MR 合并**。代价是多一轮 MR，但相比卡死在长尾，通常划算。

**手段二：自定义 Partitioner 均衡**

如果倾斜是因为默认 `HashPartitioner` 把热点 key 都打到同一个 reduce，可以自定义 Partitioner，对热点 key 做特殊路由（比如人为分到多个 partition，reduce 端再合并）。这个手段常和两阶段聚合配合。

**手段三：MapJoin（小表广播）**

如果倾斜是因为一张大表 join 一张小表（维度表），完全可以让小表先读进每个 map 容器的内存，在 map 阶段直接完成 join，**根本不进 shuffle**。Hive 里有 `/*+ MAPJOIN(dim_table) */` hint，MR 层面就是 DistributedCache + map 端 lookup。这是治 join 倾斜最干净的办法，前提是小表能装进内存（经验值 < 1GB <!-- 校准：请按真实经历核实/替换 -->）。

**手段四：倾斜 key 单独处理**

把倾斜的 key（比如那几个大 V userId）单独过滤出来，单独走一条 MR 链路，不参与正常 shuffle；其余 key 走正常逻辑。代码略繁琐但非常稳，适合热点 key 数量少且明确的场景。

## 五、与 Spark Shuffle 的对比：MR 的模型为什么仍然值得懂

最后聊聊和 Spark 的对比，这是我理解 Spark 内部机制时最大的拐杖。

Spark 的 shuffle 主要有两代实现：

- **Hash shuffle（早期）**：每个 map task 为每个 reduce task 写一个单独的文件。文件数 = `map数 × reduce数`，规模一上去文件数爆炸。优点是 reduce 端不用 merge 排序（每个文件本身就是一个 reduce 的全部数据），缺点是文件数失控。
- **Sort shuffle（Spark 1.2+，现为默认）**：每个 map task 只写一个数据文件 + 一个索引文件，数据按 partition 排序后写出，索引记录每个 partition 的 offset。reduce 端按 partition 去 map 端的索引里取自己那段。**这个模型几乎和 MR 的 map 端输出一模一样**。

看出来了吗？Spark 的 sort shuffle 本质就是把 MR 的 map 端 shuffle 模型搬过来，只不过 MR 把"每个 map 输出一个多 partition 文件"这个设计一直贯彻，而 Spark 是从 hash 演化到 sort 的。

理解了 MR 这一套（环形缓冲、spill、partition+sort、index、copy、merge），再去看 Spark 的：

- `spark.sql.shuffle.partitions`（reduce 并行度）↔ MR 的 `reduce.tasks`
- `spark.shuffle.file.buffer`（map 端缓冲）↔ `mapreduce.task.io.sort.mb`
- `spark.shuffle.io.numConnectionsPerPeer` / `retryWait` ↔ reduce copy 参数
- Spark 的 `UnsafeShuffleWriter` 用 off-heap 序列化缓冲 ↔ MR 环形缓冲区的现代变体

几乎都是一一对应。这也是为什么我说，**搞懂 MR 的 Shuffle，等于搞懂了所有大数据计算引擎 Shuffle 的一半**。另一半是 Flink 的网络栈和基于 pipeline 的 streaming shuffle，但那是另一个话题。

更进一步，理解了 MR 为什么慢（每一步都落盘、每个 stage 之间都走完整 shuffle），你才能理解 Spark 为什么在某些场景快——因为 Spark 把多个 map 操作 chain 在内存里、把宽依赖之间的窄依赖做了 pipeline，但宽依赖（shuffle boundary）两侧依然要落盘或至少 materialize，这一段的开销模型跟 MR 是同构的。

## 六、小结

把这篇文章收个口：

1. MR 的执行模型虽然老，但它是所有大数据计算引擎执行抽象的"原始标本"。split 决定并行度、环形缓冲区 + spill + merge 决定 map 端开销、shuffle copy 决定网络开销、sort + grouping 决定 reduce 端逻辑——这条链路是通用的。
2. 优化的核心是**用 CPU 换 IO、用内存换磁盘**：Combiner 用 CPU 换 shuffle 量、压缩用 CPU 换网络/磁盘字节、环形缓冲调大用内存换 spill 次数。每个参数都是取舍，没有银弹。
3. 数据倾斜是 MR（以及任何分布式聚合引擎）最常见也最棘手的问题。定位靠 counter + 采样 + task 日志；治理靠两阶段聚合、自定义 Partitioner、MapJoin、热点 key 隔离。两阶段聚合是其中最通用的武器。
4. 懂了 MR 的 Shuffle，Spark 的 sort shuffle、Tungsten 的 off-heap 排序、Flink 的网络栈都会变得好懂很多。这是性价比很高的"原理投资"。

写这篇的目的不是要大家回去写 MR——而是希望把这些底层模型当作理解现代引擎的脚手架。当你下次排一个 Spark 任务的长尾、调一个 Flink 作业的反压时，如果脑子里能浮现出 MR 那个环形缓冲区和 spill 文件，很多现象就不再神秘了。
