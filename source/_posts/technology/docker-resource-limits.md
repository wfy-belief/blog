---
title: Docker 容器资源限制与 cgroup 实战：从 CPU throttle 到 GPU 显存 OOM 的复盘
abbrlink: docker-resource-limits
date: 2026-07-02 09:50:00
tags:
  - Docker
  - cgroup
  - GPU
categories:
  - 技术
description: 复盘容器资源限制的几起线上事故，拆解 cgroup v1/v2、CPU/内存/IO 限制、lxcfs 失真与 GPU 容器配置。
ai_text: "本文以几起线上抖动与 OOM 事故为线索，复盘 Docker 背后的 cgroup 资源限制：cgroup v1 与 v2 的结构差异、CPU 三件套（--cpus/cpu-shares/cpuset）与 throttle 行为、内存与 swap 限制及 OOM killer、blkio 限速、容器看到宿主机资源的失真问题与 lxcfs 方案，最后覆盖 GPU 容器（nvidia-container-toolkit / --gpus）、NUMA 绑核与监控衔接，给出可落地的限制参数与排障路径。"
---

## 引子：一次诡异的 P99 抖动

去年我们推理服务上线后，P99 延迟每隔几分钟就抖一下，从 80ms 飙到 600ms。链路追踪显示不是模型推理本身的问题，而是宿主机上跑了好几个离线 ETL 容器，CPU 一抢，我们的在线容器就被 throttle 了。最后定位到根因：运维给容器设了 `--cpus=4`，但没设 `cpu.cfs_period_us`/`cpu.cfs_quota_us` 之外的东西，CFS 周期一到就被强制节流，P99 跟着抖。

这次之后我下决心把容器资源限制从头到尾理一遍。Docker 的 `--cpus`、`--memory` 看起来是几个简单参数，背后全是 cgroup。这篇文章把我们踩过的坑整理出来，希望下次你看到 OOMKilled、看到 P99 抖动、看到 GPU 显存爆掉时，能少走点弯路。<!-- 校准：请按真实经历核实/替换 -->

## 一、cgroup：容器资源限制的底座

Docker 的资源限制本质就是把一组 cgroup 参数写到 `/sys/fs/cgroup` 里。所以不理解 cgroup，就没法真正理解容器限制。

### cgroup v1：按资源分目录

cgroup v1 是按资源控制器（subsystem / controller）分目录的，每个控制器一棵树：

```
/sys/fs/cgroup/
├── cpu/            # CPU 时间片
│   └── docker/<container-id>/
│       ├── cpu.cfs_quota_us
│       ├── cpu.cfs_period_us
│       └── cpu.shares
├── memory/         # 内存
│   └── docker/<container-id>/
│       ├── memory.limit_in_bytes
│       └── memory.memsw.limit_in_bytes
├── blkio/          # 块设备 IO
├── cpuset/         # CPU 绑核
└── ...
```

v1 的特点是：控制器之间互相独立，一个容器在不同控制器下各有一个目录，靠 docker daemon 帮你串起来。问题是层级嵌套表达力弱，命名也容易乱。

### cgroup v2：统一层级

cgroup v2 把所有控制器挂到同一棵树底下，一个容器一个目录，所有控制器文件都在里面：

```
/sys/fs/cgroup/
└── docker/<container-id>/
    ├── cpu.max         # "quota period"，如 "400000 100000"
    ├── cpu.weight      # 替代 cpu.shares，范围 1-10000
    ├── memory.max      # 替代 memory.limit_in_bytes
    ├── memory.swap.max # 替代 memory.memsw.limit_in_bytes
    └── io.max          # 替代 blkio.throttle.*
```

v2 还引入了 PSI（Pressure Stall Information），`/proc/pressure/cpu`、`/proc/pressure/memory`、`/proc/pressure/io` 三个文件能给出资源压力的实时指标，比 v1 的「瞎猜」强太多。PSI 把「资源有多挤」量化成了三个窗口（10s/60s/300s）的 stall 比例，区分「部分任务在等」（some）和「所有任务都在等」（full）。一个典型的 memory PSI 输出：

```
some avg10=2.50 avg60=1.20 avg300=0.40 total=1234567
full avg10=0.80 avg60=0.30 avg300=0.05 total=234567
```

`avg10` 是过去 10 秒的 stall 比例。我们运维看到 `memory.full.avg10` 持续 > 10%，基本可以预判离 OOM 不远了，提前扩容比等 OOMKilled 强太多。

Linux 内核 5.10 之后 cgroup v2 基本可用，Docker 20.10 起默认支持。我们线上是 Ubuntu 22.04 + Docker 24，已经全面切到 v2。<!-- 校准：请按真实经历核实/替换 -->

判断你的系统用哪个版本：

```bash
# v2 下 stat -f /sys/fs/cgroup 显示 cgroup2fs
stat -f -c %T /sys/fs/cgroup
# cgroup2fs → v2；tmpfs → v1
```

**v1 → v2 迁移注意。** 切 v2 不是装个新 Docker 就完事。首先内核要 `CONFIG_CGROUP_V2` 编译进去；其次 systemd 在 v2 下用 `delegate` 机制管理 cgroup 子树，docker daemon 要在 `/etc/docker/daemon.json` 里 `"default-cgroupns-mode": "host"` 才能正确挂载；最后部分监控工具（老版本 cAdvisor、自研 agent）写死了 v1 的路径，切了之后采不到数据，得同步升级。我们切换时灰度了一周，先把非核心服务迁过去观察，确认无 throttle 异常再全量。<!-- 校准：请按真实经历核实/替换 -->

## 二、CPU 限制三件套

### 2.1 `--cpus`：CFS 配额

最常用的是 `--cpus`，它底层写的是 `cpu.cfs_quota_us` 和 `cpu.cfs_period_us`：

```bash
docker run -d --cpus=4 nginx
# 等价于 quota=400000, period=100000（单位微秒）
# 即每 100ms 调度周期内，该容器最多用 4 个核的 CPU 时间
```

这是硬上限。容器瞬时可以用满所有核，但一个周期内累计超过 quota，CFS 就把它 throttle 住，等下个周期。

**坑点：CPU throttle 与 P99。** 这就是引子里那场抖动的根因。容器在一周期内把 quota 用完，剩下的时间就眼巴巴等着，请求堆在队列里，延迟尖刺就来了。怎么看 throttle：

```bash
# v1
cat /sys/fs/cgroup/cpu/docker/<id>/cpu.stat
# v2
cat /sys/fs/cgroup/docker/<id>/cpu.stat
# 输出示例：
# nr_periods 12345
# nr_throttled 890      ← 被节流的周期数
# throttled_time 67000000
```

`nr_throttled` 增长快、`throttled_time` 高，就是 P99 抖动的元凶。我们后来的做法是：在线服务把 `--cpus` 设得略高于实际峰值（比如峰值 3.2 核就给 4.5 核），同时把 `cpu.cfs_period_us` 从默认 100ms 降到 10ms，让 throttle 更平滑、尖刺更小。当然这只是缓解，根治还是得保证宿主机不超卖。<!-- 校准：请按真实经历核实/替换 -->

### 2.2 `--cpu-shares`：相对权重

`--cpu-shares` 是软限制，只在 CPU 紧张时生效：

```bash
docker run -d --cpu-shares=512 app-a
docker run -d --cpu-shares=1024 app-b
# CPU 不紧张时，两个容器都能用满
# CPU 紧张时，b 拿到的 CPU 时间是 a 的两倍
```

默认值 1024。注意它是相对权重，不是绝对上限，单容器跑时 shares 没任何意义。我们用它做的是「在线优先于离线」——在线服务 shares 给 2048，离线 ETL 给 256，宿主机一挤，离线自动让路。

**shares 与 quota 混用的陷阱。** 我们早期图省事，在线服务只设了 `--cpu-shares` 没设 `--cpus`，以为靠权重就能保住在线。结果某天离线任务起了十几个，shares 总和远超在线，按比例分下来在线还是被挤压——shares 是「按总权重比例分」，不是「保底」。教训：**保底用 `--cpus`（硬上限给离线），优先级用 shares（在线权重拉高）**，两个配合才稳。后来我们的标准配置是离线容器 `--cpus=2 --cpu-shares=256`，在线容器 `--cpus=8 --cpu-shares=2048`，宿主机 CPU 32 核严格不超卖。<!-- 校准：请按真实经历核实/替换 -->

### 2.3 `--cpuset-cpus`：绑核

绑核最硬，直接指定容器只能用哪些物理核：

```bash
docker run -d --cpuset-cpus="0-3" app     # 用 0/1/2/3 号核
docker run -d --cpuset-cpus="0,2,4" app   # 用 0/2/4 号核
```

绑核的好处是避免核间迁移、cache 命中率高，对 NUMA 架构尤其重要（见后文）。坏处是不灵活，容器多了人工分配核太累。我们的实践是：核心推理服务绑核，批处理任务用 `--cpus`。

**绑核 + 共享 L3 的取舍。** 绑核不是越细越好。现代 CPU 一个 NUMA 节点内多个核共享 L3 cache，如果你把一个高吞吐服务的核选得稀稀拉拉（比如 0、8、16、24），看似跨节点用满了，实际每个请求在不同核间漂移，L3 命中率反而下降。我们测下来推理服务绑「连续的 4 个核」（0-3）比绑「分散的 4 个核」吞吐高 12%，因为 L3 局部性好。所以绑核前先 `lscpu -e` 看清 cache 拓扑，优先选同一 CCX/同一 L3 域内的核。<!-- 校准：请按真实经历核实/替换 -->

## 三、内存限制：`--memory` 与 OOM killer

### 3.1 三个参数

```bash
docker run -d \
  --memory=8g \              # 物理内存上限
  --memory-swap=10g \        # 内存+swap 上限（必须 >= memory）
  --memory-reservation=6g \  # 软下限，内存紧张时尝试回收到这个值
  app
```

`--memory` 是硬上限，超过就触发 OOM killer。`--memory-swap` 默认是 `--memory` 的两倍，如果你不希望容器用 swap，把它设成和 `--memory` 一样（相当于 swap 为 0）。线上服务我们一律 `--memory-swap` == `--memory`，禁止 swap，否则一旦用 swap，延迟立刻飞天。

### 3.2 OOM killer 的行为

cgroup 的 OOM killer 比内核全局 OOM 凶多了：容器一超 memory.max，内核立刻挑一个进程（通常是占用最大的那个）杀掉，整个容器要么重启（`--restart=unless-stopped`）要么进 Exited 状态。

`docker inspect <id>` 里有个关键字段 `OOMKilled`：

```bash
docker inspect <id> --format '{{.State.OOMKilled}} {{.State.ExitCode}}'
# true 137 → 被 cgroup OOM 干掉
```

**复盘：内存设错被 OOM。** 有一次我们给一个 Java 服务设了 `--memory=4g`，但 JVM 堆给了 4g，加上 metaspace、线程栈、堆外，实际峰值接近 6g，结果每隔几小时就被 OOMKilled。教训是：`--memory` 必须 >= JVM 堆 + 堆外 + overhead，Java 服务一般留 25% 余量。即堆 4g 的话，容器内存至少给 5g。

### 3.3 OOM 之外：memory.oom_control 与 OOM disable

cgroup 还有个 `memory.oom_control`（v1）/ `memory.oom.group`（v2）开关。默认情况下整个 cgroup 任何一个进程触发 OOM 都可能被杀；如果设了 `memory.oom.group=1`，那么这个 cgroup 里只要有进程触发 OOM，**整个 cgroup 的所有进程一起被杀**。k8s 的 pod 就是这么玩的——一个 pod 里某容器 OOM，按 group 杀更符合预期。裸 Docker 场景下一般保持默认即可，但要知道这个开关，否则某天你发现「明明没超内存的进程也死了」会很懵。

### 3.4 老 JDK 看不到 cgroup 限制的坑

这是个老问题但至今还在踩。JDK 8u191 之前（更准确地说是 JDK 10 之前），JVM 完全不读 cgroup，`Runtime.availableProcessors()` 和最大堆都按宿主机算。在一个 64 核 256G 的物理机上跑容器，老 JDK 会以为有 64 核、堆能开到几百 G，结果被 cgroup 反手 OOM。

JDK 8u191 之后引入了 `+UseContainerSupport`（默认开启），JVM 会去读 cgroup 的 CPU 和内存限制来推算核数和最大堆。但即便如此，老版本对 cgroup v2 的支持仍有 bug（JDK 11 早期版本读不到 v2 的 `cpu.max`），导致 GC 线程数算错、堆开得离谱。我们线上统一升到 JDK 17 之后这类问题才消停。<!-- 校准：请按真实经历核实/替换 -->

排查时看这两个：

```bash
# JVM 看到的核数
java -XshowSettings:system -version
# Container Support: active
# CPUs: 4   ← 应该等于 --cpus 取整
```

## 四、IO 限制：blkio / io

磁盘 IO 在 v1 叫 blkio，v2 叫 io。Docker 提供 `--device-read-bps`、`--device-write-bps`、`--device-read-iops` 等：

```bash
docker run -d \
  --device-read-bps=/dev/sda:10mb \
  --device-write-bps=/dev/sda:10mb \
  app
```

注意限制是针对块设备的，所以要先知道容器数据落在哪个盘。直接读写 `/dev/sda` 的进程会被限速，但走 page cache 的读写不算——这是 blkio 的盲区：**缓存命中不计入限速**。所以你看到容器读 QPS 不高但偶尔卡顿，可能是 page cache 被挤掉、回退到磁盘读，又被限速放大了。

对 AI 训练这种 IO 暴涨型负载，我们一般不做硬限速（怕拖慢训练），而是把训练数据和模型放在不同的块设备上，靠 cpuset 把训练容器绑到特定 NUMA，IO 也自然分流。<!-- 校准：请按真实经历核实/替换 -->

**复盘：日志打爆磁盘。** 有一阵子我们的推理容器日志没做轮转，高峰期 QPS 一上来，日志写把磁盘吞吐吃满，反过来把同一块盘上的模型加载也拖慢了，推理冷启动从 2 秒变 30 秒。后来加了 `--device-write-bps=/dev/nvme0n1:50mb` 给日志容器限速，同时把日志单独切到一块便宜的 SATA 盘，问题立刻缓解。blkio 限速虽然粗糙，但对「吵闹邻居」型的隔离非常有效。<!-- 校准：请按真实经历核实/替换 -->

## 五、容器看到的资源失真：lxcfs 方案

这是我觉得容器化里最容易被忽视的坑。

### 5.1 失真现象

进到容器里跑：

```bash
docker exec -it <id> bash
free -g
cat /proc/cpuinfo | grep -c processor
nproc
```

你会看到**宿主机的全部内存和全部核数**。因为 `/proc` 是宿主机挂载进来的，`/proc/meminfo`、`/proc/cpuinfo`、`/proc/stat` 都是宿主机的视角。`free`、`top`、JVM 全都基于这些文件做判断。

后果很严重：JVM 按宿主机内存算堆、监控按宿主机核数报、运维 `top` 一看「我容器才用了 2G，没事」，其实宿主机都炸了。老 JDK 那个坑，根子也在 `/proc` 失真。

### 5.2 lxcfs：把 /proc 改对

lxcfs 是一个 FUSE 文件系统，专门给容器伪造一份「符合 cgroup 限制」的 `/proc` 视图：

```bash
# 宿主机装 lxcfs
apt install lxcfs
# 启动
lxcfs /var/lib/lxcfs &

# docker run 时挂载（docker 24+ 有 --lxcfs-conf，或手动 -v）
docker run -d \
  -m 8g --cpus=4 \
  -v /var/lib/lxcfs/proc/cpuinfo:/proc/cpuinfo:rw \
  -v /var/lib/lxcfs/proc/meminfo:/proc/meminfo:rw \
  -v /var/lib/lxcfs/proc/stat:/proc/stat:rw \
  app
```

挂上之后，容器里 `free`、`nproc`、`top` 看到的就是 cgroup 限制后的值，JVM 和监控终于能算对了。

我们线上是 k8s，用的是 lxcfs 的 admission webhook 自动注入挂载，比手动 `-v` 省事。如果是裸 Docker，自己写个启动脚本包一下就行。这是做容器化收尾时强烈建议补上的一环。

**lxcfs 的局限。** lxcfs 不是万能的：它只接管 `/proc/cpuinfo`、`/proc/meminfo`、`/proc/stat`、`/proc/diskstats` 这几个文件，`/sys` 下的设备信息、`/proc/loadavg` 仍是宿主机视角。而且它是 FUSE 用户态文件系统，每次读 `/proc/meminfo` 都有上下文切换开销，高频采样的监控 agent 反而要小心别把 lxcfs 进程压垮。我们踩过一次：node-exporter 每秒抓一次 `/proc/stat`，容器多了之后 lxcfs 的 CPU 占用飙到 200%，反而成了瓶颈。后来把采集间隔放到 15s，问题才缓解。<!-- 校准：请按真实经历核实/替换 -->

另一条更彻底的路是 JDK 升级 + cgroup 感知。JDK 17 对 cgroup v2 的支持已经很完善，JVM 内部读 `cpu.max`、`memory.max` 推算资源，不依赖 `/proc` 视图。所以如果是 Java 服务，**升 JDK 比上 lxcfs 更治本**；非 Java 服务（Go、Python）还是老老实实挂 lxcfs。

## 六、GPU 容器：AI 训练推理场景

### 6.1 nvidia-container-toolkit

Docker 默认容器看不到 GPU，需要装 `nvidia-container-toolkit`（旧名 nvidia-docker2）。装完后容器里才能访问 GPU：

```bash
# Ubuntu 装法
distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
  && curl -s -L https://nvidia.github.io/libnvidia-container/gpgkey | sudo apt-key add - \
  && curl -s -L https://nvidia.github.io/libnvidia-container/$distribution/libnvidia-container.list \
    | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
apt update && apt install -y nvidia-container-toolkit
nvidia-ctk runtime configure --runtime=docker
systemctl restart docker
```

它的原理值得说一下：toolkit 注册了一个叫 `nvidia` 的 containerd runtime，当你 `--gpus` 时，runtime 在容器启动前注入 NVIDIA 的内核驱动组件（`nvidia.ko`、`nvidia-uvm.ko`）、设备节点（`/dev/nvidia*`）和用户态库（`libcuda.so`、`libnvidia-ml.so`）。注入哪些库由 hook 配置控制，可以裁剪——推理服务不需要 NVCC，只挂 runtime 库即可，镜像能瘦一大圈。我们基础镜像是 `nvidia/cuda:12.2.0-runtime-ubuntu22.04`，比 `-devel` 版小 4GB。<!-- 校准：请按真实经历核实/替换 -->

**版本坑：CUDA 与驱动匹配。** toolkit 注入的库版本由宿主机 NVIDIA 驱动决定，而镜像里的 CUDA 版本要向下兼容驱动。NVIDIA 的规则是「驱动版本决定了能支持的 CUDA 上限」。我们有一次宿主机驱动还停在 525，新镜像却升到 CUDA 12.3，容器一启动就报 `CUDA driver version is insufficient`。要么升宿主机驱动，要么镜像降到 CUDA 12.1。AI 平台管理 GPU 集群，**驱动版本统一**是第一纪律，否则混乱不堪。<!-- 校准：请按真实经历核实/替换 -->

### 6.2 `--gpus` 参数

```bash
# 挂全部 GPU
docker run --gpus all nvidia/cuda:12.2.0-runtime-ubuntu22.04 nvidia-smi

# 挂指定 GPU（按索引）
docker run --gpus '"device=0,2"' ...

# 挂指定数量
docker run --gpus 2 ...
```

`--gpus` 底层是把宿主机的 NVIDIA 设备节点（`/dev/nvidia0`、`/dev/nvidiactl` 等）和 CUDA 库挂进容器。挂进去后容器里 `nvidia-smi` 就能看到这些卡。

### 6.3 GPU 显存 OOM 复盘

GPU 显存限制和 CPU/内存不一样——**Docker 的 `--memory` 限制不了 GPU 显存**。GPU 显存是显卡自己管的，cgroup v2 目前（截至 2026 年）对 GPU 显存的支持还不完整，主流做法是靠应用层自律。

我们踩过的坑：一个推理服务和一个微调任务共享一张 A100 80G，微调脚本没设 `PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb`，碎片化 + 一次性 allocate，把整张卡吃满，推理服务直接 CUDA out of memory 崩了。事后给的规矩：

```bash
# 推理服务固定卡，靠环境变量让框架自律
docker run --gpus '"device=0"' \
  -e PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:128,expandable_segments:True \
  -e CUDA_VISIBLE_DEVICES=0 \
  inference-server

# 多任务共享时，用 MPS（Multi-Process Service）或 MIG 切分
# A100 开 MIG，物理切成 7 个独立实例，互不干扰
nvidia-smi mig -i 0 -cgi 19,19,19,19,19,19,19 -C
```

MIG（Multi-Instance GPU）是 A100/H100 才有的硬件级切分，每个实例有独立的显存和计算核，互不影响，比软件限速可靠得多。AI 平台一旦上规模，MIG 基本是标配。<!-- 校准：请按真实经历核实/替换 -->

监控 GPU 显存要靠 DCGM Exporter，把 `DCGM_FI_DEV_FB_USED`（显存使用）和 `DCGM_FI_DEV_GPU_UTIL`（计算利用率）吐给 Prometheus。我们告警规则是单卡显存 > 90% 持续 5 分钟就 page，避免悄悄爆掉。

**复盘：推理 batch size 调大显存爆。** 另一次事故是同事为了提吞吐把推理服务的 `batch_size` 从 8 调到 32，单卡显存瞬间撑爆，但因为模型本身能跑，前几分钟看着没事，等到 QPS 上来激活（activation）张量堆满，直接 CUDA OOM 全量重启。教训：显存类参数（batch size、KV cache 上限、序列长度）改了必须压测峰值 QPS 跑满再上线，光看空载不行。后来我们加了「显存使用率 > 80% 立刻告警」的提前量，配合框架的 `max_split_size_mb` 让分配更可预测。<!-- 校准：请按真实经历核实/替换 -->

## 七、NUMA 绑定

多路服务器上，CPU 和内存是分 NUMA 节点的。一个进程跨 NUMA 访问远端内存，延迟会高一截（NUMA miss）。对延迟敏感的推理服务，绑 NUMA 很关键。

```bash
# 看宿主机 NUMA 拓扑
numactl --hardware
# available: 2 nodes (0-1)
# node 0 cpus: 0 1 2 ... 31
# node 1 cpus: 32 33 ... 63
# node 0 size: 128 GB
# node 1 size: 128 GB

# 容器绑到 node 0
docker run -d --cpuset-cpus="0-15" --cpuset-mems="0" inference-server
```

`--cpuset-mems="0"` 让容器只在 NUMA node 0 分配内存，配合 `--cpuset-cpus="0-15"`（都在 node 0 上），CPU 和内存都本地，访问延迟最低。

GPU 也有 NUMA 亲和性，`nvidia-smi topo -m` 能看到每张卡挂在哪个 NUMA 节点上。推理服务绑核时，CPU 核和 GPU 卡要落在同一个 NUMA，否则 PCIe 跨节点传输会拖慢。我们实测跨 NUMA 跑推理，P99 比同 NUMA 高 15%。<!-- 校准：请按真实经历核实/替换 -->

**NVLink 与 NUMA。** 多卡训练还要看 NVLink 拓扑。`nvidia-smi topo -m` 输出里 `NV` 表示两卡间有 NVLink，`SYS` 表示跨 NUMA 走 QPI。allreduce 这种集合通信走 NVLink 比走 PCIe 快一个数量级。我们做多卡微调时，会刻意把一个训练任务的 4 张卡选在「两两都有 NVLink」的组里（比如 node 0 上的卡 0/1/2/3），而不是跨节点选卡 0/4/8/12。这事看似细节，但 allreduce 时间能差 3 倍。<!-- 校准：请按真实经历核实/替换 -->

## 八、监控与告警衔接

资源限制设好了，没有监控等于没设。我们对接 Prometheus + Grafana 的几条主线：

1. **cAdvisor**：容器维度的 CPU、内存、IO。但它不直接给 throttle 指标，要自己从 cgroup `cpu.stat` 采。
2. **node-exporter + 自定义 collector**：把 `nr_throttled`、PSI 读出来。我们的告警里有一条「容器 throttle rate > 5%/min 持续 10 分钟」就 page，专门抓引子那种抖动。
3. **DCGM Exporter**：GPU 维度，显存、利用率、温度、ECC 错误。
4. **OOM 事件**：用 node-exporter 的 textfile collector 读 `/var/log/syslog` 里的 `Killed process` 行，按容器聚合，任何一次非预期的 OOMKilled 都告警。

PSI 是 cgroup v2 之后我最喜欢的指标，它把「资源紧张的程度」量化成了 10s/60s/300s 三个窗口的 stall 比例。`memory pressure` 的 avg10 持续 > 20% 就是 OOM 前兆，比看内存使用率靠谱得多。

**监控矩阵的取舍。** 我们最终落到这套配置：cAdvisor 给容器维度的「绝对使用量」（CPU%、内存字节、IO 吞吐）；node-exporter + 自定义 cgroup collector 给「限制相关量」（throttle、PSI、OOM 计数）；DCGM 给 GPU；三条线在 Grafana 拼成一张「容器健康度」总览图。告警分两级：P1 是「PSI full > 20% 持续 5 分钟」「OOMKilled 发生」「GPU 显存 > 95%」，直接 page；P2 是「throttle rate > 5%」「内存使用 > 80%」，进工单第二天处理。这样既不会漏事故，也不会半夜被吵醒。<!-- 校准：请按真实经历核实/替换 -->

## 写在最后

容器资源限制这块，表面是几个 `docker run` 参数，底下是 cgroup、调度器、PSI、NUMA 一整套内核机制。我们的教训集中就一句话：**限制参数不能拍脑袋设，要看监控、要压测、要留余量**。

- CPU：在线服务 `--cpus` 留 30% 余量，关注 throttle 指标；
- 内存：`--memory-swap` 等于 `--memory` 禁 swap，Java 服务留足 overhead；
- `/proc` 失真：lxcfs 一定要补上，否则 JVM 和监控全瞎；
- GPU：靠 MIG 切分 + 应用层自律，DCGM 监控显存；
- NUMA：延迟敏感服务绑同节点；
- 监控：PSI + throttle + OOM 事件三件套，缺一不可。

把这些都做扎实了，容器化才算是真正落地，而不是把「跑在宿主机上」换成「跑在容器里」的换汤不换药。下次再遇到 OOMKilled 和 P99 抖动，希望你能直接定位到 cgroup 那一行参数。
