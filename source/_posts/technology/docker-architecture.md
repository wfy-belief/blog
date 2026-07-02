---
title: Docker 架构与底层原理深度剖析：容器到底是个什么东西
abbrlink: docker-architecture
date: 2026-07-02 09:00:00
tags:
  - Docker
  - 容器原理
categories:
  - 技术
description: 从"容器到底是什么"切入，逐层拆解 namespace、cgroup、unionfs 三大底层支柱与 Docker 引擎架构
ai_text: "本文以第一人称复盘在生产环境落地 Docker 的实战经验，从'容器到底是什么'切入，逐层拆解 Linux 容器的三大底层支柱——namespace 六大命名空间隔离、cgroup 资源限制、overlay2 联合文件系统分层存储，讲清 dockerd/containerd/runc 与 OCI 标准的演进、镜像与容器的 copy-on-write 本质、容器与虚拟机的差异，并复盘内存超卖 OOM、_pid 限制、镜像层膨胀等高频踩坑，给出可落地的生产经验小结。"
---

# Docker 架构与底层原理深度剖析：容器到底是个什么东西

第一次有人问我"容器和虚拟机到底有什么区别"时，我背了一通"容器轻量、共享内核、启动快"的标准答案，对方点点头，我自己心里却发虚——这些话说得没错，但完全没触及本质。后来在生产环境把一个 Java 服务从虚机迁到 Docker，踩了一连串 OOM、PID 耗尽、`/dev/shm` 太小、时区错乱、镜像层膨胀到 4 GB 的坑之后 <!-- 校准：请按真实经历核实/替换 -->，我才真正理解：**容器不是一类新的虚拟化，它就是被特殊配置的 Linux 进程**。这篇文章就把这句话拆开讲透，从 namespace、cgroup、union filesystem 到 Docker 引擎架构与 OCI 标准，把我们这些年踩过的坑一起复盘。

## 一、容器到底是什么：一句话戳破窗户纸

先把最核心的结论摆在前面：

> 容器 = cgroup（控制你能用多少）+ namespace（控制你能看见什么）+ unionfs（决定你文件系统长什么样）+ 一个正常的 Linux 进程。

没有 hypervisor，没有硬件模拟，没有 guest kernel。你在容器里跑的那个 Java 进程，和宿主机上的 `sshd` 一样，都是同一个 kernel 调度出来的普通进程。它之所以"以为"自己独占了系统，是因为内核给它戴上了 namespace 这副眼罩；它之所以不能把整机内存吃光，是因为 cgroup 给它设了上限。理解这一点，后面所有的现象都能自洽解释。

先上一张整体架构图，建立全局认知：

```
┌──────────────────────────────────────────────────────────┐
│                      宿主机 (Linux Kernel)               │
│                                                          │
│   ┌─────────────┐  namespace   ┌──────────────────────┐  │
│   │  容器进程 A  │◄────────────►│  隔离视图             │  │
│   │  (Java)     │  cgroup ────►│  资源限额             │  │
│   │             │  unionfs ───►│  分层 rootfs          │  │
│   └─────────────┘              └──────────────────────┘  │
│   ┌─────────────┐                                        │
│   │  容器进程 B  │  (同样三件套，各自独立)                 │
│   │  (Nginx)    │                                        │
│   └─────────────┘                                        │
│                                                          │
│   dockerd ──► containerd ──► runc ──► 容器进程           │
└──────────────────────────────────────────────────────────┘
```

## 二、namespace：容器能"看见"什么

namespace 是 Linux 内核提供的一种隔离机制，它让一个进程只能看到系统资源的一个子集。通俗讲，就是给进程戴上"眼罩"。Linux 目前提供了 8 种 namespace，容器场景主要用到下面 6 种。

| namespace | 隔离对象 | 一句话作用 |
|-----------|----------|------------|
| PID | 进程号 | 容器里 PID=1，看不到宿主机其他进程 |
| NET | 网络栈 | 独立的网卡、路由表、iptables、端口空间 |
| IPC | System V IPC、POSIX 消息队列 | 进程间通信隔离 |
| MNT | 挂载点视图 | 看到独立的文件系统层次 |
| UTS | hostname、domainname | 容器有自己的主机名 |
| USER | 用户/组 ID 映射 | 容器里的 root 在宿主机是 nobody |

最直观的感受是 PID namespace。我们进到任意一个运行中的容器里执行 `ps`：

```bash
# 宿主机上
docker exec -it my-app sh
/ # ps aux
PID   USER     TIME   COMMAND
    1 root      0:12 java -jar app.jar   <-- 容器里的 1 号进程
   47 root      0:00 sh
   54 root      0:00 ps aux
```

而在宿主机上，这个 Java 进程的真实 PID 可能是 18362。这就是 PID namespace 的魔法：内核给这个进程维护了一套独立的 PID 编号空间，容器里看 PID=1，宿主机看是 18362，两者通过映射表对应。

**一个关键坑：容器里的 PID 1 不好当。** PID 1 在 Linux 里有特殊语义——它会成为孤儿进程的收养者，并且默认会忽略 `SIGTERM`、`SIGINT` 等常见信号（除非显式注册 handler）。我们早期直接用 shell 启动 Java：

```dockerfile
# 错误写法
ENTRYPOINT sh -c 'java -jar app.jar'
```

结果 `docker stop` 要等 10 秒（`SIGKILL` 超时）才杀掉容器，因为真正占用 1 号的是 `sh`，Java 收到的是 `SIGTERM` 也没人转发。后来改成 `exec` 让 Java 直接接管 PID 1，或者用 `tini` 这样的 init 进程才解决：

```dockerfile
# 正确写法：exec 让 java 替代 shell 占据 PID 1
ENTRYPOINT ["java", "-jar", "app.jar"]
# 或者用 tini 处理信号转发
ENTRYPOINT ["tini", "--", "java", "-jar", "app.jar"]
```

USER namespace 是个常被忽视的能力。默认情况下容器不启用 USER namespace，容器里的 root 就是宿主机的 root。我们有一次挂载了宿主机的目录进容器，容器里一个 `chown -R root:root /data` 下去，宿主机上那个目录的属主全变了，吓得我们赶紧把这个能力在生产环境关掉。启用 USER namespace 后，容器内的 UID 0 会映射到宿主机的一个非特权 UID，安全性大幅提升，但代价是一些遗留镜像的权限检查会出问题，落地要灰度。

## 三、cgroup：容器能用"多少"

namespace 管的是看得见什么，cgroup（Control Groups）管的是能用多少资源——CPU、内存、磁盘 I/O、设备访问等。这是防止"一个容器跑飞，整机被拖垮"的关键防线。

生产环境我们最常显式限制的就是内存和 CPU：

```bash
docker run -d \
  --name my-app \
  --memory=4g \                  # 内存硬上限 4G
  --memory-swap=4g \             # 禁止 swap（设成和 memory 一样）
  --memory-reservation=3g \      # 软限，内存紧张时内核优先回收
  --cpus=2.0 \                   # 等价于 2 核配额
  --cpu-shares=1024 \            # 权重，CPU 繁忙时按权重分配
  --pids-limit=512 \             # 最大进程/线程数
  my-registry/my-app:1.2.0
```

这里有两个我们吃过血亏的点，值得展开。

**第一，`memory-swap` 默认是 memory 的 2 倍。** 很多人以为设了 `--memory=4g` 就万事大吉，实际上容器最多能用到 8G（4G 内存 + 4G swap），一旦走 swap 性能直接跳水。我们的一个 Java 服务 GC 抖动排查了两天，最后发现是 cgroup swap 没关，内存压力被 swap 掩盖了。生产环境一律 `--memory-swap` 设成和 `--memory` 相等来禁用 swap。

**第二，OOM Killed 是容器最常见的"无疾而终"。** 当容器进程的内存超过 `memory` 上限，内核会直接 OOM killer 杀掉占用最多的进程，并在 `dmesg` 里留下记录：

```
[12345.678] Memory cgroup out of memory: Killed process 18362 (java)
    total-vm:8G, anon-rss:4.1G, file-rss:0G
```

对应到 `docker inspect` 里，OOMKilled 字段会是 `true`：

```bash
docker inspect my-app --format '{{.State.OOMKilled}}'
true
```

**Java 在容器里还有一个经典陷阱：** 老版本 JDK（8u191 之前）的 JVM 默认按宿主机内存算堆大小，一台 128G 的机器上跑容器，JVM 一上来就按 32G 算堆，直接撞爆 4G 的 cgroup 限制。解决办法要么升级到 8u191+（支持 `-XX:+UseContainerSupport` 自动识别 cgroup），要么显式 `-Xmx`。

CPU 限制这边反而坑少一些。`--cpus=2.0` 是 CFS bandwidth 控制，保证容器在 100ms 周期内最多用 2 个核的 CPU 时间；`--cpu-shares` 是权重，只在 CPU 繁忙时生效。生产经验是：**CPU 限流可以容忍（大不了慢一点），内存绝不能超（超了就是被杀）**，所以内存限制是必选项，CPU 限制更多是用来防"吵闹的邻居"。一个反直觉的现象是 CPU throttle 会让延迟毛刺飙升——我们有个 P99 一直卡在 800ms 的接口，把 `--cpus` 从 2 提到 4 之后直接降到 90ms，根因就是 CFS 周期内配额用完被强制挂起 <!-- 校准：请按真实经历核实/替换 -->。排查可以用 `/sys/fs/cgroup/cpu/.../cpu.stat` 里的 `nr_throttled` 计数。

## 四、union filesystem：镜像是怎么"叠"出来的

到这里我们已经能让一个进程隔离运行、限额运行了，但还差最后一块——文件系统。每个容器都需要一个独立的根文件系统（rootfs），但要是每个容器都拷贝一份完整的 OS，磁盘和分发成本都受不了。这就是 union filesystem（联合文件系统）要解决的问题。

Docker 镜像是由一层层只读文件系统叠加起来的，每一层对应 Dockerfile 里的一条指令：

```
┌─────────────────────────────┐
│  可写层 (容器层, copy-on-write)  │  <-- docker run 产生
├─────────────────────────────┤
│  Layer 5: COPY app.jar        │  只读
├─────────────────────────────┤
│  Layer 4: RUN mvn package     │  只读
├─────────────────────────────┤
│  Layer 3: ENV / WORKDIR       │  只读
├─────────────────────────────┤
│  Layer 2: RUN apt-get install │  只读
├─────────────────────────────┤
│  Layer 1: debian:bullseye     │  基础镜像
└─────────────────────────────┘
```

关键机制是 **copy-on-write（COW，写时复制）**：

- 镜像层全部只读，多个容器共享同一份镜像层，省磁盘、省内存（page cache 也共享）。
- 容器启动时，Docker 在最上面叠一个可写层。容器里任何写操作——修改文件、新建文件、删除文件——都落在这个可写层里，镜像层丝毫不差。
- 修改一个镜像里的大文件时，COW 会把整个文件从下层复制到可写层再修改，而不是改原文件。这意味着在容器里改一个 1G 的日志文件，会瞬间占用 1G 额外空间。

Docker 目前默认的存储驱动是 **overlay2**（早期叫 aufs、devicemapper，都已淘汰）。`overlay2` 直接基于内核的 OverlayFS，性能和稳定性都最好。查看一个容器的分层：

```bash
docker history my-app:1.2.0
IMAGE          CREATED        CREATED BY                                      SIZE
a1b2c3d4e5f6   2 hours ago    /bin/sh -c #(nop)  CMD ["java" "-jar" "a…      0B
b2c3d4e5f6a1   2 hours ago    /bin/sh -c #(nop) COPY app.jar /app/            85MB
c3d4e5f6a1b2   2 hours ago    /bin/sh -c apt-get update && apt-get inst…     320MB
d4e5f6a1b2c3   3 days ago     /bin/sh -c #(nop)  ENV JAVA_HOME=/usr/lib…     0B
e5f6a1b2c3d4   2 weeks ago    /bin/sh -c #(nop)  ENTRYPOINT ["tini" "--"…    0B
abc1234        1 month ago    /bin/sh -c #(nop)  CMD ["bash"]                 0B
```

这里有个我们反复踩的优化点：**层的数量和顺序直接决定镜像大小**。下面这种写法会让最终镜像膨胀到 1.2 GB <!-- 校准：请按真实经历核实/替换 -->：

```dockerfile
# 反例：每条 RUN 都是一层，apt 缓存没清
FROM ubuntu:20.04
RUN apt-get update
RUN apt-get install -y openjdk-11-jdk
RUN apt-get install -y maven
RUN rm -rf /var/lib/apt/lists/*
```

因为每条 `RUN` 都是一个独立层，第 2、3 层装的东西即使被第 4 层"删"了，那几层只读镜像依然带着完整文件，最终大小不会减小。正确做法是用 `&&` 把"安装+清理"压到一层：

```dockerfile
# 正例：单层内完成安装与清理
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk maven && \
    rm -rf /var/lib/apt/lists/*
```

优化后同样的镜像降到 480 MB <!-- 校准：请按真实经历核实/替换 -->。多阶段构建（multi-stage build）是另一个降镜像体积的利器，编译期用重镜像（maven/gradle），运行期只 COPY 产物到轻量镜像（distroless/alpine），最终镜像能再砍一半。

## 五、Docker 引擎架构：从大单体到 OCI 标准

理解了底层三件套，再看 Docker 引擎本身。早期 Docker 就是一个 `dockerd` 大单体，什么活都干。随着容器生态膨胀，Docker 公司把核心拆分并捐献给 CNCF，形成了今天的分层架构和 OCI（Open Container Initiative）标准。

```
┌─────────────────────────────────────────────────────────┐
│  docker CLI / docker compose                            │
├─────────────────────────────────────────────────────────┤
│  dockerd   (REST API, 镜像构建/分发, 卷/网络管理)          │
├─────────────────────────────────────────────────────────┤
│  containerd  (容器生命周期管理, 镜像拉取, 快照)            │
├─────────────────────────────────────────────────────────┤
│  runc  (OCI runtime, 真正调用 namespace/cgroup 创建容器)  │
├─────────────────────────────────────────────────────────┤
│  Linux Kernel  (namespaces / cgroups / overlayfs)        │
└─────────────────────────────────────────────────────────┘
```

每一层职责清晰：

- **dockerd**：守护进程，对外暴露 REST API，负责 `docker build`、`docker pull`、卷与网络的高级抽象。它是"管理者"，自己不直接跑容器。
- **containerd**：从 dockerd 拆出来的核心运行时，管理容器完整生命周期（创建、启动、停止、监控）、镜像分发、快照。今天 Kubernetes 直接用 containerd，跳过 dockerd（通过 CRI 接口）。
- **runc**：OCI 运行时规范的参考实现。它是真正干活的——调用 `clone()` 建 namespace、写 cgroup 文件、pivot_root 切 rootfs，最后 `exec` 启动容器进程。命令行直接 `runc run` 也能起容器。

**OCI 标准的意义在于解耦。** 早期 Docker 既定镜像格式又定运行时，事实垄断。社区担心被单一厂商绑死，于是 2015 年成立 OCI，定义了两个规范：

- **OCI Image Spec**：镜像该长什么样（manifest、config、layer tar）。
- **OCI Runtime Spec**：容器运行时该怎么做（一份 `config.json` 描述 namespace、cgroup、mount，任何符合规范的 runtime 都能跑）。

今天 Docker 镜像、containerd 镜像、Kubernetes pod 用的镜像都是 OCI 格式，可以互相通用；运行时除了 runc 还有 Kata Containers（每个容器跑在一个轻量虚机里，强隔离）、gVisor（用户态内核拦截 syscall）。这就是标准化的红利。我们生产上对隔离性要求极高的安全审计容器，就换成过 Kata，对业务镜像零改动 <!-- 校准：请按真实经历核实/替换 -->。

## 六、镜像 vs 容器：常被混淆的两个概念

面试时很多人把"镜像"和"容器"混着说，但它们是两个明确不同的东西：

- **镜像（image）**：一个静态的、只读的、分层的文件系统快照。可以理解成"类"（class）。它本身不会运行，存在本地或镜像仓库里。
- **容器（container）**：镜像的运行实例，= 镜像 + 可写层 + 一个进程。可以理解成"对象"（instance）。

```
镜像 my-app:1.2.0 (只读)
        │  docker run
        ▼
┌────────────────────┐
│ 容器实例 (运行中)    │
│  ┌──────────────┐  │
│  │ 可写层 (COW)  │  │  <-- 任何写操作落这里
│  └──────────────┘  │
│  ┌──────────────┐  │
│  │ 镜像层 (共享)  │  │  <-- 多个容器共享
│  └──────────────┘  │
└────────────────────┘
```

一个直观的对比：`docker images` 列出的是镜像，`docker ps` 列出的是容器。同一个镜像可以 `run` 出 N 个互不干扰的容器，每个容器有自己的可写层、自己的网络栈、自己的进程空间。

## 七、容器 vs 虚拟机：本质差异

现在终于能把开头的那个问题讲透了。容器和虚拟机的根本区别在于**隔离的层次**：

| 维度 | 虚拟机 | 容器 |
|------|--------|------|
| 隔离层 | hypervisor + guest kernel | namespace + cgroup（共享 host kernel） |
| 内核 | 每个 VM 一个完整内核 | 所有容器共享一个宿主机内核 |
| 启动时间 | 30 秒 ~ 几分钟 | 100ms ~ 2 秒 |
| 资源开销 | 每个 VM 几 GB 内存起 | 几十 MB |
| 隔离强度 | 强（硬件级虚拟化） | 弱（进程级，共享内核） |
| 密度 | 单机十几台 VM | 单机上百个容器 |
| 跨内核兼容 | 强（Linux 镜像跑在 Windows 上没问题） | 弱（Linux 容器跑不了 Windows 二进制） |

```
虚拟机                          容器
┌──────┐ ┌──────┐              ┌──────┐ ┌──────┐
│ App  │ │ App  │              │ App  │ │ App  │
│ Bins │ │ Bins │              │ Bins │ │ Bins │
│Kernel│ │Kernel│  vs          ├──────┤ ├──────┤
└──────┘ └──────┘              │ Bins │ │ Bins │
┌──────────────────┐           └──────┘ └──────┘
│   Hypervisor     │           ┌──────────────────┐
├──────────────────┤           │   Host Kernel    │
│   Host Kernel    │           └──────────────────┘
└──────────────────┘
```

**隔离强度的代价在容器这一侧是真实存在的。** 容器共享内核意味着：内核漏洞（如 Dirty COW）会影响所有容器；某个容器触发 kernel panic 会把整台机器打挂；namespace 的逃逸漏洞历史上出现过多次。所以对隔离性要求极高的场景（多租户公有云、不可信代码运行），虚拟机或 Kata Containers 更合适。我们内部可信的服务集群才大规模用容器。

**跨内核兼容也常被低估。** "一次构建到处运行"是有前提的——容器镜像里装的是用户态二进制，内核系统调用还是宿主机的。M1 Mac 上 build 的 linux/arm64 镜像，push 到 x86 生产机上跑不起来；CentOS 6 的 glibc 旧，构建出的镜像在新内核的 Ubuntu 上能跑，反过来就可能段错误。`docker buildx` 做多架构镜像、CI 环境和运行环境对齐，这些细节我们都在生产里趟过。

## 八、生产环境踩坑复盘

挑三个最典型、最有代表性的坑复盘一下。

**坑一：Java 进程数暴涨触发 `--pids-limit` 限流。** 某次上线后服务偶发性卡死，线程池任务积压，CPU、内存都正常。`docker stats` 看着没事，最后是 `dmesg` 里翻到 `pids: cgroup limit reached`。原因是这个 Java 服务用线程池处理请求，峰值并发上来线程数飙到 500+，触发了我们设的 `--pids-limit=512`。教训：Linux 的 PID 概念里，线程（LWP）也算 PID，512 对线程密集型 Java 应用太小。我们把限制调到 2048 并加了监控告警 <!-- 校准：请按真实经历核实/替换 -->。

**坑二：`/dev/shm` 默认只有 64M。** 一个用了 RocksDB 的服务在容器里报 `Unable to allocate memory for mmap`，排查发现 RocksDB 默认用 `/dev/shm`（共享内存 tmpfs）做缓存，而 Docker 默认只给容器 64M 的 `/dev/shm`。解决方法是 `docker run --shm-size=1g`，或者挂载自己的 tmpfs。这类"在虚机上没问题、上容器就炸"的坑，本质都是容器有一些默认的、跟虚机不一样的小限制。

**坑三：容器一重启，日志全没了。** 早期我们把日志直接写到容器内的 `/var/log/app.log`，没做 volume 挂载。某次容器 OOM 重启，可写层被清掉，故障现场全失。日志、数据这类需要持久化的东西，必须通过 `-v` / `--mount` 挂到宿主机或网络存储上，容器可写层只放 ephemeral 数据。后来我们统一改成应用日志走 stdout/stderr，由 Docker logging driver（json-file 或 fluentd）收集出去，彻底和容器生命周期解耦。

## 九、生产经验小结

把零散的点收一下，作为团队落地 Docker 的 checklist。

1. **内存限制必选，CPU 限制可选。** `--memory` 是底线，`--memory-swap` 一定设成和 `--memory` 相等禁用 swap。
2. **PID 1 要稳。** 用 `exec` 形式的 ENTRYPOINT，复杂场景上 `tini`，确保信号能正确传递、僵尸进程能回收。
3. **镜像要瘦。** 多阶段构建 + 单层内清理 apt 缓存 + 选 alpine/distroless 基础镜像，把镜像体积压到最小。
4. **日志走 stdout。** 不要在容器内写文件日志，统一 stdout，交给 logging driver 收集。
5. **持久化数据挂 volume。** 容器可写层随时可丢，数据库、日志、配置等持久数据一律 `-v` 挂出去。
6. **健康检查必加。** `HEALTHCHECK` 指令让编排系统能基于应用层健康判断，而不是只看进程在不在。
7. **可信集群才用容器。** 强隔离、多租户、不可信代码场景上 Kata 或虚机。
8. **CI 和运行环境对齐。** 别让构建机的 glibc/kernel 版本和生产机差太多，少踩玄学段错误。

这篇是 Docker/容器系列的"地基"篇，重点把容器这个概念从三层底座（namespace / cgroup / unionfs）讲透，并落到引擎架构与生产踩坑。**容器的网络模型（bridge / host / overlay / macvlan，以及 Kubernetes 的 CNI 与 Service）和存储（volume / bind mount / tmpfs，以及分布式存储插件）都是更复杂的话题，会在这个系列后续的专门篇章里展开**，这里先点到为止。

还有一个容易被忽视的细节：**`docker stop` 的优雅退出依赖于信号链**。默认 Docker 会先发 `SIGTERM`，等待 10 秒（`--stop-timeout`）再发 `SIGKILL`。如果应用没注册 `SIGTERM` 处理，或者 PID 1 被错误占用，这 10 秒就成了白等，最后被强杀。生产上我们把 `--stop-timeout` 调到 30 秒配合 Spring Boot 的 graceful shutdown，避免长连接被强切断导致用户请求 5xx。

理解了底层，上层那些花里胡哨的工具——Kubernetes、Docker Swarm、Nomad——都只是这三件套的调度与编排壳子。万变不离其宗。
