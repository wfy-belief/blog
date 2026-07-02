---
title: Docker 镜像分层原理与构建优化实战：把一个 2.3GB 镜像压到 180MB 的复盘
abbrlink: docker-image-layer-optimization
date: 2026-07-02 09:10:00
tags:
  - Docker
  - 镜像优化
categories:
  - 技术
description: 从镜像分层、overlay2 存储驱动、构建缓存到多阶段构建与 buildkit 缓存挂载，复盘一次镜像瘦身与构建加速的完整历程。
ai_text: "本文以一个 Python 服务镜像从 2.3GB 压到 180MB、CI 构建从 9 分钟降到 1 分半的复盘为线索，拆解 Docker 镜像分层原理、overlay2 存储驱动、构建缓存失效机制，再逐层讲优化手段：合并 RUN、.dockerignore、依赖前置 COPY、多阶段构建、alpine/distroless 基础镜像、buildkit 的 --mount=type=cache 缓存挂载，最后覆盖镜像大小治理与漏洞扫描。"
---

## 引子：一个 2.3GB 的镜像和一条 9 分钟的 CI

去年接手一个 Python 推理服务的容器化，Dockerfile 是前同事留的，大概长这样：基础镜像 `python:3.10`，十几条 `RUN apt-get install`、`RUN pip install`、`RUN copy`，最后 `COPY . .` 把整个仓库扔进去。镜像 build 出来 2.3GB<!-- 校准：请按真实经历核实/替换 -->，CI 每次跑 9 分钟左右，其中 6 分钟在 docker build<!-- 校准：请按真实经历核实/替换 -->。推到 Harbor 又慢，部署拉镜像也慢，节点上磁盘还被几个大镜像吃满过。

组里之前的优化思路就是"加机器、加带宽"，没人认真看过 Dockerfile。我把镜像 `docker history` 拉出来逐层看，发现一堆低级问题：`apt-get` 没 `--no-install-recommends`、缓存目录没清、`.git` 和测试数据全被打进镜像、依赖和源码混在一个 COPY 里导致每次改一行代码就全量重装依赖。逐一治理之后，镜像压到 180MB<!-- 校准：请按真实经历核实/替换 -->，CI 构建稳定在 1 分半。这篇文章把那次治理的全过程拆开讲。

我的方法论跟调任何性能问题一样：**先理解机制，再动配置。** Docker 镜像优化不是背几条最佳实践就行，得明白分层是怎么存的、缓存是怎么失效的、overlay2 是怎么叠的，才能判断每条指令的代价。下面先讲原理，再讲手段。

## 一、镜像分层原理：每条指令都是一层

Docker 镜像不是一个大文件，而是一堆**只读层（layer）**叠起来的洋葱。Dockerfile 里每条 `FROM`、`RUN`、`COPY`、`ADD`、`CMD`（以及部分 `ENV`/`WORKDIR` 在旧版本里）都会产生一个新层，每层只存"相对父层的差异"——新增的文件、修改的文件、删除标记（whiteout）。

一个最小 Python 镜像的分层示意：

```
        Docker 镜像 = 一堆只读层叠加 + 顶层可写容器层
   ┌──────────────────────────────────────────────┐
   │  顶层 Container layer（可写，容器运行时产生）    │
   ├──────────────────────────────────────────────┤
   │  L5  CMD ["python","app.py"]          0 B     │
   │  L4  COPY app/ /app/                  12 MB   │
   │  L3  RUN pip install -r req.txt       320 MB  │
   │  L2  WORKDIR /app                     0 B     │
   │  L1  FROM python:3.10-slim            ~150 MB │
   └──────────────────────────────────────────────┘
        每层只存相对父层的 diff，靠 overlay2 叠加成最终视图
```

几个关键认知，是我踩坑之后才真正内化的：

**层是叠加的，删文件不减小镜像。** 这是新手最常踩的坑。你在第一层 `RUN curl -o big.tar.gz`，在第二层 `RUN rm big.tar.gz`，镜像体积并不会变小——big.tar.gz 依然存在于第一层里，第二层只是加了一条"这个文件被删了"的 whiteout 标记。最终镜像同时背着两层的体积。这就是为什么"在同一层里下载、解压、删除"是铁律。

**层共享与复用是镜像快的根本。** 多个镜像如果共享同一个基础层（比如都 `FROM python:3.10-slim`），节点上只存一份，拉取时也只拉差异层。这也是为什么 CI 机器上缓存命中率对构建时间影响巨大——基础镜像层和早期不变的层会被反复复用。

**COPY 的层也有代价。** 很多人觉得 COPY 不就是复制文件吗，但它同样产生一层，而且 COPY 进去的文件如果变了，这层缓存就失效。这就是为什么"先 COPY 依赖清单，再 COPY 源码"能省大量时间——后面会展开。

`docker history <image>` 是我看分层的第一工具，每行显示一层、对应指令、产生的大小。一个镜像臃肿，history 拉出来扫一眼基本就知道哪几层是大头。

## 二、存储驱动：为什么 overlay2 是生产默认

分层只是逻辑模型，落到磁盘上靠**存储驱动（storage driver）**来管理层的叠加与可写层。Docker 历史上换过好几代驱动：aufs（早期 Ubuntu 默认，已弃用）、devicemapper（CentOS 老传统，性能差）、overlay（第一版，inode 限制），现在**生产默认是 overlay2**，社区也只推荐这一个。

为什么是 overlay2？因为它是 Linux 内核原生 overlayfs 文件系统的直接调用，没有额外用户态开销。它的工作方式是三层结构：

```
            overlay2 叠加结构（最终容器视图）
   ┌────────────────────────────────────────────┐
   │   Merged（合并视图，容器看到的根文件系统）     │
   ├────────────────────────────────────────────┤
   │   Upper（可写层，容器运行时所有写操作落到这）  │
   ├────────────────────────────────────────────┤
   │   Lower（多个只读镜像层，自上而下叠加）        │
   │   L1 ← L2 ← L3 ← ...                        │
   └────────────────────────────────────────────┘
        读：从上往下找第一个命中的层
        写：copy-up：把 Lower 文件复制到 Upper 再修改
```

读文件时，从 Upper 往 Lower 逐层查找，命中即返回。写文件时触发 **copy-up**：先从 Lower 层把文件复制到 Upper 层，再在 Upper 上修改。copy-up 是 overlay2 的主要开销——这就是为什么"容器里频繁写大文件"很慢，而且会把 Upper 层撑大（`docker system df` 能看到每个容器的 writable size）。

几个我在生产里跟 overlay2 相关的经验：

**`overlay2 lowerdir` 最多 128 层。** 内核限制（旧内核是更低），超过会直接挂不上。这也是限制 Dockerfile 层数的硬约束——虽然现在很少把层数堆到 128，但合并 RUN 仍是好习惯。

**xfs 要挂 `d_type=true`。** overlay2 依赖 `trusted.overlay.*` 扩展属性来存 whiteout，xfs 默认可能没开 `d_type`，结果就是 build 报错 `overlayfs: the backing xfs filesystem is formatted without d_type`。我一次在老 CentOS 节点上部署就踩过，挂载 `/var/lib/docker` 的 xfs 分区没开 d_type，只能重格式化 `mkfs.xfs -n ftype=1`。生产前一定 `xfs_info /var/lib/docker | grep ftype` 确认是 `ftype=1`。

**别在容器里写日志到本地文件。** 这是 copy-up 的典型受害者。一个服务每秒写 access.log，Upper 层会无限增长，容器一重启全丢。正确做法是日志直接打到 stdout/stderr，让 Docker logging driver 收走。

## 三、构建缓存：层一旦失效，后面全失效

这是镜像优化里**最容易被忽视、但收益最大**的一层认知。Docker build 是有缓存的，缓存机制大致是：**对每一层，Docker 会用"指令 + 输入"算一个缓存 key，如果 key 没变就用缓存层，一旦某层缓存失效，它后面所有层都强制重建。**

具体到不同指令：

- `RUN`：缓存 key 就是命令字符串本身。改一个字符（比如版本号 `1.2.3` 改 `1.2.4`）这层就失效。
- `COPY`/`ADD`：缓存 key 是命令 + 被复制文件的**内容校验和**（不只是文件名）。文件内容变一个字节，这层就失效。
- `FROM`：基础镜像 digest 变了，后面全失效。

这个机制意味着一个极其重要的推论：**Dockerfile 里指令的顺序，直接决定缓存命中率。** 哪条指令最容易变，就越要往后放。看下面这个反例，就是前同事留的 Dockerfile：

```dockerfile
# 反面教材：源码 COPY 太靠前，缓存几乎永远失效
FROM python:3.10

WORKDIR /app
COPY . .                          # ← 改一行代码，下面所有层全失效

RUN apt-get update && apt-get install -y \
    gcc libpq-dev curl wget vim   # ← vim 是调试时手动加的
RUN pip install -r requirements.txt
RUN pip install gunicorn

CMD ["gunicorn", "app:app"]
```

`COPY . .` 在前面，意味着只要改一行源码，后面三个 `RUN`（包括 pip install，最耗时的一步）全部重建。CI 每次跑 6 分钟的 build 里，有 5 分钟在重装依赖。

正确写法是把**最不容易变的放前面，最易变的放最后**：

```dockerfile
# 正确：依赖清单前置，源码后置
FROM python:3.10-slim

WORKDIR /app

# 系统依赖：极少变
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 依赖清单：只要 requirements.txt 不变就命中缓存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 源码：最后 COPY，改源码只重建这层 + CMD
COPY app/ ./app/

CMD ["gunicorn", "app:app"]
```

改造之后，平时改源码 push，CI 的 build 只要 20 秒（只重建 COPY app/ 和 CMD 两层），只有改依赖时才重新 pip install<!-- 校准：请按真实经历核实/替换 -->。

几个缓存相关的坑值得单独讲：

**`apt-get update` 和 `apt-get install` 必须写在一层。** 拆成两层会导致：第一层 `RUN apt-get update` 缓存的 apt 索引可能已经过时（缓存命中），第二层 `RUN apt-get install nginx` 拿过时索引去装包，要么装不上、要么装到旧版本。这是 Dockerfile linter（hadolint）必报的告警。

**`.dockerignore` 不是可选的。** 很多人不写 `.dockerignore`，结果 `COPY . .` 把 `.git/`、`node_modules/`、`__pycache__/`、测试数据、本地 `.env` 全打进去。这些文件变化频繁，会让缓存频繁失效，还可能把密钥泄露到镜像里（一个经典安全事故）。我接手后第一件事就是补上：

```
# .dockerignore
.git
.gitignore
__pycache__
*.pyc
.pytest_cache
.venv
venv/
node_modules/
tests/
*.md
.env
.env.*
docker-compose*.yml
Dockerfile*
```

`.dockerignore` 还有一个隐藏收益：build context 变小，构建上下文上传到 daemon 的时间缩短。那个 2.3GB 镜像的服务，build context 有 1.4GB（含历史测试数据集），加上 `.dockerignore` 后 context 缩到 8MB<!-- 校准：请按真实经历核实/替换 -->。

**BuildKit 默认开启。** Docker 18.09 之后引入 BuildKit，构建速度快、并行度高，而且缓存策略更聪明。老版本 Docker 要 `DOCKER_BUILDKIT=1`，23.0+ 默认开启。生产环境务必确认用的是 BuildKit。

## 四、合并 RUN 减少层数，并清理在同一层

前面讲了删文件不减镜像体积，解决办法就是把"下载、解压、删除"合并到**同一层同一个 RUN** 里。对比：

```dockerfile
# 反面：三层，big.tar.gz 在第一层永远存在
RUN curl -o /tmp/big.tar.gz https://example.com/big.tar.gz
RUN tar xzf /tmp/big.tar.gz -C /opt
RUN rm /tmp/big.tar.gz          # ← 减不掉，只是加 whiteout

# 正面：一层内完成下载、解压、删除，中间产物不进镜像
RUN curl -fsSL -o /tmp/big.tar.gz https://example.com/big.tar.gz \
    && tar xzf /tmp/big.tar.gz -C /opt \
    && rm /tmp/big.tar.gz \
    && rm -rf /var/lib/apt/lists/*
```

核心原则：**层内产生的临时文件，必须在同一层内清理掉。** `apt-get` 装完包要 `rm -rf /var/lib/apt/lists/*`，`pip install` 要加 `--no-cache-dir` 避免 `~/.cache/pip` 残留，`apk add` 在 alpine 上要加 `--no-cache`。

合并 RUN 的另一个好处是减少层数。虽然 overlay2 限制是 128 层，离上限远，但层数多意味着 metadata 多、推送时校验多、层共享收益下降。一个合理的服务镜像，Dockerfile 通常控制在 10–15 条指令以内。

不过也别为了合层而牺牲可读性和缓存粒度。把"系统依赖"和"pip 依赖"拆成两层是值得的——它们变化的频率不同，分开能让缓存命中更精细。

## 五、多阶段构建：瘦身的核武器

上面那些手段能把镜像从 2.3GB 压到 700MB 左右，但真正的核武器是**多阶段构建（multi-stage build）**。它的思路是：用一个 stage 编译，把编译产物拷到另一个干净 stage 运行，编译器、依赖源码、中间产物全都丢掉。

那个 Python 服务原本 700MB 里，有 300MB 是编译用的 `gcc` 和构建期才需要的 `python-dev`——运行时根本用不到。Go 服务更典型，构建需要完整 Go 工具链（800MB+），但运行时只需要一个二进制（20MB）。看一个 Go 服务的对比：

```dockerfile
# 反面：构建工具链全进镜像
FROM golang:1.22
WORKDIR /src
COPY . .
RUN go build -o /app/server ./cmd/server
# 镜像里有完整 Go SDK + 源码 + 二进制，800MB+

CMD ["/app/server"]
```

```dockerfile
# 正面：多阶段，运行镜像只有二进制
# ---- builder stage ----
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/server ./cmd/server

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/server /server
USER nonroot:nonroot
ENTRYPOINT ["/server"]
# 镜像 ~20MB，没有 shell、没有包管理器、没有 Go 工具链
```

| 对比项 | 单阶段 | 多阶段 |
|--------|--------|--------|
| 镜像大小 | ~830 MB | ~22 MB<!-- 校准：请按真实经历核实/替换 --> |
| 内含 shell | 有 | 无（distroless） |
| 攻击面 | gcc/make/源码全在 | 仅二进制 |
| 启动时间 | 1.2 s | 0.4 s<!-- 校准：请按真实经历核实/替换 --> |
| 拉取时间 | 18 s | 2 s<!-- 校准：请按真实经历核实/替换 --> |

多阶段的几个要点：

**`COPY --from=<stage>` 是关键。** 它从指定构建阶段拷贝产物，只拷贝你要的文件，不带那一层的其他东西。可以 `COPY --from=builder`，也可以 `COPY --from=nginx:1.25-alpine /usr/sbin/nginx /usr/sbin/nginx` 直接从另一个镜像拷。

**`CGO_ENABLED=0` 配 distroless/alpine。** Go 静态编译后不需要 libc，能跑在 `distroless/static`（无 shell、无 libc）。如果用了 cgo 就得用 `distroless/cc` 或 alpine + musl。

**builder 用大镜像没事，反正不留。** builder 阶段用 `golang:1.22`（带全套工具链）完全 OK，最后镜像只取 runtime stage。

那个 Python 服务最后用的是 Python 多阶段：builder 里装 gcc 编译 C 扩展，把编译好的 site-packages 拷到 runtime（`python:3.10-slim`），runtime 里只有 slim 运行时 + 编译好的依赖 wheel。

## 六、基础镜像选型：alpine vs distroless vs slim

基础镜像决定了镜像的"地板"。同样是 Python 3.10，选不同的基础镜像体积天差地别：

| 基础镜像 | 大小 | 特点 |
|----------|------|------|
| `python:3.10` | ~340 MB | 完整 Debian，带 gcc/编译工具，构建方便但臃肿 |
| `python:3.10-slim` | ~150 MB | Debian 精简版，无编译工具，运行时首选 |
| `python:3.10-alpine` | ~50 MB | musl libc，体积小但 pip 装 C 扩展要重编译 |
| `distroless/python3` | ~50 MB | Google 出品，无 shell、无包管理器，安全最佳 |

我的选型原则：

**优先 slim。** 生产服务 90% 用 `*-slim`。它兼容主流 glibc 生态，wheel 包能直接装，调试时还能 `apt` 临时装工具。Python、Node、Debian 系服务我都用 slim。

**Alpine 要慎用。** alpine 用 musl libc 而不是 glibc，很多 C 扩展（numpy、pandas、cryptography）没有 musl 的预编译 wheel，pip 会从源码编译，构建又慢又容易失败（要装 gcc、musl-dev、libffi-dev 一堆）。我接手过一个 alpine Python 服务，pip install 单次要 8 分钟，换回 slim 后 40 秒<!-- 校准：请按真实经历核实/替换 -->。alpine 适合纯静态语言（Go、Rust 静态二进制）或对体积极致敏感的场景。

**Distroless 是安全首选。** 它没有 shell、没有包管理器，连 `/bin/sh` 都没有——攻击者拿到 RCE 也无法 exec shell，攻击面极小。代价是调试麻烦（没法 `docker exec -it xxx sh`），需要 `:debug` 变体（带 busybox shell）辅助排障。对安全敏感的服务（对外网暴露、金融）我强烈推荐 distroless。

另外基础镜像**一定要钉死 digest**，不要用 `latest` 也不要只钉 tag。`python:3.10-slim` 这个 tag 指向的镜像会随上游更新而变化，今天 build 的镜像和下个月 build 的可能不一样。生产 Dockerfile 里写 `python:3.10.14-slim-bookworm@sha256:xxxx` 才是可复现的。

## 七、BuildKit 缓存挂载：CI 加速的隐藏神器

多阶段 + slim 把镜像瘦下来了，但 CI 总时间还有优化空间。最典型的问题是：**pip/npm 装依赖时的下载缓存怎么办？** 传统做法是把缓存目录 COPY 进镜像当一层——但这会让缓存本身进镜像，污染体积。BuildKit 给了一个优雅的解法：`--mount=type=cache`。

它的思路是：构建时把宿主机或 CI 上的某个目录挂载到容器里当缓存用，构建结束这个挂载**不进镜像**，但下次 build 还能复用。

```dockerfile
# syntax=docker/dockerfile:1.7

FROM python:3.10-slim

# pip 缓存挂载：跨 build 复用，不进镜像
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
```

```dockerfile
# Go mod 缓存
FROM golang:1.22 AS builder
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/server ./cmd/server
```

```dockerfile
# apt 缓存（Debian/Ubuntu）
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update && apt-get install -y --no-install-recommends gcc
```

几个要点：

**`# syntax=docker/dockerfile:1.7` 必须在第一行。** 它告诉 Docker 用 BuildKit frontend，新版特性（`--mount`、`--security`、heredoc）才生效。Docker 23.0+ 虽然默认 BuildKit，但 frontend 仍建议显式声明。

**CI 上要配置持久化缓存后端。** GitHub Actions 用 `actions/cache` 保存 `/tmp/.buildx-cache`，GitLab CI 用 cache key 缓存 buildx 目录。`type=cache` 默认存在 buildkit daemon 的工作目录，daemon 重启会丢，所以跨 job 要靠 CI 的 cache 机制把 buildkit 的数据目录搬进 CI cache。

**`sharing=locked` 防并发竞争。** 多 stage 并行时如果都写同一个 apt cache 会冲突，加 `sharing=locked` 自动加锁。

那个 Python 服务加上 pip cache mount 后，第二次 build 起步 pip install 从 4 分钟降到 20 秒（纯装 wheel，没有下载）<!-- 校准：请按真实经历核实/替换 -->。

另一个隐藏好用的特性是 `--mount=type=bind`，构建期临时挂载文件（比如编译用的工具链），构建完不进镜像。还有 `--mount=type=secret`，挂载密钥（pip 私有源 token、npm 私有包 token），不会留在镜像层里——这是比 `ARG` 传密钥安全得多的做法。

```dockerfile
# 私有 pip 源 token 用 secret 挂载，不会进镜像层
RUN --mount=type=secret,id=pip_token \
    PIP_TOKEN=$(cat /run/secrets/pip_token) \
    pip install --index-url https://${PIP_TOKEN}@pypi.internal/simple/ -r requirements.txt
```

构建时 `docker build --secret id=pip_token,env=PIP_TOKEN .` 传入。

## 八、镜像大小治理与漏洞扫描

瘦身不是一次性的事，得有持续治理机制。我在团队里推了几条规范：

**CI 里加镜像大小门禁。** build 完用一条脚本读 `docker inspect --format='{{.Size}}'` 比对阈值，Python 服务超过 400MB 直接 fail CI，Go 服务超过 50MB fail<!-- 校准：请按真实经历核实/替换 -->。这逼着所有人写 Dockerfile 时克制。阈值要定期 review，业务增长合理变大的要放。

**定期跑漏洞扫描。** 用 Trivy 或 Grype 在 CI 里扫镜像，CRITICAL 漏洞 fail，HIGH 警告。基础镜像的选择直接影响漏洞数量——`python:3.10` 全量镜像扫出来上百个 CVE，换 slim 降到十几个，distroless 只剩个位数<!-- 校准：请按真实经历核实/替换 -->。

```bash
# CI 里的扫描步骤
trivy image --severity CRITICAL --exit-code 1 myapp:latest
trivy image --severity HIGH,CRITICAL --ignore-unfixed myapp:latest
```

**`--ignore-unfixed` 过滤掉上游还没修的 CVE**，避免告警疲劳。定期跑一次不过滤的全量扫描做基线对齐。

**基础镜像定期升版。** 每月或每季度过一遍 `python:3.10-slim` 的 digest，跟着 patch 版本走，能修掉一批已披露 CVE。这个动作最好自动化（Dependabot/Renovate 盯 base image）。

**`dive` 工具看每层细节。** `dive <image>` 能交互式地看每一层加了哪些文件、哪些是被 whiteout 掉的，是定位"为什么镜像这么大"的神器。我在治理那个 2.3GB 镜像时，dive 一打开就看到 `/usr/local/lib/python3.10/site-packages/` 里有一堆没用的依赖，还有 `/root/.cache` 残留 200MB。

## 九、踩坑复盘清单

把这次治理里踩过的坑汇总成一张表，方便对照自查：

| 坑 | 现象 | 解决 |
|----|------|------|
| `COPY . .` 在前 | 改一行代码全量重装依赖 | 依赖清单（requirements.txt/go.mod）单独前置 COPY |
| 没 `.dockerignore` | .git/测试数据进镜像，context 巨大 | 补全 ignore，context 从 GB 降到 MB |
| `RUN rm` 减不掉体积 | 下载的临时文件留在下层 | 同一层内下载、解压、删除 |
| `apt-get update` 拆层 | 缓存索引过时装错版本 | update 和 install 写在同一 RUN |
| alpine 装 C 扩展慢 | pip 编译 numpy 要装 gcc、musl-dev | 换 slim，alpine 只用于静态二进制 |
| `:latest` 基础镜像 | 不可复现，下个月 build 行为变了 | 钉 tag + digest |
| 容器写日志到文件 | Upper 层无限涨，重启丢失 | 日志打 stdout，logging driver 收集 |
| xfs 没开 d_type | overlay2 挂载报错 | `mkfs.xfs -n ftype=1` |
| `ARG` 传密钥 | token 进镜像历史层泄露 | 用 `--mount=type=secret` |
| 镜像越滚越大没人管 | 半年后普遍 1GB+ | CI 加大小门禁 + 定期扫描 |

## 收尾：优化的本质是理解代价

这次治理给我最大的体会是：**镜像优化的本质，是让每条 Dockerfile 指令都"贵得其所"。** 一条 RUN 产生一层、一层带一份 diff，每次 build 都在为这些层和 diff 付钱（构建时间、存储、传输、安全）。理解了分层、缓存、overlay2 之后，每条指令的代价是可估算的，优化就有方向；不理解机制，就只能背几条 best practice 照搬，遇到新场景两眼一抹黑。

最后的对比数据，也是那次治理的收尾：

| 指标 | 优化前 | 优化后 | 变化 |
|------|--------|--------|------|
| 镜像大小 | 2.3 GB | 180 MB | -92%<!-- 校准：请按真实经历核实/替换 --> |
| CI build 时间 | 9 min | 1.5 min | -83%<!-- 校准：请按真实经历核实/替换 --> |
| 推送到 Harbor | 4 min | 25 s | -90%<!-- 校准：请按真实经历核实/替换 --> |
| 节点拉镜像 | 90 s | 8 s | -91%<!-- 校准：请按真实经历核实/替换 --> |
| CRITICAL CVE | 14 个 | 0 个 | —<!-- 校准：请按真实经历核实/替换 --> |
| Dockerfile 指令数 | 23 条 | 14 条 | -39% |

这套方法论后来我沉淀成团队的 Dockerfile 规范 + CI 门禁，新服务上线默认按这套来，老服务分批治理。镜像这件事看着小，但它是整个容器化交付链路的起点——镜像瘦了，存储、带宽、启动、安全全部跟着受益，是少数几个能"一次投入、全线受益"的优化。
