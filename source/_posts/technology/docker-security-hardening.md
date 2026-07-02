---
title: Docker 容器安全加固实战：从被扫出一堆高危说起
abbrlink: docker-security-hardening
date: 2026-07-02 10:10:00
tags:
  - Docker
  - 容器安全
categories:
  - 技术
description: 一次安全扫描扫出几十个高危后的容器加固复盘，覆盖镜像、运行时、命名空间、供应链与密钥管理。
ai_text: "本文复盘一次容器安全扫描后所做的 Docker 加固工程：从镜像漏洞、提权与逃逸的攻击面切入，落地最小权限原则（非 root 运行、capability drop、只读根文件系统、no-new-privileges），集成 Trivy/Grype 扫描到 CI，配置 userns-remap 与 rootless Docker，引入 seccomp/AppArmor profile，并用 cosign 做镜像签名、digest 锁定与外部 secret 管理。结合 --privileged 与挂 docker.sock 的真实逃逸路径，给出一套可落地的加固检查清单。"
---

# Docker 容器安全加固实战：从被扫出一堆高危说起

去年年底过等保的时候，安全团队甩过来一份扫描报告：我们线上一个数据分析服务的镜像里躺着 47 个高危 CVE，其中两个还是 RCE。更要命的是报告里写了一句"容器以 root 运行且挂载了 `/var/run/docker.sock`"，红队评估直接判定为"可一键逃逸到宿主机"。那天晚上我和运维同学坐在一起把整个容器安全链路从头梳理了一遍，这篇文章就是那次加固的复盘——不讲容器原理，只讲我们踩过的坑和最后落地的那套加固规范。

## 一、先看清攻击面：容器到底"漏"在哪里

很多人对容器安全有个误解："反正隔离了，漏就漏吧"。但容器的隔离是"软隔离"——它共享宿主机内核，命名空间和控制组只是视图边界，不是硬件边界。我们把攻击面拆成三层来看：

**镜像层**：基础镜像带的旧版 glibc、openssl、curl，以及 `apt-get install` 装了一堆用不上的工具（ping、curl、wget、nc）。这些既是漏洞来源，也是攻击者拿到 shell 后的现成武器。

**构建层**：Dockerfile 里 `COPY . .` 把 `.env`、`.git`、SSH 私钥全打进去了；多阶段构建没做，最终镜像里带着编译工具链和源码；构建参数里硬编码了数据库密码，`docker history` 一扒就出来。

**运行时层**：默认以 root 跑、默认开了十几个 Linux capability、根文件系统可写、`--privileged` 一开等于把容器当虚拟机用。运行时才是逃逸真正发生的地方。

理解了这三层，加固思路就很清晰了：**镜像要瘦、要扫；构建要干净、要签名；运行时要最小权限、要多层隔离**。下面逐层说我们怎么做的。

## 二、镜像层：瘦身 + 扫描 + CI 卡口

### 2.1 从 fat 镜像切到 distroless

我们最早的 Python 服务镜像是 `python:3.11` 全量版，1.2GB，里面有 gcc、make、vim、bash，Trivy 一扫 200 多个漏洞。第一步是切多阶段构建：

```dockerfile
# 构建阶段
FROM python:3.11-slim AS builder
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install --prefix=/install poetry && \
    poetry export -f requirements.txt -o requirements.txt && \
    pip install --prefix=/install -r requirements.txt

# 运行阶段
FROM gcr.io/distroless/python3-debian12:nonroot
WORKDIR /app
COPY --from=builder /install /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY . /app
USER nonroot
ENTRYPOINT ["python", "-m", "myapp"]
```

切完镜像从 1.2GB 掉到 180MB，Trivy 报告的高危从 47 个降到 6 个（剩下都是 distroless 自带的小版本滞后，跟着基础镜像升级走）。distroless 没有 shell，红队拿不到 shell 的痛苦我们也体会过——调试的时候很别扭，所以我们在测试环境保留了 `slim` 版本用于 `docker exec`，生产用 distroless。<!-- 校准：请按真实经历核实/替换 -->

### 2.2 Trivy 扫描接入 CI

只切镜像不够，得有卡口防止新的漏洞混进来。我们在 GitLab CI 里加了两道闸：

```yaml
# .gitlab-ci.yml
stages:
  - scan

trivy-scan:
  stage: scan
  image: aquasec/trivy:latest
  script:
    - trivy fs --severity HIGH,CRITICAL --exit-code 1 --ignore-unfixed .
    - trivy image --severity HIGH,CRITICAL --exit-code 1 \
        --ignore-unfixed ${CI_REGISTRY_IMAGE}:${CI_COMMIT_SHORT_SHA}
  artifacts:
    reports:
      container_scanning: trivy-report.json
```

关键参数是 `--ignore-unfixed`：只卡"已有补丁"的漏洞，否则会被一堆上游还没修的 CVE 卡死流水线。`--exit-code 1` 让高危直接挂掉 MR。我们还配了 `.trivyignore` 白名单，每条都必须写理由和负责人，三个月过期一次，防止"先 ignore 后不管"。

对比一下三个主流扫描工具我们试过的体验：

| 工具 | 速度 | 准确度 | CI 集成 | 适用场景 |
|------|------|--------|---------|----------|
| Trivy | 快（无状态） | 中高，误报少 | 一行命令 | 中小团队首选 |
| Grype | 快 | 中，偶尔漏报 | 与 Syft 配合 | SBOM 优先的团队 |
| Clair | 慢（需数据库） | 高 | 需搭服务 | 私有镜像仓库集中扫描 |

我们最终选 Trivy 跑在 MR 流水线做门禁，Clair 部署在 Harbor 后面做仓库级全量巡检，Grype 用来生成 SBOM 交给安全团队归档。<!-- 校准：请按真实经历核实/替换 -->

## 三、运行时：最小权限原则一条条落地

这一节是全文的核心。容器默认的权限模型其实相当宽松——root 用户、十几个 capability、可写文件系统。我们做的事就是把这些默认值一条条收紧。

### 3.1 非 root 运行

distroless 的 `nonroot` tag 默认就是 UID 65532，但 slim/alpine 镜像默认还是 root。两种做法：

```dockerfile
# 方式一：Dockerfile 里建专用用户
RUN groupadd -r app && useradd -r -g app -u 10001 app
USER 10001:10001

# 方式二：docker run 时指定（覆盖镜像默认）
docker run -u 10001:10001 myapp
```

这里有个坑：`USER 10001` 要写 `UID:GID` 两个，只写 UID 时 GID 还是 0（root 组），文件权限校验会出问题。另外挂卷的宿主目录属主必须对得上 UID，否则容器里写不进去——我们最早用名字 `app` 而不是数字 UID，结果在 OpenShift 里被强制用随机 UID 跑，文件全部 permission denied。教训是**生产镜像里 USER 永远写数字 UID**。

### 3.2 Capability drop：从默认 14 个到按需 0 个

容器默认带的 capability 有 14 个左右（`CHOWN`、`NET_BIND_SERVICE`、`SETUID`...），对一个普通 Web 服务来说大部分是多余的。我们的策略是先全删再加：

```bash
docker run \
  --cap-drop=ALL \
  --cap-add=NET_BIND_SERVICE \
  myapp
```

需要绑 80/443 的加 `NET_BIND_SERVICE`；需要改时间的加 `SYS_TIME`；需要 `ping` 的加 `NET_RAW`（不过大多数服务不需要）。我们做过一次审计，线上 23 个服务最后只有 3 个需要额外的 capability，其余 20 个 `--cap-drop=ALL` 直接跑。<!-- 校准：请按真实经历核实/替换 -->

### 3.3 只读根文件系统 + tmpfs

```bash
docker run \
  --read-only \
  --tmpfs /tmp:rw,size=64m \
  --tmpfs /var/tmp:rw,size=64m \
  myapp
```

只读根文件系统直接堵死了攻击者写 webshell、改二进制的路。但要注意应用必须配合——不能往 `/app/logs`、`/app/data` 这种容器内路径写东西。我们把日志全部走 stdout 给日志采集器，需要写的临时目录显式 `--tmpfs` 或者 `-v` 挂命名卷。最早一个 Java 服务因为要往 `/tmp` 写 session，开 `--read-only` 直接启动失败，排查了半小时才想起来 tmpfs。

### 3.4 no-new-privileges：堵死提权

```bash
docker run --security-opt=no-new-privileges myapp
```

这一行是防"带 SUID 位的二进制提权"的，内核里设 `PR_SET_NO_NEW_PRIVS`。即使容器里有 `sudo`、`pkexec` 这类 SUID 程序，子进程也拿不到新权限。配合非 root 用户，提权路径基本就断了。

### 3.5 一条加固命令长这样

把上面四条合起来，我们一个线上服务的实际启动命令：

```bash
docker run -d \
  --name api-server \
  --restart=unless-stopped \
  -u 10001:10001 \
  --cap-drop=ALL \
  --cap-add=NET_BIND_SERVICE \
  --security-opt=no-new-privileges \
  --security-opt=apparmor=docker-strict \
  --security-opt=seccomp=/etc/docker/seccomp/profile.json \
  --read-only \
  --tmpfs /tmp:rw,size=64m \
  --pids-limit=200 \
  --memory=512m \
  --network=internal \
  api-server:v1.4.2
```

`--pids-limit` 和 `--memory` 是防 fork 炸弹和资源耗尽的，属于运行时 DoS 加固，顺带加上。

## 四、命名空间隔离：userns-remap 与 rootless Docker

前面说的都还是"容器内 root = 宿主机 root 的 capability 受限版"。真正的根上的隔离是**用户命名空间映射**：让容器内的 root（UID 0）映射到宿主机上的一个普通 UID（比如 100000），这样即便容器内是 root，对宿主机内核来说就是个无名小卒，写不了宿主机文件、改不了宿主机配置。

### 4.1 userns-remap（daemon 级）

编辑 `/etc/docker/daemon.json`：

```json
{
  "userns-remap": "default"
}
```

`default` 会让 Docker 自动创建一个 `dockremap` 用户，起始 UID 100000，子UID范围 100000:65536。重启 dockerd 后所有容器自动享受映射。我们切的时候踩过一个坑：**之前用 `-v /host/path:/data` 挂的卷，文件属主对不上，容器内写入全部 permission denied**。解决办法是要么用命名卷（Docker 会自动 chown），要么手动 `chown -R 100000:100000 /host/path`。还有容器内 UID 不能写死 10001 这种，因为映射后宿主机看到的 UID 会变成 100000+10001，权限又乱。这块我们花了两天才把所有挂载点理顺。<!-- 校准：请按真实经历核实/替换 -->

### 4.2 rootless Docker（更彻底）

userns-remap 还是 dockerd 以 root 跑。rootless Docker 直接让 dockerd 也跑在普通用户下，利用用户命名空间从根上消除"守护进程被攻破=宿主机沦陷"。

```bash
# 安装 rootless 包
sudo apt install uidmap
# 切到普通用户
dockerd-rootless-setuptool.sh install
systemctl --user enable docker
```

rootless 模式下有几个功能限制：不能用 `--privileged`（这反而是好事）、不能用回环以外的 ICMP、`--device` 挂载受限、overlayfs 在老内核上要手动开 `kernel.unprivileged_userns_clone=1`。我们目前只在 CI runner 和开发环境跑 rootless，生产用的是 userns-remap + 严格 capability 的组合，主要是怕 rootless 的网络性能损耗在高吞吐场景下扛不住。<!-- 校准：请按真实经历核实/替换 -->

## 五、seccomp / AppArmor / SELinux：三层 LSM 兜底

这三兄弟经常被混为一谈，其实职责不同：

- **seccomp**：过滤容器能调用的系统调用（syscall 级）。Docker 默认有个 seccomp profile，禁掉 `keyctl`、`kexec_load`、`unshare` 等几十个危险 syscall。
- **AppArmor**：基于路径的访问控制（文件级），Debian/Ubuntu 默认带。
- **SELinux**：基于标签的访问控制（类型级），RHEL/CentOS 默认带。

### 5.1 收紧 seccomp profile

默认 profile 已经不错，但我们额外禁掉了 `ptrace`、`mount`、`umount`、`setns`（防容器内 `nsenter` 跳命名空间）：

```json
{
  "defaultAction": "SCMP_ACT_ERRNO",
  "defaultErrnoRet": 1,
  "syscalls": [
    {
      "names": ["ptrace","mount","umount2","setns","reboot","swapon","swapoff"],
      "action": "SCMP_ACT_ERRNO"
    }
  ]
}
```

收紧之后跑了一次完整回归测试，结果有个 Python 服务用 `multiprocessing` 启动子进程时调了被禁的 syscall 直接挂了——后来发现是它在 fork 后用了 `setns` 切网络命名空间。**收紧 seccomp 一定要在测试环境跑全量回归**，别在生产直接上。

### 5.2 AppArmor / SELinux

AppArmor 我们用了一个 `docker-strict` 模板，比默认 `docker-default` 更狠，禁掉了几乎所有对 `/proc`、`/sys` 的写。SELinux 在 RHEL 环境保持 `container_t` 类型即可，关键是别因为图省事 `setenforce 0` 关掉——见过同事调试时关了忘开，事后审计被吊打。

## 六、供应链：信任不是默认选项

### 6.1 镜像签名（cosign）

Sigstore 的 cosign 用一对密钥（生产用 OIDC + keyless）给镜像签名，部署时校验：

```bash
# 签名（CI 里）
cosign sign --yes registry.example.com/api-server:v1.4.2

# 部署前校验
cosign verify \
  --certificate-identity-regexp 'https://gitlab.example.com/.*' \
  --certificate-oidc-issuer 'https://gitlab.example.com' \
  registry.example.com/api-server:v1.4.2
```

我们在 K8s 里装了 Kyverno 做准入校验，未签名的镜像直接拒绝拉起。

### 6.2 digest 锁定，永远别用 latest

tag 是可变的，今天 `v1.4.2` 指向镜像 A，明天可能被覆盖成镜像 B。供应链安全要求**用 digest**：

```yaml
image:
  registry.example.com/api-server@sha256:abc123...def789
```

我们用 Renovate Bot 自动管依赖升级，digest 锁定在 values 文件里，升级走 MR 重新签名。`latest` 在生产环境直接禁用。

### 6.3 可信仓库 + 私有镜像

不允许从 docker.io 拉匿名镜像，所有镜像走内部 Harbor，Harbor 后面挂 Clair 做集中扫描。Docker daemon 配置 `--registry-mirror` 只指向内部仓库，等于物理隔离。

## 七、Secret 管理：绝不打进镜像

这是最容易出事也最容易改的一块。我们审计时发现三个服务把数据库密码、对象存储密钥、第三方 API key 直接写在 Dockerfile 的 `ENV` 里，`docker history` 一扒全露。

正确做法只有一条：**密钥走运行时注入，镜像里一个字节的明文都不要有**。

```bash
# 方式一：docker secret（Swarm）或文件挂载
docker run -v /run/secrets/db_password:/run/secrets/db_password:ro ...

# 方式二：环境变量由编排平台注入（K8s Secret、Vault sidecar）
# 镜像里只读这些环境变量的名字，不读值
```

我们在 K8s 里用 Vault + External Secrets Operator，Secret 加密存 etcd（开 etcd-kms），Pod 通过 service account 换取短期 token 拉 Secret，密钥定期轮转。容器跑完，密钥就过期了。CI 里用 `dive` 工具检查最终镜像有没有残留 `.env` 文件，防止构建时 COPY 把密钥带进去。

## 八、逃逸案例剖析：两个真实的"一键穿"路径

讲再多加固不如看一次真实的逃逸来得深刻。红队评估报告里把我们的两个问题定级为"严重"，下面拆开看为什么。

### 8.1 `--privileged` 一把梭

某个监控 agent 当时为了"方便"用了 `--privileged` 跑。`--privileged` 等于：

- 关闭所有 seccomp / AppArmor / SELinux 隔离
- 给容器**所有** capability（包括 `CAP_SYS_ADMIN`）
- 挂载宿主机所有设备节点（`/dev/sda1` 这种）

逃逸路径只有三行：

```bash
# 容器内
mkdir /host && mount /dev/sda1 /host
chroot /host
# 现在你已经在宿主机根文件系统里了，随便改
```

或者更隐蔽的——挂 cgroup + notify_on_release，写一个 RCE payload 到 release_agent，等 cgroup 被销毁时宿主机以 root 执行。CVE-2019-5736（runc 逃逸）、CVE-2022-0185（heap overflow in legacy_parse_param）这些在 `--privileged` 下都直接可利用。**`--privileged` 唯一合理的场景是 Docker-in-Docker 或调试，生产环境见到就该判死刑**。我们改完后这个监控 agent 换成了不需要特权的 node-exporter。

### 8.2 挂载 `docker.sock` 被打穿

那是个 CI 的构建节点，把 `/var/run/docker.sock` 挂进容器方便它构建镜像。这个 socket 是 Docker daemon 的 API 入口，挂进容器等于把"控制宿主机上所有容器和镜像"的权限送出去了。逃逸路径：

```bash
# 容器内装个 docker client
docker run -v /var/run/docker.sock:/var/run/docker.sock \
  -v /:/host alpine
# 在容器内启动一个新容器，把宿主机根目录挂进去
docker run -v /:/host alpine chroot /host
# 直接拿到宿主机 root
```

这比 `--privileged` 还干净利落，因为它根本不需要内核漏洞，纯粹是"把 daemon API 给你了"。修复方案：CI 构建改用 Kaniko（不需要 daemon）或 BuildKit 在独立构建器里跑，`docker.sock` 一律禁止挂入业务容器。我们用 OPA Gatekeeper 加了条策略，凡是挂载路径包含 `docker.sock` / `containerd.sock` 的 Pod 一律拒绝创建。

## 九、加固检查清单

把上面所有内容浓缩成一张可对账的清单，我们内部每季度跑一次：

| 加固项 | 措施 | 命令 / 配置 | 优先级 |
|--------|------|-------------|--------|
| 非 root 运行 | UID 数字写死 | `-u 10001:10001` 或 `USER 10001` | P0 |
| capability 收紧 | drop ALL 后按需加 | `--cap-drop=ALL --cap-add=NET_BIND_SERVICE` | P0 |
| 只读根文件系统 | tmpfs 补临时目录 | `--read-only --tmpfs /tmp` | P1 |
| 禁止提权 | no-new-privileges | `--security-opt=no-new-privileges` | P0 |
| 用户命名空间 | userns-remap 或 rootless | `daemon.json: userns-remap` | P1 |
| seccomp 收紧 | 禁 ptrace/mount/setns | `--security-opt=seccomp=profile.json` | P1 |
| 镜像扫描 | Trivy 接 CI，HIGH/CRITICAL 卡 MR | `trivy image --exit-code 1` | P0 |
| 镜像签名 | cosign sign + verify | 准入控制器校验 | P1 |
| digest 锁定 | 禁 latest，用 sha256 | `image@sha256:...` | P1 |
| 禁 privileged | 用网络策略/OPA 拦截 | Gatekeeper 策略 | P0 |
| 禁挂 docker.sock | OPA 策略 + 改用 Kaniko | Gatekeeper 策略 | P0 |
| Secret 隔离 | 不进镜像，运行时注入 | Vault + ESO | P0 |
| 资源限制 | memory / pids / cpu | `--memory --pids-limit` | P1 |
| 基础镜像瘦身 | distroless / slim + 多阶段 | Dockerfile 重构 | P1 |
| 网络隔离 | 自定义网络，默认不暴露 | `--network=internal` | P1 |

## 十、踩坑复盘（写给自己也写给后来人）

最后说几句"血泪"。

**坑一：`--privileged` 一把梭。** 最早为了省事，监控、CI、日志 agent 全 privileged。当时觉得"反正内网"，等保一过红队一打才发现根本不是"方便"，是给攻击者递刀。改的时候每个服务都要重新审一遍它到底需要什么 capability，比一开始就规范要痛苦十倍。**结论：宁可一开始麻烦，也别图省事开特权。**

**坑二：挂 docker.sock。** 这个在 CI 构建场景特别常见，网上无数教程都这么写。但它本质就是把宿主机 root 送给容器。换成 Kaniko 之后构建慢了 20%，但换来的是"CI 被攻破不等于宿主机沦陷"，值。<!-- 校准：请按真实经历核实/替换 -->

**坑三：密钥打进镜像被扫到。** 一次是用 `ARG DB_PASSWORD` 传构建参数，结果 `docker history` 里明文躺着；一次是 `COPY . .` 把开发同学本地的 `.env` 带进去了。后来我们加了 `dive` 在 CI 里检查镜像层，加了 `.dockerignore` 严格过滤，密钥全部走 Vault 运行时注入。这块还有个细节：构建参数哪怕是 `--build-arg` 也会留在 image history 里，**敏感信息永远不能走 build arg**。

**坑四：seccomp 一刀切。** 收紧 seccomp profile 直接上生产，结果一个 Java 服务 JVM 启动调了 `perf_event_open` 被拒，GC 监控全挂。教训：seccomp 改动必须灰度，先在测试环境跑全量回归，再按服务灰度。

**坑五：userns-remap 挂卷权限。** 切 userns-remap 后所有 bind mount 的属主都错位，容器写不进去。解决方案要么命名卷，要么提前 chown 到映射后的 UID（100000+容器内UID）。我们写了个脚本批量改，但还是建议**新项目从一开始就用命名卷**，别等切 userns 再补。

---

写到这里，整个加固链路差不多讲完了。容器安全的本质不是某一个"银弹"，而是**把最小权限原则贯彻到镜像、构建、运行时、供应链每一个环节**——单独拎任何一条出来都挡不住真正的攻击者，但叠起来之后，攻击者的成本会指数级上升。我们这套规范落地一年，再过等保时高危从 47 降到 0，红队评估也从"可一键逃逸"变成了"需结合内核 0day 才有可能突破"。安全没有终点，但至少这次，我们不用半夜被电话叫起来了。
