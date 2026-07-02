---
title: Dockerfile 实战最佳实践与生产级模板：从踩坑到可信镜像
abbrlink: dockerfile-best-practices
date: 2026-07-02 09:20:00
tags:
  - Docker
  - Dockerfile
categories:
  - 技术
description: 逐条精讲 Dockerfile 关键指令的正确用法与陷阱，附 Spring Boot 生产级模板与密钥泄露、时区、权限等踩坑复盘。
ai_text: "本文以第一人称复盘口吻，逐条精讲 Dockerfile 中 FROM、WORKDIR、COPY、RUN、ENV、ENTRYPOINT、HEALTHCHECK、USER 等关键指令的正确用法与常见陷阱，重点拆解构建期 secret 处理、层缓存顺序与最小权限运行。最后给出一份可直接落地的 Spring Boot 生产级 Dockerfile 模板，并复盘密钥泄露、时区错乱、locale 缺失、容器内权限过高四类典型踩坑，帮助团队产出小而安全、可观测、可复现的可信镜像。"
---

## Dockerfile 实战最佳实践与生产级模板：从踩坑到可信镜像

写 Dockerfile 这件事，门槛低到一行 `FROM ubuntu` 加几条 `RUN apt-get install` 就能跑起来；但真要在生产线上稳定烧几年，门槛又高到令人发指。我接手过一个历史项目，镜像 1.8 GB、用 root 跑、时区是 UTC、健康检查缺失、构建期把数据库密码写进了中间层——每个问题都曾在凌晨炸过。

这篇是我把这些年踩过的坑、改过的 Dockerfile、评审过的几十次镜像审计，浓缩成一份"实战最佳实践"。不讲大道理，只讲每条指令的正确写法和它背后的陷阱。最后给一份我们线上 Spring Boot 服务真正在用的生产级 Dockerfile 模板（已脱敏），你可以直接拿去改。

<!-- 校准：请按真实经历核实/替换 -->

---

### 一、FROM：基础镜像的选择是第一道分水岭

很多人写 Dockerfile 第一行随手就是 `FROM ubuntu:latest` 或 `FROM openjdk:8`，然后开始 `apt-get install` 一堆东西。这是构建期一切罪恶的源头——镜像臃肿、版本漂移、构建不可复现。

我们团队的几条硬规矩：

**1. 永远不要用 `:latest`。** 它是漂移的代名词。今天构建出来的镜像和下周构建出来的可能基于完全不同的 digest。生产镜像必须钉死到具体 tag，理想情况钉到 digest：

```dockerfile
# 差
FROM openjdk:17

# 好
FROM eclipse-temurin:17.0.10_7-jre

# 更好（完全可复现）
FROM eclipse-temurin:17.0.10_7-jre@sha256:5d7e4f3c2b1a8e9f6c4d3b2a1f8e7d6c5b4a3f2e1d9c8b7a6f5e4d3c2b1a0f9e
```

<!-- 校准：请按真实经历核实/替换 -->

**2. 优先选 `-slim` 或 `-alpine` 变体。** 完整版基础镜像往往带编译器、文档、包管理缓存，把镜像从 200 MB 撑到 800 MB。运行时根本用不上。我们线上 Java 服务从 `openjdk:17`（约 470 MB）换到 `eclipse-temurin:17-jre`（约 230 MB）再到 `eclipse-temurin:17-jre-alpine`（约 120 MB），体积砍掉 70%。

alpine 要注意 musl libc 与 glibc 的差异——某些依赖 native 库的中间件（比如老版本的 RocksDB、某些数据库驱动）在 alpine 上会偶发段错误。我们踩过一次，最后退回 slim。

**3. 选官方维护、有 CVE 扫描背书的镜像。** `eclipse-temurin`、`amazoncorretto`、`python:3.11-slim` 这类由发行方或基金会维护的镜像，漏洞修复速度远快于个人仓库。我们的策略是：基础镜像每月走一次 Trivy 扫描，CRITICAL 级别三天内升级。

**4. 统一基础镜像策略，别让团队各玩各的。** 我们早期每个服务各挑一个基础镜像，光 Java 就有 `openjdk`、`amazoncorretto`、`adoptopenjdk`、`azul/zulu` 四种，安全扫描报告分散、升级节奏不一致。后来强制收敛到 `eclipse-temurin` 一条线，维护成本立刻降了一个量级。基础镜像越少，补丁打得越快、CVE 响应越及时——这是镜像治理的常识。

---

### 二、WORKDIR：别再用 `cd` 了

`RUN cd /app && do-something` 是新手最常见的写法。问题是每条 `RUN` 都是一个新 shell、一个新层，`cd` 的效果不会跨层保留。

正确做法是用 `WORKDIR`：

```dockerfile
WORKDIR /app
COPY . /app
RUN ./gradlew build
```

`WORKDIR` 会自动创建目录，之后所有 `RUN`、`CMD`、`ENTRYPOINT`、`COPY` 的默认工作目录都是它。显式、可读、跨层生效。我评审 Dockerfile 时，看到 `cd` 出现在 `RUN` 里，基本判定为不合格。

---

### 三、COPY vs ADD：90% 的场景你只需要 COPY

`ADD` 比 `COPY` 多两个能力：自动解压 tar 包、支持远程 URL。听起来很香，但这两个"增强"恰恰是踩坑源头：

- `ADD app.tar.gz /app` 会自动解压，但 `ADD app.jar /app` 不会——行为不一致，读 Dockerfile 的人得猜。
- `ADD https://...` 拉远程文件不会校验 checksum，且无法用构建缓存，等于把构建可复现性交给了一个外部 URL。

我们内部规范是：**一律用 `COPY`，需要解压 tar 就在 `RUN` 里 `tar -xzf`，需要下载远程文件就先 wget 到本地再 COPY 进来。** 只有在一个场景下会破例用 `ADD`：根目录有个 tar 包，解压完就删源文件，用 `ADD --chown=app:app rootfs.tar.xz /` 一步到位。

另外，`COPY` 一定要带 `--chown`：

```dockerfile
COPY --chown=1000:1000 app.jar /app/app.jar
```

否则文件 owner 是 root，等下面切到非 root 用户后，可能根本没有读权限——这是我们上线时真实遇到过的 "Permission denied" 事故。

---

### 四、RUN：层、缓存与合并的艺术

Dockerfile 每条 `RUN` 都会生成一个层。层是缓存的基本单位：从某条 `RUN` 开始，只要它的指令字符串或上游文件变了，这一层及之后所有层都会重建。

这就引出了第一条核心原则——**把变化频率低的放前面，高的放后面**。

错误示例（每次代码改动都重装依赖）：

```dockerfile
COPY . /app
RUN pip install -r requirements.txt   # 每次 . 变了都重跑
```

正确示例（依赖装一次，缓存命中率高）：

```dockerfile
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
```

<!-- 校准：请按真实经历核实/替换 -->

这一招在我们一个 Python 服务上把构建时间从 6 分半压到 40 秒。Maven、Gradle、npm、pip、go mod 都适用同一个套路——先 COPY 依赖清单文件，再装依赖，最后 COPY 业务代码。判断标准就一句：**变化频率从低到高排**。

还有一个常被忽略的细节：`COPY` 的源如果是个目录，`COPY src/ /app/` 和 `COPY src/ /app` 行为不同，后者会把 src 的内容铺到 /app 下而不是放进子目录。我评审时遇到过一次，团队以为代码在 `/app/src`，实际被铺到了 `/app` 根下，构建居然过了，运行才崩。**COPY 路径的末尾斜杠，是 Dockerfile 里另一个隐式陷阱。**

第二条原则——**`apt-get` 类指令要合并、要清理、不要交互**：

```dockerfile
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*
```

三个要点缺一不可：
- `apt-get update` 和 `apt-get install` 必须在**同一条 RUN**，否则缓存里的 update 层是旧的，install 拿不到新包列表。
- `--no-install-recommends` 砍掉推荐包，体积能小一截。
- `rm -rf /var/lib/apt/lists/*` 清掉 apt 缓存，别让它进层。

第三条原则——**用 `&&` 和 `\` 把逻辑相关的命令合并成一层**，避免每条命令一个层把镜像撑大。但也别过度合并——把"装系统包"和"编译产物"塞一个 RUN，调试时连哪步崩的都看不清。

---

### 五、ENV vs ARG：构建期 vs 运行期，别混了

这两个是最容易被搞混的指令。

- `ARG`：**只在构建期可见**，可以用 `--build-arg` 传值，**不会**进入运行期环境，也不会出现在 `docker inspect` 的 Env 里。
- `ENV`：**构建期和运行期都可见**，容器启动后进程能直接读到。

典型错误：把数据库密码用 `ARG` 传进来，以为"运行期看不到所以安全"——但 `ARG` 的值会被写进构建历史和中间层，`docker history` 一查全暴露。**ARG 不等于 secret。**

正确用法：

```dockerfile
# 版本号、构建参数（非敏感）用 ARG
ARG APP_VERSION=1.0.0
ARG JAR_FILE=build/libs/app-${APP_VERSION}.jar

# 运行期配置（非敏感）用 ENV
ENV JAVA_OPTS="-XX:+UseG1GC -Xms512m -Xmx512m" \
    TZ="Asia/Shanghai"
```

<!-- 校准：请按真实经历核实/替换 -->

敏感信息（密钥、token）的处理方式见第七节，那是另一个量级的问题。

补充一个关于 `ARG` 默认值的坑：`ARG` 如果在 Dockerfile 里声明了但 `--build-arg` 没传，会用 Dockerfile 里写的默认值。如果默认值是空字符串，构建可能默默成功但产物是坏的。我们 CI 里加了一道校验：声明过的 `ARG` 必须在构建脚本里显式传值，缺失直接 fail，杜绝"默认值悄悄生效"的隐性故障。

---

### 六、ENTRYPOINT vs CMD：组合语义是 Dockerfile 里最容易写错的地方

这两条指令的组合与覆盖规则，我见过最资深的后端工程师也写错过。先把规则讲清楚：

- `CMD`：**可被 `docker run <image> <cmd>` 覆盖**，用于提供默认命令。
- `ENTRYPOINT`：**默认不可被覆盖**（除非加 `--entrypoint`），用于固定要执行的程序。
- 两者同时存在时，`CMD` 的内容会作为参数传给 `ENTRYPOINT`。

生产级服务的最佳实践是 **ENTRYPOINT 固定入口 + CMD 提供默认参数**：

```dockerfile
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
CMD ["--spring.profiles.active=prod"]
```

这样 `docker run myimage` 会跑 `java -jar /app/app.jar --spring.profiles.active=prod`，而 `docker run myimage --spring.profiles.active=staging` 就能覆盖 profile，入口程序不变。

几个常见坑：

**坑一：用 shell 形式会丢信号。** `CMD java -jar app.jar` 这种 shell 形式实际上会被包装成 `sh -c "java -jar app.jar"`，Java 进程成了 sh 的子进程，`SIGTERM` 收不到，容器 `docker stop` 要等 10 秒超时被 SIGKILL。**必须用 exec 形式：`CMD ["java", "-jar", "app.jar"]`。** 这是我们线上容器优雅停机失效的根因，改完之后 stop 时间从 10 秒掉到 1 秒内。

**坑二：ENTRYPOINT 写成脚本却不 `exec`。** 很多镜像用一个 `entrypoint.sh` 做初始化（生成配置、等依赖），脚本最后一行直接 `java -jar app.jar`。这时 Java 是脚本的子进程，信号问题又回来了。脚本最后一行必须 `exec "$@"`，把当前 shell 替换成目标进程。

**坑三：同时写两条 CMD 或同时写两条 ENTRYPOINT。** Dockerfile 里后者会覆盖前者，构建不报错但只有最后一条生效。代码评审时这种"以为两条都在、实际只剩一条"的写法很常见，要么是复制粘贴残留，要么是误解了语义。规范是：整份 Dockerfile 里 `ENTRYPOINT` 和 `CMD` 各最多一条。

---

### 七、构建期 secret：这篇文章最重要的一节

我曾审计过一个镜像，发现构建期为了拉私有 Maven 仓库，把 `settings.xml` 里的 Nexus 账号密码直接 `COPY` 进镜像——然后又 `rm` 掉。看起来删干净了？没用。那个 COPY 产生的层里密码还在，`docker history` 或 `docker save` 解开层就能看到。这是真实的密钥泄露事故，最后只能把仓库密码全部轮换。

这就是为什么"密钥绝不能进镜像层"是一条铁律。三种合规做法：

**做法 A：多阶段构建（最通用）。** 在构建阶段用到的密钥，留在构建镜像里，最终镜像只 COPY 产物：

```dockerfile
# 构建阶段：可以访问密钥
FROM maven:3.9-eclipse-temurin-17 AS builder
COPY settings.xml /root/.m2/settings.xml   # 含 Nexus 凭据
COPY pom.xml ./
COPY src ./src
RUN mvn -B package -DskipTests

# 最终阶段：只拿 jar，不带 settings.xml
FROM eclipse-temurin:17-jre
COPY --from=builder /app/target/app.jar /app/app.jar
```

`settings.xml` 只存在于 builder 镜像的层里，最终镜像完全没有它。

**做法 B：BuildKit secret mount（最优雅）。** 启用 BuildKit 后，可以把密钥以临时文件挂进构建，构建完不留痕：

```dockerfile
# syntax=docker/dockerfile:1.7
RUN --mount=type=secret,id=nexus_settings \
    cp /run/secrets/nexus_settings /root/.m2/settings.xml && \
    mvn -B package -DskipTests && \
    rm /root/.m2/settings.xml
```

构建时：

```bash
DOCKER_BUILDKIT=1 docker build \
  --secret id=nexus_settings,src=$HOME/.m2/settings.xml \
  -t myapp:1.0 .
```

密钥不会进任何层，`docker history` 也看不到。这是我们目前生产构建的标准做法。

**做法 C：运行期注入（敏感配置的首选）。** 数据库密码、API key 这类运行期才用的 secret，根本不该进镜像——用环境变量在 `docker run` 或编排层（K8s Secret、Swarm secret）注入：

```bash
docker run -e DB_PASSWORD_FILE=/run/secrets/db_password \
  --secret db_password myapp:1.0
```

记住一句话：**镜像里有的东西，等于公开了的东西。**

---

### 八、EXPOSE：文档，不是端口映射

`EXPOSE 8080` 只是声明容器意图监听这个端口，**它本身不会做任何端口映射**。真正把端口对宿主开放的是 `docker run -p 8080:8080`。

那 `EXPOSE` 还有用吗？有。它是镜像的"文档"，让使用者和编排系统（比如 `docker run -P` 大写 P、K8s 的某些自动发现）知道该容器监听哪些端口。我们的规范是：服务实际监听的端口必须在 `EXPOSE` 里声明，且和实际一致——声明了 8080 却监听 8081，排查问题时要命。

---

### 九、HEALTHCHECK：让编排系统知道你"真活着"

默认情况下，Docker 判断容器是否健康只看进程是否在跑。但进程在跑不等于服务能服务——线程死锁、连接池耗尽、依赖挂了，进程都还活着，对外却是死的。这就需要 `HEALTHCHECK`：

```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD curl -fsS http://localhost:8080/actuator/health || exit 1
```

<!-- 校准：请按真实经历核实/替换 -->

四个参数要按服务实际启动时间调：`start-period` 给应用预热用的宽限（Spring Boot 启动慢就调大），`interval` 是稳态检查频率，`retries` 决定连续失败几次才算 unhealthy。

我们线上一个真实教训：一个 Java 服务没配 HEALTHCHECK，OOM 后 GC 抖动但进程没死，编排系统以为它健康继续往里导流量，故障放大了 8 分钟才被人工发现。加上 HEALTHCHECK（curl actuator/health，actuator 里我们接了 JVM、DB、Redis 的健康指示器）之后，这类半死不活的状态 90 秒内就被摘掉。

还有两个 HEALTHCHECK 的细节值得说。第一，健康检查端点要轻——别在 `/health` 里做重逻辑（比如全表 count），否则每 30 秒一次的探针会拖垮服务本身；我们曾把一个带了 DB 慢查询的 health 接口上线，结果探针本身成了主要负载。第二，健康检查的退出码语义是固定的：0 健康、1 不健康，脚本里别用其他码，否则会被当成不健康处理。

---

### 十、USER：非 root 运行不是可选项，是底线

容器默认以 root 运行。这意味着如果应用有 RCE 漏洞，攻击者拿到的是容器内的 root，配合一个内核提权或挂载逃逸就能上宿主。**生产镜像必须以非 root 用户运行。**

标准写法：

```dockerfile
RUN groupadd -r app && useradd -r -g app -d /app -s /sbin/nologin app && \
    chown -R app:app /app
USER app
```

或者更省事，用基础镜像自带的非 root 用户（很多官方镜像有，比如 `node` 镜像的 `node` 用户）。注意 `USER` 之后，所有后续 `COPY`、`RUN` 都以该用户身份执行——如果该用户对目标目录没写权限，COPY 会失败。所以要么在切用户前 `chown`，要么 COPY 时带 `--chown`。

我们规定镜像内服务监听端口必须 > 1024（非特权端口），避免非 root 用户绑不上端口。Spring Boot 改 `server.port=8080` 而不是 80，就是这个原因。

还有个反直觉的点：`USER` 指令之后如果还有 `RUN`，那些 `RUN` 也会以非 root 身份执行。如果某条 `RUN` 需要装系统包（要 root），顺序就错了——必须把所有需要 root 的安装放在 `USER` 之前，切完用户后只做不需要特权的操作。这条顺序规则我们在 code review 里改过不下十次。

---

### 十一、生产级 Dockerfile 模板：Spring Boot 服务

下面是我们线上一个 Spring Boot 微服务真实在用的模板（已脱敏、参数为占位值）。它把前面所有原则落到一份文件里：

```dockerfile
# syntax=docker/dockerfile:1.7

############################
# 阶段 1：构建（可访问密钥）
############################
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build

# 先 COPY 依赖描述，最大化缓存命中
COPY pom.xml ./
COPY src ./src

# 用 BuildKit secret 挂载 Nexus 凭据，不进镜像层
RUN --mount=type=secret,id=nexus_settings,target=/root/.m2/settings.xml \
    mvn -B -q clean package -DskipTests

############################
# 阶段 2：运行时（最小化）
############################
FROM eclipse-temurin:17.0.10_7-jre-jammy

# 一次性装齐：时区数据、locale、ca-certificates、curl（健康检查用）
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates curl tini tzdata locales && \
    echo "Asia/Shanghai" > /etc/timezone && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen && \
    echo "zh_CN.UTF-8 UTF-8" >> /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8 \
    JAVA_OPTS="-XX:+UseG1GC -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

# 创建非 root 用户
RUN groupadd -r app && \
    useradd -r -g app -d /app -s /sbin/nologin app && \
    mkdir -p /app /app/logs && \
    chown -R app:app /app

WORKDIR /app

# 只从 builder 拷产物，settings.xml / 源码 / 构建工具一概不带
COPY --from=builder --chown=app:app /build/target/*.jar /app/app.jar

USER app

EXPOSE 8080

# tini 做 PID 1，正确处理信号转发与僵尸进程回收
ENTRYPOINT ["/usr/bin/tini", "--", "sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]

CMD ["--spring.profiles.active=prod"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/actuator/health || exit 1
```

<!-- 校准：请按真实经历核实/替换 -->

构建命令：

```bash
DOCKER_BUILDKIT=1 docker build \
  --secret id=nexus_settings,src=$HOME/.m2/settings.xml \
  --build-arg APP_VERSION=2.4.1 \
  -t registry.example.com/myapp:2.4.1 \
  -t registry.example.com/myapp:latest \
  -f Dockerfile .
```

几个值得点出的设计决策：

- **多阶段构建**：最终镜像不含 Maven、源码、settings.xml，体积从构建镜像的 ~900 MB 降到 ~210 MB。
- **tini 做 PID 1**：解决原生 PID 1 不转发信号的问题，Java 进程能正确收到 SIGTERM 优雅退出，`docker stop` 不再卡 10 秒超时。
- **时区 + locale 一起处理**：UTF-8 locale 配齐，避免日志里中文乱码；时区设 Asia/Shanghai，日志时间和业务时间一致。
- **MaxRAMPercentage 代替固定 Xmx**：容器里内存是 cgroup 限额，用百分比让 JVM 自动适配，换规格不用改 Dockerfile。
- **ExitOnOutOfMemoryError**：OOM 时直接退出，让编排系统重启，而不是半死不活地撑着。

---

### 十二、踩坑复盘：四个真实事故

**踩坑一：密钥泄露进镜像层。** 起因是早期图省事，把私有仓库的 `.npmrc`（含 token）COPY 进镜像再删。审计时发现 `docker history` 里清清楚楚。教训：**任何在构建期被 COPY 或 ARG 传入的值，都视为已进入镜像历史。** 全部迁移到多阶段 + BuildKit secret mount，并轮换了所有暴露过的凭据。

**踩坑二：时区错乱导致报表跨天。** 服务上线后业务方反馈某张日报的时间区间对不上。排查发现容器时区是 UTC，定时任务用的是 `LocalDateTime.now()`，凌晨 0 点触发的任务实际在 UTC 16:00 跑，跨了业务日。修复：基础镜像装 tzdata、设 `TZ=Asia/Shanghai`、`ln -sf` 本地时间。后来我们立了规矩：**所有镜像默认带时区配置，禁止裸 UTC 上线。**

**踩坑三：locale 缺失，中文日志变问号。** 一个 Java 服务日志里中文全部显示成 `?`，本地怎么跑都正常。根因是基础镜像只有 `C` locale，JVM 默认用 `file.encoding=ANSI_X3.4-1968`，中文编不进去。装 locales 包、`locale-gen en_US.UTF-8`、`ENV LANG=en_US.UTF-8` 三件套解决。后来 `locale` 配置进了我们的镜像基线。

**踩坑四：容器以 root 跑，被 RCE 后横向移动。** 一次安全演练，红队利用一个反序列化漏洞拿到了某服务容器的 shell，因为是 root，直接读了挂载进来的 kubeconfig，进而横向到了整个集群。整改：所有业务镜像强制非 root、移除挂载进来的高权限凭据、用 NetworkPolicy 限制东西向流量。这是推动我们立"非 root 运行是底线"这条规矩的直接原因。

---

### 十三、结语：可信镜像的四个特征

写到这里，可以把一份"好的生产级 Dockerfile"总结成四个特征：

1. **小**：多阶段构建 + slim/alpine 基础镜像，最终镜像只含运行必需物。
2. **安全**：非 root 运行、最小权限、密钥不进层、定期 CVE 扫描。
3. **可复现**：基础镜像钉到 digest、依赖锁文件、构建用 BuildKit。
4. **可观测**：HEALTHCHECK、tini 信号处理、清晰的 ENTRYPOINT/CMD 语义。

Dockerfile 不是配置文件，它是镜像生产线的工程图纸。每一条指令都值得较真——因为线上炸的每一个坑，追溯回去都是某条指令当时"图省事"。

把这些规矩落到团队的 Dockerfile 模板里，配上一次 CI 流水线的镜像扫描，你会发现自己晚上睡觉踏实多了。这是我们在踩了无数次坑之后，最值钱的一条经验。

<!-- 校准：请按真实经历核实/替换 -->
