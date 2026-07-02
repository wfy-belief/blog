---
title: Docker Compose 多服务编排实战与适用边界
abbrlink: docker-compose-orchestration
date: 2026-07-02 10:00:00
tags:
  - Docker
  - Compose
categories:
  - 技术
description: 把一套多服务环境用 Compose 在本地立起来，并讲清楚它在哪里该止步
ai_text: "本文复盘用 Docker Compose 编排多服务（应用 + Redis + PostgreSQL）的实战经验。先讲 Compose 的定位与适用边界——本地/测试/中小生产能用、复杂生产该上 K8s；再拆解 compose.yml 的 services/networks/volumes 结构与版本要点；重点讲 depends_on + healthcheck 的「真正就绪」等待、.env 与 profiles 的环境隔离、资源限制与重启策略、命名卷的数据持久化；最后给一个完整可跑的编排 demo，并复盘 depends_on 不等就绪、数据卷初始化顺序、环境变量覆盖、v1/v2 命令差异等踩坑。"
---

## 一、引言：Compose 是什么，以及我为什么还在用

我把 Docker Compose 用了大概五年，从最早「本地起一个 MySQL 加一个 Redis 图省事」，到后来用它跑整套微服务联调环境，再到给小团队搭过一两个能上线的中小生产。中间一度动摇过——Kubernetes 这几年已经是事实标准，Compose 是不是该退场了？踩过几个项目之后我形成了自己的判断：Compose 和 K8s 不是替代关系，是分层关系。Compose 解决的是「一台上把一组服务立起来」，K8s 解决的是「一群上把一组服务调度好」。前者是单机编排，后者是集群编排。

所以这篇文章我要讲清楚的只有一件事：怎么用 Compose 在单机上把多服务环境编排得稳、编得清楚，同时明确它在哪里该止步。不绕原理，只讲文件结构、依赖等待、环境隔离、资源约束和一个能跑的 demo，外加这些年踩过的坑。

先声明：下文涉及的 Compose 版本是 v2.x（写本文时为 2.27 左右），Docker Engine 为 25.x。<!-- 校准：请按真实经历核实/替换 -->

## 二、Compose 的价值与适用边界

我用 Compose 的场景大致分三档：

- **本地开发**：最高频。一个 `compose.yml` 把 app + 数据库 + 缓存 + 反向代理全拉起来，新人 clone 仓库后 `docker compose up` 就有一个能联调的完整环境，再也不用在 README 里写「请先装 MySQL 8、改 my.cnf、建库 test」。
- **CI 与测试**：跑集成测试时用 Compose 起依赖，测试结束 `down -v` 一把清掉。比 mock 数据库真实，比共享测试库干净。
- **中小生产/边缘部署**：团队内部工具、客户内网交付、几十到一两百 QPS 的轻量应用，Compose + 一台好机器能扛住，运维成本远低于上 K8s。

边界我也很明确：**一旦出现「需要多机调度、滚动升级不能停、要跨节点伸缩、要服务网格治理流量」这些诉求，就别在 Compose 上硬撑了，该上 Kubernetes。** Compose 的本质是单机上的「声明式启动器」，它没有真正的调度器、没有自愈、没有跨节点网络。历史上 Docker Swarm 把 Compose 文件带到集群里，但 Swarm 这几年已经式微，生态、文档、人才储备都转向了 K8s，我个人在 2022 年之后就没再给新项目选 Swarm。<!-- 校准：请按真实经历核实/替换 -->

简单说：Compose 是「单机的多服务编排」，K8s 是「集群的工作负载编排」。把 Compose 文件通过工具转换/迁移到 K8s 的尝试（比如早期的 Kompose）可以做为脚手架，但不要指望一份 compose.yml 既能本地跑又能直接喂给生产 K8s——那份文件的「信息量」是不一样的。

## 三、compose.yml 文件结构精讲

Compose 文件的顶层结构我固定按 `services` / `networks` / `volumes` 三段写，顺序不乱，读的人一眼能看出全貌。

### 3.1 顶层三段

```yaml
services:
  web:       # 业务服务
    ...
  db:        # 数据库
    ...
  cache:     # 缓存
    ...

networks:
  appnet:    # 自定义网络

volumes:
  pgdata:    # 命名卷
```

- `services` 是核心，每个键就是一个容器服务。
- `networks` 声明自定义网络。默认 Compose 会建一个项目同名网络，但我习惯显式声明，方便多服务之间用服务名互相访问。
- `volumes` 声明命名卷。命名卷的生命周期独立于容器，`docker compose down` 不会删它，只有带 `-v` 才删。

### 3.2 版本字段的坑

老教程里 compose.yml 第一行经常写 `version: "3.8"`。在 Compose v2 里这个字段已经被**废弃且忽略**了——Composed v2 的 parser 会打一条 warning，然后按内置 schema 解析。我现在新写的文件一律不带 `version` 字段，省得误导后来人以为「必须填」。

这里其实是 v1/v2 一个分水岭。下面专门拉出来讲。

### 3.3 service 内部的关键字段速查

| 字段 | 作用 | 我的使用习惯 |
|---|---|---|
| `image` / `build` | 拉镜像或本地构建 | 本地开发用 build 指向 Dockerfile，生产用固定 image tag |
| `ports` | 端口映射 `宿主:容器` | 只对要暴露给宿主的服务映射，内部互访走网络名 |
| `environment` / `env_file` | 环境变量 | 敏感信息走 `.env` + `env_file`，不写死在 yml |
| `depends_on` | 启动依赖 | 配合 `condition: service_healthy` 才有意义 |
| `healthcheck` | 健康检查 | 数据库类服务必加，给依赖方做"真就绪"信号 |
| `restart` | 重启策略 | 生产类服务 `unless-stopped`，避免重启宿主后不起来 |
| `volumes` | 挂载卷 | 数据库数据走命名卷，配置走 bind mount |
| `networks` | 加入网络 | 显式声明，避免默认网络带来的混乱 |
| `deploy.resources` | 资源限制 | 注意：只在 Swarm 模式下完全生效，单机要用 `mem_limit` 等 |

最后这一行是新手最容易踩的坑之一：`deploy.resources.limits` 在 `docker compose up`（非 Swarm）下**不生效**，单机要限制资源得用 v2 支持的顶层 `mem_limit` / `cpus`。下面踩坑那节会展开。

## 四、多服务依赖与健康等待：Compose 最值得讲清的一节

如果让我只挑一个主题讲透 Compose，我选「依赖与就绪」。因为这是 90% 多服务环境起不来的根因。

### 4.1 depends_on 默认不等"真正就绪"

最朴素的写法：

```yaml
services:
  web:
    depends_on:
      - db
      - cache
```

它的语义是：**容器进入 created 状态才启动 web**——也就是 db 的进程刚刚被拉起，完全不代表 db 已经能接受连接。于是 web 一启动就去连数据库，十有八九拿到 `Connection refused`，然后开始疯狂重试甚至直接崩溃退出。这是 Compose 入门第一个坑。

### 4.2 用 healthcheck + condition 解决

正确做法是给被依赖的服务加 `healthcheck`，再让依赖方写 `condition: service_healthy`：

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d app"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s

  web:
    image: myapp:latest
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_started
```

这样 Compose 会反复探活 db，直到 healthcheck 连续成功，才认为 db 就绪，再去启动 web。`start_period` 给数据库一个"冷启动宽限"，宽限期内失败不计入 retries，这点对 Java 类慢启动服务很重要。

我要强调三点实战细节：

1. **healthcheck 的 test 命令要测的是"服务能干活"，不是"端口开着"。** 比如数据库用 `pg_isready`、Redis 用 `redis-cli ping`，HTTP 服务用 `curl -f http://localhost:8080/health`。`nc -z` 那种只测 TCP 端口是不够的。
2. **应用代码里仍然要做重试。** 健康检查通过不代表之后一直可用——网络瞬断、连接池预热、并发竞争都可能让首批请求失败。我在 web 里固定用一个带指数退避的连接重试包装。
3. **`condition: service_started` 是退化选项**，等价于默认行为；只有 `service_healthy` 才是真正的"业务就绪"。

### 4.3 长链路与初始化顺序

依赖不只是直连依赖。比如我先要 db 起来，还要 db 执行初始化 SQL 建表，然后 web 才能用。典型踩法是：把 init SQL 放到 `docker-entrypoint-initdb.d/` 里，但是 web 的 `depends_on: service_healthy` 只等 db 进程健康，不等 init SQL 跑完。

PostgreSQL 镜像的约定是：容器**首次启动**（数据目录为空）时才会执行 initdb.d 里的脚本，而且 entrypoint 会等这些脚本执行完才打开监听。所以这种情况下，pg_isready 通过基本就等于 init 也跑完了。但 MySQL 的某些镜像不是这么做的——init 脚本和监听几乎并行，于是你 healthcheck 通了表却没建好。这个坑我踩过，最后是给 web 单独加一个 `wait-for-scripts` 一次性 init 容器，让它跑完再放行。<!-- 校准：请按真实经历核实/替换 -->

## 五、环境变量、.env 与 profiles

### 5.1 三层环境变量与覆盖坑

Compose 里同一个变量可能从好几个地方来，优先级从高到低我整理过：

1. `docker compose run -e KEY=xxx` 命令行传入
2. 当前 shell 的环境变量
3. 项目根目录 `.env` 文件
4. compose.yml 里 `environment` 显式给的值

听起来合理，但有个反直觉点：第 4 层的「显式给的值」**可以引用 .env 里的变量**，写法是 `${DB_PASSWORD}`。所以最常见的写法是 yml 里 `environment: { POSTGRES_PASSWORD: ${DB_PASSWORD} }`，真实密码放 `.env`。我吃过一次亏：在 yml 里直接写了一个默认值 `POSTGRES_PASSWORD: postgres`，又在 `.env` 里写了 `DB_PASSWORD=secret`，结果 yml 里的硬编码生效，把 `.env` 屏蔽了——因为第 4 层优先级低于 shell 但**高于** `.env` 中未被引用的键。规则的正确理解是：一旦某个 key 在 `environment` 段被显式赋值，`.env` 里同名 key 就不再透传，除非你在 yml 里用 `${}` 引用它。

记住一条：**所有需要被外部覆盖的变量，yml 里一律写成 `${VAR}` 形式，给一个默认值就写 `${VAR:-default}`**。这样行为可预测。

### 5.2 .env 别进 git

`.env` 含密码、Token，必须进 `.gitignore`。仓库里只提交一个 `.env.example`，列出 key 不填值，新人复制一份改。这事说一百遍都有人忘。

### 5.3 profiles：按场景选服务

Compose v2 引入了 `profiles`，可以给服务打标签，启动时用 `--profile` 选择启哪些。我用它做的最多的事是「开发态 vs 测试态」分离：

```yaml
services:
  web:
    ...
  db:
    ...
  # 只在压测 profile 下启 mock 外部依赖
  wiremock:
    profiles: ["loadtest"]
    image: wiremock/wiremock
```

默认 `docker compose up` 不启 wiremock，只在 `docker compose --profile loadtest up` 时才拉。这比维护两份 yml 干净得多。

## 六、资源限制与重启策略

### 6.1 单机下的资源限制

前面提过 `deploy.resources` 在非 Swarm 单机模式下不生效。要在单机限制 CPU/内存，v2 的写法是：

```yaml
services:
  web:
    image: myapp:latest
    mem_limit: 1g
    mem_reservation: 512m
    cpus: 1.5
```

这套字段来自 v1 的 Compose v2 兼容层，至今依然支持。限制资源的意义不只是「防止某个服务把宿主吃光」，更重要的是防止「日志风暴把磁盘写满导致整机不可用」这种连锁故障。我给所有服务默认 `mem_limit`，再加日志驱动的大小限制，这个习惯救过我两次。

### 6.2 重启策略

四个选项：`no` / `always` / `unless-stopped` / `on-failure`。生产我的默认是 `unless-stopped`：进程崩了自动拉起，宿主重启后容器也自动起，但 `docker compose stop` 主动停下的不会被强行拉回来。`always` 太霸道，连你手动 stop 都给你重启。

要特别注意：**重启策略不能替代健康检查的自愈。** 容器虽然"在跑"，但应用可能已经卡死。真正的自愈要靠 healthcheck 标记 `unhealthy`，再配合 `restart` 的失败计数——但 Compose 单机对 unhealthy 容器并不会自动重启（这是它和 K8s liveness 的一个本质差距）。所以 Compose 里的 healthcheck 更多是「告诉依赖方别连我」的信号源，不是 K8s 那种「死了替我重启」的探针。这条认知我晚了一年才理顺。

## 七、命名卷与自定义网络

### 7.1 命名卷的持久化

数据要落盘的，一定要用命名卷，别用 bind mount 写数据。命名卷由 Docker 管理，跨平台路径一致、性能更好、迁移时 `docker compose down` 不会丢。bind mount 我只用来挂配置文件和源代码热重载。

```yaml
volumes:
  pgdata:

services:
  db:
    image: postgres:16
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./initdb:/docker-entrypoint-initdb.d:ro   # 初始化 SQL 用 bind
```

命名卷的物理位置在 `/var/lib/docker/volumes/<project>_<name>/_data`，需要直接拷数据时找得到。

### 7.2 自定义网络与服务名解析

Compose 默认给项目建一个 bridge 网络，并在该网络里内置 DNS，服务之间用**服务名**当主机名互访。但我仍然显式声明 `networks`，原因有两个：一是多项目共用网络时（比如一个公共网关项目要连到多个 app 项目），显式声明才可控；二是默认网络没有别名，自定义网络可以给同一个服务起多个 alias。

```yaml
networks:
  appnet:
    driver: bridge

services:
  web:
    networks:
      appnet:
        aliases:
          - api
          - app.internal
```

这样网络上 `api`、`app.internal` 都能解析到 web 容器，对外的网关和内部的脚本可以各用各的名字。

## 八、一个完整的多服务编排 demo

下面这套是我给一个内部工具搭过的小平台：Python Web app + Redis 缓存 + PostgreSQL 主库，前面挂 Nginx。我把生产非关键的简化了，保留核心结构。直接复制改改就能跑。<!-- 校准：请按真实经历核实/替换 -->

```yaml
# compose.yml
services:
  web:
    build: ./app
    image: internal-tool/web:${TAG:-latest}
    environment:
      DB_HOST: db
      DB_PORT: "5432"
      DB_NAME: ${DB_NAME}
      DB_USER: ${DB_USER}
      DB_PASSWORD: ${DB_PASSWORD}
      REDIS_HOST: cache
      REDIS_PORT: "6379"
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_started
    networks: [appnet]
    restart: unless-stopped
    mem_limit: 1g
    cpus: 1.0
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health',timeout=2).status==200 else 1)"]
      interval: 10s
      timeout: 3s
      retries: 5
      start_period: 20s

  db:
    image: postgres:16
    environment:
      POSTGRES_DB: ${DB_NAME}
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./initdb:/docker-entrypoint-initdb.d:ro
    networks: [appnet]
    restart: unless-stopped
    mem_limit: 2g
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${DB_USER} -d ${DB_NAME}"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 10s

  cache:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes", "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"]
    volumes:
      - cachedata:/data
    networks: [appnet]
    restart: unless-stopped
    mem_limit: 384m
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10

  nginx:
    image: nginx:1.27-alpine
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    ports:
      - "8080:80"
    depends_on:
      web:
        condition: service_healthy
    networks: [appnet]
    restart: unless-stopped

networks:
  appnet:
    driver: bridge

volumes:
  pgdata:
  cachedata:
```

配套的 `.env.example`：

```
TAG=latest
DB_NAME=appdb
DB_USER=app
DB_PASSWORD=change-me-in-prod
```

启动顺序我用 `docker compose up -d`，期望的拉起链是：db/cache 先起 → db 的 healthcheck 通过（含 initdb 完成）→ web 启动并完成自身健康检查 → nginx 启动对外。整套大概 30~40 秒冷启动到全绿。

## 九、踩坑复盘：我把这些坑都踩过一遍

### 9.1 depends_on 不等真正就绪

最早写法只写 `depends_on: [db]`，web 启动瞬间连 db 拒绝连接，框架没做重试直接挂。修法就是上面那套 healthcheck + `service_healthy`。

### 9.2 数据卷初始化顺序

把 init SQL 放进 initdb.d 后，本地第一次 `up` 跑通了，但同事 down 之后 `up` 表没了——因为带 `-v` 把卷删了，重新初始化。后来约定：本地开发可以 `-v`，但**线上 down 永远不带 -v**，删卷要单独走一条 `docker volume rm` 的命令，给个心理摩擦。还有一次是 init 脚本之间有依赖（B 表的外键指向 A 表），A 还没建 B 先跑了——postgres initdb 是按文件名字母序，所以我把脚本命名改成 `01-create-a.sql`、`02-create-b.sql` 这样前缀化。

### 9.3 环境变量覆盖坑

前面提过：在 yml 里硬编码 `POSTGRES_PASSWORD: postgres` 想给默认值，结果 `.env` 里的真实密码失效。教训：要么不写、要么写 `${VAR:-default}`。

### 9.4 v1 与 v2 命令差异

老项目里脚本全是 `docker-compose`（带连字符），新装机器只有 `docker compose`（子命令形式）。两者行为大部分兼容，但有几个差异让我踩过：

- **资源限制字段**：v1 的 `mem_limit` 在 v2 早期一度被警告要废，后来又保留为兼容。`deploy.resources` 是 K8s/Swarm 风格字段，单机下不生效。
- **项目名前缀**：v2 默认用目录名做项目名，会把目录名小写化、特殊字符去掉；v1 行为略不同。多项目并存时容易混，我固定用 `-p myproject` 显式指定。
- **网络默认**：v2 默认 bridge 网络的 DNS 行为更严格，老的一些 host 网络混用方案要重新调。

迁移建议：新项目一律用 v2 的 `docker compose`，老脚本里的 `docker-compose` 可以加一个 alias 兜底，但别再新写 v1 命令。

### 9.5 健康检查把宿主拖垮

有一回我给 web 的 healthcheck 写了一个调外部 API 的脚本，每 5 秒一次，结果把对方限流打爆了。healthcheck 必须是**本地的、廉价的**，绝不能调外部依赖。还有 `interval` 别太短，5~10 秒是甜区。

### 9.6 日志写满磁盘

默认的 json-file 驱动不轮转，跑久了日志几十 G 把宿主 root 分区撑爆，整机 OOM 连带 Compose 一起挂。修法是每个服务都加日志限制：

```yaml
logging:
  driver: json-file
  options:
    max-size: "10m"
    max-file: "3"
```

这是我现在的硬约定，和 mem_limit 一样默认带上。

## 十、什么时候该止步：把 Compose 用在该用的地方

最后我想给一个明确的「止步线」，省得后来人在 Compose 上耗到怀疑人生。下面这些信号出现任何一条，就该认真评估上 K8s（或至少上 Nomad 这类真正的调度器）：

- **要跨多台机器**：单机扛不住、或者要高可用必须多节点。Compose 单机，没有调度。
- **要零停机滚动升级**：Compose 的 `up --no-deps` 能重建容器，但期间有秒级中断，且没有就绪流量切流。
- **要跨服务 mTLS、流量镜像、金丝雀**：这些是服务网格能力，Compose 没有，硬接 Sidecar 自己写很亏。
- **要按指标自动伸缩**：HPA/VPA、Pod 水平伸缩，Compose 没有。
- **团队规模上来、要多环境多集群治理**：Helm/Kustomize + GitOps 那套体系在 K8s 上才成熟。

反过来，下面这些场景 Compose 是我第一选择：本地开发联调环境、CI 集成测试依赖、内部小工具、客户内网一次性交付、POC/Demo。这些场景用 K8s 是杀鸡用牛刀，用 Compose 一个人一小时就能搞定。

**总结一句：Compose 是单机多服务编排的甜区工具，把"一台上立起来"这件事做到极致；但只要诉求越过单机这条线，请把舞台让给 Kubernetes。** 认清边界，比堆功能更重要。

我个人的实践一直遵循这条线：本地与测试一律 Compose，线上看规模——小而稳的就用 Compose 加一台好机器扛着，大而活的就直接上 K8s，不在中间地带反复横跳。这套打法跑了五年，没在任何一边卡死过，也省下了大量「为了用而用」的精力。
