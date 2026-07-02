---
title: 私有镜像仓库 Harbor 搭建与生产运维实战
abbrlink: docker-private-registry-harbor
date: 2026-07-02 10:20:00
tags:
  - Docker
  - Harbor
  - 镜像仓库
categories:
  - 技术
description: 从官方 Registry 到 Harbor 的升级决策，拆解架构、部署、RBAC、漏扫签名复制与 GC 运维，复盘生产踩坑。
ai_text: "本文复盘我们在生产环境落地 Harbor 私有镜像仓库的全过程：先讲为什么需要私有仓库与官方 Registry 的局限，再拆解 Harbor 的 core/registry/jobservice/trivy/db/redis 多组件架构，给出 compose 与 helm 两种部署路径、HTTPS 证书与 LDAP/OIDC 身份接入、RBAC 项目权限模型，重点覆盖漏洞扫描、cosign 签名、配额、多机房复制与停机 GC 流程，最后复盘磁盘膨胀、证书过期、大镜像推送超时、复制延迟等真实踩坑。"
---

# 私有镜像仓库 Harbor 搭建与生产运维实战

## 一、为什么我们要搭私有仓库

去年我们把 CI/CD 全链路容器化之后，第一个撞上来的问题就是镜像分发的带宽 <!-- 校准：请按真实经历核实/替换 -->。当时集群规模 200+ 节点，分布在两个机房，每天构建产物大约 400 个镜像，单个镜像平均 1.2GB。如果每个节点都去 Docker Hub 拉，不仅出口带宽爆掉，拉取耗时也让部署窗口从 3 分钟拉长到 20 分钟。更麻烦的是 Docker Hub 的拉取限速（anonymous 100 pulls/6h），高峰期直接 429。

搭私有仓库的诉求其实就三条：

1. **内网带宽**：镜像分发走内网，千兆/万兆链路随便用，单次拉取从分钟级降到秒级。
2. **安全合规**：金融/政企场景镜像不能出公网，且需要漏洞扫描、签名审计、留存证据。Docker Hub 的公开仓库根本满足不了等保要求。
3. **缓存加速**：通过 proxy cache 模式把上游 Docker Hub / Quay 的镜像缓存在本地，重复拉取走缓存，既省带宽又快。

一开始我们用的是官方 `registry:2`，一个二进制跑起来就能 push/pull，简单到令人感动。但用了两个月就扛不住了——没有 Web UI、没有权限模型、没有漏扫、没有复制、GC 要手动停服务跑命令。团队 review 了一圈，最终上了 Harbor。下面把这次升级的来龙去脉讲清楚。

## 二、官方 Docker Registry vs Harbor：什么时候该升级

很多人觉得官方 Registry 够用，其实它定位就是一个"无脑可用的存储后端"，对标的是 `git daemon`。Harbor 才是企业级仓库。我把两者的差异列成一张表，决策时直接对照：

| 维度 | 官方 Docker Registry | Harbor |
|---|---|---|
| 定位 | 存储后端，最小化 | 企业级平台，全功能 |
| 权限 | 基本认证（htpasswd），无项目概念 | 项目级 RBAC，访客/开发者/项目管理员/系统管理员 |
| 身份集成 | 无 | LDAP / OIDC / SAML / uaa |
| Web UI | 无 | 完整 Portal（Angular） |
| 漏洞扫描 | 无 | 内置 Trivy，可设阻断策略 |
| 镜像签名 | 无 | Notary v2 + Cosign |
| 复制（多仓库同步） | 无 | pull/push 模式，多 remote 同步 |
| 配额 | 无 | 项目级存储 + 镜像数配额 |
| GC | 手动停服务跑 `registry garbage-collect` | Web 触发，支持不停机（在线 GC） |
| 审计日志 | 无 | 全量操作审计 |
| 部署复杂度 | 单容器 | compose 多组件 / helm chart |

**结论**：个人/小团队用官方 Registry 足够；一旦你需要项目隔离、漏扫、复制、审计中的任何一项，就该上 Harbor。我们当时四项都要，没有选择。

## 三、Harbor 架构拆解

Harbor 不是一个单体，而是一组微服务。理解组件分工是后面排障的前提。先看架构图：

```
                     用户 / CI / k8s 集群
                            |
                            v
                    +-----------------+
                    |   nginx (代理)   |  <-- TLS 终止、路由分发
                    +-----------------+
                            |
       +----------+---------+---------+----------+----------+
       |          |                   |          |          |
       v          v                   v          v          v
   +-------+ +---------+        +----------+ +--------+ +-------+
   | Portal| | Core API|        | Registry | |Jobsvc  | | Trivy |
   | (UI)  | | (业务核心)|        | (distribution)| |(异步任务)| |(漏扫) |
   +-------+ +---------+        +----------+ +--------+ +-------+
                |                    |          |          |
                |  +-----------------+----------+----------+
                v  v                 v          v
            +-------+           +-------+   +-------+
            | PG DB |           | Redis |   |存储后端|
            |(元数据)|           |(缓存) |   |/data  |
            +-------+           +-------+   +-------+
```

各组件职责与边界，这里逐个展开，因为这部分理解错了后面排障全是盲人摸象：

- **nginx**：所有入口流量经它，TLS 终止在这里，按路径分发到 portal / core / registry / trivy。证书配置在它身上。生产里我们额外在它前面挂了一层云 SLB 做四层负载与跨机房容灾，nginx 只做七层路由。**所有超时、body 大小限制都在这里调**，registry 本身只管读写存储。
- **Portal**：Angular 前端，提供项目管理、镜像浏览、扫描结果查看。它纯静态资源，挂了不影响 push/pull，只影响 Web 操作，所以多副本无脑扩。
- **Core**（harbor-core）：业务大脑，管项目、用户、RBAC、配额、复制策略、Webhook。所有 API `/api/v2.0/*` 走它。Core 是无状态的（状态都在 PG），可以水平扩；但它崩溃会直接导致 `docker login` 失败、UI 不可用，所以生产必须多副本 + 健康检查。
- **Registry**：底层就是官方 `distribution`，负责镜像 manifest/blob 的实际存储读写。Harbor 的存储目录 `/data/registry` 挂给它。所有 push/pull 流量最终落到这里，I/O 压力最大。它通过一个 token 服务（Core 提供的 `/service/token`）做鉴权，鉴权通过后才放行 blob 读写——这是理解"为什么 core 挂了 docker login 也挂"的关键。
- **JobService**：异步任务调度，跑复制、GC、扫描这类耗时任务。和 Core 解耦，避免长任务阻塞 API。JobService 用 Redis 做任务队列，worker 数量受 `max_job_workers` 控制，开太大 DB 连接会爆。
- **Trivy**：漏洞扫描引擎，扫镜像 layer 里的 OS 包与语言依赖。Harbor 2.x 起默认扫描器，替代了早期的 Clair。Trivy 维护自己的 CVE 数据库（存自己的 PG schema），库每天从 GitHub 更新一次，**离线环境必须手动导入库**，否则扫描结果是"无漏洞"假象。
- **PostgreSQL**：存项目、用户、策略、扫描结果等元数据。镜像 blob 不进库，进文件系统。这是整个系统唯一的有状态强依赖，**单点即全站挂**，生产必须外置高可用。
- **Redis**：Core/JobService 的缓存与会话存储。会话丢了用户要重新登录，不影响 push/pull，所以 Redis 可用性要求比 PG 低一档。
- **Chartmuseum**（可选）：Helm chart 仓库，2.x 后逐步弱化，迁向 OCI chart。新部署直接关掉这个组件，用 OCI 格式存 chart 更现代。

**关键认知**：镜像数据在文件系统（`/data/registry`），元数据在 PG。备份和迁移都要分开处理，这点后面会踩坑。鉴权链路是 nginx → core(token) → registry，任何一环挂都会让 `docker pull` 报奇怪的错误，排查时顺着这条链查。

## 四、部署：从 compose 到 helm

Harbor 官方提供两套部署：`docker-compose`（单机）和 Helm chart（K8s）。我们生产用 helm，测试环境用 compose。

### 4.1 compose 部署与 harbor.yml 关键配置

先装 `harbor-installer.tgz`，改 `harbor.yml`：

```yaml
# harbor.yml 核心字段
hostname: harbor.acitrus.cn

http:
  port: 80

https:
  port: 443
  certificate: /data/cert/harbor.crt
  private_key: /data/cert/harbor.key

# 数据存储目录，registry / trivy / pg 都往这写
data_volume: /data

# 数据库密码，生产务必改强密码
database:
  password: <strong-password>
  max_idle_conns: 50
  max_open_conns: 1000

# 扫描配置
trivy:
  ignore_unfixed: false
  skip_update: false
  offline_scan: false

# 复制、GC 等 job 的并发
jobservice:
  max_job_workers: 10

# 自动 GC 临时镜像
garbage_collection:
  enabled: true
  schedule: "0 0 1 * * *"  # 每天凌晨 1 点触发在线 GC（不删未引用 blob，只清 untagged）

# 对外暴露
external_url: https://harbor.acitrus.cn
```

装的时候执行 `./install.sh --with-trivy --with-chartmuseum`。注意 `--with-chartmuseum` 是可选的，我们后来弃用了，纯 OCI。

### 4.2 HTTPS 证书

这一步最容易出事。生产必须上正规 CA 证书，别用自签。我们用 Let's Encrypt 泛域名：

```bash
# acme.sh 申请泛域名
acme.sh --issue --dns dns_ali -d acitrus.cn -d *.acitrus.cn

# 装到 Harbor 路径
acme.sh --install-cert -d acitrus.cn \
  --key-file       /data/cert/harbor.key \
  --fullchain-file /data/cert/harbor.crt \
  --reloadcmd      "docker-compose -f /opt/harbor/docker-compose.yml restart nginx"
```

自签证书场景下，所有 k8s 节点都要把 CA 证书放到 `/etc/docker/certs.d/harbor.acitrus.cn/ca.crt`，否则 `docker pull` 报 `x509: certificate signed by unknown authority`。这块踩坑率 100%，下面复盘细讲。

### 4.3 LDAP / OIDC 接入企业身份

我们公司身份在 LDAP，Harbor 配 `ldap` 模式：

```yaml
# 管理界面 → 配置 → 认证模式
auth_mode: ldap_auth
ldap_url: ldap://ldap.corp.acitrus.cn:389
ldap_base_dn: ou=staff,dc=corp,dc=acitrus,dc=cn
ldap_uid: uid
ldap_scope: 2   # 子树搜索
```

后来公司迁到 OIDC（Keycloak），切到 `oidc_auth`：

```yaml
auth_mode: oidc_auth
oidc_name: Keycloak
oidc_endpoint: https://sso.corp.acitrus.cn/realms/corp
oidc_client_id: harbor
oidc_client_secret: <secret>
oidc_scope: openid,profile,email,groups
oidc_user_claim: preferred_username
```

切 OIDC 时一定要配 `oidc_group_filter` 把 Harbor 的项目角色和 OIDC group 绑定，否则每个新用户都要手动加项目权限，运维负担极重。

### 4.4 helm 部署（生产）

K8s 场景用官方 chart：

```bash
helm repo add harbor https://helm.goharbor.io
helm fetch harbor/harbor --version 1.13.0 --untar   <!-- 校准：请按真实经历核实/替换 -->
```

`values.yaml` 关键项：

```yaml
expose:
  type: ingress
  tls:
    enabled: true
    certSource: secret
    secret:
      secretName: harbor-tls
  ingress:
    hosts:
      core: harbor.acitrus.cn

externalURL: https://harbor.acitrus.cn

persistence:
  persistentVolumeClaim:
    registry:
      size: 2000Gi
      storageClass: ceph-rbd
    database:
      size: 50Gi
      storageClass: ceph-rbd
    redis:
      size: 10Gi

# 高可用：core/portal/jobservice/trivy 副本数
core: { replicas: 2 }
portal: { replicas: 2 }
jobservice: { replicas: 2 }
trivy: { replicas: 2 }

# registry 本身无状态（元数据在 PG），可水平扩展
registry: { replicas: 2 }
```

## 五、日常使用：推送拉取与 RBAC

### 5.1 RBAC 项目权限模型

Harbor 的隔离单位是**项目（Project）**，分公开/私有。每个项目下挂镜像仓库，权限角色四级：

| 角色 | 能力 |
|---|---|
| 项目管理员 | 增删成员、改配置、删镜像 |
| 维护者 | push/pull、扫镜像、看日志 |
| 开发者 | push/pull |
| 访客 | 只 pull |

我们按业务线建项目：`data-warehouse`、`ml-platform`、`online-service`，每项目配对应 LDAP/OIDC group。新人入职自动进对应项目的"开发者"组，离职同步撤销。

### 5.2 推送拉取

```bash
# 登录
docker login harbor.acitrus.cn -u fyu.wang

# 打标签（注意命名空间 = 项目名）
docker tag spark:3.5.1 harbor.acitrus.cn/data-warehouse/spark:3.5.1

# 推送
docker push harbor.acitrus.cn/data-warehouse/spark:3.5.1

# k8s 里 imagePullSecret 引用
kubectl create secret docker-registry harbor-secret \
  --docker-server=harbor.acitrus.cn \
  --docker-username=svc-k8s \
  --docker-password=<token> \
  --docker-email=devops@acitrus.cn
```

机器账号（service account）建议在 Harbor 里建一个独立账号配 API token，别用真人账号——离职清理时 K8s 的 imagePullSecret 会一起失效，排查极痛苦。

## 六、企业功能：漏扫、签名、配额、复制

### 6.1 漏洞扫描策略

Trivy 默认每天更新 CVE 库，新 push 的镜像自动扫。扫描分两种触发：`on_push`（推送即扫）和 `on_schedule`（定时重扫，因为 CVE 库更新后老镜像可能新增漏洞）。我们在项目级别设了**阻断策略**：

- Critical 漏洞存在 → 禁止 pull（k8s 拉不动）
- High 漏洞存在 → 告警但不阻断

```yaml
# Web 界面 → 项目 → 策略
scan_policy: on_push, on_schedule(每日 02:00)
vulnerability_blocking:
  severity: Critical
  action: block_pull
```

Trivy 自身的配置在 `harbor.yml`：

```yaml
trivy:
  ignore_unfixed: false      # 是否忽略未修复的 CVE
  skip_update: false         # 离线环境设 true，但需手动导入库
  offline_scan: false
  security_check: vuln       # 也可加 secret/license
```

坑：阻断策略开太严会导致半夜紧急发布时镜像拉不动。我们的做法是 `data-warehouse` 这类离线 ETL 开严格阻断，`online-service` 只告警不阻断，灰度期间靠人审。另外 Trivy 扫大镜像（>5GB）很慢，会占满 worker，建议把 `trivy replicas` 至少开 2，并限制单次扫描超时。

### 6.2 镜像签名：Cosign

Notary v2 已逐步被 Cosign 取代，Harbor 2.5+ 原生支持 Cosign 签名验证。

```bash
# 生成 key（私钥妥善保管，丢了镜像无法再签）
cosign generate-key-pair

# 签名
cosign sign --key cosign.key harbor.acitrus.cn/data-warehouse/spark:3.5.1

# 验证（k8s 部署时配合 Sigstore policy-controller 强制校验）
cosign verify --key cosign.pub harbor.acitrus.cn/data-warehouse/spark:3.5.1
```

Harbor 里可在项目策略开"只允许拉取已签名镜像"，配合 Kyverno / Sigstore policy-controller 在 admission 阶段拦截未签名镜像，形成供应链安全闭环。

签名 key 管理是这套方案的命门。我们的做法：cosign 私钥放 KMS（阿里云 KMS / HashiCorp Vault），CI 里通过短期 token 取用，不让私钥落地到磁盘。换 key 时老镜像要重签，否则验证会失败——这点在 key 轮转计划里一定要提前排。

### 6.3 配额

项目配额两条线：存储容量 + 镜像数。

```
data-warehouse: 500GB / 1000 个 tag
ml-platform:    1TB  / 2000 个 tag
```

超配额 push 直接 413 报错。我们踩过一次：CI 没清理旧 tag，三天把 `data-warehouse` 顶到上限，导致当天 ETL 镜像推不上去。后来在 CI 流水线加了 retention 规则——保留最近 30 个 tag，其余自动删。

### 6.4 复制（多机房同步）

我们在 A、B 两个机房各部署一套 Harbor，A 为主、B 为灾。复制策略用 push 模式：

```
策略：A.data-warehouse → B.data-warehouse
模式：push
触发：实时（event-based）
过滤：name=spark/*,flink/*
覆盖：dest 已存在则覆盖
```

复制走的是 Harbor 之间的 `replication` API，底层是 distribution 的 mount blob，跨机房只传差量 layer，不重复传整镜像。带宽占用比 `docker save | ssh docker load` 低一个数量级。

复制策略支持三种触发：实时（push 即触发）、定时（cron）、手动。生产建议实时 + 定时对账双保险——实时靠 webhook 事件，事件丢失时定时全量对账能补回来。多机房星型拓扑（一主多从）比网状拓扑好维护，避免循环复制导致镜像 tag 来回覆盖。

### 6.5 Proxy Cache：缓存加速上游

这是 Harbor 一个被低估的能力。建一个 **Proxy Cache 项目**，类型选 proxy，远端配置 Docker Hub / Quay / 阿里云。k8s 节点拉 `docker.io/library/nginx:1.25` 时，改为拉 `harbor.acitrus.cn/dockerhub-proxy/library/nginx:1.25`，Harbor 第一次会回源 Docker Hub，之后同 tag 的拉取全部走本地缓存。

```
项目：dockerhub-proxy（类型=Proxy Cache）
远端：Docker Hub（https://hub.docker.com）
凭证：用付费账号提高回源限速
```

这个特性上线后我们机房出口带宽降了 70% <!-- 校准：请按真实经历核实/替换 -->，且 Docker Hub 限速彻底无感。注意 proxy cache 的镜像不参与配额和漏扫（因为它本质是只读代理），重要的镜像还是要 push 到正常项目里固定下来。

## 七、生产运维：GC、备份、高可用

### 7.1 GC 流程（必须停 registry）

这是 Harbor 运维里最容易被忽略、也最容易出事的一环。**registry 的 GC 是停机 GC**——必须先把 registry 容器停掉，否则 GC 期间 push 上来的 blob 可能被误删。

为什么必须停？因为 distribution 的 GC 本质是 mark-and-sweep：它先扫一遍 manifest，把所有"被引用"的 blob 标记成 alive，然后删掉未被标记的 blob。如果 GC 跑的过程中有新 push，新 blob 还没建立 manifest 引用，就会被当成 garbage 删掉——数据直接丢。所以必须停写。

```bash
# 1. 进入 harbor 目录
cd /opt/harbor

# 2. 停掉 registry（注意只停 registry，不是停整个 compose）
docker-compose stop registry

# 3. 跑 GC（dry-run 先看一眼）
docker run -it --rm \
  -v /data/registry:/storage \
  registry:2 garbage-collect --dry-run /etc/registry/config.yml

# 4. 真删
docker run -it --rm \
  -v /data/registry:/storage \
  registry:2 garbage-collect /etc/registry/config.yml

# 5. 起回 registry
docker-compose up -d registry
```

Harbor 2.x 引入"在线 GC"，但那是只删 untagged 镜像、不删未引用 blob 的轻量清理，**不能替代真正的停机 GC**。生产上我们每月一次停机 GC，挑业务低峰窗口（凌晨 3 点），整个流程 8 分钟左右（仓库 4TB 规模） <!-- 校准：请按真实经历核实/替换 -->。

K8s 部署的 GC：把 registry 副本数先缩到 0，跑一个 job 容器挂载 PVC 执行 `garbage-collect`，完成后扩回来。

### 7.2 备份

两部分分开备份：

```bash
# 1. 元数据：PG 逻辑备份
pg_dump -h harbor-db -U postgres registry > registry_$(date +%F).sql

# 2. 存储层：/data/registry 用 rsync 到对象存储
rsync -avz --delete /data/registry/ minio:harbor-backup/registry/

# 3. 配置：harbor.yml + secret
tar czf harbor-config-$(date +%F).tgz /opt/harbor/harbor.yml /data/cert/
```

恢复演练每季度做一次，别等真出事才发现备份是坏的。

### 7.3 高可用

Harbor 的 HA 关键在三点：

1. **PG 用外置高可用**（Patroni / 云 RDS），不要用内置单点。
2. **Redis 用外置集群**，同样不依赖内置。
3. **registry 共享存储**（Ceph / S3 / OSS），多副本无状态拉同一份数据。
4. **Core/Portal/JobService/Trivy 多副本**，前置 ingress 做负载均衡。

JobService 多副本时注意：复制任务会被多个 worker 抢，Harbor 内部用 DB 行锁保证不重复，但 worker 太多会拖慢 DB，我们经验值 2-3 副本够用。

另外有个隐藏点：Harbor 的 PG 默认连接池上限是 1024，多副本扩展时要相应调大 `max_open_conns`，否则高峰期 Core 会报 `pq: too many connections`。Redis 同理，job worker 数 × replicas 不要超过 Redis 的 maxclients。

## 八、踩坑复盘

下面这些坑都是我们真金白银交过学费的，按时间线列。

### 坑 1：磁盘膨胀，GC 不及时

上线半年后某天告警 `/data` 用量 92%`。查下来是 CI 不断推 tag，老镜像没人删，配额没设，停机 GC 也没排期。最后 `df` 显示 4TB 盘剩 320GB。

**根因**：配额 + retention + GC 三件事都没做。
**修复**：
- 项目配额强制开，默认 500GB。
- CI 流水线加 retention：保留最近 30 个 tag，自动删除老 tag（Harbor API `DELETE /projects/{id}/repositories/{name}/artifacts/{tag}`）。
- 每月固定停机 GC 窗口。
- 监控 `/data` 用量，超 80% 自动告警。

### 坑 2：证书过期，整集群拉不动镜像

Let's Encrypt 证书 90 天到期，acme.sh 的 `--reloadcmd` 当时配的是 `docker-compose restart`，但 Harbor 装目录后来从 `/opt/harbor` 挪到了 `/data/harbor`，reload 命令路径错了，证书续了但 nginx 没重载。某天早上全集群 k8s 拉镜像报 `x509: certificate has expired`，ETL 全挂。

**修复**：
- reload 命令改成绝对路径 + 加 `set -e` 校验。
- Prometheus blackbox exporter 监控证书到期天数，30 天前告警。
- 证书路径用 symlink，acme 续签只换 symlink 指向。

### 坑 3：大镜像推送超时

ML 平台推一个 18GB 的 GPU 训练镜像，nginx 报 `413 Request Entity Too Large`，改了 `client_max_body_size 0`（无限制）后变成 60 秒超时。

**根因**：nginx 默认 `proxy_read_timeout 60s`，大 layer 上传超过 60 秒就被掐。
**修复**：

```nginx
# nginx http 块
client_max_body_size 0;
client_body_timeout 1800s;
proxy_read_timeout 1800s;
proxy_send_timeout 1800s;
chunked_transfer_encoding on;
```

同时 registry 容器的 `REGISTRY_HTTP_TIMEOUT` 也要调大。改完 18GB 镜像推送稳定在 4 分钟 <!-- 校准：请按真实经历核实/替换 -->。

### 坑 4：复制延迟导致跨机房发布不一致

A 机房 push 后立即触发 B 机房部署，但复制是异步的，B 的 Harbor 还没收到镜像，k8s 拉取 404。

**修复**：
- 发布流水线在 push 后 poll B 的 Harbor API，确认 manifest 存在再触发 B 的部署。
- 复制策略从"实时"改为"实时 + 每 10 分钟全量对账"，防 event 丢失。
- 关键镜像用 `--wait` 同步复制（Harbor 2.8+ 支持同步等待）。

### 坑 5：Trivy 离线扫描库过期

某次机房断网，Trivy CVE 库 7 天没更新，扫描结果全是"无漏洞"——其实是库老了。我们以为镜像很安全，实际新 CVE 都没识别。
**修复**：Trivy 加 health check，DB 修改时间超 48 小时告警；离线环境手动导 CVE 库 tar 包定期更新。

### 坑 6：retention 规则误删生产 tag

CI 自动清理"最近 30 个 tag"的策略上线第一天，把一个打了 `prod-stable` 标签的镜像删了——因为 retention 规则按 push 时间排序，没考虑保留特定 label。
**修复**：retention 规则加 `retain_tagged: true`，凡是有 `prod-*` 前缀的 tag 永不删；重要镜像走签名通道（签名镜像默认受保护）。这条规则上线前先在测试项目跑 dry-run 一周。

### 坑 7：k8s 拉镜像偶发 401

集群偶发 `401 Unauthorized`，重启 pod 又能拉。查了半天是 Core 的 token 服务有缓存，多副本间 session 不同步。Core 副本 A 签的 token，副本 B 校验时偶发失败。
**修复**：Core 副本数固定为奇数（3），session 共享 Redis；token 签名 key 强制多副本一致（`secret_key` 配置），别用默认随机生成。

## 写在最后

Harbor 看起来是"装上就能用"的镜像仓库，真正在生产里跑稳，要处理的是配额、GC、证书、复制、漏扫这一整套运维闭环。我们的经验是：**先把配额和 retention 做掉，再排 GC 周期，最后才是漏扫和签名**——顺序反了，磁盘先爆。希望这篇复盘能给正在选型或已经踩坑的同学一个对照清单，少走几个月弯路。
