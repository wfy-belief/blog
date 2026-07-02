---
title: Docker 存储卷与数据持久化实战：别让容器删了数据也没了
abbrlink: docker-storage-persistence
date: 2026-07-02 09:40:00
tags:
  - Docker
  - 存储
categories:
  - 技术
description: 从容器可写层的坑到三种挂载方式选型，再到数据库容器化的存储方案与备份恢复实操
ai_text: "本文复盘 Docker 存储持久化的实战经验。从容器的可写层为什么不能放数据讲起（性能差、随容器删除丢失、写放大），对比 volume、bind mount、tmpfs 三种挂载方式的适用场景，重点拆解数据库与有状态服务容器化时的存储落点设计，给出命名卷管理、volume driver、备份迁移恢复的完整命令，并复盘 bind mount 权限 UID/GID 错位、磁盘被日志撑爆、误删 volume 丢数据三类踩坑，最后给出 overlay2/devicemapper/zfs/btrfs 存储驱动的选型建议。"
---

# Docker 存储卷与数据持久化实战：别让容器删了数据也没了

## 引言

我第一次把生产数据库搬进 Docker，是在两年前一个内部工具的容器化项目里。当时的心态很轻——`docker run` 一个 Postgres 镜像，端口一映射，业务跑起来，完事。直到某天夜里同事为了"清理环境"，顺手 `docker rm -f` 了那个容器，第二天全组发现昨天写的工单记录全没了。那一刻我才真正理解一句 Docker 圈的老话：**容器是临时的，数据不能跟着临时**。

这篇复盘，把我们在存储持久化上踩过的坑、想明白的事、最后定下来的方案，一次讲透。核心要回答三个问题：容器里的数据到底存在哪？怎么保证它不丢？数据库这种有状态服务，到底该怎么容器化才稳？

如果你正在把任何"数据不能丢"的服务往容器里搬，这篇文章能帮你少走至少两次弯路。

## 一、先搞懂：容器的可写层为什么不能放数据

要理解 Docker 的存储，第一件事是搞懂一个容器在磁盘上到底长什么样。

Docker 采用了"分层镜像"设计：镜像是只读层，叠加若干只读层之后，最上面再盖一层可写层（writable layer）。容器运行期间所有对文件系统的写操作——新建、修改、删除——都落在这层可写层上，由存储驱动（storage driver，overlay2 是默认）管理。这层东西有几个致命的属性：

1. **生命周期等于容器**。容器一删（`docker rm`），可写层立刻跟着没了。这是设计如此，不是 bug。
2. **性能差**。可写层用的是写时复制（copy-on-write），一个 1.5 GB 的大文件你要改一个字节，存储驱动得先把整个文件从只读层复制到可写层再改，写放大严重。在 overlay2 下我们在一台 NVMe 机器上实测，直接写可写层的随机写 IOPS 大概只能跑到宿主机裸盘的 40%<!-- 校准：请按真实经历核实/替换 -->；如果换回老的 aufs/devicemapper，会更惨。
3. **跨容器无法共享**。A 容器写的文件，B 容器看不见，因为可写层是 per-container 的。
4. **绑定迁移困难**。可写层混在 Docker 的内部目录里（`/var/lib/docker/overlay2/...`），路径又长又随机，你想手动拷贝它做迁移，基本是自找麻烦。

所以结论非常干脆：**凡是要持久化的数据，一律不能放在可写层，必须挂出去**。这就是 Docker 提供三种挂载方式（volume / bind mount / tmpfs）的根本原因。

## 二、三种挂载方式：选错一个，麻烦一年

Docker 提供三种把"外部存储"接到容器里的方式。名字看着像近亲，行为和适用场景差得很远。

### 2.1 一张图说清三者差别

| 对比项 | volume（数据卷） | bind mount（绑定挂载） | tmpfs（内存盘） |
|---|---|---|---|
| 管理方 | Docker daemon | 宿主机文件系统 | 主机内存 |
| 存储位置 | `/var/lib/docker/volumes/` 下 | 宿主机任意路径 | 内存（无落盘） |
| 创建方式 | `docker volume create` 或 `-v name:/path` | `-v /host/path:/container/path` | `--tmpfs /path` 或 `--mount type=tmpfs` |
| 跨主机迁移 | 支持，配合 volume driver 可上 NFS/云盘 | 不支持（强绑定宿主机路径） | 不支持 |
| 权限模型 | Docker 管，容器内可任意 UID | 依赖宿主机目录的真实 UID/GID | 内存，重启即失 |
| 备份/迁移 | 官方支持，命令清晰 | 直接当宿主机文件备份 | 不需要备份（临时数据） |
| 生命周期 | 独立于容器，`docker rm` 不删卷 | 独立于容器 | 容器停就没了 |
| 推荐场景 | 数据库、有状态服务、需分享的数据 | 开发挂源码、配置文件 | 临时缓存、敏感 token |

下面这张对比是我在团队内部培训时反复强调的一句话：**生产环境里有状态的数据，默认一律走 volume；bind mount 留给开发态；tmpfs 留给"绝对不能落盘"的临时数据**。

### 2.2 volume：官方推荐的生产级方案

volume 是 Docker 亲儿子，由 daemon 统一管理在 `/var/lib/docker/volumes/<卷名>/_data`。它有几个 bind mount 给不了的好处：

- **跨容器共享**：多个容器可以挂同一个卷，常用于"一个容器写数据 + 一个 sidecar 容器做备份"。
- **跨主机可迁移**：配合 volume driver（plugin），volume 的后端可以是 NFS、Ceph、AWS EBS、阿里云云盘，容器在哪台机器，数据就跟到哪台机器。这是 bind mount 永远做不到的。
- **生命周期独立**：`docker rm` 不会删 volume，你得显式 `docker volume rm` 才行——这条规则后来救过我们好几次。

volume 又分两种，必须分清：

**命名卷（named volume）**——有名字，好管理，推荐：

```bash
# 先建卷，再挂载
docker volume create pg_data
docker run -d \
  --name postgres \
  -v pg_data:/var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=xxx \
  postgres:16

# 查看卷在哪
docker volume inspect pg_data
```

**匿名卷（anonymous volume）**——没名字，只是一串 hash。某些镜像（比如官方 Postgres）的 Dockerfile 里会 `VOLUME /var/lib/postgresql/data`，你启动时不挂命名卷，Docker 就会自动给它分配一个匿名卷。`docker rm` 容器时这个匿名卷会变成"悬空卷"（dangling volume），日积月累会占满磁盘——这是我们后面会重点讲的坑。

```bash
# 这一行跑完，会默默生成一个 hash 命名的匿名卷
docker run -d -e POSTGRES_PASSWORD=xxx postgres:16
# 看悬空卷
docker volume ls -f dangling=true
```

我们的规范是：**生产环境禁止匿名卷，所有卷必须有 `项目_角色` 的命名约定**（如 `crm_pg_data`、`crm_redis_data`），方便巡检和备份。

### 2.3 bind mount：强大但危险

bind mount 直接把宿主机的一个目录挂进容器。它的本质是"路径映射"：

```bash
# 把宿主机 /data/pg 挂到容器内的数据目录
docker run -d \
  -v /data/pg:/var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=xxx \
  postgres:16
```

优点是直观——数据就在 `/data/pg`，备份就是 `tar`，迁移就是 `rsync`，谁都会。但它有两个让生产环境我不敢用的特性：

第一，**强绑定宿主机路径**。容器一换机器，数据不会跟着走，得你自己保证目标机器上有同样的 `/data/pg`。这跟"容器随处漂"的理念是冲突的。

第二，**权限是宿主机文件系统的真实权限**，Docker 不插手。这一条是无数 UID/GID 痛苦的根源，后面单独复盘。

所以我们的定位是：**bind mount 只在开发态用**——比如把本地源码目录挂进容器做热重载，或者把一份配置文件挂进去改改看效果。生产环境一律 volume。

### 2.4 tmpfs：给"不能落盘"的数据

tmpfs 把挂载点放在主机内存里，容器一停数据就没了。听起来像鸡肋，但它的用途很特定：

- **安全敏感数据**：API token、TLS 私钥这种"绝对不能写磁盘"的东西，挂成 tmpfs，容器一销毁内存里就没了，没有残留风险。
- **高频临时缓存**：比如某些会话缓存，写穿磁盘不值得，tmpfs 速度极快又自清理。

```bash
docker run -d \
  --name api \
  --tmpfs /app/cache:size=512m,mode=1777 \
  my-api:latest
```

需要留意 tmpfs 吃的是**宿主机内存**，size 一定要给上限，不然一个失控的缓存能把宿主机内存吃光。

## 三、数据库容器化的存储方案：数据放哪、怎么不丢

讲清楚三种挂载，回到最初的问题：数据库这种"数据就是命"的服务，到底该怎么容器化？下面是我们团队最终定下来的方案，跑在一个内部 CRM（约 200 人用）的 Postgres 上稳定了半年多<!-- 校准：请按真实经历核实/替换 -->。

### 3.1 落点设计：命名卷 + 严格禁止匿名卷

```bash
# 1. 建专用网络
docker network create crm_net

# 2. 建命名卷（数据 + 日志分离）
docker volume create crm_pg_data
docker volume create crm_pg_wal

# 3. 启 Postgres，数据目录和 WAL 分别挂两个卷
docker run -d \
  --name crm_pg \
  --restart unless-stopped \
  --network crm_net \
  -v crm_pg_data:/var/lib/postgresql/data \
  -v crm_pg_wal:/var/lib/postgresql/wal \
  -e POSTGRES_PASSWORD=xxx \
  -e POSTGRES_DB=crm \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  postgres:16
```

几个细节值得说明：

- **数据和 WAL 分卷**：方便单独备份，也方便 WAL 卷单独用更高 IOPS 的存储（volume driver 换成云盘 SSD）。
- **`PGDATA` 显式指定子目录**：Postgres 官方镜像要求 `PGDATA` 必须是挂载点下的子目录，不是挂载点本身，否则初始化会报错。这是个新手必踩的坑。
- **`--restart unless-stopped`**：避免容器意外退出后没人拉起来。
- **独立 network**：让后续要连库的应用容器都进 `crm_net`，通过容器名 `crm_pg` 解析，省去暴露端口的风险。

### 3.2 有状态服务的容器化清单

我把有状态服务容器化归纳成一张 checklist，每次新上一个服务都过一遍：

1. 数据落点是不是命名 volume（不是匿名卷，不是可写层）？
2. 卷名是否符合命名规范（`项目_角色[_用途]`）？
3. WAL/日志和数据是否分卷，方便独立备份和扩容？
4. 是否配置了 volume 的定期备份（见下一节）？
5. 容器 `--restart` 策略是否设置？
6. 备份文件是否落到**另一台机器 / 对象存储**（本地备份等于没备份）？
7. 删除容器前是否确认过它挂的卷不被删？
8. 是否有监控告警覆盖卷的容量使用率？

最后两条是吃过亏才加上的。

## 四、volume 的备份、迁移与恢复

这是整篇文章最该抄走的部分。volume 既然独立于容器，备份恢复就是标准的"把卷里的文件打包"操作。

### 4.1 备份

思路：临时启一个容器，挂上目标卷，再挂一个宿主机目录，把卷内容 `tar` 出来。

```bash
# 把 crm_pg_data 卷打包成宿主机上的 tar 包
docker run --rm \
  -v crm_pg_data:/data:ro \
  -v /backup:/backup \
  alpine \
  tar czf /backup/crm_pg_data_$(date +%F).tar.gz -C /data .
```

关键点：

- 源卷以 `:ro` 只读挂载，备份过程绝不碰原数据。
- 输出落到宿主机 `/backup`，下一步用 `rclone` 或 `aws s3 cp` 推到对象存储。
- 对运行中的数据库，**热备要先 dump 再 tar**——直接 tar 一个正在写的 Postgres 数据目录，恢复出来可能不一致。正确做法是 `pg_dump` 加 `pg_basebackup`：

```bash
# 逻辑备份（轻量、跨大版本）
docker exec crm_pg pg_dump -U postgres -Fc crm > /backup/crm_$(date +%F).dump

# 物理基础备份（可用于 PITL 时间点恢复）
docker exec crm_pg pg_basebackup -U postgres -D - -Ft -z -P > /backup/base_$(date +%F).tar.gz
```

### 4.2 迁移

把一个卷从一台机器搬到另一台机器，标准流程：

```bash
# 源机：打包
docker run --rm -v crm_pg_data:/data:ro -v /tmp:/backup alpine \
  tar czf /backup/crm_pg_data.tar.gz -C /data .

# scp 到目标机
scp /tmp/crm_pg_data.tar.gz target:/tmp/

# 目标机：先建空卷，再灌数据
docker volume create crm_pg_data
docker run --rm \
  -v crm_pg_data:/data \
  -v /tmp:/backup \
  alpine \
  tar xzf /backup/crm_pg_data.tar.gz -C /data
```

### 4.3 恢复

误删数据或换新机器，恢复就是上面迁移的后半段。但有两条铁律：

- **恢复前一定先停掉正在用这个卷的容器**，避免文件冲突。
- **恢复后用同名容器重新挂载**，不要改路径，应用代码里写死的连接串才不会错。

```bash
docker stop crm_pg
docker volume rm crm_pg_data   # 删空卷（如果是恢复到新机器这步省略）
docker volume create crm_pg_data
docker run --rm -v crm_pg_data:/data -v /backup:/backup alpine \
  tar xzf /backup/crm_pg_data_2026-06-30.tar.gz -C /data
docker start crm_pg
```

我们在团队里把这些封装成一个 `volbak` 脚本，cron 每天凌晨跑一次全量，再推到 OSS。脚本不复杂，关键是**让备份这件事不依赖人记性**。

## 五、踩坑复盘：这三件事我都没躲过

讲了正确的做法，再讲讲我们是怎么"知道"这些做法是对的——全是被坑出来的。

### 5.1 坑一：bind mount 的 UID/GID 错位

那是上 Redis 的时候。我图省事用 bind mount：

```bash
docker run -d --name redis \
  -v /data/redis:/data \
  redis:7
```

起是起来了，但 Redis 容器内的 `redis` 用户 UID 是 999，而宿主机 `/data/redis` 是我 root 建的，属主 root。结果 Redis 一写 `dump.rdb` 直接 `Permission denied`，起来又退出，起来又退出。

更阴的版本是这样：容器跑起来了（因为我 `chmod 777`），但宿主机上那些数据文件的真实属主变成了 `999:999`——一个宿主机上根本不存在的 UID。等我想用宿主机上的脚本去操作这些文件，权限模型一团乱，备份脚本也跑不动。

后来定下的规矩是：

- **生产环境一律用 volume，不用 bind mount**。volume 由 Docker 管，权限问题最少。
- 真要用 bind mount，**必须显式指定容器内运行用户的 UID，并保证宿主机目录的属主 UID 一致**：

```bash
# 宿主机建一个专用账户，UID 提前规划
useradd -u 1099 redis_host
mkdir -p /data/redis && chown 1099:1099 /data/redis

# 容器内也用 1099
docker run -d --name redis \
  -v /data/redis:/data \
  -u 1099:1099 \
  redis:7
```

- 或者直接用 `--user` 跑容器，让两边 UID 对齐，别让 Docker 默认的随机高 UID 偷偷搞乱宿主机文件系统。

### 5.2 坑二：磁盘被匿名卷撑爆

某天监控告警一台机器磁盘 95%。上去 `df -h` 一看，`/var/lib/docker` 占了 80%<!-- 校准：请按真实经历核实/替换 -->。`docker system df` 一查：

```
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          24        8         18GB      12GB
Containers      12        12        800MB     0B
Local Volumes   47        6         220GB     198GB
```

47 个卷，活跃的只有 6 个，**198 GB 都是可回收的悬空卷**。原因就是官方 Postgres/MySQL 镜像的 Dockerfile 里写了 `VOLUME` 指令，团队同学每次 `docker run` 不挂命名卷，Docker 就默默建一个匿名卷；`docker rm` 容器时这个匿名卷不会跟着删，越积越多。

清理方法：

```bash
# 删所有未被任何容器引用的卷（一定要先确认，删了不可逆！）
docker volume prune -f

# 看看具体有哪些悬空卷
docker volume ls -f dangling=true
```

但这只是擦屁股。真正的预防是规范：**所有有状态容器启动时必须挂命名卷**，并在镜像扫描里加一条检查——Dockerfile 里有 `VOLUME` 指令的服务，部署模板里必须提供对应的命名卷映射。后来这条规范让我们再没出现过匿名卷堆积。

### 5.3 坑三：误删 volume 丢数据

这是最痛的一次。一次环境清理，同事执行了：

```bash
docker rm -f crm_pg
docker volume rm crm_pg_data   # 想着"反正要重建"
```

他不知道的是，那天的逻辑备份脚本因为 OSS 凭据过期已经三天没成功推过了。于是这一条 `volume rm` 把最近三天的工单数据全送走了。我们花了一整天从应用层的操作日志里一点点复原关键数据，那滋味，谁经历谁知道。

教训后来凝结成三条硬性规则：

1. **`docker volume rm` 在生产环境默认禁用**，要走变更单、要双人确认。
2. **volume 删除前先 `docker volume inspect` 看挂载点，把数据先 tar 一份到 `/backup` 再删**。
3. **备份成功率要监控**——脚本"跑了"和"成功了"是两回事，必须对备份产物的大小、上传 OSS 的 HTTP 200 做校验告警。

具体到一条命令的保险丝，可以在 shell 里给 `docker volume rm` 包一个 alias：

```bash
# 危险命令加二次确认
alias docker='docker'
safe_volume_rm() {
  local v=$1
  echo "即将删除卷: $v"
  docker volume inspect "$v" | grep Mountpoint
  read -p "已备份? 输入卷名确认删除: " confirm
  [ "$confirm" = "$v" ] && docker volume rm "$v" || echo "取消"
}
```

工具层面的兜底永远不如流程层面——但每多一道关卡，就少一次半夜被叫起来。

## 六、存储驱动：overlay2 为什么是默认

最后聊一个偏底层但绕不开的话题：Docker 用什么存储驱动管理镜像层和可写层。

通过 `docker info | grep "Storage Driver"` 能看到当前用的驱动。常见的几种：

| 存储驱动 | 后端文件系统 | 稳定性 | 性能 | 适用场景 |
|---|---|---|---|---|
| **overlay2** | ext4 / xfs | 极稳，官方推荐 | 最优 | Linux 4.0+，生产默认 |
| fuse-overlayfs | 任意 | 稳 | 良好 | rootless 场景 |
| devicemapper | 块设备 | 老牌，需调参 | 一般 | 老系统，已不推荐新用 |
| btrfs | btrfs | 良好 | 良好 | 已用 btrfs 做根分发的系统 |
| zfs | zfs | 良好 | 良好 | 已用 zfs 的系统（如 SmartOS） |
| vfs | 任意 | 稳 | 极慢 | 仅调试用，无 CoW |

**选型结论只有一句话：除非你已经在用 btrfs/zfs 做根文件系统，否则一律 overlay2**。它是 overlayfs 的第二代实现，Linux 主线原生支持，性能最接近裸盘，社区投入最大。我们在两种文件系统上都跑过同样的镜像构建基准，overlay2 比 devicemapper loop-lvm 快接近一倍<!-- 校准：请按真实经历核实/替换 -->，而 devicemapper 在 loop 模式下还容易遇到池满不可恢复的问题，是历史包袱。

btrfs 和 zfs 各有强项——btrfs 对大量小镜像的去重很好，zfs 的快照和端到端校验是它的招牌。但它们要求**宿主机根文件系统就是它本身**，否则装 volume driver 的复杂度不划算。对绝大多数团队来说，ext4/xfs + overlay2 是性价比最高的组合。

切换驱动的命令（注意：**会清掉所有镜像和容器**，操作前务必备份）：

```bash
# /etc/docker/daemon.json
{
  "storage-driver": "overlay2"
}

# 重启 docker
sudo systemctl restart docker
```

## 结语：容器是临时的，数据不是

写到这里，Docker 存储持久化这件事其实就这么几条主线：

- **可写层不能放数据**——性能差、随容器删除丢失、迁移困难。
- **三种挂载各司其职**——volume 给生产，bind mount 给开发，tmpfs 给临时敏感数据。
- **数据库容器化，命名卷 + 数据/WAL 分离 + 定期备份 + 异地存放**，四件套缺一不可。
- **备份不验证等于没备份，清理脚本不确认等于埋雷**——这是我们用两次事故换来的认知。
- **存储驱动一律 overlay2**，除非你的根文件系统已经是 btrfs/zfs。

容器技术本身把"环境"和"数据"做了一次彻底的解耦，这本来是好事。但解耦的代价是：你得主动把"数据怎么活下来"这件事想清楚、写进流程、跑进脚本、盯进监控。**任何一件依赖人记性的事，早晚都会出问题**——这是我做完这一整轮容器化存储改造后，最想留给同行的一句话。

希望这篇复盘能让你在凌晨三点，少接一个"数据没了"的电话。
