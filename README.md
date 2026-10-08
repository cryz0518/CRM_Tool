# CRM 线索自动录入

销售通过企业微信机器人提交客户信息，系统识别并归并线索，保存后台事实并增量同步到企业微信智能表格，供销售审核后提交 CRM。

## 环境与安全边界

- 默认 Compose 配置面向本地开发/测试，不是自动化生产发布方案；仓库没有一键生产部署脚本。
- `.env.example` 仅供本地开发参考，包含 mock 或未配置的适配器默认值，不能直接作为生产配置。真实凭据应由受控的密钥注入方式提供，不能提交到 Git。
- Compose 使用固定 PostgreSQL 卷 `crm_t21_final_postgres_data`，不同 Compose 项目也可能指向同一卷。启动前必须确认 Docker context、卷归属和数据用途；仅查看卷名不足以证明它是测试数据。
- `docker compose up` 会按依赖启动 `migrate`，执行 `alembic upgrade head`；随后启动的 Worker 可能立即消费积压任务。不要在未知或生产数据上试跑启动命令。
- `docker compose down` 会保留命名卷。禁止对重要数据运行 `docker compose down -v`、`docker volume rm` 或带卷删除的 prune。
- 当前 Compose 将应用数据库地址指向 Compose 内的 `postgres` 服务。连接外部 PostgreSQL 需要经过审查的 Compose/配置变更，不能只改 `.env` 中的 `DATABASE_URL`。

## 本地启动

需要安装 Docker Desktop（含 Docker Compose v2）。首次配置：

```powershell
Copy-Item .env.example .env
# 按本地环境编辑 .env；保留 mock/unconfigured 默认值用于离线开发
docker context show
docker compose config -q
docker volume inspect crm_t21_final_postgres_data
```

先确认当前 Docker context 和固定卷的归属。无法证明卷是可用于本地开发的数据时，停止，不要启动服务。

启动不含企业微信机器人的本地核心服务：

```powershell
docker compose up -d --build postgres redis app worker scheduler
docker compose ps
docker compose logs --tail=100 app worker scheduler
```

这会创建或使用 PostgreSQL 持久卷、运行迁移，并启动可能消费队列的 Worker。需要机器人时，先为本地测试环境配置 `WECOM_BOT_ID` 和 `WECOM_BOT_SECRET`，再启动完整服务：

```powershell
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 wecom-bot
```

应用存活检查为 <http://localhost:8000/health>，就绪检查为 <http://localhost:8000/health/ready>。机器人未配置凭据时不要启动 `wecom-bot`。

本地 CRM 代理仅用于隔离测试；如需使用，按对应测试说明配置后运行：

```powershell
docker compose -f docker-compose.yml -f docker-compose.local-proxy.yml up -d --build
```

不要在生产环境使用该代理 Compose 文件。

## 常用操作

```powershell
docker compose ps
docker compose logs -f app worker scheduler wecom-bot
docker compose stop app worker scheduler wecom-bot
docker compose down
```

`down` 不删除数据库和媒体命名卷。只有在确认数据可丢弃、目标卷身份明确且获准清理时，才可按专门的数据清理流程处理卷；不要将 `down -v` 当作常规停止命令。

读取运行配置与依赖状态（不执行迁移）：

```powershell
docker compose run --rm --no-deps app python -m app.production_verify --mode static --json
docker compose run --rm --no-deps app python -m app.production_verify --mode runtime --json
docker compose run --rm --no-deps app python -m app.production_verify --mode all --json
```

测试和静态检查应只对隔离的开发/测试数据库运行：

```powershell
docker compose run --rm app pytest
docker compose run --rm app ruff check .
docker compose run --rm app mypy app workers
```

也可使用 Makefile 快捷命令：`make up`、`make down`、`make logs`、`make test`、`make lint`、`make typecheck`。`make migrate` 会写入数据库并升级 Alembic revision，只能在确认目标数据库及变更窗口后使用。

本地或测试环境可显式授权销售：

```powershell
docker compose run --rm --no-deps app python -m app.manage_sales authorize --wecom-user-id <企业微信用户标识>
```

## 本地代码更新

先保存需要保留的工作区改动，并确认当前 Compose 使用的是本地隔离数据。以下命令会更新代码并重建/启动服务；启动依赖可能运行数据库迁移，Worker 可能消费积压任务：

```powershell
git status --short
git fetch origin
git switch master
git pull --ff-only
git rev-parse HEAD
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 app worker scheduler
```

如果只需重启现有容器进程（不重建镜像、不读取新的环境变量）：

```powershell
docker compose restart app worker scheduler
```

修改镜像代码或依赖后使用 `up -d --build` 重建；修改环境变量后需按服务配置重建容器并核对生效值，避免把密钥打印到日志或终端。上述快捷更新流程仅供已确认隔离的本地环境，不是生产更新流程。

## 首次部署与生产更新

仓库没有自动生产发布流程。生产目标、主机/Docker context、凭据注入、备份和恢复程序必须由负责人员事先确定；如果无法确认这些信息，不要选择本机或任意服务器代替生产目标。

生产变更应由获批的发布流程执行，至少按以下顺序准备和操作：

1. 确认目标服务器、Docker context、Compose 项目、固定 PostgreSQL 卷和媒体卷；记录当前代码 commit 与镜像标识。不能仅凭卷名判断数据归属。
2. 从干净检出中固定到已审查的 master commit；核对 `git status`、目标 SHA 和最终镜像构建上下文，排除本地未提交 Dockerfile、SOP 或其他文件。
3. 在维护窗口前取得数据库及媒体数据备份，并验证备份可恢复。确认消息处理检查点、远端同步幂等性及当前任务/租约状态；未完成或失败待处理消息要有明确处置计划。
4. 暂停入口和消费者，避免迁移期间新旧版本并行处理：

   ```powershell
   docker compose stop wecom-bot worker scheduler app
   ```

5. 确认 PostgreSQL 与 Redis 正在目标 Compose 项目中运行且健康。首次部署时只启动基础依赖；这一步会创建或使用 Compose 配置的持久卷：

   ```powershell
   docker compose up -d postgres redis
   docker compose ps
   ```

6. 在已确认的目标环境构建指定版本镜像，并有意执行一次数据库迁移。迁移会写数据库；需要审批、备份和变更窗口：

   ```powershell
   docker compose build app worker scheduler wecom-bot migrate
   docker compose run --rm --no-deps migrate
   docker compose run --rm --no-deps app python -m app.production_verify --mode static --json
   ```

7. 确认迁移结果及静态检查后，分阶段启动服务；此处 `--no-deps` 用于避免隐式启动依赖服务或重复触发迁移：

   ```powershell
   docker compose up -d --no-deps app
   docker compose ps
   docker compose logs --tail=100 app
   # 确认应用就绪后，按批准的顺序恢复后台任务处理
   docker compose up -d --no-deps worker scheduler
   docker compose run --rm --no-deps app python -m app.production_verify --mode all --json
   # 最后恢复企业微信入口；启动后可能接收新消息
   docker compose up -d --no-deps wecom-bot
   ```

8. 观察 readiness、服务健康、错误日志、消息归属、待归属消息、失败待处理任务及外部同步状态。未经单独授权，不发送真实测试销售消息，不创建生产测试线索，不执行远端业务写入。

如果应用版本回滚，先确认旧代码兼容已升级的数据库 schema；不得自动执行 Alembic downgrade，也不得用数据库回滚覆盖已发生的业务事实。数据库恢复是单独审批的恢复操作，应使用已验证备份并评估恢复点之后的数据影响。

## 企业微信机器人 CRM 提交命令

- `提交今天的线索`：提交当前销售当天仍为“未提交”的待创建线索。
- `提交我所有线索`：机器人列出当前销售所有系统状态为“未提交”的待创建线索，销售在卡片中勾选一条或多条后提交；销售不能手动修改提交状态。
- `帮我提交放弃提交的线索`：机器人列出当前销售由系统标记为“放弃提交”的线索，销售勾选后重新执行查重和提交流程。
- `提交我的更新`：提交已同步线索的实际字段变更。

`.env` 仅用于本地配置，禁止提交真实密钥。
