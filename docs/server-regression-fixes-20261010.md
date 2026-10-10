# 服务器缺陷修复交付与待审批部署方案

参考 master：`dc4c8965ad6b10bdb0d599ff50546b60f3a5861e`。
本地分支：`fix/server-regressions-20261010`。本次仅修复三个缺陷；没有 CRM 字典配置化改动。

开始前已阅读 `docs/server-deployment.md`，按其目录、Compose 文件、日志及更新流程进行只读定位。原有未提交 Dockerfile、README、忽略规则、服务器配置与部署脚本均保留。本次修改未部署，也未执行生产迁移、历史任务重放、真实 CRM 调用或企业微信测试发送。

## 三份问题结论

1. [重复线索覆盖、卡片 ACK 与远端未知结果](incidents/20261010-crm-duplicate.md)。Boolean 响应解析缺陷已复现和修正；当次真实 CRM 结果尚需管理员只读对账。
2. [图片扫描状态、官方语音正文与恢复清单](incidents/20261010-media.md)。含三个业务历史工件及无 Outbox 残留的处理边界。
3. [20:00 调度、销售筛选与通知状态](incidents/20261010-daily-reminder.md)。Beat 正常触发，旧数据库映射条件排除了所有销售；同时移除与 PR #74 不一致的残留授权门槛。普通业务继续依据成员启用状态，不恢复销售授权要求。

## 验证

测试镜像 `crm-tool-server-fix-test:local` 以本地已有依赖镜像为基础，只复制 app、workers、tests、scripts、Alembic 与项目配置；构建使用 `--network none`。没有复制或注入真实环境文件、员工目录及机器人凭据。单元测试容器也使用 `--network none`，项目统一测试 fixture 禁用 dotenv，并使用假外部适配器。

最终验证：全量单元测试 **921 passed**，其中每日提醒回归 15 项，覆盖取消旧授权门槛后的排程与发送；CRM 专项另已验证 145 passed；隔离 PostgreSQL 集成 **1 passed**（同时覆盖三个问题，并使用历史授权标记 false 的启用成员测试提醒）。Ruff `check app workers tests` 和 `git diff --check` 通过。16 个现有依赖弃用警告来自 websockets 和 Alembic 配置，不影响断言；本次未扩大范围修改依赖。

隔离 PostgreSQL 使用单独随机 Compose project、随机数据库/用户/密码、internal 网络、tmpfs PGDATA，没有宿主机端口或生产卷。迁移前调用既有 `_validate_worker_environment` 并只读核验数据库名、用户及 PGDATA，禁止 `.env`；随后仅在该一次性库升级至 `0034_lead_system_defaults` 并执行 `tests/integration/test_server_regressions_postgres.py`。并发确认、过期覆盖租约、通知唯一性及媒体流终态验证通过；测试结束已清理其容器和网络。

单元复核命令（只使用本地测试镜像）：

```powershell
docker run --rm --network none -e APP_ENV=test -e LLM_PROVIDER=mock --entrypoint python crm-tool-server-fix-test:local -m pytest -p no:cacheprovider -o addopts= tests/unit -q
docker run --rm --network none --entrypoint ruff crm-tool-server-fix-test:local check app workers tests
git diff --check
```

## 当前服务器环境

只读核实 `APP_ENV=development`、`MEDIA_SCANNER_PROVIDER=noop`；这表示当前并未执行真实文件安全扫描。生产要求实际扫描时应先安装、配置并验证真实 scanner，不能仅改 APP_ENV 或把 not_required 改成 clean。该基础设施配置不在本次代码修复中。

app、worker、wecom-bot、PostgreSQL、Redis 当前健康；scheduler 进程运行、20:00 调度已有执行证据，但当前健康探针超过 3 秒而 unhealthy。服务器数据库为 `0034_lead_system_defaults`，本次没有新增或修改 Alembic 迁移、表或列。

## 待审批发布步骤

部署依据仍为 `docs/server-deployment.md` 及已有 `scripts/server-update.sh`，服务器目录 `/opt/CRM_Tool`，统一使用 `docker compose -f docker-compose.yml -f docker-compose.server.yml`。PostgreSQL 保留宿主机 `/opt/CRM_TOOL/DATA/postgresql/data`，Redis/媒体保留原卷。

1. 审核本次 app/tests/docs 差异及三份恢复边界。制作仅含已审修复和既有服务器发布资产的干净发布目录；当前同步脚本会打包整个脏工作区，原有未提交改动需要单独核对发布清单。
2. 记录当前镜像 ID、Compose 状态、数据库版本及旧代码备份。沿用更新脚本先校验 Compose、构建 app/worker/scheduler/wecom-bot/migrate 镜像，再停止业务服务的顺序。
3. 消费者停止后备份数据库、媒体及 `.env`，存于服务器私有 `backups/`，并校验备份可读及完整性；配置和备份内容不进入仓库或公开报告。
4. 本次没有数据库迁移需求。脚本仍会执行既有 migrate 服务；发布时先确认服务器和镜像 migration head 同为 0034，不能引入未审迁移。
5. **代码部署与历史恢复分别审批。** 原更新脚本会自动恢复 worker、scheduler、wecom-bot，新代码可能自动消费历史 pending 图片。因此仅批准代码发布时，按同一脚本已审步骤停在消费者启动前，只恢复 app 进行就绪和控制台检查；worker、scheduler、wecom-bot 保持停止。历史任务受审计冻结或逐项恢复方案获批前不恢复消费，期间销售接入暂停。不能直接执行脚本的自动全量恢复段。
6. 获批历史任务处置后，按清单核对原始工件、归属、人工修改、当前后续消息及远端结果，再恢复必要服务。核查 callback ACK、CRM 分类审计、媒体任务和上海当日提醒记录。日志收集按部署文件执行 `systemctl restart crm-tool-logs.service`。

## 回滚

发布前保留旧镜像、旧代码和配置。若验证失败，停止 bot/scheduler/worker，恢复原发布代码与配置，使用原 Compose 文件恢复旧镜像；再次核对日志收集和健康检查。本次 schema 不变，代码回滚通常无需恢复数据库。已经产生的审计、通知和确认事实应保留，恢复旧程序前仍须冻结未知 CRM update，防止旧逻辑再次覆盖。

数据库或媒体恢复须另选明确恢复点并获得批准，避免覆盖发布后的业务数据；不能直接切回已经停止接收写入的旧 PostgreSQL 卷。回滚代码不会撤销已经成功的远端 CRM 写入，任何远端对账或恢复继续逐项审批。

本方案目前仅为本地交付文档，服务器更新、配置修改、服务重启及历史处置均未执行。
