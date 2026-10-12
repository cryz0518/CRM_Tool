# 服务器部署与更新

部署目标为 `root@8.153.164.200:/opt/CRM_Tool`，以 Docker Compose 运行。
沿用原有业务配置；`.env` 只修改以下两处宿主机路径：

```dotenv
WECOM_CLI_CREDENTIALS_PATH=/opt/CRM_Tool/.wecom-cli-credentials
EMPLOYEE_DIRECTORY_HOST_PATH=/opt/CRM_Tool/employee.csv
```

本机原配置保存在 `.env.local`，禁止提交任何真实环境文件或凭据。
容器内的 `/app/employee.csv`、`/home/appuser/.config/wecom`、
`/var/lib/crm-lead/media` 是挂载目标，不能替换成宿主机路径。
PostgreSQL 使用宿主机固定目录 `/opt/CRM_TOOL/DATA/postgresql/data`，
挂载到容器 `/var/lib/postgresql/data`；`CRM_TOOL` 大小写按指定路径保留，与代码目录 `CRM_Tool` 不同。
该挂载在服务器补充 Compose 文件中固定，本地默认 `docker-compose.yml` 仍使用原命名卷；
以后同步本地代码覆盖服务器基础配置时，服务器补充配置会继续保留固定目录挂载。
Redis 队列和媒体数据继续保存在服务器 Docker 持久卷。代码同步不覆盖数据库目录。
切换挂载时已停库复制现有数据并核对；原卷 `crm_t21_final_postgres_data` 保留作为回退副本，
它不再接收新写入，不能直接切回以免丢失切换后的数据。

服务器补充 Compose 配置将 CRM 域名 `sopapi-test.cgxi.net` 映射到原本地代理使用的
内网地址 `192.168.3.108`。服务器已验证可直连并完成签名查重，业务 URL 和密钥保持原样。
公网 DNS 对应入口返回南墙 WAF 403，因此浏览器访问公网域名不代表容器内访问结果。
若 CRM 内网地址变更，需要同步修改 `docker-compose.server.yml` 的映射并更新容器。

迁移时已核对全部 33 张数据库表的行数，包含 95 条线索、249 条消息和 23 个销售授权。
智能表格中重复且未被使用的“技术支持工单”选项已按授权移除一个，
56 条现有记录的沟通方式值前后完全一致；字段与记录快照保存在服务器私有 `backups/`。

## 从本地同步更新

在本地 PowerShell 执行：

```powershell
cd D:\code\CRM_Tool
powershell -ExecutionPolicy Bypass -File .\scripts\sync-server.ps1
```

输入服务器密码即可。脚本打包当前工作区（含未提交代码），上传到服务器，
保存旧代码，再构建镜像、暂停服务、备份数据库/媒体/配置、迁移数据库并恢复服务。
失败会返回非零状态；不要继续启动机器人或删除数据卷。
这套流程适用于代码更新，不会覆盖服务器 `.env` 或 CLI 凭据。
`sync-server.ps1` 在本地打包和服务器解压时均忽略 `.env`、`.env.*`；
`server-update.sh` 不上传本地文件，只读取并备份服务器现有 `.env` 后更新容器。
同步不会删除服务器的旧文件；若更新包含文件删除或重命名，需要核对并移除对应旧文件。

只上传代码而暂不重启：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync-server.ps1 -UploadOnly
```

已上传代码后，在服务器执行更新：

```bash
cd /opt/CRM_Tool
bash scripts/server-update.sh
```

修改配置时直接编辑服务器 `/opt/CRM_Tool/.env`，然后运行同一更新脚本；
`docker compose restart` 不会加载新的环境变量或镜像。
更新脚本保留数据库和媒体备份；数据库 schema 更新后不能直接假定旧镜像可回滚。
恢复数据库需单独确定恢复点，避免覆盖发布后的业务数据。

## 状态与日志

服务器操作均使用服务器补充 Compose 文件：

服务器已创建 `docker-compose.override.yml -> docker-compose.server.yml` 软链接，
直接运行 `docker compose` 也会自动加载服务器配置；不要删除该软链接。

```bash
cd /opt/CRM_Tool
docker compose -f docker-compose.yml -f docker-compose.server.yml ps
docker compose -f docker-compose.yml -f docker-compose.server.yml logs -f --tail=100 app worker scheduler wecom-bot
curl -fsS http://127.0.0.1:8000/health/ready
tail -f /opt/CRM_Tool/log/runtime.log
```

`log/runtime.log` 由 `crm-tool-logs.service` 收集 Docker 日志，使用 logrotate 轮转；
部署和更新日志也位于 `log/`。业务审计继续保存在数据库。
备份位于 `/opt/CRM_Tool/backups/`，含敏感数据，仅 root 可读。
Docker 自带日志单容器最多保留 3 个 20 MB 文件，仍可用 `docker compose logs` 查看。
手动重建容器后执行 `systemctl restart crm-tool-logs.service`，重新连接日志收集。

服务器 Worker/Scheduler 的 Redis 健康探针每 30 秒运行一次，允许 30 秒完成。
两核服务器繁忙时 Python 探针启动可能超过本地默认的 3 秒；过短的期限会导致
进程正常却被判为 unhealthy，使更新脚本在启动机器人前中止。探针仍执行真实 Redis PING，
更新脚本也继续检查工作进程心跳；该调整仅作用于服务器补充配置。

## 访问控制台

控制台只监听服务器 `127.0.0.1:8000`。本机通过 SSH 隧道访问：

```powershell
ssh -N -L 18000:127.0.0.1:8000 root@8.153.164.200
```

保持终端开启，浏览器访问 `http://127.0.0.1:18000/console/`。
服务器保留原有身份认证配置，不新增登录账号或改变权限。
本地机器人切换后保持停止；不要用相同机器人凭据同时启动本地和服务器两端。
