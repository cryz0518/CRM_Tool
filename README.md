# CRM 线索自动录入

## 本地运行

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
```

应用健康检查：`http://localhost:8000/health`

企业微信机器人长连接使用 `.env` 中的 `WECOM_BOT_ID` 和 `WECOM_BOT_SECRET`；智能表格配置不属于该连接服务。

本地或测试环境可显式授权销售：

```powershell
docker compose run --rm --no-deps app python -m app.manage_sales authorize --wecom-user-id <企业微信用户标识>
```

## 常用命令

```powershell
docker compose logs -f app worker
docker compose run --rm app pytest
docker compose run --rm app ruff check .
docker compose run --rm app mypy app workers
docker compose run --rm migrate
docker compose down
```

`.env` 仅用于本地配置，禁止提交真实密钥。

企微机器人 CRM 提交命令：

- `提交今天的线索`：提交当前销售当天仍为“未提交”的待创建线索。
- `提交我所有线索`：机器人列出当前销售所有系统状态为“未提交”的待创建线索，销售在卡片中勾选一条或多条后提交；销售不能手动修改提交状态。
- `帮我提交放弃提交的线索`：机器人列出当前销售由系统标记为“放弃提交”的线索，销售勾选后重新执行查重和提交流程。
- `提交我的更新`：提交已同步线索的实际字段变更。
