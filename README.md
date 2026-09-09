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
