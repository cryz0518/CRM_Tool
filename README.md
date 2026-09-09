# CRM 线索自动录入

## 本地运行

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
```

应用健康检查：`http://localhost:8000/health`

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
