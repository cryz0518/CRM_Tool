#!/usr/bin/env bash
# 在指定服务器更新镜像、备份数据、执行迁移并依次恢复服务；失败返回非零并保留日志。
# 本脚本只使用服务器现有 .env；本地上传与忽略配置由 sync-server.ps1 负责。
set -Eeuo pipefail
umask 077
cd /opt/CRM_Tool
mkdir -p log backups
stamp=$(date +%Y%m%d-%H%M%S)
exec > >(tee -a "log/update-${stamp}.log") 2>&1
trap 'echo "更新失败；请查看本次日志和容器状态，不要删除数据卷。"' ERR
compose=(docker compose -f docker-compose.yml -f docker-compose.server.yml)

# 构建期间保留现有服务；构建失败不会先中断业务。
"${compose[@]}" config -q
"${compose[@]}" build app worker scheduler wecom-bot migrate
"${compose[@]}" stop -t 60 wecom-bot scheduler worker app

# 暂停处理后备份数据库、媒体和服务器配置；备份文件只允许 root 读取。
"${compose[@]}" exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "backups/database-${stamp}.dump"
"${compose[@]}" run --rm --no-deps -T --entrypoint tar app -C /var/lib/crm-lead/media -czf - . > "backups/media-${stamp}.tar.gz"
cp .env "backups/env-${stamp}"

# 迁移显式执行一次，启动业务容器时不重复启动迁移依赖。
"${compose[@]}" run --rm --no-deps -T migrate
"${compose[@]}" up -d --no-deps --wait --wait-timeout 180 app worker scheduler
"${compose[@]}" exec -T app python -m app.production_verify --mode all --json
"${compose[@]}" up -d --no-deps --wait --wait-timeout 120 wecom-bot
systemctl restart crm-tool-logs.service
"${compose[@]}" ps
echo "更新完成。运行日志：/opt/CRM_Tool/log/runtime.log"
