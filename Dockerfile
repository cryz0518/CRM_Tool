FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Shanghai

# 安装时区数据，确保容器内时间与项目部署时区一致。
RUN apt-get update \
    && apt-get install --no-install-recommends -y tzdata \
    && ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime \
    && echo "${TZ}" > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY app ./app
COPY workers ./workers
COPY alembic ./alembic
COPY alembic.ini ./
COPY tests ./tests

RUN pip install --no-cache-dir ".[dev]"

# 运行时使用非 root 账号，避免 Worker 以高权限执行任务。
RUN groupadd --system appuser \
    && useradd --system --gid appuser --create-home appuser \
    && chown -R appuser:appuser /app

USER appuser
