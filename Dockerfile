FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Shanghai

# 安装时区数据，确保容器内时间与项目部署时区一致。
RUN sed -i \
    -e 's|http://deb.debian.org/debian-security|https://mirrors.aliyun.com/debian-security|g' \
    -e 's|http://deb.debian.org/debian|https://mirrors.aliyun.com/debian|g' \
    /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Retries=3 update \
    && apt-get -o Acquire::Retries=3 install --no-install-recommends -y nodejs npm tzdata \
    && ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime \
    && echo "${TZ}" > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# Worker 的真实智能表格适配器通过 CLI 调用企业微信；凭据仅在运行时注入。
RUN npm install --global @wecom/cli

WORKDIR /app

COPY pyproject.toml README.md ./
COPY app ./app
COPY workers ./workers
COPY alembic ./alembic
COPY alembic.ini ./
COPY tests ./tests

# 国内服务器访问 PyPI 不稳定，构建使用镜像源并允许网络超时重试。
RUN pip install --no-cache-dir --index-url https://mirrors.aliyun.com/pypi/simple \
    --extra-index-url https://pypi.org/simple \
    --timeout 60 --retries 5 ".[dev]"

# 运行时使用非 root 账号，避免 Worker 以高权限执行任务。
RUN groupadd --system appuser \
    && useradd --system --gid appuser --create-home appuser \
    && mkdir -p /var/lib/crm-lead/media \
    && chown -R appuser:appuser /app /var/lib/crm-lead

USER appuser
