"""Celery 应用基线。"""

from __future__ import annotations

from celery import Celery

from app.core.config import get_settings
from app.core.logging import configure_logging

settings = get_settings()
configure_logging(settings.log_level, environment=settings.app_env, service="worker")

# T01 只建立 Worker/Beat 运行边界，不提前注册后续业务任务。
celery_app = Celery("crm_lead", broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.update(
    broker_connection_retry_on_startup=True,
    timezone="Asia/Shanghai",
    enable_utc=False,
)
