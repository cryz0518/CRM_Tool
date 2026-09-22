"""Celery 应用基线。"""

from __future__ import annotations

from celery import Celery, signals
from redis import Redis

from app.core.config import get_settings
from app.core.heartbeat import HeartbeatPublisher
from app.core.logging import configure_logging

settings = get_settings()
configure_logging(settings.log_level, environment=settings.app_env, service="worker")

# T01 只建立 Worker/Beat 运行边界，不提前注册后续业务任务。
celery_app = Celery(
    "crm_lead",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["workers.tasks"],
)
celery_app.conf.update(
    broker_connection_retry_on_startup=True,
    timezone="Asia/Shanghai",
    enable_utc=False,
    beat_schedule={
        "consume-pending-lead-outbox-events": {
            "task": "workers.consume_pending_lead_outbox_events",
            "schedule": settings.lead_outbox_poll_seconds,
        },
        "consume-pending-wecom-actions": {
            "task": "workers.consume_pending_wecom_actions",
            "schedule": settings.lead_outbox_poll_seconds,
        },
        "issue-retention-cleanup-operations": {
            "task": "workers.issue_retention_cleanup_operations",
            "schedule": settings.lead_outbox_poll_seconds,
        },
        "reconcile-storage-ingest-operations": {
            "task": "workers.reconcile_storage_ingest_operations",
            "schedule": settings.lead_outbox_poll_seconds,
        },
    },
)


def _start_runtime_heartbeat(component: str) -> None:
    """为当前 Celery worker 或 scheduler 启动真实 Redis heartbeat。

    参数：component 为 worker 或 scheduler。
    返回值：无。
    异常：Redis 初次写入失败时由启动流程暴露，避免进程假装 ready。
    副作用：启动 daemon heartbeat 线程并写入带 TTL 的 Redis key。
    """
    publisher = HeartbeatPublisher(
        Redis.from_url(settings.redis_url, socket_connect_timeout=2),
        component,
    )
    publisher.start()


@signals.worker_ready.connect  # type: ignore[untyped-decorator]
def _worker_ready(**_: object) -> None:
    """在 Celery worker ready 信号后发布 worker heartbeat。"""
    _start_runtime_heartbeat("worker")


@signals.beat_init.connect  # type: ignore[untyped-decorator]
def _scheduler_ready(**_: object) -> None:
    """在 Celery beat 初始化后发布 scheduler heartbeat。"""
    _start_runtime_heartbeat("scheduler")
