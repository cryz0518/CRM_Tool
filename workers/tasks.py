"""由 Celery 消费可靠 Outbox 的最小 Worker 任务。"""

from __future__ import annotations

from sqlalchemy import Engine, create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.leads.service import FirstTextLeadWorkspaceService
from app.messaging.models import OutboxEvent
from app.smart_table.dependencies import get_smart_table_adapter
from workers.celery_app import celery_app


def _session_factory() -> tuple[Engine, sessionmaker[Session]]:
    """为一次 Worker 消费创建带连接健康检查的数据库会话工厂。

    参数：无。
    返回值：数据库引擎及绑定其上的 SQLAlchemy 会话工厂。
    异常：数据库引擎配置无效时由 SQLAlchemy 抛出。
    副作用：创建可由调用方关闭的数据库连接池。
    """
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    return engine, sessionmaker(engine)


@celery_app.task(name="workers.consume_lead_outbox_event")  # type: ignore[untyped-decorator]
def consume_lead_outbox_event(outbox_event_id: int) -> str:
    """消费一条 Outbox 事件，并让应用服务按同销售顺序继续后续消息。

    参数：outbox_event_id 为待消费 Outbox 事件标识。
    返回值：应用服务的确定性状态文本，供 Celery 日志和运维排查使用。
    异常：数据库或适配器未分类异常向 Celery 传播，以保留任务失败事实。
    副作用：可能调用智能表格适配器并写入线索、归属、审计和任务状态。
    """
    # 适配器始终经依赖边界构造，Worker 不直接执行 wecom-cli 或操作表格字段。
    engine, factory = _session_factory()
    try:
        service = FirstTextLeadWorkspaceService(factory, get_smart_table_adapter())
        return service.consume(outbox_event_id).status.value
    finally:
        # 每个短任务释放独立连接池，避免 Beat 持续扫描时堆积空闲连接。
        engine.dispose()


@celery_app.task(  # type: ignore[untyped-decorator]
    name="workers.consume_pending_lead_outbox_events"
)
def consume_pending_lead_outbox_events() -> int:
    """扫描待消费或可重试 Outbox，并交给并行 Worker 任务处理。

    参数：无。
    返回值：本轮已投递的 Outbox 事件数。
    异常：数据库读取失败时向 Celery 传播，以供下一次 Beat 调度重试。
    副作用：为每条待处理事件异步投递 Celery 任务。
    """
    engine, factory = _session_factory()
    try:
        with factory() as session:
            # 由服务内销售行锁和 sequence 检查决定同销售串行；扫描本身允许不同销售并发投递。
            event_ids = session.scalars(
                select(OutboxEvent.id)
                .where(OutboxEvent.status.in_(("pending", "retrying", "processing")))
                .order_by(OutboxEvent.created_at, OutboxEvent.sequence)
            ).all()
    finally:
        engine.dispose()

    for event_id in event_ids:
        # 同一消息在并发扫描中可能重复投递，但应用服务的 Outbox 状态机保持消费幂等。
        consume_lead_outbox_event.delay(event_id)
    return len(event_ids)
