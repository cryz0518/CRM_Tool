"""由 Celery 消费可靠 Outbox 的最小 Worker 任务。"""

from __future__ import annotations

from sqlalchemy import Engine, create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.ai.dependencies import get_ai_gateway
from app.ai.models import ExtractedLeadPatch, LeadAnalysis
from app.core.config import get_settings
from app.leads.review import LeadReviewService
from app.leads.service import FirstTextLeadWorkspaceService
from app.media.dependencies import get_media_attachment_service
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
        # 媒体识别先补充同一来源消息的标准化文本；失败被任务内消化，不阻塞线索顺序。
        get_media_attachment_service(factory).process_pending_for_message(
            _message_id(factory, outbox_event_id)
        )
        service = FirstTextLeadWorkspaceService(
            factory, get_smart_table_adapter(), ai_gateway=get_ai_gateway()
        )
        return service.consume(outbox_event_id).status.value
    finally:
        # 每个短任务释放独立连接池，避免 Beat 持续扫描时堆积空闲连接。
        engine.dispose()


@celery_app.task(name="workers.sync_ai_lead_patch")  # type: ignore[untyped-decorator]
def sync_ai_lead_patch(
    lead_id: str,
    source_message_id: str,
    trace_id: str,
    analysis: dict[str, object],
    fields: dict[str, str],
    pending_confirmation_fields: list[str],
    low_confidence_candidates: dict[str, str],
) -> dict[str, list[str]]:
    """消费 T08 已校验补丁并调用 T09 审核服务安全同步智能表格。

    参数：前三项定位 AI 处理事实；其余参数为 Celery JSON 序列化后的 ExtractedLeadPatch 内容。
    返回值：实际写入和人工保护字段，供调用方记录可观测任务结果。
    异常：Pydantic、数据库或表格异常向 Celery 传播，保留任务失败事实。
    副作用：重读智能表格并可能更新字段来源、审核元数据和业务审计。
    """
    # Worker 只反序列化 T08 已产生的结果；不在此处重新调用模型或解释业务字段。
    patch = ExtractedLeadPatch(
        trace_id=trace_id,
        analysis=LeadAnalysis.model_validate(analysis),
        fields=fields,
        pending_confirmation_fields=tuple(pending_confirmation_fields),
        low_confidence_candidates=low_confidence_candidates,
    )
    engine, factory = _session_factory()
    try:
        result = LeadReviewService(factory, get_smart_table_adapter()).sync_ai_patch(
            lead_id, source_message_id, patch
        )
        return {
            "updated_fields": list(result.updated_fields),
            "protected_fields": list(result.protected_fields),
        }
    finally:
        # 独立 Worker 任务完成后释放连接池，避免高频 AI 补丁同步积累空闲连接。
        engine.dispose()


def _message_id(session_factory: sessionmaker[Session], outbox_event_id: int) -> str:
    """读取 Outbox 的来源消息标识，缺失事实由 Worker 明确失败。"""
    with session_factory() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None:
            raise ValueError(f"Outbox 事件不存在：{outbox_event_id}")
        return event.message_id


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
