"""由 Celery 消费可靠 Outbox 的最小 Worker 任务。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from sqlalchemy import Engine, and_, create_engine, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session, sessionmaker

from app.ai.dependencies import get_ai_gateway
from app.ai.models import ExtractedLeadPatch, LeadAnalysis
from app.companies.service import CompanyLeadService, MockQCCAdapter
from app.core.config import get_settings
from app.leads.review import LeadReviewService
from app.leads.service import COMPLETED_CHECKPOINT_STATUSES, FirstTextLeadWorkspaceService
from app.media.dependencies import get_media_attachment_service
from app.messaging.models import OutboxEvent, SalesAuthorization, utc_now
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
def consume_lead_outbox_event(
    outbox_event_id: int,
    claimed_at: str | None = None,
    recover_expired_lease: bool = False,
) -> str:
    """消费一条 Outbox 事件，并让应用服务按同销售顺序继续后续消息。

    参数：outbox_event_id 为待消费 Outbox 事件标识；claimed_at 为调度认领时间；
    recover_expired_lease 标识本任务只执行过期租约恢复。
    返回值：应用服务的确定性状态文本，供 Celery 日志和运维排查使用。
    异常：数据库或适配器未分类异常向 Celery 传播，以保留任务失败事实。
    副作用：可能调用智能表格适配器并写入线索、归属、审计和任务状态。
    """
    # 适配器始终经依赖边界构造，Worker 不直接执行 wecom-cli 或操作表格字段。
    engine, factory = _session_factory()
    try:
        # Celery 可能重复投递同一消息；只有首个任务可接管本次持久化认领。
        if claimed_at is None or not _take_lead_outbox_claim(factory, outbox_event_id, claimed_at):
            return "already_processed"
        smart_table_adapter = get_smart_table_adapter()
        # T10 首期明确只接入 Mock QCC；真实企查查 API 留给 T21 的专用适配器。
        service = FirstTextLeadWorkspaceService(
            factory,
            smart_table_adapter,
            ai_gateway=get_ai_gateway(),
            company_lead_service=CompanyLeadService(
                factory, smart_table_adapter, MockQCCAdapter()
            ),
        )
        if recover_expired_lease:
            # 失联处理只进入既有人工复核路径，绝不重放媒体、模型或智能表格调用。
            return service.consume(
                outbox_event_id,
                claimed_for_processing=True,
                recover_expired_lease=True,
            ).status.value
        # 媒体识别先补充同一来源消息的标准化文本；失败被任务内消化，不阻塞线索顺序。
        get_media_attachment_service(factory).process_pending_for_message(
            _message_id(factory, outbox_event_id)
        )
        return service.consume(outbox_event_id, claimed_for_processing=True).status.value
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
    enrichment: dict[str, str] | None = None,
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
        enrichment=enrichment or {},
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


@dataclass(frozen=True)
class LeadOutboxDispatchClaim:
    """描述一次已持久化的 Outbox 调度认领。"""

    event_id: int
    claimed_at: datetime
    recover_expired_lease: bool


def claim_dispatchable_lead_outbox_events(
    session_factory: sessionmaker[Session], *, lease_timeout: timedelta
) -> list[LeadOutboxDispatchClaim]:
    """原子认领可投递事件，并保持同销售消息的持久化顺序。

    参数：session_factory 为数据库会话工厂；lease_timeout 为 processing 租约时长。
    返回值：本轮成功认领且可投递的事件及其认领事实。
    异常：数据库锁定或读写失败时由 SQLAlchemy 抛出。
    副作用：将 pending/retrying 或已过期 processing 事件置为新的 processing 租约。
    """
    now = utc_now()
    expired_before = now - lease_timeout
    with session_factory() as session:
        candidate_ids = session.scalars(
            select(OutboxEvent.id)
            .where(
                or_(
                    OutboxEvent.status.in_(("pending", "retrying")),
                    and_(
                        OutboxEvent.status == "processing",
                        OutboxEvent.processing_started_at.is_not(None),
                        OutboxEvent.processing_started_at < expired_before,
                    ),
                )
            )
            .order_by(OutboxEvent.created_at, OutboxEvent.sequence)
        ).all()

    claims: list[LeadOutboxDispatchClaim] = []
    for event_id in candidate_ids:
        claim = _claim_lead_outbox_event(session_factory, event_id, now, lease_timeout)
        if claim is not None:
            claims.append(claim)
    return claims


def _claim_lead_outbox_event(
    session_factory: sessionmaker[Session],
    outbox_event_id: int,
    now: datetime,
    lease_timeout: timedelta,
) -> LeadOutboxDispatchClaim | None:
    """在销售顺序锁内认领单条事件，避免并发扫描重复投递。

    参数：session_factory 为数据库会话工厂；outbox_event_id 为目标事件；now 为本轮时钟；
    lease_timeout 为 processing 租约时长。
    返回值：成功时返回认领事实，不可投递或被其他扫描认领时返回 None。
    异常：销售授权记录缺失或数据库异常时由调用方处理。
    副作用：成功时刷新事件 processing 开始时间。
    """
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None:
            return None
        authorization = session.scalar(
            select(SalesAuthorization)
            .where(SalesAuthorization.wecom_user_id == event.sales_user_id)
            .with_for_update()
        )
        if authorization is None:
            raise ValueError(f"销售授权记录不存在：{event.sales_user_id}")
        session.refresh(event)
        lease_expired = _processing_lease_expired(event, now, lease_timeout)
        if event.status not in {"pending", "retrying"} and not lease_expired:
            return None
        previous_event_id = session.scalar(
            select(OutboxEvent.id)
            .where(
                OutboxEvent.sales_user_id == event.sales_user_id,
                OutboxEvent.sequence < event.sequence,
                OutboxEvent.status.not_in(COMPLETED_CHECKPOINT_STATUSES),
            )
            .order_by(OutboxEvent.sequence)
            .limit(1)
        )
        if previous_event_id is not None:
            return None
        event.status = "processing"
        event.processing_started_at = now
        return LeadOutboxDispatchClaim(
            event_id=event.id,
            claimed_at=now,
            recover_expired_lease=lease_expired,
        )


def _processing_lease_expired(event: OutboxEvent, now: datetime, lease_timeout: timedelta) -> bool:
    """判断事件是否为需要进入既有恢复路径的失联 processing 租约。

    参数：event 为待检查事件；now 为当前时钟；lease_timeout 为配置租约。
    返回值：仅 processing 开始时间超出租约时返回 True。
    异常：无。
    副作用：无。
    """
    if event.status != "processing" or event.processing_started_at is None:
        return False
    return _as_utc(event.processing_started_at) + lease_timeout < _as_utc(now)


def _as_utc(value: datetime) -> datetime:
    """将数据库可能返回的朴素时间统一解释为 UTC。

    参数：value 为待标准化的时间值。
    返回值：带 UTC 时区的时间值。
    异常：无。
    副作用：无。
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _take_lead_outbox_claim(
    session_factory: sessionmaker[Session], outbox_event_id: int, claimed_at: str
) -> bool:
    """让首个 Celery 任务一次性接管调度认领，阻断重复消息的外部调用。

    参数：session_factory 为数据库会话工厂；outbox_event_id 为目标事件；claimed_at 为认领时间。
    返回值：仅首个与认领事实匹配的任务返回 True。
    异常：时间文本无效或数据库异常时由调用方处理。
    副作用：成功时刷新 processing 开始时间，重新开始失联租约计时。
    """
    scheduled_at = datetime.fromisoformat(claimed_at)
    with session_factory.begin() as session:
        # 时间戳也是一次性令牌：条件更新只允许一个并发 Worker 改写该认领事实。
        result = cast(
            CursorResult[object],
            session.execute(
                update(OutboxEvent)
                .where(
                    OutboxEvent.id == outbox_event_id,
                    OutboxEvent.status == "processing",
                    OutboxEvent.processing_started_at == scheduled_at,
                )
                .values(processing_started_at=utc_now())
            ),
        )
        return result.rowcount == 1


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
        claims = claim_dispatchable_lead_outbox_events(
            factory,
            lease_timeout=timedelta(seconds=get_settings().lead_processing_timeout_seconds),
        )
    finally:
        engine.dispose()

    for claim in claims:
        # 认领事实随任务传递，Worker 会在外部调用前再次原子接管该事实。
        consume_lead_outbox_event.delay(
            claim.event_id,
            claim.claimed_at.isoformat(),
            claim.recover_expired_lease,
        )
    return len(claims)
