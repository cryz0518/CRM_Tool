"""由 Celery 消费可靠 Outbox 的最小 Worker 任务。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

from sqlalchemy import Engine, and_, create_engine, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql.elements import ColumnElement

from app.ai.dependencies import get_ai_gateway
from app.ai.models import ExtractedLeadPatch, LeadAnalysis, SubmissionIntent
from app.ai.persistence import DatabaseAIExecutionRecorder
from app.companies.dependencies import get_tyc_adapter
from app.companies.service import CompanyLeadService
from app.core.config import Settings, get_settings
from app.core.failures import TaskFailureCategory, classify_task_failure
from app.core.provider_policy import get_provider_policy
from app.crm.commands import (
    consume_submission_command,
    is_explicit_submission_request,
    notification_key_for_message,
    parse_company_submission_request,
)
from app.crm.dependencies import get_crm_adapter
from app.leads.models import Lead, LeadMessageResolution
from app.leads.review import LeadReviewService
from app.leads.service import COMPLETED_CHECKPOINT_STATUSES, FirstTextLeadWorkspaceService
from app.media.dependencies import get_media_attachment_service, get_media_storage_provider
from app.media.retention import (
    RetentionCleanupScheduler,
    RetentionCleanupService,
    RetentionPayloadScrubService,
    RetentionPolicy,
    StorageIngestRecoveryService,
    retention_policy_is_configured,
)
from app.messaging.models import (
    AuditMirrorOutbox,
    BusinessAuditEvent,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
    StorageIngestOperation,
    WecomActionOutbox,
    WecomActionOutboxStatus,
    utc_now,
)
from app.smart_table.audit import SmartTableAuditSink
from app.smart_table.dependencies import get_smart_table_adapter, get_smart_table_audit_adapter
from app.wecom_bot.actions import (
    CardCapabilityUnavailable,
    DeterministicWecomActionExecutor,
    WecomActionService,
    parse_deterministic_action_command,
)
from workers.celery_app import celery_app

logger = logging.getLogger(__name__)


def audit_retry_delay_seconds(attempts: int) -> int:
    """按失败次数计算审计镜像的有界指数退避时间。

    参数：attempts 为已经开始的镜像尝试次数。
    返回值：首次失败后 30 秒起步、最多 300 秒的等待时间。
    异常：无；零或负数按第一次失败处理。
    副作用：无。
    """
    if attempts <= 1:
        return 30
    if attempts == 2:
        return 60
    if attempts == 3:
        return 120
    return 300


def _audit_mirror_retry_due_condition(now: datetime) -> ColumnElement[bool]:
    """构造按每档退避时间筛选到期 retrying 镜像的 SQL 条件。

    参数：now 为本轮统一使用的当前 UTC 时间。
    返回值：仅匹配已达到对应 attempts 退避期限的 SQLAlchemy 条件。
    异常：无。
    副作用：无；条件仅供只读扫描或原子认领查询使用。
    """
    return or_(
        and_(
            AuditMirrorOutbox.attempts <= 1,
            AuditMirrorOutbox.updated_at
            <= now - timedelta(seconds=audit_retry_delay_seconds(1)),
        ),
        and_(
            AuditMirrorOutbox.attempts == 2,
            AuditMirrorOutbox.updated_at
            <= now - timedelta(seconds=audit_retry_delay_seconds(2)),
        ),
        and_(
            AuditMirrorOutbox.attempts == 3,
            AuditMirrorOutbox.updated_at
            <= now - timedelta(seconds=audit_retry_delay_seconds(3)),
        ),
        and_(
            AuditMirrorOutbox.attempts >= 4,
            AuditMirrorOutbox.updated_at
            <= now - timedelta(seconds=audit_retry_delay_seconds(4)),
        ),
    )


def _get_retention_policy_or_skip(settings: Settings) -> RetentionPolicy | None:
    """读取清理策略；非生产未配置时安全跳过，生产仍保持 fail-closed。

    参数：settings 为当前 Worker 配置。
    返回值：完整配置时返回冻结策略；开发环境缺失配置时返回 None。
    异常：生产环境或配置格式非法时传播原始配置错误。
    副作用：未配置的非生产环境记录一次结构化告警，不访问数据库或对象存储。
    """
    if retention_policy_is_configured(settings):
        return RetentionPolicy.from_settings(settings)
    if not get_provider_policy(settings).is_production:
        logger.warning(
            "retention_policy_not_configured_skipped",
            extra={"app_env": settings.app_env},
        )
        return None
    return RetentionPolicy.from_settings(settings)


def _session_factory() -> tuple[Engine, sessionmaker[Session]]:
    """为一次 Worker 消费创建带连接健康检查的数据库会话工厂。

    参数：无。
    返回值：数据库引擎及绑定其上的 SQLAlchemy 会话工厂。
    异常：数据库引擎配置无效时由 SQLAlchemy 抛出。
    副作用：创建可由调用方关闭的数据库连接池。
    """
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    return engine, sessionmaker(engine)


def _claim_audit_mirror_outbox(
    session_factory: sessionmaker[Session], outbox_id: int
) -> str | None:
    """原子认领一条审计镜像任务，防止重复 Worker 同时写表。

    参数：session_factory 为数据库会话工厂；outbox_id 为镜像任务标识。
    返回值：本次成功取得的 claim token；任务不可认领时返回 None。
    异常：数据库错误向调用方传播。
    副作用：更新镜像任务状态、尝试次数、处理开始时间和 claim token，不修改业务审计事件。
    """
    now = utc_now()
    lease_expired_before = now - timedelta(
        seconds=get_settings().lead_processing_timeout_seconds
    )
    with session_factory.begin() as session:
        outbox = session.scalar(
            select(AuditMirrorOutbox)
            .where(
                AuditMirrorOutbox.id == outbox_id,
                or_(
                    AuditMirrorOutbox.status == "pending",
                    and_(
                        AuditMirrorOutbox.status == "retrying",
                        _audit_mirror_retry_due_condition(now),
                    ),
                    and_(
                        AuditMirrorOutbox.status == "processing",
                        AuditMirrorOutbox.processing_started_at.is_not(None),
                        AuditMirrorOutbox.processing_started_at < lease_expired_before,
                    ),
                ),
            )
            .with_for_update()
        )
        if outbox is None:
            return None
        # 每次初次认领或过期接管都生成新 token，旧 Worker 不能再提交 finalize。
        claim_token = uuid4().hex
        # 仅镜像 Outbox 自身进入 processing；业务状态不随远端失败回滚。
        outbox.status = "processing"
        outbox.attempts += 1
        outbox.processing_started_at = now
        outbox.claim_token = claim_token
        outbox.failure_category = None
        outbox.failure_code = None
        return claim_token


def _finish_audit_mirror(
    session_factory: sessionmaker[Session],
    outbox_id: int,
    claim_token: str,
    *,
    succeeded: bool,
    error: Exception | None = None,
) -> str:
    """以受控状态完成或重置审计镜像任务。

    参数：session_factory 为数据库会话工厂；outbox_id 为镜像任务标识；claim_token 为本次认领令牌；
    succeeded 表示远端已确认；error 为失败时仅用于记录异常类型。
    返回值：succeeded、retrying 或 failed_pending_review；fencing 失败时返回 stale。
    异常：数据库更新错误向 Worker 传播。
    副作用：仅在 ID、processing 状态和 claim token 同时匹配时更新镜像 Outbox，绝不修改原业务对象。
    """
    with session_factory.begin() as session:
        # 使用 claim token fencing，防止过期旧 Worker 覆盖新 Worker 的 processing 结果。
        outbox = session.scalar(
            select(AuditMirrorOutbox)
            .where(
                AuditMirrorOutbox.id == outbox_id,
                AuditMirrorOutbox.status == "processing",
                AuditMirrorOutbox.claim_token == claim_token,
            )
            .with_for_update()
        )
        if outbox is None:
            return "stale"
        outbox.processing_started_at = None
        outbox.claim_token = None
        if succeeded:
            outbox.status = "succeeded"
            outbox.failure_category = None
            outbox.failure_code = None
            return outbox.status

        # 只有 failure framework 明确认定为 transient 才进入自动退避重试。
        category = (
            classify_task_failure(error)
            if error is not None
            else TaskFailureCategory.UNKNOWN
        )
        outbox.status = (
            "retrying"
            if category is TaskFailureCategory.TRANSIENT
            else "failed_pending_review"
        )
        outbox.failure_category = category.value
        if error is not None:
            # 只保存受控异常类型，不保存 errmsg、HTTP body 或 traceback。
            outbox.failure_code = type(error).__name__[:64]
        return outbox.status


def _resolve_audit_lead_id(session: Session, event: BusinessAuditEvent) -> str | None:
    """按当前数据库中的确定性事实解析审计事件所属 Lead.id。

    参数：session 为当前审计镜像读取会话；event 为待镜像的业务审计事件。
    返回值：唯一确定的 Lead.id；无法确定或多线索冲突时返回 None。
    异常：数据库查询异常向 Worker 传播。
    副作用：仅读取数据库，不修改审计事件、线索或归属关系。
    """
    details = event.details if isinstance(event.details, dict) else {}
    detail_lead_id = details.get("lead_id")
    if isinstance(detail_lead_id, str) and detail_lead_id.strip():
        # 历史审计事实中的 lead_id 是最高优先级的确定性归属来源。
        return detail_lead_id.strip()

    smart_table_record_id = details.get("smart_table_record_id")
    if isinstance(smart_table_record_id, str) and smart_table_record_id.strip():
        record_lead_ids = list(
            session.scalars(
                select(Lead.id).where(Lead.smart_table_record_id == smart_table_record_id)
            )
        )
        if len(record_lead_ids) == 1:
            # 只有表格记录唯一对应一个 Lead 时才采用该归属。
            return record_lead_ids[0]
        if len(record_lead_ids) > 1:
            # 数据异常导致多条匹配时不得任选一条继续镜像。
            return None

    resolution_lead_ids = {
        lead_id
        for lead_id in session.scalars(
            select(LeadMessageResolution.lead_id).where(
                LeadMessageResolution.message_id == event.message_id,
                LeadMessageResolution.lead_id.is_not(None),
            )
        )
        if isinstance(lead_id, str) and lead_id.strip()
    }
    if len(resolution_lead_ids) == 1:
        # 同消息多个 segment 指向同一 Lead 时，去重后仍可安全确定归属。
        return next(iter(resolution_lead_ids))
    if len(resolution_lead_ids) > 1:
        # 多 segment 指向不同 Lead 属于 ambiguity，不能猜测第一条。
        return None

    source_lead_ids = list(
        session.scalars(select(Lead.id).where(Lead.source_message_id == event.message_id))
    )
    if len(source_lead_ids) == 1:
        # 仅保留历史 source_message_id fallback，且要求唯一匹配。
        return source_lead_ids[0]
    return None


def _should_wait_for_audit_lead_resolution(
    session: Session,
    event: BusinessAuditEvent,
    resolved_lead_id: str | None,
) -> bool:
    """判断审计镜像是否应等待来源消息的业务 Outbox 完成归属。

    参数：session 为当前数据库会话；event 为审计事件；resolved_lead_id 为当前已解析的 Lead.id。
    返回值：仍可能产生归属事实且来源 Outbox 未终态时返回 True，否则返回 False。
    异常：数据库查询异常向 Worker 传播。
    副作用：仅读取数据库，不修改任何业务状态。
    """
    if resolved_lead_id is not None:
        return False
    source_outbox = session.scalar(
        select(OutboxEvent).where(OutboxEvent.message_id == event.message_id)
    )
    if source_outbox is None:
        return False
    # 复用线索 Worker 的检查点定义，避免审计 Worker 自己维护另一套终态集合。
    return source_outbox.status not in COMPLETED_CHECKPOINT_STATUSES


def _defer_audit_mirror(
    session_factory: sessionmaker[Session], outbox_id: int, claim_token: str
) -> None:
    """释放当前审计镜像认领，等待来源业务 Outbox 形成归属事实。

    参数：session_factory 为数据库会话工厂；outbox_id 为镜像任务标识；claim_token 为当前认领令牌。
    返回值：无。
    异常：数据库更新错误向 Worker 传播。
    副作用：仅在 ID、processing 状态和 claim token 同时匹配时重置为 pending；不记录失败。
    """
    with session_factory.begin() as session:
        # defer 与 finalize 一样必须做 token fencing，旧 Worker 不能释放新 Worker 的 claim。
        outbox = session.scalar(
            select(AuditMirrorOutbox)
            .where(
                AuditMirrorOutbox.id == outbox_id,
                AuditMirrorOutbox.status == "processing",
                AuditMirrorOutbox.claim_token == claim_token,
            )
            .with_for_update()
        )
        if outbox is None:
            return
        outbox.status = "pending"
        # 等待 source Lead 的消费没有调用 SmartTable，不应消耗外部镜像尝试额度。
        outbox.attempts = max(0, outbox.attempts - 1)
        outbox.processing_started_at = None
        outbox.claim_token = None
        outbox.failure_category = None
        outbox.failure_code = None


@celery_app.task(name="workers.consume_audit_mirror_outbox")  # type: ignore[untyped-decorator]
def consume_audit_mirror_outbox(outbox_id: int) -> str:
    """消费一条审计镜像 Outbox，并用 claim fencing 与稳定镜像键控制重试。

    参数：outbox_id 为审计镜像任务标识。
    返回值：succeeded、retrying 或 already_processing 等安全状态文本。
    异常：数据库异常向 Celery 传播；外部镜像异常转为 retrying。
    副作用：最多向管理员审计子表写入一条记录，不影响原业务状态。
    """
    engine, factory = _session_factory()
    try:
        claim_token = _claim_audit_mirror_outbox(factory, outbox_id)
        if claim_token is None:
            return "already_processing"
        with factory() as session:
            outbox = session.get(AuditMirrorOutbox, outbox_id)
            event = (
                session.get(BusinessAuditEvent, outbox.audit_event_id)
                if outbox is not None
                else None
            )
            message = (
                session.get(IncomingMessage, event.message_id)
                if event is not None
                else None
            )
            sales_original_message = (
                message.normalized_text
                if message is not None and message.scrubbed_at is None
                else None
            )
            resolved_lead_id = (
                _resolve_audit_lead_id(session, event) if event is not None else None
            )
            should_wait_for_lead = (
                _should_wait_for_audit_lead_resolution(session, event, resolved_lead_id)
                if event is not None
                else False
            )
        if outbox is None or event is None:
            return _finish_audit_mirror(
                factory,
                outbox_id,
                claim_token,
                succeeded=False,
                error=ValueError("audit_event_missing"),
            )
        if should_wait_for_lead:
            _defer_audit_mirror(factory, outbox_id, claim_token)
            return "deferred"
        try:
            # 依靠服务端 claim fencing、稳定镜像键和写前远端查重，避免重试重复写行。
            SmartTableAuditSink(get_smart_table_audit_adapter()).mirror(
                event,
                sales_original_message=sales_original_message,
                resolved_lead_id=resolved_lead_id,
            )
        except Exception as error:
            final_status = _finish_audit_mirror(
                factory, outbox_id, claim_token, succeeded=False, error=error
            )
            logger_method = (
                logger.warning
                if final_status in {"retrying", "stale"}
                else logger.error
            )
            logger_method(
                "audit_smart_table_mirror_retrying"
                if final_status == "retrying"
                else (
                    "audit_smart_table_mirror_stale_finalize"
                    if final_status == "stale"
                    else "audit_smart_table_mirror_failed_pending_review"
                ),
                extra={"audit_mirror_outbox_id": outbox_id, "error_type": type(error).__name__},
            )
            return final_status
        _finish_audit_mirror(factory, outbox_id, claim_token, succeeded=True)
        logger.info(
            "audit_smart_table_mirror_succeeded",
            extra={"audit_mirror_outbox_id": outbox_id},
        )
        return "succeeded"
    finally:
        engine.dispose()


@celery_app.task(name="workers.consume_pending_audit_mirrors")  # type: ignore[untyped-decorator]
def consume_pending_audit_mirrors() -> int:
    """扫描待处理或租约过期的审计镜像任务并投递独立 Worker。

    参数：无。
    返回值：本轮投递的镜像任务数量。
    异常：数据库读取失败时向 Celery 传播。
    副作用：只投递任务，不执行外部 Smart Table 调用。
    """
    engine, factory = _session_factory()
    try:
        settings = get_settings()
        now = utc_now()
        expired_before = now - timedelta(
            seconds=settings.lead_processing_timeout_seconds
        )
        with factory() as session:
            outbox_ids = list(
                session.scalars(
                    select(AuditMirrorOutbox.id)
                    .where(
                        or_(
                            AuditMirrorOutbox.status == "pending",
                            and_(
                                AuditMirrorOutbox.status == "retrying",
                                _audit_mirror_retry_due_condition(now),
                            ),
                            and_(
                                AuditMirrorOutbox.status == "processing",
                                AuditMirrorOutbox.processing_started_at.is_not(None),
                                AuditMirrorOutbox.processing_started_at < expired_before,
                            ),
                        )
                    )
                    # defer 会刷新 updated_at，较老的等待任务因此让位给后续 pending 项。
                    .order_by(AuditMirrorOutbox.updated_at, AuditMirrorOutbox.id)
                    .limit(settings.audit_mirror_batch_size)
                )
            )
    finally:
        engine.dispose()
    for outbox_id in outbox_ids:
        # 认领留给任务本身，重复调度也由数据库条件更新收敛。
        consume_audit_mirror_outbox.delay(outbox_id)
    return len(outbox_ids)


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
        if _is_submission_command(factory, outbox_event_id):
            # 命令已在 T02 确定性分类；只编排现有 T12 服务，绝不进入 AI 线索路径。
            with factory() as session:
                event = session.get(OutboxEvent, outbox_event_id)
                message = session.get(IncomingMessage, event.message_id) if event else None
            company_request = (
                message is not None
                and parse_company_submission_request(message.normalized_text or "") is not None
            )
            return consume_submission_command(
                factory,
                smart_table_adapter,
                None if company_request else get_crm_adapter(),
                outbox_event_id,
            )
        if _is_submission_intent(factory, outbox_event_id):
            # 只有已被消息接入层分类为提交意图的事件才进入提交路由；普通线索文本必须
            # 继续进入原有抽取管线，避免补充手机号等消息被提前结束。
            with factory() as session:
                event = session.get(OutboxEvent, outbox_event_id)
                message = session.get(IncomingMessage, event.message_id) if event else None
            if message is None:
                raise ValueError("意图路由缺少来源消息")
            intent = get_ai_gateway().classify_submission_intent(
                message.normalized_text or ""
            )
            if intent.intent == "LEAD_CAPTURE":
                # 正常线索意图必须回到原有抽取管线，不能被当作未识别提交结束。
                pass
            elif intent.intent == "UNKNOWN" or not is_explicit_submission_request(
                message.normalized_text or ""
            ):
                return _finish_unrecognized_submission_intent(factory, outbox_event_id, message)
            else:
                command_text = _submission_command_text(intent)
                if command_text is None:
                    return _finish_unrecognized_submission_intent(factory, outbox_event_id, message)
                company_request = parse_company_submission_request(command_text) is not None
                return consume_submission_command(
                    factory,
                    smart_table_adapter,
                    None if company_request else get_crm_adapter(),
                    outbox_event_id,
                    command_text=command_text,
                )
        if _is_wecom_action_command(factory, outbox_event_id):
            with factory() as session:
                event = session.get(OutboxEvent, outbox_event_id)
                message = session.get(IncomingMessage, event.message_id) if event else None
            if message is None:
                raise ValueError("T18 machine command 缺少来源消息")
            parsed = parse_deterministic_action_command(message.normalized_text or "")
            if parsed is None:
                raise ValueError("T18 machine command 解析失败")
            action_service = WecomActionService(
                factory, card_callback_ready=get_settings().wecom_card_callback_ready()
            )
            try:
                if parsed.action_type == "lead_discard_confirmation":
                    action_service.issue_discard_action(
                        actor_user_id=message.sales_user_id,
                        lead_id=parsed.target_id,
                        reason="销售通过固定 T18 命令请求废弃",
                        source_message_id=message.message_id,
                    )
                else:
                    if parsed.message_id is None or parsed.segment_index is None:
                        raise ValueError("T18 重归属命令缺少服务端候选参数")
                    action_service.issue_reassignment_action(
                        actor_user_id=message.sales_user_id,
                        message_id=parsed.message_id,
                        segment_index=parsed.segment_index,
                        target_lead_id=parsed.target_id,
                        reason="销售通过固定 T18 命令请求重归属",
                        source_message_id=message.message_id,
                    )
            except CardCapabilityUnavailable:
                return "card_callback_unavailable"
            return "action_issued"
        # T10 首期明确只接入 Mock TYC；真实天眼查 API 留给后续专用适配器。
        service = FirstTextLeadWorkspaceService(
            factory,
            smart_table_adapter,
            ai_gateway=get_ai_gateway(execution_recorder=DatabaseAIExecutionRecorder(factory)),
            company_lead_service=CompanyLeadService(
                factory, smart_table_adapter, get_tyc_adapter()
            ),
            robot_submission_confirmation_available=get_settings().wecom_card_callback_ready(),
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
        result = LeadReviewService(
            factory,
            get_smart_table_adapter(),
            robot_submission_confirmation_available=get_settings().wecom_card_callback_ready(),
        ).sync_ai_patch(lead_id, source_message_id, patch)
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


def _is_submission_command(session_factory: sessionmaker[Session], outbox_event_id: int) -> bool:
    """判断已认领 Outbox 是否为 T12 确定性 CRM 提交或公司预览命令。

    参数：session_factory 为数据库会话工厂；outbox_event_id 为待消费事件。
    返回值：仅 event_type 为 crm_submission_command 时返回 True。
    异常：事件缺失时抛出 ValueError。
    副作用：仅读取数据库。
    """
    with session_factory() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None:
            raise ValueError(f"Outbox 事件不存在：{outbox_event_id}")
        return event.event_type == "crm_submission_command"


def _is_submission_intent(session_factory: sessionmaker[Session], outbox_event_id: int) -> bool:
    """判断 Outbox 是否需要异步模型提交意图识别。"""
    with session_factory() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None:
            raise ValueError(f"Outbox 事件不存在：{outbox_event_id}")
        return event.event_type == "crm_submission_intent"


def _submission_command_text(intent: SubmissionIntent) -> str | None:
    """把受限提交意图转换为既有内部命令，不接受模型生成的业务参数。

    参数：intent 为 AI 已校验的结构化意图。
    返回值：内部命令文本；录入或未知意图返回 None。
    异常：无。
    副作用：无，不调用 CRM 或其它外部系统。
    """
    command_by_intent = {
        "SUBMIT_TODAY": "提交今天的线索",
        "SUBMIT_ALL": "提交我所有线索",
        "SUBMIT_ABANDONED": "帮我提交放弃提交的线索",
        "SUBMIT_UPDATES": "提交我的更新",
        "SUBMIT_RETRY_INCOMPLETE": "重新提交待完善的线索",
    }
    if intent.intent in command_by_intent:
        return command_by_intent[intent.intent]
    if intent.intent != "SUBMIT_SINGLE" or not intent.company_name:
        return None
    return f"请帮我提交{intent.company_name}这条线索"


def _finish_unrecognized_submission_intent(
    session_factory: sessionmaker[Session], outbox_event_id: int, message: IncomingMessage
) -> str:
    """安全结束无法确定提交意图的消息，并写入可重试无关的提示通知。"""
    content = "未能识别消息意图。请提供客户信息，或明确说明要提交今天、全部、放弃或更新的线索。"
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is not None:
            event.status = "succeeded"
            event.processing_started_at = None
        key = notification_key_for_message(f"intent:{message.message_id}")
        if session.get(NotificationRecord, key) is None:
            session.add(
                NotificationRecord(
                    notification_key=key,
                    sales_user_id=message.sales_user_id,
                    source_message_id=message.message_id,
                    notification_type="crm_submission_intent_unrecognized",
                    content=content,
                )
            )
    return "submission_intent_unrecognized"


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
    异常：actor registry 记录缺失或数据库异常时由调用方处理。
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
            raise ValueError(f"actor registry 记录不存在：{event.sales_user_id}")
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


@celery_app.task(name="workers.consume_wecom_action")  # type: ignore[untyped-decorator]
def consume_wecom_action(action_id: str) -> str:
    """消费一条已经由 callback 原子认领的 T18 动作 Outbox。

    参数：action_id 为服务端内部 action UUID，不接受 callback 客户端业务字段。
    返回值：确定性的动作执行状态摘要。
    异常：数据库异常向 Celery 传播；domain 异常由动作服务收敛为 pending_recovery。
    副作用：复用字段确认、废弃、重归属或 CRM 提交领域服务，并创建最终通知。
    """
    engine, factory = _session_factory()
    try:
        service = WecomActionService(
            factory, card_callback_ready=get_settings().wecom_card_callback_ready()
        )
        executor = DeterministicWecomActionExecutor(
            factory,
            get_smart_table_adapter(),
            get_crm_adapter(),
            service,
        )
        return service.execute_action(action_id, executor).code
    finally:
        # Worker 每次动作使用短生命周期连接池，避免通知或动作重试泄漏连接。
        engine.dispose()


@celery_app.task(name="workers.consume_pending_wecom_actions")  # type: ignore[untyped-decorator]
def consume_pending_wecom_actions() -> int:
    """扫描待执行或租约已过期的 callback 动作，并投递独立 Worker 任务。

    参数：无。
    返回值：本轮投递的动作数量。
    异常：数据库读取失败时向 Celery 传播。
    副作用：仅投递任务，不直接调用任何业务服务；租约恢复仍由动作服务行锁决定。
    """
    engine, factory = _session_factory()
    now = utc_now()
    try:
        with factory.begin() as session:
            outboxes = list(
                session.scalars(
                    select(WecomActionOutbox)
                    .where(
                        and_(
                            or_(
                                WecomActionOutbox.status == WecomActionOutboxStatus.PENDING.value,
                                and_(
                                    WecomActionOutbox.status
                                    == WecomActionOutboxStatus.PROCESSING.value,
                                    WecomActionOutbox.processing_lease_expires_at.is_not(None),
                                    WecomActionOutbox.processing_lease_expires_at <= now,
                                ),
                            ),
                            or_(
                                WecomActionOutbox.dispatch_lease_expires_at.is_(None),
                                WecomActionOutbox.dispatch_lease_expires_at <= now,
                            ),
                        )
                    )
                    .with_for_update(skip_locked=True)
                )
            )
            for outbox in outboxes:
                # 调度租约与业务 processing 租约分离，Celery 任务迟到时仍可安全执行。
                outbox.dispatch_claimed_at = now
                outbox.dispatch_lease_expires_at = now + timedelta(minutes=5)
            action_ids = [outbox.action_id for outbox in outboxes]
    finally:
        engine.dispose()
    for action_id in action_ids:
        # Outbox 记录仍是唯一业务动作 claim，重复投递不会重复 domain side effect。
        consume_wecom_action.delay(action_id)
    return len(action_ids)


def _is_wecom_action_command(session_factory: sessionmaker[Session], outbox_event_id: int) -> bool:
    """判断 Outbox 是否为确定性 T18 machine command。"""

    with session_factory() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        return event is not None and event.event_type == "wecom_action_command"


@celery_app.task(name="workers.issue_retention_cleanup_operations")  # type: ignore[untyped-decorator]
def issue_retention_cleanup_operations() -> int:
    """由 Scheduler 扫描到期附件并签发 operation，不执行远端删除。"""
    settings = get_settings()
    policy = _get_retention_policy_or_skip(settings)
    if policy is None:
        return 0
    engine, factory = _session_factory()
    try:
        scheduler = RetentionCleanupScheduler(factory)
        operation_ids = scheduler.scan_and_issue(
            policy,
            batch_size=settings.retention_cleanup_batch_size,
        )
        # 同一周期同时重新派发 retry/reconcile 和已过期租约，覆盖 Worker 崩溃恢复。
        operation_ids = list(
            dict.fromkeys(
                operation_ids
                + scheduler.runnable_operation_ids(
                    batch_size=settings.retention_cleanup_batch_size
                )
            )
        )[: settings.retention_cleanup_batch_size]
    finally:
        engine.dispose()
    for operation_id in operation_ids:
        # 只把持久化 operation 投递给 Worker，Scheduler 不接触 object storage。
        execute_retention_cleanup.delay(operation_id)
    scrub_retention_payloads.delay()
    return len(operation_ids)


@celery_app.task(name="workers.reconcile_storage_ingest_operations")  # type: ignore[untyped-decorator]
def reconcile_storage_ingest_operations() -> int:
    """Scheduler 只扫描未终态 ingest intent，Worker 才执行 HEAD/reconcile。"""
    engine, factory = _session_factory()
    try:
        with factory() as session:
            operation_ids = session.scalars(
                select(StorageIngestOperation.id)
                .where(
                    or_(
                        StorageIngestOperation.status.in_(
                            ("pending", "retrying", "reconcile_required")
                        ),
                        and_(
                            StorageIngestOperation.status == "processing",
                            or_(
                                StorageIngestOperation.lease_expires_at.is_(None),
                                StorageIngestOperation.lease_expires_at <= utc_now(),
                            ),
                        ),
                    )
                )
                .order_by(StorageIngestOperation.created_at, StorageIngestOperation.id)
                .limit(get_settings().retention_cleanup_batch_size)
            ).all()
    finally:
        engine.dispose()
    for operation_id in operation_ids:
        reconcile_storage_ingest_operation.delay(operation_id)
    return len(operation_ids)


@celery_app.task(name="workers.reconcile_storage_ingest_operation")  # type: ignore[untyped-decorator]
def reconcile_storage_ingest_operation(operation_id: str) -> str:
    """Worker 先 HEAD 固定 object key，存在则 finalize，缺失则保留人工恢复事实。"""
    engine, factory = _session_factory()
    try:
        return StorageIngestRecoveryService(factory).reconcile(
            operation_id, get_media_storage_provider(get_settings())
        )
    finally:
        engine.dispose()


@celery_app.task(name="workers.execute_retention_cleanup")  # type: ignore[untyped-decorator]
def execute_retention_cleanup(operation_id: str) -> str:
    """由 Worker claim、HEAD、删除并以 fenced finalize 收敛一个清理 operation。"""
    engine, factory = _session_factory()
    try:
        storage = get_media_storage_provider(get_settings())
        return RetentionCleanupService(factory).execute(
            operation_id,
            storage,
            lease_seconds=get_settings().retention_cleanup_lease_seconds,
        )
    finally:
        engine.dispose()


@celery_app.task(name="workers.scrub_retention_payloads")  # type: ignore[untyped-decorator]
def scrub_retention_payloads() -> tuple[int, int]:
    """由 Worker 按独立 data class 策略 scrub 消息和通知正文。"""
    settings = get_settings()
    policy = _get_retention_policy_or_skip(settings)
    if policy is None:
        return (0, 0)
    engine, factory = _session_factory()
    try:
        return RetentionPayloadScrubService(factory).scrub_expired_payloads(policy)
    finally:
        engine.dispose()
