"""销售需求进度会话、统计口径和可靠通知测试。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import Settings, get_settings
from app.leads.models import (
    Lead,
    LeadMessageResolution,
    LeadProgressMessage,
    LeadProgressSession,
    SmartTableSync,
)
from app.leads.progress import (
    LeadProgressService,
    register_progress_intent_candidate,
    register_progress_message,
)
from app.leads.service import (
    FirstTextLeadWorkspaceService,
    LeadProcessingResult,
    LeadProcessingStatus,
)
from app.messaging.models import (
    Base,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.notifications.outbound import WecomOutboundNotificationSender
from app.smart_table.models import SmartTableRecord

# 先禁用项目 `.env`，再导入 Celery 模块，避免测试加载开发者本机配置。
Settings.model_config["env_file"] = None

from workers.celery_app import build_beat_schedule  # noqa: E402


@dataclass(frozen=True)
class SegmentSpec:
    """描述测试消息的一个已归属 Lead 分段或待归属分段。"""

    lead_id: str | None
    company_name: str | None = None
    lead_record_id: str | None = None
    sync_status: str = "succeeded"
    sync_record_id: str | None = None
    resolution_status: str = "assigned"


class MemorySnapshotAdapter:
    """以当前内存快照实现进度服务所需的只读智能表格读取。"""

    def __init__(self) -> None:
        """初始化空的记录映射和读取计数。"""
        self.records: dict[str, SmartTableRecord] = {}
        self.read_calls = 0

    def get_record(self, record_id: str) -> SmartTableRecord | None:
        """返回指定记录当前快照，并累计只读调用次数。"""
        self.read_calls += 1
        return self.records.get(record_id)


class RecordingClient:
    """保存测试发送目标和消息体，模拟 Bot 已认证客户端。"""

    def __init__(self, fail: bool = False) -> None:
        """初始化调用列表和是否模拟传输失败。"""
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.fail = fail

    async def send_message(self, userid_or_chatid: str, body: dict[str, object]) -> dict[str, str]:
        """记录一次消息发送，按配置可模拟可重试的连接失败。"""
        self.calls.append((userid_or_chatid, body))
        if self.fail:
            raise ConnectionError("isolated progress notification failure")
        return {"msgid": f"notice-{len(self.calls)}"}


@dataclass
class ProgressTestContext:
    """组合隔离数据库、进度服务与假智能表格适配器。"""

    engine: object
    session_factory: sessionmaker[Session]
    adapter: MemorySnapshotAdapter
    service: LeadProgressService


def progress_context(
    *, interval_minutes: int = 15, idle_stop_minutes: int = 60
) -> ProgressTestContext:
    """创建 SQLite 内存数据库与显式进度配置，不读取开发者本地业务数据。"""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine)
    adapter = MemorySnapshotAdapter()
    settings = Settings(
        lead_progress_enabled=True,
        lead_progress_interval_minutes=interval_minutes,
        lead_progress_idle_stop_minutes=idle_stop_minutes,
    )
    return ProgressTestContext(
        engine, factory, adapter, LeadProgressService(factory, adapter, settings)
    )


def seed_message(
    factory: sessionmaker[Session],
    *,
    message_id: str,
    sales_user_id: str,
    received_at: datetime,
    segments: tuple[SegmentSpec, ...] = (),
    outbox_status: str = "succeeded",
    event_type: str = "message_received",
    is_active: bool = True,
) -> None:
    """写入测试消息、Outbox、归属分段和相应 Lead 同步事实。"""
    with factory.begin() as session:
        authorization = session.get(SalesAuthorization, sales_user_id)
        if authorization is None:
            session.add(
                SalesAuthorization(
                    wecom_user_id=sales_user_id,
                    is_authorized=True,
                    is_active=is_active,
                )
            )
            session.flush()
        else:
            authorization.is_active = is_active
        sequence = (
            session.scalar(
                select(func.max(IncomingMessage.sequence)).where(
                    IncomingMessage.sales_user_id == sales_user_id
                )
            )
            or 0
        ) + 1
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
                raw_payload={"text": "原始需求：隆盛科技要求客户联络"},
                normalized_text="原始需求：隆盛科技联系人及电话需处理",
                received_at=received_at,
            )
        )
        session.add(
            OutboxEvent(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
                event_type=event_type,
                status=outbox_status,
            )
        )
        session.flush()
        for segment_index, spec in enumerate(segments):
            if spec.lead_id is not None:
                lead = session.get(Lead, spec.lead_id)
                if lead is None:
                    session.add(
                        Lead(
                            id=spec.lead_id,
                            source_message_id=message_id,
                            source_segment_index=segment_index,
                            original_capturing_sales_user_id=sales_user_id,
                            smart_table_owner_user_id=sales_user_id,
                            smart_table_record_id=spec.lead_record_id,
                            lifecycle_state="active",
                            field_values={"线索名称": spec.company_name or "测试公司"},
                        )
                    )
                    session.flush()
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == spec.lead_id)
                )
                if sync is None:
                    session.add(
                        SmartTableSync(
                            lead_id=spec.lead_id,
                            source_message_id=message_id,
                            source_segment_index=segment_index,
                            smart_table_record_id=(
                                spec.sync_record_id
                                if spec.sync_record_id is not None
                                else spec.lead_record_id
                            ),
                            status=spec.sync_status,
                        )
                    )
            session.add(
                LeadMessageResolution(
                    message_id=message_id,
                    segment_index=segment_index,
                    lead_id=spec.lead_id,
                    status=(spec.resolution_status if spec.lead_id is not None else "unassigned"),
                )
            )


def register_candidate(
    context: ProgressTestContext, message_id: str, *, now: datetime
) -> None:
    """为测试中已持久化消息登记唯一的进度恢复候选。"""
    with context.session_factory.begin() as session:
        message = session.get(IncomingMessage, message_id)
        assert message is not None
        assert register_progress_message(
            session,
            message,
            Settings(_env_file=None, lead_progress_enabled=True),
            now=now,
        )


def full_required_fields(**extra: object) -> dict[str, object]:
    """返回完整性服务要求的八个正式字段，可覆盖单项审核元数据。"""
    fields: dict[str, object] = {
        "业务线": "协作机器人",
        "线索名称": "测试公司",
        "线索来源": "展会",
        "联系人": "测试联系人",
        "职务": "采购经理",
        "沟通方式": "微信",
        "手机": "13800000000",
        "备注": "测试需求",
    }
    fields.update(extra)
    return fields


def read_notice(factory: sessionmaker[Session], sales_user_id: str) -> NotificationRecord | None:
    """读取指定销售最新一条进度汇报通知。"""
    with factory() as session:
        return session.scalar(
            select(NotificationRecord)
            .where(
                NotificationRecord.sales_user_id == sales_user_id,
                NotificationRecord.notification_type == "lead_progress_summary",
            )
            .order_by(NotificationRecord.created_at.desc())
        )


def test_sales_sessions_are_isolated_and_keep_independent_due_times() -> None:
    """交错消息只进入对应销售窗口，并按各自首条消息独立到期。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 1, 0, tzinfo=UTC)
    for message_id, sales_user_id, company in (
        ("siemens-msg", "sales-a", "西门子"),
        ("baotong-msg", "sales-b", "无锡宝通"),
        ("longsheng-msg", "sales-a", "隆盛科技"),
    ):
        record_id = f"record-{message_id}"
        seed_message(
            context.session_factory,
            message_id=message_id,
            sales_user_id=sales_user_id,
            received_at=start
            + (timedelta(minutes=5) if sales_user_id == "sales-b" else timedelta()),
            segments=(SegmentSpec(f"lead-{message_id}", company, record_id),),
        )
        assert context.service.record_resolved_message(
            message_id,
            now=start + (timedelta(minutes=5) if sales_user_id == "sales-b" else timedelta()),
        )
        context.adapter.records[record_id] = SmartTableRecord(record_id, full_required_fields())

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    sales_a = read_notice(context.session_factory, "sales-a")
    assert sales_a is not None
    assert "收到消息：2 条" in (sales_a.content or "")
    assert "识别线索：2 条" in (sales_a.content or "")
    assert all(name not in (sales_a.content or "") for name in ("西门子", "无锡宝通", "隆盛科技"))
    assert context.service.schedule_due_reports(now=start + timedelta(minutes=20)) == 1
    sales_b = read_notice(context.session_factory, "sales-b")
    assert sales_b is not None and "收到消息：1 条" in (sales_b.content or "")


def test_three_messages_same_lead_and_duplicate_message_id_are_counted_once() -> None:
    """同一 Lead 的三条补充消息只计一个 Lead，重复消息只计一次。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 2, 0, tzinfo=UTC)
    for index in range(3):
        message_id = f"followup-{index}"
        seed_message(
            context.session_factory,
            message_id=message_id,
            sales_user_id="sales-a",
            received_at=start + timedelta(minutes=index),
            segments=(SegmentSpec("one-lead", "西门子", "one-record", sync_status="succeeded"),),
        )
        assert context.service.record_resolved_message(
            message_id, now=start + timedelta(minutes=index)
        )
        assert context.service.record_resolved_message(
            message_id, now=start + timedelta(minutes=index)
        )
    context.adapter.records["one-record"] = SmartTableRecord("one-record", full_required_fields())

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None
    assert "收到消息：3 条" in (notice.content or "")
    assert "识别线索：1 条" in (notice.content or "")
    with context.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(LeadProgressMessage)) == 3


def test_one_message_with_multiple_segments_counts_each_lead_once() -> None:
    """单条消息拆分到多个客户时按独立 Lead ID 计数。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 3, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="multi-company",
        sales_user_id="sales-a",
        received_at=start,
        segments=(
            SegmentSpec("lead-one", "西门子", "record-one"),
            SegmentSpec("lead-two", "无锡宝通", "record-two"),
        ),
    )
    context.service.record_resolved_message("multi-company", now=start)
    for record_id in ("record-one", "record-two"):
        context.adapter.records[record_id] = SmartTableRecord(record_id, full_required_fields())

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None
    assert "收到消息：1 条" in (notice.content or "")
    assert "识别线索：2 条" in (notice.content or "")


def test_success_and_incomplete_counts_use_fresh_fields_and_separate_categories() -> None:
    """成功数只取核实同步事实，待完善按当前快照区分缺字段与 AI 待确认。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 4, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="incomplete-leads",
        sales_user_id="sales-a",
        received_at=start,
        segments=(
            SegmentSpec("missing-lead", "西门子", "record-missing"),
            SegmentSpec("pending-lead", "无锡宝通", "record-pending"),
        ),
    )
    with context.session_factory.begin() as session:
        # 后台草稿仍保留完整旧值，统计必须读取当前智能表格中的空快照。
        stale_lead = session.get(Lead, "missing-lead")
        assert stale_lead is not None
        stale_lead.field_values = full_required_fields()
    context.service.record_resolved_message("incomplete-leads", now=start)
    context.adapter.records["record-missing"] = SmartTableRecord("record-missing", {})
    context.adapter.records["record-pending"] = SmartTableRecord(
        "record-pending",
        full_required_fields(**{"手机": "", "AI待确认": ["职务"]}),
    )

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.payload is not None
    stats = notice.payload["stats"]
    assert isinstance(stats, dict)
    assert stats["successful_leads"] == 2
    assert stats["incomplete_leads"] == 2
    assert stats["missing_required_leads"] == 2
    assert stats["ai_pending_leads"] == 1
    assert "AI待确认 1，可重叠" in (notice.content or "")


def test_unassigned_segments_are_not_smart_table_success() -> None:
    """UNASSIGNED 分段计入无法归属，但不能成为识别或成功 Lead。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 5, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="unassigned",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec(None, resolution_status="unassigned"),),
        outbox_status="failed_pending_review",
    )
    context.service.record_resolved_message("unassigned", now=start)

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.payload is not None
    stats = notice.payload["stats"]
    assert isinstance(stats, dict)
    assert stats["identified_leads"] == 0
    assert stats["successful_leads"] == 0
    assert stats["unassigned_segments"] == 1
    assert stats["failed_messages_without_lead"] == 1
    assert "消息处理失败（无 Lead）：1 条" in (notice.content or "")


def test_failed_outbox_without_any_lead_has_separate_message_count() -> None:
    """AI 或消息处理终态失败且没有 Lead 时按唯一消息单独提示。"""
    context = progress_context()
    received_at = datetime(2026, 10, 8, 5, 30, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="failed-without-lead",
        sales_user_id="sales-a",
        received_at=received_at,
        outbox_status="failed_pending_review",
    )
    register_candidate(context, "failed-without-lead", now=received_at)

    assert context.service.recover_pending_messages() == 1
    assert context.service.schedule_due_reports(now=received_at + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.payload is not None
    stats = notice.payload["stats"]
    assert isinstance(stats, dict)
    assert stats["received_messages"] == 1
    assert stats["identified_leads"] == 0
    assert stats["successful_leads"] == 0
    assert stats["sync_failed_leads"] == 0
    assert stats["failed_messages_without_lead"] == 1


def test_retrying_is_processing_while_failed_pending_review_is_sync_failure() -> None:
    """暂态 retrying 归入处理项，只有 failed_pending_review 才归入同步失败。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 6, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="retry-and-final",
        sales_user_id="sales-a",
        received_at=start,
        segments=(
            SegmentSpec("retry-lead", "西门子", "retry-record", sync_status="retrying"),
            SegmentSpec(
                "failed-lead",
                "无锡宝通",
                "failed-record",
                sync_status="failed_pending_review",
            ),
        ),
        outbox_status="failed_pending_review",
    )
    context.service.record_resolved_message("retry-and-final", now=start)

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.payload is not None
    stats = notice.payload["stats"]
    assert isinstance(stats, dict)
    assert stats["sync_failed_leads"] == 1
    assert stats["processing_items"] == 1
    assert stats["successful_leads"] == 0
    assert stats["failed_messages_without_lead"] == 0


def test_repeated_message_to_same_failed_lead_counts_one_sync_failure() -> None:
    """同一 Lead 被多条消息关联时，同步失败仍按 Lead ID 只统计一次。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 6, 30, tzinfo=UTC)
    for message_id, received_at in (
        ("same-failed-lead-1", start),
        ("same-failed-lead-2", start + timedelta(minutes=1)),
    ):
        seed_message(
            context.session_factory,
            message_id=message_id,
            sales_user_id="sales-a",
            received_at=received_at,
            segments=(
                SegmentSpec(
                    "one-failed-lead",
                    "隆盛科技",
                    "failed-record",
                    sync_status="failed_pending_review",
                ),
            ),
            outbox_status="failed_pending_review",
        )
        context.service.record_resolved_message(message_id, now=received_at)

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.payload is not None
    stats = notice.payload["stats"]
    assert isinstance(stats, dict)
    assert stats["received_messages"] == 2
    assert stats["identified_leads"] == 1
    assert stats["sync_failed_leads"] == 1
    assert stats["failed_messages_without_lead"] == 0


def test_unverified_850005_record_and_record_id_mismatch_never_count_success() -> None:
    """850005 尚未远端核实或同步记录 ID 不一致时不得误报录入成功。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 7, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="unverified-850005",
        sales_user_id="sales-a",
        received_at=start,
        segments=(
            SegmentSpec("pending-record-lead", "西门子", "ack-record", sync_status="pending"),
            SegmentSpec(
                "mismatched-record-lead",
                "无锡宝通",
                "lead-record",
                sync_status="succeeded",
                sync_record_id="different-record",
            ),
        ),
        outbox_status="failed_pending_review",
    )
    with context.session_factory.begin() as session:
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == "pending-record-lead")
        )
        assert sync is not None
        sync.error_summary = "WeCom error 850005; awaiting verification"
    context.service.record_resolved_message("unverified-850005", now=start)

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.payload is not None
    stats = notice.payload["stats"]
    assert isinstance(stats, dict)
    assert stats["identified_leads"] == 2
    assert stats["successful_leads"] == 0
    assert stats["processing_items"] == 1
    assert "录入成功：0 条" in (notice.content or "")


def test_first_message_anchors_due_timer_and_late_scans_do_not_catch_up() -> None:
    """首条消息建立个人 due_at，未到期不发送，迟到扫描只创建一条通知。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 8, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="timer-message",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec("timer-lead", "隆盛科技", "timer-record"),),
    )
    context.service.record_resolved_message("timer-message", now=start)
    context.adapter.records["timer-record"] = SmartTableRecord(
        "timer-record", full_required_fields()
    )
    with context.session_factory() as session:
        progress = session.scalar(select(LeadProgressSession))
        assert progress is not None
        assert progress.started_at.replace(tzinfo=UTC) == start
        assert progress.next_report_at.replace(tzinfo=UTC) == start + timedelta(minutes=15)

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=14)) == 0
    assert context.service.schedule_due_reports(now=start + timedelta(minutes=50)) == 1
    assert context.service.schedule_due_reports(now=start + timedelta(minutes=50)) == 0
    with context.session_factory() as session:
        progress = session.scalar(select(LeadProgressSession))
        assert progress is not None
        assert progress.next_report_at.replace(tzinfo=UTC) == start + timedelta(minutes=65)
        assert session.scalar(select(func.count()).select_from(NotificationRecord)) == 1


def test_pending_message_is_counted_before_worker_completion_and_recovered_after_restart() -> None:
    """接收时持久化的处理中候选在 Worker 重启后由 Outbox 终态收敛。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 8, 30, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="worker-crash-candidate",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec("crash-lead", "西门子", "crash-record"),),
        outbox_status="processing",
    )
    with context.session_factory.begin() as session:
        message = session.get(IncomingMessage, "worker-crash-candidate")
        event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "worker-crash-candidate")
        )
        assert message is not None and event is not None
        register_progress_message(
            session,
            message,
            Settings(
                _env_file=None,
                lead_progress_enabled=True,
                lead_progress_interval_minutes=15,
            ),
            now=start,
        )

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None
    assert "收到消息：1 条" in (notice.content or "")
    assert "处理中／等待重试：1 项" in (notice.content or "")

    with context.session_factory.begin() as session:
        event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "worker-crash-candidate")
        )
        assert event is not None
        event.status = "succeeded"
    reads_before_recovery = context.adapter.read_calls
    restarted_service = LeadProgressService(context.session_factory, context.adapter)
    assert restarted_service.recover_pending_messages() == 1
    with context.session_factory() as session:
        business_snapshot = (
            session.get(Lead, "crash-lead").smart_table_record_id,
            session.scalar(
                select(SmartTableSync.status).where(SmartTableSync.lead_id == "crash-lead")
            ),
            session.scalar(
                select(LeadMessageResolution.status).where(
                    LeadMessageResolution.message_id == "worker-crash-candidate"
                )
            ),
            session.scalar(select(func.count()).select_from(NotificationRecord)),
        )
    assert restarted_service.recover_pending_messages() == 0
    assert restarted_service.schedule_due_reports(now=start + timedelta(minutes=15)) == 0
    with context.session_factory() as session:
        progress_message = session.get(LeadProgressMessage, "worker-crash-candidate")
        assert progress_message is not None and progress_message.status == "included"
        assert business_snapshot == (
            session.get(Lead, "crash-lead").smart_table_record_id,
            session.scalar(
                select(SmartTableSync.status).where(SmartTableSync.lead_id == "crash-lead")
            ),
            session.scalar(
                select(LeadMessageResolution.status).where(
                    LeadMessageResolution.message_id == "worker-crash-candidate"
                )
            ),
            session.scalar(select(func.count()).select_from(NotificationRecord)),
        )
    assert context.adapter.read_calls == reads_before_recovery


def test_recovery_scan_skips_first_hundred_active_candidates() -> None:
    """前 100 条 Outbox 长期活跃时，第 101 条终态候选仍进入本轮扫描。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 8, 45, tzinfo=UTC)
    for index in range(100):
        message_id = f"pending-{index:03d}"
        message_time = start + timedelta(seconds=index)
        seed_message(
            context.session_factory,
            message_id=message_id,
            sales_user_id="sales-a",
            received_at=message_time,
            outbox_status="processing",
        )
        register_candidate(context, message_id, now=message_time)

    completed_id = "completed-after-pending-window"
    completed_at = start + timedelta(seconds=100)
    seed_message(
        context.session_factory,
        message_id=completed_id,
        sales_user_id="sales-a",
        received_at=completed_at,
        segments=(SegmentSpec("late-lead", "隆盛科技", "late-record"),),
        outbox_status="succeeded",
    )
    register_candidate(context, completed_id, now=completed_at)

    assert context.service.recover_pending_messages(limit=1) == 1
    with context.session_factory() as session:
        recovered = session.get(LeadProgressMessage, completed_id)
        first_pending = session.get(LeadProgressMessage, "pending-000")
        assert recovered is not None and recovered.status == "included"
        assert first_pending is not None and first_pending.status == "processing"


def test_interleaved_sales_recovery_does_not_block_on_another_sales() -> None:
    """交错销售的活跃候选不会占用其他销售的终态恢复名额。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 9, 15, tzinfo=UTC)
    for message_id, sales_user_id, status, lead_id in (
        ("seller-a-pending-1", "sales-a", "processing", None),
        ("seller-b-ready-1", "sales-b", "succeeded", "seller-b-lead-1"),
        ("seller-a-ready-1", "sales-a", "succeeded", "seller-a-lead-1"),
        ("seller-b-pending-1", "sales-b", "retrying", None),
        ("seller-b-ready-2", "sales-b", "succeeded", "seller-b-lead-2"),
    ):
        message_time = start + timedelta(seconds=len(message_id))
        seed_message(
            context.session_factory,
            message_id=message_id,
            sales_user_id=sales_user_id,
            received_at=message_time,
            segments=(
                (SegmentSpec(lead_id, "测试公司", f"record-{lead_id}"),)
                if lead_id is not None
                else ()
            ),
            outbox_status=status,
        )
        register_candidate(context, message_id, now=message_time)

    assert context.service.recover_pending_messages(limit=3) == 3
    with context.session_factory() as session:
        assert session.get(LeadProgressMessage, "seller-a-pending-1").status == "processing"
        assert session.get(LeadProgressMessage, "seller-b-pending-1").status == "processing"
        assert session.get(LeadProgressMessage, "seller-b-ready-1").status == "included"
        assert session.get(LeadProgressMessage, "seller-a-ready-1").status == "included"
        assert session.get(LeadProgressMessage, "seller-b-ready-2").status == "included"


def test_awaiting_intent_submission_command_is_ignored_by_recovery() -> None:
    """尚待意图判断的 CRM 提交指令不会建立需求会话或进入统计。"""
    context = progress_context()
    received_at = datetime(2026, 10, 8, 10, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="awaiting-submit-command",
        sales_user_id="sales-a",
        received_at=received_at,
        event_type="crm_submission_intent",
        outbox_status="succeeded",
    )
    with context.session_factory.begin() as session:
        message = session.get(IncomingMessage, "awaiting-submit-command")
        assert message is not None
        assert register_progress_intent_candidate(
            session,
            message,
            Settings(_env_file=None, lead_progress_enabled=True),
            now=received_at,
        )

    assert context.service.recover_pending_messages() == 1
    with context.session_factory() as session:
        candidate = session.get(LeadProgressMessage, "awaiting-submit-command")
        assert candidate is not None and candidate.status == "ignored"
        assert candidate.progress_session_id is None
        assert session.scalar(select(func.count()).select_from(LeadProgressSession)) == 0
        assert session.scalar(select(func.count()).select_from(NotificationRecord)) == 0


def test_progress_update_failure_does_not_block_outbox_checkpoint_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """进度收敛异常不阻止同销售检查点推进，持久候选可由 Scheduler 修复。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 9, 30, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="progress-update-failure",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec("progress-failure-lead", "隆盛科技", "progress-record"),),
        outbox_status="succeeded",
    )
    with context.session_factory.begin() as session:
        message = session.get(IncomingMessage, "progress-update-failure")
        event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "progress-update-failure")
        )
        assert message is not None and event is not None
        event_id = event.id
        register_progress_message(
            session,
            message,
            Settings(_env_file=None, lead_progress_enabled=True),
            now=start,
        )

    class FailingProgressService:
        """模拟进度数据库故障，但保留接收事务已写入的候选事实。"""

        def record_resolved_message(self, *_args: object, **_kwargs: object) -> bool:
            """抛出进度更新错误，验证其不会逃逸到 Outbox 检查点。"""
            raise RuntimeError("isolated progress update failure")

    result = LeadProcessingResult(status=LeadProcessingStatus.UPDATED)
    workspace = FirstTextLeadWorkspaceService(
        context.session_factory,
        object(),  # type: ignore[arg-type]
        lead_progress_service=FailingProgressService(),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(workspace, "_consume_once", lambda *_args, **_kwargs: result)
    monkeypatch.setattr(workspace, "_apply_company_resolution", lambda *_args: result)
    checkpoint = Mock()
    monkeypatch.setattr(workspace, "_consume_next_after_checkpoint", checkpoint)

    assert workspace.consume(event_id) is result
    checkpoint.assert_called_once_with(event_id)
    assert context.service.recover_pending_messages() == 1
    with context.session_factory() as session:
        progress_message = session.get(LeadProgressMessage, "progress-update-failure")
        assert progress_message is not None and progress_message.status == "included"


def test_scheduler_restart_preserves_session_and_retries_only_notification() -> None:
    """重建服务后复用持久化 due 与通知，发送失败只重试 Bot 通知。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 9, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="restart-message",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec("restart-lead", "西门子", "restart-record"),),
    )
    context.service.record_resolved_message("restart-message", now=start)
    context.adapter.records["restart-record"] = SmartTableRecord(
        "restart-record", full_required_fields()
    )
    due = start + timedelta(minutes=15)
    assert context.service.schedule_due_reports(now=due) == 1
    read_calls = context.adapter.read_calls

    restarted_service = LeadProgressService(
        context.session_factory,
        context.adapter,
        Settings(lead_progress_enabled=True),
    )
    assert restarted_service.schedule_due_reports(now=due) == 0
    get_settings.cache_clear()
    client = RecordingClient(fail=True)
    sender = WecomOutboundNotificationSender(context.session_factory, client)
    assert asyncio.run(sender.send_pending_once()) == 0
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.status == "retrying"
    assert context.adapter.read_calls == read_calls

    client.fail = False
    assert asyncio.run(sender.send_pending_once()) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.status == "succeeded"
    assert context.adapter.read_calls == read_calls
    restarted_client = RecordingClient()
    restarted_sender = WecomOutboundNotificationSender(context.session_factory, restarted_client)
    assert asyncio.run(restarted_sender.send_pending_once()) == 0
    assert restarted_client.calls == []


def test_idle_session_sends_final_and_new_message_opens_fresh_window() -> None:
    """任务完成且空闲后发送最终汇总关闭会话，后续消息另起窗口。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 10, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="first-session",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec("idle-lead", "隆盛科技", "idle-record"),),
    )
    context.service.record_resolved_message("first-session", now=start)
    context.adapter.records["idle-record"] = SmartTableRecord("idle-record", full_required_fields())
    assert context.service.schedule_due_reports(now=start + timedelta(minutes=60)) == 1
    final_notice = read_notice(context.session_factory, "sales-a")
    assert final_notice is not None and final_notice.payload is not None
    assert final_notice.payload["final"] is True
    assert "最终汇报" in (final_notice.content or "")
    with context.session_factory() as session:
        old_session = session.scalar(select(LeadProgressSession))
        assert old_session is not None and old_session.status == "closed"

    next_start = start + timedelta(minutes=61)
    seed_message(
        context.session_factory,
        message_id="second-session",
        sales_user_id="sales-a",
        received_at=next_start,
        segments=(SegmentSpec("new-lead", "西门子", "new-record"),),
    )
    assert context.service.record_resolved_message("second-session", now=next_start)
    with context.session_factory() as session:
        sessions = session.scalars(
            select(LeadProgressSession).order_by(LeadProgressSession.started_at)
        ).all()
        assert len(sessions) == 2
        assert sessions[0].status == "closed"
        assert sessions[1].status == "active"
        assert sessions[1].started_at.replace(tzinfo=UTC) == next_start


def test_inactive_sales_closes_session_without_sending_progress() -> None:
    """销售停用后调度关闭会话且不创建主动汇报通知。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 11, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="disabled-sales",
        sales_user_id="sales-disabled",
        received_at=start,
        segments=(SegmentSpec(None, resolution_status="unassigned"),),
    )
    context.service.record_resolved_message("disabled-sales", now=start)
    with context.session_factory.begin() as session:
        actor = session.get(SalesAuthorization, "sales-disabled")
        assert actor is not None
        actor.is_active = False

    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 0
    with context.session_factory() as session:
        progress = session.scalar(select(LeadProgressSession))
        assert progress is not None and progress.status == "disabled"
        assert session.scalar(select(func.count()).select_from(NotificationRecord)) == 0


def test_commands_and_messages_without_resolution_do_not_enter_progress() -> None:
    """CRM 提交命令、卡片动作和尚未形成归属结论的消息不计入需求窗口。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="crm-command",
        sales_user_id="sales-a",
        received_at=start,
        event_type="crm_submission_command",
    )
    seed_message(
        context.session_factory,
        message_id="card-action",
        sales_user_id="sales-a",
        received_at=start + timedelta(minutes=1),
    )
    assert not context.service.record_resolved_message("crm-command", now=start)
    assert not context.service.record_resolved_message(
        "card-action", now=start + timedelta(minutes=1)
    )
    with context.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(LeadProgressSession)) == 0
        assert session.scalar(select(func.count()).select_from(LeadProgressMessage)) == 0


def test_ignored_demand_candidate_closes_without_emitting_a_progress_report() -> None:
    """AI 最终忽略的普通文本不会留下空汇报或占用下一次会话。"""
    context = progress_context()
    received_at = datetime(2026, 10, 8, 12, 30, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="ignored-candidate",
        sales_user_id="sales-a",
        received_at=received_at,
        outbox_status="ignored",
    )
    with context.session_factory.begin() as session:
        message = session.get(IncomingMessage, "ignored-candidate")
        assert message is not None
        register_progress_message(
            session,
            message,
            Settings(_env_file=None, lead_progress_enabled=True),
            now=received_at,
        )
    assert context.service.record_resolved_message(
        "ignored-candidate", included=False, now=received_at + timedelta(minutes=1)
    )

    assert context.service.schedule_due_reports(now=received_at + timedelta(minutes=15)) == 0
    with context.session_factory() as session:
        progress = session.scalar(select(LeadProgressSession))
        candidate = session.get(LeadProgressMessage, "ignored-candidate")
        assert progress is not None and progress.status == "closed"
        assert progress.close_reason == "no_valid_demand"
        assert candidate is not None and candidate.status == "ignored"
        assert session.scalar(select(func.count()).select_from(NotificationRecord)) == 0


def test_old_message_recovery_does_not_start_a_historical_timer() -> None:
    """超过空闲周期才恢复的旧事件从处理时刻重新起算，不补发历史窗口。"""
    context = progress_context()
    received = datetime(2026, 10, 8, 1, 0, tzinfo=UTC)
    resumed = received + timedelta(hours=4)
    seed_message(
        context.session_factory,
        message_id="old-backlog",
        sales_user_id="sales-a",
        received_at=received,
        segments=(SegmentSpec(None, resolution_status="unassigned"),),
    )
    context.service.record_resolved_message("old-backlog", now=resumed)
    with context.session_factory() as session:
        progress = session.scalar(select(LeadProgressSession))
        assert progress is not None
        assert progress.started_at.replace(tzinfo=UTC) == resumed
        assert progress.next_report_at.replace(tzinfo=UTC) == resumed + timedelta(minutes=15)


def test_progress_notification_sender_suppresses_when_feature_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bot 在设置关闭时抑制已排队的进度通知，不影响其他通知类型。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 13, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="disabled-feature",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec(None, resolution_status="unassigned"),),
    )
    context.service.record_resolved_message("disabled-feature", now=start)
    with context.session_factory.begin() as session:
        progress = session.scalar(select(LeadProgressSession))
        assert progress is not None
        session.add(
            NotificationRecord(
                notification_key="disabled-progress-notice",
                sales_user_id="sales-a",
                source_message_id=progress.id,
                notification_type="lead_progress_summary",
                content="📊 需求录入进度汇报",
            )
        )
    monkeypatch.setenv("LEAD_PROGRESS_ENABLED", "false")
    get_settings.cache_clear()
    client = RecordingClient()

    assert (
        asyncio.run(
            WecomOutboundNotificationSender(context.session_factory, client).send_pending_once()
        )
        == 0
    )
    with context.session_factory() as session:
        notice = session.get(NotificationRecord, "disabled-progress-notice")
        assert notice is not None and notice.status == "suppressed"
    assert client.calls == []
    monkeypatch.setenv("LEAD_PROGRESS_ENABLED", "true")
    get_settings.cache_clear()


def test_config_range_and_beat_schedule_use_progress_settings() -> None:
    """进度间隔拒绝非法范围，Beat 使用较短扫描频率且可整体关闭。"""
    with pytest.raises(ValidationError):
        Settings(lead_progress_interval_minutes=0)
    with pytest.raises(ValidationError):
        Settings(lead_progress_idle_stop_minutes=-1)
    settings = Settings(lead_progress_enabled=True, lead_outbox_poll_seconds=7)
    schedule = build_beat_schedule(settings)
    assert schedule["schedule-lead-progress-reports"]["schedule"] == 7
    assert schedule["schedule-lead-progress-reports"]["task"] == (
        "workers.schedule_lead_progress_reports"
    )
    disabled = build_beat_schedule(Settings(lead_progress_enabled=False))
    assert "schedule-lead-progress-reports" not in disabled


def test_progress_message_is_safe_and_retry_does_not_touch_business_adapters() -> None:
    """汇报只含数量，发送失败由通知 outbox 重试且不重读表格或运行 CRM。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 14, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="safe-message",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec("safe-lead", "隆盛科技", "safe-record"),),
    )
    context.service.record_resolved_message("safe-message", now=start)
    context.adapter.records["safe-record"] = SmartTableRecord("safe-record", full_required_fields())
    assert context.service.schedule_due_reports(now=start + timedelta(minutes=15)) == 1
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None
    assert all(
        private_value not in (notice.content or "")
        for private_value in ("隆盛科技", "原始需求", "联系人", "13800000000")
    )
    reads_before_send = context.adapter.read_calls
    get_settings.cache_clear()
    client = RecordingClient(fail=True)

    assert (
        asyncio.run(
            WecomOutboundNotificationSender(context.session_factory, client).send_pending_once()
        )
        == 0
    )
    assert context.adapter.read_calls == reads_before_send
    with context.session_factory() as session:
        notice = session.get(NotificationRecord, notice.notification_key)
        assert notice is not None and notice.status == "retrying"
        assert notice.attempts == 1


def test_session_summary_records_window_dedupe_and_send_result() -> None:
    """通知持久化统计窗口、去重键和后续发送状态，Bot 重启不重复排程。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 15, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="audited-message",
        sales_user_id="sales-a",
        received_at=start,
        segments=(SegmentSpec(None, resolution_status="unassigned"),),
    )
    context.service.record_resolved_message("audited-message", now=start)
    end = start + timedelta(minutes=15)
    assert context.service.schedule_due_reports(now=end) == 1
    restarted = LeadProgressService(context.session_factory, context.adapter, Settings())
    assert restarted.schedule_due_reports(now=end) == 0
    notice = read_notice(context.session_factory, "sales-a")
    assert notice is not None and notice.payload is not None
    assert notice.notification_key
    assert notice.payload["window_started_at"] == start.isoformat()
    assert notice.payload["window_ended_at"] == end.isoformat()
    assert notice.status == "pending"


def test_existing_receipt_crm_and_first_success_notifications_remain_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """进度通知加入后，既有接收确认、CRM 结果和首次表格链接仍可正常发送。"""
    context = progress_context()
    start = datetime(2026, 10, 8, 16, 0, tzinfo=UTC)
    seed_message(
        context.session_factory,
        message_id="existing-types",
        sales_user_id="sales-a",
        received_at=start,
    )
    monkeypatch.setenv("LEAD_PROGRESS_ENABLED", "true")
    get_settings.cache_clear()
    with context.session_factory.begin() as session:
        for key, kind, content in (
            ("receipt-existing", "lead_intake_receipt", "已收到消息"),
            ("crm-existing", "crm_submission_summary", "CRM 提交结果"),
            ("link-existing", "lead_first_smart_table_success", "查看线索表格"),
        ):
            session.add(
                NotificationRecord(
                    notification_key=key,
                    sales_user_id="sales-a",
                    source_message_id="existing-types",
                    notification_type=kind,
                    content=content,
                )
            )
    client = RecordingClient()

    assert (
        asyncio.run(
            WecomOutboundNotificationSender(context.session_factory, client).send_pending_once()
        )
        == 3
    )
    assert [call[0] for call in client.calls] == ["sales-a"] * 3
    with context.session_factory() as session:
        notification_types = {
            notice.notification_type for notice in session.scalars(select(NotificationRecord))
        }
    assert notification_types == {
        "lead_intake_receipt",
        "crm_submission_summary",
        "lead_first_smart_table_success",
    }
