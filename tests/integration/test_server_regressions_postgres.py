"""服务器缺陷的隔离 PostgreSQL 行锁、终态和通知幂等回归；全部外部依赖使用假实现。"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.crm.adapter import CRMSearchResult
from app.crm.employee_directory import EmployeeDirectory
from app.crm.mock import MockCRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionCommand
from app.leads.models import CrmSyncRecord, Lead
from app.leads.reminders import DailyUnsubmittedLeadReminderService, _unsubmitted_lead_count
from app.leads.service import FirstTextLeadWorkspaceService, LeadProcessingStatus
from app.media.providers import FakeFileScanProvider, MockASRProvider, MockOCRProvider
from app.media.service import MediaAttachmentService, MediaValidator
from app.media.storage import FakeStorageProvider
from app.messaging.models import (
    IncomingMessage,
    MediaProcessingTask,
    MessageAttachment,
    NotificationRecord,
    OutboxEvent,
)
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema
from tests.crm_submission_test_utils import submit_today_via_selection
from tests.integration.test_lead_progress_postgres import (
    postgres_session_factory as postgres_session_factory,
)
from tests.unit.test_crm_submission import _lead
from tests.unit.test_daily_unsubmitted_lead_reminders import seed_lead, seed_sales
from tests.unit.test_message_order_and_context import (
    persist_outbox_texts,
)


def test_server_regressions_with_real_postgres_locks(
    postgres_session_factory: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    """验证并发覆盖只调用一次、未知租约不重写、无旧映射提醒唯一及媒体失败解阻。

    参数：会话工厂仅允许随机 tmpfs 数据库，tmp_path 为假存储和员工目录。
    返回：无；异常：数据库或断言失败由 pytest 传播；副作用：仅写隔离 schema。
    """
    factory = postgres_session_factory
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(factory, adapter)
    directory = tmp_path / "employee.csv"
    directory.write_text("id,name,nickname\ncrm-1,sales-1,sales-1\n", encoding="utf-8")
    crm = MockCRMAdapter(
        search_results={
            "人工最终公司": (CRMSearchResult("crm-existing", "original-owner", "duplicate"),)
        }
    )
    service = CrmSubmissionService(
        factory, adapter, crm, employee_directory=EmployeeDirectory(directory)
    )
    assert (
        len(submit_today_via_selection(service, "sales-1", "message-12").duplicate_confirmations)
        == 1
    )
    # 两个独立事务同时确认，数据库行锁必须只放行一个冻结 update。
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                service.resolve_duplicate_confirmation,
                "message-12",
                "sales-1",
                continue_submission=True,
                selected_lead_ids=(lead_id,),
            )
            for _ in range(2)
        ]
        assert sum(future.result().submitted for future in futures) == 1
    assert crm.update_calls == 1
    with factory.begin() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        assert sync.status == "succeeded" and sync.attempts == 1
        # 模拟远端已调用后进程崩溃，旧租约过期也不能再次执行覆盖。
        sync.status = "processing"
        sync.processing_started_at = datetime.now(UTC) - timedelta(minutes=10)
        sync.processing_lease_expires_at = datetime.now(UTC) - timedelta(minutes=5)
        sync_id = sync.id
    assert service._claim_and_call(sync_id, "sales-1") == "failed_pending_review"
    assert crm.update_calls == 1
    with factory() as session:
        assert session.get(CrmSyncRecord, sync_id).failure_category == "unknown"

    # 旧版 permanent/空 kind 仍是不确定远端事实，改表与并发新提交不得再查重或覆盖。
    with factory.begin() as session:
        sync = session.get(CrmSyncRecord, sync_id)
        sync.failure_category = "permanent"
        sync.failure_kind = None
        session.get(Lead, lead_id).lifecycle_state = "pending_create"
    adapter.update_record(adapter.get_records()[0].record_id, {
        "手机": "13700000000", "提交状态": "未提交",
    })
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(service.submit, SubmissionCommand(
            "提交指定线索", "sales-1", f"legacy-retry-{number}", target_lead_id=lead_id,
        )) for number in range(2)]
        assert all(future.result().failed_pending_review == 1 for future in futures)
    assert crm.update_calls == 1 and crm.search_calls == 1
    with factory() as session:
        sync = session.get(CrmSyncRecord, sync_id)
        assert (sync.failure_category, sync.failure_kind, sync.attempts) == ("permanent", None, 1)
        assert session.scalar(select(func.count()).select_from(CrmSyncRecord)) == 1

    # 提醒仍依据上海当日有效消息和最新 create generation，空旧映射不再阻塞。
    now = datetime(2026, 10, 9, 12, tzinfo=UTC)
    seed_sales(factory, "reminder-sales", authorized=False, crm_user_id=None)
    seed_lead(factory, "reminder-lead", "reminder-sales", now - timedelta(hours=1))
    with factory() as session:
        assert _unsubmitted_lead_count(
            session, "reminder-sales", now - timedelta(hours=20), now + timedelta(hours=4)
        ) == 1
    reminder = DailyUnsubmittedLeadReminderService(factory)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(reminder.schedule_due_reminders, now=now) for _ in range(2)]
        assert sum(future.result() for future in futures) == 1
    with factory() as session:
        assert session.scalar(select(func.count()).select_from(NotificationRecord)) == 1

    # 以独立销售 stream 验证等待下载失联的终态会自动释放下一条消息。
    first, second = persist_outbox_texts(factory, "media-sales", ["", "客户：后续测试公司"])
    with factory.begin() as session:
        message = session.get(IncomingMessage, "media-sales-message-1")
        message.requires_media_enrichment = True
        message.received_at = datetime.now(UTC) - timedelta(minutes=6)
    media = MediaAttachmentService(
        factory,
        MediaValidator(image_mime_types=("image/png",), audio_mime_types=()),
        FakeStorageProvider(tmp_path),
        FakeFileScanProvider("clean"),
        MockOCRProvider([]),
        MockASRProvider([]),
    )
    media.process_pending_for_message("media-sales-message-1")
    FirstTextLeadWorkspaceService(factory, adapter).consume(first)
    with factory() as session:
        assert session.get(OutboxEvent, first).status == "failed_pending_review"
        assert session.get(OutboxEvent, second).status == "succeeded"
        assert session.scalar(select(func.count()).select_from(Lead)) == 3

    # 数日前工件的新扫描只恢复媒体阶段；扫描轮询期间不能提前推进来源顺序检查点。
    first, second = persist_outbox_texts(factory, "rescan-sales", ["", "客户：集成后续公司"])
    with factory.begin() as session:
        session.get(IncomingMessage, "rescan-sales-message-1").requires_media_enrichment = True
    media = MediaAttachmentService(
        factory, MediaValidator(image_mime_types=("image/png",), audio_mime_types=()),
        FakeStorageProvider(tmp_path), FakeFileScanProvider("pending_scan"),
        MockOCRProvider(["客户：历史集成图片公司"]), MockASRProvider([]),
    )
    attachment_id = media.ingest(
        "rescan-sales-message-1", b"\x89PNG\r\n\x1a\nimage",
        media_kind="image", declared_mime_type="image/png",
    )
    with factory.begin() as session:
        attachment = session.get(MessageAttachment, attachment_id)
        attachment.created_at = datetime.now(UTC) - timedelta(days=3)
        attachment.scan_status = "scan_failed"
        attachment.processing_status = "failed_pending_review"
        session.scalar(select(MediaProcessingTask).where(
            MediaProcessingTask.attachment_id == attachment_id
        )).status = "failed_pending_review"
    media.apply_scan_result(attachment_id, "scanning")
    workspace = FirstTextLeadWorkspaceService(factory, adapter)
    for _ in range(3):
        media.process_pending_for_message("rescan-sales-message-1")
        assert workspace.consume(first).status == LeadProcessingStatus.WAITING_FOR_PREVIOUS
        assert workspace.consume(second).status == LeadProcessingStatus.WAITING_FOR_PREVIOUS
    media.apply_scan_result(attachment_id, "clean")
    media.process_pending_for_message("rescan-sales-message-1")
    workspace.consume(first)
    with factory() as session:
        assert session.get(MessageAttachment, attachment_id).processing_status == "succeeded"
        assert session.get(OutboxEvent, first).status == "succeeded"
        assert session.get(OutboxEvent, second).status == "succeeded"
