"""T12 CRM 首次创建提交的应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.crm.commands import (
    consume_submission_command,
    format_submission_reply,
    notification_key_for_message,
    terminal_failure_notification_key_for_message,
)
from app.crm.mock import MockCRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionCommand
from app.leads.models import CrmSyncRecord, Lead, LeadFieldProvenance
from app.leads.review import LeadReviewService
from app.messaging.models import (
    Base,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供含 T12 持久化模型的共享内存数据库。"""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    try:
        yield sessionmaker(engine)
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def _lead(
    session_factory: sessionmaker[Session],
    adapter: MockSmartTableAdapter,
    *,
    crm_user_id: str | None = "crm-1",
) -> str:
    """创建一条当天、本人负责且可满足 CRM 创建条件的 Lead。"""
    record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": "人工最终公司",
            "业务线": "协作机器人",
            "线索来源": "展会",
            "联系人": "王工",
            "职务": "经理",
            "沟通方式": "见面拜访",
            "手机": "13800000000",
            "备注": "人工最终备注",
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-1", crm_user_id=crm_user_id, is_authorized=True, is_active=True
            )
        )
        session.add(
            IncomingMessage(
                message_id="message-12", sales_user_id="sales-1", sequence=1, raw_payload={}
            )
        )
        lead = Lead(
            source_message_id="message-12",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            field_values={"线索名称": "过期 AI 公司", "手机": "13000000000"},
        )
        session.add(lead)
        session.flush()
        return lead.id


def test_create_uses_stable_lead_key_and_current_smart_table_values(
    session_factory: sessionmaker[Session],
) -> None:
    """验证人工当前表格值进入 payload，快照变化不改变首次创建幂等键。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    first = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    second = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))

    assert first.succeeded == 1
    assert second.succeeded == 0
    assert crm.calls == 1
    assert crm.payloads[0]["线索名称"] == "人工最终公司"
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        lead = session.get(Lead, lead_id)
    assert sync is not None
    assert sync.idempotency_key == f"crm:create:{lead_id}"
    assert sync.canonical_payload["手机"] == "13800000000"
    assert lead is not None and lead.lifecycle_state == "synced"


def test_missing_crm_mapping_or_minimum_fields_never_calls_crm(
    session_factory: sessionmaker[Session],
) -> None:
    """验证映射缺失及 CRM 最低条件失败均只形成待完善结果。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter, crm_user_id=None)
    crm = MockCRMAdapter()
    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
    )

    assert result.incomplete == 1
    assert crm.calls == 0
    with session_factory() as session:
        assert session.scalars(select(CrmSyncRecord)).all() == []


def test_non_minimum_confirmation_does_not_block_but_only_pending_contact_does(
    session_factory: sessionmaker[Session],
) -> None:
    """验证非最低字段待确认可创建，而唯一联系方式待确认必须阻塞。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"AI待确认": ["客户行业"]})
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    first = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))
    assert first.succeeded == 1
    assert crm.calls == 1

    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "pending_create"
        session.add(
            LeadFieldProvenance(
                lead_id=lead_id,
                source_message_id="message-12",
                field_name="手机",
                value="13800000000",
                last_ai_synced_value="13800000000",
            )
        )
        session.query(CrmSyncRecord).delete()
    adapter.update_record(record_id, {"AI待确认": ["手机"]})

    second = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))
    assert second.incomplete == 1
    assert crm.calls == 1


def test_transport_retry_reuses_frozen_payload_and_key_after_table_edit(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证传输失败后的 retry 永远不以销售后来编辑重建 payload 或幂等键。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    original = crm.create_lead
    failed = True

    def timeout_once(payload: object, *, idempotency_key: str, crm_user_id: str) -> object:
        """第一次模拟超时，随后交由 Mock 正常创建。"""
        nonlocal failed
        if failed:
            failed = False
            raise TimeoutError()
        return original(payload, idempotency_key=idempotency_key, crm_user_id=crm_user_id)  # type: ignore[arg-type]

    monkeypatch.setattr(crm, "create_lead", timeout_once)
    assert (
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).retrying == 1
    )
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    assert (
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).succeeded == 1
    )
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert sync is not None
    assert sync.idempotency_key == f"crm:create:{lead_id}"
    assert sync.canonical_payload["手机"] == "13800000000"


def test_update_command_requires_authorized_submitting_salesperson(
    session_factory: sessionmaker[Session],
) -> None:
    """验证更新命令仍由确定性销售授权边界保护。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    crm = MockCRMAdapter()
    with pytest.raises(ValueError, match="提交销售未授权"):
        CrmSubmissionService(session_factory, adapter, crm).submit(
            SubmissionCommand("提交我的更新", "sales-1", "message-12")
        )
    assert crm.calls == 0


def test_submission_reply_is_count_only_and_includes_update_categories() -> None:
    """验证销售汇总不泄露 payload 或联系方式，更新命令明确提示 T13。"""
    from app.crm.service import SubmissionBatchResult

    create_reply = format_submission_reply(
        SubmissionBatchResult(
            succeeded=1,
            incomplete=2,
            processing=3,
            retrying=1,
            failed_pending_review=4,
        )
    )
    update_reply = format_submission_reply(SubmissionBatchResult(updated=1, unchanged=2))

    assert "创建成功 1 条" in create_reply and "更新成功 0 条" in create_reply
    assert "手机号" not in create_reply and "payload" not in create_reply.lower()
    assert "更新成功 1 条" in update_reply and "无变化 2 条" in update_reply


def test_submission_reconcile_keeps_sales_edit_and_clears_its_pending_marker(
    session_factory: sessionmaker[Session],
) -> None:
    """验证提交协调重读当前表格、落实 T09 人工保护且不返回 AI待确认。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = next(iter(adapter.get_records())).record_id
    with session_factory.begin() as session:
        session.add(
            LeadFieldProvenance(
                lead_id=lead_id,
                source_message_id="message-12",
                field_name="线索名称",
                value="过期 AI 公司",
                last_ai_synced_value="过期 AI 公司",
            )
        )
    adapter.update_record(
        record_id,
        {"线索名称": "销售确认公司", "AI待确认": ["线索名称", "客户行业"]},
    )

    reconciled = LeadReviewService(session_factory, adapter).reconcile_submission(lead_id)

    assert reconciled.fields["线索名称"] == "销售确认公司"
    assert "AI待确认" not in reconciled.fields
    assert reconciled.blocking_fields == ()
    record = adapter.get_record(record_id)
    assert record is not None and record.fields["AI待确认"] == ["客户行业"]


def test_mock_crm_idempotency_is_stable_across_adapter_instances() -> None:
    """验证独立 Mock 适配器实例对同一键返回相同 CRM identity。"""
    payload = {"线索名称": "公司", "业务线": "协作机器人", "手机": "13800000000"}
    first = MockCRMAdapter().create_lead(
        payload, idempotency_key="crm:create:lead-1", crm_user_id="crm-1"
    )
    second = MockCRMAdapter().create_lead(
        payload, idempotency_key="crm:create:lead-1", crm_user_id="crm-1"
    )
    assert first.crm_lead_id == second.crm_lead_id


def test_update_uses_current_table_values_once_and_skips_equal_snapshot(
    session_factory: sessionmaker[Session],
) -> None:
    """验证更新以表格回读快照调用一次 CRM，重放相同业务值不再调用。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert (
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).succeeded == 1
    )
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000", "AI待确认": ["客户行业"]})

    first = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-13"))
    second = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-14"))

    assert first.updated == 1 and second.unchanged == 1
    assert crm.update_calls == 1 and crm.update_payloads[0]["手机"] == "13900000000"
    with session_factory() as session:
        updates = session.scalars(
            select(CrmSyncRecord).where(CrmSyncRecord.operation == "update")
        ).all()
    assert len(updates) == 1 and updates[0].canonical_payload.get("AI待确认") is None


def test_update_company_identity_change_enters_review_without_crm_call(
    session_factory: sessionmaker[Session],
) -> None:
    """验证已同步公司名称变化只能进入人工审查，不能自动更新 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))
    adapter.update_record(next(iter(adapter.get_records())).record_id, {"线索名称": "新公司"})

    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-13"))

    assert result.company_identity_review == 1 and crm.update_calls == 0
    with session_factory() as session:
        assert (
            session.get(Lead, lead_id).lifecycle_state == "company_identity_change_pending_review"
        )  # type: ignore[union-attr]


def test_long_message_id_uses_bounded_notification_key() -> None:
    """验证 128 字符消息标识能够生成固定长度通知键。"""
    assert len(notification_key_for_message("m" * 128)) == 64


def test_replayed_command_restores_persisted_success_to_notification_summary(
    session_factory: sessionmaker[Session],
) -> None:
    """验证通知入库前中断后，命令恢复仍汇总原 CRM 成功事实。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
    )
    with session_factory.begin() as session:
        message = session.get(IncomingMessage, "message-12")
        assert message is not None
        message.normalized_text = "提交今天的线索"
        event = OutboxEvent(
            message_id="message-12",
            sales_user_id="sales-1",
            sequence=1,
            event_type="crm_submission_command",
            status="processing",
        )
        session.add(event)
        session.flush()
        event_id = event.id
    reply = consume_submission_command(session_factory, adapter, crm, event_id)
    assert "创建成功 1 条" in reply
    with session_factory() as session:
        assert (
            session.get(NotificationRecord, notification_key_for_message("message-12")) is not None
        )
        assert session.get(Lead, lead_id).lifecycle_state == "synced"  # type: ignore[union-attr]


def test_terminal_command_failure_persists_notification_before_terminal_status(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证编排异常耗尽重试后先持久化销售失败通知再结束命令。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    with session_factory.begin() as session:
        message = session.get(IncomingMessage, "message-12")
        assert message is not None
        message.normalized_text = "提交今天的线索"
        event = OutboxEvent(
            message_id="message-12",
            sales_user_id="sales-1",
            sequence=1,
            event_type="crm_submission_command",
        )
        session.add(event)
        session.flush()
        event_id = event.id

    def broken_submit(self: object, command: object) -> object:
        """模拟提交服务发生不可预期编排异常。"""
        raise RuntimeError("internal")

    monkeypatch.setattr(CrmSubmissionService, "submit", broken_submit)
    consume_submission_command(session_factory, adapter, MockCRMAdapter(), event_id)
    consume_submission_command(session_factory, adapter, MockCRMAdapter(), event_id)
    with session_factory() as session:
        event = session.get(OutboxEvent, event_id)
        notice = session.get(
            NotificationRecord, terminal_failure_notification_key_for_message("message-12")
        )
        assert event is not None and event.status == "failed_pending_review"
        assert notice is not None and "需要人工处理" in (notice.content or "")
