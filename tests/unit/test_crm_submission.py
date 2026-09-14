"""T12 CRM 首次创建提交的应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.crm.commands import format_submission_reply
from app.crm.mock import MockCRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionCommand
from app.leads.models import CrmSyncRecord, Lead, LeadFieldProvenance
from app.leads.review import LeadReviewService
from app.messaging.models import Base, IncomingMessage, SalesAuthorization
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
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))
        .retrying
        == 1
    )
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    assert (
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).succeeded
        == 1
    )
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert sync is not None
    assert sync.idempotency_key == f"crm:create:{lead_id}"
    assert sync.canonical_payload["手机"] == "13800000000"


def test_update_command_is_explicitly_not_implemented_and_never_calls_crm(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 T12 对更新命令只反馈 T13 边界，不扫描或调用 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    crm = MockCRMAdapter()
    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交我的更新", "sales-1", "message-12")
    )

    assert result.updates_not_implemented is True
    assert crm.calls == 0


def test_submission_reply_is_count_only_and_update_mentions_t13() -> None:
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
    update_reply = format_submission_reply(SubmissionBatchResult(updates_not_implemented=True))

    assert create_reply == (
        "CRM 提交结果：创建成功 1 条；待完善或待明确确认 2 条；"
        "提交处理中 3 条；可重试失败 1 条；需人工处理失败 4 条。"
    )
    assert "手机号" not in create_reply and "payload" not in create_reply.lower()
    assert update_reply == "提交我的更新将在 T13 实现；本次未调用 CRM。"


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
