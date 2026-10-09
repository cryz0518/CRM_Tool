"""T12 CRM 首次创建提交的应用服务测试。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Generator, Mapping
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.companies.models import CompanyVerificationStatus
from app.core.config import get_settings
from app.crm.adapter import CRMCreateResult, CRMSearchResult
from app.crm.commands import (
    _link_unsubmitted_smart_table_records,
    consume_submission_command,
    format_submission_reply,
    notification_key_for_message,
    prepare_batch_submission_selection,
    prepare_company_submission_preview,
    terminal_failure_notification_key_for_message,
)
from app.crm.employee_directory import EmployeeDirectory
from app.crm.mock import MockCRMAdapter
from app.crm.service import (
    CrmSubmissionService,
    SubmissionBatchResult,
    SubmissionCommand,
    SubmissionItemResult,
    SubmissionItemStatus,
)
from app.crm.sop import SopCRMError
from app.leads.discard import LeadDiscardService, LeadDiscardStatus
from app.leads.models import (
    CrmCompanyIdentity,
    CrmSyncRecord,
    Lead,
    LeadDiscardRequest,
    LeadFieldProvenance,
)
from app.leads.review import LeadReviewService
from app.messaging.models import (
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
    WecomAction,
    utc_now,
)
from app.notifications.outbound import WecomOutboundNotificationSender
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableRecord
from app.smart_table.registry import build_required_smart_table_schema
from tests.crm_submission_test_utils import submit_today_via_selection


@pytest.fixture(autouse=True)
def employee_directory_for_crm_submission_tests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """为提交单测提供显式、隔离的 Smart Table owner 员工目录。"""
    path = tmp_path / "employee.csv"
    path.write_text("id,name,nickname\ncrm-1,sales-1,sales-1\n", encoding="utf-8")
    import app.crm.service as crm_service

    settings = get_settings().model_copy(update={"employee_directory_path": str(path)})
    monkeypatch.setattr(crm_service, "get_settings", lambda: settings)
    monkeypatch.setattr(crm_service, "_TEST_EMPLOYEE_DIRECTORY_PATH", path, raising=False)
    return path


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
            "客户行业": "其他",
            "备注": "人工最终备注，客户已确认项目需求并要求销售继续跟进，内容长度满足 CRM 校验。",
            "提交状态": "未提交",
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
            standard_company_name="人工最终公司",
            field_values={"线索名称": "过期 AI 公司", "手机": "13000000000"},
        )
        session.add(lead)
        session.flush()
        return lead.id


class RecordingCRMAdapter(MockCRMAdapter):
    """记录查重与创建 payload，验证 TYC 身份是否越过提交服务边界。"""

    def __init__(self) -> None:
        """初始化空的查重和创建 payload 记录。"""
        super().__init__()
        self.search_payloads: list[dict[str, object]] = []
        self.create_payloads: list[dict[str, object]] = []

    def search_by_company_name(
        self, payload: Mapping[str, object] | str
    ) -> tuple[CRMSearchResult, ...]:
        """记录查重请求后返回空的 CRM 结果。"""
        if isinstance(payload, str):
            self.search_payloads.append({"name": payload})
        else:
            self.search_payloads.append(dict(payload))
        return super().search_by_company_name(payload)

    def create_lead(
        self,
        payload: Mapping[str, object],
        *,
        idempotency_key: str,
        crm_user_id: str,
    ) -> CRMCreateResult:
        """记录创建请求并复用 Mock 的成功响应。"""
        self.create_payloads.append(dict(payload))
        return super().create_lead(
            payload, idempotency_key=idempotency_key, crm_user_id=crm_user_id
        )


def test_create_uses_stable_lead_key_and_current_smart_table_values(
    session_factory: sessionmaker[Session],
) -> None:
    """验证人工当前表格值进入 payload，快照变化不改变首次创建幂等键。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    first = submit_today_via_selection(service, "sales-1", "message-12")
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    second = submit_today_via_selection(service, "sales-1", "message-12")

    assert first.succeeded == 1
    assert second.succeeded == 0
    assert crm.search_calls == 1 and crm.calls == 1
    assert crm.payloads[0]["name"] == "人工最终公司"
    assert adapter.get_records()[0].fields["提交状态"] == "已提交"
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        lead = session.get(Lead, lead_id)
    assert sync is not None
    assert sync.idempotency_key == f"crm:create:{lead_id}"
    assert sync.canonical_payload["mobile"] == "13800000000"
    assert lead is not None and lead.lifecycle_state == "synced"


def test_all_submission_includes_historical_pending_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证“提交我所有线索”包含非当天且仍待创建的本人线索。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.created_at = utc_now() - timedelta(days=1)

    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    with pytest.raises(ValueError, match="批量提交必须通过"):
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-today"))
    assert crm.search_calls == 0 and crm.calls == 0
    with pytest.raises(ValueError, match="批量提交必须通过"):
        service.submit(SubmissionCommand("提交我所有线索", "sales-1", "message-all"))
    all_result = service.submit_selected(
        SubmissionCommand("提交我所有线索", "sales-1", "message-all"),
        (lead_id,),
    )

    assert all_result.succeeded == 1
    assert crm.calls == 1


def test_all_submission_does_not_reopen_abandoned_sync_after_table_tampering(
    session_factory: sessionmaker[Session],
) -> None:
    """验证销售手动改写提交状态不能重新打开服务端已放弃事实。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    record = adapter.get_records()[0]
    crm = MockCRMAdapter(
        search_results={
            "人工最终公司": (CRMSearchResult("crm-existing", "crm-owner", "CRM 已有线索"),)
        }
    )
    service = CrmSubmissionService(session_factory, adapter, crm)

    first = submit_today_via_selection(service, "sales-1", "message-abandoned")
    service.resolve_duplicate_confirmation(
        "message-abandoned", "sales-1", continue_submission=False
    )
    assert first.duplicate_confirmations
    adapter.update_record(record.record_id, {"提交状态": "未提交"})
    with pytest.raises(ValueError, match="批量提交必须通过"):
        service.submit(SubmissionCommand("提交我所有线索", "sales-1", "message-reopened"))
    assert crm.search_calls == 1 and crm.calls == 0


def test_all_submission_does_not_turn_submitted_record_into_update(
    session_factory: sessionmaker[Session],
) -> None:
    """验证“提交我所有线索”只处理待创建线索，不把已提交记录转成 update。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    created = submit_today_via_selection(service, "sales-1", "message-create")
    assert created.succeeded == 1
    record = adapter.get_records()[0]
    adapter.update_record(
        record.record_id,
        {"手机": "13900000000", "提交状态": "未提交"},
    )

    with pytest.raises(ValueError, match="批量提交必须通过"):
        service.submit(SubmissionCommand("提交我所有线索", "sales-1", "message-all"))
    assert crm.calls == 1
    assert crm.update_calls == 0


def test_owner_user_id_is_converted_to_employee_name_before_crm_submit(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证真实成员字段的 userId 先转换为 userName，再解析 employee.id。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    fields = {
        "负责人": "wecom-owner",
        "线索名称": "企微成员姓名转换测试",
        "业务线": "协作机器人",
        "线索来源": "展会",
        "联系人": "王工",
        "职务": "经理",
        "沟通方式": "见面拜访",
        "手机": "13800000000",
        "客户行业": "其他",
        "备注": "验证提交前的成员身份转换，不包含真实客户信息，内容仅用于离线测试。",
        "提交状态": "未提交",
    }
    record = adapter.create_record(fields, actor=SmartTableActor.ROBOT)
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="wecom-owner",
                crm_user_id="crm-1",
                is_authorized=True,
                is_active=True,
            )
        )
        session.add(
            IncomingMessage(
                message_id="message-member-name",
                sales_user_id="wecom-owner",
                sequence=1,
                raw_payload={},
            )
        )
        lead = Lead(
            source_message_id="message-member-name",
            original_capturing_sales_user_id="wecom-owner",
            smart_table_owner_user_id="wecom-owner",
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            standard_company_name="企微成员姓名转换测试",
            field_values={},
        )
        session.add(lead)
        session.flush()

    original_get_record = adapter.get_record

    def get_record_with_member_name(record_id: str) -> SmartTableRecord | None:
        """给 Mock 记录补充真实 CLI 会返回的 userName 元数据。"""
        current = original_get_record(record_id)
        if current is None:
            return None
        return SmartTableRecord(
            record_id=current.record_id,
            fields=current.fields,
            member_names={"负责人": "sales-1"},
        )

    monkeypatch.setattr(adapter, "get_record", get_record_with_member_name)
    crm = MockCRMAdapter()

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "wecom-owner",
        "message-member-name",
    )

    assert result.succeeded == 1
    assert crm.crm_user_ids == ["crm-1"]


def test_missing_member_display_name_rejects_opaque_owner_id(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证智能表格没有可信成员显示名时不会把负责人 userId 当员工姓名。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    original_get_record = adapter.get_record

    def get_record_without_member_name(record_id: str) -> SmartTableRecord | None:
        """返回不含成员显示名的智能表格快照。"""
        record = original_get_record(record_id)
        if record is None:
            return None
        return SmartTableRecord(record.record_id, record.fields, member_names={})

    monkeypatch.setattr(adapter, "get_record", get_record_without_member_name)
    crm = MockCRMAdapter()

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )

    assert result.mapping_missing == 1
    assert crm.search_calls == 0
    assert crm.calls == 0


@pytest.mark.parametrize(
    ("category", "http_status", "expected_status", "expected_failure_category"),
    [
        ("transport", None, "retrying", "transient"),
        ("authentication", 401, "failed_pending_review", "permanent"),
        ("business", 200, "failed_pending_review", "permanent"),
        ("gateway", 403, "failed_pending_review", "permanent"),
        ("gateway", 503, "retrying", "transient"),
        ("malformed_response", 200, "failed_pending_review", "permanent"),
    ],
)
def test_sop_error_category_controls_submission_failure_semantics(
    session_factory: sessionmaker[Session],
    category: str,
    http_status: int | None,
    expected_status: str,
    expected_failure_category: str,
) -> None:
    """验证提交服务按 SopCRMError.category 保存重试或人工失败语义。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)

    class CategorizedCRM(MockCRMAdapter):
        """抛出带稳定 category 的 SOP 错误，模拟真实 Adapter。"""

        def create_lead(
            self,
            payload: Mapping[str, object],
            *,
            idempotency_key: str,
            crm_user_id: str,
        ) -> CRMCreateResult:
            """不发送外部请求，返回指定分类的错误。"""
            del payload, idempotency_key, crm_user_id
            raise SopCRMError("sanitized CRM failure", category=category, http_status=http_status)

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, CategorizedCRM()),
        "sales-1",
        "message-12",
    )

    if expected_status == "retrying":
        assert result.retrying == 1
    else:
        assert result.failed_pending_review == 1
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert sync is not None
    assert sync.status == expected_status
    assert sync.failure_category == expected_failure_category


def test_sop_transport_during_duplicate_search_reports_retrying(
    session_factory: sessionmaker[Session],
) -> None:
    """验证查重尚未建立 Sync 时的 SOP 传输故障也返回可重试结果。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)

    class SearchTransportCRM(MockCRMAdapter):
        """模拟查重请求在收到响应前发生的暂态故障。"""

        def search_by_company_name(
            self, payload: Mapping[str, object] | str
        ) -> tuple[CRMSearchResult, ...]:
            """抛出明确的 transport 分类。"""
            del payload
            raise SopCRMError("sanitized transport", category="transport")

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, SearchTransportCRM()),
        "sales-1",
        "message-12",
    )

    assert result.retrying == 1


def test_duplicate_search_failure_persists_controlled_transport_evidence(
    session_factory: sessionmaker[Session],
) -> None:
    """验证查重失败审计保存分类、状态和协议码，不保存远端正文。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)

    class SearchFailureCRM(MockCRMAdapter):
        """模拟带稳定协议码的 CRM 查重网关失败。"""

        def search_by_company_name(
            self, payload: Mapping[str, object] | str
        ) -> tuple[CRMSearchResult, ...]:
            """抛出受控的查重传输故障。"""
            del payload
            raise SopCRMError(
                "raw SOP response must not persist",
                category="gateway",
                http_status=503,
                error_code="GW_DUPLICATE",
                sub_code="DUP_RETRY",
            )

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, SearchFailureCRM()),
        "sales-1",
        "message-12",
    )

    assert result.retrying == 1
    with session_factory() as session:
        audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == "message-12",
                BusinessAuditEvent.event_type.startswith("crm_duplicate_search_failed:"),
            )
        )
    assert audit is not None
    assert audit.details == {
        "lead_id": lead_id,
        "failure_category": "transient",
        "adapter_category": "gateway",
        "http_status": 503,
        "failure_code": "DUP_RETRY",
    }
    assert "raw SOP" not in str(audit.details)


def test_duplicate_search_failure_evidence_is_persisted_per_lead(
    session_factory: sessionmaker[Session], caplog: pytest.LogCaptureFixture
) -> None:
    """验证同一请求中的不同 Lead 各自保留独立受控查重失败证据。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first_id = _lead(session_factory, adapter)
    second_fields = dict(adapter.get_records()[0].fields)
    second_fields.update({"线索名称": "第二家测试公司", "联系人": "李工"})
    second_record = adapter.create_record(second_fields, actor=SmartTableActor.ROBOT)
    with session_factory.begin() as session:
        second = Lead(
            source_message_id="message-12",
            source_segment_index=1,
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=second_record.record_id,
            lifecycle_state="pending_create",
            standard_company_name="第二家测试公司",
            field_values={"线索名称": "第二家测试公司"},
        )
        session.add(second)
        session.flush()
        second_id = second.id

    class PerLeadSearchFailureCRM(MockCRMAdapter):
        """按公司返回两类受控 HTTP 200 查重失败，不保留响应正文。"""

        def search_by_company_name(
            self, payload: Mapping[str, object] | str
        ) -> tuple[CRMSearchResult, ...]:
            """分别模拟格式错误响应和业务错误码。"""
            company = payload if isinstance(payload, str) else payload.get("name")
            if company == "人工最终公司":
                raise SopCRMError(
                    "private malformed body", category="malformed_response", http_status=200
                )
            raise SopCRMError(
                "private business body",
                category="business",
                http_status=200,
                error_code="X",
            )

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, PerLeadSearchFailureCRM()),
        "sales-1",
        "per-lead-search-failure",
    )

    assert result.failed_pending_review == 2
    items = {item.lead_id: item for item in result.items}
    malformed = items[first_id]
    assert malformed.failure_category == "permanent"
    assert malformed.adapter_category == "malformed_response"
    assert malformed.http_status == 200
    assert malformed.failure_code is None
    business = items[second_id]
    assert business.failure_category == "permanent"
    assert business.adapter_category == "business"
    assert business.http_status == 200
    assert business.failure_code == "X"
    reply = format_submission_reply(
        result,
        lead_labels={
            first_id: "希捷国际科技（无锡）有限公司｜杨总",
            second_id: "第二家测试公司｜李工",
        },
        selected_count=2,
    )
    assert "希捷国际科技（无锡）有限公司｜杨总：CRM 返回格式异常（HTTP 200）" in reply
    assert "第二家测试公司｜李工：CRM 返回业务错误（HTTP 200，错误码：X）" in reply
    assert "private malformed body" not in reply
    assert "private business body" not in reply
    assert "private malformed body" not in str(items)
    assert "private business body" not in str(items)
    assert "private malformed body" not in caplog.text
    assert "private business body" not in caplog.text
    with session_factory() as session:
        evidence = session.scalars(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == "per-lead-search-failure",
                BusinessAuditEvent.event_type.startswith("crm_duplicate_search_failed:"),
            )
        ).all()
    assert {event.details["lead_id"] for event in evidence} == {first_id, second_id}
    assert {event.details.get("http_status") for event in evidence} == {200}
    assert {
        (event.details.get("adapter_category"), event.details.get("failure_code"))
        for event in evidence
    } == {("malformed_response", None), ("business", "X")}
    evidence_by_lead = {event.details["lead_id"]: event.details for event in evidence}
    for lead_id, item in items.items():
        event = evidence_by_lead[lead_id]
        assert item.failure_category == event["failure_category"]
        assert item.adapter_category == event["adapter_category"]
        assert item.http_status == event["http_status"]
        assert item.failure_code == event["failure_code"]
    assert "private" not in str([event.details for event in evidence])


@pytest.mark.parametrize(
    ("adapter_category", "http_status", "failure_code", "expected"),
    [
        (
            "authentication",
            401,
            "AUTH_DENIED",
            "CRM 鉴权失败",
        ),
        (
            "transport",
            None,
            "timeout",
            "CRM 网络连接或超时异常",
        ),
    ],
)
def test_duplicate_search_failure_reply_maps_safe_category(
    adapter_category: str,
    http_status: int | None,
    failure_code: str,
    expected: str,
) -> None:
    """验证查重鉴权与网络失败展示安全分类，不泄露外部异常正文。"""
    from app.crm.service import _append_create_result, _create_outcome

    outcome = _create_outcome(
        "retrying" if adapter_category == "transport" else "failed_pending_review",
        reason_code=(
            "crm_duplicate_search_retrying"
            if adapter_category == "transport"
            else "crm_duplicate_search_failed"
        ),
        failure_category="transient" if adapter_category == "transport" else "permanent",
        adapter_category=adapter_category,
        http_status=http_status,
        failure_code=failure_code,
    )
    result = _append_create_result(SubmissionBatchResult(), "lead-safe", outcome)
    reply = format_submission_reply(result, lead_labels={"lead-safe": "测试公司｜联系人"})

    assert expected in reply
    assert "错误码：AUTH_DENIED" in reply if adapter_category == "authentication" else True
    assert "本次未提交，请稍后重新提交" in reply if adapter_category == "transport" else True
    assert "traceback" not in reply.lower()


def test_duplicate_target_without_lead_id_has_its_own_safe_result_and_reply() -> None:
    """验证查重命中但无可操作线索 ID 使用重复待确认语义。"""
    from app.crm.service import _append_create_result, _create_outcome

    outcome = _create_outcome(
        "failed_pending_review",
        reason_code="duplicate_target_unavailable",
        failure_category="permanent",
        adapter_category="duplicate_target_unavailable",
        failure_code="duplicate_detected_without_lead_id",
        duplicate_entity_type="lead",
    )
    result = _append_create_result(SubmissionBatchResult(), "lead-seagate", outcome)
    item = result.items[0]
    reply = format_submission_reply(
        result,
        lead_labels={"lead-seagate": "希捷国际科技（无锡）有限公司｜杨总"},
        selected_count=1,
    )

    assert item.reason_code == "duplicate_target_unavailable"
    assert item.duplicate_entity_type == "lead"
    assert "❌ 重复待人工确认 1 条" in reply
    expected_detail = (
        "希捷国际科技（无锡）有限公司｜杨总：CRM 检测到重复线索，"
        "但未返回可操作的线索 ID，请人工确认。"
    )
    assert expected_detail in reply
    assert "CRM 返回业务错误" not in reply
    assert "leadId=null" not in reply


def test_duplicate_target_unavailable_is_audited_and_never_calls_create(
    session_factory: sessionmaker[Session], caplog: pytest.LogCaptureFixture
) -> None:
    """验证无可操作重复目标逐 Lead 保存受控事实且不会创建 CRM 线索。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)

    class DuplicateTargetCRM(MockCRMAdapter):
        """模拟已确认重复但没有可操作线索 ID 的查重响应。"""

        def search_by_company_name(
            self, payload: Mapping[str, object] | str
        ) -> tuple[CRMSearchResult, ...]:
            """只抛出不含远端正文的受控错误事实。"""
            del payload
            raise SopCRMError(
                "controlled duplicate target unavailable",
                category="duplicate_target_unavailable",
                sub_code="duplicate_detected_without_lead_id",
                duplicate_entity_type="lead",
            )

    crm = DuplicateTargetCRM()
    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-duplicate-target",
    )

    assert result.failed_pending_review == 1
    assert crm.calls == 0
    item = result.items[0]
    assert item.lead_id == lead_id
    assert item.reason_code == "duplicate_target_unavailable"
    assert item.failure_category == "permanent"
    assert item.adapter_category == "duplicate_target_unavailable"
    assert item.failure_code == "duplicate_detected_without_lead_id"
    assert item.duplicate_entity_type == "lead"
    reply = format_submission_reply(
        result, lead_labels={lead_id: "测试公司｜联系人"}, selected_count=1
    )
    assert "测试公司｜联系人：CRM 检测到重复线索" in reply
    assert "controlled duplicate target unavailable" not in reply
    assert "controlled duplicate target unavailable" not in caplog.text

    with session_factory() as session:
        audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == "message-duplicate-target",
                BusinessAuditEvent.event_type.startswith("crm_duplicate_search_failed:"),
            )
        )
    assert audit is not None
    assert audit.details["duplicate_entity_type"] == "lead"
    assert audit.details["failure_code"] == "duplicate_detected_without_lead_id"
    assert "controlled duplicate target unavailable" not in str(audit.details)


def test_sales_authorization_crm_mapping_is_not_required_when_owner_directory_matches(
    session_factory: sessionmaker[Session],
) -> None:
    """验证当前销售仅凭表格负责人和员工目录匹配即可提交 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter, crm_user_id=None)
    crm = MockCRMAdapter()

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )

    assert result.succeeded == 1
    assert result.mapping_missing == 0
    assert crm.search_calls == 1
    assert crm.calls == 1
    assert crm.crm_user_ids == ["crm-1"]
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert sync is not None
    assert sync.submitting_crm_user_id == "crm-1"


def test_tyc_unique_identity_is_sent_to_crm(
    session_factory: sessionmaker[Session],
) -> None:
    """验证唯一核验的天眼查 ID 才会进入 CRM payload。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.company_verification_status = CompanyVerificationStatus.TYC_VERIFIED.value
        lead.tyc_customer_id = "tyc-verified"
    crm = RecordingCRMAdapter()

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )

    assert result.succeeded == 1
    assert crm.search_payloads[0]["tycCustomerId"] == "tyc-verified"
    assert crm.create_payloads[0]["tycCustomerId"] == "tyc-verified"


def test_tyc_ambiguous_candidate_id_never_enters_crm(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 ambiguous 的首候选 ID 即使残留在 Lead 也不会发送 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.company_verification_status = CompanyVerificationStatus.COMPANY_UNVERIFIED.value
        lead.tyc_customer_id = "stale-ambiguous-id"
    crm = RecordingCRMAdapter()

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )

    assert result.succeeded == 1
    assert "tycCustomerId" not in crm.search_payloads[0]
    assert "tycCustomerId" not in crm.create_payloads[0]


def test_salesperson_company_name_edit_drops_old_tyc_candidate_id(
    session_factory: sessionmaker[Session],
) -> None:
    """验证销售改写预填公司名后不会携带旧天眼查 ID。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.company_verification_status = CompanyVerificationStatus.TYC_VERIFIED.value
        lead.tyc_customer_id = "stale-after-edit"
        lead.standard_company_name = "人工最终公司"
    record = adapter.get_records()[0]
    adapter.update_record(record.record_id, {"线索名称": "销售修改后的公司"})
    crm = RecordingCRMAdapter()

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )

    assert result.succeeded == 1
    assert "tycCustomerId" not in crm.search_payloads[0]
    assert "tycCustomerId" not in crm.create_payloads[0]


def test_targeted_company_submission_reuses_service_for_one_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证确认卡只把一个目标 Lead 交给既有提交服务，不扩大为批量提交。"""

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    target_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand(
            "提交指定线索",
            "sales-1",
            "message-target",
            target_lead_id=target_id,
        )
    )

    assert result.succeeded == 1
    assert crm.calls == 1
    with session_factory() as session:
        assert session.get(Lead, target_id).lifecycle_state == "synced"  # type: ignore[union-attr]


def test_targeted_company_submission_accepts_complete_temporary_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证单条确认卡的 temporary 目标按当前完整快照进入 CRM 查重。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    target_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, target_id)
        assert lead is not None
        lead.lifecycle_state = "temporary"
    crm = MockCRMAdapter()

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand(
            "提交指定线索",
            "sales-1",
            "temporary-target-message",
            target_lead_id=target_id,
        )
    )

    assert result.succeeded == 1
    assert crm.search_calls == 1
    assert crm.calls == 1


def test_targeted_temporary_lead_reports_missing_required_fields_before_crm(
    session_factory: sessionmaker[Session],
) -> None:
    """验证临时线索缺少 CRM 必填项时给出缺项，不提前返回空原因。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    target_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, target_id)
        assert lead is not None and lead.smart_table_record_id is not None
        lead.lifecycle_state = "temporary"
        record_id = lead.smart_table_record_id
    adapter.update_record(record_id, {"职务": ""})
    crm = MockCRMAdapter()

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand(
            "提交指定线索",
            "sales-1",
            "temporary-incomplete-message",
            target_lead_id=target_id,
        )
    )

    assert result.incomplete == 1
    assert "职务" in result.incomplete_missing_fields
    assert crm.search_calls == 0
    assert crm.calls == 0
    with session_factory() as session:
        lead = session.get(Lead, target_id)
    assert lead is not None and lead.lifecycle_state == "temporary"


def test_incomplete_retry_reloads_table_and_never_recreates_successful_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证待完善重提每次重读表格，已成功线索不会被再次提交。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record = next(iter(adapter.get_records()))
    adapter.update_record(record.record_id, {"职务": ""})
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "temporary"
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    first = service.submit_incomplete_retry(
        SubmissionCommand("重新提交待完善的线索", "sales-1", "retry-incomplete-1"),
        (lead_id,),
    )

    assert len(first.items) == 1
    assert first.items[0].status is SubmissionItemStatus.INCOMPLETE
    assert first.items[0].missing_fields == ("职务",)
    assert crm.search_calls == 0 and crm.calls == 0
    adapter.update_record(record.record_id, {"职务": "项目经理"})

    second = service.submit_incomplete_retry(
        SubmissionCommand("重新提交待完善的线索", "sales-1", "retry-incomplete-2"),
        (lead_id,),
    )
    third = service.submit_incomplete_retry(
        SubmissionCommand("重新提交待完善的线索", "sales-1", "retry-incomplete-3"),
        (lead_id,),
    )

    assert second.succeeded == 1
    assert crm.search_calls == 1 and crm.calls == 1
    assert len(third.items) == 1
    assert third.items[0].status is SubmissionItemStatus.NOT_SUBMITTED
    assert third.items[0].reason_code == "already_submitted"
    assert crm.search_calls == 1 and crm.calls == 1


def test_incomplete_retry_fails_closed_after_owner_or_table_status_change(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 owner 转移或 Smart Table 已非未提交时均不进入 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record = next(iter(adapter.get_records()))
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "temporary"
        lead.smart_table_owner_user_id = "sales-2"
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    owner_changed = service.submit_incomplete_retry(
        SubmissionCommand("重新提交待完善的线索", "sales-1", "retry-owner-changed"),
        (lead_id,),
    )
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-1"
    adapter.update_record(record.record_id, {"提交状态": "已提交"})
    status_changed = service.submit_incomplete_retry(
        SubmissionCommand("重新提交待完善的线索", "sales-1", "retry-status-changed"),
        (lead_id,),
    )

    assert owner_changed.items[0].status is SubmissionItemStatus.NOT_SUBMITTED
    assert owner_changed.items[0].reason_code == "candidate_state_changed"
    assert status_changed.items[0].status is SubmissionItemStatus.NOT_SUBMITTED
    assert status_changed.items[0].reason_code == "candidate_state_changed"
    assert crm.search_calls == 0 and crm.calls == 0


def test_retry_command_aggregates_incomplete_results_from_all_pages(
    session_factory: sessionmaker[Session],
) -> None:
    """验证重提按原 request_message_id 聚合多页选择且使用当前表格缺项。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first_lead_id = _lead(session_factory, adapter)
    first_record = next(iter(adapter.get_records()))
    adapter.update_record(first_record.record_id, {"职务": ""})
    second_record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": "第二条测试公司",
            "业务线": "",
            "线索来源": "展会",
            "联系人": "李工",
            "职务": "经理",
            "沟通方式": "微信",
            "手机": "13800000001",
            "客户行业": "其他",
            "备注": "已确认测试需求，销售后续跟进。",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        first_lead = session.get(Lead, first_lead_id)
        assert first_lead is not None
        first_lead.lifecycle_state = "temporary"
        second_message = IncomingMessage(
            message_id="retry-page-source-2",
            sales_user_id="sales-1",
            sequence=2,
            raw_payload={},
        )
        second_lead = Lead(
            source_message_id=second_message.message_id,
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=second_record.record_id,
            lifecycle_state="temporary",
            standard_company_name="第二条测试公司",
            field_values=dict(second_record.fields),
        )
        session.add_all([second_message, second_lead])
        session.flush()
        second_lead_id = second_lead.id
        session.add_all(
            [
                WecomAction(
                    task_id="retry-page-1",
                    action_type="crm_batch_submission",
                    bound_actor_wecom_user_id="sales-1",
                    target_type="crm_batch_submission",
                    target_id="page-1",
                    expected_action_key="crm.batch_submission.confirm",
                    status="succeeded",
                    expires_at=utc_now() + timedelta(minutes=5),
                    context={
                        "request_message_id": "original-paged-request",
                        "candidate_leads": [
                            {
                                "lead_id": first_lead_id,
                                "company_name": "第一条测试公司",
                                "display_text": "第一条测试公司｜王工",
                            }
                        ],
                        "selected_lead_ids": [first_lead_id],
                        "submission_results": [
                            {
                                "lead_id": first_lead_id,
                                "status": "incomplete",
                                "reason_code": "missing_required_fields",
                                "missing_fields": ["旧字段快照不可复用"],
                            }
                        ],
                    },
                ),
                WecomAction(
                    task_id="retry-page-2",
                    action_type="crm_batch_submission",
                    bound_actor_wecom_user_id="sales-1",
                    target_type="crm_batch_submission",
                    target_id="page-2",
                    expected_action_key="crm.batch_submission.confirm",
                    status="succeeded",
                    expires_at=utc_now() + timedelta(minutes=5),
                    context={
                        "request_message_id": "original-paged-request",
                        "candidate_leads": [
                            {
                                "lead_id": second_lead_id,
                                "company_name": "第二条测试公司",
                                "display_text": "第二条测试公司｜李工",
                            }
                        ],
                        "selected_lead_ids": [second_lead_id],
                        "submission_results": [
                            {
                                "lead_id": second_lead_id,
                                "status": "incomplete",
                                "reason_code": "missing_required_fields",
                                "missing_fields": ["另一个旧字段"],
                            }
                        ],
                    },
                ),
            ]
        )
        retry_message = IncomingMessage(
            message_id="retry-current-request",
            sales_user_id="sales-1",
            sequence=3,
            raw_payload={},
            normalized_text="重新提交",
        )
        session.add(retry_message)
        session.flush()
        retry_event = OutboxEvent(
            message_id=retry_message.message_id,
            sales_user_id="sales-1",
            sequence=3,
            event_type="crm_submission_intent",
            status="processing",
        )
        session.add(retry_event)
        session.flush()
        retry_event_id = retry_event.id

    crm = MockCRMAdapter()
    reply = consume_submission_command(
        session_factory,
        adapter,
        crm,
        retry_event_id,
        command_text="重新提交待完善的线索",
    )

    assert "重新提交结果（共 2 条）" in reply
    assert "第一条测试公司｜王工：缺少「职务」" in reply
    assert "第二条测试公司｜李工：缺少「业务线」" in reply
    assert "旧字段快照不可复用" not in reply
    assert crm.search_calls == 0 and crm.calls == 0
    with session_factory() as session:
        prior_actions = session.scalars(
            select(WecomAction).where(
                WecomAction.target_id.in_(("page-1", "page-2"))
            )
        ).all()
        retry_notification = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.source_message_id == "retry-current-request",
                NotificationRecord.notification_type == "crm_submission_retry_summary",
            )
        )
    assert all(
        any(
            result["status"] == "incomplete"
            for result in action.context["submission_results"]
        )
        for action in prior_actions
    )
    assert retry_notification is not None and retry_notification.payload["reply"] == reply


def _retry_command_event(
    session_factory: sessionmaker[Session], lead_ids: tuple[str, ...],
    *, message_id: str = "retry-feedback", sequence: int = 2,
) -> int:
    """持久化本人实际选择且待完善的历史及重提命令，返回事件标识。

    参数：lead_ids 为服务端选择目标；message_id/sequence 限定本次来源消息。
    返回值：可交给现有命令消费者的 Outbox 标识。
    异常：数据库错误向测试传播。副作用：仅写入内存测试库。
    """
    with session_factory.begin() as session:
        session.add(WecomAction(
            task_id=f"prior-{message_id}",
            action_type="crm_batch_submission",
            bound_actor_wecom_user_id="sales-1",
            target_type="crm_batch_submission",
            target_id=f"prior-{message_id}",
            expected_action_key="crm.batch_submission.confirm",
            status="succeeded",
            expires_at=utc_now() + timedelta(minutes=5),
            context={
                "request_message_id": f"prior-{message_id}",
                "selected_lead_ids": list(lead_ids),
                "candidate_leads": [
                    {"lead_id": lead_id, "company_name": f"反馈线索{index}"}
                    for index, lead_id in enumerate(lead_ids, start=1)
                ],
                "submission_results": [
                    {"lead_id": lead_id, "status": "incomplete"}
                    for lead_id in lead_ids
                ],
            },
        ))
        session.add(IncomingMessage(
            message_id=message_id, sales_user_id="sales-1", sequence=sequence,
            raw_payload={}, normalized_text="重新提交",
        ))
        session.flush()
        event = OutboxEvent(
            message_id=message_id, sales_user_id="sales-1", sequence=sequence,
            event_type="crm_submission_intent", status="processing",
        )
        session.add(event)
        session.flush()
        return event.id


@pytest.mark.parametrize("failure", ["none", "once", "always", "business", "missing"])
def test_retry_feedback_covers_real_results_and_is_idempotent(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    """验证重提成功、缺项、暂时/终态失败，命令重放及通知重试均无重复业务调用。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    event_id = _retry_command_event(session_factory, (lead_id,))
    if failure == "missing":
        adapter.update_record(adapter.get_records()[0].record_id, {"职务": ""})
    crm = MockCRMAdapter()
    create = crm.create_lead
    attempts: list[str] = []

    def create_with_failure(
        payload: Mapping[str, object], *, idempotency_key: str, crm_user_id: str,
    ) -> CRMCreateResult:
        """按场景制造受控失败并记录幂等键，成功时复用原 Mock。"""
        attempts.append(idempotency_key)
        if failure == "always" or (failure == "once" and len(attempts) == 1):
            raise ConnectionError("isolated failure")
        if failure == "business":
            raise SopCRMError("business", http_status=400, error_code="40001")
        return create(payload, idempotency_key=idempotency_key, crm_user_id=crm_user_id)

    monkeypatch.setattr(crm, "create_lead", create_with_failure)
    # 仅使用伪 SDK，覆盖通知失败、租约重试与发送完成后的再次扫描。
    client = Mock()
    client.send_message = AsyncMock(side_effect=ConnectionError("isolated send failure"))
    sender = WecomOutboundNotificationSender(session_factory, client)
    first = consume_submission_command(
        session_factory, adapter, crm, event_id, command_text="重新提交待完善的线索",
    )
    if failure in {"once", "always"}:
        assert "🔄 重试中 1 条" in first and "✅ 创建成功 0 条" in first
        with session_factory() as session:
            assert session.get(OutboxEvent, event_id).status == "retrying"
    assert asyncio.run(sender.send_pending_once()) == 0
    client.send_message = AsyncMock(return_value={"msgid": "isolated"})
    assert asyncio.run(sender.send_pending_once()) > 0
    for _ in range(4):
        reply = consume_submission_command(
            session_factory, adapter, crm, event_id, command_text="重新提交待完善的线索",
        )
        with session_factory() as session:
            if session.get(OutboxEvent, event_id).status == "succeeded":
                break
    assert asyncio.run(sender.send_pending_once()) == (1 if failure in {"once", "always"} else 0)
    assert asyncio.run(sender.send_pending_once()) == 0
    if failure in {"none", "once"}:
        assert "✅ 创建成功 1 条" in reply and crm.calls == 1
    elif failure == "missing":
        assert "反馈线索1：缺少「职务」" in reply and crm.calls == crm.search_calls == 0
    else:
        assert "需人工处理 1 条" in reply and "✅ 创建成功 0 条" in reply
    call_count = len(attempts)
    assert consume_submission_command(
        session_factory, adapter, crm, event_id, command_text="重新提交待完善的线索",
    ) == reply
    assert len(attempts) == call_count and len(set(attempts)) <= 1
    with session_factory() as session:
        assert session.get(OutboxEvent, event_id).status == "succeeded"
        notices = session.scalars(select(NotificationRecord)).all()
    assert all(notice.status == "succeeded" and len(notice.content) <= 512 for notice in notices)
    delivered = "\n".join(
        call.args[1]["markdown"]["content"] for call in client.send_message.call_args_list
    )
    assert reply in delivered


@pytest.mark.parametrize("failure", ["once", "business", "snapshot"])
def test_retry_freezes_scope_and_preserves_partial_success(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    """验证多条部分成功继续处理，重放不扩大范围且最终保留已成功条目。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first_id = _lead(session_factory, adapter)
    first_record = adapter.get_records()[0]
    second_record = adapter.create_record(
        {**first_record.fields, "线索名称": "第二家反馈公司"}, actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        second = Lead(
            source_message_id="message-12", source_segment_index=1,
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1", smart_table_record_id=second_record.record_id,
            lifecycle_state="pending_create", standard_company_name="第二家反馈公司",
            field_values=dict(second_record.fields),
        )
        session.add(second)
        session.flush()
        second_id = second.id
    event_id = _retry_command_event(session_factory, (first_id, second_id))
    crm = MockCRMAdapter()
    create = crm.create_lead
    failed = False

    def create_with_second_failure(
        payload: Mapping[str, object], *, idempotency_key: str, crm_user_id: str,
    ) -> CRMCreateResult:
        """仅让第二家公司首次创建暂时失败，验证首条不被后续重放覆盖。"""
        nonlocal failed
        if failure == "business" and payload["name"] == "人工最终公司":
            raise SopCRMError("isolated rejection")
        if failure == "once" and payload["name"] == "第二家反馈公司" and not failed:
            failed = True
            raise ConnectionError("isolated")
        return create(payload, idempotency_key=idempotency_key, crm_user_id=crm_user_id)

    monkeypatch.setattr(crm, "create_lead", create_with_second_failure)
    if failure == "snapshot":
        get_record = adapter.get_record

        def get_record_with_failure(record_id: str) -> SmartTableRecord | None:
            """仅让第一条快照读取失败，验证后续条目依然创建。"""
            if record_id == first_record.record_id:
                raise ValueError("isolated snapshot failure")
            return get_record(record_id)

        monkeypatch.setattr(adapter, "get_record", get_record_with_failure)
    first = consume_submission_command(
        session_factory, adapter, crm, event_id, command_text="重新提交待完善的线索",
    )
    assert "✅ 创建成功 1 条" in first
    assert ("🔄 重试中 1 条" if failure == "once" else "需人工处理 1 条") in first
    # 重放期间历史选择改变；冻结目标仍须继续原来的第二条，而不能回退/扩大。
    _retry_command_event(
        session_factory, ("unselected-later-lead",), message_id="later", sequence=3,
    )
    final = consume_submission_command(
        session_factory, adapter, crm, event_id, command_text="重新提交待完善的线索",
    )
    succeeded = 2 if failure == "once" else 1
    assert f"✅ 创建成功 {succeeded} 条" in final
    assert "反馈线索1" in final and "反馈线索2" in final
    assert "unselected-later-lead" not in final and crm.calls == succeeded
    with session_factory() as session:
        expected_syncs = 1 if failure == "snapshot" else 2
        assert session.scalar(select(func.count()).select_from(CrmSyncRecord)) == expected_syncs


def test_retry_recovers_committed_success_and_new_command_reports_already_submitted(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证提交成功后崩溃的 Worker 恢复真实成功，新命令明确反馈已提交。"""
    from app.ai.models import SubmissionIntent
    from workers import tasks

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    event_id = _retry_command_event(session_factory, (lead_id,))
    crm = MockCRMAdapter()
    result = CrmSubmissionService(session_factory, adapter, crm).submit_incomplete_retry(
        SubmissionCommand("重新提交待完善的线索", "sales-1", "retry-feedback"), (lead_id,),
    )
    assert result.succeeded == 1 and crm.calls == 1
    # 通过真实 Worker 意图映射进入消费者；依赖均为测试库和 Mock，无外部副作用。
    engine = session_factory.kw["bind"]
    gateway = Mock()
    gateway.classify_submission_intent.return_value = SubmissionIntent(
        intent="SUBMIT_RETRY_INCOMPLETE",
    )
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(tasks, "_session_factory", lambda: (engine, session_factory))
    monkeypatch.setattr(tasks, "_take_lead_outbox_claim", lambda *_args: True)
    monkeypatch.setattr(tasks, "get_smart_table_adapter", lambda: adapter)
    monkeypatch.setattr(tasks, "get_crm_adapter", lambda: crm)
    monkeypatch.setattr(tasks, "get_ai_gateway", lambda: gateway)
    reply = tasks.consume_lead_outbox_event.run(event_id, utc_now().isoformat())
    assert "✅ 创建成功 1 条" in reply and crm.calls == 1
    gateway.classify_submission_intent.assert_called_once_with("重新提交")
    # 使用新的来源消息重复发命令时仍沿用历史选择，明确返回已提交而不创建新 generation。
    next_event = _retry_command_event(
        session_factory, (lead_id,), message_id="repeated-command", sequence=3,
    )
    repeated = consume_submission_command(
        session_factory, adapter, crm, next_event, command_text="重新提交待完善的线索",
    )
    assert "该线索已完成提交，本次未重复创建" in repeated
    assert "✅ 创建成功 0 条" in repeated and crm.calls == 1


def test_retry_history_uses_latest_selection_and_excludes_unselected_results(
    session_factory: sessionmaker[Session],
) -> None:
    """验证最近选择已成功时不回溯旧组，伪造未勾选的待完善结果不进入重提。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    _retry_command_event(session_factory, (lead_id,))
    event_id = _retry_command_event(session_factory, (lead_id,), message_id="latest", sequence=3)
    with session_factory.begin() as session:
        action = session.scalar(select(WecomAction).where(WecomAction.task_id == "prior-latest"))
        action.context = {
            **action.context,
            "submission_results": [
                {"lead_id": lead_id, "status": "created"},
                {"lead_id": "unselected", "status": "incomplete"},
            ],
        }
    crm = MockCRMAdapter()
    reply = consume_submission_command(
        session_factory, adapter, crm, event_id, command_text="重新提交待完善的线索",
    )
    assert reply == "当前没有上次已选择且待完善的线索需要重新提交。"
    assert crm.calls == crm.search_calls == 0


def test_retry_command_without_prior_incomplete_selection_is_safe_noop(
    session_factory: sessionmaker[Session],
) -> None:
    """验证没有历史待完善选择时给出安全提示且 CRM 零调用。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-1", is_authorized=True, is_active=True
            )
        )
        message = IncomingMessage(
            message_id="retry-no-history-request",
            sales_user_id="sales-1",
            sequence=1,
            raw_payload={},
            normalized_text="重新提交",
        )
        session.add(message)
        session.flush()
        event = OutboxEvent(
            message_id=message.message_id,
            sales_user_id="sales-1",
            sequence=1,
            event_type="crm_submission_intent",
            status="processing",
        )
        session.add(event)
        session.flush()
        event_id = event.id
    crm = MockCRMAdapter()

    reply = consume_submission_command(
        session_factory,
        adapter,
        crm,
        event_id,
        command_text="重新提交待完善的线索",
    )

    assert reply == "当前没有上次已选择且待完善的线索需要重新提交。"
    assert crm.search_calls == 0 and crm.calls == 0
    with session_factory() as session:
        event = session.get(OutboxEvent, event_id)
        notification = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.source_message_id == "retry-no-history-request"
            )
        )
    assert event is not None and event.status == "succeeded"
    assert notification is not None and notification.content == reply


def test_complete_temporary_duplicate_stop_promotes_and_allows_abandoned_resubmit(
    session_factory: sessionmaker[Session],
) -> None:
    """验证完整 temporary 命中重复后先晋升，再可进入 abandoned 候选。"""

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "temporary"
    crm = MockCRMAdapter(
        search_results={"人工最终公司": (CRMSearchResult("crm-A", "crm-owner", "CRM 命中"),)}
    )
    service = CrmSubmissionService(session_factory, adapter, crm)
    command = SubmissionCommand(
        "提交指定线索",
        "sales-1",
        "temporary-duplicate-stop",
        target_lead_id=lead_id,
    )

    first = service.submit(command)
    assert len(first.duplicate_confirmations) == 1
    assert crm.search_calls == 1 and crm.calls == 0 and crm.update_calls == 0
    with session_factory() as session:
        lead = session.get(Lead, lead_id)
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert lead is not None and lead.lifecycle_state == "pending_create"
    assert sync is not None and sync.status == "awaiting_duplicate_confirmation"

    stopped = service.resolve_duplicate_confirmation(
        command.request_message_id, "sales-1", continue_submission=False
    )
    assert stopped.abandoned == 1
    assert lead.smart_table_record_id is not None
    record = adapter.get_record(lead.smart_table_record_id)
    assert record is not None and record.fields["提交状态"] == "放弃提交"
    with session_factory() as session:
        lead = session.get(Lead, lead_id)
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert lead is not None and lead.lifecycle_state == "pending_create"
    assert sync is not None and sync.status == "abandoned"
    candidates = service.list_submission_candidates("帮我提交放弃提交的线索", "sales-1")
    assert [candidate.lead_id for candidate in candidates] == [lead_id]


def test_complete_temporary_duplicate_continue_updates_existing_crm_identity(
    session_factory: sessionmaker[Session],
) -> None:
    """验证完整 temporary 的重复确认继续只更新已有 CRM identity。"""

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "temporary"

    class RecordingCRMAdapter(MockCRMAdapter):
        """记录重复确认继续时服务实际传入的 CRM lead id。"""

        def __init__(self) -> None:
            """初始化 Mock CRM 和 update 目标记录。"""

            super().__init__(
                search_results={
                    "人工最终公司": (CRMSearchResult("crm-A", "crm-owner", "CRM 命中"),)
                }
            )
            self.updated_lead_ids: list[str] = []

        def update_lead(
            self,
            crm_lead_id: str,
            payload: Mapping[str, object],
            *,
            idempotency_key: str,
            crm_user_id: str,
        ) -> CRMCreateResult:
            """记录 update 目标后委托给 MockCRMAdapter。"""

            self.updated_lead_ids.append(crm_lead_id)
            return super().update_lead(
                crm_lead_id,
                payload,
                idempotency_key=idempotency_key,
                crm_user_id=crm_user_id,
            )

    crm = RecordingCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    command = SubmissionCommand(
        "提交指定线索",
        "sales-1",
        "temporary-duplicate-continue",
        target_lead_id=lead_id,
    )

    first = service.submit(command)
    assert len(first.duplicate_confirmations) == 1
    continued = service.resolve_duplicate_confirmation(
        command.request_message_id,
        "sales-1",
        continue_submission=True,
        selected_lead_ids=(lead_id,),
    )

    assert continued.submitted == 1
    assert crm.search_calls == 1 and crm.calls == 0 and crm.update_calls == 1
    assert crm.updated_lead_ids == ["crm-A"]
    assert crm.update_crm_user_ids == ["crm-1"]
    with session_factory() as session:
        lead = session.get(Lead, lead_id)
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert lead is not None and lead.lifecycle_state == "synced"
    assert sync is not None and sync.crm_lead_id == "crm-A" and sync.status == "succeeded"


def test_today_submission_shows_incomplete_temporary_and_defers_validation_to_callback(
    session_factory: sessionmaker[Session],
) -> None:
    """验证不完整 temporary 可进入 TODAY 候选，确认时返回缺项且不调用 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record = next(iter(adapter.get_records()))
    adapter.update_record(record.record_id, {"业务线": "", "职务": ""})
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "temporary"
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    candidates = service.list_submission_candidates("提交今天的线索", "sales-1")
    assert tuple(candidate.lead_id for candidate in candidates) == (lead_id,)
    result = service.submit_selected(
        SubmissionCommand("提交今天的线索", "sales-1", "today-incomplete-temporary"),
        (lead_id,),
    )

    assert len(result.items) == 1
    assert result.items[0].status is SubmissionItemStatus.INCOMPLETE
    assert result.items[0].reason_code == "missing_required_fields"
    assert result.items[0].missing_fields == ("业务线", "职务")
    assert result.incomplete == 1
    assert crm.search_calls == 0
    assert crm.calls == 0
    with session_factory() as session:
        lead = session.get(Lead, lead_id)
    assert lead is not None and lead.lifecycle_state == "temporary"


def test_all_submission_shows_incomplete_temporary_candidate(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 ALL 与 TODAY 一致，不以 CRM 必填完整度决定 temporary 是否展示。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record = next(iter(adapter.get_records()))
    adapter.update_record(record.record_id, {"沟通方式": ""})
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "temporary"

    candidates = CrmSubmissionService(
        session_factory, adapter, MockCRMAdapter()
    ).list_submission_candidates("提交我所有线索", "sales-1")

    assert tuple(candidate.lead_id for candidate in candidates) == (lead_id,)


def test_today_three_candidates_process_complete_and_incomplete_independently(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 TODAY 三条均可选择，完整项提交而两条不完整项各自零 CRM 调用。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    complete_id = _lead(session_factory, adapter)
    leads: list[tuple[str, str]] = []
    with session_factory.begin() as session:
        for index, missing in enumerate((("业务线", "职务"), ("沟通方式",)), start=1):
            record = adapter.create_record(
                {
                    "负责人": "sales-1",
                    "线索名称": f"临时测试公司-{index}",
                    "业务线": "协作机器人",
                    "线索来源": "展会",
                    "联系人": f"测试联系人-{index}",
                    "职务": "经理",
                    "沟通方式": "微信",
                    "手机": f"1380000000{index}",
                    "客户行业": "其他",
                    "备注": "已确认自动化需求，销售需继续跟进。",
                    "提交状态": "未提交",
                },
                actor=SmartTableActor.ROBOT,
            )
            adapter.update_record(record.record_id, {field: "" for field in missing})
            message_id = f"today-temporary-source-{index}"
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id="sales-1",
                    sequence=index + 1,
                    raw_payload={},
                )
            )
            lead = Lead(
                source_message_id=message_id,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                smart_table_record_id=record.record_id,
                lifecycle_state="temporary",
                standard_company_name=f"临时测试公司-{index}",
                field_values=dict(record.fields),
            )
            session.add(lead)
            session.flush()
            leads.append((lead.id, record.record_id))
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    candidates = service.list_submission_candidates("提交今天的线索", "sales-1")
    result = service.submit_selected(
        SubmissionCommand("提交今天的线索", "sales-1", "today-three-mixed"),
        tuple(candidate.lead_id for candidate in candidates),
    )

    assert {candidate.lead_id for candidate in candidates} == {
        complete_id,
        leads[0][0],
        leads[1][0],
    }
    assert {candidate.lead_id: candidate.missing_fields for candidate in candidates} == {
        complete_id: (),
        leads[0][0]: ("业务线", "职务"),
        leads[1][0]: ("沟通方式",),
    }
    assert len(result.items) == 3
    statuses = {item.lead_id: item.status for item in result.items}
    assert statuses[complete_id] is SubmissionItemStatus.CREATED
    assert statuses[leads[0][0]] is SubmissionItemStatus.INCOMPLETE
    assert statuses[leads[1][0]] is SubmissionItemStatus.INCOMPLETE
    missing = {item.lead_id: item.missing_fields for item in result.items}
    assert missing[leads[0][0]] == ("业务线", "职务")
    assert missing[leads[1][0]] == ("沟通方式",)
    assert crm.search_calls == 1 and crm.calls == 1


def test_today_submission_promotes_temporary_lead_from_complete_table_snapshot(
    session_factory: sessionmaker[Session],
) -> None:
    """验证完整 temporary 可进入 TODAY 候选，发卡只读，最终提交时才晋升。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "temporary"
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    candidates = service.list_submission_candidates("提交今天的线索", "sales-1")
    with session_factory() as session:
        lead_before_submit = session.get(Lead, lead_id)
    assert lead_before_submit is not None and lead_before_submit.lifecycle_state == "temporary"
    result = service.submit_selected(
        SubmissionCommand("提交今天的线索", "sales-1", "today-temporary-complete-message"),
        tuple(item.lead_id for item in candidates),
    )

    assert tuple(item.lead_id for item in candidates) == (lead_id,)
    assert result.succeeded == 1
    assert crm.search_calls == 1
    assert crm.calls == 1
    with session_factory() as session:
        lead_after_submit = session.get(Lead, lead_id)
    assert lead_after_submit is not None and lead_after_submit.lifecycle_state == "synced"


def test_selected_candidate_invalidated_before_worker_revalidation_returns_item(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 callback 已接受的线索转移负责人后，逐条返回未提交且不调用 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    service = CrmSubmissionService(session_factory, adapter, MockCRMAdapter())
    candidates = service.list_submission_candidates("提交今天的线索", "sales-1")
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-2"
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    result = service.submit_selected(
        SubmissionCommand("提交今天的线索", "sales-1", "state-changed-message"),
        (candidates[0].lead_id,),
    )

    assert len(result.items) == 1
    assert result.items[0].status is SubmissionItemStatus.NOT_SUBMITTED
    assert result.items[0].reason_code == "candidate_state_changed"
    assert result.not_submitted == 1
    assert crm.search_calls == 0 and crm.calls == 0


def test_today_submission_processes_only_card_selected_candidate(
    session_factory: sessionmaker[Session],
) -> None:
    """验证“今天”通过服务端候选卡选择后才执行 CRM 查重和创建。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    candidates = service.list_submission_candidates("提交今天的线索", "sales-1")
    result = service.submit_selected(
        SubmissionCommand("提交今天的线索", "sales-1", "today-selected-message"),
        (candidates[0].lead_id,),
    )

    assert tuple(item.lead_id for item in candidates) == (lead_id,)
    assert result.succeeded == 1
    assert crm.search_calls == 1
    assert crm.calls == 1


def test_company_preview_exactly_matches_table_and_does_not_call_crm(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证公司请求先生成全字段确认卡，确认前不调用 CRM。"""

    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": "上海世界纵横智能科技有限公司",
            "业务线": "协作机器人",
            "线索来源": "展会",
            "联系人": "王工",
            "职务": "经理",
            "沟通方式": "见面拜访",
            "手机": "13800000000",
            "备注": "已确认需求",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
    )
    original_find_records = adapter.find_records

    def find_records_with_member_names(filters: dict[str, object]) -> list[SmartTableRecord]:
        """为预览测试补充企业微信成员显示名元数据。"""
        records = original_find_records(filters)
        return [
            SmartTableRecord(
                record_id=item.record_id,
                fields=item.fields,
                member_names={"负责人": "张华杰(JJ)", "创建人": "杨康鑫"},
            )
            for item in records
        ]

    monkeypatch.setattr(adapter, "find_records", find_records_with_member_names)
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            IncomingMessage(
                message_id="preview-message",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
            )
        )
        session.add(
            Lead(
                id="preview-lead",
                source_message_id="preview-message",
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                smart_table_record_id=record.record_id,
                lifecycle_state="pending_create",
                field_values={"线索名称": record.fields["线索名称"]},
            )
        )
    crm = MockCRMAdapter()
    reply = prepare_company_submission_preview(
        session_factory,
        adapter,
        SubmissionCommand(
            "请帮我提交上海世界纵横智能科技有限公司这条线索",
            "sales-1",
            "preview-message",
        ),
        "上海世界纵横智能科技有限公司",
    )

    assert "确认卡" in reply
    assert crm.calls == 0
    with session_factory() as session:
        action = session.scalar(select(WecomAction))
        assert action is not None and action.target_id == "preview-lead"
        display_text = action.context.get("display_text")
        assert isinstance(display_text, str)
        assert display_text.startswith("上海世界纵横智能科技有限公司｜王工｜")
        assert len(display_text.split("｜")) == 3
        preview = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "wecom_action_preview"
            )
        )
        assert preview is not None
        preview_content = str(preview.payload["markdown"]["content"])
        assert "负责人" not in preview_content and "创建人" not in preview_content
        assert "客户行业：**未填写**" in preview_content
        assert "sales-1" not in preview_content


def test_company_preview_contains_match_still_requires_confirmation_card(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证精确无结果时的名称包含匹配仍只发行确认卡，不调用 CRM。"""
    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": "上海世界纵横智能科技有限公司",
            "联系人": "王工",
                "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
    )
    second_record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": "上海世界纵横智能科技有限公司",
            "联系人": "李工",
                "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            IncomingMessage(
                message_id="contains-preview-message",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
            )
        )
        session.add(
            Lead(
                id="contains-preview-lead",
                source_message_id="contains-preview-message",
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                smart_table_record_id=record.record_id,
                lifecycle_state="pending_create",
                field_values={"线索名称": record.fields["线索名称"]},
            )
        )
        session.add(
            Lead(
                id="contains-preview-lead-2",
                source_message_id="contains-preview-message",
                source_segment_index=1,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                smart_table_record_id=second_record.record_id,
                lifecycle_state="pending_create",
                field_values={"线索名称": second_record.fields["线索名称"]},
            )
        )

    reply = prepare_company_submission_preview(
        session_factory,
        adapter,
        SubmissionCommand("请帮我提交世界纵横这条线索", "sales-1", "contains-preview-message"),
        "世界纵横",
    )

    assert "找到 2 条同名线索" in reply
    with session_factory() as session:
        action = session.scalar(select(WecomAction))
        assert action is not None and action.target_type == "crm_company_candidates"
        candidates = action.context.get("candidate_leads")
        assert isinstance(candidates, list) and len(candidates) == 2
        assert {candidate["lead_id"] for candidate in candidates} == {
            "contains-preview-lead",
            "contains-preview-lead-2",
        }
        assert {
            candidate["display_text"].split("｜")[1] for candidate in candidates
        } == {"王工", "李工"}


@pytest.mark.parametrize(
    "candidate_count, expected_pages", [(0, 0), (1, 1), (20, 1), (21, 2), (41, 3)]
)
def test_batch_submission_issues_server_frozen_pages(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    candidate_count: int,
    expected_pages: int,
) -> None:
    """验证 20 条以上候选会发行可执行的服务端分页卡，而不是提示无法处理。"""
    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        for index in range(candidate_count):
            message_id = f"batch-page-message-{index}"
            record = adapter.create_record(
                {
                    "负责人": "sales-1",
                    "线索名称": f"同名公司-{index}",
                    "联系人": f"联系人-{index}",
                    "提交状态": "未提交",
                },
                actor=SmartTableActor.ROBOT,
            )
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id="sales-1",
                    sequence=index + 1,
                    raw_payload={},
                )
            )
            session.add(
                Lead(
                    id=f"batch-page-lead-{index}",
                    source_message_id=message_id,
                    original_capturing_sales_user_id="sales-1",
                    smart_table_owner_user_id="sales-1",
                    smart_table_record_id=record.record_id,
                    lifecycle_state="pending_create",
                    field_values={"线索名称": f"旧名称-{index}"},
                    standard_company_name=f"同名公司-{index}",
                )
            )

    reply = prepare_batch_submission_selection(
        session_factory,
        adapter,
        MockCRMAdapter(),
        SubmissionCommand("提交我所有线索", "sales-1", f"batch-page-command-{candidate_count}"),
    )

    with session_factory() as session:
        actions = session.scalars(select(WecomAction)).all()
    assert len(actions) == expected_pages
    if candidate_count > 20:
        assert "张候选卡" in reply


def test_batch_submission_sends_full_snapshot_markdown_before_short_selection_card(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证候选明细先发 Markdown，checkbox 只展示短编号且 ID 仍为 lead_id。"""

    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        for index in range(2):
            record = adapter.create_record(
                {
                    "负责人": "sales-1",
                    "线索名称": f"冻结公司-{index}",
                    "业务线": "协作机器人",
                    "线索来源": "展会",
                    "联系人": f"联系人-{index}",
                    "职务": "技术经理" if index == 0 else "",
                    "沟通方式": "微信",
                    "手机": f"1380000000{index}",
                    "客户行业": "机械加工",
                    "备注": "当前智能表格备注",
                    "AI待确认": ["职务"],
                    "提交状态": "未提交",
                },
                actor=SmartTableActor.ROBOT,
            )
            message_id = f"batch-detail-message-{index}"
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id="sales-1",
                    sequence=index + 1,
                    raw_payload={},
                )
            )
            session.add(
                Lead(
                    id=f"batch-detail-lead-{index}",
                    source_message_id=message_id,
                    original_capturing_sales_user_id="sales-1",
                    smart_table_owner_user_id="sales-1",
                    smart_table_record_id=record.record_id,
                    lifecycle_state="pending_create",
                    field_values=dict(record.fields),
                    standard_company_name=record.fields["线索名称"],
                )
            )

    prepare_batch_submission_selection(
        session_factory,
        adapter,
        MockCRMAdapter(),
        SubmissionCommand("提交我所有线索", "sales-1", "batch-detail-command"),
    )

    with session_factory() as session:
        notices = session.scalars(
            select(NotificationRecord).order_by(NotificationRecord.created_at)
        ).all()
    preview = next(
        notice for notice in notices if notice.notification_type == "wecom_action_preview"
    )
    card = next(notice for notice in notices if notice.notification_type == "wecom_action_card")
    markdown = str(preview.payload["markdown"]["content"])
    options = card.payload["template_card"]["checkbox"]["option_list"]  # type: ignore[index]
    assert "**待提交线索明细（第 1/1 页）**" in markdown
    assert "客户行业：机械加工" in markdown
    assert "提交状态：未提交" in markdown
    assert "- 缺少：职务" in markdown
    assert "AI待确认" not in markdown
    assert [option["id"] for option in options] == [
        "batch-detail-lead-0",
        "batch-detail-lead-1",
    ]
    assert [option["text"] for option in options] == ["1. 冻结公司-0", "2. 冻结公司-1"]


def test_company_preview_links_existing_owner_record_into_local_lead(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证新数据库可为当前销售负责的既有表格行建立可审计本地 Lead 映射。"""
    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": "已有表格线索",
            "业务线": "协作机器人",
            "手机": "13800000000",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            IncomingMessage(
                message_id="link-existing-message",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
            )
        )

    reply = prepare_company_submission_preview(
        session_factory,
        adapter,
        SubmissionCommand("请帮我提交已有表格线索这条线索", "sales-1", "link-existing-message"),
        "已有表格线索",
    )

    assert "确认卡" in reply
    with session_factory() as session:
        lead = session.scalar(select(Lead).where(Lead.smart_table_record_id == record.record_id))
        audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.event_type == "smart_table_existing_lead_linked"
            )
        )
    assert lead is not None
    assert lead.smart_table_owner_user_id == "sales-1"
    assert lead.lifecycle_state == "pending_create"
    assert audit is not None


def test_company_preview_keeps_temporary_owner_record_available(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证临时状态的本人表格线索仍能发行单条提交确认卡。"""
    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {"负责人": "sales-1", "线索名称": "临时状态线索", "提交状态": "未提交"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            Lead(
                id="temporary-preview-lead",
                source_message_id=None,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                smart_table_record_id=record.record_id,
                lifecycle_state="temporary",
                field_values={"线索名称": "临时状态线索"},
            )
        )

    reply = prepare_company_submission_preview(
        session_factory,
        adapter,
        SubmissionCommand("请帮我提交临时状态线索这条线索", "sales-1", "temporary-preview-message"),
        "临时状态线索",
    )

    assert "确认卡" in reply
    with session_factory() as session:
        action = session.scalar(select(WecomAction))
    assert action is not None and action.target_id == "temporary-preview-lead"


def test_today_submission_issues_selection_card_without_crm_call(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证“提交今天”只发行候选卡，发卡阶段不调用 CRM。"""
    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {"负责人": "sales-1", "线索名称": "今天的表格线索", "提交状态": "未提交"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            Lead(
                id="today-card-lead",
                source_message_id=None,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                smart_table_record_id=record.record_id,
                lifecycle_state="pending_create",
                field_values={"线索名称": "今天的表格线索"},
            )
        )
    crm = MockCRMAdapter()

    reply = prepare_batch_submission_selection(
        session_factory,
        adapter,
        crm,
        SubmissionCommand("提交今天的线索", "sales-1", "today-card-message"),
    )

    assert "候选卡" in reply
    assert crm.calls == 0
    with session_factory() as session:
        action = session.scalar(select(WecomAction))
    assert action is not None and action.context["command_text"] == "提交今天的线索"


def test_batch_selection_links_unsubmitted_table_record_missing_local_lead(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证批量卡片会按稳定表格 record_id 补齐新库遗漏的当前销售线索。"""
    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": "批量补齐表格线索",
            "业务线": "协作机器人",
            "手机": "13800000000",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            IncomingMessage(
                message_id="batch-link-message", sales_user_id="sales-1", sequence=1, raw_payload={}
            )
        )

    reply = prepare_batch_submission_selection(
        session_factory,
        adapter,
        MockCRMAdapter(),
        SubmissionCommand("提交我所有线索", "sales-1", "batch-link-message"),
    )

    assert "共 1 条" in reply
    with session_factory() as session:
        lead = session.scalar(select(Lead).where(Lead.smart_table_record_id == record.record_id))
    assert lead is not None
    assert lead.lifecycle_state == "pending_create"


def test_batch_linking_multiple_records_writes_one_audit_event(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证批量补齐多条表格记录不会因消息审计唯一键冲突而失败。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    for company_name in ("批量审计线索甲", "批量审计线索乙"):
        adapter.create_record(
            {
                "负责人": "sales-1",
                "线索名称": company_name,
                "业务线": "协作机器人",
                "手机": "13800000000",
                "提交状态": "未提交",
            },
            actor=SmartTableActor.ROBOT,
        )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            IncomingMessage(
                message_id="batch-audit-message",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
            )
        )

    _link_unsubmitted_smart_table_records(
        session_factory,
        adapter,
        SubmissionCommand("提交我所有线索", "sales-1", "batch-audit-message"),
    )

    with session_factory() as session:
        assert session.scalar(select(func.count(Lead.id))) == 2
        assert (
            session.scalar(
                select(func.count(BusinessAuditEvent.id)).where(
                    BusinessAuditEvent.message_id == "batch-audit-message",
                    BusinessAuditEvent.event_type == "smart_table_existing_leads_linked",
                )
            )
            == 1
        )


def test_batch_card_can_show_temporary_unsubmitted_lead_but_never_calls_crm(
    session_factory: sessionmaker[Session],
) -> None:
    """验证未完善草稿可被销售看到，但提交时只返回待完善且不调用 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": "待完善批量线索",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            Lead(
                source_message_id=None,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                smart_table_record_id=record.record_id,
                lifecycle_state="temporary",
                field_values={"线索名称": "待完善批量线索"},
            )
        )
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    candidates = service.list_submission_candidates("提交我所有线索", "sales-1")
    result = service.submit_selected(
        SubmissionCommand("提交我所有线索", "sales-1", "temporary-batch-message"),
        (candidates[0].lead_id,),
    )

    assert len(candidates) == 1
    assert result.incomplete == 1
    assert set(result.incomplete_missing_fields) == {
        "业务线",
        "线索来源",
        "联系人",
        "职务",
        "沟通方式",
        "手机",
        "客户行业",
        "备注",
    }
    assert crm.search_calls == 0
    assert crm.calls == 0


def test_duplicate_crm_match_waits_for_confirmation_and_stop_marks_table_status(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 CRM 查重命中不会创建，停止后写入放弃提交状态。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter(
        search_results={
            "人工最终公司": (CRMSearchResult("crm-existing", "crm-owner", "CRM 已有线索"),)
        }
    )
    service = CrmSubmissionService(session_factory, adapter, crm)

    with pytest.raises(ValueError, match="批量提交必须通过"):
        service.submit(
            SubmissionCommand("帮我提交放弃提交的线索", "sales-1", "message-abandoned-direct")
        )
    assert crm.search_calls == 0 and crm.calls == 0
    first = submit_today_via_selection(service, "sales-1", "message-12")
    stopped = service.resolve_duplicate_confirmation(
        "message-12", "sales-1", continue_submission=False
    )

    assert len(first.duplicate_confirmations) == 1
    assert crm.search_calls == 1 and crm.calls == 0 and crm.update_calls == 0
    assert stopped.abandoned == 1
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    record = adapter.get_records()[0]
    assert sync is not None and sync.status == "abandoned"
    first_generation_snapshot = {
        "canonical_payload": dict(sync.canonical_payload),
        "request_message_id": sync.request_message_id,
        "completed_at": sync.completed_at,
    }
    assert record.fields["提交状态"] == "放弃提交"

    adapter.update_record(record.record_id, {"提交状态": "未提交"})
    candidates = service.list_submission_candidates("帮我提交放弃提交的线索", "sales-1")
    assert [candidate.lead_id for candidate in candidates] == [lead_id]
    reopened = service.submit_selected(
        SubmissionCommand("帮我提交放弃提交的线索", "sales-1", "message-reopen"),
        (lead_id,),
    )
    assert len(reopened.duplicate_confirmations) == 1
    assert crm.search_calls == 2
    assert crm.calls == 0
    with session_factory() as session:
        generations = session.scalars(
            select(CrmSyncRecord)
            .where(CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "create")
            .order_by(CrmSyncRecord.generation)
        ).all()
    assert len(generations) == 2
    first_generation, second_generation = generations
    assert first_generation.generation == 1
    assert first_generation.status == "abandoned"
    assert first_generation.idempotency_key == f"crm:create:{lead_id}"
    assert second_generation.generation == 2
    assert second_generation.supersedes_sync_record_id == first_generation.id
    assert second_generation.idempotency_key == f"crm:create:{lead_id}:g2"
    assert second_generation.canonical_payload == first_generation.canonical_payload
    assert second_generation.request_message_id == "message-reopen"
    assert {
        "canonical_payload": dict(first_generation.canonical_payload),
        "request_message_id": first_generation.request_message_id,
        "completed_at": first_generation.completed_at,
    } == first_generation_snapshot
    with session_factory() as session:
        identity = session.get(CrmCompanyIdentity, "人工最终公司")
    assert identity is not None and identity.creating_sync_record_id == second_generation.id

    stopped_again = service.resolve_duplicate_confirmation(
        "message-reopen", "sales-1", continue_submission=False
    )
    assert stopped_again.abandoned == 1
    adapter.update_record(record.record_id, {"提交状态": "未提交"})
    third = service.submit_selected(
        SubmissionCommand("帮我提交放弃提交的线索", "sales-1", "message-reopen-3"),
        (lead_id,),
    )
    assert len(third.duplicate_confirmations) == 1
    with session_factory() as session:
        generations = session.scalars(
            select(CrmSyncRecord)
            .where(CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "create")
            .order_by(CrmSyncRecord.generation)
        ).all()
    assert len(generations) == 3
    assert generations[1].status == "abandoned"
    assert generations[2].generation == 3
    assert generations[2].supersedes_sync_record_id == generations[1].id
    assert generations[2].idempotency_key == f"crm:create:{lead_id}:g3"


def test_duplicate_crm_match_selected_continue_uses_update_and_marks_submitted(
    session_factory: sessionmaker[Session],
) -> None:
    """验证重复确认只更新勾选线索，并把成功线索标记为已提交。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter(
        search_results={
            "人工最终公司": (CRMSearchResult("crm-existing", "crm-owner", "CRM 已有线索"),)
        }
    )
    service = CrmSubmissionService(session_factory, adapter, crm)
    submit_today_via_selection(service, "sales-1", "message-12")

    continued = service.resolve_duplicate_confirmation(
        "message-12", "sales-1", continue_submission=True, selected_lead_ids=(lead_id,)
    )

    assert continued.submitted == 1 and continued.failed == 0
    assert crm.calls == 0 and crm.update_calls == 1
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        lead = session.get(Lead, lead_id)
    assert sync is not None and sync.status == "succeeded" and sync.operation == "update"
    assert lead is not None and lead.lifecycle_state == "synced"
    assert adapter.get_records()[0].fields["提交状态"] == "已提交"


@pytest.mark.parametrize(
    "latest_status, expected_processing, expected_incomplete",
    [("processing", 1, 0), ("succeeded", 0, 0)],
)
def test_latest_generation_state_never_forks_new_create_generation(
    session_factory: sessionmaker[Session],
    latest_status: str,
    expected_processing: int,
    expected_incomplete: int,
) -> None:
    """验证 processing/succeeded 最新 generation 都不会被重提命令复制成新行。"""

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    frozen = {
        "name": "人工最终公司",
        "product_line_data_permission": 1,
        "source": 11,
        "contactName": "王工",
        "contactTitle": "经理",
        "communicationWay": 4,
        "mobile": "13800000000",
        "remark": "人工最终备注，客户已确认项目需求并要求销售继续跟进，内容长度满足 CRM 校验。",
    }
    with session_factory.begin() as session:
        first = CrmSyncRecord(
            lead_id=lead_id,
            operation="create",
            generation=1,
            smart_table_record_id=next(iter(adapter.get_records())).record_id,
            idempotency_key=f"crm:create:{lead_id}",
            canonical_payload=frozen,
            snapshot_hash="1" * 64,
            request_message_id="generation-message-1",
            submitting_sales_user_id="sales-1",
            submitting_crm_user_id="crm-1",
            status="abandoned",
        )
        session.add(first)
        session.flush()
        session.add(
            CrmSyncRecord(
                lead_id=lead_id,
                operation="create",
                generation=2,
                supersedes_sync_record_id=first.id,
                smart_table_record_id=first.smart_table_record_id,
                idempotency_key=f"crm:create:{lead_id}:g2",
                canonical_payload=frozen,
                snapshot_hash="2" * 64,
                request_message_id="generation-message-2",
                submitting_sales_user_id="sales-1",
                submitting_crm_user_id="crm-1",
                status=latest_status,
                crm_lead_id="crm-existing" if latest_status == "succeeded" else None,
                completed_at=utc_now() if latest_status == "succeeded" else None,
                processing_lease_expires_at=(
                    utc_now() + timedelta(minutes=5) if latest_status == "processing" else None
                ),
            )
        )

    crm = MockCRMAdapter()
    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "generation-retry",
    )

    assert result.processing == expected_processing
    assert result.incomplete == expected_incomplete
    assert crm.calls == 0
    with session_factory() as session:
        generations = session.scalars(
            select(CrmSyncRecord)
            .where(CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "create")
            .order_by(CrmSyncRecord.generation)
        ).all()
    assert [item.generation for item in generations] == [1, 2]


def test_missing_crm_mapping_creates_auditable_terminal_record_without_calling_crm(
    session_factory: sessionmaker[Session],
    employee_directory_for_crm_submission_tests: Path,
) -> None:
    """验证映射缺失保留待创建线索并形成不可重试的审计事实。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    settings_path = employee_directory_for_crm_submission_tests
    settings_path.write_text("id,name,nickname\ncrm-2,其他销售,其他\n", encoding="utf-8")
    crm = MockCRMAdapter()
    service = CrmSubmissionService(
        session_factory, adapter, crm, employee_directory=EmployeeDirectory(settings_path)
    )

    result = submit_today_via_selection(service, "sales-1", "message-12")

    assert result.mapping_missing == 1
    assert crm.calls == 0
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        lead = session.get(Lead, lead_id)
    assert sync is not None
    assert sync.operation == "validation"
    assert sync.status == "failed_pending_review"
    assert sync.failure_category == "permanent"
    assert sync.failure_kind == "validation_failed"
    assert sync.failure_code == "mapping_missing"
    assert sync.submitting_crm_user_id is None
    assert lead is not None and lead.lifecycle_state == "pending_create"

    with session_factory.begin() as session:
        settings_path.write_text("id,name,nickname\ncrm-1,sales-1,sales-1\n", encoding="utf-8")

    recovered = submit_today_via_selection(
        CrmSubmissionService(
            session_factory, adapter, crm, employee_directory=EmployeeDirectory(settings_path)
        ),
        "sales-1",
        "message-13",
    )

    assert recovered.succeeded == 1
    assert crm.calls == 1


def test_mapping_missing_audit_key_is_bounded_for_a_maximum_length_message_id(
    session_factory: sessionmaker[Session],
    employee_directory_for_crm_submission_tests: Path,
) -> None:
    """验证长消息标识仍可写入映射缺失审计事实。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    settings_path = employee_directory_for_crm_submission_tests
    settings_path.write_text("id,name,nickname\ncrm-2,其他销售,其他\n", encoding="utf-8")
    service = CrmSubmissionService(
        session_factory,
        adapter,
        MockCRMAdapter(),
        employee_directory=EmployeeDirectory(settings_path),
    )

    result = submit_today_via_selection(service, "sales-1", "m" * 128)

    assert result.mapping_missing == 1
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert sync is not None
    assert len(sync.idempotency_key) <= 128


def test_ai_pending_confirmation_does_not_block_when_core_values_present(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 AI待确认 只是建议，八项必填值完整时不阻塞 CRM 提交。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"AI待确认": ["客户行业"]})
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    first = submit_today_via_selection(service, "sales-1", "message-12")
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
    adapter.update_record(record_id, {"AI待确认": ["手机"], "提交状态": "未提交"})

    second = submit_today_via_selection(service, "sales-1", "message-12")
    assert second.incomplete == 0
    assert len(second.duplicate_confirmations) == 1
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
    assert submit_today_via_selection(service, "sales-1", "message-12").retrying == 1
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-1")
        assert authorization is not None
        authorization.crm_user_id = "crm-2"
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    assert submit_today_via_selection(service, "sales-1", "message-12").succeeded == 1
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert sync is not None
    assert sync.idempotency_key == f"crm:create:{lead_id}"
    assert sync.canonical_payload["mobile"] == "13800000000"
    assert crm.crm_user_ids == ["crm-1"]


def test_unknown_crm_failure_does_not_finalize_discard_request(
    session_factory: sessionmaker[Session],
) -> None:
    """验证未分类 CRM 异常只代表外部结果未知，不能把等待中的废弃结算为有效。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)

    class UnknownCRM(MockCRMAdapter):
        """模拟无法判定远端是否已提交的未知 CRM 调用结果。"""

        def create_lead(self, payload: object, *, idempotency_key: str, crm_user_id: str) -> object:
            """抛出未分类异常，表示响应边界发生未知故障。"""
            raise RuntimeError("unknown remote outcome")

    crm = UnknownCRM()
    service = CrmSubmissionService(session_factory, adapter, crm)
    result = submit_today_via_selection(service, "sales-1", "message-12")

    assert result.failed_pending_review == 1
    discard = LeadDiscardService(session_factory).discard(lead_id, "sales-1", "等待核实 CRM 结果")
    assert discard.status is LeadDiscardStatus.WAITING_FOR_CRM
    with session_factory() as session:
        lead = session.get(Lead, lead_id)
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        request = session.scalar(
            select(LeadDiscardRequest).where(LeadDiscardRequest.lead_id == lead_id)
        )
        identity = session.scalar(
            select(CrmCompanyIdentity).where(CrmCompanyIdentity.creating_lead_id == lead_id)
        )
        audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.event_type == "crm_external_outcome_unknown"
            )
        )
    assert lead is not None and lead.lifecycle_state == "pending_create"
    assert sync is not None and sync.failure_category == "unknown"
    assert request is not None and request.status == "pending"
    # 未知结果保留公司 reservation，等待外部事实恢复而不释放预留。
    assert identity is not None and identity.state == "reserving"
    assert audit is not None and audit.details["attempts"] == 1


def test_update_command_requires_active_actor(
    session_factory: sessionmaker[Session],
) -> None:
    """验证更新命令仍由确定性 actor active 边界保护。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    crm = MockCRMAdapter()
    with pytest.raises(ValueError, match="提交销售 actor 不存在或已停用"):
        CrmSubmissionService(session_factory, adapter, crm).submit(
            SubmissionCommand("提交我的更新", "sales-1", "message-12")
        )
    assert crm.calls == 0


def test_crm_submission_does_not_use_legacy_authorization_flag_as_gate(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 active actor 即使 is_authorized=false 也可进入 CRM 提交入口。"""
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-legacy-flag",
                is_authorized=False,
                is_active=True,
            )
        )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    crm = MockCRMAdapter()

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交我的更新", "sales-legacy-flag", "message-13")
    )

    assert result == SubmissionBatchResult()
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
    incomplete_reply = format_submission_reply(
        SubmissionBatchResult(
            incomplete=1,
            incomplete_missing_fields=("业务线", "手机"),
        )
    )

    assert "创建成功 1 条" in create_reply and "更新成功 0 条" in create_reply
    assert "手机号" not in create_reply and "payload" not in create_reply.lower()
    assert "更新成功 1 条" in update_reply and "无变化 2 条" in update_reply
    assert "缺少必填字段：业务线、手机" in incomplete_reply


def test_update_item_results_are_operation_aware() -> None:
    """验证 update 成功、无变化、不完整和重试不复用 create 语义。"""
    from app.crm.service import _append_update_result

    succeeded = _append_update_result(SubmissionBatchResult(), "lead-updated", "succeeded")
    unchanged = _append_update_result(SubmissionBatchResult(), "lead-same", "unchanged")
    incomplete = _append_update_result(SubmissionBatchResult(), "lead-incomplete", "incomplete")
    retrying = _append_update_result(SubmissionBatchResult(), "lead-retry", "retrying")

    assert succeeded.items[0].status is SubmissionItemStatus.UPDATED
    assert unchanged.items[0].status is SubmissionItemStatus.UNCHANGED
    assert incomplete.items[0].status is SubmissionItemStatus.INCOMPLETE
    assert incomplete.incomplete == 1
    assert retrying.items[0].reason_code == "crm_update_retrying"


def test_submission_reply_explains_selected_candidate_skipped_after_state_change() -> None:
    """验证 callback 已接受但最终复核失效的线索会显示为未提交并说明原因。"""
    reply = format_submission_reply(
        SubmissionBatchResult(
            not_submitted=1,
            items=(
                SubmissionItemResult(
                    "lead-stale", SubmissionItemStatus.NOT_SUBMITTED, "candidate_state_changed"
                ),
            ),
        ),
        lead_labels={"lead-stale": "XX公司｜张总"},
        selected_count=1,
    )

    assert "⚪ 未提交 1 条" in reply
    assert "XX公司｜张总：线索状态已变化，请重新发起提交" in reply


def test_submission_reply_keeps_per_lead_results_and_frozen_display_labels() -> None:
    """验证批量回复逐条保留状态、原因和缺失字段，不接受 callback 注入名称。"""

    items = tuple(
        [
            SubmissionItemResult(f"created-{index}", SubmissionItemStatus.CREATED)
            for index in range(4)
        ]
        + [
            SubmissionItemResult(
                "lead-e",
                SubmissionItemStatus.INCOMPLETE,
                "missing_required_fields",
                ("客户行业", "职务"),
            ),
            SubmissionItemResult("lead-f", SubmissionItemStatus.PROCESSING, "sync_processing"),
        ]
    )
    result = SubmissionBatchResult(
        succeeded=4,
        incomplete=1,
        processing=1,
        items=items,
    )
    reply = format_submission_reply(
        result,
        selected_count=6,
        lead_labels={
            **{f"created-{index}": f"公司{index}｜联系人{index}" for index in range(4)},
            "lead-e": "公司E｜刘工",
            "lead-f": "公司F｜陈工",
        },
    )

    assert "CRM 提交结果（已选择 6 条）" in reply
    assert "创建 4｜更新 0｜待完善 1｜处理中 1｜未提交 0｜失败 0" in reply
    assert "公司E｜刘工：缺少「客户行业、职务」" in reply
    assert "公司F｜陈工：已有提交任务正在处理，本次未重复创建" in reply
    assert "injected-lead" not in reply
    assert "待完善或待明确确认" not in reply
    assert len(result.items) == 6


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
    payload = {
        "name": "公司",
        "product_line_data_permission": 1,
        "source": 11,
        "contactName": "王工",
        "contactTitle": "经理",
        "communicationWay": 4,
        "mobile": "13800000000",
        "remark": "人工最终备注，客户已确认项目需求并要求销售继续跟进，内容长度满足 CRM 校验。",
    }
    first = MockCRMAdapter().create_lead(
        payload, idempotency_key="crm:create:lead-1", crm_user_id="crm-1"
    )
    second = MockCRMAdapter().create_lead(
        payload, idempotency_key="crm:create:lead-1", crm_user_id="crm-1"
    )
    assert first.crm_lead_id == second.crm_lead_id


def test_company_advisory_lock_key_is_stable_without_python_hash() -> None:
    """验证同一规范公司名称在不同调用中生成相同的 PostgreSQL 锁键。"""
    assert CrmSubmissionService._company_advisory_lock_key("公司 X") == (
        CrmSubmissionService._company_advisory_lock_key("公司 X")
    )


def test_expired_create_lease_recovers_remote_success_with_original_frozen_fact(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证远端成功但本地崩溃后仅用原冻结 create 事实恢复并激活 reservation。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = next(iter(adapter.get_records())).record_id
    frozen = {
        "name": "人工最终公司",
        "product_line_data_permission": 1,
        "source": 11,
        "contactName": "王工",
        "contactTitle": "经理",
        "communicationWay": 4,
        "mobile": "13800000000",
        "remark": "人工最终备注，客户已确认项目需求并要求销售继续跟进，内容长度满足 CRM 校验。",
        "isInternational": False,
    }
    with session_factory.begin() as session:
        sync = CrmSyncRecord(
            lead_id=lead_id,
            operation="create",
            generation=1,
            smart_table_record_id=record_id,
            idempotency_key=f"crm:create:{lead_id}",
            canonical_payload=frozen,
            snapshot_hash="a" * 64,
            request_message_id="message-12",
            submitting_sales_user_id="sales-1",
            submitting_crm_user_id="crm-1",
            status="processing",
            processing_lease_expires_at=utc_now() - timedelta(seconds=1),
        )
        session.add(sync)
        session.flush()
        session.add(
            CrmCompanyIdentity(
                standard_company_name="人工最终公司",
                state="reserving",
                creating_lead_id=lead_id,
                creating_sync_record_id=sync.id,
            )
        )
    monkeypatch.setattr(adapter, "update_record", lambda *_args, **_kwargs: None)
    recovered = MockCRMAdapter()
    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, recovered),
        "sales-1",
        "message-12",
    )
    assert result.succeeded == 1 and recovered.calls == 1 and recovered.payloads == [frozen]
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        identity = session.get(CrmCompanyIdentity, "人工最终公司")
    assert sync is not None and identity is not None
    assert sync.status == "succeeded" and sync.idempotency_key == f"crm:create:{lead_id}"
    assert sync.snapshot_hash == "a" * 64 and sync.submitting_crm_user_id == "crm-1"
    assert identity.state == "active" and identity.crm_lead_id == sync.crm_lead_id


def test_t13_audit_and_logs_keep_identity_metadata_without_sensitive_payload(
    session_factory: sessionmaker[Session], caplog: pytest.LogCaptureFixture
) -> None:
    """验证 T13 identity 关键审计与日志只保留标识/哈希，不泄露 CRM 业务载荷。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    with caplog.at_level(logging.INFO, logger="app.crm.service"):
        result = submit_today_via_selection(
            CrmSubmissionService(session_factory, adapter, crm),
            "sales-1",
            "message-12",
        )
    assert result.succeeded == 1
    with session_factory() as session:
        event_types = set(session.scalars(select(BusinessAuditEvent.event_type)).all())
        activated = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.event_type == "crm_global_identity_activated"
            )
        )
    assert "crm_create_succeeded" in event_types
    assert "crm_global_identity_activated" in event_types
    assert activated is not None
    assert crm.search_calls == 1
    logged = caplog.text
    for sensitive in ("13800000000", "人工最终备注", "crm-1", "payload", "secret"):
        assert sensitive not in logged.lower()
    assert CrmSubmissionService._company_advisory_lock_key("公司 X") != (
        CrmSubmissionService._company_advisory_lock_key("公司 Y")
    )


def test_update_uses_current_table_values_once_and_skips_equal_snapshot(
    session_factory: sessionmaker[Session],
) -> None:
    """验证更新以表格回读快照调用一次 CRM，重放相同业务值不再调用。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "message-12").succeeded == 1
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000", "AI待确认": ["客户行业"]})

    first = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-13"))
    second = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-14"))

    assert first.updated == 1 and second.unchanged == 1
    assert crm.update_calls == 1 and crm.update_payloads[0]["mobile"] == "13900000000"
    with session_factory() as session:
        updates = session.scalars(
            select(CrmSyncRecord).where(CrmSyncRecord.operation == "update")
        ).all()
    assert len(updates) == 1 and updates[0].canonical_payload.get("AI待确认") is None


def test_update_mapping_missing_keeps_pending_update_until_a_later_submit(
    session_factory: sessionmaker[Session],
    employee_directory_for_crm_submission_tests: Path,
) -> None:
    """验证缺映射的真实更新不调用 CRM，补齐映射后才允许提交。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "message-12").succeeded
    adapter.update_record(next(iter(adapter.get_records())).record_id, {"手机": "13900000000"})
    settings_path = employee_directory_for_crm_submission_tests
    settings_path.write_text("id,name,nickname\ncrm-2,其他销售,其他\n", encoding="utf-8")
    service = CrmSubmissionService(
        session_factory, adapter, crm, employee_directory=EmployeeDirectory(settings_path)
    )

    blocked = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-13"))

    assert blocked.mapping_missing == 1
    assert crm.update_calls == 0
    with session_factory() as session:
        lead = session.get(Lead, lead_id)
        validation = session.scalar(
            select(CrmSyncRecord).where(
                CrmSyncRecord.lead_id == lead_id,
                CrmSyncRecord.operation == "validation",
            )
        )
    assert lead is not None and lead.lifecycle_state == "pending_update"
    assert validation is not None and validation.failure_code == "mapping_missing"

    settings_path.write_text("id,name,nickname\ncrm-1,sales-1,sales-1\n", encoding="utf-8")
    resumed = CrmSubmissionService(
        session_factory, adapter, crm, employee_directory=EmployeeDirectory(settings_path)
    ).submit(SubmissionCommand("提交我的更新", "sales-1", "message-14"))

    assert resumed.updated == 1
    assert crm.update_calls == 1


def test_update_company_identity_change_enters_review_without_crm_call(
    session_factory: sessionmaker[Session],
) -> None:
    """验证已同步公司名称变化只能进入人工审查，不能自动更新 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    submit_today_via_selection(service, "sales-1", "message-12")
    adapter.update_record(next(iter(adapter.get_records())).record_id, {"线索名称": "新公司"})

    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-13"))

    assert result.company_identity_review == 1 and crm.update_calls == 0
    with session_factory() as session:
        assert (
            session.get(Lead, lead_id).lifecycle_state == "company_identity_change_pending_review"
        )  # type: ignore[union-attr]
        audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.event_type == "company_identity_change_pending_review"
            )
        )
    assert audit is not None
    assert {
        "old_standard_company_name",
        "new_candidate_company_name",
        "lead_id",
        "smart_table_record_id",
        "smart_table_owner_user_id",
    } <= set(audit.details)


def test_historical_company_identity_does_not_bypass_crm_duplicate_search(
    session_factory: sessionmaker[Session],
) -> None:
    """验证历史同步记录不会绕过 CRM 查重或替代本次提交决策。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter(
        search_results={
            "人工最终公司": (
                CRMSearchResult("crm-A", "crm-owner", "CRM 已有线索 A"),
                CRMSearchResult("crm-B", "crm-owner", "CRM 已有线索 B"),
            )
        }
    )
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.standard_company_name = "公司 X"
        for crm_lead_id in ("crm-A", "crm-B"):
            session.add(
                CrmSyncRecord(
                    lead_id=lead_id,
                    operation="update",
                    smart_table_record_id=lead.smart_table_record_id or "",
                    idempotency_key=f"historical:{crm_lead_id}",
                    canonical_payload={"name": "公司 X"},
                    snapshot_hash=f"historical-{crm_lead_id}",
                    request_message_id="message-12",
                    submitting_sales_user_id="sales-1",
                    submitting_crm_user_id="crm-1",
                    crm_lead_id=crm_lead_id,
                    crm_lead_owner_user_id="crm-owner",
                    status="succeeded",
                )
            )

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )

    assert len(result.duplicate_confirmations) == 1
    assert crm.calls == 0 and crm.update_calls == 0
    with session_factory() as session:
        identity = session.get(CrmCompanyIdentity, "公司 X")
    assert identity is not None and identity.state == "reserving"
    assert crm.search_calls == 1


@pytest.mark.parametrize(
    ("duplicate_results", "expected_status"),
    [
        ((CRMSearchResult("crm-A", "crm-owner", "CRM 已有线索 A"),), "duplicate"),
        ((), "conflict"),
        ((CRMSearchResult("crm-B", "crm-owner", "CRM 已有线索 B"),), "conflict"),
    ],
)
def test_active_company_identity_only_validates_current_crm_duplicate(
    session_factory: sessionmaker[Session],
    duplicate_results: tuple[CRMSearchResult, ...],
    expected_status: str,
) -> None:
    """验证 active identity 不绕过查重，冲突时禁止 create/update。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter(search_results={"人工最终公司": duplicate_results})
    with session_factory.begin() as session:
        session.add(
            CrmCompanyIdentity(
                standard_company_name="人工最终公司",
                state="active",
                crm_lead_id="crm-A",
                crm_lead_owner_user_id="crm-owner",
            )
        )

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "active-identity-check",
    )

    assert crm.search_calls == 1
    assert crm.calls == 0 and crm.update_calls == 0
    if expected_status == "duplicate":
        assert len(result.duplicate_confirmations) == 1
        assert result.failed_pending_review == 0
    else:
        assert result.failed_pending_review == 1
        assert not result.duplicate_confirmations
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    if expected_status == "duplicate":
        assert sync is not None and sync.status == "awaiting_duplicate_confirmation"
    else:
        assert sync is None


def test_identical_historical_company_identity_does_not_bypass_crm_create_search(
    session_factory: sessionmaker[Session],
) -> None:
    """验证多个历史同步指向同一身份时首次提交仍先询问 CRM 查重。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.standard_company_name = "公司 X"
        for suffix in ("one", "two"):
            session.add(
                CrmSyncRecord(
                    lead_id=lead_id,
                    operation="update",
                    smart_table_record_id=lead.smart_table_record_id or "",
                    idempotency_key=f"historical:{suffix}",
                    canonical_payload={"name": "公司 X"},
                    snapshot_hash=f"historical-{suffix}",
                    request_message_id="message-12",
                    submitting_sales_user_id="sales-1",
                    submitting_crm_user_id="crm-1",
                    crm_lead_id="crm-A",
                    crm_lead_owner_user_id="crm-owner",
                    status="succeeded",
                )
            )

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )

    assert result.succeeded == 1
    assert crm.calls == 1 and crm.update_calls == 0 and crm.search_calls == 1
    with session_factory() as session:
        identities = session.scalars(select(CrmCompanyIdentity)).all()
    assert len(identities) == 1
    assert identities[0].creating_sync_record_id is not None
    assert identities[0].state == "active"


def test_retrying_update_does_not_read_changed_smart_table_before_frozen_retry(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 retrying update 先重发冻结快照，期间绝不回读销售后续编辑。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "message-12").succeeded == 1
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    original_update = crm.update_lead
    failed = True

    def timeout_once(*args: object, **kwargs: object) -> object:
        """首次 update 模拟网络超时，留下可恢复的冻结操作。"""
        nonlocal failed
        if failed:
            failed = False
            raise TimeoutError()
        return original_update(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(crm, "update_lead", timeout_once)
    assert service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-13")).retrying == 1
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-1")
        assert authorization is not None
        authorization.crm_user_id = "crm-2"
    adapter.update_record(record_id, {"手机": "13700000000"})
    calls = 0
    original_get_record = adapter.get_record

    def count_get_record(record_id: str) -> object:
        """记录重试阶段是否错误读取智能表格。"""
        nonlocal calls
        calls += 1
        return original_get_record(record_id)

    monkeypatch.setattr(adapter, "get_record", count_get_record)
    monkeypatch.setattr(adapter, "update_record", lambda *_args, **_kwargs: None)
    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-14"))

    assert result.updated == 1 and calls == 0
    assert crm.update_crm_user_ids == ["crm-1"]
    assert crm.update_payloads == [
        {
            "product_line_data_permission": 1,
            "name": "人工最终公司",
            "source": 11,
            "contactName": "王工",
            "contactTitle": "经理",
            "communicationWay": 4,
            "mobile": "13900000000",
            "industry": 11,
            "remark": (
                "【AI录入】人工最终备注，客户已确认项目需求并要求销售继续跟进，"
                "内容长度满足 CRM 校验。"
            ),
            "isInternational": False,
        }
    ]


@pytest.mark.parametrize("expired", [False, True])
def test_processing_update_never_reads_table_before_lease_recovery(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch, expired: bool
) -> None:
    """验证 processing update 无论租约状态均先处理冻结操作，绝不回读表格。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "message-12").succeeded == 1
    record_id = next(iter(adapter.get_records())).record_id
    with session_factory.begin() as session:
        baseline = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        assert baseline is not None
        session.add(
            CrmSyncRecord(
                lead_id=lead_id,
                operation="update",
                smart_table_record_id=record_id,
                idempotency_key="frozen-update",
                canonical_payload=dict(baseline.canonical_payload),
                snapshot_hash="f" * 64,
                request_message_id="message-13",
                submitting_sales_user_id="sales-1",
                submitting_crm_user_id="crm-1",
                crm_lead_id=baseline.crm_lead_id,
                crm_lead_owner_user_id=baseline.crm_lead_owner_user_id,
                status="processing",
                processing_lease_expires_at=utc_now() + timedelta(seconds=-1 if expired else 60),
            )
        )
    monkeypatch.setattr(adapter, "get_record", lambda _: pytest.fail("不得回读 Smart Table"))
    monkeypatch.setattr(adapter, "update_record", lambda *_args, **_kwargs: None)
    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-14"))
    assert (result.processing if not expired else result.updated) == 1


def test_long_message_id_uses_bounded_notification_key() -> None:
    """验证 128 字符消息标识能够生成固定长度通知键。"""
    assert len(notification_key_for_message("m" * 128)) == 64


def test_replayed_today_command_does_not_bypass_selection_card(
    session_factory: sessionmaker[Session],
) -> None:
    """验证“今天”命令重放时仍保持候选卡边界，不直接重放 CRM 写入。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )
    crm.calls = 0
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
    assert "没有可提交的未提交线索" in reply
    assert crm.calls == 0
    with session_factory() as session:
        assert (
            session.get(NotificationRecord, notification_key_for_message("message-12")) is not None
        )
        assert session.get(Lead, lead_id).lifecycle_state == "synced"  # type: ignore[union-attr]


def test_mapping_missing_reply_does_not_double_count_generic_terminal_failure(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    employee_directory_for_crm_submission_tests: Path,
) -> None:
    """验证 CRM 映射缺失在销售汇总中只计入专用错误分类。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    settings_path = employee_directory_for_crm_submission_tests
    settings_path.write_text("id,name,nickname\ncrm-2,其他销售,其他\n", encoding="utf-8")
    import app.crm.dependencies as crm_dependencies
    import app.crm.service as crm_service

    monkeypatch.setattr(
        crm_service,
        "get_settings",
        lambda: get_settings().model_copy(update={"employee_directory_path": str(settings_path)}),
    )
    monkeypatch.setattr(
        crm_dependencies,
        "get_settings",
        lambda: get_settings().model_copy(update={"employee_directory_path": str(settings_path)}),
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
        )
        session.add(event)
        session.flush()
        event_id = event.id

    reply = consume_submission_command(session_factory, adapter, MockCRMAdapter(), event_id)

    assert "候选线索已找到" in reply
    assert "卡片能力未就绪" in reply


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

    def broken_prepare(*_args: object, **_kwargs: object) -> object:
        """模拟候选卡编排发生不可预期异常。"""
        raise RuntimeError("internal")

    import app.crm.commands as crm_commands

    monkeypatch.setattr(crm_commands, "prepare_batch_submission_selection", broken_prepare)
    consume_submission_command(session_factory, adapter, MockCRMAdapter(), event_id)
    consume_submission_command(session_factory, adapter, MockCRMAdapter(), event_id)
    with session_factory() as session:
        event = session.get(OutboxEvent, event_id)
        notice = session.get(
            NotificationRecord, terminal_failure_notification_key_for_message("message-12")
        )
        assert event is not None and event.status == "failed_pending_review"
        assert notice is not None and "需要人工处理" in (notice.content or "")


@pytest.mark.parametrize(
    "count, failure_stage", [(1, "preview"), (1, "card"), (21, "preview"), (21, "card")]
)
def test_submission_delivery_orders_all_details_cards_then_one_configured_link(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    count: int,
    failure_stage: str,
) -> None:
    """单条及多页批量提交在实际成功投递后发链接，失败和命令重放不重复成功消息。"""
    import app.crm.commands as commands
    from app.notifications.outbound import WecomOutboundNotificationSender

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
            "lead_smart_table_url": "https://example.invalid/configured-review-table",
        }
    )
    monkeypatch.setattr(commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        for index in range(count):
            record = adapter.create_record(
                {
                    "负责人": "sales-1",
                    "线索名称": f"投递公司{index}",
                    "提交状态": "未提交",
                },
                actor=SmartTableActor.ROBOT,
            )
            session.add(
                Lead(
                    id=f"delivery-{index}",
                    original_capturing_sales_user_id="sales-1",
                    smart_table_owner_user_id="sales-1",
                    smart_table_record_id=record.record_id,
                    lifecycle_state="pending_create",
                    field_values=dict(record.fields),
                )
            )

    command = SubmissionCommand(
        "提交我所有线索" if count > 1 else "请帮我提交投递公司0这条线索",
        "sales-1",
        "delivery-command",
    )

    def prepare(request: SubmissionCommand) -> None:
        """调用实际命令入口，只构造待提交明细和卡片，不调用 CRM。"""
        if count > 1:
            prepare_batch_submission_selection(session_factory, adapter, MockCRMAdapter(), request)
        else:
            prepare_company_submission_preview(session_factory, adapter, request, "投递公司0")

    class Client:
        """按阶段模拟一次投递失败，只保存真正成功的消息。"""

        def __init__(self) -> None:
            """初始化投递记录及一次故障开关，无外部副作用。"""
            self.delivered: list[str] = []
            self.failed = False

        async def send_message(
            self, userid_or_chatid: str, body: dict[str, object]
        ) -> dict[str, str]:
            """将成功消息记入内存；指定阶段首次抛出可重试连接错误。"""
            assert userid_or_chatid == "sales-1"
            assert set(body) == {"msgtype", str(body["msgtype"])}
            stage = (
                "card"
                if body["msgtype"] == "template_card"
                else ("link" if "configured-review-table" in str(body) else "preview")
            )
            if stage == failure_stage and not self.failed:
                self.failed = True
                raise ConnectionError("isolated delivery failure")
            self.delivered.append(stage)
            return {"msgid": str(len(self.delivered))}

    prepare(command)
    prepare(command)
    client = Client()
    sender = WecomOutboundNotificationSender(session_factory, client)
    asyncio.run(sender.send_pending_once())
    assert "link" not in client.delivered
    for _ in range(4):
        asyncio.run(sender.send_pending_once())
    assert client.delivered == sorted(
        client.delivered, key={"preview": 0, "card": 1, "link": 2}.get
    )
    assert client.delivered.count("card") == (2 if count > 1 else 1)
    assert client.delivered.count("link") == 1
    prepare(command)
    assert asyncio.run(sender.send_pending_once()) == 0
    with session_factory() as session:
        notices = list(session.scalars(select(NotificationRecord)))
        assert all(notice.status == "succeeded" for notice in notices)
        assert all(notice.attempts <= 2 for notice in notices)
    # 新消息是新的有效命令，不能被首次录入成功链接的历史去重规则抑制。
    prepare(SubmissionCommand(command.text, "sales-1", "delivery-command-next"))
    for _ in range(3):
        asyncio.run(sender.send_pending_once())
    assert client.delivered.count("link") == 2


def test_submission_missing_table_url_logs_configuration_and_never_records_sent_link(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """链接缺失时明确记录配置错误，已有明细和卡片仍可投递，无假成功链接。"""
    import app.crm.commands as commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
            "lead_smart_table_url": None,
        }
    )
    monkeypatch.setattr(commands, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {"负责人": "sales-1", "线索名称": "配置测试公司", "提交状态": "未提交"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            Lead(
                id="missing-url",
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                smart_table_record_id=record.record_id,
                field_values=dict(record.fields),
                lifecycle_state="pending_create",
            )
        )
    prepare_company_submission_preview(
        session_factory,
        adapter,
        SubmissionCommand("提交", "sales-1", "missing-url-command"),
        "配置测试公司",
    )
    assert "crm_submission_table_link_configuration_missing" in caplog.text
    with session_factory() as session:
        assert (
            session.scalar(
                select(func.count(NotificationRecord.notification_key)).where(
                    NotificationRecord.notification_type == "crm_submission_table_link",
                )
            )
            == 0
        )


def test_capture_metadata_never_changes_crm_payload_or_submission_preview() -> None:
    """录入时间不进入 CRM 载荷，也不扩展提交确认十字段。"""
    from app.crm.payload import CrmPayloadBuilder
    from app.wecom_bot.actions import SUBMISSION_PREVIEW_FIELDS

    builder = CrmPayloadBuilder()
    fields = {"线索名称": "元数据测试公司", "业务线": "协作机器人"}
    assert builder.build(fields) == builder.build({**fields, "录入时间": "2026-10-09 10:20:30"})
    assert "录入时间" not in SUBMISSION_PREVIEW_FIELDS
    assert len(SUBMISSION_PREVIEW_FIELDS) == 10
