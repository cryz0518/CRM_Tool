"""T12 CRM 首次创建提交的应用服务测试。"""

from __future__ import annotations

import logging
from collections.abc import Generator, Mapping
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.companies.models import CompanyVerificationStatus
from app.core.config import get_settings
from app.crm.adapter import CRMCreateResult, CRMSearchResult
from app.crm.commands import (
    consume_submission_command,
    format_submission_reply,
    notification_key_for_message,
    prepare_company_submission_preview,
    terminal_failure_notification_key_for_message,
)
from app.crm.employee_directory import EmployeeDirectory
from app.crm.mock import MockCRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionCommand
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
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableRecord
from app.smart_table.registry import build_required_smart_table_schema


@pytest.fixture(autouse=True)
def employee_directory_for_crm_submission_tests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """为提交单测提供显式、隔离的 Smart Table owner 员工目录。"""
    path = tmp_path / "employee.csv"
    path.write_text("id,name,nickname\ncrm-1,sales-1,sales-1\n", encoding="utf-8")
    import app.crm.service as crm_service

    settings = get_settings().model_copy(update={"employee_directory_path": str(path)})
    monkeypatch.setattr(crm_service, "get_settings", lambda: settings)
    monkeypatch.setattr(crm_service, "_TEST_EMPLOYEE_DIRECTORY_PATH", path, raising=False)


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
            "备注": "人工最终备注，客户已确认项目需求并要求销售继续跟进，内容长度满足 CRM 校验。",
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

    first = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    second = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))

    assert first.succeeded == 1
    assert second.succeeded == 0
    assert crm.calls == 1
    assert crm.payloads[0]["name"] == "人工最终公司"
    assert adapter.get_records()[0].fields["提交状态"] == "已提交"
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        lead = session.get(Lead, lead_id)
    assert sync is not None
    assert sync.idempotency_key == f"crm:create:{lead_id}"
    assert sync.canonical_payload["mobile"] == "13800000000"
    assert lead is not None and lead.lifecycle_state == "synced"


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
        "备注": "验证提交前的成员身份转换，不包含真实客户信息，内容仅用于离线测试。",
    }
    record = adapter.create_record(fields, actor=SmartTableActor.ROBOT)
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="wecom-owner", is_authorized=True, is_active=True
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

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "wecom-owner", "message-member-name")
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

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
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
            raise SopCRMError(
                "sanitized CRM failure", category=category, http_status=http_status
            )

    result = CrmSubmissionService(session_factory, adapter, CategorizedCRM()).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
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

    result = CrmSubmissionService(session_factory, adapter, SearchTransportCRM()).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
    )

    assert result.retrying == 1


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

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
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

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
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

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
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
        session.add(
            SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True)
        )
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
        preview = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "wecom_action_preview"
            )
        )
        assert preview is not None
        preview_content = str(preview.payload["markdown"]["content"])
        assert "张华杰(JJ)" in preview_content
        assert "杨康鑫" in preview_content
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
        {"负责人": "sales-1", "线索名称": "上海世界纵横智能科技有限公司"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True)
        )
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

    reply = prepare_company_submission_preview(
        session_factory,
        adapter,
        SubmissionCommand(
            "请帮我提交世界纵横这条线索", "sales-1", "contains-preview-message"
        ),
        "世界纵横",
    )

    assert "名称包含关系" in reply
    with session_factory() as session:
        action = session.scalar(select(WecomAction))
        assert action is not None and action.target_id == "contains-preview-lead"


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
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True)
        )
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
        SubmissionCommand(
            "请帮我提交已有表格线索这条线索", "sales-1", "link-existing-message"
        ),
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

    first = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))
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
    assert record.fields["提交状态"] == "放弃提交"


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
    service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))

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


def test_missing_crm_mapping_creates_auditable_terminal_record_without_calling_crm(
    session_factory: sessionmaker[Session],
) -> None:
    """验证映射缺失保留待创建线索并形成不可重试的审计事实。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    settings_path = Path(get_settings().employee_directory_path)
    settings_path.write_text("id,name,nickname\ncrm-2,其他销售,其他\n", encoding="utf-8")
    crm = MockCRMAdapter()
    service = CrmSubmissionService(
        session_factory, adapter, crm, employee_directory=EmployeeDirectory(settings_path)
    )

    result = service.submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
    )

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

    recovered = CrmSubmissionService(
        session_factory, adapter, crm, employee_directory=EmployeeDirectory(settings_path)
    ).submit(SubmissionCommand("提交今天的线索", "sales-1", "message-13"))

    assert recovered.succeeded == 1
    assert crm.calls == 1


def test_mapping_missing_audit_key_is_bounded_for_a_maximum_length_message_id(
    session_factory: sessionmaker[Session],
) -> None:
    """验证长消息标识仍可写入映射缺失审计事实。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    settings_path = Path(get_settings().employee_directory_path)
    settings_path.write_text("id,name,nickname\ncrm-2,其他销售,其他\n", encoding="utf-8")
    service = CrmSubmissionService(
        session_factory,
        adapter,
        MockCRMAdapter(),
        employee_directory=EmployeeDirectory(settings_path),
    )

    result = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "m" * 128))

    assert result.mapping_missing == 1
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert sync is not None
    assert len(sync.idempotency_key) <= 128


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
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-1")
        assert authorization is not None
        authorization.crm_user_id = "crm-2"
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    assert (
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).succeeded == 1
    )
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

        def create_lead(
            self, payload: object, *, idempotency_key: str, crm_user_id: str
        ) -> object:
            """抛出未分类异常，表示响应边界发生未知故障。"""
            raise RuntimeError("unknown remote outcome")

    crm = UnknownCRM()
    service = CrmSubmissionService(session_factory, adapter, crm)
    result = service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))

    assert result.failed_pending_review == 1
    discard = LeadDiscardService(session_factory).discard(
        lead_id, "sales-1", "等待核实 CRM 结果"
    )
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
    # 新流程只以 CRM 查重结果为准，不再在首次提交前创建本地公司预留。
    assert identity is None
    assert audit is not None and audit.details["attempts"] == 1


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
            lead_id=lead_id, operation="create", smart_table_record_id=record_id,
            idempotency_key=f"crm:create:{lead_id}", canonical_payload=frozen,
            snapshot_hash="a" * 64, request_message_id="message-12",
            submitting_sales_user_id="sales-1", submitting_crm_user_id="crm-1",
            status="processing", processing_lease_expires_at=utc_now() - timedelta(seconds=1),
        )
        session.add(sync)
        session.flush()
        session.add(CrmCompanyIdentity(
            standard_company_name="人工最终公司", state="reserving", creating_lead_id=lead_id,
            creating_sync_record_id=sync.id,
        ))
    monkeypatch.setattr(adapter, "update_record", lambda *_args, **_kwargs: None)
    recovered = MockCRMAdapter()
    result = CrmSubmissionService(session_factory, adapter, recovered).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
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
        result = CrmSubmissionService(session_factory, adapter, crm).submit(
            SubmissionCommand("提交今天的线索", "sales-1", "message-12")
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
    assert "crm_global_identity_activated" not in event_types
    assert activated is None
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
    assert (
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).succeeded == 1
    )
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
) -> None:
    """验证缺映射的真实更新不调用 CRM，补齐映射后才允许提交。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).succeeded
    adapter.update_record(next(iter(adapter.get_records())).record_id, {"手机": "13900000000"})
    settings_path = Path(get_settings().employee_directory_path)
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
    service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12"))
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
    assert {"old_standard_company_name", "new_candidate_company_name", "lead_id",
            "smart_table_record_id", "smart_table_owner_user_id"} <= set(audit.details)


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

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
    )

    assert len(result.duplicate_confirmations) == 1
    assert crm.calls == 0 and crm.update_calls == 0
    with session_factory() as session:
        identity = session.get(CrmCompanyIdentity, "公司 X")
    assert identity is None
    assert crm.search_calls == 1


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

    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交今天的线索", "sales-1", "message-12")
    )

    assert result.succeeded == 1
    assert crm.calls == 1 and crm.update_calls == 0 and crm.search_calls == 1
    with session_factory() as session:
        identity = session.get(CrmCompanyIdentity, "公司 X")
    assert identity is None


def test_retrying_update_does_not_read_changed_smart_table_before_frozen_retry(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 retrying update 先重发冻结快照，期间绝不回读销售后续编辑。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert (
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).succeeded
        == 1
    )
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
    assert (
        service.submit(SubmissionCommand("提交今天的线索", "sales-1", "message-12")).succeeded
        == 1
    )
    record_id = next(iter(adapter.get_records())).record_id
    with session_factory.begin() as session:
        baseline = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        assert baseline is not None
        session.add(
            CrmSyncRecord(
                lead_id=lead_id, operation="update", smart_table_record_id=record_id,
                idempotency_key="frozen-update", canonical_payload=dict(baseline.canonical_payload),
                snapshot_hash="f" * 64, request_message_id="message-13",
                submitting_sales_user_id="sales-1", submitting_crm_user_id="crm-1",
                crm_lead_id=baseline.crm_lead_id,
                crm_lead_owner_user_id=baseline.crm_lead_owner_user_id,
                status="processing",
                processing_lease_expires_at=utc_now()
                + timedelta(seconds=-1 if expired else 60),
            )
        )
    monkeypatch.setattr(adapter, "get_record", lambda _: pytest.fail("不得回读 Smart Table"))
    monkeypatch.setattr(adapter, "update_record", lambda *_args, **_kwargs: None)
    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-14"))
    assert (result.processing if not expired else result.updated) == 1


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


def test_mapping_missing_reply_does_not_double_count_generic_terminal_failure(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 CRM 映射缺失在销售汇总中只计入专用错误分类。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    settings_path = Path(get_settings().employee_directory_path)
    settings_path.write_text("id,name,nickname\ncrm-2,其他销售,其他\n", encoding="utf-8")
    import app.crm.dependencies as crm_dependencies
    import app.crm.service as crm_service
    monkeypatch.setattr(crm_service, "get_settings", lambda: get_settings().model_copy(
        update={"employee_directory_path": str(settings_path)}
    ))
    monkeypatch.setattr(crm_dependencies, "get_settings", lambda: get_settings().model_copy(
        update={"employee_directory_path": str(settings_path)}
    ))
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

    assert "CRM 用户映射缺失 1 条" in reply
    assert "需人工处理失败 0 条" in reply


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
