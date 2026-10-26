"""T12 CRM 首次创建提交的应用服务测试。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Collection, Generator, Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Lock
from time import sleep
from types import SimpleNamespace
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
    _resolve_cross_owner_submission_scope,
    consume_submission_command,
    format_submission_reply,
    notification_key_for_message,
    parse_crm_submission_command,
    parse_cross_owner_submission_request,
    prepare_batch_submission_selection,
    prepare_company_submission_preview,
    terminal_failure_notification_key_for_message,
)
from app.crm.employee_directory import EmployeeDirectory
from app.crm.mock import MockCRMAdapter
from app.crm.service import (
    CROSS_OWNER_SUBMISSION_COMMAND,
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
from app.manage_crm_submission import grant_crm_submission_access
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
from app.messaging.wecom_directory import WecomDirectoryMember
from app.notifications.outbound import WecomOutboundNotificationSender
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableOption, SmartTableRecord
from app.smart_table.registry import build_required_smart_table_schema
from tests.crm_submission_test_utils import submit_today_via_selection


@pytest.mark.parametrize("field_name", ["客户行业", "工艺"])
def test_new_table_option_without_crm_mapping_blocks_create_and_update(
    session_factory: sessionmaker[Session], field_name: str,
) -> None:
    """新合法选项无 CRM 字典映射时，创建和更新均不调用远端且保持待提交状态。"""
    schema = build_required_smart_table_schema()
    option = "未配置行业" if field_name == "客户行业" else "激光切割"
    schema = replace(schema, fields=tuple(
        replace(field, options=(*field.options, SmartTableOption("wecom-id-99999", option)))
        if field.name == field_name else field for field in schema.fields
    ))
    adapter = MockSmartTableAdapter(schema=schema)
    lead_id = _lead(session_factory, adapter)
    record_id = adapter.get_records()[0].record_id
    value = [option] if field_name == "工艺" else option
    adapter.update_record(record_id, {field_name: value})
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    blocked_create = submit_today_via_selection(service, "sales-1", "missing-enum-create")
    assert blocked_create.incomplete == 1
    assert blocked_create.items[0].reason_code == "crm_enum_mapping_missing"
    assert field_name in blocked_create.items[0].missing_fields[0]
    assert option in blocked_create.items[0].missing_fields[0]
    assert crm.search_calls == 0 and crm.calls == 0
    with session_factory() as session:
        assert session.get(Lead, lead_id).lifecycle_state == "pending_create"
        assert session.scalar(select(func.count()).select_from(CrmSyncRecord)) == 0

    # 先以既有字典选项完成首次创建，再测试新枚举不会被当成 CRM 数字 ID 更新。
    known = ["装配"] if field_name == "工艺" else "其他"
    adapter.update_record(record_id, {field_name: known})
    assert submit_today_via_selection(service, "sales-1", "known-enum-create").succeeded == 1
    adapter.update_record(record_id, {field_name: value})
    blocked_update = service.submit(
        SubmissionCommand("提交我的更新", "sales-1", "missing-enum-update")
    )
    assert blocked_update.incomplete == 1 and crm.update_calls == 0
    assert blocked_update.items[0].reason_code == "crm_enum_mapping_missing"
    with session_factory() as session:
        assert session.get(Lead, lead_id).lifecycle_state == "pending_update"
    reply = format_submission_reply(blocked_update, lead_labels={lead_id: "测试公司"})
    assert "缺少 CRM 字典映射" in reply


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
    company_name: str = "人工最终公司",
    message_id: str = "message-12",
    sequence: int = 1,
) -> str:
    """创建一条当天、本人负责且可满足 CRM 创建条件的 Lead。

    参数：session_factory 和 adapter 为测试依赖；crm_user_id、company_name、message_id、sequence
    用于覆盖可选提交人映射、公司标识和独立来源消息。
    返回值：新建后台 Lead 的主键。
    异常：数据库约束失败时由会话层抛出。
    副作用：新增智能表格记录、测试销售身份、来源消息和线索。
    """
    record = adapter.create_record(
        {
            "负责人": "sales-1",
            "线索名称": company_name,
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
        if session.get(SalesAuthorization, "sales-1") is None:
            session.add(
                SalesAuthorization(
                    wecom_user_id="sales-1",
                    crm_user_id=crm_user_id,
                    is_authorized=True,
                    is_active=True,
                )
            )
        # PostgreSQL 实际外键需要父事实先落库，SQLite 不会替我们证明这个顺序。
        session.flush()
        session.add(
            IncomingMessage(
                message_id=message_id, sales_user_id="sales-1", sequence=sequence, raw_payload={}
            )
        )
        session.flush()
        lead = Lead(
            source_message_id=message_id,
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            standard_company_name=company_name,
            field_values={"线索名称": "过期 AI 公司", "手机": "13000000000"},
        )
        session.add(lead)
        session.flush()
        return lead.id


class RecordingCRMAdapter(MockCRMAdapter):
    """记录统一提交 payload，验证 TYC 身份是否越过提交服务边界。"""

    def __init__(self) -> None:
        """初始化空的统一提交 payload 记录。"""
        super().__init__()
        self.submit_payloads: list[dict[str, object]] = []

    def submit_lead(
        self,
        payload: Mapping[str, object],
        *,
        idempotency_key: str,
        crm_user_id: str,
    ) -> CRMCreateResult:
        """记录统一提交请求并复用 Mock 的成功响应。"""
        self.submit_payloads.append(dict(payload))
        return super().submit_lead(
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
    assert crm.submit_calls == 1 and crm.search_calls == 0 and crm.calls == 1
    assert crm.payloads[0]["name"] == "人工最终公司"
    entry_time = crm.payloads[0]["entryTime"]
    assert isinstance(entry_time, str)
    assert datetime.strptime(entry_time, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M") == entry_time
    updated = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-12"))
    assert updated.updated == 1 and crm.submit_calls == 2
    assert crm.submit_payloads[1]["entryTime"] == entry_time
    assert adapter.get_records()[0].fields["提交状态"] == "已提交"
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        lead = session.get(Lead, lead_id)
    assert sync is not None
    assert sync.idempotency_key == f"crm:create:{lead_id}"
    assert sync.canonical_payload["mobile"] == "13800000000"
    assert lead is not None and lead.lifecycle_state == "synced"


@pytest.mark.parametrize(
    "category, expected_category, reason",
    [
        ("business_rejection", "permanent", "明确拒绝"),
        ("transport", "unknown", "待核实"),
        ("malformed_response", "unknown", "待核实"),
        ("gateway", "unknown", "待核实"),
    ],
)
def test_duplicate_update_failure_is_safe_and_never_written_twice(
    session_factory: sessionmaker[Session],
    category: str,
    expected_category: str,
    reason: str,
) -> None:
    """验证确认覆盖的拒绝和未知结果独立持久化、安全解释、重复点击及重试只写一次。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)

    idempotency_keys: list[str] = []

    class FailingUpdateCRM(MockCRMAdapter):
        """模拟明确拒绝或响应丢失；参数同 Mock，记录调用数量且不访问 CRM。"""

        def update_lead(self, *args: object, **kwargs: object) -> CRMCreateResult:
            """模拟一次远端覆盖异常；返回无，抛出受控错误并增加计数。"""
            self.update_calls += 1
            key = kwargs.get("idempotency_key")
            if isinstance(key, str):
                idempotency_keys.append(key)
            raise SopCRMError(
                "secret response must stay private", category=category,
                http_status=500 if category == "gateway" else None,
            )

    crm = FailingUpdateCRM(
        search_results={
            "人工最终公司": (CRMSearchResult("crm-existing", "original-owner", "duplicate"),)
        }
    )
    service = CrmSubmissionService(session_factory, adapter, crm)
    submit_today_via_selection(service, "sales-1", "message-12")
    result = service.resolve_duplicate_confirmation(
        "message-12",
        "sales-1",
        continue_submission=True,
        selected_lead_ids=(lead_id,),
    )
    assert result.failed == 1 and reason in "".join(result.failure_reasons)
    assert "secret" not in str(result)
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        assert sync is not None
        sync_id = sync.id
        assert sync.status == "failed_pending_review" and sync.attempts == 1
        assert sync.failure_category == expected_category and sync.failure_kind == category
        assert sync.crm_lead_owner_user_id == "original-owner"
    service.resolve_duplicate_confirmation(
        "message-12",
        "sales-1",
        continue_submission=True,
        selected_lead_ids=(lead_id,),
    )
    service._claim_and_call(sync_id, "sales-1")
    if expected_category == "unknown":
        # 更改表格并发起新确认也不能绕过上次未知覆盖结果。
        adapter.update_record(adapter.get_records()[0].record_id, {"手机": "13900000000"})
        blocked = submit_today_via_selection(service, "sales-1", "new-attempt")
        assert blocked.failed_pending_review == 1
    expected_attempts = 2 if expected_category == "unknown" else 1
    assert crm.update_calls == expected_attempts and crm.calls == 0
    assert len(idempotency_keys) == expected_attempts and len(set(idempotency_keys)) == 1


@pytest.mark.parametrize("duplicate", (True, False))
def test_legacy_attempted_update_blocks_new_submissions_and_snapshots(
    session_factory: sessionmaker[Session], duplicate: bool,
) -> None:
    """模拟历史 Sync 24，旧 permanent/空 kind 不能因新消息或表格改动而重复覆盖。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter(search_results={
        "人工最终公司": (CRMSearchResult("crm-existing", "original-owner", "duplicate"),)
    } if duplicate else {})
    service = CrmSubmissionService(session_factory, adapter, crm)
    initial = submit_today_via_selection(service, "sales-1", "initial")
    assert initial.succeeded == (0 if duplicate else 1)
    record_id = adapter.get_records()[0].record_id
    if duplicate:
        service.resolve_duplicate_confirmation(
            "initial", "sales-1", continue_submission=True, selected_lead_ids=(lead_id,)
        )
    else:
        adapter.update_record(record_id, {"手机": "13900000000"})
        assert service.submit(SubmissionCommand("提交我的更新", "sales-1", "update")).updated == 1
    with session_factory.begin() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.operation == "update"))
        sync.status = "failed_pending_review"
        sync.failure_category = "permanent"
        sync.failure_kind = None
        sync.failure_code = None
        sync.failure_summary = "SopCRMError"
        frozen = (sync.id, sync.canonical_payload, sync.snapshot_hash, sync.request_message_id)
        session.get(Lead, lead_id).lifecycle_state = (
            "pending_create" if duplicate else "pending_update"
        )
        session.get(SalesAuthorization, "sales-1").is_authorized = False
    calls = (crm.calls, crm.update_calls, crm.search_calls)
    for index, fields in enumerate(({}, {"手机": "13700000000"}, {"线索名称": "另一个测试公司"})):
        adapter.update_record(record_id, {"提交状态": "未提交", **fields})
        command = SubmissionCommand(
            "提交指定线索" if duplicate else "提交我的更新", "sales-1", f"retry-{index}",
            target_lead_id=lead_id if duplicate else None,
        )
        assert service.submit(command).failed_pending_review == 1
    assert (crm.calls, crm.update_calls, crm.search_calls) == calls
    with session_factory() as session:
        sync = session.get(CrmSyncRecord, frozen[0])
        assert (
            sync.id, sync.canonical_payload, sync.snapshot_hash, sync.request_message_id
        ) == frozen
        assert (sync.failure_category, sync.failure_kind, sync.attempts) == ("permanent", None, 1)
        assert session.scalar(select(func.count()).select_from(CrmSyncRecord)) == (
            1 if duplicate else 2
        )


@pytest.mark.parametrize("entry", ("claim", "confirmation"))
def test_new_sync_cannot_bypass_legacy_unknown_update(
    session_factory: sessionmaker[Session], entry: str,
) -> None:
    """验证已有旧版未知覆盖时，另一个新同步或重复确认入口仍不能调用 CRM。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    with session_factory.begin() as session:
        for number in (24, 25):
            session.add(CrmSyncRecord(
                id=number, lead_id=lead_id,
                operation="create" if number == 25 and entry == "confirmation" else "update",
                generation=1 if number == 25 and entry == "confirmation" else None,
                smart_table_record_id=adapter.get_records()[0].record_id,
                idempotency_key=f"legacy-{number}", canonical_payload={"name": "人工最终公司"},
                snapshot_hash=str(number) * 32, request_message_id=f"message-{number}",
                submitting_sales_user_id="sales-1", submitting_crm_user_id="crm-1",
                crm_lead_id="crm-existing", attempts=1 if number == 24 else 0,
                status="failed_pending_review" if number == 24 else (
                    "awaiting_duplicate_confirmation" if entry == "confirmation" else "pending"
                ),
                failure_category="permanent" if number == 24 else None,
                failure_kind=None,
            ))
    if entry == "claim":
        assert service._claim_and_call(25, "sales-1") == "failed_pending_review"
    else:
        result = service.resolve_duplicate_confirmation(
            "message-25", "sales-1", continue_submission=True, selected_lead_ids=(lead_id,)
        )
        assert result.failed == 1
    assert crm.calls == crm.update_calls == crm.search_calls == 0
    with session_factory() as session:
        sync = session.get(CrmSyncRecord, 24)
        assert (sync.status, sync.failure_category, sync.failure_kind, sync.attempts) == (
            "failed_pending_review", "permanent", None, 1
        )


def test_explicit_business_rejection_allows_a_new_confirmed_snapshot(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """明确业务拒绝保留 permanent 分类；修正字段后新确认可以执行，旧记录不被重写。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter(search_results={
        "人工最终公司": (CRMSearchResult("crm-existing", "original-owner", "duplicate"),)
    })
    service = CrmSubmissionService(session_factory, adapter, crm)
    submit_today_via_selection(service, "sales-1", "rejected")
    original_update = crm.update_lead

    def reject(*args: object, **kwargs: object) -> CRMCreateResult:
        """模拟明确拒绝；无远端调用，增加测试计数并抛出稳定业务分类。"""
        crm.update_calls += 1
        raise SopCRMError("明确拒绝", category="business_rejection")

    monkeypatch.setattr(crm, "update_lead", reject)
    assert service.resolve_duplicate_confirmation(
        "rejected", "sales-1", continue_submission=True, selected_lead_ids=(lead_id,)
    ).failed == 1
    monkeypatch.setattr(crm, "update_lead", original_update)
    adapter.update_record(adapter.get_records()[0].record_id, {"手机": "13700000000"})
    assert submit_today_via_selection(service, "sales-1", "corrected").duplicate_confirmations
    assert service.resolve_duplicate_confirmation(
        "corrected", "sales-1", continue_submission=True, selected_lead_ids=(lead_id,)
    ).submitted == 1
    assert crm.update_calls == 2
    with session_factory() as session:
        old = session.scalar(
            select(CrmSyncRecord).where(CrmSyncRecord.request_message_id == "rejected")
        )
        assert (old.failure_category, old.failure_kind, old.attempts) == (
            "permanent", "business_rejection", 1
        )


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


def test_crm_create_batch_respects_concurrency_limit_and_returns_each_result(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 CRM 批次并发数受配置约束，且每个目标均得到独立结果。"""
    import app.crm.service as crm_service

    settings = get_settings().model_copy(update={"crm_submission_max_concurrency": 2})
    monkeypatch.setattr(crm_service, "get_settings", lambda: settings)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = CrmSubmissionService(session_factory, adapter, MockCRMAdapter())
    # 该用例只验证并发调度，模拟生产数据库方言；具体 SQLite 业务用例保持串行。
    monkeypatch.setitem(
        session_factory.kw,
        "bind",
        SimpleNamespace(dialect=SimpleNamespace(name="postgresql")),
    )
    monkeypatch.setattr(service, "_write_back_batch_submission_status", lambda _lead_ids: None)
    lock = Lock()
    active = 0
    maximum_active = 0

    def submit_one(
        lead_id: str,
        _command: SubmissionCommand,
        *,
        initial_record: SmartTableRecord | None = None,
    ) -> crm_service.CreateSubmissionOutcome:
        """模拟单条远程调用并记录同时运行的任务数量。"""
        nonlocal active, maximum_active
        with lock:
            active += 1
            maximum_active = max(maximum_active, active)
        sleep(0.02)
        with lock:
            active -= 1
        return crm_service._create_outcome("succeeded")

    monkeypatch.setattr(service, "_submit_create", submit_one)
    command = SubmissionCommand("提交指定线索", "sales-1", "batch-message")
    submissions = [(f"lead-{index}", command, None) for index in range(6)]
    callback_results: list[str] = []

    outcomes = service._submit_create_batch(
        submissions,
        on_item_result=lambda item: callback_results.append(item.lead_id),
    )

    assert maximum_active == 2
    assert set(outcomes) == {lead_id for lead_id, _, _ in submissions}
    assert all(outcome.status == "succeeded" for outcome in outcomes.values())
    assert set(callback_results) == set(outcomes)


def test_all_submission_does_not_reopen_abandoned_sync_after_table_tampering(
    session_factory: sessionmaker[Session],
) -> None:
    """验证表格状态被手动改回未提交后，CRM 已放弃的同步仍不能重开。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record = adapter.get_records()[0]
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)

    first = submit_today_via_selection(service, "sales-1", "message-abandoned")
    assert first.succeeded == 1
    with session_factory.begin() as session:
        sync = session.scalar(
            select(CrmSyncRecord).where(
                CrmSyncRecord.lead_id == lead_id,
                CrmSyncRecord.operation == "create",
            )
        )
        assert sync is not None
        sync.status = "abandoned"
    adapter.update_record(record.record_id, {"提交状态": "未提交"})
    assert service.list_submission_candidates("提交我所有线索", "sales-1") == ()
    with pytest.raises(ValueError, match="批量提交必须通过"):
        service.submit(SubmissionCommand("提交我所有线索", "sales-1", "message-reopened"))
    assert crm.search_calls == 0 and crm.calls == 1


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

    original_get_records = adapter.get_records

    def get_records_with_member_name(
        record_ids: Collection[str] | None = None,
        *,
        owner_user_ids: tuple[str, ...] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
        submission_status: str | None = None,
        capture_date: date | None = None,
    ) -> list[SmartTableRecord]:
        """给批量 Mock 记录补充真实 CLI 会返回的 userName 元数据。"""
        return [
            SmartTableRecord(
                record_id=current.record_id,
                fields=current.fields,
                member_names={"负责人": "sales-1"},
            )
            for current in original_get_records(
                record_ids,
                owner_user_ids=owner_user_ids,
                owner_display_names=owner_display_names,
                submission_status=submission_status,
                capture_date=capture_date,
            )
        ]

    monkeypatch.setattr(adapter, "get_records", get_records_with_member_name)
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
    original_get_records = adapter.get_records

    def get_records_without_member_name(
        record_ids: Collection[str] | None = None,
        *,
        owner_user_ids: tuple[str, ...] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
        submission_status: str | None = None,
        capture_date: date | None = None,
    ) -> list[SmartTableRecord]:
        """返回不含成员显示名的批量快照。"""
        return [
            SmartTableRecord(record.record_id, record.fields, member_names={})
            for record in original_get_records(
                record_ids,
                owner_user_ids=owner_user_ids,
                owner_display_names=owner_display_names,
                submission_status=submission_status,
                capture_date=capture_date,
            )
        ]

    monkeypatch.setattr(adapter, "get_records", get_records_without_member_name)
    crm = MockCRMAdapter()

    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm),
        "sales-1",
        "message-12",
    )

    assert result.mapping_missing == 1
    assert crm.search_calls == 0
    assert crm.calls == 0


def test_unified_follow_up_is_reported_as_existing_lead_followed_up(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 CRM 查重命中后的 FOLLOW_UP 作为跟进成功通知，不误报为创建。"""

    class FollowUpCRM(MockCRMAdapter):
        def submit_lead(
            self,
            payload: Mapping[str, object],
            *,
            idempotency_key: str,
            crm_user_id: str,
        ) -> CRMCreateResult:
            """记录一次统一提交并模拟 CRM 查重后完成跟进。"""
            self.submit_calls += 1
            self.submit_payloads.append(dict(payload))
            return CRMCreateResult(
                None, None, "CRM submit FOLLOW_UP succeeded", action="FOLLOW_UP"
            )

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = FollowUpCRM()
    result = submit_today_via_selection(
        CrmSubmissionService(session_factory, adapter, crm), "sales-1", "follow-up-message"
    )

    assert result.succeeded == 0 and result.followed_up == 1
    assert result.items[0].status is SubmissionItemStatus.FOLLOWED_UP
    assert result.items[0].reason_code == "crm_followed_up"
    assert crm.submit_calls == 1 and crm.calls == 0
    reply = format_submission_reply(
        result, lead_labels={lead_id: "测试公司｜张工"}, selected_count=1
    )
    assert "CRM 已存在并跟进 1 条" in reply
    assert "测试公司｜张工：CRM 中已存在该线索，已进行跟进" in reply
    assert "创建成功" not in reply

    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.event_type == "crm_follow_up_succeeded"
            )
        )
    assert sync is not None and "FOLLOW_UP" in (sync.response_summary or "")
    assert sync.crm_lead_id is None
    sync.response_summary = "CRM submit succeeded"
    assert CrmSubmissionService._completed_sync_outcome(sync) == "followed_up"
    assert audit is not None


def test_my_updates_follow_up_is_reported_as_existing_lead_followed_up(
    session_factory: sessionmaker[Session],
) -> None:
    """验证“提交我的更新”收到 FOLLOW_UP 时也使用统一跟进状态和说明。"""

    class CreateThenFollowUpCRM(MockCRMAdapter):
        def submit_lead(
            self,
            payload: Mapping[str, object],
            *,
            idempotency_key: str,
            crm_user_id: str,
        ) -> CRMCreateResult:
            """首次模拟创建，后续更新模拟 CRM 命中已有线索并完成跟进。"""
            if self.submit_calls == 0:
                return super().submit_lead(
                    payload, idempotency_key=idempotency_key, crm_user_id=crm_user_id
                )
            self.submit_calls += 1
            self.submit_payloads.append(dict(payload))
            return CRMCreateResult(
                None, None, "CRM submit FOLLOW_UP succeeded", action="FOLLOW_UP"
            )

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = CreateThenFollowUpCRM()
    service = CrmSubmissionService(session_factory, adapter, crm)
    created = service.submit(
        SubmissionCommand("提交指定线索", "sales-1", "initial", target_lead_id=lead_id)
    )
    assert created.succeeded == 1

    record = adapter.get_records()[0]
    adapter.update_record(record.record_id, {"手机": "13900000000"})
    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "update-follow-up"))

    assert result.updated == 0 and result.followed_up == 1
    assert result.items[0].status is SubmissionItemStatus.FOLLOWED_UP
    assert result.items[0].reason_code == "crm_followed_up"
    reply = format_submission_reply(
        result, lead_labels={lead_id: "测试公司｜张工"}, selected_count=1
    )
    assert "CRM 已存在并跟进 1 条" in reply
    assert "测试公司｜张工：CRM 中已存在该线索，已进行跟进" in reply
    assert "创建成功" not in reply


@pytest.mark.parametrize(
    (
        "category",
        "http_status",
        "error_code",
        "expected_status",
        "expected_failure_category",
    ),
    [
        ("transport", None, None, "retrying", "transient"),
        ("authentication", 401, None, "failed_pending_review", "permanent"),
        ("business", 200, None, "failed_pending_review", "permanent"),
        ("gateway", 403, None, "failed_pending_review", "permanent"),
        ("gateway", 503, None, "retrying", "transient"),
        ("gateway", 200, "500", "retrying", "transient"),
        ("gateway", 200, "-1", "failed_pending_review", "permanent"),
        ("malformed_response", 200, None, "failed_pending_review", "permanent"),
    ],
)
def test_sop_error_category_controls_submission_failure_semantics(
    session_factory: sessionmaker[Session],
    category: str,
    http_status: int | None,
    error_code: str | None,
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
                "sanitized CRM failure",
                category=category,
                http_status=http_status,
                error_code=error_code,
            )

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
    if expected_status == "retrying":
        expected_summary = (
            f"CRM gateway HTTP {http_status} code {error_code}"
            if category == "gateway" and http_status is not None and error_code is not None
            else f"CRM gateway HTTP {http_status}"
            if category == "gateway" and http_status is not None
            else f"CRM gateway code {error_code}"
            if category == "gateway" and error_code is not None
            else "CRM transport error"
        )
        assert sync.response_summary == expected_summary


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
    assert "需人工处理 1 条" in reply
    assert "CRM 检测到重复线索但未返回可操作的线索 ID" in reply
    expected_detail = (
        "希捷国际科技（无锡）有限公司｜杨总：CRM 检测到重复线索但未返回可操作的线索 ID"
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
    assert crm.search_calls == 0
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
    assert crm.submit_payloads[0]["tycCustomerId"] == "tyc-verified"


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
    assert "tycCustomerId" not in crm.submit_payloads[0]


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
    assert "tycCustomerId" not in crm.submit_payloads[0]


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
    """验证单条确认卡的 temporary 目标按当前完整快照调用 CRM 统一提交接口。"""
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
    assert crm.search_calls == 0
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
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch,
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
    bulk_read = Mock(wraps=adapter.get_records)
    monkeypatch.setattr(adapter, "get_records", bulk_read)

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
    assert crm.search_calls == 0 and crm.submit_calls == 1 and crm.calls == 1
    assert len(third.items) == 1
    assert third.items[0].status is SubmissionItemStatus.NOT_SUBMITTED
    assert third.items[0].reason_code == "already_submitted"
    assert crm.search_calls == 0 and crm.calls == 1
    assert bulk_read.call_count == 2
    assert [call.args[0] for call in bulk_read.call_args_list] == [
        {record.record_id},
        {record.record_id},
    ]


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
        assert "🔄 重试中 1 条" in first and "✅ CRM 新增/更新成功 0 条" in first
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
        assert "✅ CRM 新增/更新成功 1 条" in reply and crm.calls == 1
    elif failure == "missing":
        assert "反馈线索1：缺少「职务」" in reply and crm.calls == crm.search_calls == 0
    else:
        assert "需人工处理 1 条" in reply and "✅ CRM 新增/更新成功 0 条" in reply
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
        get_records = adapter.get_records

        def get_records_without_first_snapshot(
            record_ids: Collection[str] | None = None,
            *,
            owner_user_ids: tuple[str, ...] | None = None,
            owner_display_names: Mapping[str, str] | None = None,
            submission_status: str | None = None,
            capture_date: date | None = None,
        ) -> list[SmartTableRecord]:
            """模拟批量回读缺少第一条记录，验证逐条回退故障不阻塞其余线索。"""
            return [
                record
                for record in get_records(
                    record_ids,
                    owner_user_ids=owner_user_ids,
                    owner_display_names=owner_display_names,
                    submission_status=submission_status,
                    capture_date=capture_date,
                )
                if record.record_id != first_record.record_id
            ]

        monkeypatch.setattr(adapter, "get_records", get_records_without_first_snapshot)
    first = consume_submission_command(
        session_factory, adapter, crm, event_id, command_text="重新提交待完善的线索",
    )
    assert "✅ CRM 新增/更新成功 1 条" in first
    expected_status = (
        "🔄 重试中 1 条"
        if failure == "once"
        else "⚪ 未提交 1 条"
        if failure == "snapshot"
        else "需人工处理 1 条"
    )
    assert expected_status in first
    if failure == "snapshot":
        assert "反馈线索1：线索状态已变化，请重新发起提交" in first
    # 重放期间历史选择改变；冻结目标仍须继续原来的第二条，而不能回退/扩大。
    _retry_command_event(
        session_factory, ("unselected-later-lead",), message_id="later", sequence=3,
    )
    final = consume_submission_command(
        session_factory, adapter, crm, event_id, command_text="重新提交待完善的线索",
    )
    succeeded = 2 if failure == "once" else 1
    assert f"✅ CRM 新增/更新成功 {succeeded} 条" in final
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
    assert "✅ CRM 新增/更新成功 1 条" in reply and crm.calls == 1
    gateway.classify_submission_intent.assert_called_once_with("重新提交")
    # 使用新的来源消息重复发命令时仍沿用历史选择，明确返回已提交而不创建新 generation。
    next_event = _retry_command_event(
        session_factory, (lead_id,), message_id="repeated-command", sequence=3,
    )
    repeated = consume_submission_command(
        session_factory, adapter, crm, next_event, command_text="重新提交待完善的线索",
    )
    assert "该线索已完成提交，本次未重复创建" in repeated
    assert "✅ CRM 新增/更新成功 0 条" in repeated and crm.calls == 1


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


def test_batch_candidates_read_only_linked_records_for_requested_owner_scope(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证批量候选只按负责人线索已绑定的记录标识一次回读。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = adapter.get_records()[0].record_id
    original_get_records = adapter.get_records
    calls: list[tuple[Collection[str] | None, tuple[str, ...] | None]] = []

    def capture_get_records(
        record_ids: Collection[str] | None = None,
        *,
        owner_user_ids: tuple[str, ...] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
        submission_status: str | None = None,
        capture_date: date | None = None,
    ) -> list[SmartTableRecord]:
        """记录候选回读使用的负责人、状态和日期筛选条件。"""
        calls.append((record_ids, owner_user_ids))
        return original_get_records(
            record_ids,
            owner_user_ids=owner_user_ids,
            owner_display_names=owner_display_names,
            submission_status=submission_status,
            capture_date=capture_date,
        )

    monkeypatch.setattr(adapter, "get_records", capture_get_records)
    candidates = CrmSubmissionService(
        session_factory, adapter, MockCRMAdapter()
    ).list_submission_candidates("提交我所有线索", "sales-1")

    assert tuple(candidate.lead_id for candidate in candidates) == (lead_id,)
    assert calls == [(None, ("sales-1",))]


def test_today_three_candidates_process_complete_and_incomplete_independently(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
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
    initial_records = {
        candidate.record.record_id: candidate.record
        for candidate in candidates
        if candidate.record is not None
    }
    # 提交动作复用候选阶段快照，不在确认回调中再次查询智能表格。
    bulk_read = Mock(wraps=adapter.get_records)
    single_read = Mock(wraps=adapter.get_record)
    monkeypatch.setattr(adapter, "get_records", bulk_read)
    monkeypatch.setattr(adapter, "get_record", single_read)
    result = service.submit_selected(
        SubmissionCommand("提交今天的线索", "sales-1", "today-three-mixed"),
        tuple(candidate.lead_id for candidate in candidates),
        initial_records=initial_records,
    )

    assert {candidate.lead_id for candidate in candidates} == {
        complete_id,
        leads[0][0],
        leads[1][0],
    }
    assert {candidate.company_name for candidate in candidates} == {
        "人工最终公司",
        "临时测试公司-1",
        "临时测试公司-2",
    }
    assert len(result.items) == 3
    statuses = {item.lead_id: item.status for item in result.items}
    assert statuses[complete_id] is SubmissionItemStatus.CREATED
    assert statuses[leads[0][0]] is SubmissionItemStatus.INCOMPLETE
    assert statuses[leads[1][0]] is SubmissionItemStatus.INCOMPLETE
    missing = {item.lead_id: item.missing_fields for item in result.items}
    assert missing[leads[0][0]] == ("业务线", "职务")
    assert missing[leads[1][0]] == ("沟通方式",)
    assert bulk_read.call_count == 0
    # 唯一 get_record 是成功后“提交中”状态写入的安全预读。
    assert single_read.call_count == 1
    assert crm.search_calls == 0 and crm.submit_calls == 1 and crm.calls == 1


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
    assert crm.calls == 1
    assert crm.crm_user_ids == ["crm-1"]
    assert crm.search_calls == 0
    assert crm.calls == 1
    with session_factory() as session:
        lead_after_submit = session.get(Lead, lead_id)
    assert lead_after_submit is not None and lead_after_submit.lifecycle_state == "synced"


def test_backend_owner_change_does_not_override_table_candidate(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 CRM 候选负责人以冻结的智能表格行决定，不受后台 owner 变化影响。"""
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
    assert result.items[0].status is SubmissionItemStatus.CREATED
    assert result.succeeded == 1
    assert crm.search_calls == 0 and crm.calls == 1


def test_abandoned_card_after_owner_transfer_does_not_read_new_owners_record(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证过期的放弃线索卡在负责人转交后不会回读新负责人的表格记录。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = adapter.get_records()[0].record_id
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.lifecycle_state = "pending_create"
        lead.smart_table_owner_user_id = "sales-2"
        session.add(SalesAuthorization(wecom_user_id="sales-2", is_active=True))
        session.add(
            CrmSyncRecord(
                lead_id=lead_id,
                operation="create",
                generation=1,
                smart_table_record_id=record_id,
                idempotency_key=f"crm:create:{lead_id}",
                canonical_payload={},
                snapshot_hash="abandoned-owner-check",
                request_message_id="previous-abandoned-request",
                submitting_sales_user_id="sales-1",
                status="abandoned",
            )
        )
    read_records = Mock(wraps=adapter.get_records)
    monkeypatch.setattr(adapter, "get_records", read_records)

    result = service.submit_selected(
        SubmissionCommand("帮我提交放弃提交的线索", "sales-1", "stale-abandoned-card"),
        (lead_id,),
    )

    assert result.not_submitted == 1
    assert result.items[0].status is SubmissionItemStatus.NOT_SUBMITTED
    read_records.assert_not_called()
    assert crm.calls == 0


def test_today_submission_processes_only_card_selected_candidate(
    session_factory: sessionmaker[Session],
) -> None:
    """验证“今天”只有通过服务端候选卡勾选后才提交到 CRM。"""
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
    assert crm.search_calls == 0
    assert crm.calls == 1


def test_authorized_operator_can_submit_selected_other_owner_lead(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证跨员工提交需专用授权，勾选后仍使用线索负责人的 CRM 映射。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    source_record = adapter.get_records()[0]
    target_fields = {
        name: value for name, value in source_record.fields.items() if name != "录入时间"
    }
    target_fields["负责人"] = "sales-2"
    owner_record = adapter.create_record(
        target_fields,
        actor=SmartTableActor.ROBOT,
        member_names={"负责人": "张三"},
    )
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(wecom_user_id="sales-2", display_name="张三", is_active=True)
        )
        session.add(SalesAuthorization(wecom_user_id="operator", is_active=True))
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-2"
        lead.smart_table_record_id = owner_record.record_id

    crm = MockCRMAdapter()
    directory = Mock()
    directory.resolve.return_value = "crm-sales-2"
    service = CrmSubmissionService(
        session_factory, adapter, crm, employee_directory=directory
    )
    with pytest.raises(ValueError, match="没有跨员工 CRM 提交权限"):
        service.list_submission_candidates(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            owner_scope_user_ids=("sales-2",),
        )

    with session_factory.begin() as session:
        operator = session.get(SalesAuthorization, "operator")
        assert operator is not None
        operator.can_submit_other_sales_leads = True
    bulk_read = Mock(wraps=adapter.get_records)
    monkeypatch.setattr(adapter, "get_records", bulk_read)
    candidates = service.list_submission_candidates(
        CROSS_OWNER_SUBMISSION_COMMAND,
        "operator",
        owner_scope_user_ids=("sales-2",),
    )
    with session_factory.begin() as session:
        operator = session.get(SalesAuthorization, "operator")
        assert operator is not None
        operator.can_submit_other_sales_leads = False
    denied = service.submit_selected(
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "operator-submit-revoked",
            owner_scope_user_ids=("sales-2",),
        ),
        (candidates[0].lead_id,),
    )
    denied_reply = format_submission_reply(
        denied,
        lead_labels={lead_id: "人工最终公司"},
        selected_count=1,
    )
    assert denied.not_submitted == 1
    assert "跨员工提交权限已撤销" in denied_reply
    assert crm.calls == 0

    with session_factory.begin() as session:
        operator = session.get(SalesAuthorization, "operator")
        assert operator is not None
        operator.can_submit_other_sales_leads = True
    result = service.submit_selected(
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "operator-submit",
            owner_scope_user_ids=("sales-2",),
        ),
        (candidates[0].lead_id,),
    )

    assert tuple(item.lead_id for item in candidates) == (lead_id,)
    assert result.succeeded == 1
    assert bulk_read.call_count == 2
    assert bulk_read.call_args_list[0].kwargs == {"record_ids": (owner_record.record_id,)}
    assert bulk_read.call_args_list[1].args[0] == {owner_record.record_id}


@pytest.mark.parametrize("submit_by_selection", [False, True])
def test_self_crm_create_and_update_do_not_require_sales_authorization_row(
    session_factory: sessionmaker[Session], submit_by_selection: bool,
) -> None:
    """验证本人提交只校验负责人范围，不要求提交人在销售授权目录登记。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = adapter.get_records()[0].record_id
    with session_factory.begin() as session:
        session.delete(session.get(SalesAuthorization, "sales-1"))

    directory = Mock()
    directory.resolve.return_value = "crm-1"
    crm = MockCRMAdapter()
    service = CrmSubmissionService(
        session_factory, adapter, crm, employee_directory=directory
    )

    if submit_by_selection:
        command = SubmissionCommand("提交今天的线索", "sales-1", "unregistered-create")
        candidates = service.list_submission_candidates(command.text, command.sales_user_id)
        created = service.submit_selected(
            command, tuple(candidate.lead_id for candidate in candidates)
        )
    else:
        created = service.submit(
            SubmissionCommand(
                "提交指定线索", "sales-1", "unregistered-create", target_lead_id=lead_id
            )
        )
    adapter.update_record(record_id, {"手机": "13900000000"})
    updated = service.submit(SubmissionCommand("提交我的更新", "sales-1", "unregistered-update"))

    assert created.succeeded == 1
    assert updated.updated == 1
    assert crm.submit_calls == 2
    assert crm.crm_user_ids == ["crm-1", "crm-1"]


def test_existing_smart_table_lead_can_be_linked_without_authorization_row(
    session_factory: sessionmaker[Session],
) -> None:
    """验证本人负责的未提交表格行可直接绑定，不依赖授权目录登记。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {
            "负责人": "unregistered-sales",
            "线索名称": "未登记成员线索",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
    )

    _link_unsubmitted_smart_table_records(
        session_factory,
        adapter,
        SubmissionCommand("提交我所有线索", "unregistered-sales", "link-unregistered"),
    )

    with session_factory() as session:
        linked = session.scalar(select(Lead).where(Lead.smart_table_record_id == record.record_id))
    assert linked is not None
    assert linked.smart_table_owner_user_id == "unregistered-sales"


def test_submission_command_parser_routes_named_and_global_owner_scopes() -> None:
    """验证员工本名/花名和显式负责人句式都进入代提交，疑问句仍不能触发。"""

    assert parse_crm_submission_command("提交负责人张三的线索") == CROSS_OWNER_SUBMISSION_COMMAND
    assert parse_crm_submission_command("提交JJ的线索") == CROSS_OWNER_SUBMISSION_COMMAND
    assert parse_crm_submission_command("提交张华杰的线索") == CROSS_OWNER_SUBMISSION_COMMAND
    assert parse_crm_submission_command("提交所有员工的线索") == CROSS_OWNER_SUBMISSION_COMMAND
    assert parse_crm_submission_command("提交负责人张三的线索？") is None
    assert parse_crm_submission_command("提交负责人*的线索") is None


def test_cross_owner_submission_parser_resolves_shanghai_short_dates() -> None:
    """验证短日期取上海当前年最近过去日期，显式年份和无效日期边界明确。"""
    today = date(2026, 10, 14)
    assert parse_cross_owner_submission_request("提交10月13号大海的线索", today=today) == (
        "大海",
        date(2026, 10, 13),
    )
    assert parse_cross_owner_submission_request("提交11月1日大海的线索", today=today) == (
        "大海",
        date(2025, 11, 1),
    )
    assert parse_cross_owner_submission_request("提交2024年2月29日大海的线索", today=today) == (
        "大海",
        date(2024, 2, 29),
    )
    assert parse_crm_submission_command("提交10月13号大海的线索") == CROSS_OWNER_SUBMISSION_COMMAND
    assert parse_cross_owner_submission_request("提交2月30号大海的线索", today=today) is None
    assert parse_crm_submission_command("提交2月30号大海的线索") == CROSS_OWNER_SUBMISSION_COMMAND


def test_cross_owner_all_scope_does_not_resolve_a_directory_member(
    session_factory: sessionmaker[Session],
) -> None:
    """验证提交所有员工的线索按现有负责人范围处理，不访问未定义的单人通讯录结果。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="operator",
                is_active=True,
                can_submit_other_sales_leads=True,
            )
        )

    resolved, error = _resolve_cross_owner_submission_scope(
        session_factory,
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "all-owner-command",
            target_owner_name="*",
        ),
    )

    assert error is None
    assert resolved is not None
    assert resolved.owner_scope_user_ids == ("sales-1",)
    assert resolved.target_owner_name == "*"


def test_cross_owner_selection_card_freezes_target_and_shows_names_only(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证代提交按企微姓名冻结负责人范围，候选预览只展示线索名称。"""
    import app.crm.commands as crm_commands

    settings = get_settings().model_copy(
        update={
            "wecom_card_callback_enabled": True,
            "wecom_card_transport_configured": True,
            "wecom_card_callback_handler_configured": True,
        }
    )
    monkeypatch.setattr(crm_commands, "get_settings", lambda: settings)
    monkeypatch.setattr(
        crm_commands,
        "resolve_wecom_member",
        lambda *_args: WecomDirectoryMember("sales-2", "张三(花名)"),
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    next_day_lead_id = _lead(
        session_factory,
        adapter,
        message_id="cross-owner-next-day-lead",
        sequence=2,
        company_name="次日线索",
    )
    source_record = adapter.get_records()[0]
    owner_record = adapter.create_record(
        dict(source_record.fields),
        actor=SmartTableActor.ROBOT,
        member_names={"负责人": "张三"},
    )
    adapter.update_record(owner_record.record_id, {"负责人": "sales-2"})
    next_day_record = adapter.get_records()[1]
    adapter.update_record(next_day_record.record_id, {"负责人": "sales-2"})
    orphan_record = adapter.create_record(
        {
            **dict(source_record.fields),
            "负责人": "sales-2",
            "线索名称": "当日表格漏链公司",
            "备注": "原有备注\n\n原始消息：当日表格漏链公司，需求明确。",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
        member_names={"负责人": "张三"},
    )
    next_day_orphan_record = adapter.create_record(
        {
            **dict(source_record.fields),
            "负责人": "sales-2",
            "线索名称": "次日表格漏链公司",
            "备注": "原有备注\n\n原始消息：次日表格漏链公司，需求明确。",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
        member_names={"负责人": "张三"},
    )
    same_day_existing_record = adapter.create_record(
        {
            **dict(source_record.fields),
            "负责人": "sales-2",
            "线索名称": "当日已有草稿公司",
            "备注": "原有备注\n\n原始消息：当日已有草稿公司，补充信息。",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
        member_names={"负责人": "张三"},
    )
    manual_dated_record = adapter.create_record(
        {
            **dict(source_record.fields),
            "负责人": "sales-2",
            "线索名称": "手工录入日期线索",
            "备注": "人工录入的智能表格记录，没有机器人原始消息。",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
        member_names={"负责人": "张三"},
    )
    # 模拟企微读取到的只读系统时间，不允许通过正常写入接口伪造创建时间。
    adapter._records[manual_dated_record.record_id] = SmartTableRecord(
        manual_dated_record.record_id,
        {**manual_dated_record.fields, "录入时间": "2026-10-13"},
        manual_dated_record.member_names,
    )
    next_day_duplicate_record = adapter.create_record(
        {
            **dict(source_record.fields),
            "负责人": "sales-2",
            "线索名称": "跨日重名公司",
            "备注": "原有备注\n\n原始消息：跨日重名公司，次日消息。",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
        member_names={"负责人": "张三"},
    )
    with session_factory.begin() as session:
        session.add_all(
            [
                SalesAuthorization(
                    wecom_user_id="sales-2", display_name="张三", is_active=True
                ),
                SalesAuthorization(
                    wecom_user_id="operator",
                    is_active=True,
                    can_submit_other_sales_leads=True,
                ),
                IncomingMessage(
                    message_id="cross-owner-orphan-source",
                    sales_user_id="sales-2",
                    sequence=1,
                    raw_payload={},
                    normalized_text="当日表格漏链公司，需求明确。",
                    received_at=datetime(2026, 10, 13, 3, tzinfo=UTC),
                ),
                IncomingMessage(
                    message_id="cross-owner-next-day-orphan-source",
                    sales_user_id="sales-2",
                    sequence=2,
                    raw_payload={},
                    normalized_text="次日表格漏链公司，需求明确。",
                    received_at=datetime(2026, 10, 13, 16, tzinfo=UTC),
                ),
                IncomingMessage(
                    message_id="cross-owner-same-day-existing-source",
                    sales_user_id="sales-2",
                    sequence=3,
                    raw_payload={},
                    normalized_text="当日已有草稿公司，补充信息。",
                    received_at=datetime(2026, 10, 13, 4, tzinfo=UTC),
                ),
                IncomingMessage(
                    message_id="cross-owner-duplicate-target-day-source",
                    sales_user_id="sales-2",
                    sequence=4,
                    raw_payload={},
                    normalized_text="跨日重名公司，目标日消息。",
                    received_at=datetime(2026, 10, 13, 5, tzinfo=UTC),
                ),
                IncomingMessage(
                    message_id="cross-owner-duplicate-next-day-source",
                    sales_user_id="sales-2",
                    sequence=5,
                    raw_payload={},
                    normalized_text="跨日重名公司，次日消息。",
                    received_at=datetime(2026, 10, 13, 16, tzinfo=UTC),
                ),
            ]
        )
        session.flush()
        same_day_existing_lead = Lead(
            source_message_id="cross-owner-same-day-existing-source",
            original_capturing_sales_user_id="sales-2",
            smart_table_owner_user_id="sales-2",
            smart_table_record_id=None,
            lifecycle_state="pending_create",
            standard_company_name="当日已有草稿公司",
            field_values={"线索名称": "当日已有草稿公司"},
        )
        cross_day_duplicate_lead = Lead(
            source_message_id="cross-owner-duplicate-target-day-source",
            original_capturing_sales_user_id="sales-2",
            smart_table_owner_user_id="sales-2",
            smart_table_record_id=None,
            lifecycle_state="pending_create",
            standard_company_name="跨日重名公司",
            field_values={"线索名称": "跨日重名公司"},
        )
        session.add_all([same_day_existing_lead, cross_day_duplicate_lead])
        session.flush()
        same_day_existing_lead_id = same_day_existing_lead.id
        cross_day_duplicate_lead_id = cross_day_duplicate_lead.id
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-2"
        lead.smart_table_record_id = owner_record.record_id
        lead.created_at = datetime(2026, 10, 13, 16, tzinfo=UTC)
        source_message = session.get(IncomingMessage, lead.source_message_id)
        assert source_message is not None
        source_message.received_at = datetime(2026, 10, 12, 16, tzinfo=UTC)
        next_day_lead = session.get(Lead, next_day_lead_id)
        assert next_day_lead is not None
        next_day_lead.smart_table_owner_user_id = "sales-2"
        next_day_lead.smart_table_record_id = next_day_record.record_id
        next_day_source = session.get(IncomingMessage, next_day_lead.source_message_id)
        assert next_day_source is not None
        # 10 月 13 日 UTC 晚间已是上海时间 10 月 14 日，不能混入 13 日候选。
        next_day_source.received_at = datetime(2026, 10, 13, 16, tzinfo=UTC)

    reply = prepare_batch_submission_selection(
        session_factory,
        adapter,
        MockCRMAdapter(),
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "cross-owner-card-command",
            target_owner_name="张三",
            target_capture_date=date(2026, 10, 13),
        ),
    )

    with session_factory() as session:
        action = session.scalar(select(WecomAction))
        preview = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "wecom_action_preview"
            )
        )
        card = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "wecom_action_card"
            )
        )
        assert action is not None and preview is not None and card is not None
        assert action.context["owner_scope_user_ids"] == ["sales-2"]
        assert action.context["target_capture_date"] == date(2026, 10, 13).toordinal()
        assert {item["lead_id"] for item in action.context["candidate_leads"]} == {
            lead_id,
            session.scalar(
                select(Lead.id).where(Lead.smart_table_record_id == orphan_record.record_id)
            ),
            same_day_existing_lead_id,
            session.scalar(
                select(Lead.id).where(
                    Lead.smart_table_record_id == manual_dated_record.record_id
                )
            ),
        }
        assert session.scalar(
            select(Lead.id).where(
                Lead.smart_table_record_id == next_day_orphan_record.record_id
            )
        ) is None
        assert session.scalar(
            select(Lead.smart_table_record_id).where(Lead.id == same_day_existing_lead_id)
        ) == same_day_existing_record.record_id
        assert session.scalar(
            select(Lead.smart_table_record_id).where(Lead.id == cross_day_duplicate_lead_id)
        ) is None
        assert session.scalar(
            select(Lead.id).where(
                Lead.smart_table_record_id == next_day_duplicate_record.record_id
            )
        ) is None
        markdown = preview.payload["markdown"]["content"]
        options = card.payload["template_card"]["checkbox"]["option_list"]

    assert "已发送候选卡" in reply
    assert "人工最终公司" in markdown
    assert "当日表格漏链公司" in markdown
    assert "当日已有草稿公司" in markdown
    assert "手工录入日期线索" in markdown
    assert "次日表格漏链公司" not in markdown
    assert "跨日重名公司" not in markdown
    assert "王工" not in markdown
    assert "13800000000" not in markdown
    assert {
        option["text"].split(". ", 1)[-1]
        for option in options
        if not option["text"].startswith("全选本页")
    } == {
        "人工最终公司",
        "当日表格漏链公司",
        "当日已有草稿公司",
        "手工录入日期线索",
    }


def test_manually_entered_smart_table_row_can_be_submitted_without_message_or_lead(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证人工录入且未关联后台 Lead 的表格行可进入同一 CRM 提交流程。"""
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
            "线索名称": "人工录入客户",
            "业务线": "协作机器人",
            "线索来源": "展会",
            "联系人": "王工",
            "职务": "经理",
            "沟通方式": "见面拜访",
            "手机": "13800000000",
            "客户行业": "其他",
            "备注": "人工录入的完整备注，包含明确需求并达到 CRM 最小提交长度。",
            "提交状态": "未提交",
        },
        actor=SmartTableActor.ROBOT,
        member_names={"负责人": "sales-1"},
    )
    crm = MockCRMAdapter()
    command = SubmissionCommand("提交我所有线索", "sales-1", "manual-row-submit")
    bulk_reads = Mock(wraps=adapter.get_records)
    monkeypatch.setattr(adapter, "get_records", bulk_reads)

    prepare_batch_submission_selection(session_factory, adapter, crm, command)
    assert bulk_reads.call_count == 1
    assert bulk_reads.call_args.kwargs["submission_status"] == "未提交"
    with session_factory() as session:
        action = session.scalar(select(WecomAction))
    assert action is not None
    snapshots = action.context["candidate_snapshots"]
    assert isinstance(snapshots, list) and len(snapshots) == 1
    assert snapshots[0]["record_id"] == record.record_id
    assert snapshots[0]["fields"]["线索名称"] == "人工录入客户"

    service = CrmSubmissionService(session_factory, adapter, crm)
    with session_factory() as session:
        mirrored_lead = session.scalar(
            select(Lead).where(Lead.smart_table_record_id == record.record_id)
        )
        assert mirrored_lead is not None
        assert mirrored_lead.source_message_id is None

    result = service.submit_selected(
        command,
        (mirrored_lead.id,),
        initial_records={record.record_id: record},
    )
    assert result.succeeded == 1
    assert crm.calls == 1
    assert bulk_reads.call_count == 1


def test_cross_owner_date_filter_uses_smart_table_shanghai_entry_day(
    session_factory: sessionmaker[Session],
) -> None:
    """验证日期提交按表格上海录入日过滤，并只返回匹配线索名称。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    table_day_lead_id = _lead(
        session_factory,
        adapter,
        message_id="table-day-cross-owner-lead",
        sequence=1,
        company_name="表格日期线索",
    )
    next_day_lead_id = _lead(
        session_factory,
        adapter,
        message_id="next-day-cross-owner-lead",
        sequence=2,
        company_name="次日表格线索",
    )
    records = adapter.get_records()
    adapter.update_record(records[0].record_id, {"负责人": "sales-2"})
    adapter.update_record(records[1].record_id, {"负责人": "sales-2"})
    table_dates = {
        records[0].record_id: "2026-10-13T15:59:59Z",
        records[1].record_id: "2026-10-13T16:00:00Z",
    }
    for record_id, table_date in table_dates.items():
        record = adapter.get_record(record_id)
        assert record is not None
        # 模拟只读系统时间字段由智能表格返回，数据库中的副本可能尚未同步。
        adapter._records[record_id] = replace(
            record, fields={**record.fields, "录入时间": table_date}
        )
    with session_factory.begin() as session:
        for lead_id in (table_day_lead_id, next_day_lead_id):
            lead = session.get(Lead, lead_id)
            assert lead is not None
            lead.smart_table_owner_user_id = "sales-2"
            # 两条消息均在 13 日上海时间收到，但处理/建行时间已跨到 14 日。
            lead.created_at = datetime(2026, 10, 13, 16, tzinfo=UTC)
            # 数据库副本刻意设为 14 日，验证筛选以最新表格快照为准。
            lead.field_values = {
                **lead.field_values,
                "录入时间": "2026-10-14T00:00:00+08:00",
            }
            source_message = session.get(IncomingMessage, lead.source_message_id)
            assert source_message is not None
            source_message.received_at = datetime(2026, 10, 12, 16, 30, tzinfo=UTC)
        session.add_all(
            [
                SalesAuthorization(wecom_user_id="sales-2", is_active=True),
                SalesAuthorization(
                    wecom_user_id="operator",
                    is_active=True,
                    is_administrator=False,
                    can_submit_other_sales_leads=True,
                ),
            ]
        )

    crm = MockCRMAdapter()
    original_get_records = adapter.get_records
    query_calls: list[dict[str, object]] = []

    def capture_get_records(
        record_ids: Collection[str] | None = None,
        *,
        owner_user_ids: Collection[str] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
        submission_status: str | None = None,
        capture_date: date | None = None,
    ) -> list[SmartTableRecord]:
        """记录日期候选查询参数并复用原适配器。

        参数：过滤参数与 SmartTableAdapter.get_records 一致。
        返回值：原适配器读取到的记录快照。
        异常：查询错误由原适配器传播。
        副作用：向 query_calls 追加一次只读查询条件。
        """
        query_calls.append(
            {
                "record_ids": record_ids,
                "owner_user_ids": owner_user_ids,
                "owner_display_names": owner_display_names,
                "submission_status": submission_status,
                "capture_date": capture_date,
            }
        )
        return original_get_records(
            record_ids,
            owner_user_ids=owner_user_ids,
            owner_display_names=owner_display_names,
            submission_status=submission_status,
            capture_date=capture_date,
        )

    adapter.get_records = capture_get_records
    service = CrmSubmissionService(session_factory, adapter, crm)
    candidates = service.list_submission_candidates(
        CROSS_OWNER_SUBMISSION_COMMAND,
        "operator",
        owner_scope_user_ids=("sales-2",),
        target_capture_date=date(2026, 10, 13),
    )

    assert {candidate.lead_id for candidate in candidates} == {table_day_lead_id}
    assert candidates[0].company_name == "表格日期线索"
    assert query_calls[0] == {
        "record_ids": None,
        "owner_user_ids": ("sales-2",),
        "owner_display_names": None,
        "submission_status": "未提交",
        "capture_date": date(2026, 10, 13),
    }

    # 卡片发行后若表格录入日跨到 14 日，回调必须拒绝该冻结候选。
    lead_record = adapter.get_record(records[0].record_id)
    assert lead_record is not None
    adapter._records[lead_record.record_id] = replace(
        lead_record,
        fields={**lead_record.fields, "录入时间": "2026-10-13T16:00:00Z"},
    )
    result = service.submit_selected(
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "table-day-cross-owner-request",
            owner_scope_user_ids=("sales-2",),
            target_capture_date=date(2026, 10, 13),
        ),
        (table_day_lead_id,),
    )
    assert result.not_submitted == 1
    assert crm.calls == 0


def test_cross_owner_duplicate_confirmation_rechecks_current_table_day(
    session_factory: sessionmaker[Session],
) -> None:
    """验证重复线索确认卡不能在表格日期变为次日后继续提交。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record = adapter.get_records()[0]
    adapter.update_record(record.record_id, {"负责人": "sales-2"})
    current_record = adapter.get_record(record.record_id)
    assert current_record is not None
    adapter._records[record.record_id] = replace(
        current_record,
        fields={**current_record.fields, "录入时间": "2026-10-13 23:59:59"},
    )
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-2"
        lead.field_values = {**lead.field_values, "录入时间": "2026-10-13 23:59:59"}
        source_message = session.get(IncomingMessage, lead.source_message_id)
        assert source_message is not None
        source_message.received_at = datetime(2026, 10, 12, 16, tzinfo=UTC)
        session.add_all(
            [
                SalesAuthorization(wecom_user_id="sales-2", crm_user_id="crm-2"),
                SalesAuthorization(
                    wecom_user_id="operator",
                    is_active=True,
                    can_submit_other_sales_leads=True,
                ),
                CrmSyncRecord(
                    lead_id=lead_id,
                    operation="create",
                    generation=1,
                    smart_table_record_id=record.record_id,
                    idempotency_key="dated-duplicate-confirmation-sync",
                    canonical_payload={"name": "人工最终公司"},
                    snapshot_hash="dated-duplicate-confirmation-hash",
                    request_message_id="dated-duplicate-confirmation",
                    submitting_sales_user_id="operator",
                    submitting_crm_user_id="crm-2",
                    status="awaiting_duplicate_confirmation",
                    crm_lead_id="crm-existing",
                ),
            ]
        )

    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    current_record = adapter.get_record(record.record_id)
    assert current_record is not None
    adapter._records[record.record_id] = replace(
        current_record,
        fields={**current_record.fields, "录入时间": "2026-10-14 00:00:00"},
    )
    confirmation = service.resolve_duplicate_confirmation(
        "dated-duplicate-confirmation",
        "operator",
        continue_submission=True,
        selected_lead_ids=(lead_id,),
        duplicate_lead_ids=(lead_id,),
        owner_scope_user_ids=("sales-2",),
        target_capture_date=date(2026, 10, 13),
    )

    assert confirmation.remaining == 1
    assert confirmation.submitted == 0
    assert crm.update_calls == 0
    assert crm.submit_calls == 0


def test_cross_owner_date_candidates_and_callback_recheck_current_date_scope(
    session_factory: sessionmaker[Session],
) -> None:
    """验证日期只从表格录入时间筛选，callback 复用命令阶段冻结快照。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter, message_id="dated-cross-owner-lead")
    record = adapter.get_records()[0]
    adapter.update_record(record.record_id, {"负责人": "sales-2"})
    owned_record = adapter.get_record(record.record_id)
    assert owned_record is not None
    adapter._records[record.record_id] = replace(
        owned_record,
        fields={**owned_record.fields, "录入时间": "2026-10-13T19:45:00+08:00"},
    )
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-2"
        # 来源消息时间优先：消息落在 10 月 13 日上海时间，Lead 创建时间已跨到 14 日。
        lead.created_at = datetime(2026, 10, 13, 16, tzinfo=UTC)
        source_message = session.get(IncomingMessage, lead.source_message_id)
        assert source_message is not None
        source_message.received_at = datetime(2026, 10, 12, 16, tzinfo=UTC)
        session.add_all(
            [
                SalesAuthorization(wecom_user_id="sales-2", is_active=True),
                SalesAuthorization(
                    wecom_user_id="operator",
                    is_active=True,
                    is_administrator=False,
                    can_submit_other_sales_leads=True,
                ),
            ]
        )

    # 历史表格行可能已补链成 Lead，但没有来源消息；应使用原“录入时间”，而非补链时间。
    historical_lead_id = _lead(
        session_factory,
        adapter,
        message_id="historical-backfill-message",
        sequence=2,
        company_name="历史补链公司",
    )
    historical_record = adapter.get_records()[1]
    adapter.update_record(historical_record.record_id, {"负责人": "sales-2"})
    historical_record = adapter.get_record(historical_record.record_id)
    assert historical_record is not None
    adapter._records[historical_record.record_id] = replace(
        historical_record,
        fields={**historical_record.fields, "录入时间": "2026-10-13T19:45:00+08:00"},
    )
    with session_factory.begin() as session:
        historical_lead = session.get(Lead, historical_lead_id)
        assert historical_lead is not None
        historical_lead.source_message_id = None
        historical_lead.smart_table_owner_user_id = "sales-2"
        historical_lead.created_at = datetime(2026, 10, 13, 16, tzinfo=UTC)

    submitted_lead_id = _lead(
        session_factory,
        adapter,
        message_id="already-submitted-cross-owner-lead",
        sequence=3,
        company_name="已提交历史公司",
    )
    submitted_record = adapter.get_records()[2]
    adapter.update_record(
        submitted_record.record_id,
        {"负责人": "sales-2", "提交状态": "已提交"},
    )
    with session_factory.begin() as session:
        submitted_lead = session.get(Lead, submitted_lead_id)
        assert submitted_lead is not None
        submitted_lead.smart_table_owner_user_id = "sales-2"
        source_message = session.get(IncomingMessage, submitted_lead.source_message_id)
        assert source_message is not None
        source_message.received_at = datetime(2026, 10, 12, 16, tzinfo=UTC)

    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    candidates = service.list_submission_candidates(
        CROSS_OWNER_SUBMISSION_COMMAND,
        "operator",
        owner_scope_user_ids=("sales-2",),
        target_capture_date=date(2026, 10, 13),
    )
    assert {candidate.lead_id for candidate in candidates} == {lead_id, historical_lead_id}
    frozen_records = {
        candidate.record.record_id: candidate.record
        for candidate in candidates
        if candidate.record is not None
    }

    # 候选卡发送后表格被改日；实际 callback 复用原快照，不重新读表。
    current_record = adapter.get_record(record.record_id)
    assert current_record is not None
    adapter._records[record.record_id] = replace(
        current_record,
        fields={**current_record.fields, "录入时间": "2026-10-14T00:15:00+08:00"},
    )
    with session_factory.begin() as session:
        source_message = session.get(IncomingMessage, "dated-cross-owner-lead")
        assert source_message is not None
        source_message.received_at = datetime(2026, 10, 13, 16, tzinfo=UTC)
    result = service.submit_selected(
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "dated-cross-owner-request",
            owner_scope_user_ids=("sales-2",),
            target_capture_date=date(2026, 10, 13),
        ),
        (lead_id,),
        initial_records=frozen_records,
    )
    assert result.succeeded == 1
    assert crm.search_calls == 0 and crm.calls == 1

    with session_factory.begin() as session:
        operator = session.get(SalesAuthorization, "operator")
        assert operator is not None
        operator.is_administrator = False
        operator.can_submit_other_sales_leads = True
        source_message = session.get(IncomingMessage, "dated-cross-owner-lead")
        assert source_message is not None
        source_message.received_at = datetime(2026, 10, 12, 16, tzinfo=UTC)
    authorized_result = service.submit_selected(
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "dated-cross-owner-request",
            owner_scope_user_ids=("sales-2",),
            target_capture_date=date(2026, 10, 13),
        ),
        (historical_lead_id,),
        initial_records=frozen_records,
    )
    assert authorized_result.succeeded == 1
    assert crm.calls == 2
    # 系统管理员标记本身不能代替专用跨员工提交授权。
    with session_factory.begin() as session:
        operator = session.get(SalesAuthorization, "operator")
        assert operator is not None
        operator.is_administrator = True
        operator.can_submit_other_sales_leads = False
    with pytest.raises(ValueError, match="跨员工 CRM 提交权限"):
        service.list_submission_candidates(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            owner_scope_user_ids=("sales-2",),
            target_capture_date=date(2026, 10, 13),
        )
    denied_admin_only = service.submit_selected(
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "dated-cross-owner-request",
            owner_scope_user_ids=("sales-2",),
            target_capture_date=date(2026, 10, 13),
        ),
        (historical_lead_id,),
        initial_records=frozen_records,
    )
    assert denied_admin_only.not_submitted == 1


def test_crm_submission_cli_creates_actor_for_verified_wecom_member(
    session_factory: sessionmaker[Session],
) -> None:
    """验证已由企微通讯录确认的成员无需预先登记或 CRM 映射即可获权。"""
    with session_factory.begin() as session:
        assert grant_crm_submission_access(
            session, WecomDirectoryMember("grant-target", "被授权员工")
        ) == "被授权员工"
    with session_factory() as session:
        authorization = session.get(SalesAuthorization, "grant-target")
        assert authorization is not None
        assert authorization.is_active is True
        assert authorization.display_name == "被授权员工"
        assert authorization.crm_user_id is None
        assert authorization.can_submit_other_sales_leads is True
        assert authorization.updated_by == "crm_cross_owner_submission_cli"


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
    original_get_records = adapter.get_records
    owner_scopes: list[tuple[str, ...] | None] = []

    def get_records_with_member_names(
        record_ids: Collection[str] | None = None,
        *,
        owner_user_ids: tuple[str, ...] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
    ) -> list[SmartTableRecord]:
        """为负责人范围内的预览记录补充企微成员显示名元数据。

        参数：record_ids 为可选记录范围；owner_user_ids 为服务端负责人筛选条件。
        返回值：保持记录字段并添加成员显示名的快照。
        异常：Mock 适配器读取失败时传播异常。
        副作用：记录负责人范围供断言，并调用内存适配器读取。
        """
        owner_scopes.append(owner_user_ids)
        records = original_get_records(
            record_ids,
            owner_user_ids=owner_user_ids,
            owner_display_names=owner_display_names,
        )
        return [
            SmartTableRecord(
                record_id=item.record_id,
                fields=item.fields,
                member_names={"负责人": "张华杰(JJ)", "创建人": "杨康鑫"},
            )
            for item in records
        ]

    monkeypatch.setattr(adapter, "get_records", get_records_with_member_names)
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
    assert owner_scopes == [("sales-1",)]
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
    "candidate_count, expected_pages",
    [(0, 0), (1, 1), (19, 1), (20, 2), (21, 2), (39, 3), (40, 3), (41, 3), (61, 4)],
)
def test_batch_submission_issues_server_frozen_pages(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    candidate_count: int,
    expected_pages: int,
) -> None:
    """验证全选入口占一个选项后，每页最多冻结 19 条线索。"""
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
    if candidate_count:
        ordered_actions = sorted(actions, key=lambda action: int(action.context["page"]))
        assert [len(action.context["candidate_leads"]) for action in ordered_actions] == [
            min(19, max(0, candidate_count - index * 19))
            for index in range(expected_pages)
        ]
        assert all(
            action.context["select_all_option_id"] == f"all-{action.target_id[:24]}"
            for action in actions
        )
    if expected_pages > 1:
        assert "张候选卡" in reply


def test_batch_submission_sends_names_only_markdown_before_selection_card(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证只含线索名称的 Markdown 先于选择卡发送，卡片使用短编号和服务端线索 ID。"""

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
    assert "**待提交线索（第 1/1 页）**" in markdown
    assert "冻结公司-0" in markdown and "冻结公司-1" in markdown
    assert "客户行业" not in markdown
    assert "提交状态" not in markdown
    assert "缺少：职务" not in markdown
    assert "AI待确认" not in markdown
    assert options[0]["text"] == "全选本页（2条）"
    assert [option["id"] for option in options[1:]] == [
        "batch-detail-lead-0",
        "batch-detail-lead-1",
    ]
    assert [option["text"] for option in options[1:]] == [
        "1. 冻结公司-0",
        "2. 冻结公司-1",
    ]


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
                BusinessAuditEvent.event_type == "smart_table_existing_leads_linked"
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
    adapter.create_record(
        {"负责人": "sales-2", "线索名称": "其他负责人的线索", "提交状态": "未提交"},
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


def test_cross_owner_duplicate_confirmation_rechecks_permission_and_frozen_scope(
    session_factory: sessionmaker[Session],
) -> None:
    """验证重复确认回调重新校验跨员工授权，并只停止冻结范围中的负责人线索。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first_lead_id = _lead(session_factory, adapter, message_id="owner-2-message", sequence=1)
    second_lead_id = _lead(
        session_factory,
        adapter,
        company_name="owner-3-company",
        message_id="owner-3-message",
        sequence=2,
    )
    records = adapter.get_records()
    record_ids = {record.fields["线索名称"]: record.record_id for record in records}
    adapter.update_record(record_ids["人工最终公司"], {"负责人": "owner-2"})
    adapter.update_record(record_ids["owner-3-company"], {"负责人": "owner-3"})
    with session_factory.begin() as session:
        session.get(Lead, first_lead_id).smart_table_owner_user_id = "owner-2"
        session.get(Lead, second_lead_id).smart_table_owner_user_id = "owner-3"
        session.add_all(
            [
                SalesAuthorization(wecom_user_id="owner-2", is_active=True),
                SalesAuthorization(wecom_user_id="owner-3", is_active=True),
                SalesAuthorization(
                    wecom_user_id="admin-1",
                    is_active=True,
                    is_administrator=False,
                    can_submit_other_sales_leads=False,
                ),
            ]
        )
        for lead_id in (first_lead_id, second_lead_id):
            lead = session.get(Lead, lead_id)
            assert lead is not None and lead.smart_table_record_id is not None
            session.add(
                CrmSyncRecord(
                    lead_id=lead_id,
                    operation="create",
                    generation=1,
                    smart_table_record_id=lead.smart_table_record_id,
                    idempotency_key=f"crm:create:{lead_id}",
                    canonical_payload={"name": "人工最终公司"},
                    snapshot_hash=f"duplicate-{lead_id}",
                    request_message_id="admin-duplicate-request",
                    submitting_sales_user_id="admin-1",
                    submitting_crm_user_id="crm-owner",
                    crm_lead_id=f"crm-{lead_id}",
                    status="awaiting_duplicate_confirmation",
                )
            )
    service = CrmSubmissionService(session_factory, adapter, MockCRMAdapter())

    with pytest.raises(PermissionError, match="跨员工 CRM 提交权限已失效"):
        service.resolve_duplicate_confirmation(
            "admin-duplicate-request",
            "admin-1",
            continue_submission=False,
            duplicate_lead_ids=(first_lead_id, second_lead_id),
            owner_scope_user_ids=("owner-2",),
        )

    with session_factory.begin() as session:
        admin = session.get(SalesAuthorization, "admin-1")
        assert admin is not None
        admin.can_submit_other_sales_leads = True

    stopped = service.resolve_duplicate_confirmation(
        "admin-duplicate-request",
        "admin-1",
        continue_submission=False,
        duplicate_lead_ids=(first_lead_id, second_lead_id),
        owner_scope_user_ids=("owner-2",),
    )

    assert stopped.abandoned == 1
    with session_factory() as session:
        first = session.scalar(
            select(CrmSyncRecord).where(CrmSyncRecord.lead_id == first_lead_id)
        )
        second = session.scalar(
            select(CrmSyncRecord).where(CrmSyncRecord.lead_id == second_lead_id)
        )
    assert first is not None and first.status == "abandoned"
    assert second is not None and second.status == "awaiting_duplicate_confirmation"


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
    assert second.succeeded == 1
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


def test_due_crm_retry_reuses_frozen_request_and_notifies_submitter(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 Beat 重试复用冻结请求，并通过现有企微通知 Outbox 反馈成功。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    original_submit = crm.submit_lead
    attempts: list[tuple[dict[str, object], str, str]] = []

    def fail_once(
        payload: Mapping[str, object], *, idempotency_key: str, crm_user_id: str
    ) -> CRMCreateResult:
        """首次模拟暂时性 CRM 网关错误，后续使用 Mock 成功结果。"""
        attempts.append((dict(payload), idempotency_key, crm_user_id))
        if len(attempts) == 1:
            raise SopCRMError("sanitized gateway error", category="gateway", http_status=503)
        return original_submit(payload, idempotency_key=idempotency_key, crm_user_id=crm_user_id)

    monkeypatch.setattr(crm, "submit_lead", fail_once)
    service = CrmSubmissionService(session_factory, adapter, crm)
    first_result = submit_today_via_selection(service, "sales-1", "crm-retry-message")
    assert first_result.retrying == 1
    with session_factory.begin() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        assert sync is not None and sync.next_retry_at is not None
        sync.next_retry_at = utc_now() - timedelta(seconds=1)

    assert service.retry_due_creates() == 1
    assert attempts[0] == attempts[1]
    with session_factory() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        notice = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.source_message_id == "crm-retry-message",
                NotificationRecord.notification_type == "crm_submission_retry_summary",
            )
        )
    assert sync is not None and sync.status == "succeeded" and sync.attempts == 2
    assert notice is not None and notice.status == "pending"
    assert notice.content is not None and "CRM 自动重试完成" in notice.content

    client = Mock()
    client.send_message = AsyncMock()
    sent = asyncio.run(WecomOutboundNotificationSender(session_factory, client).send_pending_once())
    assert sent == 1
    body = client.send_message.await_args.args[1]
    assert "CRM 自动重试完成" in body["markdown"]["content"]


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


def test_update_command_without_leads_is_noop_for_unregistered_actor(
    session_factory: sessionmaker[Session],
) -> None:
    """验证没有本人线索时，未登记成员的更新命令为空操作而非目录拒绝。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    crm = MockCRMAdapter()
    result = CrmSubmissionService(session_factory, adapter, crm).submit(
        SubmissionCommand("提交我的更新", "sales-1", "message-12")
    )

    assert result == SubmissionBatchResult()
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

    assert "CRM 新增/更新成功 1 条" in create_reply
    assert "手机号" not in create_reply and "payload" not in create_reply.lower()
    assert "CRM 新增/更新成功 1 条" in update_reply and "无变化 2 条" in update_reply
    assert "缺少必填字段：业务线、手机" in incomplete_reply


def test_update_item_results_are_operation_aware() -> None:
    """验证 update 成功、无变化、不完整和重试不复用 create 语义。"""
    from app.crm.service import _append_update_result

    succeeded = _append_update_result(SubmissionBatchResult(), "lead-updated", "succeeded")
    unchanged = _append_update_result(SubmissionBatchResult(), "lead-same", "unchanged")
    incomplete = _append_update_result(SubmissionBatchResult(), "lead-incomplete", "incomplete")
    retrying = _append_update_result(SubmissionBatchResult(), "lead-retry", "retrying")
    followed_up = _append_update_result(SubmissionBatchResult(), "lead-existing", "followed_up")

    assert succeeded.items[0].status is SubmissionItemStatus.UPDATED
    assert followed_up.items[0].status is SubmissionItemStatus.FOLLOWED_UP
    assert followed_up.items[0].reason_code == "crm_followed_up"
    assert followed_up.followed_up == 1
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
    assert (
        "提交完成 4｜新增/更新成功 4｜"
        "已存在并跟进 0｜待完善 1｜处理中 1｜未提交 0｜失败 0"
    ) in reply
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
    assert crm.search_calls == 0
    logged = caplog.text
    for sensitive in ("13800000000", "人工最终备注", "crm-1", "payload", "secret"):
        assert sensitive not in logged.lower()


def test_update_uses_current_table_values_once_and_skips_equal_snapshot(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证更新以表格回读快照调用一次 CRM，重放相同业务值不再调用。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "message-12").succeeded == 1
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000", "AI待确认": ["客户行业"]})
    bulk_read = Mock(wraps=adapter.get_records)
    monkeypatch.setattr(adapter, "get_records", bulk_read)

    first = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-13"))
    second = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-14"))

    assert first.updated == 1 and second.unchanged == 1
    assert crm.submit_calls == 2 and crm.submit_payloads[1]["mobile"] == "13900000000"
    assert bulk_read.call_count == 4
    assert [call.args[0] for call in bulk_read.call_args_list] == [
        {record_id},
        (record_id,),
        {record_id},
        (record_id,),
    ]
    with session_factory() as session:
        updates = session.scalars(
            select(CrmSyncRecord).where(CrmSyncRecord.operation == "update")
        ).all()
    assert len(updates) == 1 and updates[0].canonical_payload.get("AI待确认") is None


def test_update_refreshes_changed_batch_snapshot_before_crm_write(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 CRM 更新使用批量读取后最新的单条回读值。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "message-12").succeeded == 1
    record_id = adapter.get_records()[0].record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    original_get_records = adapter.get_records
    reads = 0

    def mutate_after_batch_read(
        record_ids=None, *, owner_user_ids=None, owner_display_names=None
    ):
        """在批量缓存返回后模拟销售编辑，逐条回读应看到新值。"""
        nonlocal reads
        records = original_get_records(
            record_ids,
            owner_user_ids=owner_user_ids,
            owner_display_names=owner_display_names,
        )
        reads += 1
        if reads == 1:
            adapter.update_record(record_id, {"手机": "13700000000"})
        return records

    monkeypatch.setattr(adapter, "get_records", mutate_after_batch_read)

    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "update-fresh"))

    assert result.updated == 1
    assert crm.submit_payloads[-1]["mobile"] == "13700000000"
    assert reads == 2


def test_update_refreshes_even_when_batch_snapshot_was_unchanged(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证批量读取后才发生的编辑也会在更新前被发现。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "late-edit-create").succeeded == 1
    record_id = adapter.get_records()[0].record_id
    original_get_records = adapter.get_records
    reads = 0

    def edit_after_unchanged_batch(
        record_ids=None, *, owner_user_ids=None, owner_display_names=None
    ):
        """让批量快照保持旧值，然后模拟销售在批量读取后编辑表格。"""
        nonlocal reads
        records = original_get_records(
            record_ids,
            owner_user_ids=owner_user_ids,
            owner_display_names=owner_display_names,
        )
        reads += 1
        if reads == 1:
            adapter.update_record(record_id, {"手机": "13700000000"})
        return records

    monkeypatch.setattr(adapter, "get_records", edit_after_unchanged_batch)

    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "late-edit-update"))

    assert result.updated == 1
    assert reads == 2
    assert crm.submit_payloads[-1]["mobile"] == "13700000000"


def test_selected_create_reuses_request_scoped_batch_snapshot(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证确认卡仅批量读取勾选记录一次，并在当前请求内复用该快照。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = adapter.get_records()[0].record_id
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    candidates = service.list_submission_candidates("提交今天的线索", "sales-1")
    adapter.update_record(record_id, {"手机": "13700000000"})
    reads: list[set[str] | None] = []
    original_get_records = adapter.get_records

    def capture_then_edit(record_ids=None, *, owner_user_ids=None, owner_display_names=None):
        """返回批量读取时的快照，再模拟提交过程中出现的新表格编辑。"""
        reads.append(record_ids)
        records = original_get_records(
            record_ids,
            owner_user_ids=owner_user_ids,
            owner_display_names=owner_display_names,
        )
        adapter.update_record(record_id, {"手机": "13900000000"})
        return records

    monkeypatch.setattr(adapter, "get_records", capture_then_edit)

    result = service.submit_selected(
        SubmissionCommand("提交今天的线索", "sales-1", "selected-fresh-snapshot"),
        tuple(candidate.lead_id for candidate in candidates),
    )

    assert tuple(item.lead_id for item in candidates) == (lead_id,)
    assert result.succeeded == 1
    assert reads == [{record_id}]
    assert crm.submit_payloads[-1]["mobile"] == "13700000000"


def test_cross_owner_create_rechecks_frozen_owner_scope_under_lead_lock(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证表格读取后转交到授权范围外的负责人时，锁内复核会拒绝首次提交。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    record_id = adapter.get_records()[0].record_id
    adapter.update_record(record_id, {"负责人": "sales-2"})
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-2"
        session.add(
            SalesAuthorization(wecom_user_id="operator", is_active=True,
                               can_submit_other_sales_leads=True)
        )
        session.add(SalesAuthorization(wecom_user_id="sales-2", display_name="张三"))

    directory = Mock()
    directory.resolve.return_value = "crm-sales-2"
    crm = MockCRMAdapter()
    service = CrmSubmissionService(
        session_factory, adapter, crm, employee_directory=directory
    )
    candidates = service.list_submission_candidates(
        CROSS_OWNER_SUBMISSION_COMMAND,
        "operator",
        owner_scope_user_ids=("sales-2",),
    )
    original_hash = service._snapshot_hash

    def transfer_out_of_scope(payload: dict[str, object]) -> str:
        """在线索快照构造后、Lead 行锁获取前模拟负责人转交。"""
        snapshot_hash = original_hash(payload)
        adapter.update_record(record_id, {"负责人": "sales-3"})
        with session_factory.begin() as session:
            lead = session.get(Lead, lead_id)
            assert lead is not None
            lead.smart_table_owner_user_id = "sales-3"
        return snapshot_hash

    monkeypatch.setattr(service, "_snapshot_hash", transfer_out_of_scope)

    result = service.submit_selected(
        SubmissionCommand(
            CROSS_OWNER_SUBMISSION_COMMAND,
            "operator",
            "cross-owner-transfer-race",
            owner_scope_user_ids=("sales-2",),
        ),
        tuple(candidate.lead_id for candidate in candidates),
    )

    assert result.incomplete == 1
    assert crm.calls == 0
    with session_factory() as session:
        assert session.scalar(
            select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id)
        ) is None


def test_update_uses_table_snapshot_after_backend_owner_changes(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证后台 owner 在批量读取后变化时，更新仍以表格负责人快照为准。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "owner-race-create").succeeded == 1
    record_id = adapter.get_records()[0].record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    original_get_records = adapter.get_records
    reads = 0

    def transfer_after_batch(record_ids=None, *, owner_user_ids=None, owner_display_names=None):
        """在命令阶段批量读取后修改后台 owner，证明它不控制 CRM 候选。"""
        nonlocal reads
        records = original_get_records(
            record_ids,
            owner_user_ids=owner_user_ids,
            owner_display_names=owner_display_names,
        )
        reads += 1
        if reads == 1:
            with session_factory.begin() as session:
                lead = session.get(Lead, lead_id)
                assert lead is not None
                lead.smart_table_owner_user_id = "sales-2"
        return records

    monkeypatch.setattr(adapter, "get_records", transfer_after_batch)

    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "owner-race-update"))

    assert result.updated == 1
    assert crm.submit_calls == 2
    assert reads == 1
    with session_factory() as session:
        updates = session.scalars(
            select(CrmSyncRecord).where(
                CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "update"
            )
        ).all()
    assert len(updates) == 1 and updates[0].status == "succeeded"


def test_update_batch_read_failure_does_not_fall_back_to_per_record_reads(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证批量回读失败时不逐条重试，并为全部目标返回可重试结果。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_ids = [
        _lead(session_factory, adapter),
        _lead(
            session_factory,
            adapter,
            company_name="第二家测试公司",
            message_id="message-13",
            sequence=2,
        ),
    ]
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    created = submit_today_via_selection(service, "sales-1", "update-partial-create")
    assert created.succeeded == 2
    records = adapter.get_records()
    for record in records:
        adapter.update_record(record.record_id, {"手机": "13900000000"})
    original_get_records = adapter.get_records
    individual_reads = Mock(wraps=adapter.get_record)
    monkeypatch.setattr(adapter, "get_record", individual_reads)
    batch_reads = Mock(wraps=original_get_records)

    def get_records_with_item_failure(
        record_ids=None, *, owner_user_ids=None, owner_display_names=None
    ):
        """让整批读取失败，验证业务层不再对每条记录逐个补读。"""
        requested = set(record_ids or ())
        if owner_user_ids is not None or len(requested) > 1:
            batch_reads(
                record_ids,
                owner_user_ids=owner_user_ids,
                owner_display_names=owner_display_names,
            )
            raise RuntimeError("batch read unavailable")
        return original_get_records(
            record_ids,
            owner_user_ids=owner_user_ids,
            owner_display_names=owner_display_names,
        )

    monkeypatch.setattr(adapter, "get_records", get_records_with_item_failure)

    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "update-partial"))

    assert result.updated == 0
    assert result.failed_pending_review == 2
    assert crm.submit_calls == 2
    assert batch_reads.call_count == 1
    assert individual_reads.call_count == 0
    assert {item.lead_id for item in result.items} == set(lead_ids)
    assert {item.status for item in result.items} == {
        SubmissionItemStatus.FAILED_PENDING_REVIEW
    }
    assert all(item.reason_code == "wecom_smart_table_retryable" for item in result.items)


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
    assert crm.submit_calls == 1
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
    assert crm.submit_calls == 2


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
    assert crm.search_calls == 0


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

    assert crm.search_calls == 0
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


def test_unknown_update_never_reads_table_or_retries_after_edit(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证覆盖超时后沿用原冻结载荷和幂等键重试，且不读取销售后续编辑。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead(session_factory, adapter)
    crm = MockCRMAdapter()
    service = CrmSubmissionService(session_factory, adapter, crm)
    assert submit_today_via_selection(service, "sales-1", "message-12").succeeded == 1
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"手机": "13900000000"})
    bulk_read = Mock(wraps=adapter.get_records)
    monkeypatch.setattr(adapter, "get_records", bulk_read)
    calls = 0
    requests: list[tuple[dict[str, object], str]] = []

    def timeout_once(
        payload: Mapping[str, object], *, idempotency_key: str, crm_user_id: str
    ) -> object:
        """模拟远端更新后响应丢失；返回无，抛出超时并计数，不连接 CRM。"""
        nonlocal calls
        calls += 1
        requests.append((dict(payload), idempotency_key))
        del crm_user_id
        raise TimeoutError()

    monkeypatch.setattr(crm, "submit_lead", timeout_once)
    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-13"))
    assert result.failed_pending_review == 1
    # 批量快照筛出待提交记录后，真正更新前还会按记录 ID 刷新一次。
    assert bulk_read.call_count == 2
    adapter.update_record(record_id, {"手机": "13700000000"})
    monkeypatch.setattr(adapter, "get_record", lambda _: pytest.fail("不得回读 Smart Table"))
    monkeypatch.setattr(adapter, "get_records", lambda *_: pytest.fail("不得批量回读 Smart Table"))
    result = service.submit(SubmissionCommand("提交我的更新", "sales-1", "message-14"))
    assert result.failed_pending_review == 1 and calls == 2
    assert requests[0] == requests[1]
    assert requests[0][0]["mobile"] == "13900000000"
    with session_factory() as session:
        sync = session.scalar(
            select(CrmSyncRecord).where(
                CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "update"
            )
        )
        assert sync is not None and sync.attempts == 2
        assert sync.canonical_payload["mobile"] == "13900000000"
        assert sync.failure_category == "unknown"


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


def test_entry_time_maps_to_crm_without_expanding_submission_preview() -> None:
    """录入时间按 CRM 契约提交，同时不扩展提交确认十字段。"""
    from app.crm.payload import CrmPayloadBuilder
    from app.wecom_bot.actions import SUBMISSION_PREVIEW_FIELDS

    builder = CrmPayloadBuilder()
    fields = {"线索名称": "元数据测试公司", "业务线": "协作机器人"}
    payload = builder.build({**fields, "录入时间": "2026/10/09 10:20:30"})
    assert payload["entryTime"] == "2026-10-09 10:20"
    assert "录入时间" not in SUBMISSION_PREVIEW_FIELDS
    assert len(SUBMISSION_PREVIEW_FIELDS) == 10
