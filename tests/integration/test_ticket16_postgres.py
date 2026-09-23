"""T16 PostgreSQL 并发竞态与远端不可逆事实集成测试。"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import get_settings
from app.crm.mock import MockCRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionCommand
from app.leads.discard import LeadDiscardService
from app.leads.models import Lead, SmartTableOwnerTransferOperation
from app.leads.service import FirstTextLeadWorkspaceService
from app.leads.transfer import SmartTableOwnerTransferService
from app.messaging.models import Base, IncomingMessage, OutboxEvent, SalesAuthorization
from app.smart_table.models import SmartTableRecord
from app.smart_table.permissions import (
    SmartTablePermissionVerification,
    SmartTablePermissionVerificationUnavailable,
)


class BlockingTransferAdapter:
    """在远端负责人写入边界提供 Event 栅栏，不使用 sleep 排序竞态。"""

    def __init__(self) -> None:
        """初始化一次阻塞和调用记录。"""

        self.entered = Event()
        self.release = Event()
        self._lock = Lock()
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.records: dict[str, dict[str, object]] = {}

    def get_record(self, record_id: str) -> SmartTableRecord:
        """返回可供 retry/CRM 提交重读的最小表格快照。"""

        with self._lock:
            fields = self.records.setdefault(
                record_id,
                {
                    "线索名称": "并发公司",
                    "业务线": "协作机器人",
                    "线索来源": "展会",
                    "联系人": "赵六",
                    "职务": "采购经理",
                    "沟通方式": "微信",
                    "手机": "13800000000",
                    "备注": "客户已确认自动化需求，预算和现场沟通安排待进一步确认。",
                    "负责人": "sales-old",
                },
            )
            return SmartTableRecord(record_id=record_id, fields=dict(fields))

    def update_record(self, record_id: str, fields: dict[str, object]) -> SmartTableRecord:
        """阻塞首个远端写入，随后返回记录快照。"""

        with self._lock:
            self.calls.append((record_id, dict(fields)))
            first = len(self.calls) == 1
        if first:
            self.entered.set()
            assert self.release.wait(timeout=5)
        with self._lock:
            current = self.records.setdefault(record_id, {})
            current.update(fields)
            return SmartTableRecord(record_id=record_id, fields=dict(current))


class VerifiedPermissions:
    """返回满足 T16 隔离要求的权限结论。"""

    def verify_owner_transfer(
        self, record_id: str, old_owner_user_id: str, new_owner_user_id: str
    ) -> SmartTablePermissionVerification:
        """确认目标可见可编辑且旧负责人不可见。"""

        del record_id, old_owner_user_id, new_owner_user_id
        return SmartTablePermissionVerification(True, True, False, False)


@pytest.fixture
def postgres_session_factory() -> Generator[sessionmaker[Session], None, None]:
    """创建随机 PostgreSQL schema，确保并发测试使用真实行锁。"""

    engine = create_engine(get_settings().database_url)
    schema_name = f"t16_{uuid4().hex}"
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        engine.dispose()
        pytest.skip("需要 Docker Compose PostgreSQL 执行 T16 并发测试")
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema_name))
    schema_engine = engine.execution_options(schema_translate_map={None: schema_name})
    Base.metadata.create_all(schema_engine)
    try:
        yield sessionmaker(schema_engine)
    finally:
        schema_engine.dispose()
        with engine.begin() as connection:
            connection.execute(DropSchema(schema_name, cascade=True))
        engine.dispose()


def _seed(session_factory: sessionmaker[Session], suffix: str) -> str:
    """写入管理员、两个目标销售、来源消息和待创建线索。"""

    with session_factory.begin() as session:
        session.add_all(
            [
                SalesAuthorization(
                    wecom_user_id="admin-1",
                    is_authorized=True,
                    is_active=True,
                    is_administrator=True,
                ),
                SalesAuthorization(
                    wecom_user_id="admin-2",
                    is_authorized=True,
                    is_active=True,
                    is_administrator=True,
                ),
                SalesAuthorization(
                    wecom_user_id="sales-old",
                    is_authorized=True,
                    is_active=True,
                    crm_user_id="crm-old",
                ),
                SalesAuthorization(
                    wecom_user_id="sales-a",
                    is_authorized=True,
                    is_active=True,
                ),
                SalesAuthorization(
                    wecom_user_id="sales-b",
                    is_authorized=True,
                    is_active=True,
                ),
            ]
        )
        session.flush()
        session.add(
            IncomingMessage(
                message_id=f"message-{suffix}",
                sales_user_id="sales-old",
                sequence=1,
                raw_payload={"text": "客户"},
                normalized_text="客户",
            )
        )
        session.flush()
        lead = Lead(
            source_message_id=f"message-{suffix}",
            original_capturing_sales_user_id="sales-old",
            smart_table_owner_user_id="sales-old",
            smart_table_record_id=f"record-{suffix}",
            lifecycle_state="pending_create",
            field_values={
                "线索名称": f"并发公司-{suffix}",
                "业务线": "协作机器人",
                "线索来源": "展会",
                "联系人": "赵六",
                "职务": "采购经理",
                "沟通方式": "微信",
                "手机": "13800000000",
                "备注": "客户已确认自动化需求，预算和现场沟通安排待进一步确认。",
            },
            standard_company_name=f"并发公司-{suffix}",
        )
        session.add(lead)
        session.flush()
        return lead.id


def _transfer(
    session_factory: sessionmaker[Session],
    adapter: BlockingTransferAdapter,
    lead_id: str,
    admin: str,
    target: str,
    request_id: str,
) -> object:
    """在独立线程中执行一次管理员转交。"""

    return SmartTableOwnerTransferService(session_factory, adapter, VerifiedPermissions()).transfer(
        lead_id=lead_id,
        new_owner_user_id=target,
        operator_subject=admin,
        operator_role="administrator",
        auth_source="postgres-test",
        request_id=request_id,
        reason="T16 并发验证",
    )


def _prepare_failed_retry(
    session_factory: sessionmaker[Session], suffix: str
) -> None:
    """把来源消息置为 T14 可重试终态，供 transfer-vs-retry 竞态使用。"""

    with session_factory.begin() as session:
        message_id = f"message-{suffix}"
        message = session.get(IncomingMessage, message_id)
        assert message is not None
        message.normalized_text = "联系人：李四"
        session.add(
            OutboxEvent(
                message_id=message_id,
                sales_user_id="sales-old",
                sequence=1,
                status="failed_pending_review",
            )
        )


class BlockingCRM(MockCRMAdapter):
    """在 CRM create 边界阻塞，和负责人转交形成确定性竞态。"""

    def __init__(self) -> None:
        """初始化 CRM 外部调用栅栏。"""

        super().__init__()
        self.entered = Event()
        self.release = Event()

    def create_lead(self, payload: dict[str, object], **kwargs: object):
        """阻塞 CRM create，释放后复用 MockCRMAdapter 的确定性结果。"""

        self.entered.set()
        assert self.release.wait(timeout=5)
        return super().create_lead(payload, **kwargs)


class UnavailablePermissions:
    """模拟尚未具备按销售身份读取记录权限的真实部署。"""

    def verify_owner_transfer(
        self, record_id: str, old_owner_user_id: str, new_owner_user_id: str
    ) -> SmartTablePermissionVerification:
        """报告权限验证契约缺失。"""

        del record_id, old_owner_user_id, new_owner_user_id
        raise SmartTablePermissionVerificationUnavailable("权限验证契约缺失")


def test_two_admins_transfer_same_lead_have_one_remote_operation(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 行锁确保同一 Lead 的两个管理员不会并行发起转交。"""

    lead_id = _seed(postgres_session_factory, "two-admins")
    adapter = BlockingTransferAdapter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            _transfer,
            postgres_session_factory,
            adapter,
            lead_id,
            "admin-1",
            "sales-a",
            "transfer-a",
        )
        assert adapter.entered.wait(timeout=5)
        second = executor.submit(
            _transfer,
            postgres_session_factory,
            adapter,
            lead_id,
            "admin-2",
            "sales-b",
            "transfer-b",
        )
        with pytest.raises(ValueError, match="进行中"):
            second.result(timeout=5)
        adapter.release.set()
        result = first.result(timeout=5)
    assert getattr(result, "status") == "succeeded"
    assert len(adapter.calls) == 1
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None and lead.smart_table_owner_user_id == "sales-a"
        assert session.scalar(select(SmartTableOwnerTransferOperation).where(
            SmartTableOwnerTransferOperation.lead_id == lead_id
        )) is not None


def test_transfer_vs_discard_keeps_remote_fact_pending_recovery(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """转交远端成功后若 discard 先提交，本地不伪装成功且不反向转交。"""

    lead_id = _seed(postgres_session_factory, "discard-race")
    adapter = BlockingTransferAdapter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        transfer = executor.submit(
            _transfer,
            postgres_session_factory,
            adapter,
            lead_id,
            "admin-1",
            "sales-a",
            "transfer-discard",
        )
        assert adapter.entered.wait(timeout=5)
        discard = executor.submit(
            LeadDiscardService(postgres_session_factory).discard,
            lead_id,
            "admin-2",
            "废弃并发测试",
        )
        assert discard.result(timeout=5).status.value == "discarded"
        adapter.release.set()
        result = transfer.result(timeout=5)
    assert getattr(result, "status") == "pending_recovery"
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        operation = session.scalar(select(SmartTableOwnerTransferOperation))
        assert lead is not None and lead.lifecycle_state == "discarded"
        assert lead.smart_table_owner_user_id == "sales-old"
        assert operation is not None and operation.final_status == "pending_recovery"


def test_transfer_vs_retry_preserves_owner_and_retry_patch(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 下转交与 T14 retry 并行时分别收敛 owner 与字段补丁。"""

    lead_id = _seed(postgres_session_factory, "retry-race")
    _prepare_failed_retry(postgres_session_factory, "retry-race")
    adapter = BlockingTransferAdapter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        transfer = executor.submit(
            _transfer,
            postgres_session_factory,
            adapter,
            lead_id,
            "admin-1",
            "sales-a",
            "transfer-retry",
        )
        assert adapter.entered.wait(timeout=5)
        retry = executor.submit(
            FirstTextLeadWorkspaceService(postgres_session_factory, adapter).retry_failed_message,
            "message-retry-race",
            "admin-1",
            segment_index=0,
            request_id="retry-race-request",
        )
        adapter.release.set()
        transfer_result = transfer.result(timeout=5)
        retry_result = retry.result(timeout=5)
    assert getattr(transfer_result, "status") == "succeeded"
    assert retry_result.status.value == "succeeded"
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        assert lead.smart_table_owner_user_id == "sales-a"
        assert lead.field_values.get("联系人") == "李四"


def test_transfer_vs_crm_submission_preserves_submitter_and_owner_boundary(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 下 CRM 提交与转交并行时不改历史提交销售或 CRM owner。"""

    lead_id = _seed(postgres_session_factory, "crm-race")
    adapter = BlockingTransferAdapter()
    crm = BlockingCRM()
    with ThreadPoolExecutor(max_workers=2) as executor:
        transfer = executor.submit(
            _transfer,
            postgres_session_factory,
            adapter,
            lead_id,
            "admin-1",
            "sales-a",
            "transfer-crm",
        )
        assert adapter.entered.wait(timeout=5)
        submission = executor.submit(
            CrmSubmissionService(
                postgres_session_factory,
                adapter,
                crm,
                crm_create_retry_count=1,
            ).submit,
            SubmissionCommand("提交今天的线索", "sales-old", "submit-crm-race"),
        )
        assert crm.entered.wait(timeout=5)
        adapter.release.set()
        crm.release.set()
        transfer_result = transfer.result(timeout=5)
        submission_result = submission.result(timeout=5)
    assert getattr(transfer_result, "status") == "succeeded"
    assert submission_result.succeeded == 1
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None and lead.smart_table_owner_user_id == "sales-a"


def test_duplicate_transfer_request_is_idempotent_in_postgres(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 唯一 request_id 保证重复转交不重复调用远端。"""

    lead_id = _seed(postgres_session_factory, "duplicate-transfer")
    adapter = BlockingTransferAdapter()
    adapter.release.set()
    first = _transfer(
        postgres_session_factory,
        adapter,
        lead_id,
        "admin-1",
        "sales-a",
        "duplicate-request",
    )
    second = _transfer(
        postgres_session_factory,
        adapter,
        lead_id,
        "admin-2",
        "sales-b",
        "duplicate-request",
    )
    assert getattr(first, "status") == "succeeded"
    assert getattr(second, "status") == "succeeded"
    assert len(adapter.calls) == 1


def test_remote_success_local_finalize_failure_is_pending_recovery(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 持久化远端成功后本地异常，禁止反向写回并保留待恢复。"""

    lead_id = _seed(postgres_session_factory, "local-failure")
    adapter = BlockingTransferAdapter()
    adapter.release.set()
    service = SmartTableOwnerTransferService(
        postgres_session_factory, adapter, VerifiedPermissions()
    )

    def fail_local_finalize(
        operation_id: str, target_lead_id: str, old_owner: str, new_owner: str
    ) -> None:
        """模拟本地事务提交阶段发生故障。"""

        del operation_id, target_lead_id, old_owner, new_owner
        raise RuntimeError("simulated local commit failure")

    service._finalize_local_transfer = fail_local_finalize  # type: ignore[method-assign]
    result = service.transfer(
        lead_id=lead_id,
        new_owner_user_id="sales-a",
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="postgres-test",
        request_id="local-failure-request",
        reason="本地提交失败恢复测试",
    )
    assert result.status == "pending_recovery"
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        operation = session.scalar(select(SmartTableOwnerTransferOperation))
        assert lead is not None and lead.smart_table_owner_user_id == "sales-old"
        assert operation is not None
        assert operation.remote_update_state == "succeeded"
        assert operation.permission_verification_state == "verified"
        assert operation.final_status == "pending_recovery"


def test_permission_verification_failure_is_pending_recovery_in_postgres(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 记录权限验证失败，不把远端负责人写入误报为完成。"""

    lead_id = _seed(postgres_session_factory, "permission-failure")
    adapter = BlockingTransferAdapter()
    adapter.release.set()
    result = SmartTableOwnerTransferService(
        postgres_session_factory, adapter, UnavailablePermissions()
    ).transfer(
        lead_id=lead_id,
        new_owner_user_id="sales-a",
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="postgres-test",
        request_id="permission-failure-request",
        reason="权限验证失败测试",
    )
    assert result.status == "permission_verification_unavailable"
    with postgres_session_factory() as session:
        operation = session.scalar(select(SmartTableOwnerTransferOperation))
        assert operation is not None
        assert operation.remote_update_state == "succeeded"
        assert operation.final_status == "pending_recovery"


def test_target_same_company_conflict_is_rejected_in_postgres(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 下目标销售同公司冲突在远端调用前被拒绝。"""

    lead_id = _seed(postgres_session_factory, "same-company")
    with postgres_session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-same-company-duplicate",
                sales_user_id="sales-a",
                sequence=2,
                raw_payload={},
            )
        )
        session.flush()
        session.add(
            Lead(
                id="lead-same-company-duplicate",
                source_message_id="message-same-company-duplicate",
                original_capturing_sales_user_id="sales-a",
                smart_table_owner_user_id="sales-a",
                lifecycle_state="pending_create",
                field_values={"线索名称": "并发公司-same-company"},
                standard_company_name="并发公司-same-company",
            )
        )
    adapter = BlockingTransferAdapter()
    adapter.release.set()
    with pytest.raises(ValueError, match="同公司"):
        _transfer(
            postgres_session_factory,
            adapter,
            lead_id,
            "admin-1",
            "sales-a",
            "same-company-request",
        )
    assert adapter.calls == []
