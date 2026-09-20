"""T16 Smart Table Owner 转交领域服务的行为测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.leads.models import (
    CrmCompanyIdentity,
    CrmSyncRecord,
    Lead,
    SalesLeadContext,
    SmartTableOwnerTransferOperation,
)
from app.leads.transfer import SmartTableOwnerTransferService
from app.messaging.models import Base, IncomingMessage, SalesAuthorization, utc_now
from app.smart_table.models import SmartTableRecord
from app.smart_table.permissions import (
    SmartTablePermissionVerification,
    SmartTablePermissionVerificationUnavailable,
)


class FakeSmartTableAdapter:
    """只模拟表格边界，记录写入的字段补丁以便验证增量语义。"""

    def __init__(self) -> None:
        """初始化一条带旧负责人的表格记录。"""
        self.record = SmartTableRecord(
            record_id="record-1",
            fields={"负责人": "sales-old", "创建人": "sales-old", "线索名称": "星海科技"},
        )
        self.update_calls: list[tuple[str, dict[str, object]]] = []

    def update_record(self, record_id: str, fields: dict[str, object]) -> SmartTableRecord:
        """应用一次字段补丁并返回更新后的快照。"""
        assert record_id == self.record.record_id
        self.update_calls.append((record_id, dict(fields)))
        self.record = SmartTableRecord(
            record_id=record_id,
            fields={**self.record.fields, **fields},
        )
        return self.record


class FakePermissionVerifier:
    """返回可测试的双销售记录级权限结论。"""

    def verify_owner_transfer(
        self, record_id: str, old_owner_user_id: str, new_owner_user_id: str
    ) -> SmartTablePermissionVerification:
        """确认目标可见可编辑且原负责人不可见。"""
        assert (record_id, old_owner_user_id, new_owner_user_id) == (
            "record-1",
            "sales-old",
            "sales-new",
        )
        return SmartTablePermissionVerification(
            new_owner_can_view=True,
            new_owner_can_edit=True,
            old_owner_can_view=False,
            old_owner_can_edit=False,
        )


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供包含 T16 新模型的隔离数据库。"""
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


@pytest.fixture
def seeded_lead(session_factory: sessionmaker[Session]) -> str:
    """写入旧负责人、新负责人、来源、CRM 和当前上下文事实。"""
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
                    wecom_user_id="sales-old",
                    is_authorized=True,
                    is_active=True,
                ),
                SalesAuthorization(
                    wecom_user_id="sales-new",
                    is_authorized=True,
                    is_active=True,
                    crm_user_id=None,
                ),
                IncomingMessage(
                    message_id="message-1",
                    sales_user_id="sales-old",
                    sequence=1,
                    raw_payload={"text": "客户"},
                    normalized_text="客户",
                ),
            ]
        )
        lead = Lead(
            id="lead-1",
            source_message_id="message-1",
            original_capturing_sales_user_id="sales-old",
            smart_table_owner_user_id="sales-old",
            smart_table_record_id="record-1",
            lifecycle_state="pending_create",
            field_values={"线索名称": "星海科技", "业务线": "协作机器人"},
            standard_company_name="星海科技",
        )
        session.add(lead)
        session.add(
            SalesLeadContext(
                sales_user_id="sales-old",
                lead_id="lead-1",
                last_message_received_at=utc_now(),
            )
        )
        session.add(
            CrmCompanyIdentity(
                standard_company_name="星海科技",
                crm_lead_id="crm-lead-1",
                crm_lead_owner_user_id="crm-owner-old",
                state="active",
                creating_lead_id="lead-1",
            )
        )
        session.add(
            CrmSyncRecord(
                lead_id="lead-1",
                operation="create",
                smart_table_record_id="record-1",
                idempotency_key="crm:create:lead-1",
                canonical_payload={"线索名称": "星海科技"},
                snapshot_hash="a" * 64,
                request_message_id="message-1",
                submitting_sales_user_id="sales-old",
                submitting_crm_user_id="crm-old",
                crm_lead_id="crm-lead-1",
                crm_lead_owner_user_id="crm-owner-old",
                status="succeeded",
            )
        )
        return lead.id


def test_transfer_changes_only_smart_table_owner_and_clears_old_context(
    session_factory: sessionmaker[Session], seeded_lead: str
) -> None:
    """验证成功转交不改变三种历史身份、业务字段或 CRM owner。"""
    adapter = FakeSmartTableAdapter()
    result = SmartTableOwnerTransferService(
        session_factory, adapter, FakePermissionVerifier()
    ).transfer(
        lead_id=seeded_lead,
        new_owner_user_id="sales-new",
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="test",
        request_id="request-transfer-1",
        reason="管理员调整维护人",
    )

    assert result.status == "succeeded"
    assert adapter.update_calls == [("record-1", {"负责人": "sales-new"})]
    repeated = SmartTableOwnerTransferService(
        session_factory, adapter, FakePermissionVerifier()
    ).transfer(
        lead_id=seeded_lead,
        new_owner_user_id="sales-new",
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="test",
        request_id="request-transfer-1",
        reason="幂等重试",
    )
    assert repeated.status == "succeeded"
    assert adapter.update_calls == [("record-1", {"负责人": "sales-new"})]
    with session_factory() as session:
        lead = session.get(Lead, seeded_lead)
        crm_sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == seeded_lead))
        identity = session.get(CrmCompanyIdentity, "星海科技")
        assert lead is not None
        assert lead.smart_table_owner_user_id == "sales-new"
        assert lead.original_capturing_sales_user_id == "sales-old"
        assert lead.field_values == {"线索名称": "星海科技", "业务线": "协作机器人"}
        assert session.get(SalesLeadContext, "sales-old") is None
        assert crm_sync is not None
        assert crm_sync.submitting_sales_user_id == "sales-old"
        assert crm_sync.crm_lead_owner_user_id == "crm-owner-old"
        assert identity is not None and identity.crm_lead_owner_user_id == "crm-owner-old"
        operation = session.get(SmartTableOwnerTransferOperation, result.operation_id)
        assert operation is not None
        assert operation.final_status == "succeeded"
        assert operation.original_capturing_sales_user_id == "sales-old"
        assert operation.crm_lead_owner_user_id == "crm-owner-old"


def test_transfer_requires_permission_verification_before_success(
    session_factory: sessionmaker[Session], seeded_lead: str
) -> None:
    """验证仅负责人字段回写但权限验证失败时不能标记成功或改变本地 owner。"""

    class FailingVerifier(FakePermissionVerifier):
        """返回不满足原负责人隔离要求的权限结论。"""

        def verify_owner_transfer(
            self, record_id: str, old_owner_user_id: str, new_owner_user_id: str
        ) -> SmartTablePermissionVerification:
            """模拟新负责人可见但旧负责人仍可见的失败结果。"""
            del record_id, old_owner_user_id, new_owner_user_id
            return SmartTablePermissionVerification(
                new_owner_can_view=True,
                new_owner_can_edit=True,
                old_owner_can_view=True,
                old_owner_can_edit=False,
            )

    adapter = FakeSmartTableAdapter()
    result = SmartTableOwnerTransferService(
        session_factory, adapter, FailingVerifier()
    ).transfer(
        lead_id=seeded_lead,
        new_owner_user_id="sales-new",
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="test",
        request_id="request-transfer-permission-fail",
        reason="权限验证回归",
    )

    assert result.status == "permission_verification_failed"
    with session_factory() as session:
        lead = session.get(Lead, seeded_lead)
        operation = session.get(SmartTableOwnerTransferOperation, result.operation_id)
        assert lead is not None and lead.smart_table_owner_user_id == "sales-old"
        assert operation is not None
        assert operation.remote_update_state == "succeeded"
        assert operation.permission_verification_state == "failed"
        assert operation.final_status == "pending_recovery"


def test_transfer_permission_provider_unavailable_is_recoverable(
    session_factory: sessionmaker[Session], seeded_lead: str
) -> None:
    """验证真实权限契约缺失时保留远端成功事实并进入待恢复，而非伪装完成。"""

    class UnavailableVerifier(FakePermissionVerifier):
        """模拟尚未提供按销售身份验证接口的部署。"""

        def verify_owner_transfer(
            self, record_id: str, old_owner_user_id: str, new_owner_user_id: str
        ) -> SmartTablePermissionVerification:
            """报告外部权限验证契约缺失。"""
            del record_id, old_owner_user_id, new_owner_user_id
            raise SmartTablePermissionVerificationUnavailable("权限验证契约缺失")

    adapter = FakeSmartTableAdapter()
    result = SmartTableOwnerTransferService(
        session_factory, adapter, UnavailableVerifier()
    ).transfer(
        lead_id=seeded_lead,
        new_owner_user_id="sales-new",
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="test",
        request_id="request-transfer-permission-unavailable",
        reason="验证外部权限契约",
    )

    assert result.status == "permission_verification_unavailable"
    with session_factory() as session:
        lead = session.get(Lead, seeded_lead)
        operation = session.get(SmartTableOwnerTransferOperation, result.operation_id)
        assert lead is not None and lead.smart_table_owner_user_id == "sales-old"
        assert operation is not None
        assert operation.remote_update_state == "succeeded"
        assert operation.permission_verification_state == "unavailable"
        assert operation.final_status == "pending_recovery"


def test_transfer_rejects_same_company_target_before_remote_write(
    session_factory: sessionmaker[Session], seeded_lead: str
) -> None:
    """验证目标销售已有同公司线索时拒绝转交且不调用远端。"""
    with session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-2",
                sales_user_id="sales-new",
                sequence=1,
                raw_payload={},
            )
        )
        session.add(
            Lead(
                id="lead-duplicate",
                source_message_id="message-2",
                original_capturing_sales_user_id="sales-new",
                smart_table_owner_user_id="sales-new",
                lifecycle_state="pending_create",
                field_values={"线索名称": "星海科技"},
                standard_company_name="星海科技",
            )
        )
    adapter = FakeSmartTableAdapter()

    with pytest.raises(ValueError, match="同公司"):
        SmartTableOwnerTransferService(
            session_factory, adapter, FakePermissionVerifier()
        ).transfer(
            lead_id=seeded_lead,
            new_owner_user_id="sales-new",
            operator_subject="admin-1",
            operator_role="administrator",
            auth_source="test",
            request_id="request-transfer-conflict",
            reason="冲突测试",
        )

    assert adapter.update_calls == []
