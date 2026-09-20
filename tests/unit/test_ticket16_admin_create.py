"""T16 管理员补建线索的领域行为测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.leads.admin_create import AdminLeadCreationService
from app.leads.models import AdminLeadCreationOperation, Lead
from app.messaging.models import Base, SalesAuthorization
from app.smart_table.adapter import SmartTableDefiniteRemoteFailure
from app.smart_table.models import SmartTableRecord


class FakeAdminCreateTable:
    """记录管理员创建的完整最小字段集合。"""

    def __init__(self) -> None:
        """初始化调用记录。"""

        self.calls: list[tuple[dict[str, object], object]] = []
        self.records: dict[str, SmartTableRecord] = {}
        self.raise_after_create = False
        self.definite_failures = 0

    def create_record(self, fields: dict[str, object], *, actor: object) -> SmartTableRecord:
        """返回远端创建快照。"""

        self.calls.append((dict(fields), actor))
        record = SmartTableRecord(
            record_id=f"record-admin-{len(self.calls)}", fields=dict(fields)
        )
        self.records[record.record_id] = record
        if self.definite_failures:
            self.definite_failures -= 1
            raise SmartTableDefiniteRemoteFailure("adapter confirmed no remote write")
        if self.raise_after_create:
            raise RuntimeError("remote result unknown after create")
        return record

    def get_record(self, record_id: str) -> SmartTableRecord | None:
        """按记录标识读取远端已创建事实。"""

        return self.records.get(record_id)

    def find_records(self, filters: dict[str, object]) -> list[SmartTableRecord]:
        """按冻结字段返回可证明属于 operation 的候选记录。"""

        return [
            record
            for record in self.records.values()
            if all(record.fields.get(name) == value for name, value in filters.items())
        ]


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供隔离数据库。"""

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
def seeded_admin(session_factory: sessionmaker[Session]) -> None:
    """写入管理员与两个明确销售身份。"""

    with session_factory.begin() as session:
        session.add_all(
            [
                SalesAuthorization(
                    wecom_user_id="admin-1",
                    is_active=True,
                    is_authorized=True,
                    is_administrator=True,
                ),
                SalesAuthorization(
                    wecom_user_id="sales-capture",
                    is_active=True,
                    is_authorized=True,
                ),
                SalesAuthorization(
                    wecom_user_id="sales-owner",
                    is_active=True,
                    is_authorized=True,
                ),
            ]
        )


def test_admin_create_has_explicit_capture_and_owner_without_message(
    session_factory: sessionmaker[Session], seeded_admin: None
) -> None:
    """管理员补建不伪造消息、不制造提交人或 CRM owner。"""

    table = FakeAdminCreateTable()
    result = AdminLeadCreationService(session_factory, table).create(
        original_capturing_sales_user_id="sales-capture",
        smart_table_owner_user_id="sales-owner",
        field_values={
            "线索名称": "手工补建公司",
            "业务线": "协作机器人",
            "手机": "13800000000",
        },
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="console",
        request_id="request-create-1",
        reason="历史资料补录",
    )

    assert result.status == "succeeded"
    assert len(table.calls) == 1
    fields, _actor = table.calls[0]
    assert fields["负责人"] == "sales-owner"
    assert fields["创建人"] == "sales-owner"
    with session_factory() as session:
        lead = session.get(Lead, result.lead_id)
        operation = session.get(AdminLeadCreationOperation, result.operation_id)
        assert lead is not None
        assert lead.source_message_id is None
        assert lead.original_capturing_sales_user_id == "sales-capture"
        assert lead.smart_table_owner_user_id == "sales-owner"
        assert lead.smart_table_record_id == "record-admin-1"
        assert operation is not None and operation.final_status == "succeeded"
        assert not session.scalar(select(Lead).where(Lead.source_message_id.is_not(None)))


def test_admin_create_requires_reason_and_authorized_target(
    session_factory: sessionmaker[Session], seeded_admin: None
) -> None:
    """空原因或未授权目标销售不能创建。"""

    table = FakeAdminCreateTable()
    service = AdminLeadCreationService(session_factory, table)
    with pytest.raises(ValueError, match="原因"):
        service.create(
            original_capturing_sales_user_id="sales-capture",
            smart_table_owner_user_id="sales-owner",
            field_values={"线索名称": "公司", "业务线": "协作机器人", "手机": "1"},
            operator_subject="admin-1",
            operator_role="administrator",
            auth_source="console",
            request_id="request-create-reason",
            reason=" ",
        )

    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-disabled",
                is_active=False,
                is_authorized=True,
            )
        )
    with pytest.raises(ValueError, match="active"):
        service.create(
            original_capturing_sales_user_id="sales-capture",
            smart_table_owner_user_id="sales-disabled",
            field_values={"线索名称": "公司", "业务线": "协作机器人", "手机": "1"},
            operator_subject="admin-1",
            operator_role="administrator",
            auth_source="console",
            request_id="request-create-target",
            reason="补录",
        )


def test_admin_create_unknown_remote_result_reconciles_without_duplicate_create(
    session_factory: sessionmaker[Session], seeded_admin: None
) -> None:
    """远端已创建但响应未知时，重复 request 先核验事实而不再次 create。"""

    table = FakeAdminCreateTable()
    table.raise_after_create = True
    service = AdminLeadCreationService(session_factory, table)
    first = service.create(
        original_capturing_sales_user_id="sales-capture",
        smart_table_owner_user_id="sales-owner",
        field_values={"线索名称": "未知结果公司", "业务线": "协作机器人", "手机": "13800000000"},
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="console",
        request_id="request-create-unknown",
        reason="模拟远端响应未知",
    )
    assert first.status == "pending_recovery"
    assert len(table.calls) == 1

    table.raise_after_create = False
    second = service.create(
        original_capturing_sales_user_id="sales-capture",
        smart_table_owner_user_id="sales-owner",
        field_values={"线索名称": "未知结果公司", "业务线": "协作机器人", "手机": "13800000000"},
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="console",
        request_id="request-create-unknown",
        reason="恢复已创建远端记录",
    )
    assert second.status == "succeeded"
    assert len(table.calls) == 1


def test_admin_create_definite_remote_failure_can_retry_safely(
    session_factory: sessionmaker[Session], seeded_admin: None
) -> None:
    """适配器明确未写入时，同一 frozen request 可以安全重试。"""

    table = FakeAdminCreateTable()
    table.definite_failures = 1
    service = AdminLeadCreationService(session_factory, table)
    first = service.create(
        original_capturing_sales_user_id="sales-capture",
        smart_table_owner_user_id="sales-owner",
        field_values={"线索名称": "确定失败公司", "业务线": "协作机器人", "手机": "13800000000"},
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="console",
        request_id="request-create-definite-failure",
        reason="模拟明确失败",
    )
    assert first.status == "remote_create_failed"

    second = service.create(
        original_capturing_sales_user_id="sales-capture",
        smart_table_owner_user_id="sales-owner",
        field_values={"线索名称": "确定失败公司", "业务线": "协作机器人", "手机": "13800000000"},
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="console",
        request_id="request-create-definite-failure",
        reason="重试明确失败",
    )
    assert second.status == "succeeded"
    assert len(table.calls) == 2


def test_admin_create_remote_success_local_finalize_failure_recovers_without_create(
    session_factory: sessionmaker[Session], seeded_admin: None
) -> None:
    """远端已成功但本地 finalize 崩溃时，恢复只读取远端，不重复创建。"""

    table = FakeAdminCreateTable()
    service = AdminLeadCreationService(session_factory, table)
    original_finalize = service._finalize_local_creation

    def fail_finalize(operation_id: str, lead_id: str, record_id: str) -> None:
        """模拟本地提交异常。"""

        del operation_id, lead_id, record_id
        raise RuntimeError("local finalize failed")

    service._finalize_local_creation = fail_finalize  # type: ignore[method-assign]
    first = service.create(
        original_capturing_sales_user_id="sales-capture",
        smart_table_owner_user_id="sales-owner",
        field_values={
            "线索名称": "本地提交失败公司",
            "业务线": "协作机器人",
            "手机": "13800000000",
        },
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="console",
        request_id="request-create-local-failure",
        reason="模拟本地提交失败",
    )
    assert first.status == "pending_recovery"
    service._finalize_local_creation = original_finalize  # type: ignore[method-assign]
    second = service.create(
        original_capturing_sales_user_id="sales-capture",
        smart_table_owner_user_id="sales-owner",
        field_values={
            "线索名称": "本地提交失败公司",
            "业务线": "协作机器人",
            "手机": "13800000000",
        },
        operator_subject="admin-1",
        operator_role="administrator",
        auth_source="console",
        request_id="request-create-local-failure",
        reason="恢复本地提交",
    )
    assert second.status == "succeeded"
    assert len(table.calls) == 1
