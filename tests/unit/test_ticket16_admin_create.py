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
from app.smart_table.models import SmartTableRecord


class FakeAdminCreateTable:
    """记录管理员创建的完整最小字段集合。"""

    def __init__(self) -> None:
        """初始化调用记录。"""

        self.calls: list[tuple[dict[str, object], object]] = []

    def create_record(self, fields: dict[str, object], *, actor: object) -> SmartTableRecord:
        """返回远端创建快照。"""

        self.calls.append((dict(fields), actor))
        return SmartTableRecord(record_id="record-admin-1", fields=dict(fields))


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
