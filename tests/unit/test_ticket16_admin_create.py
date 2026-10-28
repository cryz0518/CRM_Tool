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

    def __init__(self, member_ids: dict[str, str] | None = None) -> None:
        """初始化管理员创建用的远端记录和通讯录映射。

        参数：member_ids 为业务用户标识到表格 userid 的可选测试映射。
        返回值：无。
        异常：无。
        副作用：初始化内存记录和创建调用计数。
        """

        self._member_ids = member_ids or {}
        self.calls: list[tuple[dict[str, object], object]] = []
        self.records: dict[str, SmartTableRecord] = {}
        self.raise_after_create = False
        self.definite_failures = 0

    def resolve_member_user_id(self, user_id: str) -> str:
        """将测试业务身份转换为智能表格成员标识。

        参数：user_id 为后台保存的业务成员标识。
        返回值：测试通讯录映射后的 userid；未配置映射时原样返回。
        异常：无。
        副作用：无。
        """
        return self._member_ids.get(user_id, user_id)

    def create_record(self, fields: dict[str, object], *, actor: object) -> SmartTableRecord:
        """模拟创建记录并将人员字段写成表格通讯录 userid。

        参数：fields 为待写入业务字段；actor 为调用主体。
        返回值：新建记录的表格快照。
        异常：字段包含系统时间列时由断言报告。
        副作用：向内存记录集新增一条记录并记录创建请求。
        """

        assert not {"创建时间", "录入时间"}.intersection(fields)
        self.calls.append((dict(fields), actor))
        table_fields = dict(fields)
        for name in ("创建人", "负责人"):
            if isinstance(table_fields.get(name), str):
                table_fields[name] = self.resolve_member_user_id(str(table_fields[name]))
        record = SmartTableRecord(
            record_id=f"record-admin-{len(self.calls)}",
            fields={**table_fields, "创建时间": "2026-10-09 10:20"},
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
    assert fields["业务线"] == "协作机器人"
    assert fields["职务"] == "经理"
    assert fields["沟通方式"] == "活动"
    assert fields["客户行业"] == "其他"
    with session_factory() as session:
        lead = session.get(Lead, result.lead_id)
        operation = session.get(AdminLeadCreationOperation, result.operation_id)
        assert lead is not None
        assert lead.source_message_id is None
        assert lead.field_values["录入时间"]
        assert not {"创建时间", "录入时间"}.intersection(fields)
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
    """远端人员字段已用通讯录 userid 时，恢复可证明记录且不会重复新增。

    参数：session_factory 提供隔离数据库；seeded_admin 初始化管理员与销售。
    返回值：无。
    异常：恢复状态、远端负责人或创建调用次数不符时由 pytest 报告。
    副作用：模拟一次结果未知的创建，随后只读核验已有记录。
    """

    table = FakeAdminCreateTable({"sales-owner": "directory-owner"})
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
    assert next(iter(table.records.values())).fields["负责人"] == "directory-owner"


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
