"""T14 真实 PostgreSQL 下 CRM create 与逻辑废弃竞态集成测试。"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import get_settings
from app.crm.mock import MockCRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionCommand
from app.leads.discard import LeadDiscardService, LeadDiscardStatus
from app.leads.models import CrmSyncRecord, Lead, LeadDiscardRequest
from app.messaging.models import Base, BusinessAuditEvent, IncomingMessage, SalesAuthorization
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


@pytest.fixture
def postgres_session_factory() -> Generator[sessionmaker[Session], None, None]:
    """创建仅用于 T14 的真实 PostgreSQL 临时 schema。

    参数：无。
    返回值：绑定临时 schema 的 SQLAlchemy 会话工厂。
    异常：Docker PostgreSQL 不可达时跳过；其他 DDL 错误直接暴露。
    副作用：测试前建表，测试后级联删除随机 schema。
    """
    engine = create_engine(get_settings().database_url)
    schema_name = f"t14_{uuid4().hex}"
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        engine.dispose()
        pytest.skip("需要 Docker Compose PostgreSQL 执行 T14 并发集成测试")
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


def prepare_pending_create_case(
    session_factory: sessionmaker[Session], adapter: MockSmartTableAdapter, suffix: str
) -> str:
    """准备一个具备真实表格记录和 CRM 最小必填字段的 pending_create 线索。

    参数：session_factory 为临时 PostgreSQL schema 会话工厂；adapter 为测试表格；
    suffix 隔离消息和公司标识。
    返回值：新建 Lead 的标识。
    异常：数据库或 Mock 表格约束失败时向测试传播。
    副作用：写入授权、原始消息、表格记录和待创建 Lead。
    """
    sales_user_id = f"sales-{suffix}"
    message_id = f"message-{suffix}"
    record = adapter.create_record(
        {
            "负责人": sales_user_id,
            "创建人": sales_user_id,
            "线索名称": f"竞态公司-{suffix}",
            "业务线": "协作机器人",
            "线索来源": "展会",
            "手机": "13800000000",
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id=sales_user_id,
                crm_user_id=f"crm-{suffix}",
                is_authorized=True,
                is_active=True,
            )
        )
        session.flush()
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=1,
                raw_payload={"text": "客户信息"},
                normalized_text="客户信息",
            )
        )
        session.flush()
        lead = Lead(
            source_message_id=message_id,
            original_capturing_sales_user_id=sales_user_id,
            smart_table_owner_user_id=sales_user_id,
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            standard_company_name=f"竞态公司-{suffix}",
            field_values={"线索名称": f"竞态公司-{suffix}", "业务线": "协作机器人"},
        )
        session.add(lead)
        session.flush()
        return lead.id


def test_crm_create_and_discard_wait_for_external_fact(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """验证 CRM create 在途时废弃请求等待，最终成功使 Lead synced 且请求未生效。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = prepare_pending_create_case(postgres_session_factory, adapter, "success")
    entered, release = Event(), Event()

    class BlockingCRM(MockCRMAdapter):
        """阻塞 create 外部边界，使废弃请求在真实 processing 状态下提交。"""

        def create_lead(self, *args: object, **kwargs: object) -> object:
            """报告 CRM create 已开始并等待测试释放。

            参数：args/kwargs 透传父类 create_lead。返回值：父类模拟 CRM 结果。
            异常：未在五秒内释放时由断言报告。副作用：设置 entered 并阻塞当前提交线程。
            """
            entered.set()
            assert release.wait(timeout=5)
            return super().create_lead(*args, **kwargs)

    crm = BlockingCRM()

    def submit() -> object:
        """在独立线程中执行确定性 CRM 提交。"""
        return CrmSubmissionService(postgres_session_factory, adapter, crm).submit(
            SubmissionCommand("提交今天的线索", "sales-success", "submit-success")
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(submit)
        assert entered.wait(timeout=5)
        discard = LeadDiscardService(postgres_session_factory).discard(
            lead_id, "sales-success", "测试并发废弃"
        )
        assert discard.status is LeadDiscardStatus.WAITING_FOR_CRM
        release.set()
        result = future.result(timeout=5)
        repeated_discard = LeadDiscardService(postgres_session_factory).discard(
            lead_id, "sales-success", "重复点击"
        )

    assert result.succeeded == 1
    assert repeated_discard.status is LeadDiscardStatus.NOT_EFFECTIVE
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        request = session.scalar(
            select(LeadDiscardRequest).where(LeadDiscardRequest.lead_id == lead_id)
        )
        audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.event_type == "discard_request_not_effective"
            )
        )
    assert lead is not None and lead.lifecycle_state == "synced"
    assert request is not None and request.status == "not_effective"
    assert audit is not None
    assert not hasattr(crm, "delete_lead")


def test_crm_create_failure_preserves_failure_and_effective_discard(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """验证 CRM create 最终失败时保留失败事实且不会错误标记 synced。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = prepare_pending_create_case(postgres_session_factory, adapter, "failure")
    entered, release = Event(), Event()

    class FailingCRM(MockCRMAdapter):
        """阻塞后返回永久 CRM 错误，模拟已知的最终失败事实。"""

        def create_lead(self, *args: object, **kwargs: object) -> object:
            """等待废弃请求提交后抛出 CRM 永久错误。

            参数：args/kwargs 为父类接口参数。返回值：无正常返回值。
            异常：抛出 ValueError 作为不可自动重试的 CRM 失败。副作用：设置 entered 并等待 release。
            """
            entered.set()
            assert release.wait(timeout=5)
            raise ValueError("CRM payload rejected")

    crm = FailingCRM()

    def submit() -> object:
        """在独立线程中执行会失败的 CRM 提交。"""
        return CrmSubmissionService(postgres_session_factory, adapter, crm).submit(
            SubmissionCommand("提交今天的线索", "sales-failure", "submit-failure")
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(submit)
        assert entered.wait(timeout=5)
        discard = LeadDiscardService(postgres_session_factory).discard(
            lead_id, "sales-failure", "测试失败后废弃"
        )
        assert discard.status is LeadDiscardStatus.WAITING_FOR_CRM
        release.set()
        result = future.result(timeout=5)

    assert result.failed_pending_review == 1
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        request = session.scalar(
            select(LeadDiscardRequest).where(LeadDiscardRequest.lead_id == lead_id)
        )
    assert lead is not None and lead.lifecycle_state == "discarded"
    assert sync is not None
    assert sync.status == "failed_pending_review"
    assert sync.failure_category == "permanent"
    assert request is not None and request.status == "effective"
