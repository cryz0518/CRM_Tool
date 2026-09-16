"""T14 真实 PostgreSQL 下 CRM create 与逻辑废弃竞态集成测试。"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event, Lock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.ai.models import ExtractedLeadPatch, LeadAnalysis
from app.core.config import get_settings
from app.crm.mock import MockCRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionCommand
from app.leads.discard import LeadDiscardService, LeadDiscardStatus
from app.leads.models import (
    CrmCompanyIdentity,
    CrmSyncRecord,
    Lead,
    LeadDiscardRequest,
    LeadFieldProvenance,
)
from app.leads.review import LeadReviewService
from app.messaging.models import Base, BusinessAuditEvent, IncomingMessage, SalesAuthorization
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableRecord
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
        # CRM adapter 仍被阻塞时，第三个独立 session 必须直接看到 Lead 尚未被废弃。
        with postgres_session_factory() as observation:
            observed_lead = observation.get(Lead, lead_id)
        assert observed_lead is not None
        assert observed_lead.lifecycle_state == "pending_create"
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
    assert crm.delete_calls == 0
    assert adapter.delete_calls == 0
    with postgres_session_factory() as session:
        identity = session.scalar(
            select(CrmCompanyIdentity).where(
                CrmCompanyIdentity.creating_lead_id == lead_id
            )
        )
    assert identity is not None
    assert identity.state == "active"


def test_discard_rechecks_sync_after_lead_lock(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """证明 Lead 锁等待期间新提交的 pending CRM create 不会被 stale read 忽略。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = prepare_pending_create_case(postgres_session_factory, adapter, "stale-read")
    holder = postgres_session_factory()
    holder.begin()
    try:
        # T1 先锁住 Lead，并在同一未提交事务创建 pending Sync。
        held_lead = holder.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
        assert held_lead is not None
        sync = CrmSyncRecord(
            lead_id=lead_id,
            operation="create",
            smart_table_record_id=held_lead.smart_table_record_id or "",
            idempotency_key=f"crm:create:{lead_id}",
            canonical_payload={
                "线索名称": held_lead.standard_company_name,
                "业务线": "协作机器人",
                "手机": "13800000000",
            },
            snapshot_hash="a" * 64,
            request_message_id=held_lead.source_message_id,
            submitting_sales_user_id="sales-stale-read",
            submitting_crm_user_id="crm-stale-read",
            status="pending",
        )
        holder.add(sync)
        holder.flush()
        holder.add(
            CrmCompanyIdentity(
                standard_company_name=held_lead.standard_company_name or "",
                state="reserving",
                creating_lead_id=lead_id,
                creating_sync_record_id=sync.id,
            )
        )
        holder.flush()

        # 在 T1 提交前，独立 session 确认 pending Sync 对其他事务不可见。
        with postgres_session_factory() as before_commit:
            assert before_commit.scalar(
                select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id)
            ) is None

        engine = postgres_session_factory.kw["bind"]
        assert engine is not None
        lead_lock_requested = Event()

        def observe_discard_sql(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: bool,
        ) -> None:
            """记录 discard 线程已经请求 Lead 行锁。"""
            normalized = statement.lower()
            if "leads" in normalized and "for update" in normalized:
                lead_lock_requested.set()

        sqlalchemy_event.listen(engine, "before_cursor_execute", observe_discard_sql)
        try:
            def discard() -> object:
                """在独立线程执行受控废弃请求。"""
                return LeadDiscardService(postgres_session_factory).discard(
                    lead_id, "sales-stale-read", "验证锁后重读"
                )

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(discard)
                # 新实现会先请求 Lead 锁；旧实现会先看到未提交 Sync 不存在，再请求 Lead 锁。
                assert lead_lock_requested.wait(timeout=5)
                holder.commit()
                result = future.result(timeout=5)
        finally:
            sqlalchemy_event.remove(engine, "before_cursor_execute", observe_discard_sql)

        assert result.status is LeadDiscardStatus.WAITING_FOR_CRM
        with postgres_session_factory() as session:
            lead = session.get(Lead, lead_id)
            request = session.scalar(
                select(LeadDiscardRequest).where(LeadDiscardRequest.lead_id == lead_id)
            )
            identity = session.get(CrmCompanyIdentity, held_lead.standard_company_name)
        assert lead is not None and lead.lifecycle_state != "discarded"
        assert request is not None and request.status == "pending"
        assert identity is not None and identity.state == "reserving"
    finally:
        if holder.in_transaction():
            holder.rollback()
        holder.close()


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
        identity = session.scalar(
            select(CrmCompanyIdentity).where(CrmCompanyIdentity.creating_lead_id == lead_id)
        )
    assert lead is not None and lead.lifecycle_state == "discarded"
    assert sync is not None
    assert sync.status == "failed_pending_review"
    assert sync.failure_category == "permanent"
    assert request is not None and request.status == "effective"
    assert identity is not None and identity.state == "failed_pending_review"
    assert adapter.delete_calls == 0
    assert crm.delete_calls == 0


def test_retry_patch_does_not_clobber_later_message_update(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """用确定性第二次读屏障证明失败补充不会覆盖已提交的后续消息字段。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = prepare_pending_create_case(postgres_session_factory, adapter, "lost-update")
    with postgres_session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-lost-update-next",
                sales_user_id="sales-lost-update",
                sequence=2,
                raw_payload={"text": "后续消息"},
                normalized_text="后续消息",
            )
        )

    second_read_started, release_second_read = Event(), Event()

    class PausingSmartTable(MockSmartTableAdapter):
        """在失败补充的第二次当前状态读取处暂停，建立确定性提交窗口。"""

        def __init__(self) -> None:
            """复用标准 Mock 表结构并初始化读取计数。"""
            super().__init__(schema=build_required_smart_table_schema())
            self._records.update({record.record_id: record for record in adapter.get_records()})
            self._read_count = 0

        def get_record(self, record_id: str) -> SmartTableRecord | None:
            """复制共享记录后在第二次读取前发出屏障并等待后续事务提交。"""
            self._read_count += 1
            if self._read_count == 2:
                second_read_started.set()
                assert release_second_read.wait(timeout=5)
            return super().get_record(record_id)

    pausing_adapter = PausingSmartTable()
    retry_service = LeadReviewService(postgres_session_factory, pausing_adapter)

    def run_retry() -> object:
        """执行失败消息的当前状态补充。"""
        return retry_service.sync_ai_patch(
            lead_id,
            "message-lost-update",
            ExtractedLeadPatch(
                trace_id="retry-lost-update",
                analysis=LeadAnalysis(intent="UPDATE_LEAD"),
                fields={"联系人": "失败消息联系人"},
                pending_confirmation_fields=(),
                low_confidence_candidates={},
            ),
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_retry)
        assert second_read_started.wait(timeout=5)
        # 屏障期间运行真实 T09 路径提交 N+1，不能依赖 sleep 或最终状态碰运气。
        LeadReviewService(postgres_session_factory, pausing_adapter).sync_ai_patch(
            lead_id,
            "message-lost-update-next",
            ExtractedLeadPatch(
                trace_id="next-message",
                analysis=LeadAnalysis(intent="UPDATE_LEAD"),
                fields={"电话": "010-12345678"},
                pending_confirmation_fields=(),
                low_confidence_candidates={},
            ),
        )
        release_second_read.set()
        retry_result = future.result(timeout=5)

    assert retry_result.updated_fields == ("联系人",)
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        next_source = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead_id,
                LeadFieldProvenance.source_message_id == "message-lost-update-next",
            )
        )
    assert lead is not None
    assert lead.field_values["电话"] == "010-12345678"
    assert lead.field_values["联系人"] == "失败消息联系人"
    assert next_source is not None and next_source.value == "010-12345678"


def test_remote_success_then_timeout_reuses_frozen_create_operation(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """验证远端已成功但响应超时后，恢复仍复用同一 payload、幂等键和 CRM 身份。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = prepare_pending_create_case(postgres_session_factory, adapter, "timeout-recovery")

    class RemoteSuccessThenTimeoutCRM(MockCRMAdapter):
        """第一次调用先落下远端成功事实再模拟响应丢失。"""

        def __init__(self) -> None:
            """初始化一次性响应超时开关。"""
            super().__init__()
            self._timeout_after_remote_success = True

        def create_lead(self, *args: object, **kwargs: object) -> object:
            """记录远端成功调用后只丢弃第一次响应，后续按同一幂等键返回成功。"""
            result = super().create_lead(*args, **kwargs)
            if self._timeout_after_remote_success:
                self._timeout_after_remote_success = False
                raise TimeoutError("CRM response lost after remote commit")
            return result

    crm = RemoteSuccessThenTimeoutCRM()
    service = CrmSubmissionService(
        postgres_session_factory, adapter, crm, crm_create_retry_count=1
    )
    first = service.submit(
        SubmissionCommand("提交今天的线索", "sales-timeout-recovery", "submit-timeout")
    )
    assert first.retrying == 1
    with postgres_session_factory() as session:
        sync_before = session.scalar(
            select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id)
        )
    assert sync_before is not None
    frozen_snapshot_hash = sync_before.snapshot_hash
    frozen_payload = dict(sync_before.canonical_payload)

    discard = LeadDiscardService(postgres_session_factory).discard(
        lead_id, "sales-timeout-recovery", "响应超时期间的废弃请求"
    )
    assert discard.status is LeadDiscardStatus.WAITING_FOR_CRM

    with postgres_session_factory.begin() as session:
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        assert sync is not None
        # 只推进持久化 processing 租约，模拟原 Worker 失联后的可恢复状态，不改冻结字段。
        sync.status = "processing"
        sync.processing_started_at = datetime.now(UTC) - timedelta(minutes=10)
        sync.processing_lease_expires_at = datetime.now(UTC) - timedelta(minutes=5)

    recovered = service.submit(
        SubmissionCommand("提交今天的线索", "sales-timeout-recovery", "submit-timeout-retry")
    )
    assert recovered.succeeded == 1
    assert crm.calls == 2
    assert crm.idempotency_keys[0] == crm.idempotency_keys[1]
    assert crm.payloads[0] == crm.payloads[1]
    assert crm.payloads[0] == frozen_payload
    assert crm.crm_user_ids == ["crm-timeout-recovery", "crm-timeout-recovery"]
    assert crm.delete_calls == 0

    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
        request = session.scalar(
            select(LeadDiscardRequest).where(LeadDiscardRequest.lead_id == lead_id)
        )
        identity = session.scalar(
            select(CrmCompanyIdentity).where(CrmCompanyIdentity.creating_lead_id == lead_id)
        )
    assert lead is not None and lead.lifecycle_state == "synced"
    assert sync is not None and sync.status == "succeeded"
    assert sync.snapshot_hash == frozen_snapshot_hash
    assert request is not None and request.status == "not_effective"
    assert identity is not None and identity.state == "active"


def test_expired_crm_claim_fences_late_worker_result(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """验证 lease 接管者成功后，旧 worker 的迟到 timeout 不会覆盖本地成功事实。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = prepare_pending_create_case(postgres_session_factory, adapter, "claim-fence")
    first_entered, second_finished, release_first = Event(), Event(), Event()
    call_lock = Lock()

    class LateTimeoutCRM(MockCRMAdapter):
        """让第一次 claim 延迟超时，让第二次过期租约恢复正常成功。"""

        def __init__(self) -> None:
            """初始化调用序号和两次请求的原始冻结参数记录。"""
            super().__init__()
            self._call_number = 0
            self.received_keys: list[str] = []
            self.received_payloads: list[dict[str, object]] = []
            self.received_users: list[str] = []

        def create_lead(self, *args: object, **kwargs: object) -> object:
            """阻塞旧 worker 并在接管 worker 成功后抛出迟到 timeout。"""
            with call_lock:
                self._call_number += 1
                call_number = self._call_number
            self.received_keys.append(str(kwargs["idempotency_key"]))
            self.received_payloads.append(dict(args[0]))  # type: ignore[arg-type]
            self.received_users.append(str(kwargs["crm_user_id"]))
            if call_number == 1:
                first_entered.set()
                assert release_first.wait(timeout=5)
                raise TimeoutError("old worker response arrived after lease takeover")
            result = super().create_lead(*args, **kwargs)
            second_finished.set()
            return result

    crm = LateTimeoutCRM()
    service = CrmSubmissionService(
        postgres_session_factory, adapter, crm, crm_create_retry_count=1
    )

    def submit() -> object:
        """执行一次独立 session 的 CRM 提交。"""
        return service.submit(
            SubmissionCommand("提交今天的线索", "sales-claim-fence", "submit-claim-fence")
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(submit)
        assert first_entered.wait(timeout=5)
        with postgres_session_factory.begin() as session:
            sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
            assert sync is not None
            # 第一 worker 仍在外部调用中时只使其 lease 过期，不改变冻结操作内容。
            sync.processing_lease_expires_at = datetime.now(UTC) - timedelta(minutes=1)
        second_future = executor.submit(submit)
        assert second_finished.wait(timeout=5)
        second_result = second_future.result(timeout=5)
        assert second_result.succeeded == 1
        release_first.set()
        first_result = first_future.result(timeout=5)

    assert first_result.succeeded == 1
    assert len(crm.received_keys) == 2
    assert len(crm.received_payloads) == 2
    assert crm.received_keys == [crm.received_keys[0], crm.received_keys[0]]
    assert crm.received_payloads == [crm.received_payloads[0], crm.received_payloads[0]]
    assert crm.received_users == ["crm-claim-fence", "crm-claim-fence"]
    assert crm.calls == 1
    assert crm.delete_calls == 0
    with postgres_session_factory() as session:
        lead = session.get(Lead, lead_id)
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == lead_id))
    assert lead is not None and lead.lifecycle_state == "synced"
    assert sync is not None and sync.status == "succeeded"
