"""人工编辑保护与 AI待确认审核服务的应用边界测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ai.models import ExtractedLeadPatch, LeadAnalysis
from app.leads.models import Lead, LeadFieldProvenance, UserConfirmationEvent
from app.leads.review import LeadReviewService
from app.messaging.models import Base, IncomingMessage, SalesAuthorization
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供包含 T05 与 T09 持久化模型的隔离内存数据库。

    参数：无。
    返回值：绑定 SQLite 内存数据库的会话工厂。
    异常：建表失败时由 SQLAlchemy 抛出。
    副作用：测试结束后删除全部表并释放数据库引擎。
    """
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


def _patch(*, fields: dict[str, str], pending: tuple[str, ...] = ()) -> ExtractedLeadPatch:
    """构造已由 T08 完成结构和业务校验的 AI 字段补丁。

    参数：fields 为正式候选字段；pending 为其中中置信度待确认字段。
    返回值：可交给 T09 审核同步服务的提取结果。
    异常：无。
    副作用：无。
    """
    return ExtractedLeadPatch(
        trace_id="trace-9",
        analysis=LeadAnalysis(intent="UPDATE_LEAD"),
        fields=fields,
        pending_confirmation_fields=pending,
        low_confidence_candidates={},
    )


def _lead_with_record(
    session_factory: sessionmaker[Session], adapter: MockSmartTableAdapter
) -> str:
    """准备一条已有 AI 同步值的销售线索和可被销售编辑的表格记录。

    参数：session_factory 提供测试事务；adapter 保存模拟表格记录。
    返回值：新建 Lead 的标识。
    异常：约束失败时由 SQLAlchemy 抛出。
    副作用：持久化销售、消息、线索和字段来源，并创建 Mock 表格记录。
    """
    record = adapter.create_record(
        {"线索名称": "长广溪智造", "业务线": "协作机器人", "负责人": "sales-1"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
        session.add(
            IncomingMessage(
                message_id="message-9",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
            )
        )
        lead = Lead(
            source_message_id="message-9",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record.record_id,
            field_values={"线索名称": "长广溪智造", "业务线": "协作机器人"},
        )
        session.add(lead)
        session.flush()
        session.add(
            LeadFieldProvenance(
                lead_id=lead.id,
                source_message_id="message-9",
                field_name="业务线",
                value="协作机器人",
                last_ai_synced_value="协作机器人",
            )
        )
        return lead.id


def test_sync_rechecks_user_edit_and_only_writes_safe_medium_confidence_field(
    session_factory: sessionmaker[Session],
) -> None:
    """验证表格人工编辑会永久保护字段，而安全中置信度候选进入待确认。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：任一审核状态或表格补丁不符时由 pytest 报告。
    副作用：模拟销售改表后执行一次 T09 AI 审核同步。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead_with_record(session_factory, adapter)
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"业务线": "车载机器人"})

    result = LeadReviewService(session_factory, adapter).sync_ai_patch(
        lead_id,
        "message-9",
        _patch(fields={"业务线": "协作机器人", "客户行业": "机械加工"}, pending=("客户行业",)),
    )

    record = adapter.get_record(record_id)
    assert record is not None
    assert result.protected_fields == ("业务线",)
    assert result.updated_fields == ("客户行业",)
    assert record.fields["业务线"] == "车载机器人"
    assert record.fields["客户行业"] == "机械加工"
    assert record.fields["AI待确认"] == ["客户行业"]
    with session_factory() as session:
        business_line = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead_id,
                LeadFieldProvenance.field_name == "业务线",
            )
        )

    assert business_line is not None
    assert business_line.is_user_modified is True
    assert business_line.is_user_confirmed is True


def test_t09_does_not_treat_a_canonical_reread_as_a_user_edit(
    session_factory: sessionmaker[Session],
) -> None:
    """验证适配器归一化显示名后，T09 不会把相同 AI 值误判为人工编辑。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：字段保护或来源状态断言失败时由 pytest 报告。
    副作用：模拟真实适配器已将带星显示名恢复为规范名后的 T09 重读。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead_with_record(session_factory, adapter)

    result = LeadReviewService(session_factory, adapter).sync_ai_patch(
        lead_id,
        "message-9",
        _patch(fields={"业务线": "协作机器人"}),
    )

    assert result.protected_fields == ()
    with session_factory() as session:
        business_line = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead_id,
                LeadFieldProvenance.field_name == "业务线",
            )
        )

    assert business_line is not None
    assert business_line.is_user_modified is False


def test_required_pending_field_blocks_until_owner_explicitly_confirms(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 CRM 必填待确认字段只可由负责人显式确认后解除提交阻塞。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：阻塞、确认审计或表格元数据断言失败时由 pytest 报告。
    副作用：模拟机器人提交阶段的查询与销售显式确认，不调用 CRM。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead_with_record(session_factory, adapter)
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"AI待确认": ["业务线"]})
    service = LeadReviewService(session_factory, adapter)

    blocked = service.get_submission_confirmation_state(lead_id)
    confirmed = service.confirm_submission_fields(lead_id, "sales-1", ("业务线",))

    assert blocked.can_submit is False
    assert blocked.blocking_fields == ("业务线",)
    assert confirmed.can_submit is True
    record = adapter.get_record(record_id)
    assert record is not None
    assert record.fields["AI待确认"] == []
    with session_factory() as session:
        event = session.scalar(
            select(UserConfirmationEvent).where(UserConfirmationEvent.lead_id == lead_id)
        )
        provenance = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead_id,
                LeadFieldProvenance.field_name == "业务线",
            )
        )

    assert event is not None
    assert event.field_name == "业务线"
    assert event.confirmed_value == "协作机器人"
    assert event.operator_sales_user_id == "sales-1"
    assert event.confirmation_source == "robot"
    assert provenance is not None
    assert provenance.is_user_confirmed is True


def test_submission_recheck_turns_sales_edit_into_confirmation_and_removes_pending_mark(
    session_factory: sessionmaker[Session],
) -> None:
    """验证销售修改业务字段时系统自动清理待确认标记且不再阻塞提交。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：人工保护、待确认清理或阻塞状态断言失败时由 pytest 报告。
    副作用：模拟销售在 AI 写入后直接编辑业务字段，再进入提交前重读。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead_with_record(session_factory, adapter)
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"AI待确认": ["业务线"], "业务线": "车载机器人"})

    state = LeadReviewService(session_factory, adapter).get_submission_confirmation_state(lead_id)

    assert state.can_submit is True
    record = adapter.get_record(record_id)
    assert record is not None
    assert record.fields["AI待确认"] == []
    with session_factory() as session:
        provenance = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead_id,
                LeadFieldProvenance.field_name == "业务线",
            )
        )

    assert provenance is not None
    assert provenance.is_user_modified is True
    assert provenance.is_user_confirmed is True


def test_medium_confidence_same_value_still_enters_confirmation_queue(
    session_factory: sessionmaker[Session],
) -> None:
    """验证中置信度候选即使等于当前 AI 值，仍会进入 AI待确认。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：待确认元数据或提交阻塞断言失败时由 pytest 报告。
    副作用：对已有 AI 同步值执行一次相同值的中置信度审核同步。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead_with_record(session_factory, adapter)

    LeadReviewService(session_factory, adapter).sync_ai_patch(
        lead_id,
        "message-9",
        _patch(fields={"业务线": "协作机器人"}, pending=("业务线",)),
    )

    record = adapter.get_record(next(iter(adapter.get_records())).record_id)
    assert record is not None
    assert record.fields["AI待确认"] == ["业务线"]
    assert LeadReviewService(session_factory, adapter).get_submission_confirmation_state(
        lead_id
    ).blocking_fields == ("业务线",)


def test_card_unavailable_does_not_prefill_required_medium_confidence_field(
    session_factory: sessionmaker[Session],
) -> None:
    """验证机器人卡片不可用时必填中置信度字段不写正式表格字段。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：降级路径仍预填或污染 AI待确认 时由 pytest 报告。
    副作用：使用关闭卡片能力的 T09 服务处理一条中置信度必填候选。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _lead_with_record(session_factory, adapter)
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"业务线": ""})

    result = LeadReviewService(
        session_factory, adapter, robot_submission_confirmation_available=False
    ).sync_ai_patch(
        lead_id,
        "message-9",
        _patch(fields={"业务线": "车载机器人"}, pending=("业务线",)),
    )

    record = adapter.get_record(record_id)
    assert record is not None
    assert result.updated_fields == ()
    assert record.fields["业务线"] == ""
    assert "AI待确认" not in record.fields
