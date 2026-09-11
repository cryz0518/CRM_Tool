"""T10 公司解析、临时线索和销售内去重的应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.companies.models import (
    CompanyRegion,
    CompanyRegionEvidence,
    CompanyUpsertCommand,
    QCCCandidate,
    QCCLookupResult,
)
from app.companies.service import CompanyLeadService, CompanyRegionResolver, MockQCCAdapter
from app.leads.models import Lead, LeadFieldProvenance
from app.messaging.models import Base, IncomingMessage, SalesAuthorization
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供含公司领域模型的隔离真实事务数据库。

    参数：无。
    返回值：逐例生成一个 SQLAlchemy 会话工厂。
    异常：建表或数据库连接失败时向 pytest 传播。
    副作用：测试前创建、测试后删除内存数据库中的全部表。
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


def persist_source_message(
    session_factory: sessionmaker[Session], message_id: str, sales_user_id: str
) -> None:
    """持久化可供公司解析审计的销售来源消息。

    参数：session_factory 创建事务；message_id 和 sales_user_id 定位来源与销售。
    返回值：无。
    异常：违反测试数据库约束时由 SQLAlchemy 抛出。
    副作用：必要时创建授权销售及对应消息。
    """
    with session_factory.begin() as session:
        if session.get(SalesAuthorization, sales_user_id) is None:
            session.add(SalesAuthorization(wecom_user_id=sales_user_id, is_authorized=True))
        # 同一销售的来源消息必须满足 T02 已定义的持久化顺序唯一约束。
        sequence = (
            session.scalar(
                select(func.max(IncomingMessage.sequence)).where(
                    IncomingMessage.sales_user_id == sales_user_id
                )
            )
            or 0
        ) + 1
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
                raw_payload={},
                normalized_text="客户信息",
            )
        )


def test_english_name_com_email_and_international_phone_do_not_prove_foreign_company() -> None:
    """验证名称风格、邮箱域名和号码区号均不能单独将公司判定为国外。

    参数：无。
    返回值：无。
    异常：地域解析规则违反保守判定时由 pytest 报告断言失败。
    副作用：无。
    """
    evidence = CompanyRegionEvidence(
        message_text="公司：Acme Robotics；邮箱：sales@acme.com；电话：+1 415 555 0100",
        company_name="Acme Robotics",
        email="sales@acme.com",
        phone="+1 415 555 0100",
    )

    assert CompanyRegionResolver().resolve(evidence) is CompanyRegion.UNKNOWN


def test_country_and_company_reference_prove_foreign_company() -> None:
    """验证销售明确说明某国公司时可确定为国外公司。

    参数：无。
    返回值：无。
    异常：明确地域信号未被识别时由 pytest 报告断言失败。
    副作用：无。
    """
    evidence = CompanyRegionEvidence(message_text="刚接待了一家德国公司，想了解协作机器人")

    assert CompanyRegionResolver().resolve(evidence) is CompanyRegion.FOREIGN


def test_missing_company_creates_temporary_lead_until_a_verified_name_is_available(
    session_factory: sessionmaker[Session],
) -> None:
    """验证联系人和联系方式可被保存为不能提交的临时线索。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：临时状态、字段或表格所有者不正确时由 pytest 报告断言失败。
    副作用：创建一条没有公司名称的销售审核记录。
    """
    persist_source_message(session_factory, "temporary-message", "sales-1")
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = CompanyLeadService(session_factory, adapter, MockQCCAdapter())

    result = service.upsert(
        CompanyUpsertCommand(
            source_message_id="temporary-message",
            sales_user_id="sales-1",
            fields={"联系人": "张三", "手机": "13800000001"},
        )
    )

    assert result.lifecycle_state == "temporary"
    assert result.standard_company_name is None
    assert adapter.get_record(result.smart_table_record_id or "").fields == {
        "联系人": "张三",
        "手机": "13800000001",
        "线索来源": "展会",
        "创建人": "sales-1",
        "负责人": "sales-1",
    }


def test_unauthorized_salesperson_cannot_use_company_lead_service(
    session_factory: sessionmaker[Session],
) -> None:
    """验证公司解析服务同样执行销售授权校验。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：越权录入未被拒绝时由 pytest 报告断言失败。
    副作用：创建一条来源消息并显式撤销销售授权。
    """
    persist_source_message(session_factory, "unauthorized-message", "sales-1")
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-1")
        assert authorization is not None
        authorization.is_authorized = False
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = CompanyLeadService(session_factory, adapter, MockQCCAdapter())

    with pytest.raises(PermissionError, match="销售未获授权"):
        service.upsert(
            CompanyUpsertCommand(
                source_message_id="unauthorized-message",
                sales_user_id="sales-1",
                fields={"联系人": "张三"},
            )
        )

    assert adapter.get_records() == []


def test_verified_company_only_deduplicates_within_the_same_salesperson(
    session_factory: sessionmaker[Session],
) -> None:
    """验证标准公司名只合并当前销售记录，并保留不同联系人为补充信息。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：跨销售合并、字段覆盖或补充信息缺失时由 pytest 报告断言失败。
    副作用：创建销售一的临时线索、升级它并创建销售二的独立记录。
    """
    for message_id, sales_user_id in (
        ("temporary-message", "sales-1"),
        ("sales-1-company", "sales-1"),
        ("sales-2-company", "sales-2"),
    ):
        persist_source_message(session_factory, message_id, sales_user_id)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    qcc = MockQCCAdapter(
        {"长广溪": QCCLookupResult.matched(QCCCandidate("无锡长广溪智能制造有限公司", "qcc-1"))}
    )
    service = CompanyLeadService(session_factory, adapter, qcc)
    temporary = service.upsert(
        CompanyUpsertCommand(
            source_message_id="temporary-message",
            sales_user_id="sales-1",
            fields={"联系人": "张三"},
        )
    )

    upgraded = service.upsert(
        CompanyUpsertCommand(
            source_message_id="sales-1-company",
            sales_user_id="sales-1",
            fields={"线索名称": "长广溪", "手机": "13800000001"},
            existing_lead_id=temporary.lead_id,
        )
    )
    same_sales = service.upsert(
        CompanyUpsertCommand(
            source_message_id="sales-1-company",
            sales_user_id="sales-1",
            fields={"线索名称": "长广溪", "联系人": "李四", "电话": "0510-12345678"},
        )
    )
    other_sales = service.upsert(
        CompanyUpsertCommand(
            source_message_id="sales-2-company",
            sales_user_id="sales-2",
            fields={"线索名称": "长广溪", "联系人": "王五"},
        )
    )

    assert upgraded.lead_id == temporary.lead_id
    assert upgraded.standard_company_name == "无锡长广溪智能制造有限公司"
    assert same_sales.lead_id == upgraded.lead_id
    assert other_sales.lead_id != upgraded.lead_id
    assert len(adapter.get_records()) == 2
    with session_factory() as session:
        first = session.get(Lead, upgraded.lead_id)
        second = session.get(Lead, other_sales.lead_id)
    assert first is not None
    assert second is not None
    assert first.field_values["联系人"] == "张三"
    assert first.field_values["电话"] == "0510-12345678"
    assert first.enrichment_values["联系人"] == "李四"
    assert second.field_values["联系人"] == "王五"


def test_qcc_failure_or_ambiguity_keeps_company_unverified_until_sales_confirmation(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 QCC 不确定结论不阻塞采集，也不被自动伪装成国外公司。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：未核验状态、候选审计或人工确认标准名不正确时由 pytest 报告。
    副作用：创建一个多候选线索，再由销售确认相同名称。
    """
    persist_source_message(session_factory, "ambiguous-company", "sales-1")
    candidates = (
        QCCCandidate("上海智造有限公司", "qcc-1"),
        QCCCandidate("上海智造科技有限公司", "qcc-2"),
    )
    service = CompanyLeadService(
        session_factory,
        MockSmartTableAdapter(schema=build_required_smart_table_schema()),
        MockQCCAdapter({"上海智造": QCCLookupResult.ambiguous(candidates)}),
    )

    unverified = service.upsert(
        CompanyUpsertCommand(
            source_message_id="ambiguous-company",
            sales_user_id="sales-1",
            fields={"线索名称": "上海智造", "联系人": "张三"},
        )
    )
    confirmed = service.upsert(
        CompanyUpsertCommand(
            source_message_id="ambiguous-company",
            sales_user_id="sales-1",
            existing_lead_id=unverified.lead_id,
            fields={"线索名称": "上海智造"},
            user_confirmed_company=True,
        )
    )

    assert unverified.lifecycle_state == "temporary"
    assert unverified.standard_company_name is None
    assert unverified.verification_status.value == "company_unverified"
    assert confirmed.standard_company_name == "上海智造"
    assert confirmed.verification_status.value == "user_confirmed_unverified"
    with session_factory() as session:
        lead = session.get(Lead, confirmed.lead_id)
    assert lead is not None
    assert lead.qcc_candidates == [
        {"standard_company_name": "上海智造有限公司", "company_id": "qcc-1"},
        {"standard_company_name": "上海智造科技有限公司", "company_id": "qcc-2"},
    ]
    assert lead.company_confirmed_by_user is True


def test_explicit_foreign_evidence_skips_qcc_and_preserves_display_name(
    session_factory: sessionmaker[Session],
) -> None:
    """验证明确国外客户可跳过 QCC，展示文本不会因比较规范化被改写。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：QCC 被调用或名称展示错误时由 pytest 报告断言失败。
    副作用：创建一个明确国外公司的销售审核记录。
    """
    persist_source_message(session_factory, "foreign-company", "sales-1")

    class FailIfCalledQCC:
        """验证国外路径不会调用企查查的测试替身。"""

        def lookup(self, company_name: str) -> QCCLookupResult:
            """收到任何查询均失败，确保测试能观察到错误调用。

            参数：company_name 为被错误送入 QCC 的公司名称。
            返回值：无正常返回。
            异常：始终抛出 AssertionError。
            副作用：无。
            """
            raise AssertionError(f"foreign company must skip QCC: {company_name}")

    service = CompanyLeadService(
        session_factory,
        MockSmartTableAdapter(schema=build_required_smart_table_schema()),
        FailIfCalledQCC(),
    )
    result = service.upsert(
        CompanyUpsertCommand(
            source_message_id="foreign-company",
            sales_user_id="sales-1",
            fields={"线索名称": "Acme   Robotics Inc."},
            region_evidence=CompanyRegionEvidence(explicit_foreign=True),
        )
    )

    assert result.standard_company_name == "acme robotics inc."
    with session_factory() as session:
        lead = session.get(Lead, result.lead_id)
    assert lead is not None
    assert lead.field_values["线索名称"] == "Acme   Robotics Inc."
    assert lead.company_region == "foreign"


def test_synced_company_name_change_requires_identity_review(
    session_factory: sessionmaker[Session],
) -> None:
    """验证已同步线索出现不同标准公司名时不会按普通更新处理。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：生命周期或已确认标准名被静默改写时由 pytest 报告断言失败。
    副作用：创建已同步线索后尝试更新其公司身份。
    """
    persist_source_message(session_factory, "synced-company", "sales-1")
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = CompanyLeadService(
        session_factory,
        adapter,
        MockQCCAdapter(
            {
                "客户甲": QCCLookupResult.matched(QCCCandidate("客户甲有限公司", "qcc-1")),
                "客户乙": QCCLookupResult.matched(QCCCandidate("客户乙有限公司", "qcc-2")),
            }
        ),
    )
    created = service.upsert(
        CompanyUpsertCommand(
            source_message_id="synced-company",
            sales_user_id="sales-1",
            fields={"线索名称": "客户甲"},
        )
    )
    with session_factory.begin() as session:
        lead = session.get(Lead, created.lead_id)
        assert lead is not None
        lead.lifecycle_state = "synced"

    changed = service.upsert(
        CompanyUpsertCommand(
            source_message_id="synced-company",
            sales_user_id="sales-1",
            existing_lead_id=created.lead_id,
            fields={"线索名称": "客户乙"},
        )
    )

    assert changed.lifecycle_state == "company_identity_change_pending_review"
    assert changed.standard_company_name == "客户甲有限公司"
    assert changed.verification_status.value == "verification_conflict"


def test_temporary_lead_upgrades_by_merging_only_its_salespersons_existing_company(
    session_factory: sessionmaker[Session],
) -> None:
    """验证临时线索获得标准名后只可合并当前销售的同公司目标。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：临时字段未补入目标或临时状态未标记合并时由 pytest 报告。
    副作用：创建正式线索、临时线索，并将后者升级至前者。
    """
    for message_id in ("formal", "temporary", "temporary-upgrade"):
        persist_source_message(session_factory, message_id, "sales-1")
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    qcc = MockQCCAdapter(
        {"同一公司": QCCLookupResult.matched(QCCCandidate("同一公司有限公司", "qcc-1"))}
    )
    service = CompanyLeadService(session_factory, adapter, qcc)
    formal = service.upsert(
        CompanyUpsertCommand(
            source_message_id="formal",
            sales_user_id="sales-1",
            fields={"线索名称": "同一公司", "联系人": "张三"},
        )
    )
    temporary = service.upsert(
        CompanyUpsertCommand(
            source_message_id="temporary",
            sales_user_id="sales-1",
            fields={"手机": "13800000001"},
        )
    )

    merged = service.upsert(
        CompanyUpsertCommand(
            source_message_id="temporary-upgrade",
            sales_user_id="sales-1",
            existing_lead_id=temporary.lead_id,
            fields={"线索名称": "同一公司", "电话": "0510-12345678"},
        )
    )

    assert merged.lead_id == formal.lead_id
    with session_factory() as session:
        original_temporary = session.get(Lead, temporary.lead_id)
        target = session.get(Lead, formal.lead_id)
    assert original_temporary is not None
    assert target is not None
    assert original_temporary.lifecycle_state == "merged"
    assert target.field_values["手机"] == "13800000001"
    assert target.field_values["电话"] == "0510-12345678"


def test_qcc_timeout_keeps_a_company_unverified_without_blocking_temporary_capture(
    session_factory: sessionmaker[Session],
) -> None:
    """验证企查查超时只留下未核验事实，不会将公司误判为国外或拒绝采集。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：超时改变采集结论时由 pytest 报告断言失败。
    副作用：使用会超时的 QCC 替身创建一条临时线索。
    """
    persist_source_message(session_factory, "qcc-timeout", "sales-1")

    class TimeoutQCC:
        """模拟尚未接入真实企查查时的瞬时网络超时。"""

        def lookup(self, company_name: str) -> QCCLookupResult:
            """对任何公司查询抛出超时。

            参数：company_name 为待查询公司名称。
            返回值：无正常返回。
            异常：始终抛出 TimeoutError。
            副作用：无。
            """
            raise TimeoutError(company_name)

    result = CompanyLeadService(
        session_factory,
        MockSmartTableAdapter(schema=build_required_smart_table_schema()),
        TimeoutQCC(),
    ).upsert(
        CompanyUpsertCommand(
            source_message_id="qcc-timeout",
            sales_user_id="sales-1",
            fields={"线索名称": "等待核验公司", "手机": "13800000001"},
        )
    )

    assert result.lifecycle_state == "temporary"
    assert result.standard_company_name is None
    assert result.verification_status.value == "company_unverified"


def test_user_modified_field_is_never_overwritten_by_company_merge(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 T09 已标记的人工字段仍优先于同公司后续消息补充。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：人工字段被覆盖或新候选未进入补充信息时由 pytest 报告。
    副作用：创建公司线索、标记人工字段后再执行一次同公司合并。
    """
    for message_id in ("manual-first", "manual-second"):
        persist_source_message(session_factory, message_id, "sales-1")
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    qcc = MockQCCAdapter(
        {"人工保护公司": QCCLookupResult.matched(QCCCandidate("人工保护公司有限公司", "qcc-1"))}
    )
    service = CompanyLeadService(session_factory, adapter, qcc)
    first = service.upsert(
        CompanyUpsertCommand(
            source_message_id="manual-first",
            sales_user_id="sales-1",
            fields={"线索名称": "人工保护公司", "联系人": "张三"},
        )
    )
    with session_factory.begin() as session:
        provenance = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == first.lead_id,
                LeadFieldProvenance.field_name == "联系人",
            )
        )
        assert provenance is not None
        provenance.is_user_modified = True
    adapter.update_record(first.smart_table_record_id or "", {"联系人": "销售确认姓名"})

    merged = service.upsert(
        CompanyUpsertCommand(
            source_message_id="manual-second",
            sales_user_id="sales-1",
            fields={"线索名称": "人工保护公司", "联系人": "李四"},
        )
    )

    assert merged.lead_id == first.lead_id
    assert adapter.get_record(first.smart_table_record_id or "").fields["联系人"] == "销售确认姓名"
    with session_factory() as session:
        lead = session.get(Lead, first.lead_id)
    assert lead is not None
    assert lead.enrichment_values["联系人"] == "李四"


def test_table_company_name_changed_by_salesperson_is_not_overwritten_during_qcc_upgrade(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 QCC 升级前重读表格，不会覆盖销售手工改过的公司名称。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：表格中的人工名称被覆盖或来源未受保护时由 pytest 报告断言失败。
    副作用：先创建未核验线索、模拟销售改表，再使用 Mock QCC 升级后台公司事实。
    """
    persist_source_message(session_factory, "manual-company", "sales-1")
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original = CompanyLeadService(session_factory, adapter, MockQCCAdapter()).upsert(
        CompanyUpsertCommand(
            source_message_id="manual-company",
            sales_user_id="sales-1",
            fields={"线索名称": "原始名称"},
        )
    )
    adapter.update_record(original.smart_table_record_id or "", {"线索名称": "销售手工名称"})
    upgraded = CompanyLeadService(
        session_factory,
        adapter,
        MockQCCAdapter(
            {"原始名称": QCCLookupResult.matched(QCCCandidate("标准公司名称", "qcc-1"))}
        ),
    ).upsert(
        CompanyUpsertCommand(
            source_message_id="manual-company",
            sales_user_id="sales-1",
            existing_lead_id=original.lead_id,
            fields={"线索名称": "原始名称"},
        )
    )

    assert upgraded.standard_company_name == "标准公司名称"
    record = adapter.get_record(original.smart_table_record_id or "")
    assert record is not None
    assert record.fields["线索名称"] == "销售手工名称"
    with session_factory() as session:
        provenance = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == original.lead_id,
                LeadFieldProvenance.field_name == "线索名称",
            )
        )
    assert provenance is not None
    assert provenance.is_user_modified is True


def test_later_qcc_result_conflicting_with_sales_confirmation_requires_review(
    session_factory: sessionmaker[Session],
) -> None:
    """验证后续 QCC 与销售确认冲突时不会静默改名。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：销售确认名被覆盖或冲突状态缺失时由 pytest 报告断言失败。
    副作用：先保存未核验人工确认，再以另一 QCC 结果重试解析。
    """
    persist_source_message(session_factory, "confirmed-company", "sales-1")
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    no_result_qcc = MockQCCAdapter()
    service = CompanyLeadService(session_factory, adapter, no_result_qcc)
    confirmed = service.upsert(
        CompanyUpsertCommand(
            source_message_id="confirmed-company",
            sales_user_id="sales-1",
            fields={"线索名称": "销售确认名称"},
            user_confirmed_company=True,
        )
    )
    later_qcc_service = CompanyLeadService(
        session_factory,
        adapter,
        MockQCCAdapter(
            {
                "销售确认名称": QCCLookupResult.matched(
                    QCCCandidate("企查查不同名称有限公司", "qcc-conflict")
                )
            }
        ),
    )

    conflict = later_qcc_service.upsert(
        CompanyUpsertCommand(
            source_message_id="confirmed-company",
            sales_user_id="sales-1",
            existing_lead_id=confirmed.lead_id,
            fields={"线索名称": "销售确认名称"},
        )
    )

    assert conflict.standard_company_name == "销售确认名称"
    assert conflict.verification_status.value == "verification_conflict"
