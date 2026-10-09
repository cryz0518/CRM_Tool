"""管理员枚举变更、统一快照和真实 CLI 写入边界的隔离回归测试。"""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

import app.smart_table.enums as enum_module
from app.ai.gateway import AIGateway
from app.ai.models import LLMRequest, LLMResponse
from app.ai.provider import MockLLMProvider
from app.core.config import Settings
from app.leads.remarks import RemarksBuilder
from app.leads.service import DeterministicFirstTextLeadExtractor
from app.smart_table.adapter import SmartTableActor
from app.smart_table.enums import EnumConfigurationError, EnumSnapshotService
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableFieldType, SmartTableOption, SmartTableSchema
from app.smart_table.readiness import SmartTableReadinessChecker
from app.smart_table.registry import build_required_smart_table_schema
from app.smart_table.wecom_cli import WecomCliSmartTableAdapter


def changed_options(field_name: str, options: tuple[str, ...]) -> SmartTableSchema:
    """替换指定字段的测试选项；返回完整结构且只模拟配置，不调用真实表格。"""
    schema = build_required_smart_table_schema()
    return replace(
        schema,
        fields=tuple(
            replace(
                field,
                options=tuple(
                    SmartTableOption(f"remote-{index}", value)
                    for index, value in enumerate(options)
                ),
            )
            if field.name == field_name
            else field
            for field in schema.fields
        ),
    )


def analysis_output(fields: dict[str, object], confidence: float = 0.95) -> str:
    """构造带逐字段置信度的 Mock 输出；返回 JSON，无外部副作用。"""
    return json.dumps(
        {
            "intent": "NEW_LEAD",
            "customer_reference": {},
            "crm_fields": fields,
            "confidence_by_field": {name: confidence for name in fields},
            "enrichment": {},
            "conflicts": [],
            "warnings": [],
        },
        ensure_ascii=False,
    )


def cli_adapter(schema: SmartTableSchema) -> WecomCliSmartTableAdapter:
    """构造不运行 CLI 的真实编码适配器；返回实例，加载器只读取传入结构。"""
    adapter = WecomCliSmartTableAdapter(
        doc_id="test-doc",
        sheet_id="test-sheet",
        sales_can_create_records=False,
        sales_can_delete_records=False,
    )
    adapter._enum_snapshots = EnumSnapshotService(lambda: schema)
    return adapter


@pytest.mark.parametrize(
    ("field_name", "options", "value", "source"),
    [
        ("客户行业", ("其他", "*半导体"), "半导体", "客户行业：半导体"),
        ("工艺", ("装配", "激光切割"), ["装配", "激光切割"], "工艺：装配和激光切割"),
        ("业务线", ("协作机器人", "移动机器人"), "移动机器人", "业务线：移动机器人"),
        ("线索来源", ("展会", "合作伙伴"), "合作伙伴", "线索来源：合作伙伴"),
        ("沟通方式", ("活动", "视频连线"), "视频连线", "沟通方式：视频连线"),
        ("客户级别", ("战略客户",), "战略客户", "客户级别：战略客户"),
    ],
)
def test_added_options_reach_ai_schema_validation_and_cli_encoding(
    field_name: str,
    options: tuple[str, ...],
    value: object,
    source: str,
) -> None:
    """验证六个动态字段新增选项进入模型契约、候选校验与真实 option ID 编码。"""
    adapter = cli_adapter(changed_options(field_name, options))
    provider = MockLLMProvider([analysis_output({field_name: value})])
    gateway = AIGateway(provider, enum_snapshots=adapter._enum_snapshots)
    with adapter._enum_snapshots.scope():
        result = gateway.extract_fields(source)
        encoded = adapter._to_cli_fields(result.fields, adapter.get_schema())
    assert result.fields[field_name] == value
    field_contract = provider.requests[0].json_schema["properties"]["crm_fields"]["properties"]
    contract = field_contract[field_name]
    assert provider.requests[0].json_schema["$defs"]["LeadSegmentAnalysis"]["properties"][
        "crm_fields"
    ]["properties"][field_name] == contract
    values = value if isinstance(value, list) else [value]
    allowed = contract["items"]["enum"] if field_name == "工艺" else contract["enum"]
    assert all(item in allowed for item in values)
    assert [item["id"] for item in encoded[field_name]] == [
        f"remote-{options.index('*' + item) if '*' + item in options else options.index(item)}"
        for item in values
    ]


def test_cache_expires_and_cli_refreshes_without_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    """用单调时钟验证 CLI 的唯一缓存到期刷新，不用睡眠或重启进程。"""
    now = [0.0]
    monkeypatch.setattr(enum_module, "monotonic", lambda: now[0])
    current = [changed_options("客户行业", ("其他", "机械加工"))]
    adapter = cli_adapter(current[0])
    adapter._enum_snapshots = EnumSnapshotService(lambda: current[0], ttl_seconds=300)
    first = adapter.get_schema()
    current[0] = changed_options("客户行业", ("其他", "半导体"))
    now[0] = 299
    assert adapter.get_schema() is first
    now[0] = 300
    assert adapter.get_schema() is current[0]
    assert adapter._enum_snapshots.get_snapshot().options["客户行业"] == ("其他", "半导体")


def test_processing_scope_pins_ai_and_write_until_next_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """模拟模型调用中 TTL 到期且管理员改名，同一处理继续用原快照，下次采用新快照。"""
    now = [0.0]
    monkeypatch.setattr(enum_module, "monotonic", lambda: now[0])
    current = [changed_options("客户行业", ("其他", "半导体"))]
    adapter = cli_adapter(current[0])
    adapter._enum_snapshots = EnumSnapshotService(lambda: current[0])

    class ChangingProvider(MockLLMProvider):
        """在返回模型结果前模拟管理员变更和过期时钟。"""

        def complete(self, request: LLMRequest, *, timeout_seconds: float) -> LLMResponse:
            """返回旧契约候选并改变只读测试配置；不访问网络或真实表格。"""
            now[0] = 301
            current[0] = changed_options("客户行业", ("其他", "芯片制造"))
            return super().complete(request, timeout_seconds=timeout_seconds)

    provider = ChangingProvider([analysis_output({"客户行业": "半导体"})])
    gateway = AIGateway(provider, enum_snapshots=adapter._enum_snapshots)
    with adapter._enum_snapshots.scope():
        before = adapter._enum_snapshots.get_snapshot()
        patch = gateway.extract_fields("客户行业：半导体")
        after = adapter._enum_snapshots.get_snapshot()
        assert before.version == after.version
        assert adapter._to_cli_fields(patch.fields, adapter.get_schema())["客户行业"] == [
            {"id": "remote-1", "text": "半导体"}
        ]
    refreshed = adapter._enum_snapshots.get_snapshot()
    assert refreshed.version != before.version
    with pytest.raises(ValueError, match="选择字段缺少选项"):
        adapter._to_cli_fields(patch.fields, adapter.get_schema())


def test_failed_refresh_does_not_fall_back_to_expired_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """刷新异常后拒绝旧选项，后续成功刷新可恢复；不保留失效合法性判断。"""
    now = [0.0]
    monkeypatch.setattr(enum_module, "monotonic", lambda: now[0])
    current: list[SmartTableSchema | Exception] = [build_required_smart_table_schema()]

    def load() -> SmartTableSchema:
        """读取测试配置或传播模拟外部异常；无真实网络副作用。"""
        if isinstance(current[0], Exception):
            raise current[0]
        return current[0]

    service = EnumSnapshotService(load)
    first = service.get_snapshot()
    now[0] = 301
    current[0] = TimeoutError("test timeout")
    with pytest.raises(TimeoutError):
        service.get_snapshot()
    current[0] = changed_options("客户行业", ("其他", "半导体"))
    assert service.get_snapshot().version != first.version


@pytest.mark.parametrize("field_name", ["业务线", "沟通方式", "客户行业"])
def test_missing_defaults_report_configuration_error_before_model(field_name: str) -> None:
    """默认选项被删除或改名时 readiness 未就绪，模型不被调用且不选第一个选项。"""
    schema = changed_options(field_name, ("管理员新名称",))
    service = EnumSnapshotService(lambda: schema)
    provider = MockLLMProvider([])
    with pytest.raises(EnumConfigurationError, match=f"字段枚举选项缺失：{field_name}"):
        AIGateway(provider, enum_snapshots=service).extract_fields("公司：测试公司")
    assert not provider.requests
    report = SmartTableReadinessChecker().check(MockSmartTableAdapter(schema=schema))
    assert not report.ready and any(field_name in issue for issue in report.issues)


@pytest.mark.parametrize("change", ["empty", "duplicate_option", "duplicate_field", "type"])
def test_invalid_configuration_fails_closed(change: str) -> None:
    """空选项、星号归一同名冲突及字段类型变化都拒绝进入 AI 处理。"""
    schema = build_required_smart_table_schema()
    field = schema.get_field("工艺")
    assert field is not None
    if change == "empty":
        schema = changed_options("工艺", ())
    elif change == "duplicate_option":
        schema = changed_options("工艺", ("装配", "*装配"))
    elif change == "duplicate_field":
        schema = replace(schema, fields=(*schema.fields, replace(field, name="*工艺")))
    else:
        schema = replace(
            schema,
            fields=tuple(
                replace(item, field_type=SmartTableFieldType.TEXT) if item.name == "工艺" else item
                for item in schema.fields
            ),
        )
    with pytest.raises(EnumConfigurationError):
        EnumSnapshotService(lambda: schema).get_snapshot()


def test_invalid_option_is_not_written_and_new_columns_are_not_ai_writable() -> None:
    """非法高置信度枚举保留待确认，新增任意列不进入 AI 白名单且编码拒绝非法值。"""
    schema = changed_options("客户级别", ("战略客户",))
    schema = replace(
        schema,
        fields=(
            *schema.fields,
            replace(schema.get_field("客户级别"), name="管理员自定义列", field_id="custom-column"),
        ),
    )
    adapter = cli_adapter(schema)
    provider = MockLLMProvider([analysis_output({"客户级别": "重点客户"})])
    gateway = AIGateway(provider, enum_snapshots=adapter._enum_snapshots)
    result = gateway.extract_fields("客户级别：重点客户")
    assert "客户级别" not in result.fields
    assert result.low_confidence_candidates["客户级别"] == "重点客户"
    assert (
        "管理员自定义列"
        not in provider.requests[0].json_schema["properties"]["crm_fields"]["properties"]
    )
    with pytest.raises(ValueError, match="选择字段缺少选项"):
        adapter._to_cli_fields({"客户级别": "重点客户"}, schema)


@pytest.mark.parametrize("confidence", [0.4, 0.7, 0.95])
def test_dynamic_option_still_requires_source_evidence(confidence: float) -> None:
    """新增行业无原文依据时不预填；明确字段证据按既有置信度维护待确认。"""
    schema = changed_options("客户行业", ("其他", "半导体"))
    provider = MockLLMProvider(
        [
            analysis_output({"客户行业": "半导体"}, confidence),
            analysis_output({"客户行业": "半导体"}, confidence),
        ]
    )
    gateway = AIGateway(provider, enum_snapshots=EnumSnapshotService(lambda: schema))
    assert "客户行业" not in gateway.extract_fields("公司：测试公司").fields
    valid = gateway.extract_fields("客户行业：半导体")
    assert valid.fields["客户行业"] == "半导体"
    assert ("客户行业" in valid.pending_confirmation_fields) == (confidence < 0.85)


def test_industry_word_in_company_name_is_not_industry_evidence() -> None:
    """公司名称含行业词不能代替字段证据，行业标签及明确经营表达仍可识别。"""
    schema = changed_options("客户行业", ("其他", "半导体"))
    provider = MockLLMProvider(
        [analysis_output({"客户行业": "半导体"}) for _ in range(3)]
    )
    gateway = AIGateway(provider, enum_snapshots=EnumSnapshotService(lambda: schema))

    company_only = gateway.extract_fields("西安芯汇半导体")
    labeled = gateway.extract_fields("客户行业：半导体")
    described = gateway.extract_fields("主要从事半导体制造")

    assert "客户行业" not in company_only.fields
    assert labeled.fields["客户行业"] == "半导体"
    assert described.fields["客户行业"] == "半导体"


@pytest.mark.parametrize(
    ("field_name", "options", "value"),
    [
        ("业务线", ("协作机器人", "移动机器人"), "移动机器人"),
        ("线索来源", ("展会", "合作伙伴"), "合作伙伴"),
        ("沟通方式", ("活动", "视频连线"), "视频连线"),
        ("客户行业", ("其他", "半导体"), "半导体"),
        ("客户级别", ("战略客户",), "战略客户"),
        ("工艺", ("激光切割",), ["激光切割"]),
    ],
)
def test_high_confidence_cannot_invent_any_dynamic_enum(
    field_name: str, options: tuple[str, ...], value: object
) -> None:
    """六个选择字段即使高置信度也必须有原文证据；返回补丁不含猜测值，无外部写入。"""
    schema = changed_options(field_name, options)
    gateway = AIGateway(
        MockLLMProvider([analysis_output({field_name: value})]),
        enum_snapshots=EnumSnapshotService(lambda: schema),
    )
    assert field_name not in gateway.extract_fields("公司：测试公司，联系人张经理").fields


def test_dynamic_other_option_and_demand_extraction_use_snapshot() -> None:
    """其他兜底只在当前枚举含其他时生效；确定性需求使用新工艺且备注支持新其他字段。"""
    schema = changed_options("工艺", ("激光切割", "其他"))
    provider = MockLLMProvider([analysis_output({"工艺": "微雕"}, 0.4)])
    gateway = AIGateway(provider, enum_snapshots=EnumSnapshotService(lambda: schema))
    result = gateway.extract_fields("工艺：微雕")
    assert result.fields["工艺"] == "其他" and result.enrichment["工艺"] == "微雕"
    extractor = DeterministicFirstTextLeadExtractor(("激光切割", "其他"))
    assert extractor.extract_patch("需求：激光切割")["工艺"] == ["激光切割"]
    assert "业务线：其他（定制机器人）" in RemarksBuilder().build(
        {"业务线": "其他"}, {"业务线": "定制机器人"}
    )


def test_cache_setting_defaults_and_rejects_zero() -> None:
    """部署 TTL 默认五分钟，零或负值不能关闭刷新边界。"""
    assert Settings(_env_file=None).smart_table_enum_cache_seconds == 300
    with pytest.raises(ValueError, match="SMART_TABLE_ENUM_CACHE_SECONDS"):
        Settings(_env_file=None, smart_table_enum_cache_seconds=0)
    with pytest.raises(ValueError):
        EnumSnapshotService(build_required_smart_table_schema, ttl_seconds=-1)


def test_deleted_source_default_is_left_empty_and_removed_alias_is_not_revived() -> None:
    """管理员删除展会和旧沟通选项后，不用第一个选项充当默认或通过别名复活删除值。"""
    schema = changed_options("线索来源", ("合作伙伴",))
    schema = replace(schema, fields=tuple(
        replace(field, options=(SmartTableOption("activity", "活动"),))
        if field.name == "沟通方式" else field for field in schema.fields
    ))
    service = EnumSnapshotService(lambda: schema)
    assert service.source_defaults() == {}
    provider = MockLLMProvider([analysis_output({"沟通方式": "电话"})])
    patch = AIGateway(provider, enum_snapshots=service).extract_fields("沟通方式：电话")
    assert "沟通方式" not in patch.fields


def test_other_fallback_is_disabled_when_administrator_deletes_other() -> None:
    """管理员删除工艺其他选项后，未知有证据候选仍只保留后台，不虚构其他选项。"""
    schema = changed_options("工艺", ("激光切割",))
    provider = MockLLMProvider([analysis_output({"工艺": "微雕"}, 0.4)])
    patch = AIGateway(provider, enum_snapshots=EnumSnapshotService(lambda: schema)).extract_fields(
        "工艺：微雕"
    )
    assert "工艺" not in patch.fields and patch.low_confidence_candidates["工艺"] == "微雕"


def test_refresh_does_not_change_historical_records() -> None:
    """管理员删除非默认选项并刷新后，既有销售记录不改名、不清空或自动改其他。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {"负责人": "sales-1", "客户行业": "医疗"}, actor=SmartTableActor.ROBOT
    )
    service = EnumSnapshotService(adapter.get_schema)
    service.get_snapshot()
    adapter._schema = changed_options("客户行业", ("其他", "半导体"))
    service._deadline = 0
    assert "医疗" not in service.get_snapshot().options["客户行业"]
    assert adapter.get_record(record.record_id) == record
