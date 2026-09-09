"""智能表格适配器契约与就绪检查测试。"""

from __future__ import annotations

import asyncio

import httpx

from app.smart_table.adapter import SmartTableActor
from app.smart_table.dependencies import get_smart_table_adapter
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import (
    SmartTableField,
    SmartTableFieldType,
    SmartTableOption,
    SmartTableSchema,
)
from app.smart_table.readiness import SmartTableReadinessChecker
from app.smart_table.registry import build_required_smart_table_schema
from app.smart_table.unconfigured import UnconfiguredSmartTableAdapter


def test_mock_adapter_reads_and_applies_only_the_requested_field_patch() -> None:
    """验证业务服务可通过稳定契约创建、读取、查询并增量更新 Mock 记录。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    record = adapter.create_record(
        {"线索名称": "长广溪智造", "联系人": "张三", "负责人": "sales-001"},
        actor=SmartTableActor.ROBOT,
    )
    updated_record = adapter.update_record(record.record_id, {"工艺": "装配"})

    assert adapter.get_record(record.record_id) == updated_record
    assert updated_record.fields == {
        "线索名称": "长广溪智造",
        "联系人": "张三",
        "负责人": "sales-001",
        "工艺": "装配",
    }
    assert adapter.find_records({"线索名称": "长广溪智造"}) == [updated_record]
    assert adapter.get_records() == [updated_record]


def test_mock_schema_keeps_administrator_field_and_option_identifiers() -> None:
    """验证稳定契约保留字段与枚举的底层标识，供未来 CLI/API 适配器映射。"""
    schema = build_required_smart_table_schema()
    business_line = schema.get_field("业务线")

    assert business_line is not None
    assert business_line.field_id == "mock-field-1"
    assert business_line.options[0] == SmartTableOption(
        option_id="mock-field-1-option-1", name="协作机器人"
    )


def test_mock_records_robot_create_poc_when_sales_create_permission_is_disabled() -> None:
    """验证关闭销售新增权限后，机器人仍可新增并写入负责人的 PoC 语义。"""
    adapter = MockSmartTableAdapter(
        schema=build_required_smart_table_schema(),
        sales_can_create_records=False,
    )

    created_record = adapter.create_record(
        {"线索名称": "长广溪智造", "负责人": "sales-001"},
        actor=SmartTableActor.ROBOT,
    )

    assert created_record.fields["负责人"] == "sales-001"
    assert adapter.sales_can_create_records is False


def test_readiness_rejects_missing_required_field_wrong_type_and_missing_enum_option() -> None:
    """验证核心字段、AI待确认字段类型和枚举选项异常会使服务未就绪。"""
    # 先删除权限关键字段，构造管理员漏配核心字段的情形。
    fields = list(build_required_smart_table_schema().fields)
    fields = [field for field in fields if field.name != "负责人"]
    # 再把 AI待确认故意改成文本类型，验证字段类型偏差不会被静默接受。
    fields = [
        SmartTableField(
            field_id=field.field_id,
            name=field.name,
            field_type=SmartTableFieldType.TEXT,
            options=field.options,
        )
        if field.name == "AI待确认"
        else field
        for field in fields
    ]
    # 最后删去业务线的一个合法选项，验证枚举完整性检查。
    fields = [
        SmartTableField(
            field_id=field.field_id,
            name=field.name,
            field_type=field.field_type,
            options=tuple(option for option in field.options if option.name != "车载机器人"),
        )
        if field.name == "业务线"
        else field
        for field in fields
    ]
    report = SmartTableReadinessChecker().check(
        MockSmartTableAdapter(schema=SmartTableSchema(fields=tuple(fields)))
    )

    assert report.ready is False
    assert "缺少必需字段：负责人" in report.issues
    assert "字段类型不匹配：AI待确认，期望 MULTI_SELECT，实际 TEXT" in report.issues
    assert "字段枚举选项缺失：业务线，缺少 车载机器人" in report.issues


def test_readiness_rejects_an_adapter_that_has_not_been_configured() -> None:
    """验证未配置真实智能表格适配器时，服务不会被误判为已就绪。"""
    report = SmartTableReadinessChecker().check(UnconfiguredSmartTableAdapter())

    assert report.ready is False
    assert report.issues == ("智能表格适配器未配置：需要部署真实 CLI/API 适配器或显式启用 Mock",)


def test_readiness_rejects_plain_text_for_the_email_field() -> None:
    """验证管理员将邮箱配置为普通文本时，readiness 明确拒绝该核心字段类型。"""
    # 仅篡改邮箱字段类型，其余管理员预配置保持完整，以隔离这一项配置错误。
    fields = [
        SmartTableField(
            field_id=field.field_id,
            name=field.name,
            field_type=SmartTableFieldType.TEXT,
            options=field.options,
        )
        if field.name == "邮箱"
        else field
        for field in build_required_smart_table_schema().fields
    ]
    report = SmartTableReadinessChecker().check(
        MockSmartTableAdapter(schema=SmartTableSchema(fields=tuple(fields)))
    )

    assert report.ready is False
    assert "字段类型不匹配：邮箱，期望 EMAIL，实际 TEXT" in report.issues


def test_readiness_endpoint_returns_configuration_issues() -> None:
    """验证 readiness 接口将智能表格配置问题以 503 和中文详情返回。"""
    from app.main import app

    invalid_adapter = MockSmartTableAdapter(schema=SmartTableSchema(fields=()))
    # 覆盖应用依赖，让 HTTP 缝稳定复现管理员未配置任何字段的状态。
    app.dependency_overrides[get_smart_table_adapter] = lambda: invalid_adapter
    try:
        async def request_readiness() -> httpx.Response:
            """通过 ASGI 调用 readiness 接口，避免启动真实网络服务。"""
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                return await client.get("/health/ready")

        response = asyncio.run(request_readiness())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["issues"] == [
        "缺少必需字段：业务线",
        "缺少必需字段：线索名称",
        "缺少必需字段：线索来源",
        "缺少必需字段：联系人",
        "缺少必需字段：职务",
        "缺少必需字段：沟通方式",
        "缺少必需字段：手机",
        "缺少必需字段：电话",
        "缺少必需字段：邮箱",
        "缺少必需字段：客户行业",
        "缺少必需字段：客户级别",
        "缺少必需字段：工艺",
        "缺少必需字段：下次联系时间",
        "缺少必需字段：附件",
        "缺少必需字段：备注",
        "缺少必需字段：地区定位",
        "缺少必需字段：AI待确认",
        "缺少必需字段：创建人",
        "缺少必需字段：负责人",
    ]
