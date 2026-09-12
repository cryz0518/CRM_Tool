"""冻结备注模板的确定性生成测试。"""

from app.leads.remarks import RemarksBuilder


def test_builder_generates_four_sections_from_reviewed_fields_and_evidence() -> None:
    """验证已审核字段和原文证据生成固定四段式备注。"""
    remark = RemarksBuilder().build(
        {
            "线索名称": "星海验收科技有限公司",
            "客户行业": "机械加工",
            "业务线": "协作机器人",
            "工艺": "装配",
        },
        {"客户需求/痛点": "客户希望使用协作机器人完成装配"},
    )

    assert remark == (
        "基本信息：星海验收科技有限公司，所属行业为机械加工；城市、主要产品、年销售额未提供。\n"
        "线索需求：客户希望使用协作机器人完成装配。\n"
        "预算情况：未提供。\n"
        "特殊要求：未提供。"
    )


def test_builder_marks_absent_evidence_as_not_provided_without_invention() -> None:
    """验证缺少城市、产品、销售额、预算及特殊要求时不杜撰。"""
    remark = RemarksBuilder().build(
        {"线索名称": "星海验收科技有限公司", "客户行业": "机械加工"}, {}
    )

    assert remark == (
        "基本信息：星海验收科技有限公司，所属行业为机械加工；城市、主要产品、年销售额未提供。\n"
        "线索需求：未提供。\n"
        "预算情况：未提供。\n"
        "特殊要求：未提供。"
    )
