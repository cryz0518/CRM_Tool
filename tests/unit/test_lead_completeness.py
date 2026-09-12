"""线索业务字段完整度的领域服务测试。"""

from app.leads.completeness import LeadCompletenessService


def test_empty_confirmation_list_does_not_hide_missing_required_fields() -> None:
    """验证 AI待确认 为空时，空的正式必填字段仍明确缺失。"""
    result = LeadCompletenessService().evaluate(
        {
            "业务线": "协作机器人",
            "线索名称": "星海验收科技有限公司",
            "线索来源": "展会",
            "联系人": "张三",
            "职务": "采购经理",
            "手机": "13800138000",
            "沟通方式": "",
            "备注": "",
            "AI待确认": [],
        }
    )

    assert result.missing_required_fields == ("沟通方式", "备注")
    assert result.pending_confirmation_fields == ()


def test_medium_confidence_formal_value_is_pending_not_missing() -> None:
    """验证已预填且列入 AI待确认 的字段只属于待确认。"""
    result = LeadCompletenessService().evaluate(
        {
            "业务线": "协作机器人",
            "线索名称": "星海验收科技有限公司",
            "线索来源": "展会",
            "联系人": "张三",
            "职务": "采购经理",
            "沟通方式": "微信",
            "手机": "13800138000",
            "备注": "已生成",
            "AI待确认": ["沟通方式"],
        }
    )

    assert result.missing_required_fields == ()
    assert result.pending_confirmation_fields == ("沟通方式",)


def test_low_confidence_backend_candidate_does_not_fill_required_field() -> None:
    """验证仅在后台的低置信度候选不会改变正式字段完整度。"""
    result = LeadCompletenessService().evaluate(
        {
            "业务线": "协作机器人",
            "线索名称": "星海验收科技有限公司",
            "线索来源": "展会",
            "联系人": "张三",
            "职务": "采购经理",
            "沟通方式": "",
            "手机": "13800138000",
            "备注": "已生成",
            "AI待确认": [],
        },
        low_confidence_candidates={"沟通方式": "微信"},
    )

    assert result.missing_required_fields == ("沟通方式",)
    assert result.pending_confirmation_fields == ()
