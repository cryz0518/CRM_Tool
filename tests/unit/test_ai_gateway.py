"""AI Gateway 结构化字段提取的应用边界测试。"""

from __future__ import annotations

import json

import pytest

from app.ai.gateway import AIGateway, BusinessValidationError
from app.ai.provider import MockLLMProvider


def valid_analysis(**changes: object) -> str:
    """构造可由模型返回的最小合法结构化线索结果。

    参数：changes 为需要覆盖的结构化字段。
    返回：JSON 字符串。
    异常：无。
    副作用：无。
    """
    payload: dict[str, object] = {
        "intent": "NEW_LEAD",
        "customer_reference": {"company_name": "长广溪智造"},
        "crm_fields": {"线索名称": "长广溪智造", "业务线": "协作机器人"},
        "enrichment": {},
        "confidence_by_field": {"线索名称": 0.9, "业务线": 0.7},
        "conflicts": [],
        "warnings": [],
    }
    payload.update(changes)
    return json.dumps(payload, ensure_ascii=False)


def test_gateway_repairs_only_one_malformed_structured_response() -> None:
    """验证 JSON 结构错误仅触发一次受约束修复且返回可消费字段补丁。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录两次请求，代表一次初始调用和一次结构修复。
    """
    provider = MockLLMProvider(responses=["not-json", valid_analysis()])

    result = AIGateway(provider).extract_fields("客户：长广溪智造；业务线：协作机器人")

    assert result.fields == {"线索名称": "长广溪智造", "业务线": "协作机器人"}
    assert result.pending_confirmation_fields == ("业务线",)
    assert result.low_confidence_candidates == {}
    assert provider.requests[1].repair_source == "not-json"
    assert len(provider.requests) == 2


def test_gateway_never_repairs_a_business_invalid_enum_value() -> None:
    """验证枚举业务校验失败直接拒绝，不能借模型再次改写业务事实。

    参数：无。
    返回：无。
    异常：BusinessValidationError 为受控业务校验结论。
    副作用：Mock Provider 仅收到初始提取请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis(crm_fields={"业务线": "未知机器人"})])

    with pytest.raises(BusinessValidationError, match="业务线"):
        AIGateway(provider).extract_fields("业务线：未知机器人")

    assert len(provider.requests) == 1


def test_gateway_keeps_low_confidence_value_out_of_formal_fields() -> None:
    """验证低置信度候选仅保留后台结果，不污染正式字段。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录一次字段提取请求。
    """
    provider = MockLLMProvider(
        responses=[
            valid_analysis(
                crm_fields={"线索名称": "长广溪智造", "联系人": "张三"},
                confidence_by_field={"线索名称": 0.9, "联系人": 0.59},
            )
        ]
    )

    result = AIGateway(provider).extract_fields("客户可能是长广溪智造，联系人可能叫张三")

    assert result.fields == {"线索名称": "长广溪智造"}
    assert result.pending_confirmation_fields == ()
    assert result.low_confidence_candidates == {"联系人": "张三"}
