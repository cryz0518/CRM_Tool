"""AI Gateway 结构化字段提取的应用边界测试。"""

from __future__ import annotations

import json
import logging

import pytest

from app.ai.gateway import AIGateway, BusinessValidationError, FailedStructuredOutputError
from app.ai.models import LLMResponse
from app.ai.provider import LLMProviderError, MockLLMProvider


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
    assert len(provider.requests[1].messages) == 2
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


def test_gateway_keeps_two_failed_outputs_for_pending_review() -> None:
    """验证两次结构失败可被任务层以失败待处理状态完整审计。

    参数：无。
    返回：无。
    异常：FailedStructuredOutputError 为受控的待人工处理结论。
    副作用：Mock Provider 完成一次初始调用和一次受约束结构修复。
    """
    provider = MockLLMProvider(responses=["first-invalid", "second-invalid"])

    with pytest.raises(FailedStructuredOutputError) as captured:
        AIGateway(provider).extract_fields("客户信息")

    assert captured.value.task_status == "failed_pending_review"
    assert captured.value.original_output == "first-invalid"
    assert captured.value.repaired_output == "second-invalid"
    assert len(provider.requests) == 2


def test_gateway_masks_unrelated_sensitive_values_before_provider_call() -> None:
    """验证身份证、银行卡、密码和验证码不会作为字段提取输入发送给 Provider。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录已脱敏的最小化请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis()])

    AIGateway(provider).extract_fields(
        "客户：长广溪；身份证110101199001011234；银行卡6222021234567890123；"
        "密码: secret；验证码 123456"
    )

    sent_text = provider.requests[0].messages[1]["content"]
    assert "110101199001011234" not in sent_text
    assert "6222021234567890123" not in sent_text
    assert "secret" not in sent_text
    assert "123456" not in sent_text


def test_gateway_records_retry_and_call_volume_without_raw_content(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """验证传输重试和调用量日志保留追踪元数据而不暴露模型内容。

    参数：caplog 为 pytest 注入的日志捕获器。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 首次抛出传输错误，网关记录一次重试后成功。
    """
    caplog.set_level(logging.INFO, logger="app.ai.gateway")
    provider = MockLLMProvider(
        responses=[
            LLMProviderError("temporary"),
            LLMResponse(content=valid_analysis(), input_tokens=12, output_tokens=7),
        ]
    )

    AIGateway(provider).extract_fields("客户：长广溪智造")

    retry_record = next(
        record for record in caplog.records if record.message == "ai_gateway_retry"
    )
    complete_record = next(
        record for record in caplog.records if record.message == "ai_gateway_call"
    )
    assert retry_record.target == "MockLLMProvider"
    assert retry_record.attempt == 1
    assert complete_record.ai_call_count == 2
    assert complete_record.ai_input_tokens == 12
    assert complete_record.ai_output_tokens == 7
    assert "长广溪智造" not in caplog.text
