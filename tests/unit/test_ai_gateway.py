"""AI Gateway 结构化字段提取的应用边界测试。"""

from __future__ import annotations

import json
import logging

import pytest

from app.ai.gateway import AIGateway, BusinessValidationError, FailedStructuredOutputError
from app.ai.models import LLMResponse
from app.ai.provider import LLMProviderError, MockLLMProvider
from app.smart_table.registry import COMMUNICATION_METHOD_OPTIONS, CRM_BUSINESS_FIELD_NAMES


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


def test_gateway_accepts_registered_communication_method() -> None:
    """验证有明确原文证据的注册沟通方式可通过 Gateway。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录一次字段提取请求。
    """
    provider = MockLLMProvider(
        responses=[
            valid_analysis(
                crm_fields={"沟通方式": "微信"},
                confidence_by_field={"沟通方式": 0.95},
            )
        ]
    )

    result = AIGateway(provider).extract_fields("后续通过微信联系客户")

    assert result.fields == {"沟通方式": "微信"}


def test_gateway_rejects_unregistered_communication_method_without_alias_conversion() -> None:
    """验证未注册沟通描述被严格拒绝，不能静默映射为任一枚举值。

    参数：无。
    返回：无。
    异常：BusinessValidationError 为受控业务校验结论。
    副作用：Mock Provider 仅收到初始提取请求，不触发模型重试或别名转换。
    """
    provider = MockLLMProvider(
        responses=[
            valid_analysis(
                crm_fields={"沟通方式": "后续沟通"},
                confidence_by_field={"沟通方式": 0.95},
            )
        ]
    )

    with pytest.raises(BusinessValidationError, match="枚举值不合法：沟通方式"):
        AIGateway(provider).extract_fields("后续沟通")

    assert len(provider.requests) == 1


def test_gateway_prompt_requires_registered_chinese_fields_and_scalar_values() -> None:
    """验证初始提示明确要求注册表中文字段名、标量值及枚举选项。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录一次字段提取请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis()])

    AIGateway(provider).extract_fields("客户：长广溪智造")

    prompt = provider.requests[0].messages[0]["content"]
    assert all(field_name in prompt for field_name in CRM_BUSINESS_FIELD_NAMES)
    assert "禁止使用英文或其他别名（例如 phone）" in prompt
    assert "每个 value 必须是单个字符串" in prompt
    assert "不得使用数组塞入 CRM 字段" in prompt
    assert "confidence_by_field 的 key 必须与 crm_fields 的 key 完全一一对应" in prompt
    assert "业务线：协作机器人、车载机器人" in prompt


def test_gateway_prompt_maps_input_labels_to_canonical_crm_field_names() -> None:
    """验证提示将公司和个人自然语言标签约束为冻结 CRM 字段名。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录一次字段提取请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis()])

    AIGateway(provider).extract_fields("星海验收科技有限公司，联系人王验收")

    prompt = provider.requests[0].messages[0]["content"]
    assert "公司名称/企业名称 -> 线索名称" in prompt
    assert "客户名称（个人语境）-> 联系人" in prompt
    assert "联系人姓名 -> 联系人" in prompt
    assert "手机号 -> 手机" in prompt
    assert "禁止输出 JSON key：客户名称、公司名称、企业名称、联系人姓名、手机号" in prompt
    assert "禁止输出 JSON key：备注、comment、remark、notes、description" in prompt
    assert "AI待确认、缺失字段、审核状态、provenance 或其他系统计算字段" in prompt
    assert prompt.rfind("禁止输出 JSON key") > prompt.rfind("必须符合此 JSON Schema")


def test_gateway_schema_restricts_model_output_keys_to_extractable_fields() -> None:
    """验证 Gateway 交给 Provider 的 schema 排除备注和未注册字段。"""
    provider = MockLLMProvider(responses=[valid_analysis()])

    AIGateway(provider).extract_fields("客户：长广溪智造")

    schema = provider.requests[0].json_schema
    crm_fields = schema["properties"]["crm_fields"]  # type: ignore[index]
    confidence_by_field = schema["properties"]["confidence_by_field"]  # type: ignore[index]
    enrichment = schema["properties"]["enrichment"]  # type: ignore[index]
    assert crm_fields["additionalProperties"] is False  # type: ignore[index]
    assert "备注" not in crm_fields["properties"]  # type: ignore[index]
    assert crm_fields["properties"]["沟通方式"]["enum"] == list(  # type: ignore[index]
        COMMUNICATION_METHOD_OPTIONS
    )
    assert confidence_by_field["additionalProperties"] is False  # type: ignore[index]
    for field_name in crm_fields["properties"]:  # type: ignore[index]
        assert {  # type: ignore[comparison-overlap]
            "if": {
                "required": ["crm_fields"],
                "properties": {"crm_fields": {"required": [field_name]}},
            },
            "then": {
                "required": ["confidence_by_field"],
                "properties": {"confidence_by_field": {"required": [field_name]}},
            },
        } in schema["allOf"]  # type: ignore[index]
    assert enrichment["additionalProperties"] is False  # type: ignore[index]
    assert set(enrichment["properties"]) == {  # type: ignore[index]
        "城市/地区",
        "主营产品",
        "年销售额",
        "客户需求/痛点",
        "预算",
        "特殊要求",
    }


def test_gateway_accepts_crm_field_with_matching_confidence() -> None:
    """验证 CRM 建议字段与同名合法置信度可共同通过 Gateway 校验。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录一次字段提取请求。
    """
    provider = MockLLMProvider(
        responses=[
            valid_analysis(
                crm_fields={"线索名称": "星海验收科技有限公司"},
                confidence_by_field={"线索名称": 0.95},
            )
        ]
    )

    result = AIGateway(provider).extract_fields("客户：星海验收科技有限公司")

    assert result.fields == {"线索名称": "星海验收科技有限公司"}
    assert result.pending_confirmation_fields == ()


def test_gateway_rejects_crm_field_without_matching_confidence() -> None:
    """验证缺少同名置信度的 CRM 建议字段仍会被严格拒绝。

    参数：无。
    返回：无。
    异常：BusinessValidationError 为受控业务校验结论。
    副作用：Mock Provider 仅收到初始提取请求，不触发模型重试。
    """
    provider = MockLLMProvider(
        responses=[
            valid_analysis(
                crm_fields={"线索名称": "星海验收科技有限公司"},
                confidence_by_field={},
            )
        ]
    )

    with pytest.raises(BusinessValidationError, match="置信度缺失或不合法：线索名称"):
        AIGateway(provider).extract_fields("客户：星海验收科技有限公司")

    assert len(provider.requests) == 1


def test_gateway_accepts_company_and_contact_in_canonical_fields() -> None:
    """验证公司主体和联系人分别使用线索名称、联系人两个规范字段。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录一次字段提取请求。
    """
    response = valid_analysis(
        crm_fields={"线索名称": "星海验收科技有限公司", "联系人": "王验收"},
        enrichment={"客户需求/痛点": "需要协作机器人完成装配"},
        confidence_by_field={"线索名称": 0.9, "联系人": 0.9},
    )
    provider = MockLLMProvider(responses=[response])

    result = AIGateway(provider).extract_fields(
        "星海验收科技有限公司，联系人王验收，需要协作机器人完成装配"
    )

    assert result.fields == {"线索名称": "星海验收科技有限公司", "联系人": "王验收"}
    assert result.enrichment == {"客户需求/痛点": "需要协作机器人完成装配"}
    assert len(provider.requests) == 1


def test_gateway_rejects_customer_name_without_alias_conversion() -> None:
    """验证未注册的客户名称不静默转换为联系人或线索名称。

    参数：无。
    返回：无。
    异常：BusinessValidationError 为受控的字段白名单失败结论。
    副作用：Mock Provider 仅收到初始提取请求。
    """
    response = valid_analysis(
        crm_fields={"客户名称": "王验收"}, confidence_by_field={"客户名称": 0.9}
    )
    provider = MockLLMProvider(responses=[response])

    with pytest.raises(BusinessValidationError, match="未知 CRM 字段：客户名称"):
        AIGateway(provider).extract_fields("客户名称王验收")

    assert len(provider.requests) == 1


def test_gateway_rejects_remarks_without_alias_conversion() -> None:
    """验证模型备注既不能绕过 T09，也不会被转换为补充信息。"""
    response = valid_analysis(
        crm_fields={"备注": "客户预算充足"}, confidence_by_field={"备注": 0.9}
    )
    provider = MockLLMProvider(responses=[response])

    with pytest.raises(BusinessValidationError, match="AI 禁止输出字段：备注"):
        AIGateway(provider).extract_fields("客户预算充足")

    assert len(provider.requests) == 1


def test_gateway_rejects_array_crm_field_values_without_conversion() -> None:
    """验证数组 CRM 值经一次结构修复后仍被拒绝，不能静默拼接为字符串。

    参数：无。
    返回：无。
    异常：FailedStructuredOutputError 为受控的结构失败结论。
    副作用：Mock Provider 完成一次初始提取和一次结构修复请求。
    """
    invalid = valid_analysis(
        crm_fields={"手机": ["13800138000", "13900139000"]},
        confidence_by_field={"手机": 0.9},
    )
    provider = MockLLMProvider(responses=[invalid, invalid])

    with pytest.raises(FailedStructuredOutputError):
        AIGateway(provider).extract_fields("客户电话已提供")

    assert len(provider.requests) == 2


def test_gateway_keeps_english_phone_alias_as_business_validation_failure() -> None:
    """验证英文 phone 别名仍由业务白名单拒绝，禁止自动映射为手机。

    参数：无。
    返回：无。
    异常：BusinessValidationError 为受控的字段白名单失败结论。
    副作用：Mock Provider 只收到初始请求，不触发结构修复。
    """
    response = valid_analysis(
        crm_fields={"phone": "13800138000"}, confidence_by_field={"phone": 0.9}
    )
    provider = MockLLMProvider(responses=[response])

    with pytest.raises(BusinessValidationError, match="phone"):
        AIGateway(provider).extract_fields("客户电话已提供")

    assert len(provider.requests) == 1


def test_gateway_accepts_registered_chinese_phone_field() -> None:
    """验证合法中文注册表字段及其标量值可通过既有确定性校验。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录一次字段提取请求。
    """
    response = valid_analysis(
        crm_fields={"手机": "13800138000"}, confidence_by_field={"手机": 0.9}
    )
    provider = MockLLMProvider(responses=[response])

    result = AIGateway(provider).extract_fields("客户电话已提供")

    assert result.fields == {"手机": "13800138000"}


def test_gateway_repair_prompt_keeps_crm_field_and_value_type_constraints() -> None:
    """验证结构修复提示重复字段白名单、标量值和置信度键集约束。

    参数：无。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：Mock Provider 记录初始请求与一次结构修复请求。
    """
    provider = MockLLMProvider(responses=["not-json", valid_analysis()])

    AIGateway(provider).extract_fields("客户：长广溪智造")

    repair_prompt = provider.requests[1].messages[0]["content"]
    assert all(field_name in repair_prompt for field_name in CRM_BUSINESS_FIELD_NAMES)
    assert "禁止使用英文或其他别名（例如 phone）" in repair_prompt
    assert "每个 value 必须是单个字符串" in repair_prompt
    assert "不得使用数组塞入 CRM 字段" in repair_prompt
    assert "confidence_by_field 的 key 必须与 crm_fields 的 key 完全一一对应" in repair_prompt
    assert "业务线：协作机器人、车载机器人" in repair_prompt


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


def test_gateway_keeps_ambiguous_follow_up_communication_out_of_formal_fields() -> None:
    """验证“后续沟通”不构成沟通方式枚举的明确证据。"""
    provider = MockLLMProvider(
        responses=[
            valid_analysis(
                crm_fields={"沟通方式": "微信"}, confidence_by_field={"沟通方式": 0.9}
            )
        ]
    )

    result = AIGateway(provider).extract_fields("展会认识，之后详细沟通")

    assert result.fields == {}
    assert len(provider.requests) == 1


def test_gateway_accepts_explicit_communication_and_evidence_enrichment_in_one_call() -> None:
    """验证明确沟通方式和限定补充信息均在首次提取调用中返回。"""
    provider = MockLLMProvider(
        responses=[
            valid_analysis(
                crm_fields={"沟通方式": "打电话"},
                enrichment={"预算": "50 万元", "客户需求/痛点": "需要完成装配"},
                confidence_by_field={"沟通方式": 0.9},
            )
        ]
    )

    result = AIGateway(provider).extract_fields("电话沟通，预算 50 万元，需要完成装配")

    assert result.fields == {"沟通方式": "打电话"}
    assert result.enrichment == {"预算": "50 万元", "客户需求/痛点": "需要完成装配"}
    assert len(provider.requests) == 1


def test_gateway_rejects_enrichment_without_verbatim_source_evidence() -> None:
    """验证补充信息不是原文片段时不能进入备注生成链路。"""
    provider = MockLLMProvider(
        responses=[valid_analysis(enrichment={"预算": "预算充足"})]
    )

    with pytest.raises(BusinessValidationError, match="补充信息缺少原文证据"):
        AIGateway(provider).extract_fields("客户希望后续沟通")

    assert len(provider.requests) == 1


def test_gateway_classifies_budget_without_generating_annual_sales() -> None:
    """验证预算金额不会仅因金额本身被归类为年销售额。

    参数：无。返回值：无。异常：断言失败时由 pytest 报告。副作用：Mock Provider 记录一次提取请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis(enrichment={"预算": "50万元"})])

    result = AIGateway(provider).extract_fields("预算50万元")

    assert result.enrichment == {"预算": "50万元"}
    assert "年销售额" not in result.enrichment


def test_gateway_classifies_annual_sales_only_with_explicit_scale_expression() -> None:
    """验证明确年销售额表达才进入年销售额补充信息。

    参数：无。返回值：无。异常：断言失败时由 pytest 报告。副作用：Mock Provider 记录一次提取请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis(enrichment={"年销售额": "50万元"})])

    result = AIGateway(provider).extract_fields("年销售额50万元")

    assert result.enrichment == {"年销售额": "50万元"}


def test_gateway_keeps_budget_and_annual_sales_as_separate_facts() -> None:
    """验证同一消息中的预算与年销售额按各自明确表达分别保留。

    参数：无。返回值：无。异常：断言失败时由 pytest 报告。副作用：Mock Provider 记录一次提取请求。
    """
    provider = MockLLMProvider(
        responses=[valid_analysis(enrichment={"预算": "50万元", "年销售额": "100万元"})]
    )

    result = AIGateway(provider).extract_fields("预算50万元，年销售额100万元")

    assert result.enrichment == {"预算": "50万元", "年销售额": "100万元"}


def test_gateway_rejects_budget_amount_misclassified_as_annual_sales() -> None:
    """验证金额没有经营规模表达时不能进入年销售额。

    参数：无。返回值：无。异常：BusinessValidationError 为受控的分类校验结论。
    副作用：Mock Provider 记录一次提取请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis(enrichment={"年销售额": "50万元"})])

    with pytest.raises(BusinessValidationError, match="年销售额缺少明确经营规模表达"):
        AIGateway(provider).extract_fields("预算50万元")


def test_gateway_rejects_adjacent_budget_amount_misclassified_as_annual_sales() -> None:
    """验证无标点相邻事实中的预算金额不能被年销售额关键词错误绑定。

    参数：无。返回值：无。异常：BusinessValidationError 为受控的分类校验结论。
    副作用：Mock Provider 记录一次提取请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis(enrichment={"年销售额": "50万元"})])

    with pytest.raises(BusinessValidationError, match="年销售额缺少明确经营规模表达"):
        AIGateway(provider).extract_fields("预算50万元 年销售额100万元")


def test_gateway_rejects_adjacent_annual_sales_misclassified_as_budget() -> None:
    """验证无标点相邻事实中的年销售额不能被预算关键词错误绑定。

    参数：无。返回值：无。异常：BusinessValidationError 为受控的分类校验结论。
    副作用：Mock Provider 记录一次提取请求。
    """
    provider = MockLLMProvider(responses=[valid_analysis(enrichment={"预算": "100万元"})])

    with pytest.raises(BusinessValidationError, match="预算缺少明确预算表达"):
        AIGateway(provider).extract_fields("预算50万元 年销售额100万元")


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
