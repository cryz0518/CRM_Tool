"""统一执行脱敏、追踪、重试与确定性校验的 AI Gateway。"""

from __future__ import annotations

import json
import logging
import re
import time
from uuid import uuid4

from pydantic import ValidationError

from app.ai.models import ExtractedLeadPatch, LeadAnalysis, LLMRequest, LLMResponse
from app.ai.provider import LLMProvider, LLMProviderError
from app.smart_table.registry import (
    BUSINESS_LINE_OPTIONS,
    COMMUNICATION_METHOD_OPTIONS,
    CRM_BUSINESS_FIELD_NAMES,
    CUSTOMER_INDUSTRY_OPTIONS,
    CUSTOMER_LEVEL_OPTIONS,
    LEAD_SOURCE_OPTIONS,
    PROCESS_OPTIONS,
)

logger = logging.getLogger(__name__)

_ENUM_OPTIONS = {
    "业务线": BUSINESS_LINE_OPTIONS,
    "线索来源": LEAD_SOURCE_OPTIONS,
    "沟通方式": COMMUNICATION_METHOD_OPTIONS,
    "客户行业": CUSTOMER_INDUSTRY_OPTIONS,
    "客户级别": CUSTOMER_LEVEL_OPTIONS,
    "工艺": PROCESS_OPTIONS,
}
_PHONE_PATTERN = re.compile(r"^\+?[0-9][0-9 -]{5,24}$")
_EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_SENSITIVE_PATTERN = re.compile(
    r"(?i)(?:(?<!\d)(?:\d{17}[\dXx]|\d{16,19}|\d{15})(?!\d)|"
    r"(?:密码|口令|验证码|password|token)\s*[:：]?\s*[^\s；;，,]+)"
)


class AIGatewayError(RuntimeError):
    """表示网关已经完成重试和错误归一化后的失败结论。"""


class BusinessValidationError(AIGatewayError):
    """表示结构合法但不满足 CRM 字段业务规则的候选。"""


class FailedStructuredOutputError(AIGatewayError):
    """承载待人工审查所需的两次模型输出和结构校验摘要。"""

    def __init__(self, original_output: str, repaired_output: str, error_summary: str) -> None:
        """保存失败事实，供任务层以 failed_pending_review 状态持久化审计。

        参数：original_output 与 repaired_output 为两次原始结果；error_summary 为校验摘要。
        返回：无。
        异常：无。
        副作用：异常对象保留待持久化的失败事实，日志不输出原文。
        """
        super().__init__("ai_structured_output_failed_pending_review")
        self.original_output = original_output
        self.repaired_output = repaired_output
        self.error_summary = error_summary
        self.task_status = "failed_pending_review"


class AIGateway:
    """封装 LLM 调用，使业务层不接触供应商、原始输出或敏感日志。"""

    def __init__(
        self,
        provider: LLMProvider,
        *,
        timeout_seconds: float = 20.0,
        retry_count: int = 1,
        high_confidence_threshold: float = 0.85,
        medium_confidence_threshold: float = 0.60,
    ) -> None:
        """注入 Provider 与冻结为配置的调用、置信度参数。

        参数：provider 为可替换模型边界；其余参数控制传输重试和候选分级。
        返回：无。
        异常：阈值或重试数非法时抛出 ValueError。
        副作用：仅保存依赖和配置。
        """
        valid_thresholds = 0 <= medium_confidence_threshold <= high_confidence_threshold <= 1
        if retry_count < 0 or not valid_thresholds:
            raise ValueError("AI Gateway 配置非法")
        self._provider = provider
        self._timeout_seconds = timeout_seconds
        self._retry_count = retry_count
        self._high_confidence_threshold = high_confidence_threshold
        self._medium_confidence_threshold = medium_confidence_threshold

    def extract_fields(self, text: str) -> ExtractedLeadPatch:
        """提取一条文本的字段补丁并完成 Parse、Schema 与 Business Validation。

        参数：text 为已持久化消息中本次字段提取必要的文本。
        返回：正式字段、中置信度待确认字段及低置信度后台候选。
        异常：传输/结构二次失败时抛出 AIGatewayError；业务校验失败抛出 BusinessValidationError。
        副作用：调用 Provider，输出不含原文和联系方式的结构化日志。
        """
        trace_id = str(uuid4())
        # 只将字段提取所需文本交给模型，并在发送前移除无关的高敏感信息。
        safe_text = _SENSITIVE_PATTERN.sub("[已遮蔽敏感号码]", text)
        json_schema = LeadAnalysis.model_json_schema()
        request = LLMRequest(
            messages=self._messages(safe_text, json_schema), json_schema=json_schema
        )
        started_at = time.monotonic()
        try:
            # 先取得可观测响应，确保 repair 与传输重试也计入调用量。
            response, attempts = self._call_with_transport_retry(request, trace_id)
            analysis, repair_response, repair_attempts = self._parse_schema_or_repair(
                request, response, trace_id
            )
            self._validate_business(analysis)
            fields, pending, low_candidates = self._apply_confidence(analysis)
        except AIGatewayError as error:
            self._log(trace_id, started_at, "failed", error_type=type(error).__name__)
            raise
        # 只写统计量，既可定位成本和重试，又不会把客户文本写入日志。
        all_responses = (response,) + ((repair_response,) if repair_response is not None else ())
        self._log(
            trace_id,
            started_at,
            "succeeded",
            call_count=attempts + repair_attempts,
            input_tokens=sum(item.input_tokens or 0 for item in all_responses),
            output_tokens=sum(item.output_tokens or 0 for item in all_responses),
        )
        return ExtractedLeadPatch(trace_id, analysis, fields, pending, low_candidates)

    def _call_with_transport_retry(
        self, request: LLMRequest, trace_id: str
    ) -> tuple[LLMResponse, int]:
        """仅针对 Provider 传输故障执行配置次数的重试。

        参数：request 为单次模型请求。
        返回：原始模型文本。
        异常：重试耗尽时抛出 AIGatewayError。
        副作用：最多调用 Provider `retry_count + 1` 次。
        """
        for attempt in range(1, self._retry_count + 2):
            try:
                # Provider 是唯一外部调用点，超时由 Provider 使用网关传入的统一配置。
                return (
                    self._provider.complete(request, timeout_seconds=self._timeout_seconds),
                    attempt,
                )
            except LLMProviderError as error:
                # 记录每次失败的白名单元数据，避免重试过程成为不可观测黑盒。
                logger.warning(
                    "ai_gateway_retry",
                    extra={
                        "ai_trace_id": trace_id,
                        "target": type(self._provider).__name__,
                        "attempt": attempt,
                        "error_type": type(error).__name__,
                    },
                )
                if attempt > self._retry_count:
                    raise AIGatewayError("ai_transport_failed") from error
        raise AssertionError("不可达：循环在成功或耗尽时结束")

    def _parse_schema_or_repair(
        self, request: LLMRequest, response: LLMResponse, trace_id: str
    ) -> tuple[LeadAnalysis, LLMResponse | None, int]:
        """解析并验证模型结构，且仅为 JSON/Schema 失败进行一次受约束修复。

        参数：request 为原请求；response 为初始模型响应。
        返回：经 Pydantic 验证的结构化分析。
        异常：第二次结构失败时抛出 AIGatewayError。
        副作用：结构失败时额外调用 Provider 一次，业务校验永不在此发生。
        """
        try:
            return LeadAnalysis.model_validate_json(response.content), None, 0
        except ValidationError:
            # 修复请求仅保留脱敏原输入、明确 Schema 与受限修复规则。
            repair_messages = (
                {
                    "role": "system",
                    "content": (
                        "只修复随后的输出 JSON 结构，不得新增、删除或改写任何事实值。"
                        f"{self._field_contract_instructions()}"
                        "必须符合此 JSON Schema："
                        f"{json.dumps(request.json_schema, ensure_ascii=False)}"
                    ),
                },
                request.messages[-1],
            )
            repair_request = LLMRequest(
                messages=repair_messages,
                json_schema=request.json_schema,
                repair_source=response.content,
            )
            try:
                # 结构修复只携带模型原输出和 Schema，不重新向模型索取业务事实。
                repaired_response, attempts = self._call_with_transport_retry(
                    repair_request, trace_id
                )
                return (
                    LeadAnalysis.model_validate_json(repaired_response.content),
                    repaired_response,
                    attempts,
                )
            except ValidationError as error:
                raise FailedStructuredOutputError(
                    response.content, repaired_response.content, str(error)
                ) from error

    def _validate_business(self, analysis: LeadAnalysis) -> None:
        """以确定性规则校验字段名、枚举、格式及置信度完整性。

        参数：analysis 为已通过 Pydantic Schema 的模型建议。
        返回：无。
        异常：发现任何候选不合法时抛出 BusinessValidationError。
        副作用：无；禁止在业务校验失败后重试模型。
        """
        for field_name, value in analysis.crm_fields.items():
            # CRM 注册表是字段白名单，禁止模型引入权限或提交等业务控制字段。
            if field_name not in CRM_BUSINESS_FIELD_NAMES:
                raise BusinessValidationError(f"未知 CRM 字段：{field_name}")
            # 枚举字段必须使用管理员配置的合法选项，失败后不再调用模型修正。
            if field_name in _ENUM_OPTIONS and value not in _ENUM_OPTIONS[field_name]:
                raise BusinessValidationError(f"枚举值不合法：{field_name}")
            # 联系方式格式由确定性规则校验，避免模型用猜测值绕过约束。
            if field_name in {"手机", "电话"} and not _PHONE_PATTERN.fullmatch(value):
                raise BusinessValidationError(f"联系方式格式不合法：{field_name}")
            if field_name == "邮箱" and not _EMAIL_PATTERN.fullmatch(value):
                raise BusinessValidationError("邮箱格式不合法")
            # 每个建议字段都必须提供区间内置信度，才能进入后续分级。
            confidence = analysis.confidence_by_field.get(field_name)
            if confidence is None or not 0 <= confidence <= 1:
                raise BusinessValidationError(f"置信度缺失或不合法：{field_name}")

    def _apply_confidence(
        self, analysis: LeadAnalysis
    ) -> tuple[dict[str, str], tuple[str, ...], dict[str, str]]:
        """按阈值将合法候选分为正式字段、待确认或后台低置信度候选。

        参数：analysis 为已完成业务校验的分析结果。
        返回：正式字段、稳定排序待确认字段与低置信度候选。
        异常：无。
        副作用：无；T08 只返回待确认元数据，不实现 T09 人工确认流程。
        """
        fields: dict[str, str] = {}
        pending: list[str] = []
        low_candidates: dict[str, str] = {}
        for field_name, value in analysis.crm_fields.items():
            confidence = analysis.confidence_by_field[field_name]
            # 高置信度可直接预填；中置信度保留待确认元数据；低置信度只后台留存。
            if confidence >= self._high_confidence_threshold:
                fields[field_name] = value
            elif confidence >= self._medium_confidence_threshold:
                fields[field_name] = value
                pending.append(field_name)
            else:
                low_candidates[field_name] = value
        return fields, tuple(pending), low_candidates

    def _messages(
        self, safe_text: str, json_schema: dict[str, object]
    ) -> tuple[dict[str, str], ...]:
        """构造仅包含当前文本和受控输出规则的最小化提取提示。

        参数：safe_text 为已移除无关高敏感号码的当前消息文本；json_schema 为输出约束。
        返回：供应商无关的聊天消息序列。
        异常：无。
        副作用：无。
        """
        return (
            {
                "role": "system",
                "content": (
                    "仅提取线索建议 JSON；不得决定提交、删除、负责人、"
                    "CRM 合并或覆盖人工值。"
                    f"{self._field_contract_instructions()}"
                    f"必须符合此 JSON Schema：{json.dumps(json_schema, ensure_ascii=False)}"
                ),
            },
            {"role": "user", "content": safe_text},
        )

    @staticmethod
    def _field_contract_instructions() -> str:
        """返回初始提取与结构修复共用的冻结 CRM 字段输出约束。

        参数：无。
        返回：要求模型遵循字段名、值类型、置信度与枚举注册表的提示文本。
        异常：无。
        副作用：无；仅基于冻结注册表拼装提示，不改变任何确定性校验。
        """
        allowed_names = "、".join(CRM_BUSINESS_FIELD_NAMES)
        enum_options = "；".join(
            f"{field_name}：{'、'.join(options)}" for field_name, options in _ENUM_OPTIONS.items()
        )
        return (
            "crm_fields 的 key 只能是以下 CRM 注册表中的中文原名："
            f"{allowed_names}。"
            "禁止使用英文或其他别名（例如 phone），不得映射或自造字段名。"
            "crm_fields 的每个 value 必须是单个字符串；多值信息不得使用数组塞入 CRM 字段。"
            "没有可靠信息的字段必须直接省略，不得返回 null、空字符串或空数组。"
            "confidence_by_field 的 key 必须与 crm_fields 的 key 完全一一对应，"
            "并使用相同中文字段名。"
            f"枚举字段只能使用以下注册表选项：{enum_options}。"
        )

    def _log(
        self,
        trace_id: str,
        started_at: float,
        status: str,
        *,
        error_type: str | None = None,
        call_count: int | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
    ) -> None:
        """写入不含输入、输出或密钥的调用观测日志。

        参数：trace_id 为调用追踪标识；started_at 为单调起点；status 为受控结果；
        error_type 为可选错误分类；call_count 为本追踪中的实际模型调用次数。
        返回：无。
        异常：无。
        副作用：向结构化日志写入脱敏运行元数据。
        """
        logger.info(
            "ai_gateway_call",
            extra={
                "ai_trace_id": trace_id,
                "ai_status": status,
                "target": type(self._provider).__name__,
                "ai_call_count": call_count,
                "ai_input_tokens": input_tokens,
                "ai_output_tokens": output_tokens,
                "duration_ms": round((time.monotonic() - started_at) * 1000),
                "error_type": error_type,
            },
        )
