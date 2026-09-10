"""统一执行脱敏、追踪、重试与确定性校验的 AI Gateway。"""

from __future__ import annotations

import logging
import re
import time
from uuid import uuid4

from pydantic import ValidationError

from app.ai.models import ExtractedLeadPatch, LeadAnalysis, LLMRequest
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
_SENSITIVE_PATTERN = re.compile(r"\b(?:\d{15,18}[0-9Xx]?|\d{16,19})\b")


class AIGatewayError(RuntimeError):
    """表示网关已经完成重试和错误归一化后的失败结论。"""


class BusinessValidationError(AIGatewayError):
    """表示结构合法但不满足 CRM 字段业务规则的候选。"""


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
        safe_text = _SENSITIVE_PATTERN.sub("[已遮蔽敏感号码]", text)
        request = LLMRequest(
            messages=self._messages(safe_text), json_schema=LeadAnalysis.model_json_schema()
        )
        started_at = time.monotonic()
        try:
            raw_output = self._call_with_transport_retry(request)
            analysis = self._parse_schema_or_repair(request, raw_output)
            self._validate_business(analysis)
            fields, pending, low_candidates = self._apply_confidence(analysis)
        except AIGatewayError as error:
            self._log(trace_id, started_at, "failed", error_type=type(error).__name__)
            raise
        self._log(trace_id, started_at, "succeeded")
        return ExtractedLeadPatch(trace_id, analysis, fields, pending, low_candidates)

    def _call_with_transport_retry(self, request: LLMRequest) -> str:
        """仅针对 Provider 传输故障执行配置次数的重试。

        参数：request 为单次模型请求。
        返回：原始模型文本。
        异常：重试耗尽时抛出 AIGatewayError。
        副作用：最多调用 Provider `retry_count + 1` 次。
        """
        for attempt in range(self._retry_count + 1):
            try:
                response = self._provider.complete(request, timeout_seconds=self._timeout_seconds)
                return response.content
            except LLMProviderError as error:
                if attempt == self._retry_count:
                    raise AIGatewayError("ai_transport_failed") from error
        raise AssertionError("不可达：循环在成功或耗尽时结束")

    def _parse_schema_or_repair(self, request: LLMRequest, raw_output: str) -> LeadAnalysis:
        """解析并验证模型结构，且仅为 JSON/Schema 失败进行一次受约束修复。

        参数：request 为原请求；raw_output 为初始模型输出。
        返回：经 Pydantic 验证的结构化分析。
        异常：第二次结构失败时抛出 AIGatewayError。
        副作用：结构失败时额外调用 Provider 一次，业务校验永不在此发生。
        """
        try:
            return LeadAnalysis.model_validate_json(raw_output)
        except ValidationError:
            repair_messages = request.messages + (
                {
                    "role": "system",
                    "content": "只修复以下输出的 JSON 结构，不得新增、删除或改写任何事实值。",
                },
            )
            repair_request = LLMRequest(
                messages=repair_messages,
                json_schema=request.json_schema,
                repair_source=raw_output,
            )
            try:
                repaired_output = self._call_with_transport_retry(repair_request)
                return LeadAnalysis.model_validate_json(repaired_output)
            except ValidationError as error:
                raise AIGatewayError("ai_structured_output_failed_pending_review") from error

    def _validate_business(self, analysis: LeadAnalysis) -> None:
        """以确定性规则校验字段名、枚举、格式及置信度完整性。

        参数：analysis 为已通过 Pydantic Schema 的模型建议。
        返回：无。
        异常：发现任何候选不合法时抛出 BusinessValidationError。
        副作用：无；禁止在业务校验失败后重试模型。
        """
        for field_name, value in analysis.crm_fields.items():
            if field_name not in CRM_BUSINESS_FIELD_NAMES:
                raise BusinessValidationError(f"未知 CRM 字段：{field_name}")
            if field_name in _ENUM_OPTIONS and value not in _ENUM_OPTIONS[field_name]:
                raise BusinessValidationError(f"枚举值不合法：{field_name}")
            if field_name in {"手机", "电话"} and not _PHONE_PATTERN.fullmatch(value):
                raise BusinessValidationError(f"联系方式格式不合法：{field_name}")
            if field_name == "邮箱" and not _EMAIL_PATTERN.fullmatch(value):
                raise BusinessValidationError("邮箱格式不合法")
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
            if confidence >= self._high_confidence_threshold:
                fields[field_name] = value
            elif confidence >= self._medium_confidence_threshold:
                fields[field_name] = value
                pending.append(field_name)
            else:
                low_candidates[field_name] = value
        return fields, tuple(pending), low_candidates

    def _messages(self, safe_text: str) -> tuple[dict[str, str], ...]:
        """构造仅包含当前文本和受控输出规则的最小化提取提示。

        参数：safe_text 为已移除无关高敏感号码的当前消息文本。
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
                ),
            },
            {"role": "user", "content": safe_text},
        )

    def _log(
        self,
        trace_id: str,
        started_at: float,
        status: str,
        *,
        error_type: str | None = None,
    ) -> None:
        """写入不含输入、输出或密钥的调用观测日志。

        参数：trace_id 为调用追踪标识；started_at 为单调起点；status 为受控结果；
        error_type 为可选错误分类。
        返回：无。
        异常：无。
        副作用：向结构化日志写入脱敏运行元数据。
        """
        logger.info(
            "ai_gateway_call",
            extra={
                "ai_trace_id": trace_id,
                "ai_status": status,
                "duration_ms": round((time.monotonic() - started_at) * 1000),
                "error_type": error_type,
            },
        )
