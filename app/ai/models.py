"""AI Gateway 输入、输出及 Pydantic 结构化模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.smart_table.registry import AI_FIELD_ALIASES

LeadFieldValue: TypeAlias = str | list[str]


class LeadSegmentAnalysis(BaseModel):
    """描述模型从一条消息中拆出的单个、原文有边界的客户候选。"""

    model_config = ConfigDict(extra="forbid", strict=True)

    segment_index: int
    source_text_span: str
    customer_reference: dict[str, str] = Field(default_factory=dict)
    crm_fields: dict[str, LeadFieldValue] = Field(default_factory=dict)
    enrichment: dict[str, str] = Field(default_factory=dict)
    confidence_by_field: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_segment_shape(self) -> LeadSegmentAnalysis:
        """限制分段序号和字段多值结构，避免模型借分段绕过现有字段契约。

        返回值：当前已校验的分段模型。
        异常：分段序号为负数、原文片段为空或非工艺字段为数组时抛出 ValueError。
        副作用：无。
        """
        if self.segment_index < 0:
            raise ValueError("segment_index 不能为负数")
        if not self.source_text_span.strip():
            raise ValueError("source_text_span 不能为空")
        forbidden_reference_keys = {
            "lead_id",
            "database_id",
            "smart_table_record_id",
            "record_id",
        }
        if any(key.strip().lower() in forbidden_reference_keys for key in self.customer_reference):
            raise ValueError("客户引用不得包含业务目标标识")
        for field_name, value in self.crm_fields.items():
            canonical_name = AI_FIELD_ALIASES.get(field_name.strip().lower(), field_name)
            if isinstance(value, list) and canonical_name != "工艺":
                raise ValueError(f"字段不支持多值：{field_name}")
        return self


class LeadAnalysis(BaseModel):
    """约束 LLM 仅返回增量线索建议，不承载任何业务写入决定。"""

    model_config = ConfigDict(extra="forbid", strict=True)

    intent: Literal[
        "NEW_LEAD",
        "UPDATE_LEAD",
        "MULTI_LEAD",
        "MULTI_LEAD_AMBIGUOUS",
        "IGNORE",
    ]
    customer_reference: dict[str, str] = Field(default_factory=dict)
    crm_fields: dict[str, LeadFieldValue] = Field(default_factory=dict)
    enrichment: dict[str, str] = Field(default_factory=dict)
    confidence_by_field: dict[str, float] = Field(default_factory=dict)
    segments: list[LeadSegmentAnalysis] = Field(default_factory=list)
    conflicts: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_field_value_shapes(self) -> LeadAnalysis:
        """限制 CRM 字段多值结构，仅允许工艺使用多选数组。

        返回值：当前已校验的模型实例。
        异常：除工艺外的 CRM 字段出现数组时抛出 ValueError，触发结构修复流程。
        副作用：无。
        """
        for field_name, value in self.crm_fields.items():
            canonical_name = AI_FIELD_ALIASES.get(field_name.strip().lower(), field_name)
            if isinstance(value, list) and canonical_name != "工艺":
                raise ValueError(f"字段不支持多值：{field_name}")
        forbidden_reference_keys = {
            "lead_id",
            "database_id",
            "smart_table_record_id",
            "record_id",
        }
        if any(key.strip().lower() in forbidden_reference_keys for key in self.customer_reference):
            raise ValueError("客户引用不得包含业务目标标识")
        return self


class SubmissionIntent(BaseModel):
    """约束模型只识别 CRM 提交意图，不承载 CRM 写入参数。"""

    model_config = ConfigDict(extra="forbid", strict=True)

    intent: Literal[
        "LEAD_CAPTURE",
        "SUBMIT_TODAY",
        "SUBMIT_ALL",
        "SUBMIT_SINGLE",
        "SUBMIT_ABANDONED",
        "SUBMIT_UPDATES",
        "SUBMIT_RETRY_INCOMPLETE",
        "UNKNOWN",
    ]
    company_name: str | None = None


@dataclass(frozen=True)
class LLMRequest:
    """定义 Provider 接收的最小化模型请求。"""

    messages: tuple[dict[str, str], ...]
    json_schema: dict[str, object]
    repair_source: str | None = None


@dataclass(frozen=True)
class LLMResponse:
    """保存 Provider 返回的原始文本及可观测调用量。"""

    content: str
    input_tokens: int | None = None
    output_tokens: int | None = None


@dataclass(frozen=True)
class ExtractedLeadPatch:
    """返回通过确定性校验后的正式字段与仅供后续流程使用的候选。"""

    trace_id: str
    analysis: LeadAnalysis
    fields: dict[str, LeadFieldValue]
    pending_confirmation_fields: tuple[str, ...]
    low_confidence_candidates: dict[str, LeadFieldValue]
    enrichment: dict[str, str] = field(default_factory=dict)
    # 仅允许明确来源（例如 TYC 多候选首项）的待确认字段在无卡片时预填。
    pending_prefill_allowed_fields: tuple[str, ...] = ()
    # 多客户消息的每个分段沿用同一套字段校验与置信度规则。
    segments: tuple["ExtractedLeadSegmentPatch", ...] = ()


@dataclass(frozen=True)
class ExtractedLeadSegmentPatch:
    """保存一个经过网关校验、待服务端独立归属的语义分段补丁。"""

    segment_index: int
    source_text_span: str
    patch: ExtractedLeadPatch
