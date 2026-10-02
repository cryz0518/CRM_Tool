"""AI Gateway 输入、输出及 Pydantic 结构化模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.smart_table.registry import AI_FIELD_ALIASES

LeadFieldValue: TypeAlias = str | list[str]


class LeadAnalysis(BaseModel):
    """约束 LLM 仅返回增量线索建议，不承载任何业务写入决定。"""

    model_config = ConfigDict(extra="forbid", strict=True)

    intent: Literal["NEW_LEAD", "UPDATE_LEAD", "MULTI_LEAD", "IGNORE"]
    customer_reference: dict[str, str] = Field(default_factory=dict)
    crm_fields: dict[str, LeadFieldValue] = Field(default_factory=dict)
    enrichment: dict[str, str] = Field(default_factory=dict)
    confidence_by_field: dict[str, float] = Field(default_factory=dict)
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
