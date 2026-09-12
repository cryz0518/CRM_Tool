"""AI Gateway 输入、输出及 Pydantic 结构化模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class LeadAnalysis(BaseModel):
    """约束 LLM 仅返回增量线索建议，不承载任何业务写入决定。"""

    model_config = ConfigDict(extra="forbid", strict=True)

    intent: Literal["NEW_LEAD", "UPDATE_LEAD", "MULTI_LEAD", "IGNORE"]
    customer_reference: dict[str, str] = Field(default_factory=dict)
    crm_fields: dict[str, str] = Field(default_factory=dict)
    enrichment: dict[str, str] = Field(default_factory=dict)
    confidence_by_field: dict[str, float] = Field(default_factory=dict)
    conflicts: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


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
    fields: dict[str, str]
    pending_confirmation_fields: tuple[str, ...]
    low_confidence_candidates: dict[str, str]
    enrichment: dict[str, str] = field(default_factory=dict)
