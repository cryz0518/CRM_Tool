"""计算线索审核工作区的业务字段完整度。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

AI_CONFIRMATION_FIELD = "AI待确认"
REQUIRED_LEAD_FIELDS = (
    "业务线",
    "线索名称",
    "线索来源",
    "联系人",
    "职务",
    "沟通方式",
    "手机",
    "备注",
)


@dataclass(frozen=True)
class LeadCompleteness:
    """承载正式字段缺失与已预填待确认字段的独立结论。"""

    missing_required_fields: tuple[str, ...]
    pending_confirmation_fields: tuple[str, ...]


class LeadCompletenessService:
    """仅依据正式业务字段和 AI待确认 元数据计算线索完整度。"""

    def evaluate(
        self,
        fields: Mapping[str, object],
        *,
        low_confidence_candidates: Mapping[str, str] | None = None,
    ) -> LeadCompleteness:
        """区分真实缺失字段与已预填但尚待销售确认的字段。

        参数：fields 为当前正式字段及 AI待确认 快照；low_confidence_candidates 为仅后台留存的候选。
        返回值：按冻结必填字段顺序返回缺失字段和已预填待确认字段。
        异常：无。
        副作用：无；低置信度候选只显式保留接口语义，不参与完整度填充。
        """
        del low_confidence_candidates
        missing = tuple(
            field_name
            for field_name in REQUIRED_LEAD_FIELDS
            if not self._has_value(fields.get(field_name))
        )
        pending_names = self._confirmation_names(fields.get(AI_CONFIRMATION_FIELD))
        pending = tuple(
            field_name
            for field_name in REQUIRED_LEAD_FIELDS
            if field_name in pending_names and self._has_value(fields.get(field_name))
        )
        return LeadCompleteness(missing, pending)

    @staticmethod
    def _has_value(value: object) -> bool:
        """判断正式字段值是否可视为已填写。

        参数：value 为智能表格或后台字段值。
        返回值：非空字符串或非空非字符串值时返回 True。
        异常：无。
        副作用：无。
        """
        return value not in (None, "")

    @staticmethod
    def _confirmation_names(value: object) -> set[str]:
        """将多选字段的适配器返回值规范化为字段名集合。

        参数：value 为 AI待确认 的原始值。
        返回值：只含非空字符串的字段名称集合。
        异常：无。
        副作用：无。
        """
        if not isinstance(value, list):
            return set()
        return {item for item in value if isinstance(item, str) and item}
