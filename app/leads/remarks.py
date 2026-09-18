"""按冻结模板将已审核事实组织为线索备注。"""

from __future__ import annotations

from typing import Mapping

from app.smart_table.registry import ENUM_FIELDS_WITH_OTHER


class RemarksBuilder:
    """只使用已审核字段和有原文证据的补充信息生成备注。"""

    def build(self, fields: Mapping[str, object], enrichment: Mapping[str, str]) -> str:
        """生成不猜测缺失事实的固定四段式备注。

        参数：fields 为已通过 T09 保护的正式字段；enrichment 为单次提取中有文本证据的补充信息。
        返回值：可写入智能表格“备注”字段的四段式文本。
        异常：无。
        副作用：无。
        """
        company = self._text(fields.get("线索名称"))
        industry = self._text(fields.get("客户行业"))
        city = self._text(enrichment.get("城市/地区"))
        product = self._text(enrichment.get("主营产品"))
        revenue = self._text(enrichment.get("年销售额"))
        demand = self._text(enrichment.get("客户需求/痛点")) or self._demand_from_fields(fields)
        budget = self._text(enrichment.get("预算"))
        requirement = self._text(enrichment.get("特殊要求"))
        sections = [
            self._basic_information(company, city, product, revenue, industry),
            f"线索需求：{self._sentence(demand)}",
            f"预算情况：{self._sentence(budget)}",
            f"特殊要求：{self._sentence(requirement)}",
        ]
        other_details = self._other_enum_details(fields, enrichment)
        if other_details:
            # 枚举字段的实际自由文本统一追加在备注尾部，保留字段名与“其他”选项语义。
            sections.append(f"备注补充：{'；'.join(other_details)}")
        return "\n".join(sections)

    @staticmethod
    def _other_enum_details(
        fields: Mapping[str, object], enrichment: Mapping[str, str]
    ) -> tuple[str, ...]:
        """生成所有已选择“其他”的枚举字段补充文本。

        参数：fields 为已审核正式字段；enrichment 为有原文证据的补充信息。
        返回值：按字段名排序的“字段名：其他（内容）”文本；无对应字段时使用“请补充”。
        异常：无。
        副作用：无。
        """
        details: list[str] = []
        for field_name in sorted(ENUM_FIELDS_WITH_OTHER):
            if fields.get(field_name) != "其他":
                continue
            detail = RemarksBuilder._text(enrichment.get(field_name)) or "请补充"
            details.append(f"{field_name}：其他（{detail}）")
        return tuple(details)

    def _basic_information(
        self,
        company: str | None,
        city: str | None,
        product: str | None,
        revenue: str | None,
        industry: str | None,
    ) -> str:
        """组织冻结模板中的基本信息段落。

        参数：各参数为已审核公司字段或有证据的补充信息。
        返回值：带明确未提供标记的基本信息段落。
        异常：无。
        副作用：无。
        """
        known = [
            value
            for value in (
                company,
                f"位于{city}" if city else None,
                f"主要产品为{product}" if product else None,
                f"年销售额可达{revenue}" if revenue else None,
                f"所属行业为{industry}" if industry else None,
            )
            if value
        ]
        missing = [
            label
            for label, value in (
                ("公司", company),
                ("城市", city),
                ("主要产品", product),
                ("年销售额", revenue),
                ("所属行业", industry),
            )
            if not value
        ]
        text = "基本信息：" + "，".join(known or ["未提供"])
        return f"{text}；{'、'.join(missing)}未提供。" if missing else f"{text}。"

    def _demand_from_fields(self, fields: Mapping[str, object]) -> str | None:
        """仅在业务线和工艺均已审核时组织可追溯的需求描述。

        参数：fields 为 T09 已允许的正式字段。
        返回值：两项齐全时返回由字段直接组成的需求，否则返回 None。
        异常：无。
        副作用：无。
        """
        business_line = self._text(fields.get("业务线"))
        process = self._text(fields.get("工艺"))
        return f"客户希望使用{business_line}完成{process}" if business_line and process else None

    @staticmethod
    def _text(value: object) -> str | None:
        """规范化可用于备注的非空文本值。

        参数：value 为候选字段或补充信息值。
        返回值：去除首尾空白后的文本；非字符串或空白时返回 None。
        异常：无。
        副作用：无。
        """
        return value.strip() if isinstance(value, str) and value.strip() else None

    @staticmethod
    def _sentence(value: str | None) -> str:
        """将已知事实或缺失占位符规范为一句中文句子。

        参数：value 为待输出的事实文本。
        返回值：以中文句号收尾的文本。
        异常：无。
        副作用：无。
        """
        return f"{(value or '未提供').rstrip('。')}。"
