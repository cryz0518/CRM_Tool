"""将智能表格中文字段转换为 CRM 接口字段和值。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Literal


class CrmPayloadError(ValueError):
    """表示 CRM 字段映射、字典值或格式校验失败。"""


CRM_FIELD_NAMES = {
    "业务线": "product_line_data_permission",
    "线索名称": "name",
    "线索来源": "source",
    "联系人": "contactName",
    "职务": "contactTitle",
    "沟通方式": "communicationWay",
    "手机": "mobile",
    "电话": "telephone",
    "邮箱": "email",
    "客户行业": "industry",
    "客户级别": "customer_level",
    "工艺": "process",
    "下次联系时间": "contactDate",
    "备注": "remark",
    "是否为国际客户": "isInternational",
}
# 附件当前按用户要求忽略；地区定位需先具备经纬度和行政区划解析能力，暂不猜测 CRM 地址字段。

BUSINESS_LINE_IDS = {"协作机器人": 1, "车载机器人": 2}
LEAD_SOURCE_IDS = {
    "促销": 1,
    "搜索引擎": 2,
    "广告": 3,
    "转介绍": 4,
    "线上注册": 5,
    "线上询价": 6,
    "预约上门": 7,
    "陌拜": 8,
    "电话咨询": 9,
    "邮件咨询": 10,
    "展会": 11,
    "信息渠道（通信运营商）": 12,
    "信息渠道（商会及协会）": 13,
    "信息渠道（政府职能部门）": 14,
    "信息渠道（异业合作）": 15,
}
INDUSTRY_IDS = {
    "3C电子": 1,
    "医疗": 2,
    "机械加工": 3,
    "汽车": 4,
    "食品": 5,
    "航空航天": 6,
    "精密制造": 7,
    "新能源": 8,
    "纺织": 9,
    "院校": 10,
    "其他": 11,
}
COMMUNICATION_WAY_IDS = {
    "打电话": 1,
    "发邮件": 2,
    "发短信": 3,
    "见面拜访": 4,
    "活动": 5,
    "微信": 6,
    "陪访": 7,
    "线上会议": 8,
    # CRM 说明重复声明“技术支持工单”，当前按首个字典值保留。
    "技术支持工单": 9,
    "技术支持跟进": 11,
}
PROCESS_IDS = {
    "上下料": 1,
    "锁螺丝": 2,
    "焊接": 3,
    "喷涂": 4,
    "打磨": 5,
    "涂胶": 6,
    "装配": 7,
    "码垛": 8,
    "视觉检测": 9,
    "贴标": 10,
    "开箱机": 11,
    "自助加油/充电": 12,
    "商业应用": 13,
    "其他": 14,
}
CUSTOMER_LEVEL_IDS = {
    "legacy": {"重点客户": 1, "普通客户": 2, "非优先客户": 3},
    "current": {"重点客户": 10000, "普通客户": 9000, "非优先客户": 8000},
}


class CrmPayloadBuilder:
    """根据目标 CRM 字段契约构造确定性的业务请求载荷。"""

    def __init__(self, customer_level_scheme: Literal["legacy", "current"] = "current") -> None:
        """保存客户级别字典版本选择。

        参数：customer_level_scheme 为 CRM 当前使用的客户级别字典版本。
        返回值：无。
        异常：版本名称不受支持时抛出 CrmPayloadError。
        副作用：无，仅保存映射选择。
        """
        if customer_level_scheme not in CUSTOMER_LEVEL_IDS:
            raise CrmPayloadError(f"不支持的客户级别字典版本：{customer_level_scheme}")
        self._customer_level_ids = CUSTOMER_LEVEL_IDS[customer_level_scheme]

    def build(
        self, fields: Mapping[str, object], *, tyc_customer_id: str | None = None
    ) -> dict[str, object]:
        """将智能表格字段转换为 CRM 接口字段并校验可确定的格式。

        参数：fields 为最终读取的智能表格中文字段；tyc_customer_id 为天眼查客户标识。
        返回值：只包含非空 CRM 字段的英文键请求载荷。
        异常：枚举、日期、备注或类型无法确定时抛出 CrmPayloadError。
        副作用：无，不调用 CRM 或修改数据库。
        """
        payload: dict[str, object] = {}
        for field_name, crm_name in CRM_FIELD_NAMES.items():
            value = fields.get(field_name)
            if value in (None, "", []):
                continue
            payload[crm_name] = self._convert_field(field_name, value)
        # 新表默认国内；旧记录没有该字段时也必须向 CRM 发送 false。
        payload.setdefault("isInternational", False)
        # 国际客户不使用天眼查客户标识参与查重；即使历史线索残留标识也不得继续发送。
        if tyc_customer_id and payload.get("isInternational") is not True:
            payload["tycCustomerId"] = tyc_customer_id.strip()
        return payload

    def _convert_field(self, field_name: str, value: object) -> object:
        """转换单个中文字段的 CRM 类型和值。

        参数：field_name 为智能表格字段名；value 为表格当前值。
        返回值：CRM 接口所需的字符串、整数、布尔值或整数数组。
        异常：值不在确定性字典或格式不合法时抛出 CrmPayloadError。
        副作用：无。
        """
        if field_name == "业务线":
            return self._dictionary_value(field_name, value, BUSINESS_LINE_IDS)
        if field_name == "线索来源":
            return self._dictionary_value(field_name, value, LEAD_SOURCE_IDS)
        if field_name == "客户行业":
            # CRM 字典已明确给出“其他”为 11，允许表格选项进入正式接口值。
            return self._dictionary_value(field_name, value, INDUSTRY_IDS)
        if field_name == "沟通方式":
            return self._dictionary_value(field_name, value, COMMUNICATION_WAY_IDS)
        if field_name == "客户级别":
            return self._dictionary_value(field_name, value, self._customer_level_ids)
        if field_name == "工艺":
            values = value if isinstance(value, list) else [value]
            return [self._dictionary_value(field_name, item, PROCESS_IDS) for item in values]
        if field_name == "是否为国际客户":
            if value not in {"国内", "国外", True, False}:
                raise CrmPayloadError(f"是否为国际客户值不合法：{value}")
            return value == "国外" or value is True
        if field_name == "下次联系时间":
            return self._date_value(value)
        if field_name == "备注":
            return self._remark_value(value)
        if not isinstance(value, str):
            raise CrmPayloadError(f"CRM 字段类型不合法：{field_name}")
        return value.strip()

    @staticmethod
    def _dictionary_value(
        field_name: str, value: object, options: Mapping[str, int]
    ) -> int:
        """将中文枚举值转换为 CRM 数字字典值。

        参数：field_name 为字段名；value 为中文候选；options 为确定性字典。
        返回值：CRM 要求的整数值。
        异常：字段值不存在或不在字典中时抛出 CrmPayloadError。
        副作用：无。
        """
        if not isinstance(value, str) or value not in options:
            raise CrmPayloadError(f"{field_name} 的 CRM 字典值未确认：{value}")
        return options[value]

    @staticmethod
    def _date_value(value: object) -> str:
        """将表格日期转换为 CRM 的 yyyy-MM-dd 字符串。

        参数：value 为表格日期、日期时间或其字符串表示。
        返回值：CRM 接口要求的日期字符串。
        异常：日期无法解析时抛出 CrmPayloadError。
        副作用：无。
        """
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str):
            normalized = value.strip().replace("/", "-")
            try:
                return datetime.fromisoformat(normalized.replace("Z", "+00:00")).date().isoformat()
            except ValueError:
                try:
                    return date.fromisoformat(normalized).isoformat()
                except ValueError as error:
                    raise CrmPayloadError(f"下次联系时间格式不合法：{value}") from error
        raise CrmPayloadError(f"下次联系时间类型不合法：{value}")

    @staticmethod
    def _remark_value(value: object) -> str:
        """添加 CRM 备注前缀并校验接口长度限制。

        参数：value 为审核后的备注文本。
        返回值：带 `【AI录入】` 前缀的 CRM 备注。
        异常：备注不是文本或长度不满足 CRM DTO 要求时抛出 CrmPayloadError。
        副作用：无。
        """
        if not isinstance(value, str):
            raise CrmPayloadError("备注必须是文本")
        remark = value.strip()
        if not remark.startswith("【AI录入】"):
            remark = f"【AI录入】{remark}"
        if not 30 <= len(remark) <= 1000:
            raise CrmPayloadError("备注长度必须在 30 到 1000 个字符之间")
        return remark
