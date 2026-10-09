"""CRM 线索智能表格的管理员预配置结构约束。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeVar

from app.smart_table.models import (
    SmartTableField,
    SmartTableFieldType,
    SmartTableOption,
    SmartTableSchema,
)


@dataclass(frozen=True)
class RequiredSmartTableField:
    """定义一个必须存在的字段、其类型及必需枚举选项。"""

    name: str
    field_type: SmartTableFieldType
    required_options: tuple[str, ...] = ()


BUSINESS_LINE_OPTIONS = ("协作机器人", "车载机器人")
LEAD_SOURCE_OPTIONS = (
    "促销",
    "搜索引擎",
    "广告",
    "转介绍",
    "线上注册",
    "线上询价",
    "预约上门",
    "陌拜",
    "电话咨询",
    "邮件咨询",
    "展会",
    "信息渠道（通信运营商）",
    "信息渠道（商会及协会）",
    "信息渠道（政府职能部门）",
    "信息渠道（异业合作）",
)
COMMUNICATION_METHOD_OPTIONS = (
    "打电话",
    "发邮件",
    "发短信",
    "见面拜访",
    "活动",
    "微信",
    "陪访",
    "线上会议",
    "技术支持工单",
    "技术支持跟进",
)
CUSTOMER_INDUSTRY_OPTIONS = (
    "3C电子",
    "医疗",
    "机械加工",
    "汽车",
    "食品",
    "航空航天",
    "精密制造",
    "新能源",
    "纺织",
    "院校",
    "其他",
)
CUSTOMER_LEVEL_OPTIONS = ("重点客户", "普通客户", "非优先客户")
SUBMISSION_STATUS_OPTIONS = ("已提交", "未提交", "放弃提交")
INTERNATIONAL_CUSTOMER_OPTIONS = ("国内", "国外")
PROCESS_OPTIONS = (
    "上下料",
    "锁螺丝",
    "焊接",
    "喷涂",
    "打磨",
    "涂胶",
    "装配",
    "码垛",
    "视觉检测",
    "贴标",
    "开箱机",
    "自助加油/充电",
    "商业应用",
    "其他",
)

CRM_BUSINESS_FIELD_NAMES = (
    "业务线",
    "线索名称",
    "线索来源",
    "联系人",
    "职务",
    "沟通方式",
    "手机",
    "电话",
    "邮箱",
    "客户行业",
    "客户级别",
    "工艺",
    "是否为国际客户",
    "下次联系时间",
    "附件",
    "备注",
    "地区定位",
)
# AI 可以识别的自然语言字段别名；所有别名都必须收敛到已有注册字段，禁止模糊匹配。
AI_FIELD_ALIASES = {
    "公司": "线索名称",
    "公司名称": "线索名称",
    "企业": "线索名称",
    "企业名称": "线索名称",
    "客户名称": "线索名称",
    "联系人姓名": "联系人",
    "联系人名称": "联系人",
    "手机号": "手机",
    "手机号码": "手机",
    "固定电话": "电话",
    "联系电话": "电话",
    "电子邮箱": "邮箱",
    "邮箱地址": "邮箱",
    "职位": "职务",
    "岗位": "职务",
    "职称": "职务",
    "来源": "线索来源",
    "沟通渠道": "沟通方式",
    "联系渠道": "沟通方式",
    "行业": "客户行业",
    "所属行业": "客户行业",
    "客户所属行业": "客户行业",
    "业务领域": "客户行业",
    "客户等级": "客户级别",
    "级别": "客户级别",
    "应用工艺": "工艺",
    "工艺需求": "工艺",
    "客户工艺": "工艺",
    "国内外": "是否为国际客户",
    "客户地域": "是否为国际客户",
    "国家/地区": "是否为国际客户",
    "跟进时间": "下次联系时间",
    "下次跟进时间": "下次联系时间",
    "预计联系时间": "下次联系时间",
    "地址": "地区定位",
    "所在地区": "地区定位",
    "所在地": "地区定位",
    "城市": "城市/地区",
    "地区": "城市/地区",
    "产品": "主营产品",
    "主营业务": "主营产品",
    "客户需求": "客户需求/痛点",
    "需求": "客户需求/痛点",
    "痛点": "客户需求/痛点",
    "项目预算": "预算",
    "投资预算": "预算",
    "营收": "年销售额",
    "收入": "年销售额",
}
# 业务默认值独立于历史表格元数据，禁止在历史记录读取时批量补写。
DEFAULT_LEAD_BUSINESS_VALUES = {
    "业务线": "协作机器人",
    "职务": "经理",
    "沟通方式": "活动",
    "客户行业": "其他",
}
DEFAULT_SMART_TABLE_FIELD_VALUES = {"是否为国际客户": "国内", "提交状态": "未提交"}
# 原生创建时间只读；旧后台“录入时间”仅保留审计，不要求表格存在或映射其原生类型。
READ_ONLY_SYSTEM_TIME_FIELDS = frozenset({"创建时间", "录入时间"})
_FieldValue = TypeVar("_FieldValue")


def writable_smart_table_fields(fields: Mapping[str, _FieldValue]) -> dict[str, _FieldValue]:
    """剔除系统时间后生成可写补丁，保留后台审计值。

    参数：fields 为草稿或本轮补丁。返回：不含创建时间及旧录入时间的副本。
    异常：无。副作用：无，不修改输入或真实表格。
    """
    return {
        name: value
        for name, value in fields.items()
        if name.removeprefix("*") not in READ_ONLY_SYSTEM_TIME_FIELDS
    }


REQUIRED_SMART_TABLE_FIELDS = (
    RequiredSmartTableField("业务线", SmartTableFieldType.SINGLE_SELECT, BUSINESS_LINE_OPTIONS),
    RequiredSmartTableField("线索名称", SmartTableFieldType.TEXT),
    RequiredSmartTableField(
        "是否为国际客户", SmartTableFieldType.SINGLE_SELECT, INTERNATIONAL_CUSTOMER_OPTIONS
    ),
    RequiredSmartTableField("线索来源", SmartTableFieldType.SINGLE_SELECT, LEAD_SOURCE_OPTIONS),
    RequiredSmartTableField("联系人", SmartTableFieldType.TEXT),
    RequiredSmartTableField("职务", SmartTableFieldType.TEXT),
    RequiredSmartTableField(
        "沟通方式", SmartTableFieldType.SINGLE_SELECT, COMMUNICATION_METHOD_OPTIONS
    ),
    RequiredSmartTableField("手机", SmartTableFieldType.PHONE_NUMBER),
    RequiredSmartTableField("电话", SmartTableFieldType.PHONE_NUMBER),
    RequiredSmartTableField("邮箱", SmartTableFieldType.EMAIL),
    RequiredSmartTableField(
        "客户行业", SmartTableFieldType.SINGLE_SELECT, CUSTOMER_INDUSTRY_OPTIONS
    ),
    RequiredSmartTableField("客户级别", SmartTableFieldType.SINGLE_SELECT, CUSTOMER_LEVEL_OPTIONS),
    RequiredSmartTableField("工艺", SmartTableFieldType.MULTI_SELECT, PROCESS_OPTIONS),
    RequiredSmartTableField("下次联系时间", SmartTableFieldType.DATE),
    RequiredSmartTableField("附件", SmartTableFieldType.ATTACHMENT),
    # 备注由 AI Prompt 生成固定文案，智能表格只保存文本，不承担格式模板职责。
    RequiredSmartTableField("备注", SmartTableFieldType.TEXT),
    RequiredSmartTableField("地区定位", SmartTableFieldType.LOCATION),
    RequiredSmartTableField("AI待确认", SmartTableFieldType.MULTI_SELECT, CRM_BUSINESS_FIELD_NAMES),
    RequiredSmartTableField("创建人", SmartTableFieldType.MEMBER),
    RequiredSmartTableField("负责人", SmartTableFieldType.MEMBER),
    RequiredSmartTableField(
        "提交状态", SmartTableFieldType.SINGLE_SELECT, SUBMISSION_STATUS_OPTIONS
    ),
)
ENUM_FIELDS_WITH_OTHER = frozenset(
    field.name for field in REQUIRED_SMART_TABLE_FIELDS if "其他" in field.required_options
)


def build_required_smart_table_schema() -> SmartTableSchema:
    """构造项目可写字段的 Mock 结构，不模拟尚未核实类型的原生系统时间列。

    返回：包含稳定字段与选项 ID 的 Mock 表结构。
    副作用：按字段注册表顺序生成仅用于 Mock 的底层标识。
    """
    return SmartTableSchema(
        fields=tuple(
            # 用稳定序号模拟管理员的 field_id/option_id，业务层只使用字段名称与值。
            SmartTableField(
                field_id=f"mock-field-{index}",
                name=field.name,
                field_type=field.field_type,
                options=tuple(
                    SmartTableOption(
                        option_id=f"mock-field-{index}-option-{option_index}",
                        name=option,
                    )
                    for option_index, option in enumerate(field.required_options, start=1)
                ),
            )
            for index, field in enumerate(REQUIRED_SMART_TABLE_FIELDS, start=1)
        )
    )
