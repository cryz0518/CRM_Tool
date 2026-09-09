"""CRM 线索智能表格的管理员预配置结构约束。"""

from __future__ import annotations

from dataclasses import dataclass

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
PROCESS_OPTIONS = (
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
    "下次联系时间",
    "附件",
    "备注",
    "地区定位",
)
REQUIRED_SMART_TABLE_FIELDS = (
    RequiredSmartTableField("业务线", SmartTableFieldType.SINGLE_SELECT, BUSINESS_LINE_OPTIONS),
    RequiredSmartTableField("线索名称", SmartTableFieldType.TEXT),
    RequiredSmartTableField("线索来源", SmartTableFieldType.SINGLE_SELECT, LEAD_SOURCE_OPTIONS),
    RequiredSmartTableField("联系人", SmartTableFieldType.TEXT),
    RequiredSmartTableField("职务", SmartTableFieldType.TEXT),
    RequiredSmartTableField(
        "沟通方式", SmartTableFieldType.SINGLE_SELECT, COMMUNICATION_METHOD_OPTIONS
    ),
    RequiredSmartTableField("手机", SmartTableFieldType.TEXT),
    RequiredSmartTableField("电话", SmartTableFieldType.TEXT),
    RequiredSmartTableField("邮箱", SmartTableFieldType.EMAIL),
    RequiredSmartTableField(
        "客户行业", SmartTableFieldType.SINGLE_SELECT, CUSTOMER_INDUSTRY_OPTIONS
    ),
    RequiredSmartTableField("客户级别", SmartTableFieldType.SINGLE_SELECT, CUSTOMER_LEVEL_OPTIONS),
    RequiredSmartTableField("工艺", SmartTableFieldType.SINGLE_SELECT, PROCESS_OPTIONS),
    RequiredSmartTableField("下次联系时间", SmartTableFieldType.DATE),
    RequiredSmartTableField("附件", SmartTableFieldType.ATTACHMENT),
    RequiredSmartTableField("备注", SmartTableFieldType.LONG_TEXT),
    RequiredSmartTableField("地区定位", SmartTableFieldType.LOCATION),
    RequiredSmartTableField("AI待确认", SmartTableFieldType.MULTI_SELECT, CRM_BUSINESS_FIELD_NAMES),
    RequiredSmartTableField("创建人", SmartTableFieldType.MEMBER),
    RequiredSmartTableField("负责人", SmartTableFieldType.MEMBER),
)


def build_required_smart_table_schema() -> SmartTableSchema:
    """构造符合当前字段注册表的 Mock 管理员预配置结构。

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
