"""智能表格适配器传递的领域数据结构。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping


class SmartTableFieldType(StrEnum):
    """定义本项目会校验的智能表格字段类型。"""

    TEXT = "TEXT"
    LONG_TEXT = "LONG_TEXT"
    EMAIL = "EMAIL"
    SINGLE_SELECT = "SINGLE_SELECT"
    MULTI_SELECT = "MULTI_SELECT"
    DATE = "DATE"
    ATTACHMENT = "ATTACHMENT"
    LOCATION = "LOCATION"
    MEMBER = "MEMBER"


@dataclass(frozen=True)
class SmartTableOption:
    """描述单选或多选字段的管理员配置选项及其底层标识。"""

    option_id: str
    name: str


@dataclass(frozen=True)
class SmartTableField:
    """描述管理员预先配置的一个智能表格字段及其可选项。"""

    field_id: str
    name: str
    field_type: SmartTableFieldType
    options: tuple[SmartTableOption, ...] = ()


@dataclass(frozen=True)
class SmartTableSchema:
    """承载一个子表的字段结构，供业务层与就绪检查读取。"""

    fields: tuple[SmartTableField, ...]

    def get_field(self, name: str) -> SmartTableField | None:
        """按展示名称取得字段，避免适配器泄露底层查询细节。

        参数：name 为管理员配置的字段展示名称。
        返回：匹配的字段快照；找不到时返回 None。
        """
        for field in self.fields:
            # 字段名是启动时绑定的稳定业务键，不能按数组顺序或底层 ID 推断。
            if field.name == name:
                return field
        return None


@dataclass(frozen=True)
class SmartTableRecord:
    """表示业务层可读写的一条智能表格记录快照。"""

    record_id: str
    fields: Mapping[str, object]


@dataclass(frozen=True)
class SmartTablePermissions:
    """记录普通销售在目标智能表格中的受控操作权限。"""

    sales_can_create_records: bool
    sales_can_delete_records: bool


@dataclass(frozen=True)
class SmartTableReadinessReport:
    """汇总智能表格配置校验结果，供健康接口和运维界面消费。"""

    ready: bool
    issues: tuple[str, ...]
