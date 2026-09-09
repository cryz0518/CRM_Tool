"""业务层使用的智能表格稳定适配器契约。"""

from __future__ import annotations

from enum import StrEnum
from typing import Mapping, Protocol

from app.smart_table.models import (
    SmartTablePermissions,
    SmartTableRecord,
    SmartTableSchema,
)


class SmartTableActor(StrEnum):
    """标识请求智能表格操作的主体，用于保留权限语义。"""

    ROBOT = "ROBOT"
    SALES = "SALES"
    ADMIN = "ADMIN"


class SmartTablePermissionError(PermissionError):
    """表示调用方不具备智能表格操作权限。"""


class SmartTableRecordNotFoundError(LookupError):
    """表示请求的智能表格记录不存在。"""


class SmartTableAdapterConfigurationError(RuntimeError):
    """表示部署尚未提供可读取管理员配置的智能表格适配器。"""


class SmartTableAdapter(Protocol):
    """隔离企业微信 CLI/API 的稳定智能表格读写契约。"""

    def get_schema(self) -> SmartTableSchema:
        """读取管理员维护的字段、类型和枚举结构。

        返回：带字段与选项底层标识的表结构快照。
        异常：底层连接或结构读取失败时由具体适配器抛出。
        """
        ...

    def get_permissions(self) -> SmartTablePermissions:
        """读取普通销售的新增与删除权限配置。

        返回：当前管理员配置的销售新增与删除权限快照。
        异常：底层连接或权限读取失败时由具体适配器抛出。
        """
        ...

    def get_record(self, record_id: str) -> SmartTableRecord | None:
        """按记录标识读取单条记录。

        参数：record_id 为智能表格记录标识。
        返回：记录快照；记录不存在时返回 None。
        异常：底层读取失败时由具体适配器抛出。
        """
        ...

    def find_records(self, filters: Mapping[str, object]) -> list[SmartTableRecord]:
        """按字段精确匹配查询记录，供业务层进行确定性查找。

        参数：filters 是字段名到期望值的全部匹配条件。
        返回：符合所有条件的记录快照列表。
        异常：底层查询失败时由具体适配器抛出。
        """
        ...

    def get_records(self) -> list[SmartTableRecord]:
        """返回当前子表的记录快照列表。

        返回：当前子表全部记录的快照列表。
        异常：底层查询失败时由具体适配器抛出。
        """
        ...

    def create_record(
        self,
        fields: Mapping[str, object],
        *,
        actor: SmartTableActor,
    ) -> SmartTableRecord:
        """创建一条记录，并保留调用主体以便适配器执行权限控制。

        参数：fields 为待写入字段；actor 为机器人、销售或管理员主体。
        返回：创建后的记录快照。
        异常：无权限或底层写入失败时由具体适配器抛出。
        副作用：在目标智能表格中新增记录。
        """
        ...

    def update_record(self, record_id: str, fields: Mapping[str, object]) -> SmartTableRecord:
        """仅更新传入字段补丁，禁止用整行数据覆盖既有记录。

        参数：record_id 为目标记录标识；fields 为本轮字段补丁。
        返回：更新后的记录快照。
        异常：记录不存在、无权限或底层写入失败时由具体适配器抛出。
        副作用：修改目标记录的传入字段。
        """
        ...
