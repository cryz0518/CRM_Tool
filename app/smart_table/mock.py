"""用于开发、测试和 PoC 的内存智能表格适配器。"""

from __future__ import annotations

from collections.abc import Mapping

from app.smart_table.adapter import (
    SmartTableActor,
    SmartTablePermissionError,
    SmartTableRecordNotFoundError,
)
from app.smart_table.models import (
    SmartTablePermissions,
    SmartTableRecord,
    SmartTableSchema,
)


class MockSmartTableAdapter:
    """以进程内数据实现稳定契约，避免业务服务直接依赖企业微信 CLI。"""

    def __init__(
        self,
        *,
        schema: SmartTableSchema,
        sales_can_create_records: bool = False,
        sales_can_delete_records: bool = False,
    ) -> None:
        """初始化 Mock 的预配置结构、权限和空记录集。

        参数：schema 为管理员预配置结构；两个 sales 参数控制普通销售新增和删除权限。
        副作用：创建独立的内存记录集与递增记录标识计数器。
        """
        self._schema = schema
        self._permissions = SmartTablePermissions(
            sales_can_create_records=sales_can_create_records,
            sales_can_delete_records=sales_can_delete_records,
        )
        self._records: dict[str, SmartTableRecord] = {}
        self._next_record_number = 1

    @property
    def sales_can_create_records(self) -> bool:
        """返回当前 Mock 的销售新增权限，便于 PoC 和配置测试明确断言。"""
        return self._permissions.sales_can_create_records

    def get_schema(self) -> SmartTableSchema:
        """返回管理员预配置的字段结构快照。

        返回：初始化时传入的不可变表结构。
        """
        return self._schema

    def get_permissions(self) -> SmartTablePermissions:
        """返回普通销售在当前 Mock 表中的受控权限。

        返回：初始化时传入的新增与删除权限快照。
        """
        return self._permissions

    def get_record(self, record_id: str) -> SmartTableRecord | None:
        """按记录标识返回快照，缺失记录不抛出底层实现异常。

        参数：record_id 为 Mock 创建的记录标识。
        返回：记录快照；缺失时返回 None。
        """
        return self._records.get(record_id)

    def find_records(self, filters: Mapping[str, object]) -> list[SmartTableRecord]:
        """以所有过滤字段精确匹配的方式查询 Mock 记录。

        参数：filters 为字段名和期望值组成的全部匹配条件。
        返回：符合所有条件的记录快照，保持创建顺序。
        """
        # 过滤条件必须全部成立，模拟业务层依赖的确定性公司名称查找语义。
        return [
            record
            for record in self._records.values()
            if all(record.fields.get(name) == value for name, value in filters.items())
        ]

    def get_records(self) -> list[SmartTableRecord]:
        """返回按创建顺序保存的全部记录快照。

        返回：当前内存记录集的快照列表。
        """
        return list(self._records.values())

    def create_record(
        self,
        fields: Mapping[str, object],
        *,
        actor: SmartTableActor,
    ) -> SmartTableRecord:
        """创建记录；机器人必须写负责人且不受销售新增开关影响。

        参数：fields 为待写入字段；actor 为发起操作的主体。
        返回：创建后的不可变记录快照。
        异常：销售无新增权限时抛出 SmartTablePermissionError；机器人未写负责人时抛出 ValueError。
        副作用：向内存记录集新增记录并递增记录编号。
        """
        # 销售关闭新增权限时，只有机器人或管理员能进入正式记录创建路径。
        if actor is SmartTableActor.SALES and not self._permissions.sales_can_create_records:
            raise SmartTablePermissionError("普通销售没有智能表格新增记录权限")
        # 负责人是共享表记录级权限的关键字段，机器人缺失时必须拒绝创建。
        if actor is SmartTableActor.ROBOT and not fields.get("负责人"):
            raise ValueError("机器人新增智能表格记录时必须写入负责人")

        # 生成稳定的 Mock 记录标识，并复制输入避免调用方后续修改污染记录。
        record_id = f"mock-record-{self._next_record_number}"
        self._next_record_number += 1
        record = SmartTableRecord(record_id=record_id, fields=dict(fields))
        self._records[record_id] = record
        return record

    def update_record(self, record_id: str, fields: Mapping[str, object]) -> SmartTableRecord:
        """合并字段补丁到原记录，仅修改本轮明确提供的字段。

        参数：record_id 为目标记录标识；fields 为本轮增量字段补丁。
        返回：更新后的不可变记录快照。
        异常：记录不存在时抛出 SmartTableRecordNotFoundError。
        副作用：替换内存记录集中的目标记录快照。
        """
        record = self.get_record(record_id)
        if record is None:
            raise SmartTableRecordNotFoundError(f"智能表格记录不存在：{record_id}")

        # 先复制旧字段再叠加补丁，严格模拟增量更新而非整行替换。
        updated_fields = dict(record.fields)
        updated_fields.update(fields)
        updated_record = SmartTableRecord(record_id=record_id, fields=updated_fields)
        self._records[record_id] = updated_record
        return updated_record
