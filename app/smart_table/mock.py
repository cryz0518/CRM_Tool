"""用于开发、测试和 PoC 的内存智能表格适配器。"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from datetime import date, datetime
from zoneinfo import ZoneInfo

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
from app.smart_table.registry import READ_ONLY_SYSTEM_TIME_FIELDS


class MockSmartTableAdapter:
    """以进程内数据实现稳定契约，避免业务服务直接依赖企业微信 CLI。"""

    def __init__(
        self,
        *,
        schema: SmartTableSchema,
        sales_can_create_records: bool = False,
        sales_can_delete_records: bool = False,
        require_owner_field: bool = True,
    ) -> None:
        """初始化 Mock 的预配置结构、权限和空记录集。

        参数：schema 为管理员预配置结构；两个 sales 参数控制普通销售新增和删除权限；
        require_owner_field 控制机器人是否必须写入“负责人”，审计子表可关闭。
        副作用：创建独立的内存记录集与递增记录标识计数器。
        """
        self._schema = schema
        self._permissions = SmartTablePermissions(
            sales_can_create_records=sales_can_create_records,
            sales_can_delete_records=sales_can_delete_records,
        )
        self._require_owner_field = require_owner_field
        self._records: dict[str, SmartTableRecord] = {}
        self._next_record_number = 1
        self.delete_calls = 0

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

    def find_records_by_field_values(
        self, field_name: str, values: Collection[str]
    ) -> list[SmartTableRecord]:
        """在内存记录集中匹配一个字段的任意给定文本值。

        参数：field_name 为字段名；values 为待匹配文本值集合。
        返回值：字段值命中集合的记录快照。
        异常：无。
        副作用：无，仅读取 Mock 内存记录。
        """
        requested = set(values)
        return [
            record
            for record in self._records.values()
            if record.fields.get(field_name) in requested
        ]

    def get_records(
        self,
        record_ids: Collection[str] | None = None,
        *,
        owner_user_ids: Collection[str] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
        submission_status: str | None = None,
        capture_date: date | None = None,
    ) -> list[SmartTableRecord]:
        """返回按创建顺序保存的全部记录、指定记录或负责人范围快照。

        参数：record_ids 和负责人过滤最多提供一种；显示名模拟服务端预筛；提交状态和日期
        仅在负责人范围内筛选，缺少日期的行保留给来源消息回退。
        返回：当前请求范围内的内存记录快照列表。
        异常：同时提供两种过滤集合时抛出 ValueError。
        副作用：无；只从内存记录集中筛选。
        """
        if record_ids is not None and (
            owner_user_ids is not None
            or owner_display_names is not None
            or submission_status is not None
            or capture_date is not None
        ):
            raise ValueError("记录标识与负责人范围不能同时过滤")
        if (submission_status is not None or capture_date is not None) and not owner_user_ids:
            raise ValueError("提交状态和日期筛选必须指定负责人范围")
        if owner_display_names is not None and (
            owner_user_ids is None or set(owner_user_ids) != set(owner_display_names)
        ):
            raise ValueError("负责人显示名过滤必须同时提供负责人 user_id")
        if record_ids is not None:
            requested = set(record_ids)
            return [record for record in self._records.values() if record.record_id in requested]
        if owner_user_ids is not None:
            owners = set(owner_user_ids)
            names_by_owner = dict(owner_display_names or {})
            return [
                record
                for record in self._records.values()
                if record.fields.get("负责人") in owners
                and (
                    not names_by_owner
                    or record.member_names.get("负责人")
                    == names_by_owner.get(record.fields.get("负责人"))
                )
                and (
                    submission_status is None or record.fields.get("提交状态") == submission_status
                )
                and (
                    capture_date is None
                    or self._capture_date(record.fields) in {None, capture_date}
                )
            ]
        return list(self._records.values())

    @staticmethod
    def _capture_date(fields: Mapping[str, object]) -> date | None:
        """解析表格录入时间为上海自然日。

        参数：fields 为智能表格字段快照。
        返回值：可解析时返回自然日；缺失或无法解析时返回 None 供来源时间回退。
        异常：无；格式不支持时按缺失处理。
        副作用：无，仅转换快照中的时间值。
        """
        value = fields.get("录入时间") or fields.get("创建时间")
        if isinstance(value, datetime):
            local = value.astimezone(ZoneInfo("Asia/Shanghai")) if value.tzinfo else value
            return local.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value.strip():
            normalized = value.strip().replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                try:
                    return date.fromisoformat(normalized[:10])
                except ValueError:
                    return None
            local = parsed.astimezone(ZoneInfo("Asia/Shanghai")) if parsed.tzinfo else parsed
            return local.date()
        return None

    def create_record(
        self,
        fields: Mapping[str, object],
        *,
        actor: SmartTableActor,
        member_names: Mapping[str, str] | None = None,
    ) -> SmartTableRecord:
        """创建记录；机器人必须写负责人且不受销售新增开关影响。

        参数：fields 为待写入字段；actor 为发起操作的主体；member_names 为可选的成员显示名快照。
        返回：创建后的不可变记录快照。
        异常：销售无权限或写只读时间时抛出 SmartTablePermissionError；缺负责人抛出 ValueError。
        副作用：向内存记录集新增记录并递增记录编号。
        """
        # Mock 同样拒绝系统时间写入，让业务路径测试发现错误补丁而非静默吞掉。
        if any(name.removeprefix("*") in READ_ONLY_SYSTEM_TIME_FIELDS for name in fields):
            raise SmartTablePermissionError("系统创建时间字段只读，不能新增写值")
        # 销售关闭新增权限时，只有机器人或管理员能进入正式记录创建路径。
        if actor is SmartTableActor.SALES and not self._permissions.sales_can_create_records:
            raise SmartTablePermissionError("普通销售没有智能表格新增记录权限")
        # 负责人是共享表记录级权限的关键字段，机器人缺失时必须拒绝创建。
        if (
            actor is SmartTableActor.ROBOT
            and self._require_owner_field
            and not fields.get("负责人")
        ):
            raise ValueError("机器人新增智能表格记录时必须写入负责人")

        # 生成稳定的 Mock 记录标识，并复制输入避免调用方后续修改污染记录。
        record_id = f"mock-record-{self._next_record_number}"
        self._next_record_number += 1
        # 默认把 Mock 输入的负责人作为显示名，显式传入空映射可覆盖为“无可信显示名”。
        resolved_member_names = (
            dict(member_names)
            if member_names is not None
            else ({"负责人": str(fields["负责人"])} if fields.get("负责人") else {})
        )
        record = SmartTableRecord(
            record_id=record_id, fields=dict(fields), member_names=resolved_member_names
        )
        self._records[record_id] = record
        return record

    def create_records(
        self, records: Sequence[Mapping[str, object]], *, actor: SmartTableActor
    ) -> list[SmartTableRecord]:
        """通过现有 Mock 创建路径依次新增批量记录，保持单条校验语义。

        参数：records 为按请求顺序排列的字段映射；actor 为调用主体。
        返回值：按输入顺序创建的记录快照。
        异常：单条创建权限、字段或负责人校验失败时向调用方传播。
        副作用：向 Mock 内存记录集逐条添加记录。
        """
        return [self.create_record(fields, actor=actor) for fields in records]

    def update_record(
        self,
        record_id: str,
        fields: Mapping[str, object],
        *,
        skip_preflight: bool = False,
    ) -> SmartTableRecord:
        """合并字段补丁到原记录，仅修改本轮明确提供的字段。

        参数：record_id 为目标记录标识；fields 为本轮增量字段补丁；skip_preflight 表示调用方已持有
        刚读取或刚创建的记录快照，可跳过重复预读。
        返回：更新后的不可变记录快照。
        异常：记录缺失抛出 SmartTableRecordNotFoundError；写只读时间抛出 SmartTablePermissionError。
        副作用：替换内存记录集中的目标记录快照。
        """
        # 更新与新增同样拒绝系统时间，失败恢复也不能夹带旧后台审计值。
        if any(name.removeprefix("*") in READ_ONLY_SYSTEM_TIME_FIELDS for name in fields):
            raise SmartTablePermissionError("系统创建时间字段只读，不能更新写值")
        # 调用方持有新鲜快照时直接使用内存索引，模拟跳过适配器内重复预读。
        record = self._records.get(record_id) if skip_preflight else self.get_record(record_id)
        if record is None:
            raise SmartTableRecordNotFoundError(f"智能表格记录不存在：{record_id}")

        # 先复制旧字段再叠加补丁，严格模拟增量更新而非整行替换。
        updated_fields = dict(record.fields)
        updated_fields.update(fields)
        updated_record = SmartTableRecord(
            record_id=record_id,
            fields=updated_fields,
            member_names=record.member_names,
        )
        self._records[record_id] = updated_record
        return updated_record

    def update_records(
        self, records: Sequence[tuple[str, Mapping[str, object]]]
    ) -> list[SmartTableRecord]:
        """使用 Mock 单条更新规则处理批量字段补丁。

        参数：records 为记录标识与字段补丁组成的序列。
        返回值：按输入顺序排列的更新后记录快照。
        异常：单条记录缺失或字段校验失败时向调用方传播。
        副作用：逐条修改 Mock 内存记录。
        """
        return [
            self.update_record(record_id, fields)
            for record_id, fields in records
        ]

    def delete_record(self, _record_id: str, *, actor: SmartTableActor) -> None:
        """记录并拒绝物理删除调用，供 T14 断言逻辑废弃不删除审核表行。

        参数：_record_id 为待删除记录；actor 为调用主体。
        返回：无。
        异常：始终抛出 AssertionError，因为业务服务不应提供物理删除路径。
        副作用：递增 delete_calls，保留测试对错误调用的直接证据。
        """
        self.delete_calls += 1
        raise AssertionError(f"T14 禁止物理删除 Smart Table 记录：{actor}")
