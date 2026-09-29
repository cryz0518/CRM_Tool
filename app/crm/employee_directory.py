"""从受控员工目录解析 CRM SOP owner。"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path


class EmployeeDirectoryError(RuntimeError):
    """表示员工目录缺失、格式错误或 owner 无法唯一解析。"""


@dataclass(frozen=True)
class Employee:
    """保存 CRM headerId 解析所需的最小员工事实。"""

    employee_id: str
    name: str
    nickname: str


class EmployeeDirectory:
    """通过 employee.csv 提供确定性的姓名/昵称到 employee.id 解析。"""

    def __init__(self, path: str | Path) -> None:
        """加载员工目录；CRM Adapter 不直接读取目录文件。"""
        self._path = Path(path)
        try:
            with self._path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
        except (OSError, csv.Error) as error:
            raise EmployeeDirectoryError("员工目录不可用") from error
        if not rows or set(rows[0]) != {"id", "name", "nickname"}:
            raise EmployeeDirectoryError("员工目录字段不完整")
        self._employees = tuple(
            Employee(str(row["id"]).strip(), str(row["name"]).strip(), str(row["nickname"]).strip())
            for row in rows
            if str(row["id"]).strip() and (str(row["name"]).strip() or str(row["nickname"]).strip())
        )

    def resolve(self, owner: str) -> str:
        """按姓名、花名或姓名（花名）组合精确解析唯一 employee.id。

        参数：owner 为智能表格成员显示名，可为姓名、花名或企微返回的组合显示文本。
        返回值：唯一员工的 employee.id。
        异常：姓名/花名缺失、组合不一致或匹配多个员工时抛出 EmployeeDirectoryError。
        副作用：无，仅读取已加载的员工目录。
        """
        normalized = owner.strip()
        display_name = normalized
        display_nickname: str | None = None
        # 企微成员单元格可能展示为“姓名(花名)”或“姓名（花名）”；先拆出姓名，花名只用于重名消歧。
        combined = re.fullmatch(r"(.+?)[(（]([^()（）]+)[)）]", normalized)
        if combined is not None:
            display_name, display_nickname = (part.strip() for part in combined.groups())

        name_matches = {
            employee.employee_id
            for employee in self._employees
            if display_name and display_name == employee.name
        }
        if len(name_matches) == 1:
            return next(iter(name_matches))
        if len(name_matches) > 1 and display_nickname:
            nickname_matches = {
                employee.employee_id
                for employee in self._employees
                if employee.employee_id in name_matches and employee.nickname == display_nickname
            }
            if len(nickname_matches) == 1:
                return next(iter(nickname_matches))

        # 没有姓名命中时，允许单独输入花名进行唯一精确匹配；不进行模糊或跨字段猜测。
        matches = {
            employee.employee_id
            for employee in self._employees
            if normalized and normalized == employee.nickname
        }
        if len(matches) != 1:
            raise EmployeeDirectoryError("负责人无法唯一匹配")
        return next(iter(matches))
