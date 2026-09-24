"""从受控员工目录解析 CRM SOP owner。"""

from __future__ import annotations

import csv
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
        """按姓名或昵称精确解析唯一 employee.id，歧义或缺失均拒绝。"""
        normalized = owner.strip()
        matches = {
            employee.employee_id
            for employee in self._employees
            if normalized and (normalized == employee.name or normalized == employee.nickname)
        }
        if len(matches) != 1:
            raise EmployeeDirectoryError("负责人无法唯一匹配")
        return next(iter(matches))
