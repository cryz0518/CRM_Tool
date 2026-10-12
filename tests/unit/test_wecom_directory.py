"""测试企微通讯录成员解析。"""

from __future__ import annotations

import json
import subprocess

import pytest

from app.messaging.wecom_directory import (
    WecomDirectoryError,
    WecomDirectoryMember,
    resolve_wecom_member,
)


def test_resolve_wecom_member_requires_unique_exact_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证查询会检查完整列表，并只接受唯一的精确姓名。"""
    calls: list[list[str]] = []

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "users": [
                        {"userid": "member-1", "name": "王明", "departments": ["研发部"]},
                        {"userid": "member-2", "name": "王小明", "departments": ["销售部"]},
                    ],
                    "hint": "",
                },
                ensure_ascii=False,
            ),
            "",
        )

    monkeypatch.setattr("app.messaging.wecom_directory.subprocess.run", run)

    assert resolve_wecom_member("wecom-cli", " 王明 ", 20) == WecomDirectoryMember(
        "member-1", "王明"
    )
    assert calls[0][1:5] == ["contact", "users", "search", "--json"]
    assert json.loads(calls[0][5]) == {"keywords": ["王明"], "search_mode": "list"}


def test_resolve_wecom_member_rejects_duplicate_exact_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证企微通讯录中同名成员不会被任意选中。"""
    response = {
        "users": [
            {"userid": "member-1", "name": "王明"},
            {"userid": "member-2", "name": "王明"},
        ]
    }
    monkeypatch.setattr(
        "app.messaging.wecom_directory.subprocess.run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, 0, json.dumps(response, ensure_ascii=False), ""
        ),
    )

    with pytest.raises(ValueError, match="存在同名成员"):
        resolve_wecom_member("wecom-cli", "王明", 20)


def test_resolve_wecom_member_rejects_truncated_directory_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证通讯录提示结果不完整时不授予姓名匹配权限。"""
    monkeypatch.setattr(
        "app.messaging.wecom_directory.subprocess.run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command,
            0,
            json.dumps({"users": [{"userid": "member-1", "name": "王明"}], "hint": "结果受限"}),
            "",
        ),
    )

    with pytest.raises(WecomDirectoryError, match="结果不完整"):
        resolve_wecom_member("wecom-cli", "王明", 20)
