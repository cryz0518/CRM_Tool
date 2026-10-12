"""通过 wecom-cli 查询企业微信通讯录中的成员。"""

from __future__ import annotations

import json
import shlex
import subprocess
from dataclasses import dataclass
from typing import Mapping


class WecomDirectoryError(RuntimeError):
    """表示企微通讯录查询失败或结果不完整，不能确认成员身份。"""


@dataclass(frozen=True)
class WecomDirectoryMember:
    """保存已由企微通讯录确认的成员姓名和内部标识。"""

    user_id: str
    name: str


def resolve_wecom_member(
    command: str, employee_name: str, timeout_seconds: float
) -> WecomDirectoryMember:
    """按企微通讯录中的精确姓名解析唯一成员。

    参数：command 为 wecom-cli 可执行命令；employee_name 为待授权成员姓名；
    timeout_seconds 为通讯录查询超时时间。
    返回值：唯一匹配的企微成员事实。
    异常：姓名无效、成员未找到或重名时抛 ValueError；CLI 不可用或结果不完整时抛
    WecomDirectoryError。
    副作用：调用 wecom-cli 查询通讯录，不修改目录或发送消息。
    """
    normalized_name = employee_name.strip()
    if not normalized_name or len(normalized_name) > 128:
        raise ValueError("企微员工姓名不能为空且不能超过 128 个字符")
    command_parts = shlex.split(command)
    if not command_parts:
        raise WecomDirectoryError("企微通讯录查询命令未配置")

    # 列出完整匹配结果后再做精确姓名比对，避免最相关候选掩盖重名成员。
    arguments = {
        "keywords": [normalized_name],
        "search_mode": "list",
    }
    try:
        completed = subprocess.run(
            [
                *command_parts,
                "contact",
                "users",
                "search",
                "--json",
                json.dumps(arguments, ensure_ascii=False),
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise WecomDirectoryError("企微通讯录查询失败，请确认 wecom-cli 可用并已授权") from error
    if completed.returncode != 0:
        raise WecomDirectoryError("企微通讯录查询失败，请检查 wecom-cli 授权状态")

    try:
        response = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise WecomDirectoryError("企微通讯录返回内容不是有效 JSON") from error
    if not isinstance(response, Mapping) or not isinstance(response.get("users"), list):
        raise WecomDirectoryError("企微通讯录返回内容缺少成员列表")
    # 结果被服务端截断时无法证明姓名唯一，因此不授予任何成员权限。
    if response.get("hint"):
        raise WecomDirectoryError("企微通讯录结果不完整，无法确认姓名唯一性")

    matches: dict[str, str] = {}
    for item in response["users"]:
        if not isinstance(item, Mapping):
            raise WecomDirectoryError("企微通讯录成员格式无效")
        user_id, name = item.get("userid"), item.get("name")
        if not isinstance(user_id, str) or not user_id.strip() or not isinstance(name, str):
            raise WecomDirectoryError("企微通讯录成员缺少必要信息")
        if name.strip() == normalized_name:
            matches[user_id.strip()] = name.strip()

    if not matches:
        raise ValueError("企微通讯录中未找到该姓名的成员")
    if len(matches) != 1:
        raise ValueError("企微通讯录中存在同名成员，请先确认唯一姓名")
    user_id, matched_name = next(iter(matches.items()))
    return WecomDirectoryMember(user_id, matched_name)
