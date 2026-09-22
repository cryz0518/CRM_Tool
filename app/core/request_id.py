"""服务端 request id 的校验、规范化和 fail-closed 生成。"""

from __future__ import annotations

import re
from uuid import uuid4

_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def normalize_request_id(value: str | None) -> str:
    """校验客户端请求标识，非法或不可信时生成新的服务端标识。

    参数：value 为 HTTP 请求中可能存在的 X-Request-ID 值。
    返回值：合法且长度不超过 128 的规范化标识，或新生成的 UUID。
    异常：无；任何不可信输入都会安全降级为新标识。
    副作用：无外部调用。
    """
    normalized = value.strip() if value is not None else ""
    has_control_character = value is not None and any(ord(character) < 32 for character in value)
    if (
        not normalized
        or has_control_character
        or _REQUEST_ID_PATTERN.fullmatch(normalized) is None
    ):
        # 不把攻击者提供的控制字符、超长值或结构化内容带入日志和审计。
        return str(uuid4())
    return normalized
