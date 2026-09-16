"""Console 默认展示的字段级脱敏策略。"""

from __future__ import annotations

import re

_EMAIL_PATTERN = re.compile(r"(?P<local>[^\s@]+)@(?P<domain>[^\s@]+)")
_PHONE_PATTERN = re.compile(r"(?<!\d)(?P<number>\+?[0-9][0-9 -]{6,22}[0-9])(?!\d)")
_LABELED_NAME_PATTERN = re.compile(
    r"(?P<label>联系人姓名|客户联系人|联系人|姓名|客户是|叫)\s*[:：]?\s*"
    r"(?P<name>[\u4e00-\u9fff]{2,4}|[A-Za-z][A-Za-z .'-]{1,39})"
)
_SENSITIVE_PATTERN = re.compile(
    r"(?i)(?:(?<!\d)(?:\d{17}[\dXx]|\d{16,19}|\d{15})(?!\d)|"
    r"(?:密码|口令|验证码|password|token)\s*[:：]?\s*[^\s；;，,]+)"
)


class MaskingPolicy:
    """集中执行默认 DTO 的保守脱敏，失败时不扩大数据可见范围。"""

    _PHONE_FIELDS = frozenset({"手机", "手机号", "电话", "phone", "mobile"})
    _EMAIL_FIELDS = frozenset({"邮箱", "email"})
    _NAME_FIELDS = frozenset({"联系人", "姓名", "name", "contact"})

    def mask_field(self, field_name: str, value: str | None) -> str | None:
        """按 CRM 字段语义脱敏单个值。

        参数：field_name 为字段名；value 为待展示的字段值。
        返回值：适合默认 Console 展示的脱敏值；空输入返回 None。
        异常：无。
        副作用：无，不修改传入对象。
        """
        if value is None:
            return None
        if field_name in self._PHONE_FIELDS:
            return self.mask_phone(value)
        if field_name in self._EMAIL_FIELDS:
            return self.mask_email(value)
        if field_name in self._NAME_FIELDS:
            return self.mask_name(value)
        return self.mask_text(value)

    def mask_text(self, value: str | None) -> str | None:
        """遮蔽文本中的邮箱、电话号码和明显高敏感凭据。

        参数：value 为待展示文本。
        返回值：替换敏感片段后的文本；空输入返回 None。
        异常：无。
        副作用：无。
        """
        if value is None:
            return None
        masked = _SENSITIVE_PATTERN.sub("[已遮蔽]", value)
        masked = _EMAIL_PATTERN.sub(self._replace_email, masked)
        masked = _LABELED_NAME_PATTERN.sub(self._replace_labeled_name, masked)
        return _PHONE_PATTERN.sub(self._replace_phone, masked)

    @staticmethod
    def mask_phone(value: str) -> str:
        """保留电话号码首三尾四位，遮蔽中间数字。

        参数：value 为电话号码文本。
        返回值：保留少量定位信息的脱敏号码。
        异常：无。
        副作用：无。
        """
        digits = re.sub(r"\D", "", value)
        if len(digits) <= 7:
            return "*" * len(digits)
        return f"{digits[:3]}{'*' * (len(digits) - 7)}{digits[-4:]}"

    @staticmethod
    def mask_email(value: str) -> str:
        """保留邮箱首字符和域名，遮蔽本地部分其余内容。

        参数：value 为邮箱文本。
        返回值：邮箱脱敏文本；无法拆分时返回固定遮蔽值。
        异常：无。
        副作用：无。
        """
        match = _EMAIL_PATTERN.fullmatch(value.strip())
        if match is None:
            return "[已遮蔽邮箱]"
        local = match.group("local")
        return f"{local[0]}***@{match.group('domain')}"

    @staticmethod
    def mask_name(value: str) -> str:
        """保留联系人首尾字符并遮蔽中间字符。

        参数：value 为联系人姓名。
        返回值：姓名脱敏文本。
        异常：无。
        副作用：无。
        """
        stripped = value.strip()
        if len(stripped) <= 1:
            return "*"
        if len(stripped) == 2:
            return f"{stripped[0]}*"
        return f"{stripped[0]}{'*' * (len(stripped) - 2)}{stripped[-1]}"

    @staticmethod
    def _replace_email(match: re.Match[str]) -> str:
        """将文本正则捕获的邮箱替换为脱敏值。"""
        return MaskingPolicy.mask_email(match.group(0))

    @staticmethod
    def _replace_phone(match: re.Match[str]) -> str:
        """将文本正则捕获的电话号码替换为脱敏值。"""
        return MaskingPolicy.mask_phone(match.group("number"))

    @staticmethod
    def _replace_labeled_name(match: re.Match[str]) -> str:
        """将自由文本中带联系人标签的姓名替换为脱敏值。"""
        return f"{match.group('label')}{MaskingPolicy.mask_name(match.group('name'))}"
