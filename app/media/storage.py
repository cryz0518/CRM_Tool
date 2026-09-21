"""媒体对象存储的供应商无关契约及开发/测试实现。"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import uuid4


class StorageDeleteOutcome(StrEnum):
    """定义远端删除可以被安全确认的三种结果。"""

    CONFIRMED_DELETED = "confirmed_deleted"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class StorageCapabilities:
    """描述存储实现是否满足生产安全所需的基础能力。"""

    private_storage: bool
    tls: bool
    server_side_encryption: bool
    signed_url: bool
    head: bool
    delete: bool


@dataclass(frozen=True)
class GeneratedObjectKey:
    """封装由存储边界生成的不透明键，业务层不能自行拼接路径。"""

    value: str


@dataclass(frozen=True)
class StoredObject:
    """描述成功写入存储提供器的非敏感对象引用和元数据。"""

    storage_key: str
    size_bytes: int | None = None
    content_type: str | None = None
    sha256: str | None = None
    etag: str | None = None
    encryption_mode: str | None = None


@dataclass(frozen=True)
class StorageObjectMetadata:
    """描述 head/stat 返回的稳定对象事实。"""

    storage_key: str
    size_bytes: int | None = None
    content_type: str | None = None
    etag: str | None = None
    encryption_mode: str | None = None


@dataclass(frozen=True)
class SignedDownloadURL:
    """描述签名器实际采用的短时 URL 及其有效期事实。"""

    url: str
    effective_expires_at: datetime
    effective_ttl_seconds: int


@dataclass(frozen=True)
class StorageDeleteResult:
    """描述删除调用的可确认结果，不把 unknown 伪装成成功。"""

    storage_key: str
    outcome: StorageDeleteOutcome


class StorageProvider(Protocol):
    """定义二进制工件的独立持久化边界。"""

    @property
    def capabilities(self) -> StorageCapabilities:
        """返回当前实现声明的安全和恢复能力。"""

    def create_object_key(self, *, suffix: str = "") -> GeneratedObjectKey:
        """生成带固定命名空间且不可预测的不透明对象键。"""

    def put(
        self,
        content: bytes,
        *,
        suffix: str = "",
        object_key: GeneratedObjectKey | None = None,
        content_type: str | None = None,
        sha256: str | None = None,
    ) -> StoredObject:
        """保存内容并返回稳定的对象元数据。"""

    def get(self, storage_key: str) -> bytes:
        """读取此前由本提供器保存的内容。"""

    def head(self, storage_key: str) -> StorageObjectMetadata | None:
        """读取对象元数据；对象不存在时返回 None。"""

    def get_metadata(self, storage_key: str) -> StorageObjectMetadata | None:
        """读取对象元数据的语义别名，供恢复流程调用。"""

    def delete(self, storage_key: str) -> StorageDeleteResult:
        """删除对象并明确返回已确认、已不存在或未知结果。"""

    def generate_signed_download_url(
        self, storage_key: str, *, expires_in_seconds: int, download: bool
    ) -> SignedDownloadURL:
        """为私有对象生成短时下载地址。"""


class SignedURLProvider(Protocol):
    """定义附件预览和下载所需的私有签名地址边界。"""

    def create_signed_url(
        self,
        storage_key: str,
        *,
        expires_in_seconds: int,
        download: bool,
    ) -> SignedDownloadURL:
        """为单个私有对象生成带有效期的签名地址。"""


class LocalVolumeStorageProvider:
    """将开发工件保存到 Docker 持久卷目录，不宣称生产加密或签名能力。"""

    _KEY_PATTERN = re.compile(r"^artifacts/[0-9a-f]{32}(?:\.[a-z0-9]+)?$")
    provider_name = "local"

    def __init__(self, root: Path) -> None:
        """保存本地根目录，并在首次使用前创建目录。"""
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def capabilities(self) -> StorageCapabilities:
        """返回本地实现的能力，明确不满足生产签名和 at-rest encryption。"""
        return StorageCapabilities(True, True, False, False, True, True)

    def create_object_key(self, *, suffix: str = "") -> GeneratedObjectKey:
        """以 UUID4 生成固定 artifacts 命名空间下的不透明键。"""
        safe_suffix = self._safe_suffix(suffix)
        return GeneratedObjectKey(f"artifacts/{uuid4().hex}{safe_suffix}")

    def put(
        self,
        content: bytes,
        *,
        suffix: str = "",
        object_key: GeneratedObjectKey | None = None,
        content_type: str | None = None,
        sha256: str | None = None,
    ) -> StoredObject:
        """以预生成或新生成的 opaque key 写入内容并返回元数据。"""
        del content_type
        key = object_key or self.create_object_key(suffix=suffix)
        self._validate_key(key.value)
        target = self._path_for(key.value)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        digest = sha256 or hashlib.sha256(content).hexdigest()
        return StoredObject(
            storage_key=key.value,
            size_bytes=len(content),
            sha256=digest,
            etag=digest,
        )

    def get(self, storage_key: str) -> bytes:
        """读取不透明键对应内容，非法路径键会被拒绝。"""
        return self._path_for(storage_key).read_bytes()

    def head(self, storage_key: str) -> StorageObjectMetadata | None:
        """读取本地对象大小和摘要；对象不存在时返回 None。"""
        target = self._path_for(storage_key)
        if not target.is_file():
            return None
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        return StorageObjectMetadata(
            storage_key=storage_key,
            size_bytes=target.stat().st_size,
            etag=digest,
        )

    def get_metadata(self, storage_key: str) -> StorageObjectMetadata | None:
        """返回与 head 相同的本地对象元数据。"""
        return self.head(storage_key)

    def delete(self, storage_key: str) -> StorageDeleteResult:
        """删除本地对象并区分已删除和原本不存在。"""
        target = self._path_for(storage_key)
        existed = target.is_file()
        target.unlink(missing_ok=True)
        return StorageDeleteResult(
            storage_key=storage_key,
            outcome=(
                StorageDeleteOutcome.CONFIRMED_DELETED
                if existed
                else StorageDeleteOutcome.NOT_FOUND
            ),
        )

    def generate_signed_download_url(
        self, storage_key: str, *, expires_in_seconds: int, download: bool
    ) -> SignedDownloadURL:
        """拒绝本地 Volume 生成伪造的生产下载地址。"""
        del storage_key, expires_in_seconds, download
        raise RuntimeError("local_storage_signed_url_unsupported")

    def _path_for(self, storage_key: str) -> Path:
        """将不透明键解析为根目录内路径，阻止路径穿越和任意路径调用。"""
        self._validate_key(storage_key)
        candidate = (self._root / storage_key).resolve()
        if self._root.resolve() not in candidate.parents:
            raise ValueError("storage_key_invalid")
        return candidate

    @classmethod
    def _validate_key(cls, storage_key: str) -> None:
        """校验键只能是本服务生成的 artifacts UUID 形式。"""
        if not cls._KEY_PATTERN.fullmatch(storage_key):
            raise ValueError("storage_key_invalid")

    @staticmethod
    def _safe_suffix(suffix: str) -> str:
        """只保留受控扩展名，避免调用方注入路径或业务文本。"""
        return suffix if re.fullmatch(r"\.[a-z0-9]{1,8}", suffix) else ""


class FakeStorageProvider(LocalVolumeStorageProvider):
    """提供测试用内存式签名地址能力，避免引入任何云厂商 SDK。"""

    provider_name = "fake"

    @property
    def capabilities(self) -> StorageCapabilities:
        """声明 fake 在测试中可验证的完整安全能力。"""
        return StorageCapabilities(True, True, True, True, True, True)

    def generate_signed_download_url(
        self, storage_key: str, *, expires_in_seconds: int, download: bool
    ) -> SignedDownloadURL:
        """返回不写入数据库或日志的测试签名地址。"""
        self._validate_key(storage_key)
        disposition = "attachment" if download else "inline"
        effective_ttl = expires_in_seconds
        return SignedDownloadURL(
            url=f"https://fake-storage.invalid/{storage_key}?ttl={effective_ttl}&mode={disposition}",
            effective_expires_at=datetime.now(UTC) + timedelta(seconds=effective_ttl),
            effective_ttl_seconds=effective_ttl,
        )

    def create_signed_url(
        self,
        storage_key: str,
        *,
        expires_in_seconds: int,
        download: bool,
    ) -> SignedDownloadURL:
        """适配 T15 Break-glass 的现有签名接口。"""
        return self.generate_signed_download_url(
            storage_key, expires_in_seconds=expires_in_seconds, download=download
        )
