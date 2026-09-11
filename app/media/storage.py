"""开发和测试环境使用的本地 Docker Volume 存储实现。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import uuid4


@dataclass(frozen=True)
class StoredObject:
    """描述成功写入存储提供器的非敏感对象引用。"""

    storage_key: str


class StorageProvider(Protocol):
    """定义二进制工件的独立持久化边界。"""

    def put(self, content: bytes, *, suffix: str = "") -> StoredObject:
        """保存内容并返回不透明存储键。"""

    def get(self, storage_key: str) -> bytes:
        """读取此前由本提供器保存的内容。"""

    def delete(self, storage_key: str) -> None:
        """删除此前保存的对象。"""


class LocalVolumeStorageProvider:
    """将开发和测试工件保存到 Docker 持久卷目录。"""

    def __init__(self, root: Path) -> None:
        """保存本地根目录，并在首次使用前创建目录。"""
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    def put(self, content: bytes, *, suffix: str = "") -> StoredObject:
        """以 UUID 键写入内容，避免文件名或业务标识进入存储键。"""
        # 仅保留受控后缀，避免调用方通过路径片段逃逸出 Docker Volume。
        safe_suffix = suffix if suffix.startswith(".") and "/" not in suffix else ""
        storage_key = f"artifacts/{uuid4().hex}{safe_suffix}"
        target = self._root / storage_key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        return StoredObject(storage_key=storage_key)

    def get(self, storage_key: str) -> bytes:
        """读取不透明键对应内容，非法路径键会被拒绝。"""
        return self._path_for(storage_key).read_bytes()

    def delete(self, storage_key: str) -> None:
        """删除不透明键对应对象，不存在时保持幂等。"""
        self._path_for(storage_key).unlink(missing_ok=True)

    def _path_for(self, storage_key: str) -> Path:
        """将不透明键解析为根目录内路径，阻止路径穿越。"""
        candidate = (self._root / storage_key).resolve()
        if self._root.resolve() not in candidate.parents:
            raise ValueError("storage_key_invalid")
        return candidate
