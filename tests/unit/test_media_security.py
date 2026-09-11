"""媒体安全、存储与识别补充的应用边界测试。"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from app.media.service import MediaValidationError, MediaValidator
from app.media.storage import LocalVolumeStorageProvider


def test_validator_accepts_png_with_matching_declared_type_and_digest() -> None:
    """验证 PNG 内容、声明类型和 SHA-256 一致时可安全进入后续处理。"""
    content = b"\x89PNG\r\n\x1a\nimage-content"

    artifact = MediaValidator(image_mime_types=("image/png",), audio_mime_types=()).validate(
        content, declared_mime_type="image/png", media_kind="image"
    )

    assert artifact.detected_mime_type == "image/png"
    assert artifact.sha256 == hashlib.sha256(content).hexdigest()


def test_validator_rejects_declared_type_that_disagrees_with_real_content() -> None:
    """验证伪装为图片的音频不会进入 OCR 或其他 AI 链路。"""
    with pytest.raises(MediaValidationError, match="mime_type_mismatch"):
        MediaValidator(image_mime_types=("image/png",), audio_mime_types=()).validate(
            b"ID3audio-content", declared_mime_type="image/png", media_kind="image"
        )


def test_local_volume_storage_uses_opaque_key_and_round_trips_content(tmp_path: Path) -> None:
    """验证开发存储以不含原始文件名的键持久化二进制内容。"""
    storage = LocalVolumeStorageProvider(tmp_path)

    stored = storage.put(b"secret-media", suffix=".png")

    assert "secret" not in stored.storage_key
    assert storage.get(stored.storage_key) == b"secret-media"
