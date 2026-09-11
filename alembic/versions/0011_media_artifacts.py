"""创建媒体工件和独立识别任务表。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0011_media_artifacts"
down_revision = "0010_human_edit_protection"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建消息附件元数据及 OCR/ASR 失败可审计任务表。"""
    # 媒体消息必须等待附件终态，防止 Worker 在下载尚未完成时提前忽略其补充文本。
    op.add_column(
        "incoming_messages",
        sa.Column(
            "requires_media_enrichment",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.create_table(
        "message_attachments",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("message_id", sa.String(length=128), nullable=False),
        sa.Column("media_kind", sa.String(length=16), nullable=False),
        sa.Column("declared_mime_type", sa.String(length=128)),
        sa.Column("detected_mime_type", sa.String(length=128)),
        sa.Column("size_bytes", sa.Integer()),
        sa.Column("sha256", sa.String(length=64), unique=True),
        sa.Column("storage_key", sa.String(length=256), unique=True),
        sa.Column("scan_status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column(
            "processing_status", sa.String(length=32), nullable=False, server_default="pending"
        ),
        sa.Column("recognized_text", sa.String()),
        sa.Column("error_summary", sa.String(length=128)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["message_id"], ["incoming_messages.message_id"]),
    )
    op.create_index("ix_message_attachments_message_id", "message_attachments", ["message_id"])
    op.create_table(
        "media_processing_tasks",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("attachment_id", sa.String(length=36), nullable=False),
        sa.Column("task_type", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_summary", sa.String(length=128)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["attachment_id"], ["message_attachments.id"]),
        sa.UniqueConstraint("attachment_id", "task_type"),
    )


def downgrade() -> None:
    """按依赖逆序删除媒体任务和消息附件表。"""
    op.drop_table("media_processing_tasks")
    op.drop_index("ix_message_attachments_message_id", table_name="message_attachments")
    op.drop_table("message_attachments")
    op.drop_column("incoming_messages", "requires_media_enrichment")
