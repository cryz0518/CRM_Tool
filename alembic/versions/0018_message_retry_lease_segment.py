"""为 T14 失败消息补充重试增加分段定位和处理租约。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0018_message_retry_lease_segment"
down_revision = "0017_ticket14_failure_discard"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为既有失败重试事实补充分段唯一定位和崩溃恢复租约。"""
    # 既有 T14 数据全部属于默认单分段消息，先以 0 回填后再建立非空约束。
    op.add_column(
        "message_retry_attempts",
        sa.Column("segment_index", sa.Integer(), nullable=False, server_default="0"),
    )
    # processing 租约用于区分仍在执行的 retry 和可安全恢复的失联执行者。
    op.add_column(
        "message_retry_attempts",
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "message_retry_attempts",
        sa.Column("processing_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    # 原唯一键不允许同一消息的不同 segment 各自执行 retry，必须替换为分段级唯一键。
    op.drop_constraint(
        "message_retry_attempts_message_id_attempt_number_key",
        table_name="message_retry_attempts",
    )
    op.create_unique_constraint(
        "uq_message_retry_attempts_message_segment_attempt",
        "message_retry_attempts",
        ["message_id", "segment_index", "attempt_number"],
    )


def downgrade() -> None:
    """删除 T14 retry 分段与租约字段，保留 0017 及更早历史结构。"""
    op.drop_constraint(
        "uq_message_retry_attempts_message_segment_attempt",
        table_name="message_retry_attempts",
    )
    op.create_unique_constraint(
        "message_retry_attempts_message_id_attempt_number_key",
        "message_retry_attempts",
        ["message_id", "attempt_number"],
    )
    op.drop_column("message_retry_attempts", "processing_lease_expires_at")
    op.drop_column("message_retry_attempts", "processing_started_at")
    op.drop_column("message_retry_attempts", "segment_index")
