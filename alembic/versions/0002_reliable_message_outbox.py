"""建立可靠消息接收、销售授权与事务发件箱表。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0002_reliable_message_outbox"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建销售授权、消息、发件箱、通知及消息审计表。

    参数：无。
    返回值：无。
    异常：数据库 DDL 失败时由 Alembic 抛出并回滚事务。
    副作用：创建 T02 的持久化结构和索引。
    """
    # 销售目录是生产授权权威来源，并保存每位销售的持久化消息顺序。
    op.create_table(
        "sales_authorizations",
        sa.Column("wecom_user_id", sa.String(length=128), primary_key=True),
        sa.Column("display_name", sa.String(length=128), nullable=True),
        sa.Column("department_id", sa.String(length=128), nullable=True),
        sa.Column("crm_user_id", sa.String(length=128), nullable=True),
        sa.Column("is_authorized", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("next_message_sequence", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    # 消息原文和标准化文本仅在授权成功后保存，message_id 是接收幂等键。
    op.create_table(
        "incoming_messages",
        sa.Column("message_id", sa.String(length=128), primary_key=True),
        sa.Column("sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("normalized_text", sa.String(), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["sales_user_id"], ["sales_authorizations.wecom_user_id"]),
        sa.UniqueConstraint("sales_user_id", "sequence"),
    )
    op.create_index("ix_incoming_messages_sales_user_id", "incoming_messages", ["sales_user_id"])
    # Worker 只能从本表读取待处理事件，外部调用不允许处于接收事务中。
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("message_id", sa.String(length=128), nullable=False, unique=True),
        sa.Column("sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column(
            "event_type", sa.String(length=64), nullable=False, server_default="message_received"
        ),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["incoming_messages.message_id"]),
        sa.UniqueConstraint("message_id", "event_type"),
    )
    op.create_index("ix_outbox_events_sales_user_id", "outbox_events", ["sales_user_id"])
    # 通知记录独立于业务 Outbox，未授权成员不会因此进入 AI 或 CRM 处理链路。
    op.create_table(
        "notification_records",
        sa.Column("notification_key", sa.String(length=64), primary_key=True),
        sa.Column("sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("source_message_id", sa.String(length=128), nullable=False),
        sa.Column("notification_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("provider_message_id", sa.String(length=128), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_notification_records_sales_user_id", "notification_records", ["sales_user_id"]
    )
    op.create_table(
        "business_audit_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("message_id", sa.String(length=128), nullable=False),
        sa.Column("sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("message_id", "event_type"),
    )
    op.create_index(
        "ix_business_audit_events_sales_user_id", "business_audit_events", ["sales_user_id"]
    )


def downgrade() -> None:
    """按依赖逆序删除 T02 建立的可靠消息接收表。

    参数：无。
    返回值：无。
    异常：数据库 DDL 失败时由 Alembic 抛出。
    副作用：不可恢复地删除 T02 数据，仅用于显式迁移回退。
    """
    op.drop_index("ix_business_audit_events_sales_user_id", table_name="business_audit_events")
    op.drop_table("business_audit_events")
    op.drop_index("ix_notification_records_sales_user_id", table_name="notification_records")
    op.drop_table("notification_records")
    op.drop_index("ix_outbox_events_sales_user_id", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_index("ix_incoming_messages_sales_user_id", table_name="incoming_messages")
    op.drop_table("incoming_messages")
    op.drop_table("sales_authorizations")
