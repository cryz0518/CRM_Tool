"""新增企业微信引用消息路由所需的会话字段和解析事实表。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0032_wecom_quote_routing"
down_revision = "0031_sales_lead_context_sequence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """新增 IncomingMessage 会话字段和 MessageQuoteResolution 表。

    参数：无，Alembic 通过当前数据库连接执行 DDL。
    返回值：无。
    异常：数据库结构错误由 Alembic 或数据库驱动抛出。
    副作用：仅修改当前执行环境的 schema，不调用任何业务外部依赖。
    """
    op.add_column("incoming_messages", sa.Column("chat_id", sa.String(length=128), nullable=True))
    op.add_column(
        "incoming_messages", sa.Column("chat_type", sa.String(length=32), nullable=True)
    )
    op.create_index("ix_incoming_messages_chat_id", "incoming_messages", ["chat_id"])
    op.create_index("ix_incoming_messages_chat_type", "incoming_messages", ["chat_type"])
    op.create_table(
        "message_quote_resolutions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("current_message_id", sa.String(length=128), nullable=False),
        sa.Column("quoted_source_message_id", sa.String(length=128), nullable=True),
        sa.Column("resolution_status", sa.String(length=32), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        sa.Column("matched_by", sa.String(length=32), nullable=True),
        sa.Column("conflict_code", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["current_message_id"], ["incoming_messages.message_id"]),
        sa.ForeignKeyConstraint(
            ["quoted_source_message_id"], ["incoming_messages.message_id"]
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("current_message_id"),
    )
    op.create_index(
        "ix_message_quote_resolutions_quoted_source",
        "message_quote_resolutions",
        ["quoted_source_message_id"],
    )


def downgrade() -> None:
    """删除引用解析表及本迁移新增的 IncomingMessage 字段。

    参数：无，Alembic 通过当前数据库连接执行 DDL。
    返回值：无。
    异常：数据库结构错误由 Alembic 或数据库驱动抛出。
    副作用：仅删除本迁移创建的 schema 对象，不访问业务数据或外部系统。
    """
    op.drop_index(
        "ix_message_quote_resolutions_quoted_source", table_name="message_quote_resolutions"
    )
    op.drop_table("message_quote_resolutions")
    op.drop_index("ix_incoming_messages_chat_type", table_name="incoming_messages")
    op.drop_index("ix_incoming_messages_chat_id", table_name="incoming_messages")
    op.drop_column("incoming_messages", "chat_type")
    op.drop_column("incoming_messages", "chat_id")
