"""新增销售隔离的需求进度会话与消息窗口关联。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0033_lead_progress_sessions"
down_revision = "0032_wecom_quote_routing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建进度会话表及消息关联表，并约束每名销售最多一个活跃会话。

    参数：无。
    返回值：无。
    异常：数据库 DDL 失败时由 Alembic 回滚。
    副作用：新增两张进度统计表和对应索引，不回填既有消息或线索。
    """
    op.create_table(
        "lead_progress_sessions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_activity_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_report_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("close_reason", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('active', 'closed', 'disabled')",
            name="ck_lead_progress_session_status",
        ),
        sa.ForeignKeyConstraint(["sales_user_id"], ["sales_authorizations.wecom_user_id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_lead_progress_sessions_sales_user_id",
        "lead_progress_sessions",
        ["sales_user_id"],
        unique=False,
    )
    op.create_index(
        "ix_lead_progress_sessions_due",
        "lead_progress_sessions",
        ["status", "next_report_at"],
        unique=False,
    )
    op.create_index(
        "uq_lead_progress_sessions_active_sales_user",
        "lead_progress_sessions",
        ["sales_user_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
        sqlite_where=sa.text("status = 'active'"),
    )
    op.create_table(
        "lead_progress_messages",
        sa.Column("message_id", sa.String(length=128), nullable=False),
        sa.Column("progress_session_id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["incoming_messages.message_id"]),
        sa.ForeignKeyConstraint(["progress_session_id"], ["lead_progress_sessions.id"]),
        sa.PrimaryKeyConstraint("message_id"),
    )
    op.create_index(
        "ix_lead_progress_messages_progress_session_id",
        "lead_progress_messages",
        ["progress_session_id"],
        unique=False,
    )


def downgrade() -> None:
    """删除进度窗口关联和会话表。

    参数：无。
    返回值：无。
    异常：数据库 DDL 失败时由 Alembic 回滚。
    副作用：显式降级时删除进度窗口及其历史关联数据。
    """
    op.drop_index(
        "ix_lead_progress_messages_progress_session_id",
        table_name="lead_progress_messages",
    )
    op.drop_table("lead_progress_messages")
    op.drop_index(
        "uq_lead_progress_sessions_active_sales_user",
        table_name="lead_progress_sessions",
    )
    op.drop_index("ix_lead_progress_sessions_due", table_name="lead_progress_sessions")
    op.drop_index(
        "ix_lead_progress_sessions_sales_user_id",
        table_name="lead_progress_sessions",
    )
    op.drop_table("lead_progress_sessions")
