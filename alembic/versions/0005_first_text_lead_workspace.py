"""建立 T05 首条文本线索审核工作区持久化结构。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0005_first_text_lead_workspace"
down_revision = "0004_audit_event_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建线索、字段来源和智能表格同步结果表。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚。
    副作用：为 T05 的首条文本线索创建持久化结构与查询索引。
    """
    # 原始采集销售和表格负责人显式分列，禁止未来以模糊 owner_id 覆盖语义。
    op.create_table(
        "leads",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("source_message_id", sa.String(length=128), nullable=False, unique=True),
        sa.Column("original_capturing_sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("smart_table_owner_user_id", sa.String(length=128), nullable=False),
        sa.Column("smart_table_record_id", sa.String(length=128), nullable=True, unique=True),
        sa.Column(
            "lifecycle_state", sa.String(length=64), nullable=False, server_default="temporary"
        ),
        sa.Column("field_values", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_message_id"], ["incoming_messages.message_id"]),
        sa.ForeignKeyConstraint(
            ["original_capturing_sales_user_id"], ["sales_authorizations.wecom_user_id"]
        ),
        sa.ForeignKeyConstraint(
            ["smart_table_owner_user_id"], ["sales_authorizations.wecom_user_id"]
        ),
    )
    op.create_index(
        "ix_leads_original_capturing_sales_user_id",
        "leads",
        ["original_capturing_sales_user_id"],
    )
    op.create_index("ix_leads_smart_table_owner_user_id", "leads", ["smart_table_owner_user_id"])
    op.create_table(
        "lead_field_provenances",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("lead_id", sa.String(length=36), nullable=False),
        sa.Column("source_message_id", sa.String(length=128), nullable=False),
        sa.Column("field_name", sa.String(length=64), nullable=False),
        sa.Column("value", sa.String(length=512), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
        sa.ForeignKeyConstraint(["source_message_id"], ["incoming_messages.message_id"]),
    )
    op.create_index("ix_lead_field_provenances_lead_id", "lead_field_provenances", ["lead_id"])
    op.create_table(
        "smart_table_syncs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("lead_id", sa.String(length=36), nullable=False, unique=True),
        sa.Column("source_message_id", sa.String(length=128), nullable=False, unique=True),
        sa.Column("smart_table_record_id", sa.String(length=128), nullable=True, unique=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("error_summary", sa.String(length=256), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
        sa.ForeignKeyConstraint(["source_message_id"], ["incoming_messages.message_id"]),
    )


def downgrade() -> None:
    """按依赖逆序删除 T05 审核工作区表。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出。
    副作用：显式回退时删除 T05 保存的线索、来源和同步数据。
    """
    op.drop_table("smart_table_syncs")
    op.drop_index("ix_lead_field_provenances_lead_id", table_name="lead_field_provenances")
    op.drop_table("lead_field_provenances")
    op.drop_index("ix_leads_smart_table_owner_user_id", table_name="leads")
    op.drop_index("ix_leads_original_capturing_sales_user_id", table_name="leads")
    op.drop_table("leads")
