"""建立 T12 CRM 首次创建同步记录。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0014_crm_sync"
down_revision = "0013_company_resolution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建逻辑 CRM create 操作及 PostgreSQL 首次创建并发约束。

    返回值：无。
    异常：DDL 失败时由 Alembic 抛出。
    副作用：新增 CRM 同步表、稳定幂等键唯一约束和 create 部分唯一索引。
    """
    op.create_table(
        "crm_sync_records",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("lead_id", sa.String(length=36), sa.ForeignKey("leads.id"), nullable=False),
        sa.Column("operation", sa.String(length=32), nullable=False),
        sa.Column("smart_table_record_id", sa.String(length=128), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("canonical_payload", sa.JSON(), nullable=False),
        sa.Column("snapshot_hash", sa.String(length=64), nullable=False),
        sa.Column("request_message_id", sa.String(length=128), nullable=False),
        sa.Column("submitting_sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("submitting_crm_user_id", sa.String(length=128), nullable=False),
        sa.Column("crm_lead_id", sa.String(length=128)),
        sa.Column("crm_lead_owner_user_id", sa.String(length=128)),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processing_started_at", sa.DateTime(timezone=True)),
        sa.Column("processing_lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("response_summary", sa.String(length=256)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("idempotency_key", name="uq_crm_sync_records_idempotency_key"),
    )
    op.create_index("ix_crm_sync_records_lead_id", "crm_sync_records", ["lead_id"])
    op.create_index(
        "ix_crm_sync_records_processing_lease", "crm_sync_records", ["processing_lease_expires_at"]
    )
    # 一个 Lead 首次 create 只允许一个逻辑操作；update 不受此索引限制，留给 T13。
    op.create_index(
        "uq_crm_sync_records_one_create_per_lead",
        "crm_sync_records",
        ["lead_id"],
        unique=True,
        postgresql_where=sa.text("operation = 'create'"),
    )


def downgrade() -> None:
    """按逆序删除 T12 CRM 同步记录结构。

    返回值：无。
    异常：DDL 失败时由 Alembic 抛出。
    副作用：删除 CRM 同步记录及其索引。
    """
    op.drop_index("uq_crm_sync_records_one_create_per_lead", table_name="crm_sync_records")
    op.drop_index("ix_crm_sync_records_processing_lease", table_name="crm_sync_records")
    op.drop_index("ix_crm_sync_records_lead_id", table_name="crm_sync_records")
    op.drop_table("crm_sync_records")
