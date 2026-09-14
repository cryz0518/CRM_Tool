"""为 T13 CRM update 快照幂等增加数据库并发兜底。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0016_crm_update_snapshot"
down_revision = "0015_outbound_notice"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """确保同一线索的同一 update 规范快照只能形成一个逻辑操作。"""
    op.create_index(
        "uq_crm_sync_records_update_snapshot",
        "crm_sync_records",
        ["lead_id", "snapshot_hash"],
        unique=True,
        postgresql_where=sa.text("operation = 'update'"),
    )
    op.create_table(
        "crm_company_identities",
        sa.Column("standard_company_name", sa.String(length=512), primary_key=True),
        sa.Column("crm_lead_id", sa.String(length=128)),
        sa.Column("crm_lead_owner_user_id", sa.String(length=128)),
        sa.Column("state", sa.String(length=32), nullable=False, server_default="reserving"),
        sa.Column("creating_lead_id", sa.String(length=36), sa.ForeignKey("leads.id")),
        sa.Column("creating_sync_record_id", sa.Integer(), sa.ForeignKey("crm_sync_records.id")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.add_column(
        "business_audit_events",
        sa.Column("details", sa.JSON(), nullable=False, server_default="{}"),
    )


def downgrade() -> None:
    """删除 T13 update 快照并发约束。"""
    op.drop_table("crm_company_identities")
    op.drop_column("business_audit_events", "details")
    op.drop_index("uq_crm_sync_records_update_snapshot", table_name="crm_sync_records")
