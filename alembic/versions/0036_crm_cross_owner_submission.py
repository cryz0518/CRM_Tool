"""增加受控的跨员工 CRM 提交权限。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0036_crm_cross_owner_submission"
down_revision = "0035_audit_mirror_dispatch_lease"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为既有 actor 增加默认关闭的跨员工 CRM 提交权限。"""
    op.add_column(
        "sales_authorizations",
        sa.Column(
            "can_submit_other_sales_leads",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )


def downgrade() -> None:
    """移除跨员工 CRM 提交权限列。"""
    op.drop_column("sales_authorizations", "can_submit_other_sales_leads")
