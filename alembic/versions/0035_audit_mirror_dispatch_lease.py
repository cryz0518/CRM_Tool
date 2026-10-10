"""为审计镜像调度增加独立派发租约，避免重复投递。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0035_audit_mirror_dispatch_lease"
down_revision = "0034_lead_system_defaults"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """增加可空派发租约列；既有待处理镜像仍可被首次调度。"""
    op.add_column(
        "audit_mirror_outbox",
        sa.Column("dispatch_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    """删除派发租约列；不改变审计镜像和业务审计数据。"""
    op.drop_column("audit_mirror_outbox", "dispatch_lease_expires_at")
