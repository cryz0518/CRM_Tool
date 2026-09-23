"""将公司核验持久化字段从历史企查查命名迁移为天眼查命名。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0024_tyc_company_identity"
down_revision = "0023_ticket22_storage_retention"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """重命名线索中的第三方客户标识与候选字段，并迁移核验状态值。"""
    # 历史表结构已经存在 qcc 列，只做可逆重命名，不丢失已保存的核验审计事实。
    op.alter_column("leads", "qcc_company_id", new_column_name="tyc_customer_id")
    op.alter_column("leads", "qcc_candidates", new_column_name="tyc_candidates")
    op.execute(
        sa.text(
            "UPDATE leads SET company_verification_status = 'tyc_verified' "
            "WHERE company_verification_status = 'qcc_verified'"
        )
    )


def downgrade() -> None:
    """将天眼查字段和核验状态恢复为历史命名。"""
    op.execute(
        sa.text(
            "UPDATE leads SET company_verification_status = 'qcc_verified' "
            "WHERE company_verification_status = 'tyc_verified'"
        )
    )
    op.alter_column("leads", "tyc_candidates", new_column_name="qcc_candidates")
    op.alter_column("leads", "tyc_customer_id", new_column_name="qcc_company_id")
