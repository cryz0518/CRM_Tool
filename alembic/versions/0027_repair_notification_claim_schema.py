"""修复历史开发数据库中通知认领列缺失的问题。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0027_repair_notification_claim_schema"
down_revision = "0026_repair_wecom_callback_status_constraint"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """幂等补齐通知发送器使用的认领令牌列。

    参数：无，Alembic 通过当前数据库连接执行迁移。
    返回值：无。
    异常：数据库无法读取元数据或修改结构时由 Alembic 向上抛出。
    重要副作用：只在 notification_records 缺列时新增可为空的认领令牌列。
    """

    # 历史开发库可能已标记为 0022 及以后，但实际结构没有同步完成。
    connection = op.get_bind()
    column_names = {
        column["name"]
        for column in sa.inspect(connection).get_columns("notification_records")
    }
    if "processing_claim_token" not in column_names:
        # 可为空是为了兼容已有通知记录，只有发送器认领中的记录才会写入令牌。
        op.add_column(
            "notification_records",
            sa.Column("processing_claim_token", sa.String(length=64), nullable=True),
        )


def downgrade() -> None:
    """保留通知认领列，避免回退时破坏并发发送保护。"""

    # 该修复只处理历史结构漂移，禁止自动删除已有认领数据。
    return None
