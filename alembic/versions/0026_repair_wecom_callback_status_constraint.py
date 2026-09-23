"""修复历史开发数据库中企业微信 callback 状态约束缺失的问题。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0026_repair_wecom_callback_status_constraint"
down_revision = "0025_repair_wecom_action_outbox_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """幂等补齐 callback delivery 的合法状态约束。

    参数：无，Alembic 通过当前数据库连接执行迁移。
    返回值：无。
    异常：已有非法状态或数据库无法修改结构时由 Alembic 向上抛出。
    重要副作用：只新增缺失的 CHECK 约束，不删除或改写 callback 事实。
    """

    # 历史卷可能已记录 0022 版本但缺少该约束，按约束名检查后再补齐。
    connection = op.get_bind()
    existing_constraints = {
        constraint["name"]
        for constraint in sa.inspect(connection).get_check_constraints(
            "wecom_callback_deliveries"
        )
    }
    if "ck_wecom_callback_delivery_status" not in existing_constraints:
        op.create_check_constraint(
            "ck_wecom_callback_delivery_status",
            "wecom_callback_deliveries",
            "processing_status IN ('received', 'claimed', 'duplicated', 'rejected', 'completed')",
        )


def downgrade() -> None:
    """保留 callback 状态约束，避免回退时放宽不可变审计事实。"""

    # 该修复只收紧历史结构，禁止自动删除约束造成新的结构漂移。
    return None
