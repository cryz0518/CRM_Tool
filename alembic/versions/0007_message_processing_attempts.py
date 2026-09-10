"""补齐消息消费失败检查点所需的重试计数。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0007_message_processing_attempts"
down_revision = "0006_sales_message_context"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为 Outbox 事件新增可配置重试上限使用的尝试次数列。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚事务。
    副作用：既有事件以零次尝试初始化，新事件可在失败后进入检查点。
    """
    # 非空默认值保证历史待消费事件无需回填脚本即可安全进入重试判断。
    op.add_column(
        "outbox_events",
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    """删除消息处理尝试次数列。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出。
    副作用：显式回退时删除历史失败尝试次数。
    """
    op.drop_column("outbox_events", "attempts")
