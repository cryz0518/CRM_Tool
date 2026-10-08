"""提供应用和运维验证共用的只读 Alembic readiness 检查。"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.core.readiness import ReadinessComponent, not_ready_component, ready_component

_ALEMBIC_CONFIG_PATH = Path(__file__).resolve().parents[2] / "alembic.ini"


def _get_code_heads() -> tuple[str, ...]:
    """从当前应用包的 Alembic revision graph 读取所有 head。

    返回值：按 Alembic 顺序排列的 head revision。
    异常：配置缺失、脚本目录缺失或 revision graph 无法读取时抛出异常。
    副作用：仅读取本地 migration 配置和脚本，不连接数据库。
    """
    if not _ALEMBIC_CONFIG_PATH.is_file():
        raise FileNotFoundError("Alembic 配置不可用")

    config = Config(str(_ALEMBIC_CONFIG_PATH))
    # 配置文件可能含相对路径；固定到当前代码包，避免依赖进程工作目录。
    config.set_main_option("script_location", str(_ALEMBIC_CONFIG_PATH.parent / "alembic"))
    return tuple(ScriptDirectory.from_config(config).get_heads())


def migration_graph_component() -> ReadinessComponent:
    """只读验证代码包含且仅包含一个可读取的 Alembic head。

    返回值：head 可用时为 migration_head_declared，否则为 migration_unavailable。
    异常：脚本目录和 revision graph 异常转换为稳定原因码。
    副作用：仅读取本地 migration 配置和脚本。
    """
    try:
        if len(_get_code_heads()) != 1:
            return not_ready_component("migration", "migration_unavailable")
    except Exception:
        return not_ready_component("migration", "migration_unavailable")
    return ready_component("migration", "migration_head_declared")


def check_migration_readiness(connection: Connection) -> ReadinessComponent:
    """验证唯一代码 head 与数据库全部 current revision 完全一致。

    参数：connection 为已打开的 SQLAlchemy 数据库连接。
    返回值：完全一致时为 migration_current；否则返回稳定的 not_ready 组件。
    异常：图读取、表查询和数据库异常均转换为脱敏原因码。
    副作用：仅读取 migration 文件并执行一条 SELECT，不执行 migration 或数据库写入。
    """
    try:
        heads = _get_code_heads()
    except Exception:
        return not_ready_component("migration", "migration_unavailable")
    if len(heads) != 1:
        return not_ready_component("migration", "migration_unavailable")

    try:
        revisions = tuple(
            connection.execute(text("SELECT version_num FROM alembic_version"))
            .scalars()
            .all()
        )
    except Exception:
        return not_ready_component("migration", "migration_unavailable")

    if len(revisions) != 1 or not revisions[0]:
        return not_ready_component("migration", "migration_unavailable")
    if revisions[0] != heads[0]:
        return not_ready_component("migration", "migration_pending")
    return ready_component("migration", "migration_current")
