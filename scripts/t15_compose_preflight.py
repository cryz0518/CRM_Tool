"""T15 的只读 Compose 预检入口；只解析 Docker context 和 Compose config。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from tests.integration.test_ticket15_migrations import (  # noqa: E402
    T15ValidationError,
    _resolve_and_validate_compose,
)


def _clean_process_environment(run_id: str) -> dict[str, str]:
    """构造只保留进程运行必需变量及随机 T15 标识的环境。

    参数：run_id 为本次随机 32 位十六进制标识。
    返回值：不继承数据库、适配器、Docker endpoint 覆盖或业务凭据的子进程环境。
    异常：无。
    副作用：只读取当前进程的环境变量。
    """
    allowed = (
        "PATH",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
        "TEMP",
        "TMP",
        "HOME",
        "USERPROFILE",
    )
    environment = {key: os.environ[key] for key in allowed if key in os.environ}
    environment["T15_RUN_ID"] = run_id
    return environment


def _result_record(
    *,
    status: str,
    stage: str,
    error_code: str,
    exit_code: int,
    exception_type: str | None,
    external_command_executed: bool | None,
    reason: str,
    command_exit_code: int | None = None,
) -> dict[str, Any]:
    """生成不包含命令输出、环境变量或 Compose 数据的结果记录。

    参数：状态、阶段、错误码及进程结果字段均为安全诊断值；command_exit_code 是可选子命令退出码。
    返回值：供 stdout 和临时目录诊断文件共用的 JSON 对象。
    异常：无。
    副作用：无。
    """
    record: dict[str, Any] = {
        "status": status,
        "stage": stage,
        "error_code": error_code,
        "exit_code": exit_code,
        "exception_type": exception_type,
        "external_command_executed": external_command_executed,
        "reason": reason,
    }
    if command_exit_code is not None:
        record["command_exit_code"] = command_exit_code
    return record


def _persist_diagnostic(record: dict[str, Any], run_id: str) -> None:
    """以独立随机文件名将安全结果写入系统临时目录。

    参数：record 为已脱敏结果；run_id 为本次随机运行标识。
    返回值：成功写入时无。
    异常：文件创建失败时抛出 OSError，不覆盖已有文件。
    副作用：只在系统临时目录创建一个仅含安全结果的 JSON 文件。
    """
    temp_root = Path(tempfile.gettempdir()).resolve()
    try:
        temp_root.relative_to(_REPOSITORY_ROOT)
    except ValueError:
        pass
    else:
        raise OSError("diagnostic directory resolves inside the repository")
    path = temp_root / f"t15-compose-preflight-{run_id}.json"
    with path.open("x", encoding="utf-8") as diagnostic_file:
        diagnostic_file.write(json.dumps(record, ensure_ascii=False, indent=2) + "\n")


def main(argv: list[str] | None = None) -> int:
    """执行只读安全预检，输出并持久化结构化诊断后返回进程退出码。

    参数：argv 为命令行参数；该入口不接受任何参数。
    返回值：安全门通过返回 0；检查或诊断持久化失败返回非零。
    异常：预期检查错误转换为脱敏记录；未分类错误仅输出异常类型。
    副作用：只调用现有 context/config 安全解析器，并写入系统临时目录诊断文件。
    """
    arguments = sys.argv[1:] if argv is None else argv
    run_id = uuid4().hex
    if arguments:
        record = _result_record(
            status="failed",
            stage="arguments",
            error_code="UNSUPPORTED_ARGUMENTS",
            exit_code=2,
            exception_type=None,
            external_command_executed=False,
            reason="该入口不接受命令行参数",
        )
        return _emit_result(record, run_id)

    try:
        environment = _clean_process_environment(run_id)
        _resolve_and_validate_compose(f"t15-migration-{run_id}", environment)
    except T15ValidationError as error:
        record = _result_record(
            status="failed",
            stage=error.stage,
            error_code=error.error_code,
            exit_code=1,
            exception_type=error.exception_type,
            external_command_executed=error.external_command_executed,
            reason=error.reason,
            command_exit_code=error.exit_code,
        )
        return _emit_result(record, run_id)
    except Exception as error:
        record = _result_record(
            status="failed",
            stage="preflight",
            error_code="PREFLIGHT_UNEXPECTED_ERROR",
            exit_code=1,
            exception_type=type(error).__name__,
            external_command_executed=None,
            reason="预检遇到未分类异常；未输出异常文本",
        )
        return _emit_result(record, run_id)

    record = _result_record(
        status="passed",
        stage="compose_safety",
        error_code="OK",
        exit_code=0,
        exception_type=None,
        external_command_executed=True,
        reason="Docker context 与独立 Compose 配置均通过只读安全门",
    )
    return _emit_result(record, run_id)


def _emit_result(record: dict[str, Any], run_id: str) -> int:
    """输出安全结果并保存诊断；持久化失败时仍以非零状态结束。

    参数：record 为已脱敏结果；run_id 为本次随机运行标识。
    返回值：结果记录中的退出码，诊断写入失败时为 1。
    异常：不向调用方传播临时文件的敏感异常文本。
    副作用：向 stdout 输出 JSON，并尝试在系统临时目录创建诊断文件。
    """
    record["diagnostic_file"] = f"t15-compose-preflight-{run_id}.json"
    try:
        _persist_diagnostic(record, run_id)
    except Exception as error:
        record = _result_record(
            status="failed",
            stage="diagnostic_persist",
            error_code="DIAGNOSTIC_WRITE_FAILED",
            exit_code=1,
            exception_type=type(error).__name__,
            external_command_executed=record["external_command_executed"],
            reason="无法持久化安全诊断文件",
        )
    print(json.dumps(record, ensure_ascii=False, indent=2))
    return int(record["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
