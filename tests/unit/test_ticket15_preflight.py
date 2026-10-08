"""T15 Compose preflight 入口的离线契约测试。"""

from __future__ import annotations

import ast
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest

from scripts import t15_compose_preflight

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_FILE = _REPOSITORY_ROOT / "tests" / "integration" / "ticket15-compose.json"


def _successful_compose_output(run_id: str) -> str:
    """构造供 fake subprocess 返回的隔离配置，不调用 Docker。

    参数：run_id 为测试进程捕获的随机 T15 标识。
    返回值：JSON 编码的模拟最终配置。
    异常：测试配置缺失或无效时抛出解析异常。
    副作用：只读取独立 ticket15-compose.json。
    """
    config: dict[str, Any] = json.loads(_COMPOSE_FILE.read_text(encoding="utf-8"))
    encoded = json.dumps(config).replace("${T15_RUN_ID:?T15_RUN_ID required}", run_id)
    resolved: dict[str, Any] = json.loads(encoded)
    project_name = f"t15-migration-{run_id}"
    resolved["name"] = project_name
    resolved["services"]["migrate"]["build"]["context"] = str(_REPOSITORY_ROOT)
    resolved["networks"] = {
        "default": {"name": f"{project_name}_default", "external": False}
    }
    return json.dumps(resolved)


def _assert_diagnostic_fields(record: dict[str, Any]) -> None:
    """确认诊断 JSON 持有调用方要求的全部安全字段。

    参数：record 为入口输出或诊断文件解析出的对象。
    返回值：断言全部字段存在时无。
    异常：缺少字段时由 pytest 报告断言失败。
    副作用：无。
    """
    assert {
        "status",
        "stage",
        "error_code",
        "exit_code",
        "exception_type",
        "external_command_executed",
    } <= record.keys()


def test_preflight_uses_clean_environment_and_persists_safe_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """成功路径只模拟 context 与 Compose config，并持久化不含敏感值的记录。

    参数：monkeypatch 替换环境与 subprocess；tmp_path 接收安全诊断；capsys 捕获 JSON 输出。
    返回值：无。
    异常：入口或隔离断言错误时由 pytest 标记失败。
    副作用：fake subprocess 不启动进程，临时文件只写入 pytest 临时目录。
    """
    for key, value in {
        "DATABASE_URL": "postgresql://secret-db",
        "POSTGRES_PASSWORD": "secret-password",
        "DOCKER_HOST": "tcp://remote.invalid:2375",
        "DOCKER_CONTEXT": "remote-context",
        "WECOM_SECRET": "secret-wecom",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []

    def fake_run(arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """记录调用并按 context/config 返回模拟结果。

        参数：arguments 和 kwargs 是被替换的 subprocess.run 参数。
        返回值：对应安全阶段的模拟 CompletedProcess。
        异常：出现白名单以外的命令时抛出断言错误。
        副作用：只记录参数，不启动外部进程。
        """
        environment = kwargs["env"]
        calls.append((tuple(arguments), environment))
        if arguments[1:3] == ["context", "inspect"]:
            return subprocess.CompletedProcess(arguments, 0, "npipe:////./pipe/docker_engine", "")
        if arguments[1:3] == ["compose", "--env-file"]:
            return subprocess.CompletedProcess(
                arguments, 0, _successful_compose_output(environment["T15_RUN_ID"]), ""
            )
        raise AssertionError("preflight attempted an unsupported command")

    monkeypatch.setattr(subprocess, "run", fake_run)

    exit_code = t15_compose_preflight.main([])

    output = capsys.readouterr().out
    record = json.loads(output)
    _assert_diagnostic_fields(record)
    assert exit_code == record["exit_code"] == 0
    assert record["status"] == "passed"
    assert record["stage"] == "compose_safety"
    assert record["error_code"] == "OK"
    assert record["exception_type"] is None
    assert record["external_command_executed"] is True
    assert len(calls) == 2
    assert calls[0][0][1:3] == ("context", "inspect")
    assert calls[1][0][-3:] == ("config", "--format", "json")
    assert Path(calls[1][0][calls[1][0].index("--env-file") + 1]) == (
        _REPOSITORY_ROOT / "tests" / "integration" / "ticket15-compose.env"
    )
    environment = calls[0][1]
    assert environment["T15_RUN_ID"] == calls[1][1]["T15_RUN_ID"]
    assert len(environment["T15_RUN_ID"]) == 32
    assert all(char in "0123456789abcdef" for char in environment["T15_RUN_ID"])
    assert calls[1][0][calls[1][0].index("-p") + 1] == (
        f"t15-migration-{environment['T15_RUN_ID']}"
    )
    assert record["diagnostic_file"].endswith(f"{environment['T15_RUN_ID']}.json")
    assert all(
        not any(flag in arguments for flag in ("build", "up", "run", "down", "restart"))
        for arguments, _environment in calls
    )
    assert not {
        "DATABASE_URL",
        "POSTGRES_PASSWORD",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "WECOM_SECRET",
    } & environment.keys()
    assert "secret-password" not in output
    diagnostics = list(tmp_path.glob("t15-compose-preflight-*.json"))
    assert len(diagnostics) == 1
    assert diagnostics[0].name == record["diagnostic_file"]
    saved = diagnostics[0].read_text(encoding="utf-8")
    assert json.loads(saved) == record
    assert all(
        secret not in output and secret not in saved
        for secret in (
            "secret-db",
            "secret-password",
            "secret-wecom",
            "DATABASE_URL",
            "POSTGRES_PASSWORD",
            "postgresql+psycopg://",
            "services",
        )
    )
    assert len(calls) == 2


def test_preflight_failure_is_nonzero_and_diagnostic_is_persisted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """context 非零失败生成脱敏记录、写入独立文件并返回非零。

    参数：monkeypatch 替换子进程和临时目录；tmp_path 接收记录；capsys 捕获入口输出。
    返回值：无。
    异常：脱敏或失败状态断言不满足时由 pytest 报告失败。
    副作用：fake subprocess 提供含敏感标记的输出，不启动进程。
    """
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    calls: list[tuple[str, ...]] = []

    def fake_run(arguments: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        """只模拟 context 命令失败并返回敏感标记。

        参数：arguments 为被替换调用的命令参数；其余关键字参数被忽略。
        返回值：非零退出码和用于脱敏断言的模拟输出。
        异常：无。
        副作用：记录命令调用，不启动外部进程。
        """
        calls.append(tuple(arguments))
        return subprocess.CompletedProcess(
            arguments, 29, "DATABASE_URL=hidden", "POSTGRES_PASSWORD=hidden"
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    exit_code = t15_compose_preflight.main([])

    output = capsys.readouterr().out
    record = json.loads(output)
    _assert_diagnostic_fields(record)
    assert exit_code == record["exit_code"] == 1
    assert record["status"] == "failed"
    assert record["stage"] == "docker_context"
    assert record["error_code"] == "DOCKER_CONTEXT_EXIT_NONZERO"
    assert record["exception_type"] is None
    assert record["external_command_executed"] is True
    assert "DATABASE_URL=hidden" not in output
    assert "POSTGRES_PASSWORD=hidden" not in output
    assert len(calls) == 1
    saved = next(tmp_path.glob("t15-compose-preflight-*.json")).read_text(encoding="utf-8")
    assert json.loads(saved) == record


@pytest.mark.parametrize(
    ("stderr", "error_code", "reason"),
    (
        (
            "docker: 'compose' is not a docker command.",
            "COMPOSE_CLI_PLUGIN_UNAVAILABLE",
            "Docker Compose CLI 插件不可用",
        ),
        (
            "unknown flag: --format",
            "COMPOSE_ARGUMENT_UNSUPPORTED",
            "Compose 参数不受当前 CLI 支持",
        ),
        (
            "failed to read env file: C:/private/path",
            "COMPOSE_ENV_FILE_UNREADABLE",
            "专用 Compose 环境文件无法读取",
        ),
        (
            "required variable T15_RUN_ID is missing a value",
            "COMPOSE_INTERPOLATION_FAILED",
            "Compose 配置变量插值失败",
        ),
        (
            'invalid value "json" for --format',
            "COMPOSE_CONFIG_FORMAT_UNSUPPORTED",
            "Compose JSON 输出格式不受当前 CLI 支持",
        ),
        (
            "daemon request failed: secret DATABASE_URL=not-for-output",
            "COMPOSE_ERROR_UNCLASSIFIED",
            "Compose config 命令失败；错误未匹配安全分类白名单",
        ),
    ),
)
def test_preflight_classifies_compose_stderr_without_persisting_it(
    stderr: str,
    error_code: str,
    reason: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """按 stderr 白名单输出固定诊断，并确保原始子进程输出不落盘。

    参数：stderr 为模拟分类文本；error_code 和 reason 为预期固定结果；
    monkeypatch 替换 subprocess 与临时目录；tmp_path 接收 JSON；capsys 捕获 stdout。
    返回值：无。
    异常：分类、退出状态或脱敏断言失败时由 pytest 报告。
    副作用：模拟 context/config 返回，不启动任何外部进程。
    """
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    calls: list[tuple[str, ...]] = []

    def fake_run(arguments: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        """为只读检查返回本机 endpoint 和模拟 config 错误。

        参数：arguments 和关键字参数为替换的 subprocess 调用参数。
        返回值：context 成功结果或退出码 125 的 Compose config 结果。
        异常：无。
        副作用：记录调用，不启动外部进程。
        """
        calls.append(tuple(arguments))
        if arguments[1:3] == ["context", "inspect"]:
            return subprocess.CompletedProcess(
                arguments, 0, "unix:///run/docker.sock", ""
            )
        return subprocess.CompletedProcess(
            arguments,
            125,
            "RAW_STDOUT_SECRET=do-not-store",
            f"{stderr}\nRAW_STDERR_SECRET=do-not-store",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    exit_code = t15_compose_preflight.main([])

    output = capsys.readouterr().out
    record = json.loads(output)
    _assert_diagnostic_fields(record)
    assert exit_code == record["exit_code"] == 1
    assert record["status"] == "failed"
    assert record["stage"] == "compose_config"
    assert record["error_code"] == error_code
    assert record["reason"] == reason
    assert record["command_exit_code"] == 125
    assert record["external_command_executed"] is True
    assert len(calls) == 2
    saved = next(tmp_path.glob("t15-compose-preflight-*.json")).read_text(encoding="utf-8")
    assert json.loads(saved) == record
    for raw_value in (stderr, "RAW_STDOUT_SECRET=do-not-store", "RAW_STDERR_SECRET=do-not-store"):
        assert raw_value not in output + saved


def test_preflight_isolation_failure_stays_nonzero_and_redacted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Compose 返回危险 named volume 时，入口 fail closed 且只留安全诊断。

    参数：monkeypatch 替换 subprocess 与临时目录；tmp_path 接收诊断；capsys 捕获 JSON。
    返回值：无。
    异常：隔离安全门未返回非零或泄露配置值时由 pytest 报告失败。
    副作用：fake subprocess 只返回模拟 JSON，不连接 Docker 或数据库。
    """
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    calls: list[tuple[str, ...]] = []

    def fake_run(arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """返回本机 context 和含固定卷的模拟 Compose 配置。

        参数：arguments 和 kwargs 是被替换调用的 subprocess.run 参数。
        返回值：本机 endpoint 或带安全回归标记的配置结果。
        异常：无。
        副作用：记录调用参数，不启动外部进程。
        """
        calls.append(tuple(arguments))
        if arguments[1:3] == ["context", "inspect"]:
            return subprocess.CompletedProcess(
                arguments, 0, "unix:///run/docker.sock", ""
            )
        environment = kwargs["env"]
        config = json.loads(_successful_compose_output(environment["T15_RUN_ID"]))
        config["volumes"] = {
            "legacy": {"name": "crm_t21_final_postgres_data"}
        }
        return subprocess.CompletedProcess(arguments, 0, json.dumps(config), "")

    monkeypatch.setattr(subprocess, "run", fake_run)

    exit_code = t15_compose_preflight.main([])

    output = capsys.readouterr().out
    record = json.loads(output)
    _assert_diagnostic_fields(record)
    assert exit_code == record["exit_code"] == 1
    assert record["status"] == "failed"
    assert record["stage"] == "compose_safety"
    assert record["error_code"] == "COMPOSE_ISOLATION_FAILED"
    assert record["external_command_executed"] is True
    assert len(calls) == 2
    saved = next(tmp_path.glob("t15-compose-preflight-*.json")).read_text(encoding="utf-8")
    assert json.loads(saved) == record
    assert "crm_t21_final_postgres_data" not in output + saved
    assert "POSTGRES_PASSWORD" not in output + saved


def test_preflight_docker_cli_missing_is_structured_and_nonzero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """CLI 启动失败不会输出原始 OSError，并保留明确阶段与错误码。

    参数：monkeypatch 替换子进程和临时目录；capsys 捕获安全 JSON。
    返回值：无。
    异常：结构化失败断言不满足时由 pytest 报告失败。
    副作用：替身抛出模拟 FileNotFoundError，不启动外部进程。
    """
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))

    def fake_run(_arguments: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        """模拟系统找不到 Docker CLI。

        参数：subprocess.run 参数仅用于替身调用兼容。
        返回值：无；此分支通过启动异常报告失败。
        异常：抛出模拟 FileNotFoundError。
        副作用：不启动外部进程。
        """
        raise FileNotFoundError("DATABASE_URL=private")

    monkeypatch.setattr(subprocess, "run", fake_run)

    exit_code = t15_compose_preflight.main([])

    output = capsys.readouterr().out
    record = json.loads(output)
    _assert_diagnostic_fields(record)
    assert exit_code == record["exit_code"] == 1
    assert record["stage"] == "docker_context"
    assert record["error_code"] == "DOCKER_CLI_NOT_FOUND"
    assert record["exception_type"] == "FileNotFoundError"
    assert record["external_command_executed"] is False
    assert "DATABASE_URL=private" not in output


def test_preflight_refuses_diagnostic_file_inside_repository(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """临时目录若指向仓库内部则拒绝落盘并以非零状态报告。

    参数：monkeypatch 替换命令与临时目录；capsys 捕获结构化诊断。
    返回值：无。
    异常：落盘边界或非零状态断言不满足时由 pytest 报告失败。
    副作用：所有子进程被替身接管，仓库根目录不得产生诊断文件。
    """
    monkeypatch.setattr(
        tempfile, "gettempdir", lambda: str(_REPOSITORY_ROOT)
    )

    def fake_run(arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """模拟有效的只读命令结果，测试仅验证落盘边界。

        参数：arguments 和 kwargs 是被替换调用的 subprocess.run 参数。
        返回值：本机 endpoint 或模拟 Compose JSON 结果。
        异常：无。
        副作用：只记录调用，不启动外部进程。
        """
        if arguments[1:3] == ["context", "inspect"]:
            return subprocess.CompletedProcess(
                arguments, 0, "unix:///run/docker.sock", ""
            )
        environment = kwargs["env"]
        return subprocess.CompletedProcess(
            arguments, 0, _successful_compose_output(environment["T15_RUN_ID"]), ""
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    exit_code = t15_compose_preflight.main([])

    record = json.loads(capsys.readouterr().out)
    _assert_diagnostic_fields(record)
    assert exit_code == record["exit_code"] == 1
    assert record["stage"] == "diagnostic_persist"
    assert record["error_code"] == "DIAGNOSTIC_WRITE_FAILED"
    assert record["external_command_executed"] is True
    assert not list(_REPOSITORY_ROOT.glob("t15-compose-preflight-*.json"))


def test_preflight_entrypoint_only_calls_existing_safety_resolver() -> None:
    """静态检查入口只委派现有只读安全解析器，没有 migration 测试调用路径。

    参数：无。
    返回值：断言通过表示没有执行完整 T15 集成测试的调用路径。
    异常：AST 中出现不允许的调用时由 pytest 报告断言失败。
    副作用：只读取并解析入口源码。
    """
    source_path = _REPOSITORY_ROOT / "scripts" / "t15_compose_preflight.py"
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    names_called = {
        node.func.id
        for node in calls
        if isinstance(node.func, ast.Name)
    }
    resolver_calls = [
        node
        for node in calls
        if isinstance(node.func, ast.Name) and node.func.id == "_resolve_and_validate_compose"
    ]
    subprocess_calls = [
        node
        for node in calls
        if isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "subprocess"
    ]

    assert "_resolve_and_validate_compose" in names_called
    assert len(resolver_calls) == 1
    assert not subprocess_calls
    assert "test_current_migration_chain_reaches_single_head" not in names_called
    assert "pytest.main" not in source
    assert "alembic" not in source
