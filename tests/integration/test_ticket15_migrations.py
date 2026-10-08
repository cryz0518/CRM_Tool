"""历史 migration 链升级、head 和 current 验证。"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from copy import deepcopy
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from sqlalchemy.engine import make_url

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_FILE = Path(__file__).with_name("ticket15-compose.json")
_COMPOSE_ENV_FILE = Path(__file__).with_name("ticket15-compose.env")


class T15ValidationError(RuntimeError):
    """保留 T15 检查阶段和脱敏诊断字段的安全门异常。"""

    def __init__(
        self,
        stage: str,
        error_code: str,
        reason: str,
        *,
        exit_code: int | None = None,
        exception_type: str | None = None,
        external_command_executed: bool,
    ) -> None:
        """创建不包含环境变量、命令输出或 Compose 数据的诊断记录。

        参数：stage 和 error_code 标记失败阶段及安全错误码；reason 为固定脱敏原因；
        其余字段描述退出码、异常类型及失败前是否有外部进程实际运行。
        返回值：无。
        异常：无。
        副作用：设置异常属性，供 pytest 和离线诊断读取。
        """
        self.stage = stage
        self.error_code = error_code
        self.reason = reason
        self.exit_code = exit_code
        self.exception_type = exception_type
        self.external_command_executed = external_command_executed
        details = f"T15[{error_code}] stage={stage}: {reason}"
        if exit_code is not None:
            details += f" (exit_code={exit_code})"
        super().__init__(details)

    def to_safe_dict(self) -> dict[str, str | int | bool | None]:
        """返回仅包含安全诊断字段的结构化记录，不包含命令或配置内容。

        参数：无。
        返回值：可供日志或测试检查的脱敏字段映射。
        异常：无。
        副作用：无。
        """
        return {
            "status": "failed",
            "stage": self.stage,
            "error_code": self.error_code,
            "reason": self.reason,
            "exit_code": self.exit_code,
            "exception_type": self.exception_type,
            "external_command_executed": self.external_command_executed,
        }


def _subprocess_start_error(
    stage: str, error: OSError, *, prior_command_executed: bool
) -> T15ValidationError:
    """把 subprocess 启动异常转换为不泄露本机路径的阶段诊断。

    参数：stage 为待运行的安全检查阶段；error 为启动异常；prior_command_executed
    表示本轮此前是否已有 subprocess 成功返回。
    返回值：包含固定错误码和异常类型的安全诊断。
    异常：无。
    副作用：无。
    """
    missing_cli = isinstance(error, FileNotFoundError)
    return T15ValidationError(
        stage,
        "DOCKER_CLI_NOT_FOUND" if missing_cli else "SUBPROCESS_START_FAILED",
        "未找到 Docker CLI" if missing_cli else "无法启动检查子进程",
        exception_type=type(error).__name__,
        external_command_executed=prior_command_executed,
    )


def _run_compose(
    project_name: str,
    environment: dict[str, str],
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    """在独立 Compose 项目中执行一条 Docker Compose 命令。

    参数：project_name 为随机本次项目名；environment 仅含运行标识与 Docker CLI 必需变量；
    arguments 为通过配置安全门后执行的 Compose 子命令。
    返回值：包含退出码、标准输出和标准错误的命令结果。
    异常：找不到 docker 命令时由测试入口转换为失败。
    副作用：命令可能操作本次随机项目资源；Compose 定义无持久卷。
    """
    return subprocess.run(
        # 只加载独立定义与测试 env；不解析根 Compose、.env 或本机 adapter 凭据。
        [
            "docker",
            "compose",
            "--env-file",
            str(_COMPOSE_ENV_FILE),
            "-p",
            project_name,
            "-f",
            str(_COMPOSE_FILE),
            *arguments,
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=environment,
        cwd=_REPOSITORY_ROOT,
    )


def _validate_resolved_compose(config: dict[str, object], project_name: str) -> None:
    """校验 Compose 最终展开配置确为唯一命名的 tmpfs 测试环境。

    参数：config 为 Docker Compose config JSON；project_name 为本次随机项目名。
    返回值：无。
    异常：任何卷、宿主挂载、固定资源名或 adapter/凭据继承都会触发断言失败。
    副作用：无，不连接 Docker daemon 或数据库。
    """
    _require_isolation(
        re.fullmatch(r"t15-migration-[0-9a-f]{32}", project_name) is not None,
        "Compose project name 不是本次生成的唯一名称",
    )
    _require_isolation(config.get("name") == project_name, "项目名不是本次随机项目")
    _require_isolation(not config.get("volumes"), "包含 Compose named volume")
    _require_isolation(
        "configs" not in config and "secrets" not in config,
        "Compose 引用了额外配置或密钥资源",
    )
    services = config.get("services")
    _require_isolation(isinstance(services, dict), "services 配置结构无效")
    _require_isolation(set(services) == {"postgres", "migrate"}, "服务集合不符合隔离白名单")
    postgres = services["postgres"]
    migrate = services["migrate"]
    _require_isolation(
        isinstance(postgres, dict) and isinstance(migrate, dict), "服务配置结构无效"
    )
    for service in (postgres, migrate):
        _require_isolation(not service.get("volumes"), "服务包含卷挂载")
        _require_isolation(not service.get("ports"), "服务暴露宿主端口")
        _require_isolation(not service.get("container_name"), "服务设置了固定容器名")
        _require_isolation(not service.get("network_mode"), "服务复用了外部网络")
        _require_isolation(
            not service.get("configs")
            and not service.get("secrets")
            and not service.get("volumes_from")
            and not service.get("devices"),
            "服务引用额外配置、密钥或宿主设备",
        )
    _require_isolation(postgres.get("image") == "postgres:16-alpine", "PostgreSQL 镜像不匹配")
    _require_isolation(
        postgres.get("tmpfs") == ["/var/lib/postgresql/data:rw,size=1073741824"],
        "PostgreSQL PGDATA 未绑定专用 tmpfs",
    )

    postgres_env = postgres["environment"]
    migrate_env = migrate["environment"]
    _require_isolation(
        isinstance(postgres_env, dict) and isinstance(migrate_env, dict),
        "服务 environment 配置结构无效",
    )
    _require_isolation(
        set(postgres_env)
        == {"PGDATA", "POSTGRES_DB", "POSTGRES_PASSWORD", "POSTGRES_USER"},
        "PostgreSQL environment 超出测试白名单",
    )
    _require_isolation(
        postgres_env["PGDATA"] == "/var/lib/postgresql/data/pgdata",
        "PGDATA 未落在 tmpfs 目录内",
    )
    _require_isolation(
        set(migrate_env)
        == {
            "APP_ENV",
            "CRM_ADAPTER",
            "DATABASE_URL",
            "LLM_PROVIDER",
            "MEDIA_STORAGE_PROVIDER",
            "SMART_TABLE_ADAPTER",
            "TEST_DATABASE_ID",
            "TEST_DATABASE_URL",
            "TYC_PROVIDER",
        },
        "迁移容器 environment 超出测试白名单",
    )
    _require_isolation(
        migrate_env["APP_ENV"] == "test"
        and migrate_env["SMART_TABLE_ADAPTER"] == "mock"
        and migrate_env["CRM_ADAPTER"] == "unconfigured"
        and migrate_env["LLM_PROVIDER"] == "mock"
        and migrate_env["TYC_PROVIDER"] == "unconfigured"
        and migrate_env["MEDIA_STORAGE_PROVIDER"] == "fake",
        "迁移容器启用了非隔离 adapter",
    )

    run_id = migrate_env["TEST_DATABASE_ID"]
    _require_isolation(
        isinstance(run_id, str) and re.fullmatch(r"[0-9a-f]{32}", run_id) is not None,
        "TEST_DATABASE_ID 不是随机 32 位标识",
    )
    expected_db = f"crm_lead_test_{run_id}"
    _require_isolation(project_name.endswith(str(run_id)), "项目与测试数据库运行标识不一致")
    for key in ("DATABASE_URL", "TEST_DATABASE_URL"):
        try:
            dsn = make_url(str(migrate_env[key]))
        except Exception as error:
            raise RuntimeError("T15 isolation check failed: database URL 无法解析") from error
        _require_isolation(
            dsn.drivername == "postgresql+psycopg"
            and dsn.host == "postgres"
            and dsn.port == 5432
            and dsn.username == "t15_migration"
            and dsn.password == f"t15_{run_id}"
            and dsn.database == expected_db
            and not dsn.query,
            "database URL 未指向本轮测试容器",
        )
    _require_isolation(
        postgres_env["POSTGRES_DB"] == expected_db
        and postgres_env["POSTGRES_USER"] == "t15_migration"
        and postgres_env["POSTGRES_PASSWORD"] == f"t15_{run_id}",
        "PostgreSQL 未使用当前随机测试身份",
    )
    build = migrate.get("build")
    _require_isolation(isinstance(build, dict), "迁移服务未使用仓库构建镜像")
    _require_isolation(
        set(build) == {"context", "dockerfile"}
        and Path(str(build["context"])).resolve() == _REPOSITORY_ROOT
        and build["dockerfile"] == "Dockerfile",
        "迁移服务构建上下文超出仓库根目录",
    )

    networks = config.get("networks", {})
    _require_isolation(
        isinstance(networks, dict) and set(networks) <= {"default"},
        "Compose 包含白名单外网络",
    )
    if "default" in networks:
        default_network = networks["default"]
        _require_isolation(
            isinstance(default_network, dict)
            and default_network.get("external") is not True
            and str(default_network.get("name", "")).startswith(f"{project_name}_"),
            "默认网络不是本次项目独占网络",
        )


def _require_isolation(condition: bool, reason: str) -> None:
    """以不受 Python assert 优化影响的异常阻止不安全 Compose 配置。

    参数：condition 为安全断言；reason 为不含凭据的安全门失败原因。
    返回值：条件为真时正常返回。
    异常：条件为假时抛出 RuntimeError，保证 fail closed。
    副作用：无。
    """
    if not condition:
        raise RuntimeError(f"T15 isolation check failed: {reason}")


def _validate_local_docker_endpoint(endpoint: str) -> None:
    """验证 T15 只能连接本机 Unix socket 或 Windows named pipe。

    参数：endpoint 为 Docker context 的 daemon 地址。
    返回值：无。
    异常：TCP/HTTP 转发以及无法确认本机 socket 的 endpoint 均 fail closed。
    副作用：无，不访问该 endpoint。
    """
    parsed = urlsplit(endpoint)
    local_endpoint = (
        parsed.scheme == "unix" and not parsed.netloc and parsed.path.startswith("/")
    ) or (
        parsed.scheme == "npipe"
        and not parsed.netloc
        and parsed.path.startswith("//./pipe/")
    )
    if not local_endpoint:
        raise RuntimeError(
            "T15 仅允许本机 Unix socket 或 Windows named pipe；其余 daemon endpoint 均拒绝"
        )


def _resolve_and_validate_compose(
    project_name: str, environment: dict[str, str]
) -> dict[str, object]:
    """在任何 build/up 之前验证本机 daemon 和 Compose 最终配置。

    参数：project_name 为随机项目名；environment 为仅含测试变量的进程环境。
    返回值：通过全部门槛的最终 Compose 配置。
    异常：命令失败、JSON 无效或配置存在持久化资源时测试失败且不启动容器。
    副作用：仅读取 Docker context 与 Compose 配置，不调用 daemon 创建资源。
    """
    try:
        context = subprocess.run(
            ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=environment,
            cwd=_REPOSITORY_ROOT,
        )
    except OSError as error:
        raise _subprocess_start_error(
            "docker_context", error, prior_command_executed=False
        ) from None
    _assert_compose_success(
        context,
        ("context", "inspect", "<local-endpoint>"),
        stage="docker_context",
    )
    try:
        _validate_local_docker_endpoint(context.stdout.strip())
    except Exception as error:
        raise T15ValidationError(
            "docker_endpoint",
            "DOCKER_ENDPOINT_UNSUPPORTED",
            "Docker endpoint 不在本机 socket 白名单",
            exception_type=type(error).__name__,
            external_command_executed=True,
        ) from None
    try:
        resolved = _run_compose(project_name, environment, "config", "--format", "json")
    except OSError as error:
        raise _subprocess_start_error(
            "compose_config", error, prior_command_executed=True
        ) from None
    _assert_compose_success(
        resolved, ("config", "--format", "json"), stage="compose_config"
    )
    try:
        config = json.loads(resolved.stdout)
    except (json.JSONDecodeError, TypeError) as error:
        raise T15ValidationError(
            "compose_json",
            "COMPOSE_JSON_INVALID",
            "Compose JSON 格式无效",
            exit_code=resolved.returncode,
            exception_type=type(error).__name__,
            external_command_executed=True,
        ) from None
    try:
        _validate_resolved_compose(config, project_name)
    except Exception as error:
        prefix = "T15 isolation check failed: "
        message = str(error)
        reason = (
            message.removeprefix(prefix)
            if message.startswith(prefix)
            else "最终 Compose 配置未通过隔离安全门"
        )
        raise T15ValidationError(
            "compose_safety",
            "COMPOSE_ISOLATION_FAILED",
            reason,
            exception_type=type(error).__name__,
            external_command_executed=True,
        ) from None
    return config


def _assert_compose_success(
    completed: subprocess.CompletedProcess[str],
    arguments: tuple[str, ...],
    *,
    stage: str | None = None,
) -> None:
    """检查子进程退出状态，并以脱敏阶段信息报告非零退出。

    参数：completed 为命令结果；arguments 只用于确定安全阶段；stage 可显式指定阶段。
    返回值：命令成功时无。
    异常：命令非零退出时抛出不包含 stdout/stderr 的 T15ValidationError。
    副作用：无。
    """
    if completed.returncode != 0:
        failure_stage = stage or (
            "docker_context"
            if arguments[:1] == ("context",)
            else "compose_config"
            if arguments[:1] == ("config",)
            else "compose_runtime"
        )
        failure_reason = {
            "docker_context": "Docker context 检查命令退出非零",
            "compose_config": "Compose 配置解析命令退出非零",
        }.get(failure_stage, "Compose 检查命令退出非零")
        raise T15ValidationError(
            failure_stage,
            {
                "docker_context": "DOCKER_CONTEXT_EXIT_NONZERO",
                "compose_config": "COMPOSE_CONFIG_EXIT_NONZERO",
            }.get(failure_stage, "COMPOSE_COMMAND_EXIT_NONZERO"),
            failure_reason,
            exit_code=completed.returncode,
            external_command_executed=True,
        )


def test_ticket15_compose_source_is_tmpfs_only_without_docker() -> None:
    """静态验证 T15 专用 Compose 与最终配置门槛，不依赖 Docker 或 PostgreSQL。

    参数：无。
    返回值：断言通过表示定义不含持久卷且拒绝固定卷回归。
    异常：配置出现危险资源时 pytest 断言失败。
    副作用：只读取仓库中的 JSON 文件。
    """
    config = json.loads(_COMPOSE_FILE.read_text(encoding="utf-8"))
    services = config["services"]
    assert set(services) == {"postgres", "migrate"}
    assert not config.get("volumes")
    postgres = services["postgres"]
    migrate = services["migrate"]
    assert postgres["tmpfs"] == ["/var/lib/postgresql/data:rw,size=1073741824"]
    assert "volumes" not in postgres and "volumes" not in migrate
    assert "container_name" not in postgres and "container_name" not in migrate
    assert "ports" not in postgres and "ports" not in migrate
    assert migrate["build"] == {"context": "../..", "dockerfile": "Dockerfile"}
    assert set(migrate["environment"]) == {
        "APP_ENV",
        "CRM_ADAPTER",
        "DATABASE_URL",
        "LLM_PROVIDER",
        "MEDIA_STORAGE_PROVIDER",
        "SMART_TABLE_ADAPTER",
        "TEST_DATABASE_ID",
        "TEST_DATABASE_URL",
        "TYC_PROVIDER",
    }
    serialized = json.dumps(config)
    assert "crm_t21_final_postgres_data" not in serialized
    assert "crm-lead-postgres-1" not in serialized
    assert "media_data" not in serialized
    assert "${T15_RUN_ID:?T15_RUN_ID required}" in serialized
    run_id = "a" * 32
    project_name = f"t15-migration-{run_id}"
    resolved = json.loads(serialized.replace("${T15_RUN_ID:?T15_RUN_ID required}", run_id))
    resolved["name"] = project_name
    resolved["services"]["migrate"]["build"]["context"] = str(_REPOSITORY_ROOT)
    resolved["networks"] = {"default": {"name": f"{project_name}_default"}}
    _validate_resolved_compose(resolved, project_name)
    unsafe = deepcopy(resolved)
    unsafe["services"]["postgres"]["volumes"] = [
        {"type": "volume", "source": "crm_t21_final_postgres_data", "target": "/data"}
    ]
    with pytest.raises(RuntimeError):
        _validate_resolved_compose(unsafe, project_name)


@pytest.mark.parametrize(
    "endpoint",
    (
        "tcp://127.0.0.1:2375",
        "http://localhost:2375",
        "https://[::1]:2376",
        "unix://remote-host/run/docker.sock",
        "ssh://remote-host",
    ),
)
def test_ticket15_rejects_nonlocal_or_forwardable_endpoints(endpoint: str) -> None:
    """静态安全门拒绝 TCP 转发及无法证明为本机 socket 的 endpoint。"""
    with pytest.raises(RuntimeError, match="其余 daemon endpoint 均拒绝"):
        _validate_local_docker_endpoint(endpoint)


@pytest.mark.parametrize(
    "endpoint",
    ("unix:///var/run/docker.sock", "npipe:////./pipe/docker_engine"),
)
def test_ticket15_accepts_local_socket_endpoints(endpoint: str) -> None:
    """静态安全门仅接受结构明确的本机 socket endpoint。"""
    _validate_local_docker_endpoint(endpoint)


def _resolved_compose_for_offline_test() -> tuple[str, dict[str, object]]:
    """从专用测试定义构造通过安全门的最终配置，不调用 Compose。

    参数：无。
    返回值：随机格式项目名及其隔离配置。
    异常：Compose 定义无法读取或替换时向调用方抛出解析异常。
    副作用：只读取仓库中的测试 JSON 文件。
    """
    run_id = "b" * 32
    project_name = f"t15-migration-{run_id}"
    serialized = json.dumps(json.loads(_COMPOSE_FILE.read_text(encoding="utf-8")))
    resolved = json.loads(serialized.replace("${T15_RUN_ID:?T15_RUN_ID required}", run_id))
    resolved["name"] = project_name
    resolved["services"]["migrate"]["build"]["context"] = str(_REPOSITORY_ROOT)
    resolved["networks"] = {
        "default": {"name": f"{project_name}_default", "external": False}
    }
    return project_name, resolved


def _mock_ticket15_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    context_result: subprocess.CompletedProcess[str] | OSError,
    compose_result: subprocess.CompletedProcess[str] | OSError | None = None,
) -> list[tuple[str, ...]]:
    """用记录调用的替身取代 subprocess.run，确保离线用例不启动外部程序。

    参数：monkeypatch 为 pytest 替换器；context_result 和 compose_result 为预置结果或启动异常。
    返回值：替身接收到的参数记录，可断言真实命令路径从未执行。
    异常：意外的第三次调用或缺少 Compose 结果时抛出断言错误。
    副作用：仅替换当前测试进程中的 subprocess.run。
    """
    calls: list[tuple[str, ...]] = []

    def fake_run(arguments: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        """返回预置结果并只记录参数，不创建进程。"""
        calls.append(tuple(arguments))
        result: subprocess.CompletedProcess[str] | OSError | None = (
            context_result if len(calls) == 1 else compose_result
        )
        if isinstance(result, OSError):
            raise result
        if result is None or len(calls) > 2:
            raise AssertionError("offline subprocess fake received an unexpected call")
        return result

    monkeypatch.setattr(subprocess, "run", fake_run)
    return calls


def _assert_safe_t15_diagnostic(
    error: T15ValidationError,
    *,
    stage: str,
    error_code: str,
    external_command_executed: bool,
    forbidden: tuple[str, ...] = (),
) -> None:
    """验证失败阶段、错误码及诊断内容脱敏。"""
    assert error.stage == stage
    assert error.error_code == error_code
    assert error.external_command_executed is external_command_executed
    record = error.to_safe_dict()
    assert record["status"] == "failed"
    safe_text = f"{error} {json.dumps(record, ensure_ascii=False)}"
    assert all(secret not in safe_text for secret in forbidden)


def test_ticket15_offline_diagnostic_when_docker_cli_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Docker CLI 缺失时报告启动阶段，且不暴露启动异常文本。"""
    project_name, _ = _resolved_compose_for_offline_test()
    calls = _mock_ticket15_subprocess(
        monkeypatch, FileNotFoundError("POSTGRES_PASSWORD=do-not-log")
    )

    with pytest.raises(T15ValidationError) as caught:
        _resolve_and_validate_compose(project_name, {"T15_RUN_ID": "b" * 32})

    error = caught.value
    _assert_safe_t15_diagnostic(
        error,
        stage="docker_context",
        error_code="DOCKER_CLI_NOT_FOUND",
        external_command_executed=False,
        forbidden=("POSTGRES_PASSWORD=do-not-log",),
    )
    assert error.exception_type == "FileNotFoundError"
    assert len(calls) == 1


def test_ticket15_offline_diagnostic_when_subprocess_cannot_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """普通 subprocess 启动错误与 Docker CLI 缺失使用不同安全错误码。"""
    project_name, _ = _resolved_compose_for_offline_test()
    calls = _mock_ticket15_subprocess(
        monkeypatch, PermissionError("private host path and DATABASE_URL=hidden")
    )

    with pytest.raises(T15ValidationError) as caught:
        _resolve_and_validate_compose(project_name, {})

    error = caught.value
    _assert_safe_t15_diagnostic(
        error,
        stage="docker_context",
        error_code="SUBPROCESS_START_FAILED",
        external_command_executed=False,
        forbidden=("private host path", "DATABASE_URL=hidden"),
    )
    assert error.exception_type == "PermissionError"
    assert len(calls) == 1


def test_ticket15_offline_diagnostic_when_context_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """context 命令非零退出时保留退出码并隐藏标准输出和错误。"""
    project_name, _ = _resolved_compose_for_offline_test()
    calls = _mock_ticket15_subprocess(
        monkeypatch,
        subprocess.CompletedProcess(
            ["docker", "context", "inspect"],
            17,
            "DATABASE_URL=hidden",
            "POSTGRES_PASSWORD=hidden",
        ),
    )

    with pytest.raises(T15ValidationError) as caught:
        _resolve_and_validate_compose(project_name, {})

    error = caught.value
    _assert_safe_t15_diagnostic(
        error,
        stage="docker_context",
        error_code="DOCKER_CONTEXT_EXIT_NONZERO",
        external_command_executed=True,
        forbidden=("DATABASE_URL=hidden", "POSTGRES_PASSWORD=hidden"),
    )
    assert error.exit_code == 17
    assert len(calls) == 1


@pytest.mark.parametrize("endpoint", ("npipe:////./pipe/docker_engine", "unix:///run/docker.sock"))
def test_ticket15_offline_accepts_local_endpoints_and_valid_compose(
    monkeypatch: pytest.MonkeyPatch, endpoint: str
) -> None:
    """模拟支持的本机 endpoint 与安全 Compose 配置完整通过只读检查链路。"""
    project_name, expected = _resolved_compose_for_offline_test()
    calls = _mock_ticket15_subprocess(
        monkeypatch,
        subprocess.CompletedProcess(["docker", "context", "inspect"], 0, endpoint, ""),
        subprocess.CompletedProcess(
            ["docker", "compose", "config"], 0, json.dumps(expected), ""
        ),
    )

    actual = _resolve_and_validate_compose(project_name, {"T15_RUN_ID": "b" * 32})

    assert actual == expected
    assert len(calls) == 2
    assert calls[0][1:3] == ("context", "inspect")
    assert calls[1][0:3] == ("docker", "compose", "--env-file")
    assert calls[1][-3:] == ("config", "--format", "json")


def test_ticket15_offline_rejects_tcp_endpoint_before_compose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """拒绝 TCP endpoint，并确认 Compose 解析没有被调用。"""
    project_name, _ = _resolved_compose_for_offline_test()
    calls = _mock_ticket15_subprocess(
        monkeypatch,
        subprocess.CompletedProcess(
            ["docker", "context", "inspect"], 0, "tcp://192.0.2.10:2375", ""
        ),
    )

    with pytest.raises(T15ValidationError) as caught:
        _resolve_and_validate_compose(project_name, {})

    error = caught.value
    _assert_safe_t15_diagnostic(
        error,
        stage="docker_endpoint",
        error_code="DOCKER_ENDPOINT_UNSUPPORTED",
        external_command_executed=True,
    )
    assert "192.0.2.10" not in str(error)
    assert len(calls) == 1


def test_ticket15_offline_diagnostic_when_compose_command_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compose config 非零退出被定位到配置阶段且不打印命令输出。"""
    project_name, _ = _resolved_compose_for_offline_test()
    calls = _mock_ticket15_subprocess(
        monkeypatch,
        subprocess.CompletedProcess(
            ["docker", "context", "inspect"], 0, "unix:///run/docker.sock", ""
        ),
        subprocess.CompletedProcess(
            ["docker", "compose", "config"],
            23,
            "DATABASE_URL=hidden",
            "POSTGRES_PASSWORD=hidden",
        ),
    )

    with pytest.raises(T15ValidationError) as caught:
        _resolve_and_validate_compose(project_name, {})

    error = caught.value
    _assert_safe_t15_diagnostic(
        error,
        stage="compose_config",
        error_code="COMPOSE_CONFIG_EXIT_NONZERO",
        external_command_executed=True,
        forbidden=("DATABASE_URL=hidden", "POSTGRES_PASSWORD=hidden"),
    )
    assert error.exit_code == 23
    assert len(calls) == 2


def test_ticket15_offline_diagnostic_when_compose_json_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compose JSON 解析错误返回独立阶段码，不泄露原始内容。"""
    project_name, _ = _resolved_compose_for_offline_test()
    calls = _mock_ticket15_subprocess(
        monkeypatch,
        subprocess.CompletedProcess(
            ["docker", "context", "inspect"], 0, "unix:///run/docker.sock", ""
        ),
        subprocess.CompletedProcess(
            ["docker", "compose", "config"],
            0,
            '{"POSTGRES_PASSWORD":"json-secret", invalid}',
            "",
        ),
    )

    with pytest.raises(T15ValidationError) as caught:
        _resolve_and_validate_compose(project_name, {})

    error = caught.value
    _assert_safe_t15_diagnostic(
        error,
        stage="compose_json",
        error_code="COMPOSE_JSON_INVALID",
        external_command_executed=True,
        forbidden=("json-secret", "POSTGRES_PASSWORD"),
    )
    assert error.exception_type == "JSONDecodeError"
    assert error.exit_code == 0
    assert len(calls) == 2


def test_ticket15_offline_diagnostic_when_compose_has_named_volume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """最终配置含危险 named volume 时在安全门阶段 fail closed。"""
    project_name, unsafe = _resolved_compose_for_offline_test()
    unsafe["volumes"] = {"legacy": {"name": "crm_t21_final_postgres_data"}}
    unsafe_text = json.dumps(unsafe)
    calls = _mock_ticket15_subprocess(
        monkeypatch,
        subprocess.CompletedProcess(
            ["docker", "context", "inspect"], 0, "unix:///run/docker.sock", ""
        ),
        subprocess.CompletedProcess(["docker", "compose", "config"], 0, unsafe_text, ""),
    )

    with pytest.raises(T15ValidationError) as caught:
        _resolve_and_validate_compose(project_name, {})

    error = caught.value
    _assert_safe_t15_diagnostic(
        error,
        stage="compose_safety",
        error_code="COMPOSE_ISOLATION_FAILED",
        external_command_executed=True,
        forbidden=("crm_t21_final_postgres_data", "POSTGRES_PASSWORD"),
    )
    assert error.reason == "包含 Compose named volume"
    assert len(calls) == 2


def test_current_migration_chain_reaches_single_head() -> None:
    """在独立临时 PostgreSQL 容器中执行真实 Alembic upgrade、heads 和 current。"""
    if shutil.which("docker") is None:
        pytest.fail("T15 migration 验证需要可用的 docker 命令，禁止静默跳过真实验证")

    run_id = uuid4().hex
    project_name = f"t15-migration-{run_id}"
    database_user = "t15_migration"
    database_name = f"crm_lead_test_{run_id}"
    # Docker 进程不继承数据库、WeCom、CRM、LLM 或 Smart Table 真实配置。
    environment = {
        key: os.environ[key]
        for key in (
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
        if key in os.environ
    }
    environment.update(
        {
            "T15_RUN_ID": run_id,
        }
    )
    compose_started = False
    try:
        # 必须先检查本机 daemon 与 Compose 的最终展开结果，安全门未通过不执行任何写操作。
        _resolve_and_validate_compose(project_name, environment)
        # 先构建当前代码镜像，确保容器内执行的 Alembic 与被测提交一致。
        # Compose 的 BuildKit raw 输出可能填满 subprocess pipe；wrapper 只需验证构建成功。
        build = _run_compose(project_name, environment, "build", "--quiet", "migrate")
        _assert_compose_success(build, ("build", "--quiet", "migrate"))

        # 只启动本测试专用 PostgreSQL 服务；不复用损坏的默认 Compose Volume。
        compose_started = True
        start = _run_compose(project_name, environment, "up", "-d", "postgres")
        _assert_compose_success(start, ("up", "-d", "postgres"))

        # 等待临时数据库接受连接，再由 migrate 容器执行迁移命令。
        for _ in range(30):
            readiness = _run_compose(
                project_name,
                environment,
                "exec",
                "-T",
                "postgres",
                "pg_isready",
                "-U",
                database_user,
                "-d",
                database_name,
            )
            if readiness.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail("独立临时 PostgreSQL 未在 30 秒内 ready")

        for arguments in (
            ("run", "--rm", "--no-deps", "migrate", "alembic", "upgrade", "head"),
            ("run", "--rm", "--no-deps", "migrate", "alembic", "heads"),
            ("run", "--rm", "--no-deps", "migrate", "alembic", "current"),
        ):
            completed = _run_compose(project_name, environment, *arguments)
            _assert_compose_success(completed, arguments)

        # 已写入的目录操作人属于 T17 审计事实，回退不能静默删除该字段数据。
        audit_seed = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "connection.execute(text(\"INSERT INTO sales_authorizations "
                "(wecom_user_id, is_authorized, is_active, is_administrator, "
                "next_message_sequence, created_by, updated_by, created_at, updated_at) "
                "VALUES ('audit-sales', true, true, false, 0, 'test-operator', "
                "'test-operator', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)\")); "
                "connection.commit(); connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(audit_seed, ("run", "migrate", "python", "-c", "<audit-seed>"))
        downgrade = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "alembic",
            "downgrade",
            "0019_ticket15_console_observability",
        )
        assert downgrade.returncode != 0
        assert "存在 T17 审计数据" in (downgrade.stdout + downgrade.stderr)

        heads = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "alembic",
            "heads",
        )
        _assert_compose_success(
            heads, ("run", "--rm", "--no-deps", "migrate", "alembic", "heads")
        )
        # 当前最新迁移继续保持单一 migration head。
        assert heads.stdout.count("0033_lead_progress_sessions") == 1

        # 只有 notification payload 的 T18 事实也必须阻止 downgrade，不能因没有 action 行而丢列。
        payload_seed = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "connection.execute(text(\"INSERT INTO notification_records "
                "(notification_key, sales_user_id, source_message_id, notification_type, "
                "status, attempts, payload, created_at) VALUES ('payload-only', 'audit-sales', "
                "'payload-source', 'wecom_action_card', 'pending', 0, "
                "'{\\\"msgtype\\\":\\\"template_card\\\"}', CURRENT_TIMESTAMP)\")); "
                "connection.commit(); connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(payload_seed, ("run", "migrate", "python", "-c", "<payload-seed>"))
        payload_downgrade = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "alembic",
            "downgrade",
            "0021_ticket16_admin_maintenance",
        )
        assert payload_downgrade.returncode != 0
        assert "notification payload" in (payload_downgrade.stdout + payload_downgrade.stderr)
        payload_check = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "assert connection.execute(text(\"SELECT payload FROM notification_records "
                "WHERE notification_key='payload-only'\")).scalar() is not None; "
                "connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(
            payload_check, ("run", "migrate", "python", "-c", "<payload-check>")
        )
        payload_cleanup = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "connection.execute(text(\"DELETE FROM notification_records WHERE "
                "notification_key='payload-only'\")); connection.commit(); "
                "connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(
            payload_cleanup, ("run", "migrate", "python", "-c", "<payload-cleanup>")
        )

        # T18 action/callback 事实一旦产生，同样禁止通过 downgrade 静默丢失不可变审计。
        t18_seed = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "connection.execute(text(\"INSERT INTO wecom_actions "
                "(id, task_id, action_type, bound_actor_wecom_user_id, target_type, target_id, "
                "expected_action_key, status, expires_at, context, created_at, updated_at) "
                "VALUES ('t18-action-1', 't18-task-1', 'lead.discard.confirmation', 'audit-sales', "
                "'lead', 'lead-1', 'lead.discard.confirm', 'pending', CURRENT_TIMESTAMP, '{}', "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)\")); "
                "connection.execute(text(\"INSERT INTO wecom_callback_deliveries "
                "(action_id, provider_msgid, req_id, actor_user_id, event_key, task_id, "
                "processing_status, received_at) VALUES ('t18-action-1', 'provider-1', 'req-1', "
                "'audit-sales', 'lead.discard.confirm', 't18-task-1', 'claimed', "
                "CURRENT_TIMESTAMP)\")); "
                "connection.commit(); connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(t18_seed, ("run", "migrate", "python", "-c", "<t18-seed>"))
        t18_downgrade = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "alembic",
            "downgrade",
            "0021_ticket16_admin_maintenance",
        )
        assert t18_downgrade.returncode != 0
        assert "存在 T18 企业微信动作或 callback 事实" in (
            t18_downgrade.stdout + t18_downgrade.stderr
        )
    finally:
        if compose_started:
            # 项目与网络名唯一；配置无任何 Volume，清理不带 -v 且只针对该项目。
            cleanup = _run_compose(project_name, environment, "down", "--remove-orphans")
            _assert_compose_success(cleanup, ("down", "--remove-orphans"))
