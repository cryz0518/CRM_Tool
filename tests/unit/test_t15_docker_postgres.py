"""T15 直接 Docker PostgreSQL 运行器的离线安全契约测试。"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from scripts import t15_postgres_docker_test, t15_postgres_test_worker
from tests.integration.test_lead_progress_postgres import _validated_test_database_url


def _safe_postgres_inspect(identity: Any) -> dict[str, Any]:
    """构造仅含随机身份与 tmpfs 的模拟 PostgreSQL inspect 数据。

    参数：identity 为运行器生成的单次测试身份。
    返回值：Docker inspect 结构，不连接 Docker 或数据库。
    异常：无。
    副作用：无。
    """
    return {
        "Name": f"/{identity.postgres_container_name}",
        "Config": {
            "Image": "postgres:16",
            "Env": [
                f"POSTGRES_USER={identity.database_user}",
                f"POSTGRES_PASSWORD={identity.database_password}",
                f"POSTGRES_DB={identity.database_name}",
                "PGDATA=/var/lib/postgresql/data/pgdata",
            ],
            "Labels": {"codex.t15.run_id": identity.run_id},
        },
        "HostConfig": {
            "NetworkMode": identity.network_name,
            "Binds": [],
            "VolumesFrom": [],
            "PortBindings": None,
            "Tmpfs": {
                "/var/lib/postgresql/data": "rw,nosuid,nodev,size=1073741824"
            },
        },
        "Mounts": [
            {"Type": "tmpfs", "Destination": "/var/lib/postgresql/data"}
        ],
    }


def test_test_database_identity_uses_random_user_database_and_password() -> None:
    """新运行器生成的用户名、数据库名与密码都绑定本次随机标识。

    参数：无。
    返回值：无。
    异常：身份规则不一致时由 pytest 报告断言失败。
    副作用：只构造 URL 并调用现有纯解析身份校验，不连接数据库。
    """
    run_id = "a" * 32
    identity = t15_postgres_docker_test._new_test_identity(run_id)
    url, checked_run_id, checked_user = _validated_test_database_url(
        {
            "TEST_DATABASE_ID": identity.run_id,
            "TEST_DATABASE_USER": identity.database_user,
            "TEST_DATABASE_URL": identity.database_url,
        }
    )

    assert checked_run_id == run_id
    assert checked_user == f"t15_{run_id}"
    assert url == identity.database_url
    assert identity.database_name == f"crm_lead_test_{run_id}"
    assert identity.database_password == f"t15_{run_id}"


def test_postgres_and_worker_commands_have_only_expected_tmpfs_mounts() -> None:
    """PostgreSQL 与测试容器命令不含宿主挂载、卷挂载或端口发布。

    参数：无。
    返回值：无。
    异常：命令计划越过安全白名单时由 pytest 报告断言失败。
    副作用：只构造 Docker 参数，不启动外部进程。
    """
    identity = t15_postgres_docker_test._new_test_identity("b" * 32)
    image = f"crm-tool-pr83-test:{'c' * 40}"
    postgres = t15_postgres_docker_test._postgres_create_argv(identity)
    worker = t15_postgres_docker_test._worker_run_argv(identity, image)

    assert postgres[:3] == ["docker", "container", "create"]
    assert "--internal" in t15_postgres_docker_test._network_create_argv(identity)
    assert worker[:3] == ["docker", "container", "run"]
    assert "--read-only" in worker
    assert "--network" in worker
    assert "none" not in worker
    assert "postgres:16" in postgres
    assert image in worker
    for argv in (postgres, worker):
        assert not any(
            value == flag or value.startswith(f"{flag}=")
            for value in argv
            for flag in ("-v", "--volume", "--mount", "-p", "--publish", "--volumes-from")
        )
        assert argv.count("--tmpfs") == 1
    assert postgres[postgres.index("--tmpfs") + 1].startswith(
        "/var/lib/postgresql/data:"
    )
    assert worker[worker.index("--tmpfs") + 1].startswith("/tmp:")


def test_postgres_inspect_accepts_tmpfs_and_rejects_bind_or_named_volume() -> None:
    """启动测试前仅接受本轮容器上的 tmpfs，不接受 bind 或 named volume。

    参数：无。
    返回值：无。
    异常：不安全 Mounts、端口或身份时由 pytest 报告断言失败。
    副作用：只校验模拟 inspect JSON。
    """
    identity = t15_postgres_docker_test._new_test_identity("d" * 32)
    safe = _safe_postgres_inspect(identity)
    t15_postgres_docker_test._validate_postgres_inspect(safe, identity)

    for unsafe_mount in (
        {"Type": "volume", "Name": "crm_t21_final_postgres_data", "Destination": "/data"},
        {"Type": "bind", "Source": "C:/private/database", "Destination": "/data"},
    ):
        unsafe = json.loads(json.dumps(safe))
        unsafe["Mounts"] = [unsafe_mount]
        with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
            t15_postgres_docker_test._validate_postgres_inspect(unsafe, identity)


def test_postgres_inspect_rejects_published_ports_and_wrong_database_identity() -> None:
    """inspect 安全门拒绝宿主端口发布和身份不匹配的容器。

    参数：无。
    返回值：无。
    异常：不安全模拟数据未被拒绝时由 pytest 报告断言失败。
    副作用：无，不访问 Docker 或数据库。
    """
    identity = t15_postgres_docker_test._new_test_identity("e" * 32)
    unsafe_port = _safe_postgres_inspect(identity)
    unsafe_port["HostConfig"]["PortBindings"] = {"5432/tcp": [{"HostPort": "5432"}]}
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
        t15_postgres_docker_test._validate_postgres_inspect(unsafe_port, identity)

    unsafe_identity = _safe_postgres_inspect(identity)
    unsafe_identity["Config"]["Env"][1] = "POSTGRES_PASSWORD=production-looking-value"
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError) as caught:
        t15_postgres_docker_test._validate_postgres_inspect(unsafe_identity, identity)
    assert "production-looking-value" not in str(caught.value)


def test_unit_test_command_is_read_only_networkless_and_has_no_mounts() -> None:
    """单元测试容器命令使用 none 网络、只读根目录和唯一 /tmp tmpfs。

    参数：无。
    返回值：无。
    异常：命令计划越过安全边界时由 pytest 报告断言失败。
    副作用：只构造和验证参数，不启动容器。
    """
    image = f"crm-tool-pr83-test:{'f' * 40}"
    argv = t15_postgres_docker_test._unit_test_run_argv(image)

    assert argv[:3] == ["docker", "container", "run"]
    assert argv[argv.index("--network") + 1] == "none"
    assert "--read-only" in argv
    assert argv[argv.index("--tmpfs") + 1].startswith("/tmp:")
    assert argv[-3:] == ["-m", "pytest", "tests/unit"]
    assert not any(flag in argv for flag in ("-v", "--volume", "--mount", "-p", "--publish"))
    t15_postgres_docker_test._validate_direct_docker_argv(argv)


def test_docker_subprocess_boundary_is_fake_and_rejects_non_allowlisted_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Docker 命令验证只触达 fake subprocess，且拒绝 Compose 和危险挂载。

    参数：monkeypatch 替换 subprocess.run，禁止任何真实 Docker 调用。
    返回值：无。
    异常：安全校验未拒绝危险命令或 fake 调用边界被绕过时断言失败。
    副作用：只在内存中记录模拟命令。
    """
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """记录模拟命令及清理后的环境，不启动真实子进程。

        参数：arguments 为模拟命令；kwargs 为传给 subprocess.run 的参数。
        返回值：成功的假 CompletedProcess。
        异常：无。
        副作用：向测试本地 calls 列表追加安全检查输入。
        """
        calls.append((arguments, kwargs))
        return subprocess.CompletedProcess(arguments, 0, "local endpoint", "")

    monkeypatch.setattr(t15_postgres_docker_test.subprocess, "run", fake_run)
    output = t15_postgres_docker_test._run_docker(
        ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
        {"PATH": "fake-path"},
        "docker_context",
    )

    assert output == "local endpoint"
    assert len(calls) == 1
    assert calls[0][1]["env"] == {"PATH": "fake-path"}
    for unsafe in (
        ["docker", "compose", "config"],
        ["docker", "container", "run", "-v", "C:/private:/data", "image"],
        ["docker", "system", "prune", "--all"],
        ["docker", "container", "run", "--privileged", "image"],
    ):
        with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
            t15_postgres_docker_test._run_docker(unsafe, {"PATH": "fake-path"}, "test")
    assert len(calls) == 1


@pytest.mark.parametrize(
    "endpoint",
    (
        "npipe:////./pipe/docker_engine",
        "npipe:////./pipe/dockerDesktopLinuxEngine",
    ),
)
def test_docker_runner_accepts_only_local_desktop_named_pipes(endpoint: str) -> None:
    """Windows 运行器仅接受 Docker Desktop 的本机 named pipe。

    参数：endpoint 为参数化的本机 Docker Desktop named pipe。
    返回值：无。
    异常：白名单接受失败时由 pytest 报告。
    副作用：只做字符串校验，不访问 Docker。
    """
    t15_postgres_docker_test._validate_local_docker_desktop_endpoint(endpoint)


@pytest.mark.parametrize(
    "endpoint",
    (
        "tcp://127.0.0.1:2375",
        "tcp://remote.invalid:2375",
        "ssh://remote.invalid",
        "npipe:////remote/pipe/docker_engine",
        "npipe:////./pipe/unrecognized_engine",
        "unix:///var/run/docker.sock",
    ),
)
def test_docker_runner_rejects_remote_or_unrecognized_endpoints(endpoint: str) -> None:
    """TCP、SSH、非本机 named pipe 和 Unix socket 均不满足本轮本机约束。

    参数：endpoint 为参数化的远程或未知 endpoint。
    返回值：无。
    异常：endpoint 未被拒绝时由 pytest 报告断言失败。
    副作用：只做字符串校验，不访问 Docker。
    """
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
        t15_postgres_docker_test._validate_local_docker_desktop_endpoint(endpoint)


def test_postgres_runner_does_not_call_docker_without_explicit_execute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """无 --execute 时脚本拒绝运行且不会调用任何 Docker CLI。

    参数：monkeypatch 用于替换子进程函数并记录潜在调用。
    返回值：无。
    异常：脚本发生外部命令调用或错误接受执行时由 pytest 报告断言失败。
    副作用：只在测试内替换 subprocess.run。
    """
    calls: list[list[str]] = []

    def fake_run(arguments: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        """记录被意外调用的命令，供无副作用断言使用。"""
        calls.append(arguments)
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(t15_postgres_docker_test.subprocess, "run", fake_run)

    assert t15_postgres_docker_test.main([]) != 0
    assert calls == []


def test_worker_script_targets_migration_0033_and_scheduler_concurrency() -> None:
    """未来数据库 worker 固定验证 0033 并只选择调度器并发集成用例。

    参数：无。
    返回值：无。
    异常：源文件缺少固定迁移/测试目标或出现 Compose 时由 pytest 报告。
    副作用：只读取脚本源文件，不启动 worker、数据库或 Docker。
    """
    worker_path = Path(t15_postgres_docker_test.__file__).with_name(
        "t15_postgres_test_worker.py"
    )
    source = worker_path.read_text(encoding="utf-8")

    assert "command.upgrade(config, \"head\")" in source
    assert "0033_lead_progress_sessions" in source
    assert "test_concurrent_schedulers_create_one_logical_notification" in source
    assert "docker compose" not in source.lower()


def test_postgres_worker_requires_matching_random_database_identity() -> None:
    """worker 只接受相同的随机测试 DSN、用户名和应用数据库配置。

    参数：无。
    返回值：无。
    异常：不安全环境未被拒绝时由 pytest 报告断言失败。
    副作用：只做纯字符串校验，不创建连接或运行 Alembic。
    """
    run_id = "a" * 32
    database_user = f"t15_{run_id}"
    database_url = (
        f"postgresql+psycopg://{database_user}:t15_{run_id}@postgres:5432/"
        f"crm_lead_test_{run_id}"
    )
    environment = {
        "APP_ENV": "test",
        "DATABASE_URL": database_url,
        "TEST_DATABASE_ID": run_id,
        "TEST_DATABASE_USER": database_user,
        "TEST_DATABASE_URL": database_url,
    }
    assert t15_postgres_test_worker._validate_worker_environment(environment) == (
        database_url,
        run_id,
        database_user,
    )
    for key, value in (
        ("DATABASE_URL", "postgresql://unexpected"),
        ("TEST_DATABASE_USER", "t15_migration"),
    ):
        unsafe = {**environment, key: value}
        with pytest.raises(ValueError):
            t15_postgres_test_worker._validate_worker_environment(unsafe)


def test_test_image_excludes_local_secrets_and_real_wecom_cli() -> None:
    """测试镜像仅安装 Python 依赖并排除本地环境文件和凭据文件。

    参数：无。
    返回值：无。
    异常：镜像清单包含真实 CLI 或遗漏敏感路径排除规则时由 pytest 报告。
    副作用：只读取仓库内受版本管理的配置文件。
    """
    repository_root = Path(t15_postgres_docker_test.__file__).parents[1]
    dockerfile = (repository_root / "Dockerfile.pr83-test").read_text(encoding="utf-8")
    ignore = (repository_root / "Dockerfile.pr83-test.dockerignore").read_text(
        encoding="utf-8"
    )

    assert "COPY scripts ./scripts" in dockerfile
    assert "COPY tests ./tests" in dockerfile
    assert "COPY alembic ./alembic" in dockerfile
    assert "@wecom/cli" not in dockerfile
    assert "**/.env.*" in ignore
    assert "tests/integration/ticket15-compose.env" in ignore
    assert "**/*credentials*" in ignore
    assert "**/*secret*" in ignore
