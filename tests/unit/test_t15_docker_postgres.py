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
            # 官方镜像声明 VOLUME；实际挂载仍须由 tmpfs Mounts 明确覆盖。
            "Volumes": {"/var/lib/postgresql/data": {}},
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


def _safe_worker_inspect(identity: Any, image: str, network_name: str) -> dict[str, Any]:
    """构造本次 Worker 的有效 inspect 数据供清理边界测试使用。

    参数：identity、image 和 network_name 描述唯一的一次性测试容器。
    返回值：包含运行状态、随机 label 和 /tmp tmpfs 的 inspect 字典。
    异常：无。
    副作用：无，不连接 Docker。
    """
    return {
        "Name": f"/{identity.worker_container_name}",
        "Config": {
            "Image": image,
            "Labels": {"codex.t15.run_id": identity.run_id},
            "Volumes": None,
        },
        "HostConfig": {
            "NetworkMode": network_name,
            "Binds": [],
            "VolumesFrom": [],
            "PortBindings": None,
            "Tmpfs": {"/tmp": t15_postgres_docker_test._WORKER_TMPFS.split(":", 1)[1]},
        },
        "Mounts": [{"Type": "tmpfs", "Destination": "/tmp"}],
        "State": {"Running": True},
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

    missing_mount = json.loads(json.dumps(safe))
    missing_mount["Mounts"] = []
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
        t15_postgres_docker_test._validate_postgres_inspect(missing_mount, identity)

    configured_volume = json.loads(json.dumps(safe))
    configured_volume["HostConfig"]["Mounts"] = [
        {"Type": "volume", "Source": "untrusted", "Target": "/var/lib/postgresql/data"}
    ]
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
        t15_postgres_docker_test._validate_postgres_inspect(configured_volume, identity)


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
    identity = t15_postgres_docker_test._new_test_identity("f" * 32)
    argv = t15_postgres_docker_test._unit_test_run_argv(identity, image)

    assert argv[:3] == ["docker", "container", "run"]
    assert argv[argv.index("--network") + 1] == "none"
    assert "--read-only" in argv
    assert argv[argv.index("--tmpfs") + 1].startswith("/tmp:")
    assert argv[-5:] == ["-m", "pytest", "-p", "no:cacheprovider", "tests/unit"]
    docker_options = argv[: argv.index(image)]
    assert not any(
        flag in docker_options
        for flag in ("-v", "--volume", "--mount", "-p", "--publish")
    )
    t15_postgres_docker_test._validate_direct_docker_argv(argv)


def test_unit_entry_is_separate_and_timeout_triggers_identity_scoped_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """单元模式超时只清理本轮 Worker，不创建网络或进入 PostgreSQL 流程。

    参数：monkeypatch 替换 Docker 子进程和清理函数，所有执行均为 fake。
    返回值：无。
    异常：模式路由、命令边界或超时清理不符合要求时由 pytest 报告。
    副作用：只在内存中记录模拟命令。
    """
    image = f"crm-tool-pr83-test:{'1' * 40}"
    calls: list[list[str]] = []
    cleanups: list[tuple[str, str]] = []

    def fake_run(
        arguments: list[str],
        _environment: dict[str, str],
        stage: str,
        **_kwargs: Any,
    ) -> str:
        """模拟本机 endpoint 检查和单元容器超时，不执行外部命令。

        参数：arguments、environment、stage 及附加参数模拟 Docker 包装器调用。
        返回值：context 检查返回白名单 endpoint。
        异常：模拟单元 Worker 超时以验证专属清理路径。
        副作用：把命令添加到测试本地列表。
        """
        calls.append(arguments)
        if stage == "docker_context":
            return "npipe:////./pipe/docker_engine"
        assert _kwargs["timeout"] == t15_postgres_docker_test._TEST_WORKER_TIMEOUT_SECONDS
        raise t15_postgres_docker_test.T15DockerSafetyError(
            stage, "DOCKER_COMMAND_TIMEOUT", "模拟 Worker 超时"
        )

    def fake_cleanup(
        identity: Any, passed_image: str, network: str, _environment: dict[str, str]
    ) -> None:
        """记录身份约束的清理目标，不调用 Docker。

        参数：identity、image、network 与 environment 来自测试调用。
        返回值：无。
        异常：image 或网络越界时断言失败。
        副作用：仅向测试列表记录资源名。
        """
        assert passed_image == image
        assert network == "none"
        cleanups.append((identity.worker_container_name, network))

    monkeypatch.setattr(t15_postgres_docker_test.shutil, "which", lambda _name: "docker")
    monkeypatch.setattr(t15_postgres_docker_test, "_docker_environment", lambda: {})
    monkeypatch.setattr(t15_postgres_docker_test, "_run_docker", fake_run)
    monkeypatch.setattr(t15_postgres_docker_test, "_cleanup_worker", fake_cleanup)
    monkeypatch.setattr(
        t15_postgres_docker_test,
        "_run_once",
        lambda _image: pytest.fail("单元入口不得进入 PostgreSQL 流程"),
    )

    status, stage, error_code, _exception = t15_postgres_docker_test._run_unit_once(image)

    assert status == "failed"
    assert stage == "unit_tests"
    assert error_code == "DOCKER_COMMAND_TIMEOUT"
    assert len(calls) == 2
    assert calls[1][1:3] == ["container", "run"]
    assert "postgres" not in calls[1]
    assert "alembic" not in calls[1]
    assert cleanups and cleanups[0][1] == "none"


def test_main_dispatches_unit_mode_without_invoking_postgres_mode(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """主入口显式 --unit 只派发无数据库单元测试流程。

    参数：monkeypatch 替换两个模式的执行函数；capsys 捕获脱敏状态 JSON。
    返回值：无。
    异常：默认或模式分派错误时由 pytest 报告断言失败。
    副作用：只输出模拟成功状态，不运行 Docker 或数据库。
    """
    image = f"crm-tool-pr83-test:{'6' * 40}"
    monkeypatch.setattr(
        t15_postgres_docker_test,
        "_run_unit_once",
        lambda _image: ("passed", "unit_tests", "OK", None),
    )
    monkeypatch.setattr(
        t15_postgres_docker_test,
        "_run_once",
        lambda _image: pytest.fail("--unit 不得启动 PostgreSQL 流程"),
    )

    assert t15_postgres_docker_test.main(["--unit", "--image", image]) == 0
    assert json.loads(capsys.readouterr().out)["stage"] == "unit_tests"


def test_worker_cleanup_stops_only_verified_run_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker 清理仅作用于名称、镜像、网络和随机 label 全部匹配的容器。

    参数：monkeypatch 把 Docker 调用替换为可控 fake。
    返回值：无。
    异常：清理边界不符时由 pytest 报告断言失败。
    副作用：只记录 fake inspect、stop 与 remove 命令。
    """
    identity = t15_postgres_docker_test._new_test_identity("2" * 32)
    image = f"crm-tool-pr83-test:{'3' * 40}"
    container = _safe_worker_inspect(identity, image, "none")
    calls: list[list[str]] = []
    removed = False

    def fake_run(
        arguments: list[str],
        _environment: dict[str, str],
        stage: str,
        **_kwargs: Any,
    ) -> str:
        """返回模拟 inspect 并记录针对已验证容器的 stop/remove。"""
        nonlocal removed
        calls.append(arguments)
        if stage == "worker_cleanup_inspect":
            return json.dumps([container])
        if stage == "worker_cleanup_stop":
            container["State"]["Running"] = False
            return ""
        if stage == "worker_cleanup_remove":
            removed = True
            return ""
        if stage == "worker_cleanup_verify" and removed:
            return ""
        raise AssertionError(f"未预期的 fake Docker 阶段: {stage}")

    monkeypatch.setattr(t15_postgres_docker_test, "_run_docker", fake_run)
    t15_postgres_docker_test._cleanup_worker(identity, image, "none", {})

    assert [argv[2] for argv in calls] == ["inspect", "stop", "rm", "inspect"]
    assert calls[1][-1] == identity.worker_container_name
    assert calls[2][-1] == identity.worker_container_name


def test_worker_cleanup_rejects_replaced_identity_before_stop_or_remove(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """身份校验失败时清理代码不得停止或删除同名非本次容器。

    参数：monkeypatch 替换 Docker 调用，使其只返回伪造 inspect 数据。
    返回值：无。
    异常：若代码继续 stop 或 remove 未验证资源，则断言失败。
    副作用：只记录 fake inspect 命令。
    """
    identity = t15_postgres_docker_test._new_test_identity("4" * 32)
    image = f"crm-tool-pr83-test:{'5' * 40}"
    container = _safe_worker_inspect(identity, image, "none")
    container["Config"]["Labels"]["codex.t15.run_id"] = "0" * 32
    calls: list[list[str]] = []

    def fake_run(
        arguments: list[str],
        _environment: dict[str, str],
        _stage: str,
        **_kwargs: Any,
    ) -> str:
        """只返回伪造的同名容器 inspect 结果。

        参数：arguments 为待执行参数；其他值为模拟执行上下文。
        返回值：含错误 run label 的模拟 inspect JSON。
        异常：无。
        副作用：只记录参数，不执行 Docker。
        """
        calls.append(arguments)
        return json.dumps([container])

    monkeypatch.setattr(t15_postgres_docker_test, "_run_docker", fake_run)
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
        t15_postgres_docker_test._cleanup_worker(identity, image, "none", {})

    assert len(calls) == 1
    assert calls[0][2] == "inspect"


def test_docker_timeout_has_specific_safe_error_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Docker 命令超时会成为独立安全错误，不泄露原始命令输出。

    参数：monkeypatch 把 subprocess.run 替换为超时异常。
    返回值：无。
    异常：错误码或脱敏结果错误时由 pytest 报告。
    副作用：只在内存中生成 TimeoutExpired，不创建外部进程。
    """

    def fake_run(arguments: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        """模拟 subprocess 超时，不创建真实 Docker 进程。

        参数：arguments 为伪命令；附加参数由被测包装器提供。
        返回值：此替身必定抛出 TimeoutExpired。
        异常：有意抛出超时异常以验证固定诊断。
        副作用：无外部进程。
        """
        raise subprocess.TimeoutExpired(arguments, timeout=1, stderr="hidden-secret")

    monkeypatch.setattr(t15_postgres_docker_test.subprocess, "run", fake_run)
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError) as caught:
        t15_postgres_docker_test._run_docker(
            ["docker", "context", "inspect"], {}, "docker_context"
        )
    assert caught.value.error_code == "DOCKER_COMMAND_TIMEOUT"
    assert "hidden-secret" not in str(caught.value)


def test_docker_not_found_is_accepted_only_for_exact_cleanup_resource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cleanup 只把本次资源名称完全匹配的 not-found 视为已清理。

    参数：monkeypatch 替换子进程，模拟 Docker 的安全 not-found 响应。
    返回值：无。
    异常：未脱敏处理或接受不匹配名称时断言失败。
    副作用：只构造 CompletedProcess，不运行 Docker。
    """
    name = "t15-it-worker-" + "a" * 32

    def fake_run(
        arguments: list[str], **_kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        """模拟 Docker daemon 返回的精确 not-found 响应。"""
        return subprocess.CompletedProcess(
            arguments, 1, "", f"Error response from daemon: No such container: {name}"
        )

    monkeypatch.setattr(t15_postgres_docker_test.subprocess, "run", fake_run)
    assert (
        t15_postgres_docker_test._run_docker(
            ["docker", "container", "inspect", name],
            {},
            "cleanup",
            not_found_name=name,
        )
        == ""
    )
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
        t15_postgres_docker_test._run_docker(
            ["docker", "container", "inspect", name],
            {},
            "cleanup",
            not_found_name="other-container",
        )


def test_docker_network_not_found_response_confirms_exact_cleanup_resource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """网络清理只接受 Docker daemon 针对本轮随机网络的精确 not-found 响应。"""
    name = "t15-it-net-" + "b" * 32

    def fake_run(
        arguments: list[str], **_kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        """模拟 Docker Desktop 的网络 not-found 错误格式。"""
        return subprocess.CompletedProcess(
            arguments, 1, "", f"Error response from daemon: network {name} not found"
        )

    monkeypatch.setattr(t15_postgres_docker_test.subprocess, "run", fake_run)
    assert (
        t15_postgres_docker_test._run_docker(
            ["docker", "network", "inspect", name],
            {},
            "network_cleanup_verify",
            not_found_name=name,
        )
        == ""
    )
    with pytest.raises(t15_postgres_docker_test.T15DockerSafetyError):
        t15_postgres_docker_test._run_docker(
            ["docker", "network", "inspect", name],
            {},
            "network_cleanup_verify",
            not_found_name="other-network",
        )


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
        ["docker", "container", "run", "--network", "host", "image"],
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
