"""直接 Docker CLI 的 T15 隔离测试运行器。

默认只拒绝执行；--unit 与 --postgres 是两个独立入口，后者才会创建一次性 PostgreSQL。
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

_LOCAL_DOCKER_DESKTOP_ENDPOINTS = {
    "npipe:////./pipe/docker_engine",
    "npipe:////./pipe/dockerDesktopLinuxEngine",
}
_POSTGRES_IMAGE = "postgres:16"
_DATABASE_TMPFS = "/var/lib/postgresql/data:rw,nosuid,nodev,size=1073741824"
_WORKER_TMPFS = "/tmp:rw,nosuid,nodev,noexec,size=268435456"
_DOCKER_CHECK_TIMEOUT_SECONDS = 20
_TEST_WORKER_TIMEOUT_SECONDS = 900
_LABEL_KEY = "codex.t15.run_id"
_REJECTED_DOCKER_OVERRIDES = {
    "DOCKER_HOST",
    "DOCKER_CONTEXT",
    "DOCKER_CONFIG",
    "DOCKER_TLS_VERIFY",
    "DOCKER_CERT_PATH",
}


@dataclass(frozen=True, slots=True)
class _TestIdentity:
    """保存单次运行的随机资源名；repr 隐去数据库凭据。"""

    run_id: str
    network_name: str
    postgres_container_name: str
    worker_container_name: str
    database_user: str
    database_name: str
    database_password: str = field(repr=False)
    database_url: str = field(repr=False)


class T15DockerSafetyError(RuntimeError):
    """表示 Docker 测试安全门失败，错误文本不包含命令输出或凭据。"""

    def __init__(self, stage: str, error_code: str, reason: str) -> None:
        """保存安全阶段与固定错误信息。

        参数：stage、error_code 与 reason 均为固定诊断值。
        返回值：无。
        异常：无。
        副作用：无。
        """
        self.stage = stage
        self.error_code = error_code
        super().__init__(reason)


def _new_test_identity(run_id: str | None = None) -> _TestIdentity:
    """生成本次测试独占的容器、网络和 PostgreSQL 身份。

    参数：run_id 可选，仅供离线测试传入确定性随机标识。
    返回值：所有资源名和数据库凭据均绑定同一个 32 位随机标识的对象。
    异常：run_id 格式不符合一次性测试约束时抛出 T15DockerSafetyError。
    副作用：不访问 Docker 或数据库。
    """
    identity_id = uuid4().hex if run_id is None else run_id
    if re.fullmatch(r"[0-9a-f]{32}", identity_id) is None:
        raise T15DockerSafetyError(
            "identity", "TEST_ID_INVALID", "测试运行标识不是 32 位随机十六进制值"
        )
    database_user = f"t15_{identity_id}"
    database_name = f"crm_lead_test_{identity_id}"
    database_password = f"t15_{identity_id}"
    database_url = (
        f"postgresql+psycopg://{database_user}:{database_password}"
        f"@postgres:5432/{database_name}"
    )
    return _TestIdentity(
        run_id=identity_id,
        network_name=f"t15-it-net-{identity_id}",
        postgres_container_name=f"t15-it-postgres-{identity_id}",
        worker_container_name=f"t15-it-worker-{identity_id}",
        database_user=database_user,
        database_name=database_name,
        database_password=database_password,
        database_url=database_url,
    )


def _validate_local_docker_desktop_endpoint(endpoint: str) -> None:
    """仅接受两个已知的本机 Docker Desktop named pipe。

    参数：endpoint 为 Docker context 返回的 daemon 地址。
    返回值：匹配本机 Docker Desktop 白名单时无。
    异常：TCP、Unix、SSH、转发或未知 named pipe 均抛出安全错误。
    副作用：不连接 endpoint。
    """
    if endpoint.strip() not in _LOCAL_DOCKER_DESKTOP_ENDPOINTS:
        raise T15DockerSafetyError(
            "docker_endpoint",
            "DOCKER_ENDPOINT_NOT_LOCAL_DESKTOP_PIPE",
            "当前 Docker endpoint 不是受支持的本机 Docker Desktop named pipe",
        )


def _network_create_argv(identity: _TestIdentity) -> list[str]:
    """生成带随机名称和标识标签的内部 bridge 网络命令。

    参数：identity 为本次随机测试身份。
    返回值：直接 Docker CLI 参数，不包含 Compose 或宿主机挂载。
    异常：无。
    副作用：无，不执行命令。
    """
    return [
        "docker",
        "network",
        "create",
        "--driver",
        "bridge",
        "--internal",
        "--label",
        f"{_LABEL_KEY}={identity.run_id}",
        identity.network_name,
    ]


def _postgres_create_argv(identity: _TestIdentity) -> list[str]:
    """生成不发布端口且 PGDATA 只落在 tmpfs 的 PostgreSQL create 命令。

    参数：identity 为本次随机测试身份。
    返回值：Docker create 参数；未启动容器，也未挂载宿主路径或 Docker volume。
    异常：无。
    副作用：无，不执行命令。
    """
    return [
        "docker",
        "container",
        "create",
        "--pull=never",
        "--name",
        identity.postgres_container_name,
        "--network",
        identity.network_name,
        "--network-alias",
        "postgres",
        "--label",
        f"{_LABEL_KEY}={identity.run_id}",
        "--tmpfs",
        _DATABASE_TMPFS,
        "--env",
        f"POSTGRES_USER={identity.database_user}",
        "--env",
        f"POSTGRES_PASSWORD={identity.database_password}",
        "--env",
        f"POSTGRES_DB={identity.database_name}",
        "--env",
        "PGDATA=/var/lib/postgresql/data/pgdata",
        "--health-cmd",
        f"pg_isready -U {identity.database_user} -d {identity.database_name}",
        "--health-interval",
        "2s",
        "--health-timeout",
        "2s",
        "--health-retries",
        "30",
        _POSTGRES_IMAGE,
    ]


def _worker_run_argv(identity: _TestIdentity, image: str) -> list[str]:
    """生成只读、无挂载且仅接入本轮内部网络的测试容器命令。

    参数：identity 为本次随机测试身份；image 必须是本仓库测试镜像的完整 SHA 标签。
    返回值：包含显式测试数据库变量的 Docker run 参数。
    异常：镜像引用不符合测试镜像白名单时抛出安全错误。
    副作用：无，不启动容器。
    """
    if re.fullmatch(r"crm-tool-pr83-test:[0-9a-f]{40}", image) is None:
        raise T15DockerSafetyError(
            "image", "TEST_IMAGE_NOT_PINNED", "测试镜像必须使用本仓库的完整 SHA 标签"
        )
    environment = (
        ("APP_ENV", "test"),
        ("CRM_ADAPTER", "unconfigured"),
        ("LLM_PROVIDER", "mock"),
        ("MEDIA_STORAGE_PROVIDER", "fake"),
        ("SMART_TABLE_ADAPTER", "mock"),
        ("TEST_DATABASE_ID", identity.run_id),
        ("TEST_DATABASE_USER", identity.database_user),
        ("TEST_DATABASE_URL", identity.database_url),
        ("DATABASE_URL", identity.database_url),
        ("TYC_PROVIDER", "unconfigured"),
    )
    arguments = [
        "docker",
        "container",
        "run",
        "--pull=never",
        "--rm",
        "--name",
        identity.worker_container_name,
        "--network",
        identity.network_name,
        "--label",
        f"{_LABEL_KEY}={identity.run_id}",
        "--read-only",
        "--tmpfs",
        _WORKER_TMPFS,
    ]
    for key, value in environment:
        arguments.extend(("--env", f"{key}={value}"))
    arguments.extend((image, "python", "scripts/t15_postgres_test_worker.py"))
    return arguments


def _unit_test_run_argv(identity: _TestIdentity, image: str) -> list[str]:
    """生成无网络、无挂载的只读单元测试容器命令。

    参数：identity 为本次随机测试身份；image 必须是本仓库测试镜像的完整 SHA 标签。
    返回值：仅运行 tests/unit 的 Docker CLI 参数。
    异常：镜像标签不符合白名单时抛出安全错误。
    副作用：无，不启动容器。
    """
    _validate_test_image(image)
    return [
        "docker",
        "container",
        "run",
        "--pull=never",
        "--rm",
        "--name",
        identity.worker_container_name,
        "--network",
        "none",
        "--label",
        f"{_LABEL_KEY}={identity.run_id}",
        "--read-only",
        "--tmpfs",
        _WORKER_TMPFS,
        image,
        "python",
        "-m",
        "pytest",
        "-p",
        "no:cacheprovider",
        "tests/unit",
    ]


def _validate_network_inspect(
    network: dict[str, Any], identity: _TestIdentity
) -> None:
    """确认网络是本轮创建的内部 bridge 且未被替换。

    参数：network 为 Docker network inspect 的单个 JSON 对象；identity 为本轮身份。
    返回值：安全时无。
    异常：名称、driver、internal 标记或标签不符时抛出安全错误。
    副作用：无。
    """
    if (
        network.get("Name") != identity.network_name
        or network.get("Driver") != "bridge"
        or network.get("Internal") is not True
        or network.get("Labels", {}).get(_LABEL_KEY) != identity.run_id
    ):
        raise T15DockerSafetyError(
            "network_safety", "TEST_NETWORK_UNSAFE", "测试网络身份或 internal 属性不符合要求"
        )


def _validate_worker_inspect(
    container: dict[str, Any],
    identity: _TestIdentity,
    image: str,
    network_name: str,
) -> None:
    """确认待清理 Worker 确属本次运行且无宿主挂载或发布端口。

    参数：container 为 inspect 对象；其余参数为本轮随机身份和预期配置。
    返回值：身份和挂载均安全时无。
    异常：容器名称、镜像、标签、网络或挂载不符时抛出安全错误。
    副作用：无，不停止或删除容器。
    """
    host_config = container.get("HostConfig") or {}
    mounts = container.get("Mounts") or []
    tmpfs = host_config.get("Tmpfs") or {}
    config = container.get("Config") or {}
    if (
        container.get("Name") != f"/{identity.worker_container_name}"
        or config.get("Image") != image
        or (config.get("Labels") or {}).get(_LABEL_KEY) != identity.run_id
        or config.get("Volumes")
        or host_config.get("NetworkMode") != network_name
        or host_config.get("Binds")
        or host_config.get("VolumesFrom")
        or host_config.get("PortBindings")
        or set(tmpfs) != {"/tmp"}
        or len(mounts) > 1
        or any(
            mount.get("Type") != "tmpfs" or mount.get("Destination") != "/tmp"
            for mount in mounts
        )
    ):
        raise T15DockerSafetyError(
            "worker_cleanup_identity",
            "WORKER_CONTAINER_IDENTITY_MISMATCH",
            "Worker 清理目标身份或挂载未通过安全校验",
        )


def _cleanup_worker(
    identity: _TestIdentity,
    image: str,
    network_name: str,
    environment: dict[str, str],
) -> None:
    """只停止并删除本次身份验证通过的 Worker，随后确认资源已消失。

    参数：identity、image、network_name 与 environment 均来自本次执行。
    返回值：Worker 已不存在时无。
    异常：无法验证、停止或确认清理时抛出脱敏安全错误。
    副作用：可能停止并删除唯一匹配本次随机名称、标签和镜像的 Worker。
    """
    inspect_argv = ["docker", "container", "inspect", identity.worker_container_name]
    initial = _run_docker(
        inspect_argv,
        environment,
        "worker_cleanup_inspect",
        not_found_name=identity.worker_container_name,
    )
    # 正常 --rm 完成的 Worker 已不存在；仅将精确的 Docker not-found 响应视为已清理。
    if not initial:
        return
    container = _inspect_json(initial, "worker_cleanup_inspect")
    _validate_worker_inspect(container, identity, image, network_name)
    if (container.get("State") or {}).get("Running") is True:
        try:
            _run_docker(
                [
                    "docker",
                    "container",
                    "stop",
                    "--time",
                    "10",
                    identity.worker_container_name,
                ],
                environment,
                "worker_cleanup_stop",
                timeout=_DOCKER_CHECK_TIMEOUT_SECONDS,
            )
        except T15DockerSafetyError:
            # stop 客户端超时不代表容器已停止；后续只对已验证身份的名称执行 rm --force。
            pass
    try:
        _run_docker(
            ["docker", "container", "rm", "--force", identity.worker_container_name],
            environment,
            "worker_cleanup_remove",
            timeout=_DOCKER_CHECK_TIMEOUT_SECONDS,
        )
    except T15DockerSafetyError:
        # rm 超时后需 inspect 证实消失；不能把 CLI 的退出状态当成清理完成。
        pass
    remaining = _run_docker(
        inspect_argv,
        environment,
        "worker_cleanup_verify",
        not_found_name=identity.worker_container_name,
    )
    if remaining:
        still_present = _inspect_json(remaining, "worker_cleanup_verify")
        _validate_worker_inspect(still_present, identity, image, network_name)
        raise T15DockerSafetyError(
            "worker_cleanup_verify",
            "WORKER_CLEANUP_UNCONFIRMED",
            "本次 Worker 清理后仍可见，不能确认其已停止",
        )


def _cleanup_postgres(identity: _TestIdentity, environment: dict[str, str]) -> None:
    """只删除随机身份标签与名称均匹配的本次 PostgreSQL 容器。

    参数：identity 和 environment 来自本次执行。
    返回值：目标容器已不存在时无。
    异常：目标身份不符或删除后仍可见时抛出安全错误。
    副作用：删除本次容器，不删除任何 Docker volume。
    """
    inspect_argv = ["docker", "container", "inspect", identity.postgres_container_name]
    output = _run_docker(
        inspect_argv,
        environment,
        "postgres_cleanup_inspect",
        not_found_name=identity.postgres_container_name,
    )
    if not output:
        return
    container = _inspect_json(output, "postgres_cleanup_inspect")
    if (
        container.get("Name") != f"/{identity.postgres_container_name}"
        or (container.get("Config") or {}).get("Image") != _POSTGRES_IMAGE
        or ((container.get("Config") or {}).get("Labels") or {}).get(_LABEL_KEY)
        != identity.run_id
    ):
        raise T15DockerSafetyError(
            "postgres_cleanup_identity",
            "POSTGRES_CLEANUP_IDENTITY_MISMATCH",
            "PostgreSQL 清理目标不属于本次测试",
        )
    try:
        _run_docker(
            ["docker", "container", "rm", "--force", identity.postgres_container_name],
            environment,
            "postgres_cleanup_remove",
            timeout=_DOCKER_CHECK_TIMEOUT_SECONDS,
        )
    except T15DockerSafetyError:
        # CLI 超时或报错后仍通过精确名称检查最终状态，不推断容器已停止。
        pass
    remaining = _run_docker(
        inspect_argv,
        environment,
        "postgres_cleanup_verify",
        not_found_name=identity.postgres_container_name,
    )
    if remaining:
        raise T15DockerSafetyError(
            "postgres_cleanup_verify",
            "POSTGRES_CLEANUP_UNCONFIRMED",
            "本次 PostgreSQL 容器清理后仍可见",
        )


def _cleanup_network(identity: _TestIdentity, environment: dict[str, str]) -> None:
    """只删除本轮随机标签和 internal 属性验证通过的测试网络。

    参数：identity 和 environment 来自本次执行。
    返回值：目标网络已不存在时无。
    异常：网络身份不符或删除后仍可见时抛出安全错误。
    副作用：删除本次创建的唯一测试网络。
    """
    inspect_argv = ["docker", "network", "inspect", identity.network_name]
    output = _run_docker(
        inspect_argv,
        environment,
        "network_cleanup_inspect",
        not_found_name=identity.network_name,
    )
    if not output:
        return
    _validate_network_inspect(_inspect_json(output, "network_cleanup_inspect"), identity)
    try:
        _run_docker(
            ["docker", "network", "rm", identity.network_name],
            environment,
            "network_cleanup_remove",
            timeout=_DOCKER_CHECK_TIMEOUT_SECONDS,
        )
    except T15DockerSafetyError:
        # 删除命令状态不明时先 inspect；只有确认为消失才算清理完成。
        pass
    remaining = _run_docker(
        inspect_argv,
        environment,
        "network_cleanup_verify",
        not_found_name=identity.network_name,
    )
    if remaining:
        raise T15DockerSafetyError(
            "network_cleanup_verify",
            "NETWORK_CLEANUP_UNCONFIRMED",
            "本次测试网络清理后仍可见",
        )


def _validate_postgres_inspect(
    container: dict[str, Any], identity: _TestIdentity
) -> None:
    """验证 PostgreSQL 实例身份、网络和实际 Mounts 白名单。

    参数：container 为 Docker inspect 的单个对象；identity 为本轮随机身份。
    必须由 Mounts 明确证明目标 tmpfs，不提供跳过该检查的参数。
    返回值：所有检查通过时无。
    异常：存在 bind/volume、端口发布、身份错误或缺少 tmpfs 时抛出安全错误。
    副作用：无，不连接数据库。
    """
    config = container.get("Config") or {}
    host_config = container.get("HostConfig") or {}
    env_values = set(config.get("Env") or [])
    expected_env = {
        f"POSTGRES_USER={identity.database_user}",
        f"POSTGRES_PASSWORD={identity.database_password}",
        f"POSTGRES_DB={identity.database_name}",
        "PGDATA=/var/lib/postgresql/data/pgdata",
    }
    tmpfs = host_config.get("Tmpfs") or {}
    mounts = container.get("Mounts") or []
    # 官方镜像 Config.Volumes 只是声明；实际挂载必须由 HostConfig.Tmpfs 覆盖且 Mounts 证明。
    configured_mounts = host_config.get("Mounts") or []
    mount_is_safe = (
        len(mounts) == 1
        and mounts[0].get("Type") == "tmpfs"
        and mounts[0].get("Destination") == "/var/lib/postgresql/data"
    )
    if (
        container.get("Name") != f"/{identity.postgres_container_name}"
        or config.get("Image") != _POSTGRES_IMAGE
        or (config.get("Labels") or {}).get(_LABEL_KEY) != identity.run_id
        or host_config.get("NetworkMode") != identity.network_name
        or host_config.get("Binds")
        or host_config.get("VolumesFrom")
        or configured_mounts
        or host_config.get("PortBindings")
        or set(tmpfs) != {"/var/lib/postgresql/data"}
        or not expected_env <= env_values
        or not mount_is_safe
        or any(mount.get("Type") != "tmpfs" for mount in mounts)
    ):
        raise T15DockerSafetyError(
            "postgres_mount_safety",
            "POSTGRES_CONTAINER_UNSAFE",
            "PostgreSQL 容器挂载、端口或随机身份未通过安全校验",
        )
    ports = (container.get("NetworkSettings") or {}).get("Ports") or {}
    if any(bindings for bindings in ports.values()):
        raise T15DockerSafetyError(
            "postgres_mount_safety",
            "POSTGRES_PORT_PUBLISHED",
            "PostgreSQL 容器存在宿主端口映射",
        )


def _docker_environment() -> dict[str, str]:
    """给 Docker CLI 提供最小环境并拒绝 endpoint 覆盖变量。

    参数：无。
    返回值：保留 CLI 启动和当前 Windows 用户配置所需的环境变量。
    异常：存在可能覆盖 context 的 Docker 环境变量时抛出安全错误。
    副作用：只读取当前进程环境，不复制数据库、适配器或生产凭据。
    """
    if _REJECTED_DOCKER_OVERRIDES & os.environ.keys():
        raise T15DockerSafetyError(
            "docker_environment",
            "DOCKER_ENDPOINT_OVERRIDE_PRESENT",
            "检测到 Docker endpoint 覆盖变量，拒绝继续",
        )
    allowed = (
        "PATH",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
        "TEMP",
        "TMP",
        "USERPROFILE",
        "HOMEDRIVE",
        "HOMEPATH",
        "APPDATA",
        "LOCALAPPDATA",
    )
    return {key: os.environ[key] for key in allowed if key in os.environ}


def _run_docker(
    arguments: list[str],
    environment: dict[str, str],
    stage: str,
    *,
    timeout: int = _DOCKER_CHECK_TIMEOUT_SECONDS,
    not_found_name: str | None = None,
) -> str:
    """执行单条预先构造的 Docker CLI 命令并隐藏原始输出。

    参数：arguments 为直接 Docker 参数；environment 为最小环境；stage 为固定阶段名；
    timeout 为本次命令超时秒数；not_found_name 仅允许精确识别指定资源不存在。
    返回值：成功时返回 stdout，调用方只解析 JSON 或固定状态字段。
    异常：启动失败、超时或非零退出时抛出不含原始输出的安全错误。
    副作用：根据 arguments 执行 Docker CLI；只允许本脚本的白名单命令。
    """
    _validate_direct_docker_argv(arguments)
    try:
        completed = subprocess.run(
            arguments,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=environment,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise T15DockerSafetyError(stage, "DOCKER_COMMAND_TIMEOUT", "Docker 命令超时") from None
    except OSError as error:
        raise T15DockerSafetyError(
            stage, "DOCKER_COMMAND_UNAVAILABLE", type(error).__name__
        ) from None
    if completed.returncode != 0:
        missing_responses = {
            f"Error: No such object: {not_found_name}",
            f"Error: No such container: {not_found_name}",
            f"Error response from daemon: No such object: {not_found_name}",
            f"Error response from daemon: No such container: {not_found_name}",
            f"Error: No such network: {not_found_name}",
            f"Error response from daemon: network {not_found_name} not found",
        }
        if (
            not_found_name is not None
            and str(completed.stderr or "").strip() in missing_responses
        ):
            return ""
        raise T15DockerSafetyError(stage, "DOCKER_COMMAND_FAILED", "Docker 命令未成功")
    return completed.stdout


def _validate_direct_docker_argv(arguments: list[str]) -> None:
    """拒绝 Compose、危险挂载、端口发布及全局清理命令。

    参数：arguments 为即将交给 subprocess 的完整 Docker 参数。
    返回值：命令属于直接 Docker 白名单且没有危险参数时无。
    异常：命令前缀、子命令或参数不符合白名单时抛出安全错误。
    副作用：无。
    """
    allowed_operations = {
        ("context", "inspect"),
        ("network", "create"),
        ("network", "inspect"),
        ("network", "rm"),
        ("container", "create"),
        ("container", "inspect"),
        ("container", "start"),
        ("container", "run"),
        ("container", "rm"),
        ("container", "stop"),
    }
    forbidden_flags = (
        "-v", "--volume", "--mount", "-p", "--publish", "--volumes-from",
        "--privileged", "--device", "--cap-add", "--pid", "--ipc", "--net",
        "--userns", "--uts", "--cgroupns", "--security-opt", "--add-host",
        "--sysctl",
    )
    operation = tuple(arguments[1:3])
    # run/create 的 image 之后属于容器命令；例如 pytest 的 -p 不是 Docker 端口参数。
    option_end = len(arguments)
    if operation in {("container", "run"), ("container", "create")}:
        image_index = next(
            (
                index
                for index, argument in enumerate(arguments[3:], start=3)
                if argument == _POSTGRES_IMAGE
                or re.fullmatch(r"crm-tool-pr83-test:[0-9a-f]{40}", argument)
            ),
            None,
        )
        if image_index is None:
            raise T15DockerSafetyError(
                "docker_command_safety",
                "DOCKER_COMMAND_NOT_ALLOWED",
                "Docker 容器命令缺少固定测试镜像",
            )
        option_end = image_index
    docker_options = arguments[3:option_end]
    if (
        not arguments
        or arguments[0] != "docker"
        or "compose" in arguments
        or operation not in allowed_operations
        or any(
            argument == flag
            or argument.startswith(f"{flag}=")
            or (
                flag in ("-v", "-p")
                and argument.startswith(flag)
                and not argument.startswith("--")
                and len(argument) > 2
            )
            for argument in docker_options
            for flag in forbidden_flags
        )
        or "--network=host" in docker_options
        or any(
            docker_options[index : index + 2] == ["--network", "host"]
            for index in range(len(docker_options) - 1)
        )
    ):
        raise T15DockerSafetyError(
            "docker_command_safety",
            "DOCKER_COMMAND_NOT_ALLOWED",
            "Docker 命令包含白名单外操作或挂载参数",
        )


def _inspect_json(output: str, stage: str) -> dict[str, Any]:
    """解析单个 inspect JSON 对象而不回显原始内容。

    参数：output 为 Docker inspect stdout；stage 为固定安全阶段。
    返回值：单个 JSON 对象。
    异常：JSON 结构无效或不是单对象时抛出脱敏错误。
    副作用：无。
    """
    try:
        value = json.loads(output)
    except (TypeError, json.JSONDecodeError):
        raise T15DockerSafetyError(
            stage, "DOCKER_INSPECT_INVALID", "Docker inspect JSON 无效"
        ) from None
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise T15DockerSafetyError(stage, "DOCKER_INSPECT_INVALID", "Docker inspect 结构不符合要求")
    return value[0]


def _validate_test_image(image: str) -> None:
    """限制测试镜像为 PR 测试仓库名和 40 位提交 SHA 标签。

    参数：image 为调用方传入的本地测试镜像引用。
    返回值：引用符合白名单时无。
    异常：引用可能覆盖业务镜像或未固定 SHA 时抛出安全错误。
    副作用：无。
    """
    if re.fullmatch(r"crm-tool-pr83-test:[0-9a-f]{40}", image) is None:
        raise T15DockerSafetyError(
            "image", "TEST_IMAGE_NOT_PINNED", "测试镜像必须使用本仓库的完整 SHA 标签"
        )


def _emit(status: str, stage: str, error_code: str, exception_type: str | None) -> int:
    """只输出固定字段的脱敏 JSON 结果。

    参数：status、stage、error_code 和 exception_type 均为非敏感状态字段。
    返回值：成功为 0，失败为 1，显式执行参数缺失为 2。
    异常：无。
    副作用：向 stdout 写入一行 JSON，不记录 URL、密码或原始命令结果。
    """
    print(
        json.dumps(
            {
                "status": status,
                "stage": stage,
                "error_code": error_code,
                "exception_type": exception_type,
            },
            ensure_ascii=False,
        )
    )
    return 0 if status == "passed" else 2 if status == "blocked" else 1


def _run_unit_once(image: str) -> tuple[str, str, str, str | None]:
    """只在 --unit 授权下运行一次无网络单元测试容器。

    参数：image 为固定到提交 SHA 的 PR 专用测试镜像。
    返回值：status、stage、error_code、exception_type 四项脱敏结果。
    异常：Docker 或安全门错误转换为固定安全错误。
    副作用：仅启动命令中的一个无网络容器；不创建网络或 PostgreSQL。
    """
    _validate_test_image(image)
    if shutil.which("docker") is None:
        raise T15DockerSafetyError("docker_cli", "DOCKER_CLI_NOT_FOUND", "Docker CLI 不可用")
    identity = _new_test_identity()
    environment = _docker_environment()
    worker_attempted = False
    failure: T15DockerSafetyError | None = None
    try:
        endpoint = _run_docker(
            ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
            environment,
            "docker_context",
        ).strip()
        _validate_local_docker_desktop_endpoint(endpoint)
        # 先记录尝试状态，CLI 超时时仍按随机名称和 label 检查/清理可能已启动的容器。
        worker_attempted = True
        _run_docker(
            _unit_test_run_argv(identity, image),
            environment,
            "unit_tests",
            timeout=_TEST_WORKER_TIMEOUT_SECONDS,
        )
    except T15DockerSafetyError as error:
        failure = error
    finally:
        if worker_attempted:
            try:
                _cleanup_worker(identity, image, "none", environment)
            except T15DockerSafetyError as error:
                # 无法证明本轮 Worker 已停止时以清理状态为最终诊断。
                failure = error
    if failure is not None:
        return "failed", failure.stage, failure.error_code, type(failure).__name__
    return "passed", "unit_tests", "OK", None


def _run_once(image: str) -> tuple[str, str, str, str | None]:
    """创建并清理一套随机内部 PostgreSQL 测试资源。

    参数：image 为已构建且固定到提交 SHA 的测试镜像。
    返回值：status、stage、error_code、exception_type 四项脱敏结果。
    异常：Docker/安全门失败转换为固定 T15DockerSafetyError。
    副作用：显式执行时创建临时内部网络与 PostgreSQL 容器，运行后只清理本次资源。
    """
    _validate_test_image(image)
    if shutil.which("docker") is None:
        raise T15DockerSafetyError("docker_cli", "DOCKER_CLI_NOT_FOUND", "Docker CLI 不可用")
    identity = _new_test_identity()
    environment = _docker_environment()
    network_may_exist = False
    postgres_may_exist = False
    worker_attempted = False
    failure: T15DockerSafetyError | None = None
    try:
        endpoint = _run_docker(
            ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
            environment,
            "docker_context",
        ).strip()
        _validate_local_docker_desktop_endpoint(endpoint)
        # 在发出可能超时的 create 前标记清理目标；清理仍需按名称与 label 实际核验。
        network_may_exist = True
        _run_docker(_network_create_argv(identity), environment, "network_create")
        network = _inspect_json(
            _run_docker(
                ["docker", "network", "inspect", identity.network_name],
                environment,
                "network_inspect",
            ),
            "network_safety",
        )
        _validate_network_inspect(network, identity)
        postgres_may_exist = True
        _run_docker(_postgres_create_argv(identity), environment, "postgres_create")
        created_container = _inspect_json(
            _run_docker(
                ["docker", "container", "inspect", identity.postgres_container_name],
                environment,
                "postgres_inspect",
            ),
            "postgres_mount_safety",
        )
        # 创建后、启动数据库前先检查 Mounts，卷或宿主机路径出现时不会启动。
        _validate_postgres_inspect(created_container, identity)
        _run_docker(
            ["docker", "container", "start", identity.postgres_container_name],
            environment,
            "postgres_start",
        )
        for _ in range(30):
            health = _run_docker(
                [
                    "docker",
                    "container",
                    "inspect",
                    "--format",
                    "{{.State.Health.Status}}",
                    identity.postgres_container_name,
                ],
                environment,
                "postgres_health",
            ).strip()
            if health == "healthy":
                break
            if health == "unhealthy":
                raise T15DockerSafetyError(
                    "postgres_health", "POSTGRES_NOT_READY", "一次性 PostgreSQL 未通过健康检查"
                )
            time.sleep(1)
        else:
            raise T15DockerSafetyError(
                "postgres_health", "POSTGRES_NOT_READY", "一次性 PostgreSQL 未在限定时间内就绪"
            )
        running_container = _inspect_json(
            _run_docker(
                ["docker", "container", "inspect", identity.postgres_container_name],
                environment,
                "postgres_inspect",
            ),
            "postgres_mount_safety",
        )
        _validate_postgres_inspect(running_container, identity)
        # Worker CLI 超时时仍可能有容器存活；先记录启动尝试供 finally 定向清理。
        worker_attempted = True
        _run_docker(
            _worker_run_argv(identity, image),
            environment,
            "integration_worker",
            timeout=_TEST_WORKER_TIMEOUT_SECONDS,
        )
    except T15DockerSafetyError as error:
        failure = error
    except Exception as error:
        failure = T15DockerSafetyError(
            "postgres_test", "POSTGRES_TEST_UNEXPECTED", type(error).__name__
        )
    finally:
        # 超时或 CLI 失败时，先验证 Worker 标签、名称、镜像、网络和挂载，再定向清理。
        worker_cleanup_failed = False
        if worker_attempted:
            try:
                _cleanup_worker(identity, image, identity.network_name, environment)
            except T15DockerSafetyError as error:
                failure = error
                worker_cleanup_failed = True
        # 若 Worker 清理状态未知，保留其所依赖的数据库和网络，避免误拆运行中的任务。
        if worker_cleanup_failed:
            postgres_may_exist = False
            network_may_exist = False
        # 清理前重新校验资源身份；删除 PostgreSQL 容器时不带 --volumes。
        postgres_cleanup_failed = False
        if postgres_may_exist:
            try:
                _cleanup_postgres(identity, environment)
            except T15DockerSafetyError as error:
                failure = error
                postgres_cleanup_failed = True
        if network_may_exist and not postgres_cleanup_failed:
            try:
                _cleanup_network(identity, environment)
            except T15DockerSafetyError as error:
                failure = error
    if failure is not None:
        return "failed", failure.stage, failure.error_code, type(failure).__name__
    return "passed", "postgres_migration_and_scheduler", "OK", None


def main(argv: list[str] | None = None) -> int:
    """默认拒绝运行；--unit 与 --postgres 分别授权单元或 PostgreSQL 测试。

    参数：argv 为 CLI 参数；测试可显式传入列表，不传时使用进程命令行。
    返回值：成功为 0；拒绝、失败或缺少执行授权参数时为非零。
    异常：运行错误均转换为脱敏 JSON，不输出原始 stdout/stderr。
    副作用：仅显式 --unit 或 --postgres 时分别启动单元容器或隔离数据库测试资源。
    """
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 3 or arguments[1] != "--image":
        return _emit("blocked", "arguments", "EXPLICIT_TEST_MODE_REQUIRED", None)
    mode, image = arguments[0], arguments[2]
    try:
        if mode == "--unit":
            status, stage, error_code, exception_type = _run_unit_once(image)
        elif mode == "--postgres":
            status, stage, error_code, exception_type = _run_once(image)
        else:
            return _emit("blocked", "arguments", "EXPLICIT_TEST_MODE_REQUIRED", None)
    except T15DockerSafetyError as error:
        return _emit("failed", error.stage, error.error_code, type(error).__name__)
    return _emit(
        status,
        stage,
        error_code,
        exception_type,
    )


if __name__ == "__main__":
    raise SystemExit(main())
