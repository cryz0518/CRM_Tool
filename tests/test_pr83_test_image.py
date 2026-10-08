"""在仓库构建上下文中检查 PR83 测试镜像文件策略。"""

from pathlib import Path


def test_pr83_test_image_copies_test_modules_and_excludes_secrets() -> None:
    """验证测试镜像包含 Python 测试模块且构建上下文排除本地凭据。

    参数：无。
    返回值：无。
    异常：镜像文件清单或忽略规则不符合隔离要求时断言失败。
    副作用：只读取仓库内两个受版本管理的镜像配置文件。
    """
    repository_root = Path(__file__).parents[1]
    dockerfile = (repository_root / "Dockerfile.pr83-test").read_text(encoding="utf-8")
    ignore = (repository_root / "Dockerfile.pr83-test.dockerignore").read_text(
        encoding="utf-8"
    )
    unit_test = (repository_root / "tests/unit/test_t15_docker_postgres.py").read_text(
        encoding="utf-8"
    )

    assert "COPY app ./app" in dockerfile
    assert "COPY workers ./workers" in dockerfile
    assert "COPY tests ./tests" in dockerfile
    assert "COPY scripts ./scripts" in dockerfile
    assert "COPY alembic ./alembic" in dockerfile
    assert "@wecom/cli" not in dockerfile
    assert "Dockerfile.pr83-test" not in unit_test
    assert "**/.env.*" in ignore
    assert "tests/integration/ticket15-compose.env" in ignore
    assert "**/*credentials*" in ignore
    assert "**/*secret*" in ignore
