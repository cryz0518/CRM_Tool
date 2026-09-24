"""真实外部 Adapter 的离线契约测试。"""

from __future__ import annotations

import base64
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from app.companies.service import TYCAdapterError
from app.companies.tyc import TianYanChaAdapter
from app.core.config import Settings
from app.core.provider_policy import ProviderPolicy
from app.crm.employee_directory import EmployeeDirectory, EmployeeDirectoryError
from app.crm.sop import SopCRMAdapter, SopCRMError


def _tyc(response: object, *, status: int = 200) -> TianYanChaAdapter:
    """以 fake transport 构造无网络天眼查客户端。"""
    transport = httpx.MockTransport(
        lambda request: httpx.Response(status, json=response, request=request)
    )
    return TianYanChaAdapter(
        "https://tyc.invalid/search",
        "fake-key",
        client=httpx.Client(transport=transport),
    )


def test_tyc_accepts_only_one_exact_company_match() -> None:
    """验证唯一规范化精确匹配，且非企业条目被过滤。"""
    adapter = _tyc({"error_code": 0, "result": {"items": [
        {"name": "示例科技有限公司", "id": "1", "type": "company"},
        {"name": "示例科技有限公司", "id": "2", "type": "person"},
    ]}})
    result = adapter.lookup(" 示例 科技有限公司 ")
    assert result.status == "matched" and result.candidates[0].company_id == "1"


def test_tyc_not_found_is_only_documented_code_or_no_company_items() -> None:
    """验证文档无结果码和空企业结果可返回 not_found。"""
    assert _tyc({"error_code": 300000}).lookup("公司").status == "not_found"
    assert (
        _tyc({"error_code": 0, "result": {"items": [
            {"name": "人", "id": "1", "type": "person"}
        ]}}).lookup("公司").status
        == "not_found"
    )


def test_tyc_ambiguous_and_remote_errors_are_not_collapsed() -> None:
    """验证非唯一候选保留歧义，服务错误不伪装成无结果。"""
    ambiguous = _tyc({"error_code": 0, "result": {"items": [
        {"name": "甲有限公司", "id": "1"}, {"name": "乙有限公司", "id": "2"}
    ]}}).lookup("甲乙")
    assert ambiguous.status == "ambiguous" and len(ambiguous.candidates) == 2
    for code in (401, 403, 429, 500):
        with pytest.raises(TYCAdapterError):
            _tyc({"error_code": code, "reason": "fake-key"}).lookup("公司")


def test_tyc_malformed_response_and_timeout_raise_classified_error() -> None:
    """验证超时和协议异常均进入 TYCAdapterError。"""
    with pytest.raises(TYCAdapterError):
        _tyc({"error_code": 0, "result": {}}).lookup("公司")
    transport = httpx.MockTransport(lambda _request: (_ for _ in ()).throw(httpx.ReadTimeout("x")))
    adapter = TianYanChaAdapter(
        "https://tyc.invalid", "fake-key", client=httpx.Client(transport=transport)
    )
    with pytest.raises(TYCAdapterError):
        adapter.lookup("公司")


def _crm(
    response: object, callback: object | None = None
) -> tuple[SopCRMAdapter, list[httpx.Request]]:
    """以临时 RSA key 和 fake transport 构造 SOP 客户端。"""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    requests: list[httpx.Request] = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if callback:
            callback(request, key)
        return httpx.Response(200, json=response, request=request)

    adapter = SopCRMAdapter(
        "https://crm.invalid/gateway",
        "fake-app",
        "fake-token",
        pem,
        client=httpx.Client(transport=httpx.MockTransport(send)),
    )
    return adapter, requests


def test_sop_signing_matches_posted_biz_content_and_header_identity() -> None:
    """驗證 RSA2 canonical signing、form body 一致性及 headerId 來源。"""
    def verify(request: httpx.Request, key: rsa.RSAPrivateKey) -> None:
        form = parse_qs(request.content.decode())
        posted = {name: values[0] for name, values in form.items()}
        sign = base64.b64decode(posted.pop("sign"))
        canonical = "&".join(f"{name}={value}" for name, value in sorted(posted.items()) if value)
        key.public_key().verify(sign, canonical.encode(), padding.PKCS1v15(), hashes.SHA256())
        assert posted["biz_content"] == '{"name":"样例","businessLine":1,"headerId":"crm-user"}'

    adapter, requests = _crm({"code": 0, "message": "success", "data": {"id": 123}}, verify)
    result = adapter.create_lead(
        {"name": "样例", "businessLine": 1, "headerId": "spoof"},
        idempotency_key="i",
        crm_user_id="crm-user",
    )
    assert result.crm_lead_id == "123" and len(requests) == 1


def test_employee_directory_resolves_unique_name_and_nickname(tmp_path: Path) -> None:
    """验证姓名和昵称唯一精确匹配到 employee.id。"""
    path = tmp_path / "employee.csv"
    path.write_text("id,name,nickname\nE1,张三,三哥\nE2,李四,小李\n", encoding="utf-8")
    directory = EmployeeDirectory(path)
    assert directory.resolve("张三") == "E1"
    assert directory.resolve("小李") == "E2"


def test_employee_directory_rejects_duplicate_and_missing_owner(tmp_path: Path) -> None:
    """验证重复员工和不存在负责人均 fail closed。"""
    path = tmp_path / "employee.csv"
    path.write_text("id,name,nickname\nE1,张三,三哥\nE2,张三,小李\n", encoding="utf-8")
    directory = EmployeeDirectory(path)
    with pytest.raises(EmployeeDirectoryError):
        directory.resolve("张三")
    with pytest.raises(EmployeeDirectoryError):
        directory.resolve("不存在")


def test_sop_duplicate_without_lead_id_fails_closed_and_no_duplicate_is_empty() -> None:
    """驗證重複無安全目標不會被當成无重复。"""
    adapter, _ = _crm({"code": 0, "data": {"result": 0, "leadId": None}})
    assert adapter.search_by_company_name({"name": "样例", "businessLine": 2}) == ()
    adapter, _ = _crm({"code": 0, "data": {"result": 100, "leadId": None}})
    with pytest.raises(SopCRMError, match="manual review"):
        adapter.search_by_company_name({"name": "样例", "businessLine": 2})


def test_sop_duplicate_create_and_update_response_shapes() -> None:
    """驗證查重命中、create code 0 與 update code 200 成功形狀。"""
    adapter, _ = _crm({"code": 0, "data": {"result": 100, "leadId": 99}})
    assert (
        adapter.search_by_company_name({"name": "样例", "businessLine": 1})[0].crm_lead_id
        == "99"
    )
    adapter, _ = _crm({"code": 0, "data": 1001})
    assert adapter.create_lead({}, idempotency_key="x", crm_user_id="u").crm_lead_id == "1001"
    adapter, _ = _crm({"code": 200, "data": {"success": True}})
    assert adapter.update_lead("99", {}, idempotency_key="x", crm_user_id="u").crm_lead_id == "99"


def test_provider_policy_requires_real_configuration_and_forbids_mock_in_production() -> None:
    """驗證真實 provider 配置缺失和生產 mock 都 fail closed。"""
    settings = Settings(
        _env_file=None,
        app_env="production",
        crm_adapter="sop",
        tyc_provider="tianyancha",
    )
    policy = ProviderPolicy("production")
    results = {item.component: item for item in policy.evaluate_settings(settings)}
    assert results["crm"].reason_code == "provider_configuration_missing"
    assert results["tyc"].reason_code == "provider_configuration_missing"
    assert (
        policy.evaluate("crm", "mock", settings=settings).reason_code
        == "production_test_provider_forbidden"
    )
