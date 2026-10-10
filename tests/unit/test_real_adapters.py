"""真实外部 Adapter 的离线契约测试。"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

import app.crm.sop as crm_sop
from app.companies.service import TYCAdapterError
from app.companies.tyc import TianYanChaAdapter
from app.core.config import Settings
from app.core.provider_policy import ProviderPolicy
from app.crm.employee_directory import EmployeeDirectory, EmployeeDirectoryError
from app.crm.sop import (
    SopCRMAdapter,
    SopCRMError,
    _classify_duplicate_entity_message,
    _duplicate_entity_type_for_digest,
)


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


def test_tyc_accepts_numeric_company_type_from_real_search_envelope() -> None:
    """验证天眼查真实搜索响应的数字 type=1 能解析为企业候选。"""
    adapter = _tyc({"error_code": 0, "reason": "ok", "result": {"total": 2, "items": [
        {
            "name": "希捷国际科技（无锡）有限公司",
            "id": 2343827108,
            "type": 1,
        },
        {
            "name": "希捷国际科技（无锡）有限公司工会委员会",
            "id": 4470657997,
            "type": 1,
        },
    ]}})

    result = adapter.lookup("希捷国际科技（无锡）有限公司")

    assert result.status == "matched"
    assert len(result.candidates) == 1
    assert result.candidates[0].standard_company_name == "希捷国际科技（无锡）有限公司"


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
    response: object, callback: object | None = None, authorization: str | None = None,
    *, status: int = 200,
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
        """记录假请求并返回指定 HTTP 状态；参数为请求，返回假响应，不访问网络。"""
        requests.append(request)
        if callback:
            callback(request, key)
        return httpx.Response(status, json=response, request=request)

    adapter = SopCRMAdapter(
        "https://crm.invalid/gateway",
        "fake-app",
        "fake-token",
        pem,
        client=httpx.Client(transport=httpx.MockTransport(send)),
        authorization=authorization,
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


@pytest.mark.parametrize(
    ("action", "lead_id"),
    [("CREATE", "123"), ("FOLLOW_UP", None)],
)
def test_sop_unified_submit_sends_stable_request_id_and_maps_actions(
    action: str, lead_id: str | None,
) -> None:
    """验证统一接口字段、重试稳定 requestId 及两种成功动作的映射。"""
    posted: list[dict[str, str]] = []

    def record_request(request: httpx.Request, _key: rsa.RSAPrivateKey) -> None:
        """记录 SOP 表单，便于断言统一 method 与 JSON 业务参数。"""
        form = parse_qs(request.content.decode())
        posted.append({name: values[0] for name, values in form.items()})

    data = {"action": action}
    if lead_id is not None:
        data["leadId"] = lead_id
    adapter, _ = _crm({"code": 0, "data": data}, record_request)
    payload = {"name": "样例公司", "mobile": "13800000000"}

    first = adapter.submit_lead(payload, idempotency_key="lead-key", crm_user_id="crm-user")
    second = adapter.submit_lead(payload, idempotency_key="lead-key", crm_user_id="crm-user")

    assert first.crm_lead_id == second.crm_lead_id == lead_id
    assert len(posted) == 2
    bodies = [json.loads(item["biz_content"]) for item in posted]
    assert all(item["method"] == "crm_submit_lead" for item in posted)
    assert bodies[0] == bodies[1]
    assert bodies[0]["name"] == "样例公司"
    assert bodies[0]["mobile"] == "13800000000"
    assert bodies[0]["headerId"] == "crm-user"
    assert bodies[0]["requestId"] == hashlib.sha256(b"lead-key").hexdigest()


def test_sop_authorization_header_is_sent_when_configured() -> None:
    """验证配置的 SOP Authorization 仅作为请求头发送。"""
    adapter, requests = _crm({"code": 0, "data": {"result": 0}}, authorization="Bearer fake")
    assert adapter.search_by_company_name({"name": "样例", "businessLine": 1}) == ()
    assert requests[0].headers["Authorization"] == "Bearer fake"


def test_employee_directory_resolves_unique_name_and_nickname(tmp_path: Path) -> None:
    """验证姓名和昵称唯一精确匹配到 employee.id。"""
    path = tmp_path / "employee.csv"
    path.write_text("id,name,nickname\nE1,张三,三哥\nE2,李四,小李\n", encoding="utf-8")
    directory = EmployeeDirectory(path)
    assert directory.resolve("张三") == "E1"
    assert directory.resolve("小李") == "E2"


def test_employee_directory_resolves_wecom_combined_name_and_nickname(
    tmp_path: Path,
) -> None:
    """验证企微姓名（花名）组合仍要求两部分精确匹配。"""
    path = tmp_path / "employee.csv"
    path.write_text("id,name,nickname\nE1,张华杰,JJ\n", encoding="utf-8")
    directory = EmployeeDirectory(path)

    assert directory.resolve("张华杰(JJ)") == "E1"
    assert directory.resolve("张华杰（其他花名）") == "E1"


def test_employee_directory_uses_nickname_only_to_disambiguate_duplicate_names(
    tmp_path: Path,
) -> None:
    """验证姓名唯一时不校验花名，姓名重复时才用花名消歧。"""
    path = tmp_path / "employee.csv"
    path.write_text("id,name,nickname\nE1,张三,三哥\nE2,张三,小张\n", encoding="utf-8")
    directory = EmployeeDirectory(path)

    assert directory.resolve("张三(小张)") == "E2"


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
    adapter, _ = _crm(
        {
            "code": 0,
            "data": {
                "result": 100,
                "leadId": None,
                "customerId": None,
                "message": "PRIVATE SOP RESPONSE",
            },
        }
    )
    with pytest.raises(SopCRMError) as error:
        adapter.search_by_company_name({"name": "样例", "businessLine": 2})
    assert error.value.category == "duplicate_target_unavailable"
    assert error.value.sub_code == "duplicate_detected_without_lead_id"
    assert error.value.duplicate_entity_type == "unknown"
    assert "PRIVATE SOP RESPONSE" not in str(error.value)


def test_sop_duplicate_entity_allowlist_uses_exact_message_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 SOP 精确文案哈希映射，并将未登记消息归为 unknown。"""
    assert _duplicate_entity_type_for_digest(
        "3e767f7522343ef3d9328eafd549855a6e1aa55829509bf29dbe75a6e4686da6b8"
    ) == "lead"
    assert _duplicate_entity_type_for_digest("0" * 64) == "unknown"

    customer_message = "synthetic customer fixture"
    customer_digest = hashlib.sha256(customer_message.encode("utf-8")).hexdigest()
    monkeypatch.setitem(crm_sop._DUPLICATE_ENTITY_MESSAGE_HASHES, customer_digest, "customer")
    assert crm_sop._classify_duplicate_entity_message(customer_message) == "customer"
    assert _classify_duplicate_entity_message("unlisted SOP message") == "unknown"


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


@pytest.mark.parametrize("success", [True, False])
def test_sop_update_boolean_response(success: bool) -> None:
    """验证 SOP 修改接口的真实 Boolean 契约；拒绝不会被记为成功，无外部调用。"""
    adapter, _ = _crm({"code": 200, "data": success})
    if success:
        assert (
            adapter.update_lead("99", {}, idempotency_key="x", crm_user_id="u").crm_lead_id == "99"
        )
    else:
        with pytest.raises(SopCRMError) as error:
            adapter.update_lead("99", {}, idempotency_key="x", crm_user_id="u")
        assert error.value.category == "business_rejection"


@pytest.mark.parametrize(
    "data",
    [None, "", 1, {}, {"success": "true"}],
)
def test_sop_update_unknown_response_stays_unknown(data: object) -> None:
    """验证缺失或畸形覆盖响应不能证明成功或拒绝；参数为假响应，异常由断言检查。"""
    adapter, _ = _crm({"code": 200, "data": data})
    with pytest.raises(SopCRMError) as error:
        adapter.update_lead("99", {}, idempotency_key="x", crm_user_id="u")
    assert error.value.category == "malformed_response"


@pytest.mark.parametrize("status", [401, 403, 500])
def test_sop_http_failure_cannot_masquerade_as_update_success(status: int) -> None:
    """验证 HTTP 错误与成功 JSON 冲突时保留错误事实；仅使用假 transport，无外部调用。"""
    adapter, _ = _crm({"code": 200, "data": True}, status=status)
    with pytest.raises(SopCRMError) as error:
        adapter.update_lead("99", {}, idempotency_key="x", crm_user_id="u")
    assert error.value.http_status == status


@pytest.mark.parametrize(
    ("error_code", "category"),
    [
        ("isv.invalid-signature", "authentication"),
        ("aop.invalid-app-auth-token", "authentication"),
        ("isv.route-no-permissions", "gateway"),
        ("crm.field-validation-failed", "business"),
    ],
)
def test_sop_error_envelopes_are_classified_without_transport_confusion(
    error_code: str, category: str
) -> None:
    """验证 HTTP 200 的 SOP 错误 envelope 按协议类别分类。"""
    adapter, _ = _crm({"code": "400", "sub_code": error_code, "sub_msg": "redacted"})
    with pytest.raises(SopCRMError) as error:
        adapter.search_by_company_name({"name": "样例", "businessLine": 1})
    assert error.value.category == category
    assert error.value.sub_code == error_code


def test_sop_alternate_gateway_envelope_is_gateway() -> None:
    """验证 SOP 的 code/data/status/message 错误 envelope 不会误报 malformed。"""
    adapter, _ = _crm({"code": -1, "data": None, "message": "redacted", "status": "error"})
    with pytest.raises(SopCRMError) as error:
        adapter.search_by_company_name({"name": "样例", "businessLine": 1})
    assert error.value.category == "gateway"


def test_sop_malformed_json_and_unexpected_shape_are_not_transport() -> None:
    """验证 HTTP 200 的非法 JSON 或缺少 code 进入 malformed_response。"""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    for response in (
        httpx.Response(200, text="not-json"),
        httpx.Response(200, json={"message": "missing code"}),
    ):
        adapter = SopCRMAdapter(
            "https://crm.invalid", "app", "token", pem,
            client=httpx.Client(transport=httpx.MockTransport(lambda _request, r=response: r)),
        )
        with pytest.raises(SopCRMError, match="CRM") as error:
            adapter.search_by_company_name({"name": "样例", "businessLine": 1})
        assert error.value.category == "malformed_response"


def test_sop_connect_error_is_transport() -> None:
    """验证真正的连接异常仍分类为 transport。"""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    transport = httpx.MockTransport(lambda _request: (_ for _ in ()).throw(httpx.ConnectError("x")))
    adapter = SopCRMAdapter(
        "https://crm.invalid", "app", "token", pem,
        client=httpx.Client(transport=transport),
    )
    with pytest.raises(SopCRMError) as error:
        adapter.search_by_company_name({"name": "样例", "businessLine": 1})
    assert error.value.category == "transport"


@pytest.mark.parametrize(
    ("status", "response", "category"),
    [
        (401, {"code": "401", "sub_code": "isv.invalid-signature"}, "authentication"),
        (403, {"code": "403", "sub_code": "isv.route-no-permissions"}, "gateway"),
        (500, {"code": "500", "sub_code": "isv.gateway-error"}, "gateway"),
    ],
)
def test_sop_http_error_statuses_are_not_transport(
    status: int, response: object, category: str
) -> None:
    """验证收到 HTTP 响应后按 status/envelope 分类，不误报 transport。"""
    adapter, _ = _crm(response)
    # _crm 固定 200，因此用独立 fake transport 覆盖状态码。
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    adapter = SopCRMAdapter(
        "https://crm.invalid", "app", "token", pem,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda req: httpx.Response(status, json=response, request=req)
            )
        ),
    )
    with pytest.raises(SopCRMError) as error:
        adapter.search_by_company_name({"name": "样例", "businessLine": 1})
    assert error.value.category == category
    assert error.value.http_status == status


def test_tianyancha_uppercase_environment_names_are_loaded() -> None:
    """验证正式 TYC 环境变量名为 TIANYANCHA_API_KEY/TIANYANCHA_URL。"""
    settings = Settings(
        _env_file=None,
        TIANYANCHA_API_KEY="key",  # type: ignore[call-arg]
        TIANYANCHA_URL="https://tyc.invalid",  # type: ignore[call-arg]
    )
    assert settings.tianyancha_api_key == "key"
    assert settings.tianyancha_url == "https://tyc.invalid"


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
