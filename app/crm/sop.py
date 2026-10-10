"""CRM SOP Gateway RSA2 适配器。"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from app.crm.adapter import CRMCreateResult, CRMSearchResult

DuplicateEntityType = Literal["lead", "customer", "dealer", "unknown"]
# SOP 文档只提供一条线索类型示例；使用其 SHA-256 精确匹配，源文案不进入代码库。
_DUPLICATE_ENTITY_MESSAGE_HASHES: dict[str, DuplicateEntityType] = {
    "3e767f7522343ef3d9328eafd549855a6e1aa55829509bf29dbe75a6e4686da6b8": "lead",
}


def _duplicate_entity_type_for_digest(digest: str) -> DuplicateEntityType:
    """按 SOP 固定文案的哈希白名单返回重复对象类型。

    参数：digest 为 data.message 的 SHA-256 十六进制摘要。
    返回值：精确 allowlist 中的 lead/customer/dealer，未登记时返回 unknown。
    异常：无。
    副作用：无；摘要和原文均不写入持久化存储。
    """
    return _DUPLICATE_ENTITY_MESSAGE_HASHES.get(digest, "unknown")


def _classify_duplicate_entity_message(message: object) -> DuplicateEntityType:
    """只在内存中精确识别 SOP 重复对象文案，不保留或记录原文。

    参数：message 为 SOP data.message 的临时值。
    返回值：受控对象类型枚举；类型非法、过长或不在白名单时返回 unknown。
    异常：无；非字符串输入不会被编码或强制转换。
    副作用：仅对临时字符串计算摘要，不记录、返回或持久化原文。
    """
    if not isinstance(message, str) or len(message) > 1024:
        return "unknown"
    digest = hashlib.sha256(message.encode("utf-8")).hexdigest()
    return _duplicate_entity_type_for_digest(digest)


class SopCRMError(RuntimeError):
    """表示 SOP Gateway 或 CRM 业务拒绝。"""

    def __init__(
        self,
        message: str,
        *,
        category: str = "business_rejection",
        http_status: int | None = None,
        error_code: str | None = None,
        sub_code: str | None = None,
        duplicate_entity_type: DuplicateEntityType | None = None,
    ) -> None:
        """保存受控 CRM 错误事实，供任务层区分失败并支持人工诊断。

        参数：message 为适配器生成的固定摘要；其余参数为白名单分类、状态码和对象类型。
        返回值：无。
        异常：无。
        副作用：异常仅保留固定摘要和受控元数据，不保存远端响应正文。
        """
        self.category = category
        self.http_status = http_status
        # 仅保存 Gateway 的非敏感枚举码，不保存 message、签名或业务内容。
        self.error_code = error_code
        self.sub_code = sub_code
        self.duplicate_entity_type = duplicate_entity_type
        super().__init__(message)


class SopCRMAdapter:
    """调用 CRM SOP 线索接口。"""

    def __init__(
        self,
        url: str,
        app_id: str,
        app_auth_token: str,
        private_key: str,
        *,
        timeout: float = 10.0,
        client: httpx.Client | None = None,
        trust_env: bool = True,
        proxy: str | None = None,
        authorization: str | None = None,
    ) -> None:
        """初始化 SOP 配置并加载 RSA 私钥。"""
        if not all(value.strip() for value in (url, app_id, app_auth_token, private_key)):
            raise ValueError("CRM SOP 配置不完整")
        self._url, self._app_id, self._token, self._timeout = url, app_id, app_auth_token, timeout
        self._authorization = (
            authorization.strip() if authorization and authorization.strip() else None
        )
        loaded_key = serialization.load_pem_private_key(private_key.encode(), password=None)
        if not isinstance(loaded_key, rsa.RSAPrivateKey):
            raise ValueError("CRM 私钥必须是 RSA 私钥")
        self._private_key = loaded_key
        # 代理只绑定 CRM 客户端，避免宿主机代理环境意外影响其他外部 Provider。
        self._client = client or httpx.Client(
            timeout=timeout,
            trust_env=trust_env,
            proxy=proxy.strip() if proxy and proxy.strip() else None,
        )
        self._owns_client = client is None

    def search_by_company_name(
        self, payload: Mapping[str, object] | str
    ) -> tuple[CRMSearchResult, ...]:
        """按 canonical payload 的 name/businessLine/tycCustomerId 查重。"""
        canonical = {"name": payload} if isinstance(payload, str) else dict(payload)
        data = self._call("crm_check_lead_duplicate", canonical)
        result = data.get("result")
        if result == 0:
            return ()
        lead_id = data.get("leadId")
        # 只接受明确返回的 CRM Lead ID；其他重复对象或缺少 ID 都转人工处理。
        if lead_id is None:
            raise SopCRMError(
                "CRM duplicate target unavailable",
                category="duplicate_target_unavailable",
                sub_code="duplicate_detected_without_lead_id",
                duplicate_entity_type=_classify_duplicate_entity_message(data.get("message")),
            )
        return (CRMSearchResult(str(lead_id), None, "CRM duplicate"),)

    def create_lead(
        self,
        payload: Mapping[str, object],
        *,
        idempotency_key: str,
        crm_user_id: str,
    ) -> CRMCreateResult:
        """创建 CRM 线索，并在边界注入冻结 CRM 用户身份。"""
        body = dict(payload)
        body["headerId"] = crm_user_id
        data = self._call("crm_create_lead", body)
        lead_id = data.get("id", data.get("leadId", data))
        if isinstance(lead_id, Mapping):
            lead_id = lead_id.get("id")
        if lead_id is None:
            raise SopCRMError("CRM create response missing lead id")
        return CRMCreateResult(str(lead_id), crm_user_id, "CRM create succeeded")

    def update_lead(
        self,
        crm_lead_id: str,
        payload: Mapping[str, object],
        *,
        idempotency_key: str,
        crm_user_id: str,
    ) -> CRMCreateResult:
        """更新冻结 CRM 线索，不改变既有负责人语义。"""
        body = dict(payload)
        body["id"] = crm_lead_id
        body["headerId"] = crm_user_id
        data = self._call("crm_update_lead", body)
        # 无明确布尔结果时不能证明远端拒绝，更不能自动再次覆盖。
        if type(data.get("success")) is not bool:
            raise SopCRMError("CRM update result unknown", category="malformed_response")
        if data.get("success") is False:
            raise SopCRMError("CRM update rejected")
        return CRMCreateResult(crm_lead_id, None, "CRM update succeeded")

    def submit_lead(
        self,
        payload: Mapping[str, object],
        *,
        idempotency_key: str,
        crm_user_id: str,
    ) -> CRMCreateResult:
        """提交字段到 CRM 统一线索接口，由 CRM 内部执行查重和分流。

        参数：payload 为已校验的 CRM 字段；idempotency_key 为本地冻结操作键；
        crm_user_id 为已映射的 CRM 提交人。
        返回值：CREATE 返回新线索身份，FOLLOW_UP 返回无新线索身份的成功结果。
        异常：接口错误或未知响应形态抛出 SopCRMError。
        副作用：向 CRM SOP 发起一次签名请求；requestId 在重试间保持稳定。
        """
        body = dict(payload)
        # SOP 的 requestId 限制为 100 字符；哈希同时保证键长稳定且重试时值不变。
        body["requestId"] = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        body["headerId"] = crm_user_id
        data = self._call("crm_submit_lead", body)
        action = data.get("action")
        if action == "CREATE":
            lead_id = data.get("leadId")
            if (
                isinstance(lead_id, bool)
                or not isinstance(lead_id, (str, int))
                or not str(lead_id).strip()
            ):
                raise SopCRMError(
                    "CRM submit response missing lead id", category="malformed_response"
                )
            return CRMCreateResult(
                str(lead_id), crm_user_id, "CRM submit CREATE succeeded", action="CREATE"
            )
        if action == "FOLLOW_UP":
            return CRMCreateResult(
                None, None, "CRM submit FOLLOW_UP succeeded", action="FOLLOW_UP"
            )
        raise SopCRMError("CRM submit response missing action", category="malformed_response")

    def _call(self, method: str, biz_content: Mapping[str, object]) -> Mapping[str, object]:
        """构造签名一致的表单请求并解析严格响应。"""
        content = json.dumps(dict(biz_content), ensure_ascii=False, separators=(",", ":"))
        params = {
            "app_id": self._app_id,
            "method": method,
            "format": "json",
            "charset": "utf-8",
            "sign_type": "RSA2",
            "timestamp": datetime.now(ZoneInfo("Asia/Shanghai")).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "version": "1.0",
            "app_auth_token": self._token,
            "biz_content": content,
        }
        sign_text = "&".join(f"{key}={value}" for key, value in sorted(params.items()) if value)
        signature = self._private_key.sign(sign_text.encode(), padding.PKCS1v15(), hashes.SHA256())
        params["sign"] = base64.b64encode(signature).decode("ascii")
        try:
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            if self._authorization is not None:
                headers["Authorization"] = self._authorization
            response = self._client.post(
                self._url,
                data=params,
                headers=headers,
            )
            status = response.status_code
            try:
                payload = response.json()
            except ValueError as error:
                category = (
                    "authentication"
                    if status == 401
                    else ("gateway" if status >= 400 else "malformed_response")
                )
                raise SopCRMError(
                    "CRM gateway response is not JSON", category=category, http_status=status
                ) from error
        except httpx.TimeoutException as error:
            raise SopCRMError("CRM gateway timeout", category="transport") from error
        except httpx.HTTPError as error:
            raise SopCRMError("CRM gateway transport failure", category="transport") from error
        except TypeError as error:
            raise SopCRMError(
                "CRM gateway malformed response", category="malformed_response"
            ) from error
        if not isinstance(payload, Mapping):
            raise SopCRMError(
                "CRM gateway malformed response", category="malformed_response", http_status=status
            )
        code = payload.get("code")
        if not isinstance(code, (int, str)):
            category = (
                "authentication"
                if status == 401
                else ("gateway" if status >= 400 else "malformed_response")
            )
            raise SopCRMError("CRM response missing code", category=category, http_status=status)
        if code not in (0, 200):
            raw_error_code = payload.get("error_code")
            raw_sub_code = payload.get("sub_code")
            error_code = str(raw_error_code) if isinstance(raw_error_code, (str, int)) else None
            sub_code = str(raw_sub_code) if isinstance(raw_sub_code, (str, int)) else None
            reported_error_code = error_code or (
                str(code) if isinstance(code, (str, int)) else None
            )
            # 仅 sub_code/error_code 参与精确分类；纯数字顶层 code 不能单独证明是业务错误。
            classification_code = sub_code or error_code or ""
            auth_codes = (
                "missing-signature", "invalid-signature", "invalid-app-id",
                "invalid-timestamp", "invalid-auth-token", "invalid-app-auth-token",
                "aop.invalid-auth-token", "aop.invalid-app-auth-token",
            )
            gateway_codes = (
                "route-no-permissions",
                "invalid-content-type",
                "insufficient-isv-permissions",
            )
            if any(token in classification_code for token in auth_codes):
                category = "authentication"
            elif any(token in classification_code for token in gateway_codes):
                category = "gateway"
            elif classification_code:
                category = "business"
            elif "status" in payload or "message" in payload or "msg" in payload:
                # SOP 的另一种合法错误 envelope 可能只有 code/data/status/message；
                # 已收到可解释 JSON 时，未知错误必须归入 Gateway，而不是伪装成传输失败。
                category = "gateway"
            else:
                category = "malformed_response"
            if status == 401:
                category = "authentication"
            elif status >= 400 and category == "business":
                category = "gateway"
            raise SopCRMError(
                "CRM gateway rejected request",
                category=category,
                http_status=status,
                error_code=reported_error_code,
                sub_code=sub_code,
            )
        # HTTP 错误与成功 envelope 冲突时仍保留未知事实，不能把 5xx 记成成功覆盖。
        if status >= 400:
            raise SopCRMError(
                "CRM HTTP failure", category="authentication" if status == 401 else "gateway",
                http_status=status,
            )
        data = payload.get("data")
        # 修改接口返回 Boolean；空值或未知响应不能被当作远端成功。
        if method == "crm_update_lead" and type(data) is bool:
            return {"success": data}
        if isinstance(data, Mapping):
            return data
        if method == "crm_create_lead" and data not in (None, ""):
            return {"id": data}
        raise SopCRMError(
            "CRM response missing data", category="malformed_response", http_status=status
        )
