"""CRM SOP Gateway RSA2 适配器。"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from app.crm.adapter import CRMCreateResult, CRMSearchResult


class SopCRMError(RuntimeError):
    """表示 SOP Gateway 或 CRM 业务拒绝。"""

    def __init__(
        self, message: str, *, category: str = "business_rejection", http_status: int | None = None
    ) -> None:
        """保存脱敏错误分类，供任务层区分永久拒绝与传输失败。"""
        self.category = category
        self.http_status = http_status
        super().__init__(message)


class SopCRMAdapter:
    """调用 CRM SOP 查重、新增和修改接口。"""

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
    ) -> None:
        """初始化 SOP 配置并加载 RSA 私钥。"""
        if not all(value.strip() for value in (url, app_id, app_auth_token, private_key)):
            raise ValueError("CRM SOP 配置不完整")
        self._url, self._app_id, self._token, self._timeout = url, app_id, app_auth_token, timeout
        loaded_key = serialization.load_pem_private_key(private_key.encode(), password=None)
        if not isinstance(loaded_key, rsa.RSAPrivateKey):
            raise ValueError("CRM 私钥必须是 RSA 私钥")
        self._private_key = loaded_key
        self._client = client or httpx.Client(timeout=timeout, trust_env=trust_env)
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
        if lead_id is None:
            raise SopCRMError("CRM duplicate requires manual review")
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
        if data.get("success") is False:
            raise SopCRMError("CRM update rejected")
        return CRMCreateResult(crm_lead_id, None, "CRM update succeeded")

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
            response = self._client.post(
                self._url,
                data=params,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            status = response.status_code
            try:
                payload = response.json()
            except ValueError as error:
                category = "authentication" if status == 401 else (
                    "gateway" if status >= 400 else "malformed_response"
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
            error_code = str(payload.get("sub_code") or payload.get("error_code") or "")
            auth_codes = (
                "missing-signature", "invalid-signature", "invalid-app-id",
                "invalid-timestamp", "invalid-auth-token", "invalid-app-auth-token",
                "aop.invalid-auth-token", "aop.invalid-app-auth-token",
            )
            gateway_codes = (
                "route-no-permissions", "invalid-content-type", "insufficient-isv-permissions"
            )
            if any(token in error_code for token in auth_codes):
                category = "authentication"
            elif any(token in error_code for token in gateway_codes):
                category = "gateway"
            elif error_code:
                category = "business"
            else:
                category = "malformed_response"
            if status == 401:
                category = "authentication"
            elif status >= 400 and category == "business":
                category = "gateway"
            raise SopCRMError("CRM gateway rejected request", category=category, http_status=status)
        data = payload.get("data")
        if isinstance(data, Mapping):
            return data
        if method == "crm_create_lead" and data not in (None, ""):
            return {"id": data}
        if method == "crm_update_lead" and data in (None, ""):
            return {"success": True}
        raise SopCRMError("CRM response missing data")
