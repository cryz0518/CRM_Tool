"""天眼查真实 HTTP 适配器。"""

from __future__ import annotations

import re
from collections.abc import Mapping

import httpx

from app.companies.models import TYCCandidate, TYCLookupResult
from app.companies.service import TYCAdapterError


class TianYanChaAdapter:
    """通过天眼查开放搜索接口查询企业候选，并严格区分故障与无结果。"""

    def __init__(
        self,
        url: str,
        api_key: str,
        *,
        timeout: float = 10.0,
        client: httpx.Client | None = None,
    ) -> None:
        """初始化天眼查请求配置。

        参数：url 为搜索接口地址；api_key 为授权值；timeout 为请求超时；client 可注入测试客户端。
        返回值：无。异常：配置为空时抛出 ValueError。副作用：仅保存配置，不发起请求。
        """
        if not url.strip() or not api_key.strip():
            raise ValueError("天眼查配置不完整")
        self._url = url
        self._api_key = api_key
        self._timeout = timeout
        self._client = client or httpx.Client(timeout=timeout)

    def lookup(self, company_name: str) -> TYCLookupResult:
        """查询企业候选并返回 matched/not_found/ambiguous 结论。"""
        try:
            response = self._client.get(
                self._url,
                params={"word": company_name, "pageNum": 1, "pageSize": 20},
                headers={"Authorization": self._api_key},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.TimeoutException, TimeoutError) as error:
            raise TYCAdapterError("天眼查请求超时") from error
        except (httpx.HTTPError, ValueError, TypeError) as error:
            raise TYCAdapterError("天眼查请求或响应异常") from error
        if not isinstance(payload, Mapping):
            raise TYCAdapterError("天眼查响应协议异常")
        error_code = payload.get("error_code")
        if error_code == 300000:
            return TYCLookupResult.not_found()
        if error_code != 0:
            raise TYCAdapterError(f"天眼查接口错误:{self._classify_error(error_code)}")
        result = payload.get("result")
        items = result.get("items") if isinstance(result, Mapping) else None
        if not isinstance(items, list):
            raise TYCAdapterError("天眼查响应缺少候选列表")
        candidates = tuple(
            TYCCandidate(str(item["name"]).strip(), str(item["id"]))
            for item in items
            if self._is_company(item)
        )
        if not candidates:
            return TYCLookupResult.not_found()
        query = self._normalize(company_name)
        exact = tuple(
            candidate
            for candidate in candidates
            if self._normalize(candidate.standard_company_name) == query
        )
        if len(exact) == 1:
            return TYCLookupResult.matched(exact[0])
        return TYCLookupResult.ambiguous(candidates)

    @staticmethod
    def _normalize(value: str) -> str:
        """执行最小安全名称规范化。"""
        return re.sub(r"\s+", "", value.strip()).casefold()

    @staticmethod
    def _is_company(item: object) -> bool:
        """过滤非企业搜索结果。"""
        if not isinstance(item, Mapping) or not item.get("name") or not item.get("id"):
            return False
        # 天眼查搜索接口将企业类型返回为数字 1；不能只按字符串枚举判断，否则会把真实企业全部过滤掉。
        raw_type = item.get("type")
        if isinstance(raw_type, int):
            return raw_type == 1
        if isinstance(raw_type, str) and raw_type.strip().isdigit():
            return raw_type.strip() == "1"
        kind = str(raw_type or item.get("entityType") or item.get("category") or "")
        return not kind or any(token in kind.casefold() for token in ("company", "企业", "公司"))

    @staticmethod
    def _classify_error(error_code: object) -> str:
        """把外部错误码转换成不含凭据的稳定分类。"""
        text = str(error_code).casefold()
        if "auth" in text or text in {"401", "403"}:
            return "authentication_or_permission"
        if "limit" in text or "quota" in text or "balance" in text or text in {"429"}:
            return "rate_limit_or_quota"
        return "remote_error"
