"""Break-glass 单对象原始数据访问及审计先行实现。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from sqlalchemy.orm import Session, sessionmaker

from app.console.models import BreakGlassAccessAudit
from app.leads.models import Lead
from app.media.storage import SignedURLProvider
from app.messaging.models import IncomingMessage, MessageAttachment


class BreakGlassAccessError(RuntimeError):
    """表示 Break-glass 请求未能在安全约束下完成。"""


@dataclass(frozen=True)
class BreakGlassAccessRequest:
    """定义单次原始数据访问请求，明确禁止批量对象。"""

    operator_subject: str
    operator_role: str
    object_type: str
    object_id: str
    access_type: str
    reason: str
    request_id: str
    auth_source: str = "unknown"
    request_context: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class BreakGlassAccessResult:
    """承载 Break-glass 返回的单个对象内容或附件签名地址。"""

    object_type: str
    object_id: str
    access_type: str
    payload: dict[str, Any] | None = None
    signed_url: str | None = None


class BreakGlassAccessService:
    """执行认证之后的审计先行、单对象读取和完成审计。"""

    _ACCESS_TYPES = {
        ("message", "view_raw_message"),
        ("contact", "view_full_contact"),
        ("attachment", "preview_attachment"),
        ("attachment", "download_attachment"),
    }
    _ALLOWED_ROLES = frozenset({"administrator", "operations_admin"})

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        signed_url_provider: SignedURLProvider | None = None,
    ) -> None:
        """注入数据库会话工厂和可选附件存储提供器。

        参数：session_factory 为审计和对象读取事务工厂；signed_url_provider 为私有附件签名边界。
        返回值：无。
        异常：无。
        副作用：仅保存依赖，不发起读取。
        """
        self._session_factory = session_factory
        self._signed_url_provider = signed_url_provider

    def access(self, request: BreakGlassAccessRequest) -> BreakGlassAccessResult:
        """先提交授权审计，再读取单个原始消息、联系人或附件。

        参数：request 为包含访问原因和单个对象标识的 Break-glass 请求。
        返回值：单个对象内容；附件只返回带有效期的签名地址。
        异常：原因为空、对象类型非法、审计失败或读取失败时抛出 BreakGlassAccessError。
        副作用：至少追加授权审计和完成/失败审计各一条。
        """
        self._validate_request(request)
        access_id = str(uuid4())
        self._record_audit(
            request,
            access_id=access_id,
            phase="granted",
            outcome="granted",
            data_returned=False,
        )
        try:
            result = self._read_object(request)
        except Exception as error:
            self._record_audit(
                request,
                access_id=access_id,
                phase="completed",
                outcome="failed",
                data_returned=False,
            )
            if isinstance(error, BreakGlassAccessError):
                raise
            raise BreakGlassAccessError("原始数据读取失败") from error

        self._record_audit(
            request,
            access_id=access_id,
            phase="completed",
            outcome="succeeded",
            data_returned=True,
        )
        return result

    @classmethod
    def _validate_request(cls, request: BreakGlassAccessRequest) -> None:
        """校验访问原因、对象数量语义和访问类型组合。

        参数：request 为待校验的访问请求。
        返回值：无。
        异常：不符合安全约束时抛出 BreakGlassAccessError。
        副作用：无。
        """
        reason = request.reason.strip()
        if not reason:
            raise BreakGlassAccessError("Break-glass 必须填写原因")
        if len(reason) > 512:
            raise BreakGlassAccessError("Break-glass 原因过长")
        if not request.object_id or "," in request.object_id or "[" in request.object_id:
            raise BreakGlassAccessError("Break-glass 只允许单对象访问")
        if request.operator_role not in cls._ALLOWED_ROLES:
            raise BreakGlassAccessError("Break-glass 需要管理员角色")
        if (
            request.access_type == "download_attachment"
            and request.operator_role != "administrator"
        ):
            raise BreakGlassAccessError("下载附件需要管理员角色")
        if (request.object_type, request.access_type) not in cls._ACCESS_TYPES:
            raise BreakGlassAccessError("Break-glass 访问类型不受支持")

    def _record_audit(
        self,
        request: BreakGlassAccessRequest,
        *,
        access_id: str,
        phase: str,
        outcome: str,
        data_returned: bool,
    ) -> None:
        """追加一条不可变 Break-glass 审计事件。

        参数：request 为访问事实；其余参数描述审计阶段和结果。
        返回值：无。
        异常：审计数据库不可写时抛出 BreakGlassAccessError。
        副作用：提交一条新的审计行，绝不保存原始数据内容。
        """
        try:
            with self._session_factory.begin() as session:
                session.add(
                    BreakGlassAccessAudit(
                        access_id=access_id,
                        request_id=request.request_id,
                        operator_subject=request.operator_subject,
                        operator_role=request.operator_role,
                        auth_source=request.auth_source,
                        object_type=request.object_type,
                        object_id=request.object_id,
                        access_type=request.access_type,
                        reason=request.reason.strip(),
                        phase=phase,
                        outcome=outcome,
                        data_returned=data_returned,
                        request_context=dict(request.request_context),
                    )
                )
        except Exception as error:
            # 首次审计失败时调用方会在读取前拒绝；完成审计失败也不能伪装成功。
            raise BreakGlassAccessError("Break-glass 审计失败，拒绝原始访问") from error

    def _read_object(self, request: BreakGlassAccessRequest) -> BreakGlassAccessResult:
        """读取已完成授权审计的单个对象。

        参数：request 为已通过基本校验的请求。
        返回值：单对象 Break-glass 内容。
        异常：对象不存在、附件未配置存储或读取失败时抛出受控错误。
        副作用：读取数据库或附件存储，不修改领域状态。
        """
        with self._session_factory() as session:
            if request.object_type == "message":
                message = session.get(IncomingMessage, request.object_id)
                if message is None:
                    raise BreakGlassAccessError("原始消息不存在")
                return BreakGlassAccessResult(
                    request.object_type,
                    request.object_id,
                    request.access_type,
                    payload=dict(message.raw_payload),
                )
            if request.object_type == "contact":
                lead = session.get(Lead, request.object_id)
                if lead is None:
                    raise BreakGlassAccessError("线索联系人不存在")
                return BreakGlassAccessResult(
                    request.object_type,
                    request.object_id,
                    request.access_type,
                    payload={
                        "field_values": dict(lead.field_values),
                        "enrichment_values": dict(lead.enrichment_values),
                    },
                )
            attachment = session.get(MessageAttachment, request.object_id)
            if attachment is None or not attachment.storage_key:
                raise BreakGlassAccessError("附件不存在或尚未落盘")
            if self._signed_url_provider is None:
                raise BreakGlassAccessError("附件签名提供器未配置")
            # Break-glass 只取得短期签名地址，避免应用响应或日志承载二进制内容。
            signed_url = self._signed_url_provider.create_signed_url(
                attachment.storage_key,
                expires_in_seconds=300,
                download=request.access_type == "download_attachment",
            )
            return BreakGlassAccessResult(
                request.object_type,
                request.object_id,
                request.access_type,
                signed_url=signed_url,
            )
