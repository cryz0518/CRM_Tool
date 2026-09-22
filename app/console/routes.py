"""Operations Console HTTP 路由，仅编排认证和只读模块。"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from app.console.auth import (
    AdminCredentials,
    AdminPrincipal,
    AuthenticatedPrincipal,
    AuthenticationProvider,
    AuthorizationDecision,
    CapabilityAuthorizer,
    ConsoleCapability,
)
from app.console.break_glass import (
    BreakGlassAccessError,
    BreakGlassAccessRequest,
    BreakGlassAccessService,
)
from app.console.dependencies import (
    get_admin_identity_provider,
    get_break_glass_access_service,
    get_capability_authorizer,
    get_console_maintenance_service,
    get_console_query_service,
)
from app.console.dto import (
    ConsoleAIExecutionDTO,
    ConsoleAttachmentDTO,
    ConsoleAuditEventDTO,
    ConsoleConfigIssueDTO,
    ConsoleConflictDTO,
    ConsoleLeadDTO,
    ConsoleMaintenanceResultDTO,
    ConsoleMessageDTO,
    ConsoleOverviewDTO,
    ConsolePage,
    ConsoleSalesAuthorizationDTO,
    ConsoleSyncDTO,
    ConsoleTaskDTO,
)
from app.console.maintenance import ConsoleMaintenanceResult, ConsoleMaintenanceService
from app.console.queries import ConsoleQueryService
from app.core.request_id import normalize_request_id

router = APIRouter()
console_api = APIRouter(prefix="/api/console", tags=["Operations Console"])


class BreakGlassRequestBody(BaseModel):
    """定义单对象 Break-glass HTTP 请求，不允许额外批量字段。"""

    model_config = ConfigDict(extra="forbid")

    object_type: str
    object_id: str
    access_type: str
    reason: str = Field(min_length=1, max_length=512)


class RetryMaintenanceBody(BaseModel):
    """定义失败消息重试的受控分段输入。"""

    model_config = ConfigDict(extra="forbid")

    segment_index: int = Field(default=0, ge=0)


class ReassignMaintenanceBody(BaseModel):
    """定义消息分段重新归属输入。"""

    model_config = ConfigDict(extra="forbid")

    segment_index: int = Field(default=0, ge=0)
    new_lead_id: str = Field(min_length=1, max_length=128)
    reason: str = Field(min_length=1, max_length=512)


class DiscardMaintenanceBody(BaseModel):
    """定义线索逻辑废弃输入。"""

    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=512)


class AdminCreateLeadBody(BaseModel):
    """定义管理员补建线索的显式四种身份和最小业务字段。"""

    model_config = ConfigDict(extra="forbid")

    original_capturing_sales_user_id: str = Field(min_length=1, max_length=128)
    smart_table_owner_user_id: str = Field(min_length=1, max_length=128)
    field_values: dict[str, str]
    reason: str = Field(min_length=1, max_length=512)


class TransferOwnerBody(BaseModel):
    """定义 Smart Table Owner 转交输入。"""

    model_config = ConfigDict(extra="forbid")

    new_owner_user_id: str = Field(min_length=1, max_length=128)
    reason: str = Field(min_length=1, max_length=512)


class MaintenanceRecoveryBody(BaseModel):
    """定义恢复未知远端结果所需的受审计原因。"""

    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=512)


def _as_legacy_admin_principal(principal: AuthenticatedPrincipal) -> AdminPrincipal:
    """将稳定认证主体投影为旧维护服务仍使用的 AdminPrincipal。

    参数：principal 为认证 Provider 返回的已验证主体。
    返回值：保留 T16 maintenance 输入契约的管理员主体。
    异常：主体不含可用群组时仍返回无角色主体，后续 capability 会拒绝。
    副作用：只创建内存对象，不保存凭据。
    """
    if isinstance(principal, AdminPrincipal):
        return principal
    roles = frozenset(getattr(principal, "roles", principal.groups))
    return AdminPrincipal(
        principal.subject,
        roles,
        principal.provider,
        authenticated_at=principal.authenticated_at,
        claims=principal.claims,
        groups=principal.groups,
        tenant=principal.tenant,
        display_name=principal.display_name,
    )


def _require_console_admin(
    request: Request,
    provider: Annotated[AuthenticationProvider, Depends(get_admin_identity_provider)],
    authorizer: Annotated[CapabilityAuthorizer, Depends(get_capability_authorizer)],
) -> AdminPrincipal:
    """认证并授权普通 Console 读取请求。

    参数：request 为 HTTP 请求；provider 为管理员身份 Provider。
    返回值：已通过 Console 读取能力校验的管理员主体。
    异常：认证缺失返回 401；认证主体无权返回 403。
    副作用：不读取业务数据。
    """
    authenticated = provider.authenticate(
        AdminCredentials(token=request.headers.get("X-Console-Admin-Token"))
    )
    if authenticated is None:
        raise HTTPException(status_code=401, detail="需要管理员认证")
    principal = _as_legacy_admin_principal(authenticated)
    if not authorizer.authorize(principal, ConsoleCapability.CONSOLE_READ):
        raise HTTPException(status_code=403, detail="没有 Console 读取权限")
    return principal


def _require_audit_admin(
    request: Request,
    provider: Annotated[AuthenticationProvider, Depends(get_admin_identity_provider)],
    authorizer: Annotated[CapabilityAuthorizer, Depends(get_capability_authorizer)],
) -> AdminPrincipal:
    """认证并授权审计查询请求。"""
    authenticated = provider.authenticate(
        AdminCredentials(token=request.headers.get("X-Console-Admin-Token"))
    )
    if authenticated is None:
        raise HTTPException(status_code=401, detail="需要管理员认证")
    principal = _as_legacy_admin_principal(authenticated)
    if not authorizer.authorize(principal, ConsoleCapability.AUDIT_READ):
        raise HTTPException(status_code=403, detail="没有审计读取权限")
    return principal


def _require_console_maintenance_admin(
    request: Request,
    provider: Annotated[AuthenticationProvider, Depends(get_admin_identity_provider)],
    authorizer: Annotated[CapabilityAuthorizer, Depends(get_capability_authorizer)],
    service: Annotated[ConsoleMaintenanceService, Depends(get_console_maintenance_service)],
) -> AdminPrincipal:
    """认证 Console maintenance capability，并核对持久化管理员目录。"""

    authenticated = provider.authenticate(
        AdminCredentials(token=request.headers.get("X-Console-Admin-Token"))
    )
    if authenticated is None:
        raise HTTPException(status_code=401, detail="需要管理员认证")
    principal = _as_legacy_admin_principal(authenticated)
    if not authorizer.authorize(principal, ConsoleCapability.CONSOLE_MAINTENANCE_WRITE):
        raise HTTPException(status_code=403, detail="没有 Console 管理写权限")
    try:
        service.require_admin(principal)
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error)) from error
    return principal


def _request_id(request: Request) -> str:
    """保留 T16 maintenance 的严格 request id 输入契约。

    参数：request 为当前 HTTP 请求。
    返回值：合法的管理请求幂等标识；未提供时使用中间件服务端标识。
    异常：非法客户端值抛出 HTTP 400，保持 T16 maintenance 兼容性。
    副作用：无；Break-glass 路由使用中间件规范化值，不调用本函数。
    """
    request_id = request.headers.get("X-Request-ID")
    normalized = request_id.strip() if request_id else ""
    if not normalized:
        return normalize_request_id(getattr(request.state, "request_id", None))
    if len(normalized) > 128 or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-"
        for character in normalized
    ):
        raise HTTPException(status_code=400, detail="X-Request-ID 格式非法")
    return normalized


def _maintenance_dto(result: ConsoleMaintenanceResult) -> ConsoleMaintenanceResultDTO:
    """将领域结果投影为不含敏感业务载荷的 HTTP DTO。"""

    return ConsoleMaintenanceResultDTO(
        operation_id=result.operation_id,
        status=result.status,
        lead_id=result.lead_id,
        message_id=result.message_id,
        attempt_id=result.attempt_id,
        record_id=result.record_id,
        detail=result.detail,
    )


def _raise_maintenance_error(error: Exception) -> None:
    """把领域服务输入、权限和冲突异常映射为 HTTP 状态。"""

    if isinstance(error, PermissionError):
        raise HTTPException(status_code=403, detail=str(error)) from error
    message = str(error)
    if "已有进行中" in message or "同公司" in message:
        raise HTTPException(status_code=409, detail=message) from error
    raise HTTPException(status_code=400, detail=message) from error


def _require_break_glass_capability(
    authorizer: CapabilityAuthorizer,
    principal: AdminPrincipal,
    access_type: str,
) -> AuthorizationDecision:
    """将访问类型映射到最小 Break-glass 能力并返回已验证授权依据。

    参数：provider 为认证后的授权 Provider；principal 为已验证主体；access_type 为请求访问类型。
    返回值：允许时返回带 capability、角色和依据的授权判定。
    异常：能力未知、Provider 无法提供可审计判定或无权时抛出 HTTP 403。
    副作用：无；不会读取请求体中的 operator 或 role。
    """
    capability_map = {
        "view_raw_message": ConsoleCapability.BREAK_GLASS_RAW_MESSAGE,
        "view_full_contact": ConsoleCapability.BREAK_GLASS_FULL_CONTACT,
        "preview_attachment": ConsoleCapability.BREAK_GLASS_PREVIEW_ATTACHMENT,
        "download_attachment": ConsoleCapability.BREAK_GLASS_DOWNLOAD_ATTACHMENT,
    }
    capability = capability_map.get(access_type)
    decision = authorizer.decide(principal, capability) if capability is not None else None
    if decision is None or not decision.allowed or not decision.role:
        raise HTTPException(status_code=403, detail="没有 Break-glass 访问权限")
    return decision


@console_api.get("/overview", response_model=ConsoleOverviewDTO)
def overview(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
) -> ConsoleOverviewDTO:
    """返回 Console 首页健康、数量和 readiness 状态。"""
    return service.get_overview()


@console_api.get("/leads", response_model=ConsolePage[ConsoleLeadDTO])
def leads(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
    status: str | None = None,
) -> ConsolePage[ConsoleLeadDTO]:
    """返回脱敏线索分页。"""
    return service.list_leads(limit=limit, cursor=cursor, status=status)


@console_api.get("/leads/{lead_id}", response_model=ConsoleLeadDTO)
def lead_detail(
    lead_id: str,
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
) -> ConsoleLeadDTO:
    """返回单条脱敏线索详情。"""
    result = service.get_lead(lead_id)
    if result is None:
        raise HTTPException(status_code=404, detail="线索不存在")
    return result


@console_api.get("/messages", response_model=ConsolePage[ConsoleMessageDTO])
def messages(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
) -> ConsolePage[ConsoleMessageDTO]:
    """返回脱敏消息分页。"""
    return service.list_messages(limit=limit, cursor=cursor)


@console_api.get("/messages/{message_id}", response_model=ConsoleMessageDTO)
def message_detail(
    message_id: str,
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
) -> ConsoleMessageDTO:
    """返回单条脱敏消息详情，不返回原始 payload。"""
    result = service.get_message(message_id)
    if result is None:
        raise HTTPException(status_code=404, detail="消息不存在")
    return result


@console_api.get("/unassigned", response_model=ConsolePage[ConsoleMessageDTO])
def unassigned(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
) -> ConsolePage[ConsoleMessageDTO]:
    """返回待归属消息分页。"""
    return service.list_messages(limit=limit, cursor=cursor, unassigned=True)


@console_api.get("/media", response_model=ConsolePage[ConsoleAttachmentDTO])
def media(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
) -> ConsolePage[ConsoleAttachmentDTO]:
    """返回附件安全元数据。"""
    return service.list_attachments(limit=limit, cursor=cursor)


@console_api.get("/tasks", response_model=ConsolePage[ConsoleTaskDTO])
def tasks(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    status: str | None = None,
) -> ConsolePage[ConsoleTaskDTO]:
    """返回聚合后的任务和失败状态。"""
    return service.list_tasks(limit=limit, status=status)


@console_api.get("/ai-executions", response_model=ConsolePage[ConsoleAIExecutionDTO])
def ai_executions(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
    status: str | None = None,
) -> ConsolePage[ConsoleAIExecutionDTO]:
    """返回 AI 执行元数据。"""
    return service.list_ai_executions(limit=limit, cursor=cursor, status=status)


@console_api.get("/smart-table-syncs", response_model=ConsolePage[ConsoleSyncDTO])
def smart_table_syncs(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
) -> ConsolePage[ConsoleSyncDTO]:
    """返回智能表格同步状态。"""
    return service.list_smart_table_syncs(limit=limit)


@console_api.get("/crm-syncs", response_model=ConsolePage[ConsoleSyncDTO])
def crm_syncs(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
) -> ConsolePage[ConsoleSyncDTO]:
    """返回 CRM 同步状态和快照摘要。"""
    return service.list_crm_syncs(limit=limit)


@console_api.get("/conflicts", response_model=ConsolePage[ConsoleConflictDTO])
def conflicts(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
    source: str | None = None,
) -> ConsolePage[ConsoleConflictDTO]:
    """返回可按来源筛选并分页的完整冲突状态投影。"""
    return service.list_conflicts(limit=limit, cursor=cursor, source=source)


@console_api.get("/audits", response_model=ConsolePage[ConsoleAuditEventDTO])
def audits(
    _: Annotated[AdminPrincipal, Depends(_require_audit_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
) -> ConsolePage[ConsoleAuditEventDTO]:
    """返回业务和 Break-glass 审计的安全摘要。"""
    return service.list_audits(limit=limit)


@console_api.get("/config", response_model=ConsolePage[ConsoleConfigIssueDTO])
def config(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
) -> ConsolePage[ConsoleConfigIssueDTO]:
    """返回 readiness 和配置风险摘要。"""
    return service.list_config()


@console_api.get(
    "/sales-authorizations", response_model=ConsolePage[ConsoleSalesAuthorizationDTO]
)
def sales_authorizations(
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
) -> ConsolePage[ConsoleSalesAuthorizationDTO]:
    """返回销售授权范围、启用状态和 CRM 映射异常。

    参数：认证主体仅用于执行 Console 读取授权；service 提供只读查询；limit 和 cursor 控制分页。
    返回值：不包含 CRM 用户标识的目录分页结果。
    异常：无效查询参数由 FastAPI 返回 422；读取失败由框架转换为服务错误。
    副作用：无，不修改销售授权、CRM 映射或线索。
    """
    return service.list_sales_authorizations(limit=limit, cursor=cursor)


@console_api.get(
    "/sales-authorizations/{sales_user_id}/affected-leads",
    response_model=ConsolePage[ConsoleLeadDTO],
)
def sales_authorization_affected_leads(
    sales_user_id: str,
    _: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    service: Annotated[ConsoleQueryService, Depends(get_console_query_service)],
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
) -> ConsolePage[ConsoleLeadDTO]:
    """返回指定 CRM 映射缺失销售当前受阻的脱敏线索。

    参数：sales_user_id 为企业微信销售用户标识；认证主体用于读取授权；service 执行查询；
    limit 和 cursor 控制分页。
    返回值：仅含 pending_create 或 pending_update 的脱敏线索分页结果。
    异常：无效查询参数由 FastAPI 返回 422；读取失败由框架转换为服务错误。
    副作用：无，不修改目录、线索或 CRM 同步事实。
    """
    return service.list_mapping_missing_leads(sales_user_id, limit=limit, cursor=cursor)


@console_api.post(
    "/maintenance/messages/{message_id}/retry",
    response_model=ConsoleMaintenanceResultDTO,
)
def maintenance_retry(
    message_id: str,
    body: RetryMaintenanceBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_maintenance_admin)],
    service: Annotated[ConsoleMaintenanceService, Depends(get_console_maintenance_service)],
) -> ConsoleMaintenanceResultDTO:
    """复用 T14 失败消息受保护重试。"""

    try:
        return _maintenance_dto(
            service.retry_failed_message(
                principal,
                message_id=message_id,
                segment_index=body.segment_index,
                request_id=_request_id(request),
            )
        )
    except (ValueError, PermissionError) as error:
        _raise_maintenance_error(error)
        raise AssertionError("unreachable")


@console_api.post(
    "/maintenance/messages/{message_id}/reassign",
    response_model=ConsoleMaintenanceResultDTO,
)
def maintenance_reassign(
    message_id: str,
    body: ReassignMaintenanceBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_maintenance_admin)],
    service: Annotated[ConsoleMaintenanceService, Depends(get_console_maintenance_service)],
) -> ConsoleMaintenanceResultDTO:
    """复用 T14 消息分段人工重新归属。"""

    try:
        return _maintenance_dto(
            service.reassign(
                principal,
                message_id=message_id,
                segment_index=body.segment_index,
                new_lead_id=body.new_lead_id,
                reason=body.reason,
                request_id=_request_id(request),
            )
        )
    except (ValueError, PermissionError) as error:
        _raise_maintenance_error(error)
        raise AssertionError("unreachable")


@console_api.post(
    "/maintenance/leads/{lead_id}/discard",
    response_model=ConsoleMaintenanceResultDTO,
)
def maintenance_discard(
    lead_id: str,
    body: DiscardMaintenanceBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_maintenance_admin)],
    service: Annotated[ConsoleMaintenanceService, Depends(get_console_maintenance_service)],
) -> ConsoleMaintenanceResultDTO:
    """复用 T14 线索逻辑废弃。"""

    try:
        return _maintenance_dto(
            service.discard(
                principal,
                lead_id=lead_id,
                reason=body.reason,
                request_id=_request_id(request),
            )
        )
    except (ValueError, PermissionError) as error:
        _raise_maintenance_error(error)
        raise AssertionError("unreachable")


@console_api.post(
    "/maintenance/leads",
    response_model=ConsoleMaintenanceResultDTO,
)
def maintenance_create_lead(
    body: AdminCreateLeadBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_maintenance_admin)],
    service: Annotated[ConsoleMaintenanceService, Depends(get_console_maintenance_service)],
) -> ConsoleMaintenanceResultDTO:
    """通过独立 domain service 补建管理员线索。"""

    try:
        return _maintenance_dto(
            service.create_lead(
                principal,
                original_capturing_sales_user_id=body.original_capturing_sales_user_id,
                smart_table_owner_user_id=body.smart_table_owner_user_id,
                field_values=body.field_values,
                reason=body.reason,
                request_id=_request_id(request),
            )
        )
    except (ValueError, PermissionError) as error:
        _raise_maintenance_error(error)
        raise AssertionError("unreachable")


@console_api.post(
    "/maintenance/leads/{lead_id}/smart-table-owner-transfer",
    response_model=ConsoleMaintenanceResultDTO,
)
def maintenance_transfer_owner(
    lead_id: str,
    body: TransferOwnerBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_maintenance_admin)],
    service: Annotated[ConsoleMaintenanceService, Depends(get_console_maintenance_service)],
) -> ConsoleMaintenanceResultDTO:
    """通过权限验证 seam 转交 Smart Table Owner。"""

    try:
        return _maintenance_dto(
            service.transfer_owner(
                principal,
                lead_id=lead_id,
                new_owner_user_id=body.new_owner_user_id,
                reason=body.reason,
                request_id=_request_id(request),
            )
        )
    except (ValueError, PermissionError) as error:
        _raise_maintenance_error(error)
        raise AssertionError("unreachable")


@console_api.post(
    "/maintenance/transfer-operations/{operation_id}/reconcile",
    response_model=ConsoleMaintenanceResultDTO,
)
def maintenance_reconcile_transfer(
    operation_id: str,
    body: MaintenanceRecoveryBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_maintenance_admin)],
    service: Annotated[ConsoleMaintenanceService, Depends(get_console_maintenance_service)],
) -> ConsoleMaintenanceResultDTO:
    """核验远端负责人事实并恢复待处理转交 operation。"""

    try:
        return _maintenance_dto(
            service.reconcile_transfer(
                principal,
                operation_id=operation_id,
                reason=body.reason,
                request_id=_request_id(request),
            )
        )
    except (ValueError, PermissionError) as error:
        _raise_maintenance_error(error)
        raise AssertionError("unreachable")


@console_api.post(
    "/maintenance/creation-operations/{operation_id}/reconcile",
    response_model=ConsoleMaintenanceResultDTO,
)
def maintenance_reconcile_create(
    operation_id: str,
    body: MaintenanceRecoveryBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_maintenance_admin)],
    service: Annotated[ConsoleMaintenanceService, Depends(get_console_maintenance_service)],
) -> ConsoleMaintenanceResultDTO:
    """核验远端记录事实并恢复管理员补建 operation。"""

    try:
        return _maintenance_dto(
            service.reconcile_create(
                principal,
                operation_id=operation_id,
                reason=body.reason,
                request_id=_request_id(request),
            )
        )
    except (ValueError, PermissionError) as error:
        _raise_maintenance_error(error)
        raise AssertionError("unreachable")


@console_api.post("/break-glass/access")
def break_glass_access(
    body: BreakGlassRequestBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    authorizer: Annotated[CapabilityAuthorizer, Depends(get_capability_authorizer)],
    service: Annotated[BreakGlassAccessService, Depends(get_break_glass_access_service)],
) -> JSONResponse:
    """执行单对象 Break-glass 访问，审计失败时拒绝返回原始数据。"""
    decision = _require_break_glass_capability(authorizer, principal, body.access_type)
    access_request = BreakGlassAccessRequest(
        operator_subject=principal.subject,
        operator_role=decision.role or "unknown",
        auth_source=principal.provider,
        object_type=body.object_type,
        object_id=body.object_id,
        access_type=body.access_type,
        reason=body.reason,
        # Break-glass 使用 middleware 已规范化的 server id，不直接信任 header。
        request_id=normalize_request_id(getattr(request.state, "request_id", None)),
        request_context={
            "route": str(request.url.path),
            "verified_subject": principal.subject,
            "provider": principal.provider,
            "capability": decision.capability.value,
            "authorization_basis": decision.basis,
        },
    )
    try:
        result = service.access(access_request)
    except BreakGlassAccessError as error:
        message = str(error)
        status_code = 503 if "审计失败" in message else 400
        raise HTTPException(status_code=status_code, detail=message) from error
    # 原始消息/联系人仅在已审计的 JSON 响应中返回；附件只能返回签名地址。
    payload = result.payload or ({"signed_url": result.signed_url} if result.signed_url else {})
    return JSONResponse(content=payload, headers={"Cache-Control": "no-store"})


@router.get("/console", response_class=HTMLResponse)
def console_shell(_: Annotated[AdminPrincipal, Depends(_require_console_admin)]) -> HTMLResponse:
    """返回内部 Console 可用的轻量查询页面，不承载业务写操作。"""
    return HTMLResponse(
        """
        <!doctype html>
        <html lang="zh-CN">
          <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Operations Console</title>
            <style>
              body { font-family: sans-serif; margin: 2rem; }
              nav { display: flex; flex-wrap: wrap; gap: .5rem; margin: 1rem 0; }
              button { cursor: pointer; }
              pre { background: #f5f5f5; padding: 1rem; overflow: auto; }
            </style>
          </head>
          <body>
            <h1>Operations Console</h1>
            <label>开发/测试管理员令牌（仅保留在当前页面内存）：
              <input id="token" type="password" autocomplete="off">
            </label>
            <nav>
              <button data-path="overview">Overview</button>
              <button data-path="leads">Leads</button>
              <button data-path="messages">Messages</button>
              <button data-path="unassigned">Unassigned</button>
              <button data-path="media">Media</button>
              <button data-path="tasks">Tasks</button>
              <button data-path="ai-executions">AI</button>
              <button data-path="smart-table-syncs">Smart Table</button>
              <button data-path="crm-syncs">CRM</button>
              <button data-path="conflicts">Conflicts</button>
              <button data-path="audits">Audit</button>
              <button data-path="config">Config</button>
              <button data-path="sales-authorizations">Sales Authorization</button>
            </nav>
            <pre id="result">选择一个查询页面。</pre>
            <script>
              // 只用 textContent 展示 API 结果，避免把服务端字段当作 HTML 执行。
              const output = document.getElementById("result");
              const token = document.getElementById("token");
              async function loadPage(path) {
                output.textContent = "加载中…";
                const response = await fetch(`/api/console/${path}`, {
                  headers: { "X-Console-Admin-Token": token.value }
                });
                const body = await response.json();
                output.textContent = JSON.stringify(body, null, 2);
              }
              document.querySelectorAll("button[data-path]").forEach((button) => {
                button.addEventListener("click", () => loadPage(button.dataset.path));
              });
            </script>
          </body>
        </html>
        """
    )


router.include_router(console_api)
