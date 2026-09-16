"""Operations Console HTTP 路由，仅编排认证和只读模块。"""

from __future__ import annotations

from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from app.console.auth import (
    AdminCredentials,
    AdminIdentityProvider,
    AdminPrincipal,
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
    get_console_query_service,
)
from app.console.dto import (
    ConsoleAIExecutionDTO,
    ConsoleAttachmentDTO,
    ConsoleAuditEventDTO,
    ConsoleConfigIssueDTO,
    ConsoleConflictDTO,
    ConsoleLeadDTO,
    ConsoleMessageDTO,
    ConsoleOverviewDTO,
    ConsolePage,
    ConsoleSyncDTO,
    ConsoleTaskDTO,
)
from app.console.queries import ConsoleQueryService

router = APIRouter()
console_api = APIRouter(prefix="/api/console", tags=["Operations Console"])


class BreakGlassRequestBody(BaseModel):
    """定义单对象 Break-glass HTTP 请求，不允许额外批量字段。"""

    model_config = ConfigDict(extra="forbid")

    object_type: str
    object_id: str
    access_type: str
    reason: str = Field(min_length=1, max_length=512)


def _require_console_admin(
    request: Request,
    provider: Annotated[AdminIdentityProvider, Depends(get_admin_identity_provider)],
) -> AdminPrincipal:
    """认证并授权普通 Console 读取请求。

    参数：request 为 HTTP 请求；provider 为管理员身份 Provider。
    返回值：已通过 Console 读取能力校验的管理员主体。
    异常：认证缺失返回 401；认证主体无权返回 403。
    副作用：不读取业务数据。
    """
    principal = provider.authenticate(
        AdminCredentials(token=request.headers.get("X-Console-Admin-Token"))
    )
    if principal is None:
        raise HTTPException(status_code=401, detail="需要管理员认证")
    if not provider.authorize(principal, ConsoleCapability.CONSOLE_READ):
        raise HTTPException(status_code=403, detail="没有 Console 读取权限")
    return principal


def _require_audit_admin(
    request: Request,
    provider: Annotated[AdminIdentityProvider, Depends(get_admin_identity_provider)],
) -> AdminPrincipal:
    """认证并授权审计查询请求。"""
    principal = provider.authenticate(
        AdminCredentials(token=request.headers.get("X-Console-Admin-Token"))
    )
    if principal is None:
        raise HTTPException(status_code=401, detail="需要管理员认证")
    if not provider.authorize(principal, ConsoleCapability.AUDIT_READ):
        raise HTTPException(status_code=403, detail="没有审计读取权限")
    return principal


def _require_break_glass_capability(
    provider: AdminIdentityProvider,
    principal: AdminPrincipal,
    access_type: str,
) -> None:
    """将访问类型映射到最小 Break-glass 能力并执行授权。"""
    capability_map = {
        "view_raw_message": ConsoleCapability.BREAK_GLASS_RAW_MESSAGE,
        "view_full_contact": ConsoleCapability.BREAK_GLASS_FULL_CONTACT,
        "preview_attachment": ConsoleCapability.BREAK_GLASS_PREVIEW_ATTACHMENT,
        "download_attachment": ConsoleCapability.BREAK_GLASS_DOWNLOAD_ATTACHMENT,
    }
    capability = capability_map.get(access_type)
    if capability is None or not provider.authorize(principal, capability):
        raise HTTPException(status_code=403, detail="没有 Break-glass 访问权限")


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


@console_api.post("/break-glass/access")
def break_glass_access(
    body: BreakGlassRequestBody,
    request: Request,
    principal: Annotated[AdminPrincipal, Depends(_require_console_admin)],
    provider: Annotated[AdminIdentityProvider, Depends(get_admin_identity_provider)],
    service: Annotated[BreakGlassAccessService, Depends(get_break_glass_access_service)],
) -> JSONResponse:
    """执行单对象 Break-glass 访问，审计失败时拒绝返回原始数据。"""
    _require_break_glass_capability(provider, principal, body.access_type)
    access_request = BreakGlassAccessRequest(
        operator_subject=principal.subject,
        operator_role=sorted(principal.roles)[0] if principal.roles else "unknown",
        auth_source=principal.auth_source,
        object_type=body.object_type,
        object_id=body.object_id,
        access_type=body.access_type,
        reason=body.reason,
        request_id=request.headers.get("X-Request-ID", str(uuid4())),
        request_context={"route": str(request.url.path)},
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
