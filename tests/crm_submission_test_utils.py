"""CRM 提交测试使用的候选卡路径辅助函数。"""

from app.crm.service import (
    CrmSubmissionService,
    SubmissionBatchResult,
    SubmissionCommand,
)


def submit_today_via_selection(
    service: CrmSubmissionService, sales_user_id: str, request_message_id: str
) -> SubmissionBatchResult:
    """通过服务端候选列表和勾选结果执行 TODAY 测试提交。

    参数：service 为 CRM 提交服务；sales_user_id 和 request_message_id 为命令事实。
    返回值：仅处理服务端当前候选中的线索所得到的提交结果。
    异常：command 不是 TODAY 命令时抛出 ValueError；服务异常向调用方传播。
    副作用：读取服务端候选并执行所选线索的 CRM 提交流程。
    """

    command = SubmissionCommand("提交今天的线索", sales_user_id, request_message_id)
    candidates = service.list_submission_candidates(command.text, command.sales_user_id)
    return service.submit_selected(command, tuple(candidate.lead_id for candidate in candidates))
