# 重复线索覆盖：故障结论与恢复边界

核查日期：2026-10-10。以下业务时间均为 Asia/Shanghai。仅执行服务器日志及数据库只读查询，未调用真实 CRM，更未重新覆盖目标线索。

## 实际记录与结论

| 节点 | 只读证据 |
| --- | --- |
| 首次提交 | 消息 `6cae09cd259c41f6cea868b748890653`，10-09 16:51:49，销售顺序号 184；批量动作 `7f9ab2cc-60a5-472f-8df6-69f3ec08b46d`，16:52:03 创建、16:52:31 完成，当时字段待完善 |
| 再次提交 | 消息 `adc5b4dcf73a6eac3fb7a51f5d41cf65`，16:53:00，顺序号 185 |
| 重复确认动作 | `2718d7c9-33c4-42e7-b871-f1898de93d2c`，16:53:28 创建，16:54:27 业务动作完成；动作成功表示确认流程完成，不代表 CRM 覆盖成功 |
| CRM Sync | `24`；Lead `d86e2749-89d1-4200-b053-e725cf769547`；`status=failed_pending_review`，`operation=update`，`attempts=1`，`failure_category=permanent`，`failure_kind=NULL`，`failure_code=NULL`，`failure_summary=SopCRMError` |
| 第一次确认回调 | delivery `61`，16:54:18；业务回调完成，卡片更新 `transport_status=failed`，错误码 `sdk_ack_errcode_42045` |
| 重复点击 | delivery `62`，16:55:07；被识别为已完成动作，没有再次执行卡片传输或远端覆盖 |
| 审计 | `1268` 接收再次提交消息，`1269` 等待重复确认，`1270` 确认覆盖；旧实现没有保存对应覆盖失败审计及协议分类 |

已复现的代码缺陷：本地 SOP 接入说明定义修改接口返回 `code=200, data=true/false`；旧 Adapter 不接受 Boolean `true`，会抛出“缺失 data”异常。因此一次真实成功覆盖可能被本地记为失败。旧代码还把空返回当成成功，且没有保存足够的失败分类。已同时修正这些结果判定。

**历史请求的真实失败原因仍不能从保留记录唯一还原。** 现有同步事实只保存异常类名，缺少当次响应及错误码，无法区分 Boolean 成功误判、编辑权限拒绝或其他错误。不能以本地 `failed_pending_review` 证明远端未修改，也不能断言上次一定成功。需要 CRM 管理员只读核对当次远端审计和当前字段。

卡片是独立问题：原卡是 `vote_interaction`，旧 callback claim 没有为重复确认生成冻结的同类型更新，更新路径可能回退到 `text_notice`；delivery 61 的实际 ACK 非零。已修正本地更新载荷的类型、task_id、question_key、选项和禁用状态。42045 的具体服务端原因没有进一步证据；真实客户端视觉效果留待批准后的受控验证。

## 修复文件与回归

- `app/crm/sop.py`：接受明确 Boolean 结果，拒绝空值和畸形结果；HTTP 错误不能伪装为成功。
- `app/crm/service.py`：持久化安全失败分类及协议码、失败审计；向销售返回受控中文解释。覆盖遇到超时、5xx 或未知响应时进入未知终态，停止重试；已尝试覆盖的过期租约不能再次写，重新提交及表格修改也不能绕过未知记录。
- `app/wecom_bot/actions.py`：重复确认冻结 `vote_interaction`，选项禁用；业务结果通知包含安全原因。卡片更新失败不回滚已接受的确认、不重新执行覆盖。
- `tests/unit/test_real_adapters.py`、`test_crm_submission.py`、`test_wecom_actions.py`：成功、明确拒绝、未知结果、空/畸形响应、5xx、重复点击、新提交绕过防护、ACK 42045。
- `tests/integration/test_server_regressions_postgres.py`：真实 PostgreSQL 并发确认只覆盖一次，已调用后的过期租约不重写。

销售未知结果文案为：“CRM 远端覆盖结果待核实，已停止自动重试；请联系管理员核对后处理，勿重复提交。”明确业务拒绝则提示管理员核对编辑权限及字段校验，始终不透传外部响应正文。

## 历史恢复

Sync 24 保持不动，不重放原动作或消息、不重置 attempts。先取得 CRM 只读核对结果：若已成功，审批后以原冻结快照对账并登记本地成功事实；若明确没有写入，核对编辑权限及提交身份后，再审批一个新的显式操作；若无法确认，继续待核实。卡片 ACK 故障不构成重复覆盖的理由。对账和任何新操作均需另外批准及审计，本次没有执行。

## PR #90 复审修正：旧版未知记录

旧版本可能把已调用远端的异常记为 permanent 且 failure_kind 为空，不能把这个分类当成“确定未写入”。现在统一查询冻结 update：除新版 unknown 外，failed_pending_review、attempts > 0 且 failure_kind NULL 的旧记录也阻止新覆盖。提交创建、提交更新、回读和查重后的持锁复核、最终认领入口均使用该保护，另一个新 Sync 或新确认也不能绕过。判定只读取原记录，不修改其分类、payload、attempts 或请求标识。

新增单测模拟 Sync 24，覆盖多次新提交、手机/公司名改变、新 Sync 和重复确认；断言更新调用数不增加。另验证明确 business_rejection 仍为 permanent，修正字段后新的显式确认可以提交；新版 unknown 的既有回归继续执行。隔离 PostgreSQL 增加旧分类记录的并发新提交验证。历史恢复仍必须先只读对账，无生产数据回填或自动重分类。
