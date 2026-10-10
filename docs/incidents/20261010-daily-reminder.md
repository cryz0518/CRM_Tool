# 20:00 每日提醒：故障结论

故障业务日：2026-10-09，Asia/Shanghai。核查只读，没有向企业微信发送消息或补发旧提醒。

## 实际根因

20:00 对应 UTC 12:00。该窗口日志确认 Beat 发出 `workers.schedule_daily_unsubmitted_lead_reminders`，worker 收到任务并成功返回 `0`，没有对应每日提醒 `notification_records`；pending、succeeded、suppressed、retrying、failed 均无记录。故障发生在收件人筛选阶段，尚未进入通知出箱与发送。

数据库目录共 23 个成员，均启用且非管理员；历史兼容标记 `is_authorized` 为 true 的有 21 个，false 的有 2 个，全部 `crm_user_id` 为空。旧提醒候选查询和发送前复核都要求这个数据库映射，因此全部成员被错误排除。当前 CRM 提交实际通过智能表格负责人名称及 `employee.csv` 解析身份，旧数据库字段已不能代表提交能力。

此外，提醒的两处筛选仍残留 `is_authorized` 门槛，与已合并的 [PR #74](https://github.com/cryz0518/CRM_Tool/pull/74) 不一致。该 PR 已取消普通业务销售授权要求，新成员自动登记时该兼容字段为 false。残留条件也会错误排除正常启用的新成员；本次同时删除排程与发送前复核的旧授权门槛，不恢复销售授权规则。

按截至 20:00 的消息和同步时间只读筛选，当前保留事实包含：历史标记 true、pending_create / assigned / 最新 create 不存在的线索 3 条；同条件但解析 processing 的关联 2 条；历史标记 false、pending_create / assigned 1 条；synced / assigned / 最新 create succeeded 2 条。历史授权标记仅用于说明原筛选错误，不决定业务资格。查询按关联状态分组，可能存在多条消息关联，不能相加当作独立总数。生命周期与成员标记是当前快照，数据库未保存该时刻完整快照，不能把当前值宣称为历史原样值。

本地 PostgreSQL 集成还复现：插入一条通知时驱动 `rowcount` 不能可靠表示新增数量，旧实现可能报告创建 0。已改为 `INSERT ... RETURNING`。这是创建数量统计及日志的缺陷；它不能解释此次完全没有通知记录，此次直接原因仍是旧映射筛选。

## 业务规则及运行环境

只要求成员已登记、启用、非管理员，有上海当日有效需求消息，存在 temporary/pending_create 线索，最新 CRM create generation 未 succeeded/abandoned。`is_authorized` 和旧数据库 `crm_user_id` 均不决定提醒资格；兼容字段、表名和外键保留，无需迁移。20:00 前不排队；任务迟到仍只处理当天。通知唯一键继续按销售和业务日生成；发送前再次检查启用状态、管理员状态、线索和业务日，次日过期通知 suppressed。管理员仍被排除，本次没有变更是否应提醒管理员的业务规则。

该时间窗口 scheduler、worker、wecom-bot 的日志未显示每日任务调度或执行中断。当前只读容器状态：worker 与 wecom-bot healthy；scheduler 进程运行，但健康检查持续超过 3 秒导致 unhealthy。20:00 调度已实际完成，因此该告警不能当作漏调度根因；也不能把当前 scheduler 宣称为完全健康。需在批准的维护窗口核对探针耗时、心跳及主机负载。

## 修复文件与回归

- `app/leads/reminders.py`：移除排程和发送前复核的旧授权及数据库 CRM 映射筛选；采用 RETURNING 准确报告创建数量，保留业务筛选和幂等。
- `app/notifications/outbound.py`：同步发送前复核的说明；仍调用统一资格函数和原发送重试流程。
- `tests/unit/test_daily_unsubmitted_lead_reminders.py`：北京时间 20:00、提前/迟到、资格不满足、旧授权标记 true/false 与映射有/无的四种组合均成功排程及假客户端发送；排程后停用或转管理员仍 suppressed；最新 create generation、重复/并发调度、发送失败重试及跨日 suppressed。
- `tests/unit/test_wecom_bot_adapter.py`：新增语音仅文字、仅音频、双来源和重复投递回归使用 `is_authorized=False` 的启用成员，确保接入不恢复授权门槛。
- `tests/integration/test_server_regressions_postgres.py`：真实 PostgreSQL 使用 `is_authorized=False`、无旧映射的启用成员，并发排程只创建一条通知，并准确累计为 1。

## 历史恢复

不补发 10-09 提醒，不创建伪造的昨日通知。部署后仅观察当日真实 20:00 排程与新通知状态；如迟到执行，仍复核当天条件。已有通知失败重试沿用既有有限重试及发送前资格检查，不把提醒改成每天无条件发送。
