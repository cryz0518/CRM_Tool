# T03 智能表格权限 PoC

## 已验证结果

项目 Phase 0 已在管理员预配置的共享 `CRM线索` 企业微信智能表格中验证：关闭普通销售“新增记录”权限后，机器人仍可新增记录。机器人新增记录时写入当前销售的 `负责人`，记录保持销售本人可见的记录级权限语义。该结果是 T03 就绪检查要求 `sales_can_create_records=false` 的依据。

出于客户数据与企业微信标识保护，验证记录不在仓库保存真实 `doc_id`、`sheet_id`、`record_id` 或销售 `user_id`。`test_mock_records_robot_create_poc_when_sales_create_permission_is_disabled` 将此已验证平台行为固定为本项目 Mock 契约；未来真实 `WecomCliSmartTableAdapter` 必须保持相同语义。
