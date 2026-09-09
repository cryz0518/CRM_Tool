# 首期以 wecom-cli 传输智能表格数据

首期真实 SmartTableAdapter 使用 `wecom-cli`，并由 Worker 通过 `WecomCliSmartTableAdapter` 执行；业务层只依赖稳定适配器接口。此决定复用当前可用的企业微信能力，同时保留 `WecomApiSmartTableAdapter` 作为未来替换实现，不将 CLI 绑定扩散到业务模块。
