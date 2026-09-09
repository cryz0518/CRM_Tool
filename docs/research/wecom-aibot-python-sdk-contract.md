# T04 企业微信 AI 机器人 Python SDK 契约核验

核验日期：2026-09-09。范围仅限企业微信官方 PyPI 发布包、WeComTeam 官方 GitHub 源码；未读取 `.env`，本文不含任何凭据。

## 结论

T04 的 SDK/机器人长连接契约不存在 External Blocker（外部阻塞项）。应锁定并使用：

```toml
wecom-aibot-python-sdk==1.0.2
```

- PyPI 包名为 `wecom-aibot-python-sdk`，发布版为 `1.0.2`，要求 Python `>=3.8`；导入名是 `aibot`。[官方 PyPI 发布页](https://pypi.org/project/wecom-aibot-python-sdk/)
- 官方 GitHub `master` 的 `pyproject.toml` 仍标记 `1.0.1`，不能据此选择版本；以 PyPI 正式发布包 `1.0.2` 为准。[官方仓库配置](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/pyproject.toml)
- 本项目环境变量固定映射为 `WECOM_BOT_ID -> bot_id` 与 `WECOM_BOT_SECRET -> secret`。SDK 示例中的 `WECHAT_BOT_*` 只是示例应用的环境变量命名，并非 SDK 契约。

## 已确认的接入契约

| 事项 | T04 应使用的准确契约 | 官方证据 |
| --- | --- | --- |
| 初始化 | `from aibot import WSClient, WSClientOptions`；`WSClient(WSClientOptions(bot_id=..., secret=...))`。构造后，SDK 在建立 WebSocket 后发送 `aibot_subscribe`，其 body 为 `bot_id`、`secret`。 | [Python SDK client.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/aibot/client.py)、[ws.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/aibot/ws.py) |
| 启动与关闭 | 已有应用生命周期中使用 `await client.connect()`；关闭时调用同步的 `client.disconnect()`。不要用 `client.run()`，它会创建并独占事件循环。`connect()` 只表示 WebSocket 建立流程已启动，认证成功以 `authenticated` 事件为准。 | [官方 basic.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/examples/basic.py)、[client.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/aibot/client.py) |
| 文本事件 | 注册 `@client.on("message.text")`；回调参数是完整 `frame`。文本消息满足 `frame["cmd"] == "aibot_msg_callback"`、`frame["body"]["msgtype"] == "text"`，正文为 `frame["body"]["text"]["content"]`。 | [Python message_handler.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/aibot/message_handler.py)、[官方 Node 协议类型](https://github.com/WecomTeam/aibot-node-sdk/blob/master/src/types/message.ts) |
| 发送者 ID | `frame["body"]["from"]["userid"]`。 | [官方 Node 协议类型](https://github.com/WecomTeam/aibot-node-sdk/blob/master/src/types/message.ts) |
| 消息 ID | `frame["body"]["msgid"]`，官方定义为“本次回调的唯一性标志，用于事件排重”。 | [官方 Node 协议类型](https://github.com/WecomTeam/aibot-node-sdk/blob/master/src/types/message.ts) |
| 文本回复 | `await client.reply(frame, {"msgtype": "text", "text": {"content": text}})`。`reply` 自动从入站帧透传 `headers.req_id`，并以 `aibot_respond_msg` 发送；T04 不需要流式回复。 | [Python SDK client.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/aibot/client.py)、[官方 WebSocket 协议实现](https://github.com/WecomTeam/aibot-node-sdk/blob/master/src/ws.ts) |
| 心跳与重连 | 由 SDK 负责，不应由应用重复实现：认证成功后默认每 30 秒 `ping`；连续 2 次未收到 ack 则关闭连接；非人工断线按 1/2/4/... 秒退避、最多 30 秒，默认最多 10 次（`-1` 为无限）。 | [Python SDK ws.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/aibot/ws.py)、[SDK README](https://github.com/WecomTeam/wecom-aibot-python-sdk#%EF%B8%8F%E9%85%8D%E7%BD%AE%E9%80%89%E9%A1%B9) |
| 状态与异常 | 监听 `connected`、`authenticated`、`disconnected(reason)`、`reconnecting(attempt)`、`error(error)`；读取 `client.is_connected`。回复的服务端非零 `errcode`、回复回执超时或未连接会使 `await reply(...)` 失败。 | [Python SDK client.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/aibot/client.py)、[Python SDK ws.py](https://github.com/WecomTeam/wecom-aibot-python-sdk/blob/master/aibot/ws.py) |

Python SDK 的消息处理器未为回调 body 提供静态数据类，而是将原始 `dict` 透传。因此 `from.userid` 与 `msgid` 依据企业微信官方 Node SDK 的协议类型读取；该 Python SDK 在发布包 README 中明确声明自己是该官方 Node SDK 的 Python 等价实现。[Python SDK README](https://github.com/WecomTeam/wecom-aibot-python-sdk#wecom-aibot-python-sdk-python)

## 1.0.2 的实施边界

- `is_connected` 仅表示底层 WebSocket 为打开状态，并不代表认证已成功；健康状态必须另外记录 `authenticated` 事件。
- 发布版 `1.0.2` 在认证响应 `errcode != 0` 时会触发 `error` 事件，但没有暴露 `is_authenticated` 属性或专用 Python 异常类型。应用应将该状态记录为未就绪，不得把它误报为可接收消息。
- SDK 已拥有运行期心跳和断线重连；部署层只负责进程启动、优雅关闭和把连接状态暴露给健康检查。

## 唯一剩余部署验证边界

这不是 SDK 契约阻塞项：使用实际但不输出的 `WECOM_BOT_ID` / `WECOM_BOT_SECRET` 启动后，必须在目标企业微信环境确认收到 `authenticated`，再发送一条测试文本并核对 `body.msgid`、`body.from.userid` 与 `body.text.content`。在该验证完成前，运行状态应为“未验证”，而不是“接口未知”。
