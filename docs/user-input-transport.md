# Gateway composition 的通用消息传输

基于 main 94cade7，Web / React Native 的 messages.send 与 Node composition 一样保留通用 type/content。不注册 Question 类型，不解析 schema、单选、多选或答案，不设业务消息白名单。

所有类型使用同一 {type, content, idempotency_key} 请求体；完整 JSON 原样序列化，不转成聊天 message 文本，也不剔除 mentions 等业务字段。发送者权限和产品边界由 Gateway 校验；明确 HTTP 拒绝不转换为 outcome unknown。

配套产品 Gateway 接受 chat_message 和用户 namespace user.*，要求 content 为对象以写入当前运行路由 metadata。MS/Node SDK 仍可承载其他 JSON 值；Web Gateway 的产品入口不是直接 MS 裸接口。SDK 本身不解释 Question schema。

旧 {message, mentions, ...} HTTP 客户端由后端兼容；新版 SDK 的 {type:chat_message,content:{text,mentions,...}} 由 Gateway 规范化到原有聊天/附件/模型/queue/browser 路径。后端 #1396 必须先部署，再发布 SDK #33；旧后端仅认 message，不能兼容新版 typed chat。SDK 不自动尝试第二种格式或重复发送。

SDK 成功仅代表投递；问答 schema、原生接受、生命周期、取消和卡片由业务消费者负责。现有 OpenClaw #469 的 beeos.user_input.v1 与用户提供的 beeos.question.v1 不同，本次通用传输改造不等于该问答协议一致性通过。

验证：152 tests passed / 1 原有 skipped，npm run build 通过。包括聊天、未知 user.*、任意 JSON 序列化、完整数组、幂等键和服务器拒绝；另通过实际构建 SDK → 实际 Go Gateway handler → 本地 MS HTTP fixture 的跨仓库测试。本次不发布 npm、不更新产品依赖、不部署。
