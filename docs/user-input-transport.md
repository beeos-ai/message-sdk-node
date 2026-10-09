# Gateway composition 的 user.continue

基于 main `94cade7`，Web / React Native 共用的 Gateway composition 在现有 `messages.send` 中增加明确的 `user.continue` 透传。Node composition 保持现有通用 type/content 传输。

```ts
await client.messages.send({
  conversationId,
  agentId,
  type: "user.continue",
  idempotencyKey: answerMessageId,
  content: {
    schema_version: "beeos.user_input.v1",
    request_id: question.content.request_id,
    answers: { checks: { answers: ["中文,标签", "B C"] } },
  },
});
```

SDK 发给既有会话 messages endpoint 的 body 是 `{type, content, idempotency_key}`。选择数组原样保留，不转成 `message` 文本。普通 `chat_message` 保留现有 message、mentions、附件与模型配置路径；Gateway composition 拒绝其他 Agent 类型。

取消使用同样的请求 ID 和 `cancel: true`，省略 answers。问题及答案消息不设置 replyTo；保持原父回复负责结束运行。SDK 成功结果仅代表投递成功，原生接受状态以父流中的 `user_input_resolution` 为准。

SDK 不定义问题业务生命周期、不校验原生选项、不提供跨进程 waiter 恢复。新版 schema 的渲染、multi_select、提交态与原生接受态由客户端后续适配；不要把现有仅支持旧 `openclaw.user_input.v1` 的 radio 卡片直接启用为多选。

验证覆盖 Gateway、React Native 和 Node 的完整两项数组透传、稳定幂等键及普通聊天兼容；执行 `npm test` 与 `npm run build`。本次不发布 npm、不更新产品端 SDK 依赖或部署客户端。
