# @beeos-ai/message-sdk

Node.js client for the BeeOS Message Service (IM channel-primitives + Centrifugo real-time + Ed25519 messaging-token provider).

Used by both `beeos-claw` and `device-agent` to receive `chat_message` envelopes from L0 / A2A / MCP / OpenAPI invocations and reply over `ch:{channelId}` with `agent_reply` / `agent_reply_delta` carrying `in_reply_to`.

## Install

```bash
npm install @beeos-ai/message-sdk
```

## Quick Start (agent — token via Agent Gateway)

```typescript
import {
  MessageSDK,
  createMessagingTokenProvider,
  extractChatPrompt,
  type MessagingIdentity,
} from "@beeos-ai/message-sdk";

const identity: MessagingIdentity = {
  publicKeyBase64: () => myKeyPair.publicKeyBase64,
  sign: (bytes) => myKeyPair.sign(bytes),
};

const tokenProvider = createMessagingTokenProvider({
  agentGatewayUrl: "https://agent-gw.beeos.ai",
  identity,
});

const sdk = new MessageSDK({}, console);
const tok = await tokenProvider("");

await sdk.connect(tok.principal_id, {
  centrifugoUrl: tok.centrifugo_url,
  tokenProvider,
});

sdk.onMessage("chat_message", async (envelope) => {
  const prompt = extractChatPrompt(envelope);
  if (!prompt) return;
  const reply = await runMyAgent(prompt.message);
  await sdk.sendToChannel(prompt.channelId, "agent_reply", { text: reply }, {
    inReplyTo: prompt.messageId,
  });
});
```

The Agent Gateway endpoint (`POST /api/v1/messaging/token`) enforces `principal_id == instance_id` (pod-is-principal) — the SDK ignores any `principal_id` you pass in and uses whatever the server returns. Always re-read `tok.principal_id` after the first call.

## Quick Start (server — token via Message Service REST)

For non-agent callers that hold a Message Service `X-API-Key`, omit `tokenProvider` and configure `serviceUrl`:

```typescript
const sdk = new MessageSDK(
  { serviceUrl: "https://msg.beeos.ai", centrifugoUrl: "wss://msg-ws.beeos.ai/connection/websocket" },
  console,
);
const tok = await sdk.getToken("user:42");
await sdk.connect("user:42", { centrifugoUrl: tok.centrifugo_url });
```

This path is for control-plane services and tests only — agents should always use Agent Gateway.

## API surface

### `MessageSDK`

| Method | Notes |
|---|---|
| `connect(principalId, opts)` | Opens Centrifugo WSS, server-side subscribes `personal:{principalId}` |
| `disconnect()` | Closes WSS, clears subscriptions |
| `onMessage(type, handler)` | Register a per-type dispatcher for inbound publications |
| `onceConnected(cb)` | One-shot ready callback (fires after `connected` event) |
| `sendToChannel(chId, type, payload, { inReplyTo, messageId, idempotencyKey })` | Centrifugo `publish` to `ch:{chId}`; falls back to REST when offline AND `serviceUrl` is set |
| `sendChannelMessage(chId, input)` | REST `POST /channels/{id}/messages` — durable, returns assigned offset |
| `replyToMessage(envelope, type, payload, opts)` | Convenience: pulls `channel_id` + `message_id` off the envelope |
| `sendToPersonal(targetId, type, payload)` | Centrifugo `publish` to `personal:{targetId}` |
| `waitForReply(chId, { inReplyTo, timeoutMs, since })` | REST `POST /channels/{id}/wait` long poll |
| `listChannelMessages(chId, opts)` | REST `GET /channels/{id}/messages` (history paging) |
| `getToken(principalId, claims?, ttlSeconds?)` | REST `POST /tokens` (REST-only) |
| `joinGroup(groupId, onMessage)` / `publishToGroup(...)` | Optional Centrifugo group channels |
| `isConnected` / `currentPrincipalId` | Read-only state |

### Helpers

- **`createMessagingTokenProvider({ agentGatewayUrl, identity })`** — returns a `TokenProvider` that calls `POST /api/v1/messaging/token` with Ed25519 headers.
- **`buildAgentAuthHeaders(method, path, identity)`** — exposed for callers that need to sign other Agent Gateway endpoints with the same scheme.
- **`extractChatPrompt(envelope)`** — normalizes the L0 / A2A `chat_message` payload into `{ message, channelId, messageId, contextId, sessionKey, targetAgentId, files }`.

## Wire format

The persistent envelope (`channel_messages` row) returned by `GET /channels/{id}/messages`, `POST /channels/{id}/wait`, and emitted on `ch:{channelId}` looks like:

```json
{
  "message_id": "<UUID>",
  "channel_id": "<channelId>",
  "type": "chat_message" | "agent_reply" | "agent_reply_delta" | ...,
  "payload": { ... },
  "publisher_id": "<principalId>",
  "in_reply_to": "<message_id of the request>",
  "offset": 42,
  "created_at": "2026-05-03T17:00:00.000Z",
  "metadata": { "from": "...", "channel_id": "...", "timestamp": "..." }
}
```

Reply types and `in_reply_to` are mandatory for the caller's `POST /channels/{id}/wait` to match. Use the constants from `backend/pkg/convmsg/extract.go` as the canonical type vocabulary (`chat_message`, `agent_reply`, `agent_reply_delta`, `a2a_streaming_delta`, `task_status`, …).

### Agent reply payload shape (must match for full parity)

Agents that want to be invoked uniformly by OpenAPI / A2A / MCP / L0 — and produce wire frames that match what beeos-claw produces — MUST use these payload shapes:

**`agent_reply_delta`** — streaming chunk:

```json
{
  "session_key": "agent:<agentId>:invoke:ctx-<contextId>",
  "update": {
    "sessionUpdate": "agent_message_chunk",
    "content": { "type": "text", "text": "<delta>" }
  }
}
```

`sessionUpdate` may also be `agent_thought_chunk` for hidden reasoning tokens — server-side `extractNestedDelta` filters those out so reasoning never leaks into the visible reply.

**`agent_reply`** — terminal:

```json
// success
{ "session_key": "agent:<agentId>:invoke:ctx-<contextId>", "text": "<final>" }
// failure
{ "session_key": "...", "error": "<errCode>", "text": "<optional>" }
```

`session_key` is forward-compatible — no current backend consumer reads it (verified via `rg session_key backend/`), but emitting it preserves byte-for-byte parity with beeos-claw and keeps the door open for downstream observers (dashboards, A2A SSE projection) that may want to group chunks by logical turn.

Use `extractChatPrompt(envelope)` to derive a canonical `sessionKey` on the inbound side — it returns `agent:{localAgentId}:invoke:ctx-{contextId}` when the L0 chat envelope carries a `context_id`, or `agent:{localAgentId}:invoke:ch-{channelId}` as a fallback.

## Requirements

- Node.js >= 18
- Centrifugo 5.x (server-side; the SDK speaks the v5 protocol via `centrifuge@^5`)

## License

MIT
