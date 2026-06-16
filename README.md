# @beeos-ai/message-sdk

Node.js SDK for the BeeOS Message Service.

**Single unified client class — `MessageClient`** — covering both REST
(Conversations / Messages / Identities) and realtime (Centrifugo).
Mirrors the Go SDK (`backend/sdks/message-sdk-go`).

```bash
npm install @beeos-ai/message-sdk
```

## Quick start — service-side (REST only)

```typescript
import { MessageClient } from "@beeos-ai/message-sdk";

const client = new MessageClient({
  serviceUrl: "https://msg.beeos.ai",
  apiKey: process.env.MESSAGE_API_KEY,
});

const conv = await client.conversations.create({
  participants: ["service:gateway", "agent:42"],
});

const reply = await client.messages.sendAndWait({
  conversationId: conv.id,
  id: crypto.randomUUID(),       // idempotency key
  type: "chat_message",
  content: { message: "What's the weather?" },
}, 30_000);

console.log(reply.content);
```

## Quick start — agent-side (REST + realtime)

```typescript
import { MessageClient } from "@beeos-ai/message-sdk";
import { createTokenProvider } from "@beeos-ai/message-sdk/auth";

const client = new MessageClient({
  serviceUrl: "https://msg.beeos.ai",
  centrifugoUrl: "wss://msg-ws.beeos.ai/connection/websocket",
  tokenProvider: createTokenProvider({
    agentGatewayUrl: process.env.AGENT_GATEWAY_URL!,
    identity: myEd25519Identity,
  }),
});

await client.connect({ identityId: "instance-42" });

client.on("message", async (msg) => {
  console.log(`got ${msg.type} from ${msg.sender}`);
  // reply on the same conversation
  await client.messages.send({
    conversationId: msg.conversationId,
    id: crypto.randomUUID(),
    type: "agent_reply",
    content: { text: "ok" },
    replyTo: msg.id,    // IM thread anchor
  });
});

client.on("error", (err) => console.error(err));
```

## API surface

The 4-concept frame (Identity, Conversation, Message, Subscription)
mapped onto namespaced REST + realtime events:

| Concept | Surface |
|---|---|
| **Conversation** | `client.conversations.{create, get, close, wait, subscribe}` |
| **Message** | `client.messages.{send, sendAndWait, list, startStream, sendV3, getEnvelope}` |
| **Identity** | `client.identities.{send, presence, conversations}` |
| **Subscription** | `client.connect()` + `client.on("message", …)` (personal channel) |
| | `client.conversations.subscribe(id)` (group channel) |

### Personal vs group subscriptions (required reading for streaming)

The Message Service publishes on two Centrifugo channel families per
conversation:

- `personal:{identityId}` — fired on POST only (the initial
  `message.created` frame, plus inbound notifications addressed to you).
- `ch:{conversationId}` — fired on POST **and** every v3 PATCH frame
  (`message.updated`).

**If you want to follow a streaming reply live, you MUST call
`await client.conversations.subscribe(conversationId)`** in addition to
the default `client.on("message", …)` personal subscription. Subscribing
to `personal:` alone gives you only the first frame of a streaming
message — the per-chunk `message.updated` PATCHes are intentionally not
mirrored to personal channels (one delta × N participants would amplify
traffic with no upside). Backend gateways (Agent / A2A / OpenAPI) get
this for free via their SSE proxy on
`GET /api/v2/conversations/{id}/stream`, which subscribes to `ch:` on
the caller's behalf; direct SDK consumers must do it explicitly.

See [`backend/services/message/docs/channel-primitives.md`](https://github.com/beeos-ai/beeos/blob/main/backend/services/message/docs/channel-primitives.md#centrifugo-fan-out--subscription-model) for the canonical fan-out
contract.

## Streaming agent replies — Envelope v3

`client.messages.startStream(...)` opens a mutable v3 message envelope:
each `appendBody(chunk)` ships an append-only `{ body_append, body_from }`
PATCH immediately (no buffer, no debounce — ADR-0025), and a single
terminal PATCH closes the row. See the cadence note below for the only
coalescing (in-flight back-pressure).

```typescript
const s = client.messages.startStream({
  conversationId,
  id: crypto.randomUUID(),     // → Idempotency-Key + stream.id
  replyTo: incoming.id,
  type: "agent_reply",
});

s.appendBody("Hello ");
s.appendBody("world");
s.appendThinking("user wants a greeting");
s.appendToolUse("call-1", "search", { q: "weather" });
// ... more deltas as the LLM streams ...
await s.finalize({ stopReason: "end_turn" });
```

**`startStream` returns synchronously.** The underlying POST runs in the
background and is exposed via `await s.opened()`. There is **no buffer
and no debounce timer**: each `appendBody(chunk)` ships a
`{ body_append, body_from }` PATCH immediately (ADR-0025 append-only
delta wire), where `body_from` is the UTF-8 byte offset the chunk
attaches at. The only coalescing is back-pressure — chunks appended
while a PATCH is in flight are merged into the next one — so the SDK
preserves the agent's native output cadence (1 PATCH per chunk at most,
fewer under load) and a restart never loses already-emitted text.
Appends issued before the POST resolves are held in the same pending
buffer and ship on the first PATCH after open. `setBody` / `finalize`
ship the full cumulative body as a snapshot replace; a `409
append_offset_mismatch` self-heals by resending the whole body.

**Open-failure fallback.** If the underlying POST fails (network blip,
4xx, 5xx), the terminal call (`finalize` / `fail` / `refuse` / `cancel`)
automatically falls back to a single-shot `sendV3` POST that creates a
NEW envelope row with the terminal state pre-applied. `/wait` and SSE
consumers still see a terminal frame — the open failure does not leave
the conversation in an indefinite "still streaming" gap. Fallback
itself failing is warn-only via the `onError("terminal")` hook (no
re-throw), matching the at-least-once semantics of the rest of the
streaming layer.

**`stopReason` whitelist.** `finalize({ stopReason })` accepts only the
five values that map cleanly onto `state="completed"` —
`end_turn` / `max_tokens` / `tool_use` / `timeout` / `content_filter`.
Anything else silently falls back to `end_turn` so an upstream vocabulary
mismatch (e.g. Anthropic's `stop_sequence`) never 400s the terminal
PATCH. `fail` / `refuse` / `cancel` have their own dedicated terminal
vocabularies (`error` / `refused` / `user_stop`).

## Delayed-connect mode

`MessageClient` can be constructed BEFORE its runtime config is known:

```typescript
// At process start, before the bootstrap handshake:
const client = new MessageClient();      // no serviceUrl / auth yet
const streamer = new MyStreamer(client); // can wire up dependents now

// ... later, once the handshake resolves:
client.setConfig({
  serviceUrl: "https://msg.beeos.ai",
  tokenProvider,
});
// Any REST calls issued before this point unblock here.
```

REST methods internally `await configReady` before issuing fetch, so
callers that fire requests during the boot window get queued instead
of failing with "missing serviceUrl". `setConfig` is single-write per
field: calling it again with a different value for `serviceUrl` /
`apiKey` / `tokenProvider` / `centrifugoUrl` throws — the SDK does not
support hot-swapping these in-flight. Adding a previously-missing field
is fine.

Service-side concerns (token issuance, webhook configuration) live in
the Go SDK only — the Node SDK is targeted at agent and gateway-proxy
consumers.

## Subpath imports

```typescript
import {
  extractChatPrompt,
  A2A_PROTOCOL_FIELDS,
} from "@beeos-ai/message-sdk/chat-envelope";

import {
  buildAgentAuthHeaders,
  buildSigningString,
  createTokenProvider,
  type Identity,
} from "@beeos-ai/message-sdk/auth";
```

These keep the root client surface small and let bundlers tree-shake
the agent-specific helpers out of service-side builds.

## Requirements

- Node.js >= 18 (built-in fetch + `node:crypto` Ed25519).
- For realtime: an Agent Gateway or Message Service that issues
  Centrifugo JWTs.

## License

MIT.
