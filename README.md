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
| **Message** | `client.messages.{send, sendAndWait, list}` |
| **Identity** | `client.identities.{send, presence, conversations}` |
| **Subscription** | `client.connect()` + `client.on("message", …)` (personal channel) |
| | `client.conversations.subscribe(id)` (group channel) |

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
