# @beeos-ai/message-sdk

BeeOS's unified conversation, message, runtime-method, and realtime client.

```bash
npm install --save-exact @beeos-ai/message-sdk@2.0.5
```

## One public client

Feature code imports one factory and one client type from the package root:

```typescript
import {
  createMessageClient,
  type MessageClient,
} from "@beeos-ai/message-sdk";

const client: MessageClient = createMessageClient(composition);
await client.connect();

const watch = client.conversations.watch(conversationId);
await watch.ready;

await client.messages.send({
  conversationId,
  clientMessageId,
  idempotencyKey,
  type: "chat_message",
  content: { text: "hello" },
});
```

The application composition root supplies narrow HTTPS, realtime-session, and
lifecycle ports. Raw channels, Centrifugo, EventSource, tokens, and transport
fallback are not part of the root API.

`watch()` is a local ref-count plus generation-fenced HTTP hydrate. It never
changes the physical WSS subscription. The server binds the one connection to
the caller's personal inbox; `conversationId` is only an event scope and local
`listen()` filter. The SDK owns projection, process-local eventId dedupe, and
HTTP recovery. UI and agent code consume `getSnapshot()`, `subscribe()`, and
filtered `listen()` events.

## Node Message Service composition

Node agents can use the explicit Message Service composition helper while
still creating the same root `MessageClient`:

```typescript
import { createMessageClient } from "@beeos-ai/message-sdk";
import {
  createNodeMessageClientComposition,
} from "@beeos-ai/message-sdk/node";
import {
  createTokenProvider,
} from "@beeos-ai/message-sdk/auth";

const client = createMessageClient(createNodeMessageClientComposition({
  identityId: "agent:42",
  tokenProvider: createTokenProvider({
    agentGatewayUrl: process.env.AGENT_GATEWAY_URL!,
    identity: myEd25519Identity,
  }),
}));

await client.connect();
```

This composition explicitly selects Message Service. It performs SDK-owned
durable private-inbox recovery from the authoritative open and closed conversation
directories. It does not fall back to Gateway or ACP. Unsupported MS commands
fail explicitly.

## React Native Gateway composition

Mobile composition roots use Gateway v1 without constructing EventSource or a
second Centrifugo client:

```typescript
import { createMessageClient } from "@beeos-ai/message-sdk";
import {
  createReactNativeMessageClientComposition,
} from "@beeos-ai/message-sdk/react-native";

const client = createMessageClient(createReactNativeMessageClientComposition({
  gatewayUrl,
  accessTokenProvider,
  currentPrincipal,
  lifecycle,
}));
```

The messaging-token response pins `currentPrincipal`, and the builder owns the
single physical server-bound personal WSS. There are no dynamic conversation
subscriptions and no realtime cursor/checkpoint store.
One login-scoped client is shared by all agents and conversations. Mobile
passes the explicit agent target through `listForAgent`, `create`, `watch`, and
`messages.send`; the composition's conversation-route registry rejects
missing or conflicting mappings and never infers them from events or authors.
`conversations.update` is rename-only and requires the caller's
`idempotencyKey`. Model changes use typed
`methods.execute({ operationId, target, method: "session/set_model", ... })`;
they are never converted into a generic conversation update.

## Streaming replies

`startStream()` returns synchronously. The POST result is available through
`opened()`, and every write uses a deterministic child idempotency key derived
from the caller-owned base key.

```typescript
const stream = client.messages.startStream({
  conversationId,
  clientMessageId: replyId,
  idempotencyKey: replyId,
  replyTo: inboundMessageId,
  type: "agent_reply",
  content: {},
});

const opened = await stream.opened();
if (opened.outcome !== "duplicate") {
  stream.appendBody("Hello ");
  stream.appendBody("world");
  stream.appendToolUse("call-1", "search", { query: "weather" });
  await stream.finalize({ stopReason: "end_turn" });
}
```

Body appends carry UTF-8 byte offsets. Writes are serialized and are never
automatically replayed after an uncertain response. An unknown outcome keeps
the original message ID and idempotency key; explicit retry reconciles first.

## Platform entrypoints

`@beeos-ai/message-sdk/web`, `/react-native`, and `/node` expose the same
`MessageClient` API and types. The React Native and Node subpaths additionally
export composition infrastructure; feature code still receives only the root
client. Protocol types and codecs are available from `/protocol`; agent
authentication helpers and credential types are available from `/auth`.

## Requirements

- Node.js 18 or later for the Node composition.
- An authoritative Message Service and authenticated realtime session.

## License

MIT.
