# Message SDK 2.0.0 Frozen Contract

Status: frozen for the 2.0.0 implementation. Changes require an explicit
protocol decision and matching reducer/recovery vectors.

## Public surface

Applications import one client and one client type from the package root:

```ts
import { createMessageClient, type MessageClient } from "@beeos-ai/message-sdk";
```

The same client instance exposes `conversations`, `messages`, `methods`,
`listen`, `connect`, `disconnect`, `getSnapshot`, and `subscribe`. Platform,
Centrifugo, channel names, tokens, EventSource, ACP, and fallback selection are
not public configuration. `messages.send` and `methods.execute` remain distinct
typed commands.

`conversations.update` is the rename command and requires a caller-owned
`idempotencyKey`. Model changes are runtime operations:
`methods.execute` carries caller-owned `operationId`, `instanceId`, typed
`target`, method, params and idempotency key. `session/set_model` requires a
conversation target; `session/clear` and `session/cancel` are not silently
routed through the generic methods endpoint. Operation HTTP and WSS snapshots
share the strict full projection contract, and restart discovery uses
`methods.listActive(instanceId, cursor?)`.

The SDK may expose narrow TypeScript ports for composition roots. Application
features depend on the smallest port they consume:

- conversation query/command;
- message query/command/stream writer;
- runtime method;
- realtime session;
- projection store;
- lifecycle.
- current-principal identity.

These are dependency-inversion seams, not a second client implementation.

The `/node` and `/react-native` subpaths expose composition-root builders, and
`/auth` exposes an agent credential helper. Their token provider and WSS URL
fields are infrastructure inputs consumed inside the composition root; they do
not add a business client, raw channel, arbitrary subscription, publish method,
or transport selector to the root API. Token response routing metadata is
ignored rather than surfaced to callers.

React Native owns one login-scoped `MessageClient` and WSS across multiple
agents and conversations. It seeds the composition-only route registry through
explicit `listForAgent(agentId)`, create, watch and send targets. A duplicate
conversation ID mapped to another agent, a Gateway DTO with the wrong agent,
or an unbound hydrate/mutation fails closed. Login has no global agent
directory: an unmapped private inbox availability fact remains pending until
an explicit agent-scoped list/watch binds it; the SDK does not guess or
subscribe from that fact alone.

## Realtime event

`RealtimeEventV1` is the only realtime envelope. Its server-authored fields are
`schemaVersion`, stable `eventId`, `type`, `scope`, `actor`, `ordering`,
`correlation`, `occurredAt`, and typed `data`.

The producer, never the SDK, supplies:

- conversation/message/operation identifiers;
- `streamSequence`, entity revision, offset, projection epoch and history
  generation where applicable;
- request/correlation/causation identifiers and idempotency-key hash.

Malformed or incomplete envelopes fail closed. The SDK never guesses ordering
or correlation data.

## Projection ownership

The SDK owns the durable domain projection:

- conversations, including revision and `historyGeneration`;
- messages, including offset, revision, generation, optimistic state and
  `outcome_unknown`;
- operations, including revision, progress and terminal state;
- the latest committed offset.

UI stores may derive display state only. They do not own recovery or replay.
Projection snapshots have stable identity until a committed state change.
Audience cursors are SDK-internal recovery state. `ProjectionStore` loads and
commits one `{ projection, cursors }` checkpoint; an adapter must write that
checkpoint atomically in one storage transaction. A cursor is admitted before
event dedupe logically, but it is never durably advanced separately from the
projection it covers. Cursors are intentionally absent from the public
snapshot; neither raw realtime namespaces nor transport scope keys leak into
feature state.

Duplicate `eventId` is ignored. Older offsets/revisions are ignored for the
projection. A new ordered lifecycle event may still be dispatched to
`listen()` when its full snapshot is a semantic no-op (for example a terminal
fact following an already-terminal created snapshot). Events from an older
history generation are ignored. Committing a new conversation generation
atomically removes messages from prior generations, so clear history can never
be re-exposed.

The canonical message author is only `data.message.senderId`. `event.actor`
identifies the mutation causer and delivery audience identifies transport
scope; neither is an author fallback. Optimistic messages stamp the
composition's current principal as `senderId`. `messages.isMine(...)` compares
that viewer-local principal without persisting an `isMine` flag.
Node `startStream` does not create an optimistic agent-authored row. The server
validates and writes the canonical agent `senderId`; HTTP/WSS projection then
overwrites local state. Runtime binding identity and agent authorship are
separate concepts, and `isMine` is never used as an execution gate.

## Watch and recovery

`conversations.watch(id)` is ref-counted and singleflight. The first reference:

1. registers the hidden conversation subscription on the existing physical
   WSS and starts buffering;
2. reads conversation `G1`;
3. pages messages until `hasMore` is false;
4. reads conversation `G2`;
5. discards the read and retries when `G1.historyGeneration !==
   G2.historyGeneration`, for at most two complete attempts;
6. atomically commits conversation, messages, generation and latest offset;
7. merges buffered newer events through the same reducer;
8. marks the watch ready.

The last release removes the hidden logical subscription. It does not create or
destroy another physical WSS. Concurrent watch/hydrate requests share one
recovery operation.

The SDK is the only recovery owner and the realtime adapter is the only WSS
connection owner. No `/sync` or `/api/v2/realtime/session` endpoint is used.
Recovery uses existing conversation and paginated message reads. Transport
failure fails on the selected route; there is no SSE/WSS, Gateway/MS,
OpenClaw/ACP, or other automatic fallback.

## Commands and uncertain outcomes

Caller-owned `clientMessageId`/idempotency keys are stable. The SDK never
generates a replacement key during recovery or retry.

For Message Service v2/v3, `clientMessageId`, `idempotencyKey`, and the
authoritative `messageId` are the same stable value. Commands with different
client/idempotency values fail before transport; a receipt with a different
message ID is a server contract violation. This makes optimistic, HTTP, and
WSS race reconciliation unambiguous without inventing a client-side mapping.

An HTTP request that may have reached the service but has no authoritative
response becomes `outcome_unknown`. It is not automatically replayed. Explicit
retry first reconciles by the same idempotency key, then reuses that key.

## Realtime authority and security

One `MessageClient` owns one physical WSS. The server binds a private
control/inbox scope. Ordinary conversation events are delivered only on the
server-authorized `conv:<conversationId>` namespace; they are not normally
duplicated onto the private scope. Conversation watches accept only a
conversation ID and manage those hidden logical subscriptions on the existing
WSS. A server authorization rejection fails `watch.ready` closed. Raw
namespace names, arbitrary subscribe, client publish, transport tokens, and
foreign subscriptions are not part of any public interface.

The ordinary conversation delivery topology is exact:

- one durable mutation creates one `RealtimeEventV1` and one outbox intent;
- that intent produces one `conv:<conversationId>` publish;
- the sender observes the same publish through its own conversation echo;
- there is no participant-private fan-out, queue fallback, or second runtime
  (`rt:*`) intent for the same conversation event.

Each private or conversation audience has its own monotonic cursor. The SDK
admits a newer audience cursor before applying `eventId` and entity
offset/revision dedupe, while persisting that cursor only in the same atomic
checkpoint as the projection it covers. Message Service regression tests pin
one conversation intent, zero personal/queue publishes, and sender/receiver
consumption of the identical `eventId`.

The private scope has two minimal directory controls:
`inbox.conversation.available { conversationId }` and
`inbox.conversation.unavailable { conversationId }`. Availability triggers an
SDK-internal authorized `conv:<id>` subscription before generation-fenced
hydrate. Unavailability triggers an authoritative private-directory HTTP
refresh before unsubscribe/removal; the control event alone never deletes
projection state. These controls are not conversation timeline events.

Destination selection remains outside this SDK and is always explicit:

- OpenClaw uses the long-lived Node WSS, authorized conversation projection,
  and the frozen three-key execution gate from producer metadata.
- AgentBay uses the existing
  `AgentMessageDeliveryService.Dispatch` gRPC route. Its outcomes are
  `ACCEPTED`, `ALREADY_ACCEPTED`, or `REJECTED`; an ambiguous dispatch is
  reconciled with `GetDispatch` using the same `deliveryId`. AgentBay never
  subscribes to a conversation and never owns a Node SDK WSS.

OpenClaw and AgentBay are mutually exclusive explicit destinations for one
inbound command. The producer does not double-send and neither destination
falls back to the other. AgentBay writes output back to Message Service over
HTTPS; only after that durable write does the conversation projection serve as
Mobile/UI timeline. There is no AgentBay target type, WSS route, business API,
or fallback in this SDK.

## Runtime dispatch signal

The canonical artifact is
`contracts/runtime-dispatch-contract-v1.schema.json`, revision
`beeos.runtime-dispatch.v1`, SHA256
`6e4f6b6a60433ffe75ed2d2902c20ad651ac2058026819384d8a74a4a8b3525b`.
Its content and hash are release inputs.

`runtime.dispatch.failed` is an exact, provider-neutral, ephemeral conversation
signal. It is service-authored, scoped to tenant/conversation/message, uses
`streamSequence: "0"` plus `completeness: "delta"`, and carries only
`failed` with a frozen failure code or `unconfirmed` with
`delivery_unconfirmed`. It is delivered to `listen()` only: it never advances
an audience cursor, enters the reducer/checkpoint/SQLite projection, or
participates in offline recovery. It is not an OpenClaw execution command.

The message-creation HTTP receipt may omit `runtime_dispatch`. When present it
must decode exactly; an incompatible status/code pair or extra field is a
protocol error, not `outcome_unknown`. The sending device learns this receipt
from its HTTPS response. Other devices of the same author learn the
provider-neutral failure hint from the WSS signal; all devices still determine
message ownership only from canonical `senderId`.

## Node runtime delivery

`client.methods.consume(...)` is a Node composition capability on the same
`MessageClient`; it does not create another client or WSS. WSS is only a wake
signal. Durable read, renew, acknowledgement, history, and append remain HTTPS
truth. The Node adapter owns credential expiry/skew caching, token
singleflight, one 401 refresh/retry, origin pinning, transport backoff,
lease-fence abort, and ambiguous append history reconciliation. It never
replays an ambiguous append.

The composition root supplies the runtime lease authority and a static scoped
delivery key. Neither raw credential nor key reaches the delivery handler;
handlers receive only the current lease/epoch/journal fence and an abort
signal. OpenClaw retains grant/JWKS verification, explicit command target and
epoch gates, poison journal, Core dispatch, and business recovery. AgentBay is
not a consumer of this capability.

## Release gate

The package remains version `2.0.0` until the same `npm pack` tarball passes SDK,
Mobile, and beeos-claw joint validation. Publishing, tags, commits, pushes, and
PR changes are outside this implementation step.
