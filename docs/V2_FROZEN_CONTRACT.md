# Message SDK 2.0.5 Personal Realtime Contract

Status: frozen for the personal-inbox/no-realtime-cursor rollout.

## Public client

Applications continue to use one `MessageClient` with `conversations`,
`messages`, `methods`, `listen`, `connect`, `disconnect`, `getSnapshot`, and
`subscribe`. `messages.send`, `methods.execute`, and `methods.consume` remain
the typed command surfaces. Raw channels, Centrifugo, publish, arbitrary
subscribe, ACP, and fallback selection are not public configuration.

## Personal transport

Each client owns one physical WSS whose single personal inbox is computed and
authorized by the server. Mobile/Web use `personal:user:<userId>` and runtime
instances use `personal:instance:<instanceId>`. The SDK never constructs a
channel and never calls `newSubscription("conv:" + conversationId)`.

`conversationId` exists only in `RealtimeEventV1.scope`. `listen()` filters and
`conversations.watch()` are local. A watch adds a ref-count and performs a
generation-fenced HTTP hydrate; it does not change transport subscriptions.

## Notification envelope

`RealtimeEventV1` contains `schemaVersion`, stable `eventId`, `type`, `scope`,
`actor`, `correlation`, `occurredAt`, and `data`. There is no transport
`ordering`, `streamSequence`, projection epoch, realtime cursor, audience
cursor, checkpoint, or sequence-gap rebase.

The decoder checks only the minimum envelope needed to avoid application
crashes. Unknown event types and additive fields are preserved. Known reducers
handle known data and ignore malformed known payloads without making the SDK a
second business-validation authority.

`personal.notification` is an additive event type: the Gateway composition
(web/desktop/mobile) normalizes any thin, envelope-less personal-channel frame
into this canonical shape at the transport boundary, before it reaches
`listen()`. It carries no cursor/sequence; receiving one only triggers the same
existing private-directory HTTP recovery as
`inbox.conversation.available`/`unavailable`. A frame that already arrives as a
well-formed `RealtimeEventV1` (e.g. `message.created`) is never rewrapped.

Message, conversation, and operation revisions live on those entities.
`historyGeneration` remains a conversation/message business fence for clear
history. It is not a realtime cursor.

## Projection and recovery

`eventId` de-duplication is a bounded process-local LRU and is never persisted.
Entity revisions reject stale notifications. A message body delta is applied
only at the exact `bodyFrom` boundary; a missing base or boundary mismatch
singleflights one HTTP hydrate for that conversation.

WSS is notification only. On reconnect the SDK hydrates the current private
conversation directory, watched conversations, and non-terminal operations
through existing HTTP query APIs. HTTP pagination tokens remain local to each
query loop. There is no SDK realtime `ProjectionStore` or cursor checkpoint.

HTTP 202 from `methods.execute` creates a non-terminal queued operation
projection; it is never treated as success. `operation.started`,
`operation.progress`, and `operation.terminal` update the stable operation
snapshot by revision. A successful `session/new` terminal result is typed as
`{ sessionId, conversationId }`.

No command is automatically retried after an ambiguous outcome. The caller's
operation/message identity and idempotency key are preserved.

The shipped runtime-dispatch artifact remains revision
`beeos.runtime-dispatch.v1`; its 2.0.5 SHA256 is
`a60bee8263d25bfe401b4820fb8cf8ca786f8a15c5605f7b6ec503bb108fb1a7`.

## Runtime delivery

`methods.consume(...)` retains durable HTTP claim/read/renew/ack/history
semantics and lease/epoch fencing. `operation.available` on runtime personal is
only a wake signal. This SDK does not add an ACP, Redis, conversation-channel,
or other fallback.
