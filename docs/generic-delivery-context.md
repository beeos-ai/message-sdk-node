# Generic delivery context

`SendMessageCommand.type/content` remain an opaque application contract. This SDK does not recognize Question schemas, answer shapes, pending lifecycles or provider handles.

- `contextMessageId`: generic user-ingress reference. Gateway authorizes the referenced original Agent message and copies producer context. It is not replyTo or an idempotency key.
- `deliveryContext`: immutable outer producer provenance for Runtime/service writes. Public Gateway rejects user-supplied values. HTTP, realtime and full recovery projections carry it without interpreting content.
- `conversations.refresh(id)`: await the existing RecoveryCoordinator's complete history hydration; it adds no physical connection or private cursor.
- Local optimistic/outcome_unknown projections retain contextMessageId as a UI correlation hint. They do not mint deliveryContext or prove application processing acceptance.

Normal chat commands keep their existing behavior. Generic user controls require a Gateway context reference; unknown content keys, scalars/arrays/null, and business metadata remain opaque. A processing receipt belongs to an application, not to SDK delivery state.

Validation: build and 156 tests pass, with one pre-existing skipped test; additional tests cover HTTP selectors, producer context, realtime/recovery preservation and unknown local references. The actual built SDK is exercised against the real Go Gateway with an MS HTTP fixture and pinned native OpenClaw loopback probes.

Release gate: this PR does not publish npm or change the package version. Consumers must adopt an independently published SDK version before enabling their new contextual application flows.
