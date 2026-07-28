# SDK 2.0 protocol baseline

The 1.x source checkout declared `@beeos-ai/message-sdk` `1.1.0`, while
`agents/beeos-claw` declared `^1.1.1`. Version `2.0.0` keeps the caller-visible
message stream semantics needed by beeos-claw, but deliberately replaces the
legacy root client with the one unified `createMessageClient` factory.
Runtime-specific builders live on platform subpaths and remain composition
infrastructure rather than second client implementations.

The immutable npm `1.1.1` rollback/baseline artifact was verified as:

- registry integrity:
  `sha512-k/Qn29ye/2BFt2EukQ2aS7s8nmauyA9TC8aCSot+iahTcV2sTzaERnNAZN2IqdiVdI/wFOSTXX4YFCkAqyQpOQ==`;
- SHA1: `289ae359e7ce5c7bd6cfc5361a4ca972da87f76f`;
- downloaded tgz SHA256:
  `620a97bbde679aa69d36eba964e9b2119c8c18e22f51f04b58b13486f34c707c`.

Its exports are root, `/chat-envelope`, and `/auth`. The tarball contains the
legacy Node `MessageClient`, SSE, reducer, wire, and stream implementations.
beeos-claw migration preserves caller-visible `connect`/configuration,
message send/list/envelope, synchronous `startStream`, ordered UTF-8
`appendBody`, and opened/terminal/finalize/fail/cancel semantics through the
unified client. It does not preserve the second legacy core, personal realtime
delivery, SSE, or automatic fallback.

The new `src/protocol` modules are platform-neutral: they do not import
`node:*`, `ws`, Centrifuge, EventEmitter, React, or a platform storage API.
They define the cross-platform realtime contract before browser and React
Native adapters are introduced.

The 1.x automatic open-failure `sendV3` fallback has been removed. A
transport-ambiguous open or terminal write now raises `OutcomeUnknownError`
with the original idempotency key so the caller can explicitly reconcile.
