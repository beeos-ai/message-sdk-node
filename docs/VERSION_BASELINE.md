# SDK 2.0 protocol baseline

The 1.x source checkout declared `@beeos-ai/message-sdk` `1.1.0`, while
`agents/beeos-claw` declares `^1.1.1`. Version `2.0.0` treats the published
1.1.1 API surface as its compatibility baseline and keeps existing root/node
exports intact while introducing platform-specific subpath exports.

The new `src/protocol` modules are platform-neutral: they do not import
`node:*`, `ws`, Centrifuge, EventEmitter, React, or a platform storage API.
They define the cross-platform realtime contract before browser and React
Native adapters are introduced.

The 1.x automatic open-failure `sendV3` fallback has been removed. A
transport-ambiguous open or terminal write now raises `OutcomeUnknownError`
with the original idempotency key so the caller can explicitly reconcile.
