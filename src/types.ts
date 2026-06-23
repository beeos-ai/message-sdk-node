// Public wire shapes for `@beeos-ai/message-sdk`.
//
// Mirrors the Go SDK (backend/sdks/message-sdk-go) — same field names,
// same JSON shapes — so producers and consumers cannot drift. All
// public types use **camelCase TypeScript fields** mapped to the v2
// **snake_case JSON wire shape**; the client class handles the casing
// at the request/response boundary.

export interface Conversation {
  id: string;
  participants: string[];
  metadata?: Record<string, string>;
  owner_identity_id?: string;
  target_identity_id?: string;
  target_kind?: string;
  title?: string;
  last_activity_at?: string;
  state: "open" | "closed";
  closed_reason?: string;
  single_shot?: boolean;
  deadline_at?: string; // RFC3339
  created_at: string; // RFC3339
  closed_at?: string; // RFC3339
}

export interface CreateConversationInput {
  participants: string[];
  metadata?: Record<string, string>;
  ownerIdentityId?: string;
  targetIdentityId?: string;
  targetKind?: string;
  title?: string;
  /** Single-shot conversation: closes on first reply. */
  singleShot?: boolean;
  /** Deadline in milliseconds (server adds Date.now()). */
  deadlineMs?: number;
}

export interface Message<TContent = unknown> {
  /** Stable message id (== caller-supplied Idempotency-Key). */
  id: string;
  conversationId: string;
  type: string;
  content: TContent;
  sender: string;
  /** Set when this message replies to another. */
  replyTo?: string;
  /** RFC3339Nano timestamp. */
  createdAt: string;
  /**
   * Most recent PATCH timestamp. Present only when the message has been
   * updated after creation (Message Envelope v3 PATCH semantics). When
   * absent, the message was either pre-v3 or has never been patched.
   */
  updatedAt?: string;
  /**
   * v3 envelope cumulative main text. Populated by `MessageStream`
   * `appendBody` / direct PATCH; remains "" or undefined on legacy
   * pre-v3 rows.
   */
  body?: string;
  /**
   * v3 envelope structured parts (thinking / tool / file / source /
   * custom). Undefined on legacy rows; consumers MUST default-tolerate
   * unknown `part.type` values for forward compatibility.
   */
  parts?: Part[];
  /**
   * v3 envelope lifecycle state. When present this is the PRIMARY
   * terminal-detection signal: `"streaming"` keeps the conversation
   * stream open; the four terminal states (`completed`/`failed`/
   * `refused`/`cancelled`) close it. Undefined on legacy rows; clients
   * fall back to the legacy `type`-based classification.
   */
  state?: MessageState;
  /** Producer-supplied terminal reason, paired with terminal `state`. */
  stopReason?: StopReason;
  /**
   * The text THIS streaming frame appended to `body`, reconstructed by
   * the delta-wire reducer (ADR-0025). On a `message.delta` publication
   * it is the decoded chunk; on a full snapshot it is the suffix `body`
   * grew by since the previous frame (empty when the body did not grow
   * or was replaced non-monotonically). Consumers driving a typewriter
   * UI append `bodyDelta`; `body` stays the authoritative cumulative
   * text. Populated on the streaming consumer paths
   * (`ConversationSubscription` over WSS, `MessageStreamReader` over
   * HTTP SSE); undefined elsewhere.
   */
  bodyDelta?: string;
}

export interface SendInput<TContent = unknown> {
  conversationId: string;
  /**
   * Stable client-generated UUID forwarded as the `Idempotency-Key`
   * HTTP header. The server uses this as the persistent message id.
   * Callers SHOULD set this for safe retries; when absent the server
   * generates one.
   */
  id?: string;
  type: string;
  content: TContent;
  replyTo?: string;
  requireSubscriber?: boolean;
}

export interface IdentitySendInput<TContent = unknown> {
  identityId: string;
  /** Optional stable id; server picks one when absent. */
  id?: string;
  type: string;
  content: TContent;
  requireSubscriber?: boolean;
}

// =============================================================
// Envelope v3 (mutable body/parts/state) — plan
// `message-envelope-v3-kiss`.
//
// v3 introduces a per-message state machine (streaming → terminal) and
// a `parts` array for structured supplementary content. The body wire
// is APPEND-ONLY (ADR-0025): `appendBody(chunk)` ships an incremental
// `{body_append, body_from}` PATCH (body_from = UTF-8 byte offset);
// only `setBody` / the terminal write ship a full-body snapshot. SDK
// callers use `MessageStream` instead of hand-rolled PATCH calls; it
// serializes appends into an in-order stream of HTTP PATCHes with no
// time/char batching buffer.
// =============================================================

/** v3 lifecycle states. Terminal = anything other than "streaming". */
export type MessageState =
  | "streaming"
  | "completed"
  | "failed"
  | "refused"
  | "cancelled";

/**
 * v3 terminal stop reasons. Borrowed from Anthropic vocabulary
 * (`end_turn` / `max_tokens` / `tool_use`) and extended with
 * platform-level terminations.
 *
 * `agent_lost` is set by the Message Service stale-streaming reaper
 * when an agent crashes / disconnects without finalising its v3
 * envelope — the row is transitioned from `state=streaming` to
 * `state=failed` after the configured idle threshold so blocking
 * `/wait` callers and SSE consumers receive a terminal frame instead
 * of hanging indefinitely. Producers MUST NOT emit `agent_lost`
 * themselves; the server is the only authority that sets it.
 */
export type StopReason =
  | "end_turn"
  | "max_tokens"
  | "tool_use"
  | "user_stop"
  | "timeout"
  | "error"
  | "refused"
  | "content_filter"
  | "agent_lost";

/** Per-part lifecycle for thinking / tool_use. Other types ignore. */
export type PartState = "streaming" | "done";

/**
 * Discriminated union of the six recognized part shapes. `custom` is
 * the extension escape hatch — add new structural payloads under
 * `custom.kind` without a schema migration.
 */
export type Part =
  | { type: "thinking"; text: string; state?: PartState }
  | {
      type: "tool_use";
      id: string;
      name: string;
      arguments: unknown;
      state?: PartState;
    }
  | {
      type: "tool_result";
      tool_use_id: string;
      content: string | object;
      is_error?: boolean;
    }
  | {
      type: "file";
      url: string;
      mime_type: string;
      name?: string;
      size?: number;
    }
  | { type: "source"; url: string; title?: string; snippet?: string }
  | { type: "custom"; kind: string; data: unknown };

/**
 * Server snapshot returned by POST/PATCH/GET on the v3 endpoints.
 * Mirrors `V3MessageResponse` in
 * backend/services/message/pkg/infrastructure/server/http/dto_v3.go.
 */
export interface MessageEnvelope {
  id: string;
  conversationId: string;
  type: string;
  sender: string;
  replyTo?: string;

  body: string;
  parts?: Part[];
  state: MessageState;
  /**
   * Terminal stop reason on a non-streaming envelope. Empty for
   * `state="streaming"` and for legacy pre-v3 rows.
   *
   * Plan 5.2 (v3 envelope second review): the field is camelCase
   * `stopReason` here to mirror the existing `Message.stopReason`
   * naming on the streaming-side public type. The wire shape
   * remains snake_case `stop_reason`; the SDK remaps on ingress
   * inside `rawToEnvelope` / `envelopeToMessage`.
   */
  stopReason?: StopReason;

  /** Legacy v1/v2 payload mirror — v3 readers ignore. */
  content?: unknown;

  /**
   * True only on v3 POST when the server returned an existing row for
   * the same Idempotency-Key. Runtime connectors use this as a
   * lightweight processing claim and skip duplicate dispatch.
   */
  idempotent?: boolean;

  createdAt: string;
  updatedAt?: string;
}

/**
 * Options for `MessageClient.messages.startStream(...)`. The caller
 * supplies the conversation and (recommended) a stable client UUID;
 * everything else has sensible defaults.
 *
 *   - `type` defaults to `"agent_reply"` (the dominant use case).
 *   - `replyTo` is required for /wait callers; it threads the reply
 *     back to the inbound request.
 *   - `state` defaults to `"streaming"`. Pass `"completed"` to write
 *     a single-shot terminal envelope without PATCHing.
 */
export interface StartStreamInput {
  conversationId: string;
  /** Stable UUID for retries + PATCH addressing. Strongly recommended. */
  id?: string;
  /** Defaults to "agent_reply". */
  type?: string;
  replyTo?: string;
  body?: string;
  parts?: Part[];
  state?: MessageState;
  /**
   * Terminal stop reason. camelCase aligns with `Message.stopReason`
   * / `MessageEnvelope.stopReason`; the wire shape is still snake_case.
   * The SDK remaps inside `startStream` / `sendV3`.
   */
  stopReason?: StopReason;
  requireSubscriber?: boolean;
}

/**
 * Options for the MessageStream.
 *
 * @remarks
 * Streaming is now an **append-only delta wire** (ADR-0025): each
 * `appendBody(chunk)` ships the chunk immediately as a `body_append`
 * PATCH; there is no time/char/parts batching buffer anymore. The
 * `flushDelayMs` / `flushChars` / `flushParts` fields are retained as
 * accepted-but-**ignored** no-ops for source compatibility with older
 * callers; new code must not pass them. Send pacing is
 * back-pressure-driven: while a PATCH is in flight, appended chunks
 * coalesce into the next one — no timer, no fixed batch size.
 */
export interface MessageStreamOptions {
  /** @deprecated ignored since ADR-0025 (append-only delta wire); removed in the next major. */
  flushDelayMs?: number;
  /** @deprecated ignored since ADR-0025 (append-only delta wire); removed in the next major. */
  flushChars?: number;
  /** @deprecated ignored since ADR-0025 (append-only delta wire); removed in the next major. */
  flushParts?: number;
  /**
   * Observability hook for PATCH failures. Optional.
   *
   * - phase="midstream": a mid-stream body PATCH failed. The next
   *   write ships the cumulative snapshot and self-heals — the
   *   stream stays open and the SDK swallows the error. Use this
   *   hook to log / increment a counter; do NOT rethrow.
   *
   * - phase="terminal": the final PATCH from finalize() / fail() /
   *   cancel() did NOT land. Fires exactly when the awaiting caller
   *   also receives a thrown error, i.e. when the stream truly did
   *   not end cleanly. Specifically:
   *     - 4xx (channel closed / row already terminal / forbidden):
   *       fires once on the first (and only) attempt. Retrying a
   *       semantic failure is futile.
   *     - 5xx / network / parse: the SDK silently retries once after
   *       200ms. If that retry SUCCEEDS, onError does NOT fire — the
   *       snapshot landed, the stream ended cleanly, no observer
   *       action is needed. If the retry ALSO fails, onError fires
   *       once with the retry's error and the awaiting caller sees
   *       the throw.
   *
   *   Contract: every `phase="terminal"` invocation corresponds to a
   *   stream that did not end successfully — safe to wire to
   *   logger.warn / metric.failure without filtering out
   *   retry-rescued transient blips.
   *
   * Throwing from the callback is caught and silently dropped — the
   * stream's lifecycle MUST NOT depend on observer faults.
   */
  onError?: (err: Error, phase: "midstream" | "terminal") => void;
}

export interface WaitInput {
  conversationId: string;
  replyTo: string;
  timeoutMs?: number;
  /**
   * Server-side filter on `message.type`. When set, the server returns
   * the first message where `replyTo == replyTo AND type IN expectTypes`.
   * Empty / undefined = match any type.
   */
  expectTypes?: string[];
}

export interface ListOptions {
  cursor?: string;
  limit?: number;
  /**
   * Return only chat_message rows that have not yet been answered by this
   * agent identity. Server-side filter used by runtime recovery.
   */
  unhandledBy?: string;
}

export interface MessagePage<TContent = unknown> {
  messages: Array<Message<TContent>>;
  nextCursor?: string;
  hasMore: boolean;
}

export interface Presence {
  identity: string;
  online: boolean;
  platforms?: string[];
}

export interface ListConversationsOptions {
  state?: "open" | "closed";
  limit?: number;
  cursor?: string;
  targetIdentityId?: string;
  targetKind?: string;
}

export interface ConversationPage {
  conversations: Conversation[];
  nextCursor?: string;
  hasMore: boolean;
}

export interface ConversationFocus {
  owner_identity_id?: string;
  target_identity_id?: string;
  surface?: string;
  conversation_id?: string;
  metadata?: Record<string, string>;
  updated_at?: string;
}

export interface GetConversationFocusInput {
  ownerIdentityId: string;
  targetIdentityId: string;
  surface?: string;
}

export interface PutConversationFocusInput {
  ownerIdentityId: string;
  targetIdentityId: string;
  surface?: string;
  conversationId: string;
  metadata?: Record<string, string>;
}

/**
 * Token response from a `TokenProvider`. Mirrors the Agent Gateway
 * `/api/v1/messaging/token` response and the MS session-issue REST
 * shape — agents and services can both feed this shape into the
 * client.
 */
export interface TokenResponse {
  token: string;
  /** Centrifugo WebSocket URL the client should connect to. */
  centrifugoUrl: string;
  /** Public Message Service REST URL. Optional late-bind. */
  serviceUrl?: string;
  /** Resolved identity (canonical principal id). */
  identityId: string;
  /** Unix epoch seconds. */
  expiresAt: number;
  /** Optional pre-subscribed private channels. */
  channels?: string[];
}

/**
 * Async function that obtains a Centrifugo connection token. The
 * caller decides how to authenticate (Ed25519 via Agent Gateway,
 * service API Key, etc.). `MessageClient` is agnostic.
 */
export type TokenProvider = (identityId: string) => Promise<TokenResponse>;

/** Minimal logger contract — pino, console, anything with these methods. */
export interface Logger {
  info?(...a: unknown[]): void;
  warn?(...a: unknown[]): void;
  error?(...a: unknown[]): void;
  debug?(...a: unknown[]): void;
}
