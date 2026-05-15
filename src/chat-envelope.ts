/**
 * `chat_message` / `agent_request` envelope adapter.
 *
 * BeeOS L0 AgentInvocationService publishes `chat_message` envelopes to
 * a target agent's personal Centrifugo channel. The payload shape is
 * defined in `backend/pkg/chatinvoke/invoker.go` (`buildChatPayload`):
 *
 *   {
 *     "message": "<user prompt>",
 *     "channel_id": "<im channel for replies>",
 *     "context_id": "<stable session anchor, defaults to channel_id>",
 *     "metadata": { "target_agent_id": "<agentId>", "delivery_principal": "<pid>" }
 *   }
 *
 * Some legacy callers (A2A SendTask before the L0 cutover) publish
 * `agent_request` envelopes with `payload.parts: [{ type: "text", text }]`
 * and `payload.files: ["<url>", …]` — kept here as a fallback so the
 * same extractor works for both transports.
 *
 * The returned `ChatPrompt` has a deliberately minimal, fixed shape so
 * agents cannot accidentally read protocol-specific A2A / MCP / OpenAPI
 * fields off the envelope (see `A2A_PROTOCOL_FIELDS` for the exclusion
 * list). The SDK is the boundary that enforces the L0 protocol-agnostic
 * contract — agents must depend ONLY on the keys in `ChatPrompt`.
 */

import type { MessageEnvelope } from "./envelope.js";

/**
 * Output shape — fixed surface. Adding fields is a breaking change.
 *
 * - `message`     — prompt text to feed the model.
 * - `files`       — best-effort `string[]` of URLs, NOT object form.
 *                   Extracted from `payload.parts[].file_url` and the
 *                   legacy `payload.files: string[]` shape.
 * - `channelId`   — IM channel agents publish replies to (`ch:{channelId}`).
 *                   May be `undefined` for legacy `agent_request` paths.
 * - `contextId`   — stable session anchor; agents derive their per-session
 *                   state key off this when present.
 * - `sessionKey`  — derived session id used by agents that already key
 *                   local sessions off a string id. Format:
 *                     - `chat_message`:
 *                       - explicit `payload.session_key`, OR
 *                       - `agent:{localAgentId}:invoke:ctx-{contextId}`, OR
 *                       - `agent:{localAgentId}:invoke:ch-{channelId}`, OR
 *                       - `agent:{localAgentId}:invoke:{generateSuffix()}`
 *                     - `agent_request`:
 *                       - explicit `payload.session_key`, OR
 *                       - `agent:{localAgentId}:a2a:{generateSuffix()}`
 * - `messageId`   — request `message_id`, must be echoed back as
 *                   `in_reply_to` on the agent's reply so the caller's
 *                   `/wait` matches. Resolved as
 *                   `envelope.message_id ?? envelope.metadata.message_id`
 *                   (payload.message_id is intentionally NOT consulted —
 *                   it is reserved for protocol headers that must not
 *                   leak through this boundary). Empty strings are
 *                   normalised to `undefined`.
 */
export interface ChatPrompt {
  message: string;
  files: string[];
  channelId?: string;
  contextId?: string;
  sessionKey: string;
  messageId?: string;
}

export interface ExtractChatPromptOptions {
  /** Inbound transport tag — used for sessionKey prefix selection. */
  source?: "chat_message" | "agent_request";
  /**
   * Optional local agent identity. When set, prefixes the sessionKey as
   * `agent:{localAgentId}:invoke:...` / `agent:{localAgentId}:a2a:...`.
   * Without it the prefix collapses to `agent::invoke:...` — useful for
   * tests but never expected in production.
   */
  localAgentId?: string;
  /**
   * Random suffix generator for sessionKeys when neither contextId nor
   * channelId is available. Defaults to `Math.random().toString(36)`.
   * Tests pass a deterministic stub.
   */
  generateSuffix?: () => string;
}

/**
 * Forbidden A2A / MCP / OpenAPI protocol header fields that must never
 * leak from the L0 transport into the agent prompt. The extractor
 * silently ignores any of these on the inbound payload and never emits
 * them on the output `ChatPrompt`.
 *
 * Treat this list as part of the `ChatPrompt` contract — adding a new
 * forbidden field is a backward-compatible safety tightening; removing
 * one is a breaking semantic change.
 */
export const A2A_PROTOCOL_FIELDS: readonly string[] = Object.freeze([
  "task_id",
  "skills",
  "artifacts",
  "protocol_version",
  "caller_protocol",
  "correlation_id",
  "callback_url",
  "trace_parent",
  "trace_state",
  "openapi_invocation_id",
  "mcp_request_id",
  "agent_card",
]);

/**
 * Extract a normalized `ChatPrompt` from an inbound envelope.
 * Returns `null` when the payload is missing the required `message` text
 * so handlers can no-op safely.
 */
export function extractChatPrompt(
  envelope: MessageEnvelope,
  opts: ExtractChatPromptOptions = {},
): ChatPrompt | null {
  const payload = (envelope.payload ?? {}) as Record<string, unknown>;
  const meta = (envelope.metadata ?? {}) as Record<string, unknown>;
  const payloadMeta = (payload.metadata ?? {}) as Record<string, unknown>;

  const message = extractMessageText(payload);
  if (!message) return null;

  // Channel precedence: envelope > metadata > payload — locked in by
  // beeos-claw test "prefers envelope.channel_id over metadata over payload".
  const channelId = pickString(
    envelope.channel_id,
    meta.channel_id,
    payloadMeta.channel_id,
    payload.channel_id,
  );

  const contextId = pickString(payload.context_id, payloadMeta.context_id);

  // Files: prefer parts[].file_url, fall back to payload.files (already
  // a string[] in the legacy A2A path).
  const files = extractFiles(payload);

  // messageId precedence: envelope > envelope.metadata. Payload.message_id
  // is intentionally NOT consulted — it is reserved for protocol headers
  // that must never leak through this boundary.
  const messageIdRaw = pickString(envelope.message_id, meta.message_id);
  const messageId = messageIdRaw || undefined;

  const source: "chat_message" | "agent_request" =
    opts.source ?? (envelope.type === "agent_request" ? "agent_request" : "chat_message");

  const sessionKey = computeSessionKey({
    payload,
    source,
    localAgentId: opts.localAgentId ?? "",
    contextId,
    channelId,
    generateSuffix: opts.generateSuffix ?? defaultSuffix,
  });

  const out: ChatPrompt = {
    message,
    files,
    sessionKey,
  };
  if (channelId) out.channelId = channelId;
  if (contextId) out.contextId = contextId;
  if (messageId) out.messageId = messageId;
  return out;
}

function extractMessageText(payload: Record<string, unknown>): string {
  const direct = pickString(payload.message);
  if (direct) return direct;

  const parts = payload.parts;
  if (Array.isArray(parts)) {
    const chunks: string[] = [];
    for (const part of parts) {
      if (
        part &&
        typeof part === "object" &&
        (part as { type?: string }).type === "text"
      ) {
        const text = (part as { text?: unknown }).text;
        if (typeof text === "string") chunks.push(text);
      }
    }
    if (chunks.length > 0) return chunks.join("\n");
  }

  return "";
}

function extractFiles(payload: Record<string, unknown>): string[] {
  const out: string[] = [];

  const parts = payload.parts;
  if (Array.isArray(parts)) {
    for (const part of parts) {
      if (!part || typeof part !== "object") continue;
      const fileUrl = (part as { file_url?: unknown }).file_url;
      if (typeof fileUrl === "string" && fileUrl.length > 0) {
        out.push(fileUrl);
      }
    }
  }

  const filesField = payload.files;
  if (Array.isArray(filesField)) {
    for (const item of filesField) {
      if (typeof item === "string" && item.length > 0) {
        out.push(item);
        continue;
      }
      if (item && typeof item === "object") {
        const url = pickString(
          (item as Record<string, unknown>).url,
          (item as Record<string, unknown>).uri,
        );
        if (url) out.push(url);
      }
    }
  }

  return out;
}

function computeSessionKey(args: {
  payload: Record<string, unknown>;
  source: "chat_message" | "agent_request";
  localAgentId: string;
  contextId: string;
  channelId: string;
  generateSuffix: () => string;
}): string {
  // Priority 1 (legacy): explicit payload.session_key from server. The
  // server-side a2a/chatinvoke paths historically embedded the platform
  // agent UUID here (PR-FIX-F era), which broke openclaw's agentSend
  // validation (sessionKey's agent segment must equal the agent's local
  // name, not the platform UUID). The producer-side fix (PR-FIX-G)
  // stopped emitting session_key entirely and now surfaces the wire
  // protocol via metadata.protocol instead — see priority 2. We keep
  // this branch only to honour an explicit session_key when a caller
  // genuinely needs to pin one (test fixtures, future use cases that
  // know their own agent name).
  const explicit = pickString(args.payload.session_key);
  if (explicit) return explicit;

  // Priority 2 (PR-FIX-G): read the wire protocol tag from
  // payload.metadata.protocol. This is generic IM metadata — the SDK
  // treats it as an opaque string and composes
  // `agent:{localAgentId}:{protocol}:{discriminator}`, where the agent
  // segment uses the SDK consumer's own localAgentId (not the server's
  // platform UUID), so it lines up with whatever agent-runtime layer
  // sits below the SDK (openclaw's agentSend validation, beeos-claw's
  // BACKGROUND_SESSION_PATTERNS filter, etc.). The SDK is intentionally
  // ignorant of those downstream concerns.
  const payloadMeta = (args.payload.metadata && typeof args.payload.metadata === "object"
    ? (args.payload.metadata as Record<string, unknown>)
    : {});
  const protoTag = pickString(payloadMeta.protocol);

  // Priority 3 (legacy fallback): derive the tag from envelope.type so
  // pre-PR-FIX-G producers (no metadata.protocol) keep working.
  // agent_request → :a2a:, chat_message → :invoke:.
  const tag = protoTag || (args.source === "agent_request" ? "a2a" : "invoke");
  const prefix = `agent:${args.localAgentId}:${tag}`;

  // When metadata.protocol is set we treat the message like a
  // chat_message for sessionKey-discriminator purposes (prefer
  // contextId, then channelId) regardless of envelope.type. This keeps
  // the new server contract (always emit metadata.protocol) uniform
  // across both envelope types.
  if (args.source === "chat_message" || protoTag) {
    if (args.contextId) return `${prefix}:ctx-${args.contextId}`;
    if (args.channelId) return `${prefix}:ch-${args.channelId}`;
  }
  return `${prefix}:${args.generateSuffix()}`;
}

function defaultSuffix(): string {
  return Math.random().toString(36).slice(2, 14);
}

function pickString(...candidates: unknown[]): string {
  for (const c of candidates) {
    if (typeof c === "string" && c.length > 0) return c;
  }
  return "";
}
