/**
 * `chat_message` / `agent_request` content adapter.
 *
 * BeeOS L0 AgentInvocationService publishes `chat_message` messages
 * to a target agent's personal Centrifugo channel. The content shape
 * is defined in `backend/pkg/chatinvoke/invoker.go` (`buildChatPayload`):
 *
 *   {
 *     "message": "<user prompt>",
 *     "channel_id": "<im channel for replies>",
 *     "context_id": "<stable session anchor, defaults to channel_id>",
 *     "metadata": { "target_agent_id": "<agentId>", "delivery_principal": "<pid>", "protocol": "..." }
 *   }
 *
 * Some legacy callers (A2A SendTask before the L0 cutover) publish
 * `agent_request` envelopes with `content.parts: [{ type: "text", text }]`
 * and `content.files: ["<url>", …]` — kept here as a fallback so the
 * same extractor works for both transports.
 *
 * The returned `ChatPrompt` has a deliberately minimal, fixed shape
 * so agents cannot accidentally read protocol-specific A2A / MCP /
 * OpenAPI fields off the content (see `A2A_PROTOCOL_FIELDS` for the
 * exclusion list). The SDK is the boundary that enforces the L0
 * protocol-agnostic contract — agents must depend ONLY on the keys
 * in `ChatPrompt`.
 *
 * Exposed as a subpath import to keep the root client API surface
 * minimal:
 *   import { extractChatPrompt, A2A_PROTOCOL_FIELDS } from "@beeos-ai/message-sdk/chat-envelope";
 */

import type { Message } from "./types.js";

/**
 * Output shape — fixed surface. Adding fields is a breaking change.
 *
 * - `message`     — prompt text to feed the model.
 * - `files`       — best-effort `string[]` of URLs, NOT object form.
 * - `channelId`   — IM conversation agents publish replies to.
 *                   May be `undefined` for legacy `agent_request` paths.
 * - `contextId`   — stable session anchor.
 * - `sessionKey`  — derived session id used by agents that already
 *                   key local sessions off a string id. Format:
 *                     - `chat_message`:
 *                       - explicit `content.session_key`, OR
 *                       - `agent:{localAgentId}:{protocol}:ctx-{contextId}`, OR
 *                       - `agent:{localAgentId}:{protocol}:ch-{channelId}`, OR
 *                       - `agent:{localAgentId}:{protocol}:{generateSuffix()}`
 *                     - `agent_request`:
 *                       - explicit `content.session_key`, OR
 *                       - `agent:{localAgentId}:a2a:{generateSuffix()}`
 * - `messageId`   — request id, must be echoed back as `replyTo` on
 *                   the agent's reply so the caller's `/wait` matches.
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
   * Optional local agent identity. When set, prefixes the sessionKey
   * as `agent:{localAgentId}:invoke:...` / `agent:{localAgentId}:a2a:...`.
   * Without it the prefix collapses to `agent::invoke:...` — useful
   * for tests but never expected in production.
   */
  localAgentId?: string;
  /**
   * Random suffix generator for sessionKeys when neither contextId
   * nor channelId is available. Defaults to `Math.random().toString(36)`.
   * Tests pass a deterministic stub.
   */
  generateSuffix?: () => string;
}

/**
 * Forbidden A2A / MCP / OpenAPI protocol header fields that must
 * never leak from the L0 transport into the agent prompt. The
 * extractor silently ignores any of these on the inbound content and
 * never emits them on the output `ChatPrompt`.
 *
 * Treat this list as part of the `ChatPrompt` contract — adding a
 * new forbidden field is a backward-compatible safety tightening;
 * removing one is a breaking semantic change.
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
 * Extract a normalized `ChatPrompt` from an inbound v2 message.
 * Returns `null` when the content is missing the required `message`
 * text so handlers can no-op safely.
 *
 * NOTE: the parser reads `msg.content`, `msg.conversationId`, `msg.id`.
 * It does NOT touch `msg.sender`, `msg.replyTo`, `msg.createdAt` —
 * those are L1 transport metadata, not agent-prompt input.
 */
export function extractChatPrompt(
  msg: Message,
  opts: ExtractChatPromptOptions = {},
): ChatPrompt | null {
  const content = (msg.content ?? {}) as Record<string, unknown>;
  const contentMeta = (content.metadata ?? {}) as Record<string, unknown>;

  const messageText = extractMessageText(content);
  if (!messageText) return null;

  // Channel precedence: outer message.conversationId > content.channel_id.
  const channelId = pickString(msg.conversationId, content.channel_id);

  const contextId = pickString(content.context_id, contentMeta.context_id);

  const files = extractFiles(content);

  const messageIdRaw = pickString(msg.id);
  const messageId = messageIdRaw || undefined;

  const source: "chat_message" | "agent_request" =
    opts.source ?? (msg.type === "agent_request" ? "agent_request" : "chat_message");

  const sessionKey = computeSessionKey({
    content,
    source,
    localAgentId: opts.localAgentId ?? "",
    contextId,
    channelId,
    generateSuffix: opts.generateSuffix ?? defaultSuffix,
  });

  const out: ChatPrompt = {
    message: messageText,
    files,
    sessionKey,
  };
  if (channelId) out.channelId = channelId;
  if (contextId) out.contextId = contextId;
  if (messageId) out.messageId = messageId;
  return out;
}

function extractMessageText(content: Record<string, unknown>): string {
  const direct = pickString(content.message);
  if (direct) return direct;

  const parts = content.parts;
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

function extractFiles(content: Record<string, unknown>): string[] {
  const out: string[] = [];

  const parts = content.parts;
  if (Array.isArray(parts)) {
    for (const part of parts) {
      if (!part || typeof part !== "object") continue;
      const fileUrl = (part as { file_url?: unknown }).file_url;
      if (typeof fileUrl === "string" && fileUrl.length > 0) {
        out.push(fileUrl);
      }
    }
  }

  const filesField = content.files;
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
  content: Record<string, unknown>;
  source: "chat_message" | "agent_request";
  localAgentId: string;
  contextId: string;
  channelId: string;
  generateSuffix: () => string;
}): string {
  // Priority 1: explicit `content.session_key` — caller pinned a key
  // (test fixtures, future use cases that know their own agent name).
  const explicit = pickString(args.content.session_key);
  if (explicit) return explicit;

  // Priority 2: read the wire protocol tag from
  // `content.metadata.protocol`. The SDK treats it as an opaque
  // string and composes `agent:{localAgentId}:{protocol}:{discriminator}`,
  // where the agent segment uses the consumer's own localAgentId
  // (not the platform UUID), so it lines up with whatever
  // agent-runtime layer sits below the SDK.
  const contentMeta = (args.content.metadata && typeof args.content.metadata === "object"
    ? (args.content.metadata as Record<string, unknown>)
    : {});
  const protoTag = pickString(contentMeta.protocol);

  // Priority 3: derive the tag from `msg.type` for pre-protocol-tag
  // producers (no metadata.protocol). agent_request → :a2a:,
  // chat_message → :invoke:.
  const tag = protoTag || (args.source === "agent_request" ? "a2a" : "invoke");
  const prefix = `agent:${args.localAgentId}:${tag}`;

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
