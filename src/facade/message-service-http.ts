import { decodeRealtimeEvent, type AnyRealtimeEventV1, type RealtimeCursor } from "../protocol/index.js";
import type {
  ConversationHydration,
  ExecuteMethodInput,
  ExecuteMethodResult,
  HydrateConversationInput,
  MessageHttpTransportPort,
  RealtimeRebase,
  RebaseInput,
  SendMessageInput,
  SendMessageResult,
} from "./types.js";
import type { RealtimeAuthProvider } from "./centrifugo-realtime.js";

/**
 * Explicit application-owned seams which Message Service does not currently
 * expose as v2 HTTPS resources.  They are intentionally required: callers
 * cannot accidentally route methods or conversation hydration through a
 * guessed Message Service endpoint.
 */
export interface MessageServiceHttpExplicitPorts {
  executeMethod(input: ExecuteMethodInput): Promise<ExecuteMethodResult>;
  hydrateConversation(input: HydrateConversationInput): Promise<ConversationHydration>;
}

export interface MessageServiceHttpTransportOptions extends MessageServiceHttpExplicitPorts {
  /** Message Service API origin, e.g. https://msg.beeos.ai. */
  apiBaseUrl: string;
  /** Identity-bound bearer source; service keys are not accepted by realtime. */
  authProvider: RealtimeAuthProvider;
  /** Injectable for browser, React Native, Node and deterministic tests. */
  fetchImpl?: typeof fetch;
}

/**
 * HTTPS+JSON portion of the v2 SDK transport.
 *
 * It owns only endpoints proved to exist in Message Service: send and
 * authority sync.  It has no EventSource, WebSocket, retry loop, fallback,
 * generated idempotency key, channel, or token-returning public API.
 */
export class MessageServiceHttpTransport implements MessageHttpTransportPort {
  private readonly apiBaseUrl: string;
  private readonly fetchImpl: typeof fetch;

  constructor(private readonly options: MessageServiceHttpTransportOptions) {
    this.apiBaseUrl = normalizeMessageServiceApiBaseUrl(options.apiBaseUrl);
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  async sendMessage(input: SendMessageInput): Promise<SendMessageResult> {
    const body = v2SendBody(input);
    if (input.replyTo !== undefined) body.reply_to = input.replyTo;
    const response = await this.request(
      `/api/v2/conversations/${encodeURIComponent(input.conversationId)}/messages`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": input.clientMessageId,
        },
        body: stringifyBody(body),
      },
    );
    if (response.status !== 200 && response.status !== 201) {
      throw httpError("message send", response.status);
    }
    const raw = await jsonObject(response, "message send");
    const messageId = requiredString(raw, "id", "message send response");
    // The confirmed v2 endpoint returns 200 for a previous idempotency key,
    // but does not distinguish a duplicate from another accepted response.
    // Never invent that distinction from status alone.
    return {
      messageId,
      outcome: response.status === 201 ? "created" : "accepted",
      correlationId: response.headers.get("x-request-id") ?? undefined,
    };
  }

  executeMethod(input: ExecuteMethodInput): Promise<ExecuteMethodResult> {
    return this.options.executeMethod(input);
  }

  hydrateConversation(input: HydrateConversationInput): Promise<ConversationHydration> {
    return this.options.hydrateConversation(input);
  }

  async rebase(input: RebaseInput): Promise<RealtimeRebase> {
    if (!input.syncCursor) {
      throw new Error("message-sdk realtime rebase requires a server-issued sync cursor");
    }
    const url = new URL(`${this.apiBaseUrl}/api/v2/sync`);
    url.searchParams.set("cursor", input.syncCursor);
    const response = await this.request(url.pathname + url.search, { method: "GET" });
    if (response.status !== 200) throw httpError("realtime sync", response.status);
    const raw = await jsonObject(response, "realtime sync");
    const completeness = requiredString(raw, "completeness", "realtime sync response");
    if (completeness !== "full") {
      throw new Error("message-sdk realtime sync requires a full authoritative response");
    }
    const nextCursor = requiredString(raw, "next_cursor", "realtime sync response");
    if (!Array.isArray(raw.events)) throw new Error("message-sdk realtime sync response requires events");
    const events = raw.events.map((event) => decodeRealtimeEvent(event));
    return {
      events,
      cursor: cursorFromEvents(events, input.cursor),
      syncCursor: nextCursor,
    };
  }

  private async request(path: string, init: RequestInit): Promise<Response> {
    const token = await this.options.authProvider.getAccessToken();
    if (!token || !token.trim()) {
      throw new Error("message-sdk message-service auth provider returned an empty bearer token");
    }
    const headers = new Headers(init.headers);
    headers.set("Authorization", `Bearer ${token}`);
    return this.fetchImpl(`${this.apiBaseUrl}${path}`, { ...init, headers });
  }
}

export function createMessageServiceHttpTransport(
  options: MessageServiceHttpTransportOptions,
): MessageServiceHttpTransport {
  return new MessageServiceHttpTransport(options);
}

function cursorFromEvents(
  events: readonly AnyRealtimeEventV1[],
  previous: RealtimeCursor | undefined,
): RealtimeCursor | undefined {
  if (events.length === 0) return previous;
  const latest = events.reduce((selected, event) => (
    BigInt(event.ordering.streamSequence) > BigInt(selected.ordering.streamSequence) ? event : selected
  ));
  return {
    streamSequence: latest.ordering.streamSequence,
    historyGeneration: latest.ordering.historyGeneration,
    projectionUid: latest.ordering.projectionUid,
    projectionEpoch: latest.ordering.projectionEpoch,
  };
}

function normalizeMessageServiceApiBaseUrl(value: string): string {
  if (!value) throw new Error("message-sdk message-service apiBaseUrl is required");
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("message-sdk message-service apiBaseUrl must be an absolute HTTPS URL");
  }
  const localHttp = parsed.protocol === "http:"
    && (parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1" || parsed.hostname === "[::1]");
  if (parsed.protocol !== "https:" && !localHttp) {
    throw new Error("message-sdk message-service apiBaseUrl must use https:// outside localhost tests");
  }
  return value.replace(/\/+$/, "");
}

function stringifyBody(body: Record<string, unknown>): string {
  const encoded = JSON.stringify(body);
  if (!encoded) throw new Error("message-sdk message send body must be JSON serializable");
  return encoded;
}

function v2SendBody(input: SendMessageInput): Record<string, unknown> {
  const raw = input as SendMessageInput & { text?: unknown; parts?: unknown; content?: unknown; type?: unknown };
  const hasText = Object.prototype.hasOwnProperty.call(raw, "text");
  const hasContent = Object.prototype.hasOwnProperty.call(raw, "content");
  const hasType = Object.prototype.hasOwnProperty.call(raw, "type");
  const hasParts = Object.prototype.hasOwnProperty.call(raw, "parts");
  let type: string;
  let content: unknown;
  if (hasText) {
    if (typeof raw.text !== "string") throw new Error("message-sdk text send requires a string text");
    if (hasContent || hasType) throw new Error("message-sdk text send cannot include content or type");
    if (hasParts && (!Array.isArray(raw.parts) || !isMessageJson(raw.parts))) {
      throw new Error("message-sdk text send parts must be JSON-safe array");
    }
    // V2SendMessageRequest additive envelope contract: Body and Parts are
    // distinct from legacy opaque Content. `chat_message` is the established
    // Mobile/beeos-claw normal inbound user-message type.
    const body: Record<string, unknown> = { type: "chat_message", body: raw.text };
    if (hasParts) body.parts = raw.parts;
    if (input.replyTo !== undefined) body.reply_to = input.replyTo;
    return body;
  } else {
    if (!hasContent || !hasType || typeof raw.type !== "string" || !raw.type) {
      throw new Error("message-sdk content send requires explicit non-empty type and content");
    }
    if (hasParts) throw new Error("message-sdk content send cannot include parts; include them inside content");
    if (!isMessageJson(raw.content)) throw new Error("message-sdk content send must be JSON-safe");
    type = raw.type;
    content = raw.content;
  }
  const body: Record<string, unknown> = { type, content };
  if (input.replyTo !== undefined) body.reply_to = input.replyTo;
  return body;
}

function isMessageJson(value: unknown, seen = new Set<unknown>()): boolean {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value !== "object") return false;
  if (seen.has(value)) return false;
  seen.add(value);
  try {
    if (Array.isArray(value)) return value.every((item) => isMessageJson(item, seen));
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) return false;
    return Object.values(value).every((item) => isMessageJson(item, seen));
  } finally {
    seen.delete(value);
  }
}

async function jsonObject(response: Response, operation: string): Promise<Record<string, unknown>> {
  let raw: unknown;
  try {
    raw = await response.json();
  } catch {
    throw new Error(`message-sdk ${operation} response must be JSON`);
  }
  if (!isRecord(raw)) throw new Error(`message-sdk ${operation} response must be an object`);
  return raw;
}

function requiredString(raw: Record<string, unknown>, key: string, source: string): string {
  const value = raw[key];
  if (typeof value !== "string" || !value) throw new Error(`message-sdk ${source} requires ${key}`);
  return value;
}

function httpError(operation: string, status: number): Error {
  // Do not read error bodies: they can carry server diagnostics or tokens.
  return new Error(`message-sdk ${operation} request failed with HTTP ${status}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
