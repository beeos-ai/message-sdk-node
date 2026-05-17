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
}

export interface ConversationPage {
  conversations: Conversation[];
  nextCursor?: string;
  hasMore: boolean;
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
