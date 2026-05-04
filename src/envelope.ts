/**
 * Wire-shape types for the BeeOS Message Service IM channel-primitives.
 *
 * These are the canonical representations of:
 *
 *   - `MessageEnvelope` — what publishers put on Centrifugo and what
 *     subscribers receive in `client.on("publication", …)`.
 *   - `ChannelMessage` — the persisted `channel_messages` row returned by
 *     `GET /channels/{id}/messages`, `POST /channels/{id}/wait`, and the
 *     SSE `/stream` endpoint.
 *
 * Mirrors `services/message/pkg/domain/message/entity.go` and
 * `services/message/pkg/infrastructure/server/http/dto.go`.
 */

export interface MessageSDKConfig {
  /**
   * Optional Message Service REST base URL (e.g. `https://msg.beeos.ai`).
   * Required for the `getToken`, `sendChannelMessage`, `waitForReply`,
   * `listChannelMessages`, `send`, `isOnline`, `registerWebhook` methods.
   * Agents under the P6 architecture should leave this UNDEFINED — they
   * obtain Centrifugo tokens via Agent Gateway proxy, not via direct
   * Message Service REST.
   */
  serviceUrl?: string;
  /**
   * Optional Centrifugo WebSocket URL override. When omitted, the SDK
   * reads `centrifugo_url` from the token response.
   */
  centrifugoUrl?: string;
}

export interface TokenResponse {
  token: string;
  centrifugo_url: string;
  channels: string[];
  principal_id: string;
  /** Unix epoch seconds. */
  expires_at: number;
}

export interface MessageEnvelope {
  type: string;
  payload: unknown;
  correlationId?: string;
  channel_id?: string;
  metadata?: Record<string, string>;
  /** Set server-side on channel-primitives messages. */
  message_id?: string;
  /** Set server-side on channel-primitives messages. */
  offset?: number;
  /** Set when this envelope is a reply to another message. */
  in_reply_to?: string;
  /** Set when the caller supplied an idempotency_key. */
  idempotency_key?: string;
  /** Publisher principal_id, set server-side on persisted rows. */
  publisher_id?: string;
  /** RFC3339 timestamp, set server-side. */
  created_at?: string;
}

/** Persisted channel_messages row returned by GET / POST / wait. */
export interface ChannelMessage {
  message_id: string;
  offset: number;
  channel_id: string;
  type: string;
  payload: unknown;
  publisher_id: string;
  in_reply_to?: string;
  idempotency_key?: string;
  created_at: string;
  metadata?: Record<string, string>;
}

export interface SendChannelMessageInput {
  /** Defaults to currentPrincipalId when omitted. */
  senderId?: string;
  type: string;
  payload: unknown;
  /** Optional UUID; the server generates one when absent. */
  messageId?: string;
  /** Set on replies — lets callers POST `/wait` for the matching reply. */
  inReplyTo?: string;
  idempotencyKey?: string;
}

export interface SendChannelMessageResponse {
  status: string;
  message_id: string;
  offset: number;
  channel_id: string;
  recipients: number;
  idempotent: boolean;
  created_at: string;
  in_reply_to?: string;
  idempotency_key?: string;
}

export interface ListChannelMessagesResponse {
  messages: ChannelMessage[];
  latest_offset: number;
}

export interface WaitForReplyInput {
  inReplyTo: string;
  timeoutMs?: number;
  since?: number;
}

export type MessageHandler = (envelope: MessageEnvelope) => Promise<unknown>;

/**
 * Async function that obtains a Centrifugo connection token for a principal.
 * The caller decides how to authenticate (e.g. Ed25519 via Agent Gateway,
 * API Key direct to Message Service, etc.) — `MessageSDK` is agnostic.
 */
export type TokenProvider = (principalId: string) => Promise<TokenResponse>;

export interface ConnectOptions {
  centrifugoUrl?: string;
  tokenProvider?: TokenProvider;
}

export interface MessageSDKLogger {
  info?(...a: unknown[]): void;
  warn?(...a: unknown[]): void;
  error?(...a: unknown[]): void;
  debug?(...a: unknown[]): void;
}
