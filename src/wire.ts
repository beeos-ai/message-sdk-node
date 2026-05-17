// Internal — Centrifugo publish wire envelope shapes and v1→v2 message
// translation. NOT part of the public API; consumers only see v2
// `Message<T>` (defined in ./types.ts) emitted from MessageClient.
//
// Message Service publishes envelopes on `personal:{identityId}` and
// `ch:{conversationId}` Centrifugo channels using the historical v1
// shape (`payload`, `channel_id`, `publisher_id`, `message_id`,
// `in_reply_to`, …). This module owns the only place in the SDK that
// understands that shape; everything above this layer sees v2.

import type { Message } from "./types.js";

/**
 * v1 wire envelope shape as published by Message Service via Centrifugo.
 * @internal — never expose to public types.ts. Subject to change when
 * the server switches Centrifugo publishes to v2-shape natively.
 */
export interface WireEnvelope {
  type: string;
  payload: unknown;
  channel_id?: string;
  message_id?: string;
  offset?: number;
  in_reply_to?: string;
  idempotency_key?: string;
  publisher_id?: string;
  created_at?: string;
  metadata?: Record<string, string>;
}

/**
 * Translate a wire envelope into the public `Message<T>` shape.
 *
 *   wire.message_id    -> v2.id
 *   wire.channel_id    -> v2.conversationId
 *   wire.payload       -> v2.content
 *   wire.publisher_id  -> v2.sender   (fallback: metadata.from)
 *   wire.in_reply_to   -> v2.replyTo
 *   wire.created_at    -> v2.createdAt
 *
 * Fields that disappear at the v2 boundary (`offset`,
 * `idempotency_key`, top-level `metadata`) are dropped — their content
 * is either redundant with the v2 fields above or, for legitimate
 * envelope metadata, embedded inside `content` by the producer.
 */
export function envelopeToMessage<TContent = unknown>(
  env: WireEnvelope,
): Message<TContent> {
  return {
    id: env.message_id ?? "",
    conversationId: env.channel_id ?? "",
    type: env.type,
    content: env.payload as TContent,
    sender: env.publisher_id ?? env.metadata?.from ?? "",
    replyTo: env.in_reply_to,
    createdAt: env.created_at ?? "",
  };
}
