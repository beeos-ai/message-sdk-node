import { createHash } from "node:crypto";

/**
 * Retention-safe, hash-only structured logging for the Message SDK's
 * delta/terminal POST/PATCH boundary — the "Message SDK delta/terminal POST"
 * hop in the OpenClaw chat delivery chain (Message Service -> Centrifugo ->
 * beeos-claw -> OpenClaw Core -> message-sdk-node -> Message Service).
 *
 * Every log line emitted through this module carries only SHA-256 hashes of
 * identifiers and fixed schema enums (stage/status/code/reason) — never a
 * raw conversation/message id, a token, or message body/payload content.
 *
 * The hash construction (namespace + value, each length-prefixed before
 * SHA-256) is bit-for-bit identical to the Go backend's
 * `application.StableIdentifierHash` in services/message and to beeos-claw's
 * own `hashIdentifier`, using the same default namespace
 * ("beeos/runtime-dispatch-log/v1"), so hashes of the same underlying id
 * are directly comparable across all three services' logs without ever
 * comparing raw identifiers.
 */

const DEFAULT_HASH_NAMESPACE = "beeos/runtime-dispatch-log/v1";

export function hashIdentifier(
  value: string | undefined | null,
  namespace: string = DEFAULT_HASH_NAMESPACE,
): string {
  const hash = createHash("sha256");
  for (const field of [namespace, value ?? ""]) {
    const bytes = Buffer.from(field, "utf8");
    const length = Buffer.alloc(8);
    length.writeBigUInt64BE(BigInt(bytes.length));
    hash.update(length);
    hash.update(bytes);
  }
  return hash.digest("hex");
}

export interface OpenClawDeliveryBoundaryFields {
  readonly stage: string;
  readonly status: string;
  readonly code?: string;
  readonly reason?: string;
  readonly conversationId?: string;
  readonly messageId?: string;
}

/** Formats a single-line, greppable, hash-only log message. */
export function formatOpenClawDeliveryBoundary(fields: OpenClawDeliveryBoundaryFields): string {
  const parts: string[] = [
    "[message-sdk-node] openclaw_delivery_boundary",
    `stage=${fields.stage}`,
    `status=${fields.status}`,
  ];
  if (fields.code) parts.push(`code=${fields.code}`);
  if (fields.reason) parts.push(`reason=${fields.reason}`);
  if (fields.conversationId !== undefined) {
    parts.push(`conversation_id_hash=${hashIdentifier(fields.conversationId)}`);
  }
  if (fields.messageId !== undefined) {
    parts.push(`message_id_hash=${hashIdentifier(fields.messageId)}`);
  }
  return parts.join(" ");
}
