import type { RealtimeEventV1 } from "../../src/protocol/index.js";

/**
 * One captured production assistant turn under Message Envelope v3 (ADR-0023).
 *
 * Message Service keeps a single `channel_messages` row per turn and advances
 * it with PATCH, deriving both `ordering.streamSequence` and
 * `ordering.messageOffset` from that row's offset. So all 31 events of this
 * turn — one create, 29 deltas, one terminal — report offset and sequence
 * `"14"`, and only `entityRevision` separates them.
 *
 * Both admission gates are tested against this same shape: the live-connection
 * gate in `protocol/recovery.ts` and the replay gate in
 * `facade/watch-registry.ts`.
 */

export const at = "2026-07-28T00:00:00.000Z";
export const conversationId = "c1";
export const messageId = "asst_1bd1e88a505c4605e6f329921a7a25b6";
export const historyGeneration = "0";
export const streamSequence = "14";
export const messageOffset = "14";

export const createdRevision = "1785305995400000";
export const terminalRevision = "1785305996991994";

const firstDeltaRevision = 1785305995422560n;
const deltaRevisionStep = 53171n;

export const chunks = [
  "The ", "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog. ",
  "Pack ", "my ", "box ", "with ", "five ", "dozen ", "liquor ", "jugs. ",
  "How ", "vexingly ", "quick ", "daft ", "zebras ", "jump! ",
  "Sphinx ", "of ", "black ", "quartz, ", "judge ", "my vow.",
];

export const fullBody = chunks.join("");

export function deltaRevision(index: number): string {
  return (firstDeltaRevision + BigInt(index) * deltaRevisionStep).toString();
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).length;
}

export function ordering(entityRevision: string, sequence = streamSequence) {
  return {
    streamSequence: sequence,
    entityRevision,
    messageOffset,
    historyGeneration,
    completeness: "full" as const,
  };
}

export function createdEvent(): RealtimeEventV1<"message.created"> {
  return {
    schemaVersion: 1,
    eventId: `message.created-${messageId}`,
    type: "message.created",
    scope: { tenantId: "t1", conversationId, messageId },
    actor: { kind: "service", id: "ms" },
    ordering: ordering(createdRevision),
    correlation: {},
    occurredAt: at,
    data: {
      message: {
        id: messageId,
        conversationId,
        senderId: "agent_a",
        type: "agent_reply",
        body: "",
        state: "streaming",
        createdAt: at,
        updatedAt: at,
        historyGeneration,
      },
    },
  } as RealtimeEventV1<"message.created">;
}

export function deltaEvent(index: number): RealtimeEventV1<"message.delta"> {
  const revision = deltaRevision(index);
  return {
    schemaVersion: 1,
    eventId: `message.delta-${messageId}-${revision}`,
    type: "message.delta",
    scope: { tenantId: "t1", conversationId, messageId },
    actor: { kind: "service", id: "ms" },
    ordering: ordering(revision),
    correlation: {},
    occurredAt: at,
    data: {
      message: { id: messageId, conversationId },
      bodyAppend: chunks[index]!,
      bodyFrom: utf8Length(chunks.slice(0, index).join("")),
    },
  } as RealtimeEventV1<"message.delta">;
}

export function terminalEvent(): RealtimeEventV1<"message.terminal"> {
  return {
    schemaVersion: 1,
    eventId: `message.terminal-${messageId}`,
    type: "message.terminal",
    scope: { tenantId: "t1", conversationId, messageId },
    actor: { kind: "service", id: "ms" },
    ordering: ordering(terminalRevision),
    correlation: {},
    occurredAt: at,
    data: {
      message: {
        id: messageId,
        conversationId,
        senderId: "agent_a",
        type: "agent_reply",
        body: fullBody,
        state: "completed",
        createdAt: at,
        updatedAt: at,
        historyGeneration,
      },
    },
  } as RealtimeEventV1<"message.terminal">;
}
