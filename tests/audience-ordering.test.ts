import { describe, expect, it } from "vitest";

import {
  evaluateScopedRealtimeEvent,
  withScopedRealtimeCursor,
  type RealtimeDeliveryAudience,
  type RealtimeEventV1,
  type ScopedRealtimeCursors,
} from "../src/protocol/index.js";

function event(sequence: string): RealtimeEventV1<"conversation.updated"> {
  return {
    schemaVersion: 1,
    eventId: "stable-event-id",
    type: "conversation.updated",
    scope: { tenantId: "t1", conversationId: "c1" },
    actor: { kind: "service", id: "ms" },
    ordering: {
      streamSequence: sequence,
      entityRevision: "1",
      historyGeneration: "1",
      completeness: "full",
    },
    correlation: {},
    occurredAt: "2026-07-28T00:00:00Z",
    data: {
      conversation: {
        id: "c1",
        state: "open",
        metadataVersion: "1",
        historyGeneration: "1",
        updatedAt: "2026-07-28T00:00:00Z",
      },
    },
  };
}

function privateEvent(sequence: string): RealtimeEventV1<"instance.updated"> {
  return {
    schemaVersion: 1,
    eventId: `private-${sequence}`,
    type: "instance.updated",
    scope: { tenantId: "t1", instanceId: "i1" },
    actor: { kind: "service", id: "instance-service" },
    ordering: {
      streamSequence: sequence,
      entityRevision: sequence,
      completeness: "full",
    },
    correlation: {},
    occurredAt: "2026-07-28T00:00:00Z",
    data: { instanceId: "i1", status: "running" },
  };
}

describe("per-delivery-audience ordering", () => {
  it("advances private-control and conversation cursors independently", () => {
    let cursors: ScopedRealtimeCursors = { conversations: {} };
    const privateControl: RealtimeDeliveryAudience = { kind: "private-control" };
    const conversation: RealtimeDeliveryAudience = { kind: "conversation", conversationId: "c1" };

    const privateCopy = privateEvent("10");
    const privateDecision = evaluateScopedRealtimeEvent(cursors, privateCopy, privateControl);
    expect(privateDecision.action).toBe("apply");
    if (privateDecision.action === "apply") {
      cursors = withScopedRealtimeCursor(cursors, privateControl, privateDecision.cursor);
    }

    const conversationCopy = event("2");
    conversationCopy.eventId = "conversation-event";
    const conversationDecision = evaluateScopedRealtimeEvent(cursors, conversationCopy, conversation);
    expect(conversationDecision.action).toBe("apply");
    if (conversationDecision.action === "apply") {
      cursors = withScopedRealtimeCursor(cursors, conversation, conversationDecision.cursor);
    }
    expect(cursors.privateControl?.streamSequence).toBe("10");
    expect(cursors.conversations.c1?.streamSequence).toBe("2");

    const nextPrivate = privateEvent("11");
    expect(evaluateScopedRealtimeEvent(cursors, nextPrivate, privateControl).action).toBe("apply");

    const nextConversation = event("3");
    nextConversation.eventId = "next-conversation";
    expect(evaluateScopedRealtimeEvent(cursors, nextConversation, conversation).action).toBe("apply");
  });
});
