import { describe, expect, it } from "vitest";

import {
  evaluateScopedRealtimeEvent,
  withScopedRealtimeCursor,
  type RealtimeEventV1,
  type ScopedRealtimeCursors,
} from "../src/protocol/index.js";

function updated(conversationId: string, sequence: string): RealtimeEventV1<"conversation.updated"> {
  return {
    schemaVersion: 1,
    eventId: `${conversationId}-${sequence}`,
    type: "conversation.updated",
    scope: { tenantId: "t1", conversationId },
    actor: { kind: "service", id: "ms" },
    ordering: {
      streamSequence: sequence,
      entityRevision: sequence,
      historyGeneration: "1",
      completeness: "full",
    },
    correlation: {},
    occurredAt: "2026-07-28T00:00:00.000Z",
    data: {
      conversation: {
        id: conversationId,
        state: "open",
        metadataVersion: sequence,
        historyGeneration: "1",
        updatedAt: "2026-07-28T00:00:00.000Z",
      },
    },
  };
}

describe("logical-scope realtime ordering", () => {
  it("does not manufacture a gap between independent conversations", () => {
    let cursors: ScopedRealtimeCursors = { conversations: {} };
    const first = updated("c1", "1");
    const firstDecision = evaluateScopedRealtimeEvent(cursors, first);
    expect(firstDecision.action).toBe("apply");
    if (firstDecision.action === "apply") {
      cursors = withScopedRealtimeCursor(cursors, { kind: "conversation", conversationId: "c1" }, firstDecision.cursor);
    }

    // c2 starts at its own sequence 77; c1's cursor is irrelevant.
    const other = updated("c2", "77");
    const otherDecision = evaluateScopedRealtimeEvent(cursors, other);
    expect(otherDecision.action).toBe("apply");
    if (otherDecision.action === "apply") {
      cursors = withScopedRealtimeCursor(cursors, { kind: "conversation", conversationId: "c2" }, otherDecision.cursor);
    }

    const second = updated("c1", "2");
    expect(evaluateScopedRealtimeEvent(cursors, second)).toMatchObject({ action: "apply" });
  });

  it("still detects a gap inside one conversation scope", () => {
    const cursors: ScopedRealtimeCursors = {
      conversations: {
        c1: { streamSequence: "1", historyGeneration: "1" },
      },
    };
    expect(evaluateScopedRealtimeEvent(cursors, updated("c1", "3"))).toEqual({
      action: "rebase",
      reason: "sequence_gap",
    });
  });
});
