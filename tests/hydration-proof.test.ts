import { describe, expect, it } from "vitest";

import { ProjectionEngine } from "../src/facade/projection.js";
import type { MessageProjection } from "../src/facade/contracts.js";
import type { RealtimeEventV1 } from "../src/protocol/index.js";

const at = "2026-07-28T00:00:00Z";

function message(): MessageProjection {
  return {
    id: "m1", conversationId: "c1", type: "chat_message", body: "old",
    state: "completed", historyGeneration: "1", offset: "9", revision: "9",
    createdAt: at, updatedAt: at,
  };
}

describe("conversation hydration commit proof", () => {
  it("is written only by a complete atomic commit and removed on generation change", () => {
    const projection = new ProjectionEngine();
    expect(projection.getSnapshot().hydrationByConversation).toEqual({});
    projection.commitHydration({
      conversation: {
        id: "c1", state: "open", historyGeneration: "1", revision: "7", updatedAt: at,
      },
      messages: [message()],
      latestOffset: "9",
    });
    const committed = projection.getSnapshot();
    expect(committed.hydrationByConversation.c1).toEqual({
      conversationId: "c1",
      historyGeneration: "1",
      conversationRevision: "7",
      latestOffset: "9",
      projectionRevision: "1",
      complete: true,
    });
    expect(projection.getSnapshot()).toBe(committed);
    expect(projection.commitHydration({
      conversation: {
        id: "c1", state: "open", historyGeneration: "1", revision: "7", updatedAt: at,
      },
      messages: [message()],
      latestOffset: "9",
    })).toBe(false);
    expect(projection.getSnapshot()).toBe(committed);
    expect(projection.getSnapshot().hydrationByConversation.c1.projectionRevision).toBe("1");

    const terminal: RealtimeEventV1<"message.terminal"> = {
      schemaVersion: 1,
      eventId: "terminal",
      type: "message.terminal",
      scope: { tenantId: "t1", conversationId: "c1", messageId: "m1" },
      actor: { kind: "service", id: "ms" },
      ordering: {
        streamSequence: "9",
        messageOffset: "9",
        entityRevision: "10",
        historyGeneration: "1",
        completeness: "full",
      },
      correlation: {},
      occurredAt: at,
      data: {
        message: {
          id: "m1",
          conversationId: "c1",
          type: "chat_message",
          body: "old",
          state: "failed",
          stopReason: "error",
          historyGeneration: "1",
          offset: 9,
          revision: "10",
          createdAt: at,
          updatedAt: at,
        },
      },
    };
    expect(projection.apply(terminal)).toBe(true);
    expect(projection.getSnapshot().hydrationByConversation.c1).toMatchObject({
      latestOffset: "9",
      projectionRevision: "2",
      complete: true,
    });

    const cleared: RealtimeEventV1<"conversation.updated"> = {
      schemaVersion: 1,
      eventId: "clear",
      type: "conversation.updated",
      scope: { tenantId: "t1", conversationId: "c1" },
      actor: { kind: "user", id: "u1" },
      ordering: {
        streamSequence: "10", entityRevision: "8", historyGeneration: "2", completeness: "full",
      },
      correlation: {},
      occurredAt: at,
      data: {
        conversation: {
          id: "c1", state: "open", metadataVersion: "8", historyGeneration: "2", updatedAt: at,
        },
      },
    };
    expect(projection.apply(cleared)).toBe(true);
    expect(projection.getSnapshot().hydrationByConversation.c1).toBeUndefined();
    expect(projection.getSnapshot().messages).toEqual({});
  });
});
