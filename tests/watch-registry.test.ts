import { describe, expect, it } from "vitest";

import {
  ConversationWatchRegistry,
  ProjectionEngine,
  type ConversationHydrationCommit,
  type MessageProjection,
} from "../src/facade/index.js";
import type { RealtimeEventV1 } from "../src/protocol/index.js";

const at = "2026-07-28T00:00:00.000Z";

function message(id: string, offset: string): MessageProjection {
  return {
    id,
    conversationId: "c1",
    senderId: "u1",
    type: "chat_message",
    body: id,
    state: "completed",
    historyGeneration: "1",
    offset,
    revision: offset,
    createdAt: at,
    updatedAt: at,
  };
}

function created(id: string, sequence: string): RealtimeEventV1<"message.created"> {
  return {
    schemaVersion: 1,
    eventId: `event-${id}`,
    type: "message.created",
    scope: { tenantId: "t1", conversationId: "c1", messageId: id },
    actor: { kind: "user", id: "u1" },
    ordering: {
      streamSequence: sequence,
      entityRevision: sequence,
      messageOffset: sequence,
      historyGeneration: "1",
      completeness: "full",
    },
    correlation: {},
    occurredAt: at,
    data: {
      message: {
        id,
        conversationId: "c1",
        senderId: "u1",
        type: "chat_message",
        body: id,
        state: "completed",
        createdAt: at,
        updatedAt: at,
        historyGeneration: "1",
      },
    },
  };
}

describe("ConversationWatchRegistry", () => {
  it("subscribes before hydrate, ref-counts, buffers and merges once", async () => {
    const projection = new ProjectionEngine();
    const subscriptionCalls: Array<[string, boolean]> = [];
    let finishRecovery!: (commit: ConversationHydrationCommit) => void;
    const recovery = new Promise<ConversationHydrationCommit>((resolve) => {
      finishRecovery = resolve;
    });
    let recoverCalls = 0;
    const registry = new ConversationWatchRegistry({
      projection,
      getSession: () => ({
        setConversationWatched: async (id, watched) => {
          subscriptionCalls.push([id, watched]);
        },
        close: () => undefined,
      }),
      recovery: {
        recoverConversation: async () => {
          recoverCalls++;
          const commit = await recovery;
          projection.commitHydration(commit);
          return commit;
        },
      },
    });

    const first = registry.watch("c1");
    const second = registry.watch("c1");
    await Promise.resolve();
    expect(subscriptionCalls).toEqual([["c1", true]]);
    expect(registry.refCount("c1")).toBe(2);
    expect(recoverCalls).toBe(1);

    expect(registry.accept(created("m2", "2"))).toBe(true);
    expect(projection.getSnapshot().messages.m2).toBeUndefined();
    finishRecovery({
      conversation: {
        id: "c1", state: "open", historyGeneration: "1", revision: "1", updatedAt: at,
      },
      messages: [message("m1", "1")],
      latestOffset: "1",
    });
    await Promise.all([first.ready, second.ready]);
    expect(Object.keys(projection.getSnapshot().messages).sort()).toEqual(["m1", "m2"]);

    // Duplicate delivery within the authorized conversation is accepted once.
    expect(registry.accept(created("m2", "2"))).toBe(false);
    first.release();
    expect(subscriptionCalls).toEqual([["c1", true]]);
    second.release();
    await Promise.resolve();
    expect(subscriptionCalls).toEqual([["c1", true], ["c1", false]]);
  });
});
