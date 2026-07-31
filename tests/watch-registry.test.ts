import { describe, expect, it } from "vitest";

import type { AnyRealtimeEventV1 } from "../src/protocol/index.js";
import { ProjectionEngine } from "../src/facade/projection.js";
import { ConversationWatchRegistry } from "../src/facade/watch-registry.js";

const at = "2026-07-31T00:00:00.000Z";

function event(id: string, revision: string, body = id): AnyRealtimeEventV1 {
  return {
    schemaVersion: 1,
    eventId: `event-${id}-${revision}`,
    type: "message.created",
    scope: { tenantId: "t1", conversationId: "c1", messageId: id },
    actor: { kind: "agent", id: "agent-1" },
    correlation: {},
    occurredAt: at,
    data: {
      message: {
        id,
        conversationId: "c1",
        senderId: "agent-1",
        revision,
        offset: 1,
        type: "chat_message",
        body,
        state: "streaming",
        historyGeneration: "1",
        createdAt: at,
        updatedAt: at,
      },
    },
  } as AnyRealtimeEventV1;
}

describe("local conversation watch registry", () => {
  it("ref-counts one HTTP hydrate without changing transport subscriptions", async () => {
    const projection = new ProjectionEngine();
    let recoverCalls = 0;
    const registry = new ConversationWatchRegistry({
      projection,
      recovery: {
        async recoverConversation() {
          recoverCalls++;
          const commit = {
            conversation: {
              id: "c1",
              state: "open" as const,
              historyGeneration: "1",
              revision: "1",
              updatedAt: at,
            },
            messages: [],
            historyBoundaryOffset: "0",
            latestOffset: "0",
          };
          projection.commitHydration(commit);
          return commit;
        },
      },
    });
    const first = registry.watch("c1");
    const second = registry.watch("c1");
    await Promise.all([first.ready, second.ready]);
    expect(recoverCalls).toBe(1);
    expect(registry.refCount("c1")).toBe(2);
    first.release();
    second.release();
    expect(registry.refCount("c1")).toBe(0);
  });

  it("buffers personal-inbox events during hydrate and replays by entity revision", async () => {
    const projection = new ProjectionEngine();
    let resolve!: () => void;
    const gate = new Promise<void>((done) => { resolve = done; });
    const registry = new ConversationWatchRegistry({
      projection,
      recovery: {
        async recoverConversation() {
          await gate;
          const commit = {
            conversation: {
              id: "c1",
              state: "open" as const,
              historyGeneration: "1",
              revision: "1",
              updatedAt: at,
            },
            messages: [],
            historyBoundaryOffset: "0",
            latestOffset: "0",
          };
          projection.commitHydration(commit);
          return commit;
        },
      },
    });
    const watch = registry.watch("c1");
    expect(registry.accept(event("m1", "2", "new"))).toBe("changed");
    expect(registry.accept(event("m1", "1", "old"))).toBe("changed");
    resolve();
    await watch.ready;
    expect(projection.getSnapshot().messages.m1.body).toBe("new");
  });

  it("projects authorized personal events even without a UI watch", () => {
    const projection = new ProjectionEngine();
    const registry = new ConversationWatchRegistry({
      projection,
      recovery: { recoverConversation: async () => { throw new Error("not called"); } },
    });
    expect(registry.accept(event("m1", "1"))).toBe("changed");
    expect(projection.getSnapshot().messages.m1).toBeDefined();
  });
});
