import { describe, expect, it } from "vitest";

import {
  ConversationWatchRegistry,
  ProjectionEngine,
  type ConversationHydrationCommit,
  type MessageProjection,
} from "../src/facade/index.js";
import type { RealtimeEventV1 } from "../src/protocol/index.js";

const at = "2026-07-28T00:00:00.000Z";

function message(id: string, offset: string, revision = offset): MessageProjection {
  return {
    id, conversationId: "c1", senderId: "u1", type: "chat_message",
    body: id, state: "streaming", historyGeneration: "1", offset, revision,
    createdAt: at, updatedAt: at,
  };
}

function messageEvent(
  type: "message.created" | "message.terminal",
  id: string,
  offset: string,
  revision: string,
): RealtimeEventV1<"message.created"> | RealtimeEventV1<"message.terminal"> {
  return {
    schemaVersion: 1,
    eventId: `${type}-${id}-${revision}`,
    type,
    scope: { tenantId: "t1", conversationId: "c1", messageId: id },
    actor: { kind: "service", id: "ms" },
    ordering: {
      streamSequence: revision,
      entityRevision: revision,
      messageOffset: offset,
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
        state: type === "message.terminal" ? "completed" : "streaming",
        createdAt: at,
        updatedAt: at,
        historyGeneration: "1",
      },
    },
  } as RealtimeEventV1<"message.created"> | RealtimeEventV1<"message.terminal">;
}

describe("ConversationWatchRegistry recovery boundaries", () => {
  it("fails watch.ready on server authorization rejection before HTTP hydrate", async () => {
    let hydrates = 0;
    const watched: Array<[string, boolean]> = [];
    const registry = new ConversationWatchRegistry({
      projection: new ProjectionEngine(),
      getSession: () => ({
        async setConversationWatched(id, value) {
          watched.push([id, value]);
          if (value) throw new Error("server subscribe 403 permission denied");
        },
        close: () => undefined,
      }),
      recovery: {
        async recoverConversation() {
          hydrates++;
          throw new Error("must not hydrate an unauthorized conversation");
        },
      },
    });
    const watch = registry.watch("guessed-conversation-id");
    await expect(watch.ready).rejects.toThrow("403 permission denied");
    expect(hydrates).toBe(0);
    expect(watched).toEqual([["guessed-conversation-id", true]]);
    expect(registry.refCount("guessed-conversation-id")).toBe(0);
  });

  it("cancels an authorization wait when the last ref releases while pending", async () => {
    let rejectAuthorization!: (error: Error) => void;
    const authorization = new Promise<void>((_resolve, reject) => {
      rejectAuthorization = reject;
    });
    const calls: boolean[] = [];
    const registry = new ConversationWatchRegistry({
      projection: new ProjectionEngine(),
      getSession: () => ({
        setConversationWatched(_id, watched) {
          calls.push(watched);
          if (!watched) rejectAuthorization(new Error("cancelled"));
          return watched ? authorization : undefined;
        },
        close: () => undefined,
      }),
      recovery: {
        async recoverConversation() {
          throw new Error("hydrate must not begin");
        },
      },
    });
    const watch = registry.watch("c1");
    await Promise.resolve();
    watch.release();
    await expect(watch.ready).rejects.toThrow("cancelled");
    expect(calls).toEqual([true, false]);
    expect(registry.refCount("c1")).toBe(0);
  });

  it("rejects a late full event after the last authorized watch releases", async () => {
    const projection = new ProjectionEngine();
    const registry = new ConversationWatchRegistry({
      projection,
      getSession: () => ({
        setConversationWatched: () => undefined,
        close: () => undefined,
      }),
      recovery: {
        async recoverConversation() {
          const commit = {
            conversation: {
              id: "c1",
              state: "open" as const,
              historyGeneration: "1",
              revision: "1",
              updatedAt: at,
            },
            messages: [message("m1", "1")],
            latestOffset: "1",
          };
          projection.commitHydration(commit);
          return commit;
        },
      },
    });
    const watch = registry.watch("c1");
    await watch.ready;
    watch.release();
    await Promise.resolve();
    expect(registry.accept(messageEvent("message.created", "late", "2", "2"))).toBe(false);
    expect(projection.getSnapshot().messages.late).toBeUndefined();
  });

  it("does not re-expose a buffered create at or below committed latestOffset", async () => {
    const projection = new ProjectionEngine();
    let finish!: (commit: ConversationHydrationCommit) => void;
    const pending = new Promise<ConversationHydrationCommit>((resolve) => { finish = resolve; });
    const registry = new ConversationWatchRegistry({
      projection,
      getSession: () => ({ setConversationWatched: () => undefined, close: () => undefined }),
      recovery: {
        recoverConversation: async () => {
          const commit = await pending;
          projection.commitHydration(commit);
          return commit;
        },
      },
    });
    const watch = registry.watch("c1");
    await Promise.resolve();
    registry.accept(messageEvent("message.created", "omitted-by-page", "4", "4"));
    finish({
      conversation: { id: "c1", state: "open", historyGeneration: "1", revision: "5", updatedAt: at },
      messages: [message("latest", "5")],
      latestOffset: "5",
    });
    await watch.ready;
    expect(projection.getSnapshot().messages["omitted-by-page"]).toBeUndefined();
    expect(projection.getSnapshot().messages.latest).toBeDefined();
  });

  it("applies same-offset terminal when entity revision is newer", async () => {
    const projection = new ProjectionEngine();
    let finish!: (commit: ConversationHydrationCommit) => void;
    const pending = new Promise<ConversationHydrationCommit>((resolve) => { finish = resolve; });
    const registry = new ConversationWatchRegistry({
      projection,
      getSession: () => ({ setConversationWatched: () => undefined, close: () => undefined }),
      recovery: {
        recoverConversation: async () => {
          const commit = await pending;
          projection.commitHydration(commit);
          return commit;
        },
      },
    });
    const watch = registry.watch("c1");
    await Promise.resolve();
    registry.accept(messageEvent("message.terminal", "m1", "7", "8"));
    finish({
      conversation: { id: "c1", state: "open", historyGeneration: "1", revision: "7", updatedAt: at },
      messages: [message("m1", "7", "7")],
      latestOffset: "7",
    });
    await watch.ready;
    expect(projection.getSnapshot().messages.m1).toMatchObject({
      offset: "7",
      revision: "8",
      state: "completed",
    });
  });

  it("unsubscribes and removes the entry when recovery fails", async () => {
    const calls: boolean[] = [];
    const registry = new ConversationWatchRegistry({
      projection: new ProjectionEngine(),
      getSession: () => ({
        setConversationWatched: (_id, watched) => { calls.push(watched); },
        close: () => undefined,
      }),
      recovery: {
        recoverConversation: async () => { throw new Error("hydrate failed"); },
      },
    });
    const watch = registry.watch("c1");
    await expect(watch.ready).rejects.toThrow("hydrate failed");
    expect(calls).toEqual([true, false]);
    expect(registry.refCount("c1")).toBe(0);

    const retry = registry.watch("c1");
    await expect(retry.ready).rejects.toThrow("hydrate failed");
    expect(calls).toEqual([true, false, true, false]);
  });
});
