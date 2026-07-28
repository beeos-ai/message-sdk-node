import { describe, expect, it } from "vitest";

import {
  HistoryGenerationChangedError,
  ProjectionEngine,
  RecoveryCoordinator,
  type ConversationProjection,
  type MessageProjection,
} from "../src/facade/index.js";
import type { AnyRealtimeEventV1, RealtimeEventType } from "../src/protocol/index.js";

const timestamp = "2026-07-28T00:00:00.000Z";

function conversation(generation: string, revision = generation): ConversationProjection {
  return {
    id: "c1",
    state: "open",
    historyGeneration: generation,
    revision,
    updatedAt: timestamp,
  };
}

function message(id: string, generation: string, offset: string, body = ""): MessageProjection {
  return {
    id,
    conversationId: "c1",
    senderId: "u1",
    type: "chat_message",
    body,
    state: "streaming",
    historyGeneration: generation,
    offset,
    revision: offset,
    createdAt: timestamp,
    updatedAt: timestamp,
  };
}

function event<T extends RealtimeEventType>(
  type: T,
  sequence: string,
  generation: string,
  data: Extract<AnyRealtimeEventV1, { type: T }>["data"],
): Extract<AnyRealtimeEventV1, { type: T }> {
  return {
    schemaVersion: 1,
    eventId: `event-${sequence}`,
    type,
    scope: {
      tenantId: "tenant",
      conversationId: "c1",
      ...(["message.created", "message.updated", "message.delta", "message.terminal", "message.deleted"].includes(type)
        ? { messageId: type === "message.deleted" ? "m1" : (data as { message: { id: string } }).message.id }
        : {}),
    },
    actor: { kind: "service", id: "ms" },
    ordering: {
      streamSequence: sequence,
      entityRevision: sequence,
      messageOffset: sequence,
      historyGeneration: generation,
      completeness: type === "message.delta" ? "delta" : "full",
    },
    correlation: {},
    occurredAt: timestamp,
    data,
  } as Extract<AnyRealtimeEventV1, { type: T }>;
}

describe("ProjectionEngine", () => {
  it("keeps stable snapshots until a commit and rejects stale revisions", () => {
    const engine = new ProjectionEngine();
    const initial = engine.getSnapshot();
    expect(engine.getSnapshot()).toBe(initial);

    expect(engine.commitHydration({
      conversation: conversation("1"),
      messages: [message("m1", "1", "1", "hello")],
      latestOffset: "1",
    })).toBe(true);
    const committed = engine.getSnapshot();
    expect(committed).not.toBe(initial);
    expect(engine.getSnapshot()).toBe(committed);

    expect(engine.apply(event("message.updated", "2", "1", {
      message: {
        id: "m1",
        conversationId: "c1",
        senderId: "u1",
        type: "chat_message",
        body: "older",
        state: "streaming",
        createdAt: timestamp,
        updatedAt: timestamp,
        historyGeneration: "1",
      },
    }))).toBe(true);
    const afterUpdate = engine.getSnapshot();
    expect(engine.apply({
      ...event("message.updated", "2", "1", {
        message: {
          id: "m1",
          conversationId: "c1",
          senderId: "u1",
          type: "chat_message",
          body: "duplicate",
          state: "streaming",
          createdAt: timestamp,
          updatedAt: timestamp,
          historyGeneration: "1",
        },
      }),
      eventId: "another-id",
    })).toBe(false);
    expect(engine.getSnapshot()).toBe(afterUpdate);
    expect(engine.getSnapshot().messages.m1?.body).toBe("older");
  });

  it("preserves canonical model override set and explicit clear in conversation projection", () => {
    const engine = new ProjectionEngine();
    expect(engine.apply(event("conversation.updated", "2", "1", {
      conversation: {
        id: "c1",
        modelOverrideId: "provider/model",
        state: "open",
        metadataVersion: "2",
        historyGeneration: "1",
        updatedAt: timestamp,
      },
    }))).toBe(true);
    expect(engine.getSnapshot().conversations.c1?.modelOverrideId).toBe("provider/model");
    expect(engine.apply(event("conversation.updated", "3", "1", {
      conversation: {
        id: "c1",
        modelOverrideId: null,
        state: "open",
        metadataVersion: "3",
        historyGeneration: "1",
        updatedAt: timestamp,
      },
    }))).toBe(true);
    expect(engine.getSnapshot().conversations.c1?.modelOverrideId).toBeNull();
  });

  it("fences old history and atomically removes messages after clear", () => {
    const engine = new ProjectionEngine();
    engine.commitHydration({
      conversation: conversation("1"),
      messages: [message("m1", "1", "1", "old")],
      latestOffset: "1",
    });
    expect(engine.apply(event("conversation.updated", "2", "2", {
      conversation: {
        id: "c1",
        state: "open",
        metadataVersion: "2",
        historyGeneration: "2",
        updatedAt: timestamp,
      },
    }))).toBe(true);
    expect(engine.getSnapshot().messages).toEqual({});
    expect(engine.getSnapshot().latestOffsetByConversation.c1).toBeUndefined();

    expect(engine.apply(event("message.created", "3", "1", {
      message: {
        id: "old-late",
        conversationId: "c1",
        senderId: "u1",
        type: "chat_message",
        body: "must stay hidden",
        state: "completed",
        createdAt: timestamp,
        updatedAt: timestamp,
        historyGeneration: "1",
      },
    }))).toBe(false);
    expect(engine.getSnapshot().messages["old-late"]).toBeUndefined();
  });

  it("applies ordered delta only at the exact body boundary", () => {
    const engine = new ProjectionEngine();
    engine.commitHydration({
      conversation: conversation("1"),
      messages: [message("m1", "1", "1", "中")],
      latestOffset: "1",
    });
    expect(engine.apply(event("message.delta", "2", "1", {
      message: { id: "m1", conversationId: "c1", senderId: "u1" },
      bodyAppend: "文",
      bodyFrom: 3,
    }))).toBe(true);
    expect(engine.getSnapshot().messages.m1?.body).toBe("中文");
    expect(engine.apply(event("message.delta", "3", "1", {
      message: { id: "m1", conversationId: "c1", senderId: "u1" },
      bodyAppend: "skip",
      bodyFrom: 99,
    }))).toBe(false);
    expect(engine.getSnapshot().messages.m1?.body).toBe("中文");
  });
});

describe("RecoveryCoordinator", () => {
  it("singleflights callers, pages to completion, and retries a G1/G2 race", async () => {
    const engine = new ProjectionEngine();
    const generations = ["1", "2", "2", "2"];
    let conversationReads = 0;
    let messageReads = 0;
    const coordinator = new RecoveryCoordinator({
      conversations: {
        getConversation: async () => conversation(generations[conversationReads++] ?? "2"),
      },
      messages: {
        listMessages: async (_id, since) => {
          messageReads++;
          if (!since) return {
            messages: [message(`m-${messageReads}`, messageReads === 1 ? "1" : "2", "1")],
            latestOffset: "2",
            nextSince: "1",
            hasMore: true,
          };
          return {
            messages: [message(`m-${messageReads}`, messageReads <= 2 ? "1" : "2", "2")],
            latestOffset: "2",
            hasMore: false,
          };
        },
      },
      projection: engine,
    });
    const first = coordinator.recoverConversation("c1");
    const second = coordinator.recoverConversation("c1");
    expect(second).toBe(first);
    const [result] = await Promise.all([first, second]);
    expect(result.conversation.historyGeneration).toBe("2");
    expect(conversationReads).toBe(4);
    expect(messageReads).toBe(4);
    expect(Object.values(engine.getSnapshot().messages).every((item) => item.historyGeneration === "2")).toBe(true);
  });

  it("fails closed after two unstable generations", async () => {
    const engine = new ProjectionEngine();
    let generation = 0;
    const coordinator = new RecoveryCoordinator({
      conversations: {
        getConversation: async () => conversation(String(++generation)),
      },
      messages: {
        listMessages: async () => ({ messages: [], latestOffset: "0", hasMore: false }),
      },
      projection: engine,
    });
    await expect(coordinator.recoverConversation("c1")).rejects.toBeInstanceOf(HistoryGenerationChangedError);
    expect(engine.getSnapshot().conversations).toEqual({});
  });
});
