import { describe, expect, it } from "vitest";

import type { AnyRealtimeEventV1 } from "../src/protocol/index.js";
import { ProjectionEngine } from "../src/facade/projection.js";
import { RecoveryCoordinator } from "../src/facade/recovery-coordinator.js";

const at = "2026-07-31T00:00:00.000Z";

function messageEvent(
  type: "message.created" | "message.updated" | "message.terminal",
  revision: string,
  body: string,
  historyGeneration = "1",
): AnyRealtimeEventV1 {
  return {
    schemaVersion: 1,
    eventId: `${type}-${revision}`,
    type,
    scope: { tenantId: "t1", conversationId: "c1", messageId: "m1" },
    actor: { kind: "agent", id: "agent-1" },
    correlation: {},
    occurredAt: at,
    data: {
      message: {
        id: "m1",
        conversationId: "c1",
        senderId: "agent-1",
        revision,
        offset: 1,
        type: "chat_message",
        body,
        state: type === "message.terminal" ? "completed" : "streaming",
        historyGeneration,
        createdAt: at,
        updatedAt: at,
      },
    },
  } as AnyRealtimeEventV1;
}

function conversationEvent(generation: string, revision: string): AnyRealtimeEventV1 {
  return {
    schemaVersion: 1,
    eventId: `conversation-${revision}`,
    type: "conversation.updated",
    scope: { tenantId: "t1", conversationId: "c1" },
    actor: { kind: "service", id: "message-service" },
    correlation: {},
    occurredAt: at,
    data: {
      conversation: {
        id: "c1",
        state: "open",
        metadataVersion: revision,
        historyGeneration: generation,
        updatedAt: at,
      },
    },
  } as AnyRealtimeEventV1;
}

describe("entity revision projection", () => {
  it("accepts newer full snapshots and ignores stale entity revisions", () => {
    const projection = new ProjectionEngine();
    expect(projection.apply(messageEvent("message.created", "2", "new"))).toBe("changed");
    expect(projection.apply(messageEvent("message.updated", "1", "old"))).toBe("stale");
    expect(projection.getSnapshot().messages.m1.body).toBe("new");
  });

  it("applies an exact body delta and requests hydrate for a body gap", () => {
    const projection = new ProjectionEngine();
    projection.apply(messageEvent("message.created", "1", "A"));
    const delta = {
      schemaVersion: 1,
      eventId: "delta-2",
      type: "message.delta",
      scope: { tenantId: "t1", conversationId: "c1", messageId: "m1" },
      actor: { kind: "agent", id: "agent-1" },
      correlation: {},
      occurredAt: at,
      data: {
        message: {
          id: "m1",
          conversationId: "c1",
          senderId: "agent-1",
          revision: "2",
          offset: 1,
        },
        bodyFrom: 1,
        bodyAppend: "B",
      },
    } as AnyRealtimeEventV1;
    expect(projection.apply(delta)).toBe("changed");
    expect(projection.getSnapshot().messages.m1.body).toBe("AB");
    expect(projection.apply({
      ...delta,
      eventId: "delta-3",
      data: {
        ...delta.data,
        message: { ...delta.data.message, revision: "3" },
        bodyFrom: 99,
      },
    } as AnyRealtimeEventV1)).toBe("message_delta_gap");
  });

  it("uses historyGeneration as a business fence and clears old messages", () => {
    const projection = new ProjectionEngine();
    projection.apply(conversationEvent("1", "1"));
    projection.apply(messageEvent("message.created", "1", "old", "1"));
    expect(projection.apply(conversationEvent("2", "2"))).toBe("changed");
    expect(projection.getSnapshot().messages).toEqual({});
    expect(projection.apply(messageEvent("message.updated", "3", "late", "1"))).toBe("stale");
  });

  it("reduces typed session/new terminal results by operation revision", () => {
    const projection = new ProjectionEngine();
    const operation = {
      id: "op-1",
      instanceId: "instance-1",
      target: { scope: "instance" as const },
      method: "session/new",
      capability: "session",
      contractRevision: "2026-07-14.3" as const,
      transport: "service" as const,
      sequence: "1",
      status: "succeeded" as const,
      effectState: "committed" as const,
      terminal: true,
      result: { sessionId: "session-1", conversationId: "conversation-1" },
      createdAt: at,
      updatedAt: at,
      revision: "2",
    };
    const event = {
      schemaVersion: 1,
      eventId: "operation-terminal-2",
      type: "operation.terminal",
      scope: { tenantId: "t1", instanceId: "instance-1", operationId: "op-1" },
      actor: { kind: "service", id: "message-service" },
      correlation: { correlationId: "corr-1" },
      occurredAt: at,
      data: { operation },
    } as AnyRealtimeEventV1;
    expect(projection.apply(event)).toBe("changed");
    expect(projection.getSnapshot().operations["op-1"].result).toEqual({
      sessionId: "session-1",
      conversationId: "conversation-1",
    });
  });
});

describe("HTTP recovery", () => {
  it("singleflights concurrent hydrate callers and commits one durable snapshot", async () => {
    const projection = new ProjectionEngine();
    let conversationReads = 0;
    let messageReads = 0;
    const conversation = {
      id: "c1",
      state: "open" as const,
      historyGeneration: "1",
      revision: "1",
      updatedAt: at,
    };
    const recovery = new RecoveryCoordinator({
      conversations: {
        async getConversation() {
          conversationReads++;
          await Promise.resolve();
          return conversation;
        },
      },
      messages: {
        async listMessages() {
          messageReads++;
          return {
            messages: [],
            historyGeneration: "1",
            historyBoundaryOffset: "0",
            latestOffset: "0",
            hasMore: false,
          };
        },
      },
      projection,
    });
    const [left, right] = await Promise.all([
      recovery.recoverConversation("c1"),
      recovery.recoverConversation("c1"),
    ]);
    expect(left).toBe(right);
    expect(conversationReads).toBe(2);
    expect(messageReads).toBe(1);
    expect(projection.getSnapshot().conversations.c1).toEqual(conversation);
  });
});
