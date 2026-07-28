import { describe, expect, it } from "vitest";

import {
  decodeRealtimeEvent,
  encodeRealtimeEvent,
  evaluateRealtimeEvent,
  RecoveryOwnership,
  RealtimeDedupe,
  RealtimeEventValidationError,
  SingleflightHydrator,
  type RealtimeEventV1,
} from "../src/protocol/index.js";

function messageCreated(sequence = "1"): RealtimeEventV1<"message.created"> {
  return {
    schemaVersion: 1,
    eventId: `event-${sequence}`,
    type: "message.created",
    scope: {
      tenantId: "tenant-1",
      conversationId: "conversation-1",
      messageId: "message-1",
    },
    actor: { kind: "user", id: "user-1" },
    ordering: { streamSequence: sequence, messageOffset: sequence, completeness: "full", historyGeneration: "generation-1" },
    correlation: { requestId: "request-1", idempotencyKeyHash: "hash-1" },
    occurredAt: "2026-07-28T00:00:00.000Z",
    data: {
      message: {
        id: "message-1",
        conversationId: "conversation-1",
        senderId: "user-1",
        type: "chat_message",
        body: "hello",
        state: "completed",
        createdAt: "2026-07-28T00:00:00.000Z",
        updatedAt: "2026-07-28T00:00:00.000Z",
        historyGeneration: "1",
      },
    },
  };
}

function messageDelta(sequence: string): RealtimeEventV1<"message.delta"> {
  return {
    ...messageCreated(sequence),
    eventId: `delta-${sequence}`,
    type: "message.delta",
    ordering: { streamSequence: sequence, messageOffset: sequence, completeness: "delta", historyGeneration: "generation-1" },
    data: {
      message: {
        id: "message-1",
        conversationId: "conversation-1",
        senderId: "agent-1",
      },
      bodyAppend: "world",
      bodyFrom: 0,
    },
  };
}

describe("RealtimeEventV1 protocol", () => {
  it("encodes and decodes the typed event contract", () => {
    const event = messageCreated();
    expect(decodeRealtimeEvent(encodeRealtimeEvent(event))).toEqual(event);
  });

  it("rejects a message event without its scope identity", () => {
    const malformed = messageCreated() as unknown as { scope: Record<string, unknown> };
    delete malformed.scope.messageId;
    expect(() => decodeRealtimeEvent(malformed)).toThrow(RealtimeEventValidationError);
  });

  it("requires delta offset and a stable message identity", () => {
    const malformed = messageDelta("2") as unknown as {
      data: { bodyFrom?: number; message: { conversationId?: string } };
    };
    delete malformed.data.bodyFrom;
    expect(() => decodeRealtimeEvent(malformed)).toThrow(RealtimeEventValidationError);

    const wrongConversation = messageDelta("2") as unknown as {
      data: { message: { conversationId: string } };
    };
    wrongConversation.data.message.conversationId = "other-conversation";
    expect(() => decodeRealtimeEvent(wrongConversation)).toThrow(RealtimeEventValidationError);
  });

  it("requires snapshots for durable conversation and operation events", () => {
    const conversation = {
      ...messageCreated(),
      eventId: "conversation-1",
      type: "conversation.updated" as const,
      scope: { tenantId: "tenant-1", conversationId: "conversation-1" },
      data: {
        conversation: {
          id: "conversation-1",
          state: "open" as const,
          metadataVersion: "3",
          historyGeneration: "1",
          lastActivityAt: "2026-07-28T00:00:00.000Z",
          updatedAt: "2026-07-28T00:00:00.000Z",
        },
      },
    };
    expect(() => decodeRealtimeEvent(conversation)).not.toThrow();

    const modelSet = {
      ...conversation,
      eventId: "conversation-model-set",
      data: { conversation: { ...conversation.data.conversation, modelOverrideId: "provider/model" } },
    };
    expect(decodeRealtimeEvent(modelSet)).toMatchObject({
      data: { conversation: { modelOverrideId: "provider/model" } },
    });
    const modelCleared = {
      ...modelSet,
      eventId: "conversation-model-cleared",
      data: { conversation: { ...conversation.data.conversation, modelOverrideId: null } },
    };
    expect(decodeRealtimeEvent(modelCleared)).toMatchObject({
      data: { conversation: { modelOverrideId: null } },
    });
    const invalidModel = {
      ...modelSet,
      eventId: "conversation-model-invalid",
      data: { conversation: { ...conversation.data.conversation, modelOverrideId: 42 } },
    };
    expect(() => decodeRealtimeEvent(invalidModel)).toThrow(RealtimeEventValidationError);

    const operation = {
      ...messageCreated(),
      eventId: "operation-1",
      type: "operation.terminal" as const,
      scope: { tenantId: "tenant-1", instanceId: "instance-1", operationId: "operation-1" },
      data: {
        operation: {
          id: "operation-1",
          instanceId: "instance-1",
          target: { scope: "instance" as const },
          method: "instance/start",
          capability: "instance",
          contractRevision: "2026-07-14.3" as const,
          transport: "service" as const,
          sequence: "3",
          status: "succeeded" as const,
          effectState: "committed" as const,
          terminal: true,
          result: {},
          createdAt: "2026-07-28T00:00:00.000Z",
          updatedAt: "2026-07-28T00:00:00.000Z",
          revision: "3",
        },
      },
    };
    expect(() => decodeRealtimeEvent(operation)).not.toThrow();
  });

  it("deduplicates repeated WSS events while MessageClient owns HTTP/WSS reconciliation", () => {
    const dedupe = new RealtimeDedupe();
    expect(dedupe.accept(messageCreated())).toBe(true);
    expect(dedupe.accept(messageCreated())).toBe(false);
    expect(dedupe.accept(messageDelta("2"))).toBe(true);
    expect(dedupe.accept(messageDelta("2"))).toBe(false);
  });

  it("singleflights concurrent conversation hydration", async () => {
    const hydrator = new SingleflightHydrator();
    let calls = 0;
    const load = async () => {
      calls++;
      return { revision: 4 };
    };
    const [first, second] = await Promise.all([
      hydrator.hydrate("conversation:1", load),
      hydrator.hydrate("conversation:1", load),
    ]);
    expect(first).toEqual({ revision: 4 });
    expect(second).toEqual({ revision: 4 });
    expect(calls).toBe(1);
  });

  it("requires a rebase on a delta sequence gap and allows only one recovery owner", () => {
    const cursor = { streamSequence: "1", historyGeneration: "generation-1" };
    expect(evaluateRealtimeEvent(cursor, messageDelta("3"))).toEqual({
      action: "rebase",
      reason: "sequence_gap",
    });

    const ownership = new RecoveryOwnership();
    const release = ownership.acquire("sdk-core");
    expect(() => ownership.acquire("ui-layer")).toThrow("already owned");
    release();
    expect(() => ownership.acquire("ui-layer")).not.toThrow();
  });
});
