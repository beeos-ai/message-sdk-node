import { describe, expect, it, vi } from "vitest";

import type {
  MessageClientComposition,
  MessageProjection,
  OperationProjection,
  RealtimeConnectInput,
} from "../src/facade/contracts.js";
import { createMessageClient } from "../src/unified-client.js";

const at = "2026-07-31T00:00:00.000Z";

function fixture() {
  let realtimeInput: RealtimeConnectInput | undefined;
  let connectCalls = 0;
  let operationGets = 0;
  let conversationReads = 0;
  let hydratedMessages: MessageProjection[] = [];
  let operation: OperationProjection = {
    id: "op-1",
    instanceId: "instance-1",
    target: { scope: "instance" },
    method: "session.new",
    capability: "session",
    contractRevision: "2026-07-14.3",
    transport: "service",
    sequence: "1",
    status: "running",
    effectState: "running",
    terminal: false,
    createdAt: at,
    updatedAt: at,
    revision: "1",
  };
  const conversation = {
    id: "c1",
    state: "open" as const,
    historyGeneration: "1",
    revision: "1",
    updatedAt: at,
  };
  const composition = {
    conversationQuery: {
      async getConversation() {
        conversationReads++;
        return conversation;
      },
      async listConversations() {
        return { conversations: [], hasMore: false };
      },
    },
    conversationCommand: {
      async createConversation() { return conversation; },
      async updateConversation() { return conversation; },
      async clearConversation() { return conversation; },
      async deleteConversation() {},
    },
    messageQuery: {
      async listMessages() {
        return {
          messages: hydratedMessages,
          historyGeneration: "1",
          historyBoundaryOffset: "0",
          latestOffset: hydratedMessages.at(-1)?.offset ?? "0",
          hasMore: false,
        };
      },
      async reconcileMessage() { return undefined; },
    },
    messageCommand: {
      async sendMessage(command: { clientMessageId: string }) {
        return { messageId: command.clientMessageId, outcome: "accepted" as const };
      },
      async cancelMessage() {},
    },
    messageStream: {
      async startStream(command: { clientMessageId: string }) {
        return { messageId: command.clientMessageId, outcome: "accepted" as const };
      },
      async append() {},
      async finalize() {},
    },
    runtimeMethods: {
      async executeMethod(command: { operationId: string }) {
        return {
          operationId: command.operationId,
          outcome: "accepted" as const,
          contractRevision: "2026-07-14.3" as const,
          correlationId: "corr-1",
        };
      },
      async listActiveOperations() { return { operations: [] }; },
      async getOperation() {
        operationGets++;
        return operation;
      },
      async cancelOperation() { return operation; },
    },
    realtime: {
      async connect(input: RealtimeConnectInput) {
        connectCalls++;
        realtimeInput = input;
        input.onState("connected");
        return { close() {} };
      },
    },
    currentPrincipal: { currentPrincipalId: () => "user:u1" },
  } as unknown as MessageClientComposition;
  return {
    composition,
    get input() { return realtimeInput!; },
    get connectCalls() { return connectCalls; },
    get operationGets() { return operationGets; },
    get conversationReads() { return conversationReads; },
    setHydratedMessages(value: MessageProjection[]) { hydratedMessages = value; },
    setOperation(value: OperationProjection) { operation = value; },
  };
}

function message(
  eventId: string,
  messageId: string,
  conversationId: string,
  revision: string,
  body: string,
) {
  return {
    schemaVersion: 1,
    eventId,
    type: "message.created",
    scope: { tenantId: "t1", conversationId, messageId },
    actor: { kind: "agent", id: "agent-1" },
    correlation: {},
    occurredAt: at,
    data: {
      message: {
        id: messageId,
        conversationId,
        senderId: "agent-1",
        revision,
        offset: 1,
        type: "chat_message",
        body,
        state: "completed",
        historyGeneration: "1",
        createdAt: at,
        updatedAt: at,
      },
    },
  };
}

describe("personal-only UnifiedMessageClient", () => {
  it("owns one physical WSS and keeps conversation listen filtering local", async () => {
    const state = fixture();
    const client = createMessageClient(state.composition);
    const c1Events: string[] = [];
    client.listen({ conversationIds: ["c1"] }, (event) => c1Events.push(event.eventId));
    await Promise.all([client.connect(), client.connect()]);
    expect(state.connectCalls).toBe(1);

    state.input.onEvent(message("event-c2", "m2", "c2", "1", "two"));
    state.input.onEvent(message("event-c1", "m1", "c1", "1", "one"));
    expect(client.getSnapshot().messages.m2).toBeDefined();
    expect(client.getSnapshot().messages.m1).toBeDefined();
    expect(c1Events).toEqual(["event-c1"]);
  });

  it("treats HTTP 202 as queued and reduces typed session.new terminal success", async () => {
    const state = fixture();
    const client = createMessageClient(state.composition);
    await client.connect();
    const receipt = await client.methods.execute({
      operationId: "op-1",
      instanceId: "instance-1",
      target: { scope: "instance" },
      method: "session.new",
      params: {},
      idempotencyKey: "op-1",
    });
    expect(receipt.outcome).toBe("accepted");
    expect(client.getSnapshot().operations["op-1"]).toMatchObject({
      status: "queued",
      terminal: false,
    });

    state.input.onEvent({
      schemaVersion: 1,
      eventId: "operation-terminal",
      type: "operation.terminal",
      scope: {
        tenantId: "t1",
        instanceId: "instance-1",
        operationId: "op-1",
        runtimeEpoch: "epoch-1",
      },
      actor: { kind: "service", id: "message-service" },
      correlation: { correlationId: "corr-1" },
      occurredAt: at,
      data: {
        operation: {
          id: "op-1",
          instanceId: "instance-1",
          target: { scope: "instance" },
          method: "session.new",
          capability: "session",
          contractRevision: "2026-07-14.3",
          transport: "service",
          sequence: "2",
          status: "succeeded",
          effectState: "committed",
          terminal: true,
          result: { sessionId: "session-1", conversationId: "c1" },
          createdAt: at,
          updatedAt: at,
          revision: "2",
          additive: "preserved",
        },
      },
    });
    expect(client.getSnapshot().operations["op-1"]).toMatchObject({
      status: "succeeded",
      terminal: true,
      result: { sessionId: "session-1", conversationId: "c1" },
    });
    await vi.waitFor(() => {
      expect(client.getSnapshot().conversations.c1).toBeDefined();
    });
  });

  it("hydrates one conversation over HTTP on a body delta gap", async () => {
    const state = fixture();
    const client = createMessageClient(state.composition);
    await client.connect();
    state.input.onEvent(message("created-1", "m1", "c1", "1", "A"));
    state.setHydratedMessages([{
      id: "m1",
      conversationId: "c1",
      senderId: "agent-1",
      type: "chat_message",
      body: "authoritative",
      state: "completed",
      historyGeneration: "1",
      offset: "1",
      revision: "3",
      createdAt: at,
      updatedAt: at,
    }]);
    state.input.onEvent({
      schemaVersion: 1,
      eventId: "delta-gap",
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
        },
        bodyFrom: 99,
        bodyAppend: "B",
      },
    });
    await vi.waitFor(() => {
      expect(client.getSnapshot().messages.m1.body).toBe("authoritative");
    });
    expect(state.conversationReads).toBe(2);
  });

  it("rehydrates watched conversations and pending operations after reconnect", async () => {
    const state = fixture();
    const client = createMessageClient(state.composition);
    const watch = client.conversations.watch("c1");
    await watch.ready;
    await client.connect();
    await client.methods.execute({
      operationId: "op-1",
      instanceId: "instance-1",
      target: { scope: "instance" },
      method: "session.new",
      params: {},
      idempotencyKey: "op-1",
    });
    state.setOperation({
      ...(client.getSnapshot().operations["op-1"]),
      status: "running",
      effectState: "running",
      terminal: false,
      revision: "1",
    });
    const readsBefore = state.conversationReads;
    state.input.onState("disconnected");
    state.input.onState("connected");
    await vi.waitFor(() => {
      expect(client.getSnapshot().operations["op-1"].status).toBe("running");
    });
    expect(state.operationGets).toBe(1);
    expect(state.conversationReads).toBeGreaterThan(readsBefore);
  });

  it("deduplicates repeated eventId without inventing transport cursors", async () => {
    const state = fixture();
    const client = createMessageClient(state.composition);
    await client.connect();
    const observed: string[] = [];
    client.listen({}, (event) => observed.push(event.eventId));
    const event = message("same-event", "m1", "c1", "1", "one");
    state.input.onEvent(event);
    state.input.onEvent(event);
    expect(observed).toEqual(["same-event"]);
    expect(client.getSnapshot().messages.m1.body).toBe("one");
  });
});
