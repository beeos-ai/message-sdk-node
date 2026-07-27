import { describe, expect, it } from "vitest";

import {
  createMessageClient,
  type AnyRealtimeEventV1,
  type MessageClientFacadeOptions,
  type RealtimeConnectInput,
} from "../src/facade/index.js";

function created(sequence = "1"): AnyRealtimeEventV1 {
  return {
    schemaVersion: 1,
    eventId: `event-${sequence}`,
    type: "message.created",
    scope: { tenantId: "tenant-1", conversationId: "conversation-1", messageId: "message-1" },
    actor: { kind: "user", id: "user-1" },
    ordering: { streamSequence: sequence, completeness: "full", historyGeneration: "generation-1" },
    correlation: {},
    occurredAt: "2026-07-28T00:00:00.000Z",
    data: {
      message: {
        id: "message-1",
        conversationId: "conversation-1",
        senderId: "user-1",
        body: "hello",
      },
    },
  };
}

function delta(sequence: string): AnyRealtimeEventV1 {
  return {
    ...created(sequence),
    eventId: `delta-${sequence}`,
    type: "message.delta",
    ordering: { streamSequence: sequence, completeness: "delta", historyGeneration: "generation-1" },
    data: {
      message: { id: "message-1", conversationId: "conversation-1", senderId: "agent-1" },
      bodyAppend: "delta",
      bodyFrom: 0,
    },
  };
}

function makeOptions(): {
  options: MessageClientFacadeOptions;
  calls: { send: number; execute: number; hydrate: number; rebase: number };
  realtime: { input?: RealtimeConnectInput };
} {
  const calls = { send: 0, execute: 0, hydrate: 0, rebase: 0 };
  const realtime: { input?: RealtimeConnectInput } = {};
  return {
    calls,
    realtime,
    options: {
      transport: {
        async sendMessage(input) {
          calls.send++;
          return { messageId: input.clientMessageId, outcome: "created" };
        },
        async executeMethod() {
          calls.execute++;
          return { operationId: "operation-1", outcome: "accepted" };
        },
        async hydrateConversation(input) {
          calls.hydrate++;
          return { conversationId: input.conversationId, events: [] };
        },
        async rebase() {
          calls.rebase++;
          return { cursor: { streamSequence: "3", historyGeneration: "generation-1" }, events: [] };
        },
      },
      realtime: {
        async connect(input) {
          realtime.input = input;
          return { close: () => undefined };
        },
      },
    },
  };
}

describe("platform-neutral MessageClient facade", () => {
  it("keeps message and method commands explicit", async () => {
    const { options, calls } = makeOptions();
    const client = createMessageClient(options);

    await client.messages.send({
      conversationId: "conversation-1",
      clientMessageId: "message-1",
      type: "chat_message",
      content: { text: "hello" },
    });
    await client.methods.execute({
      instanceId: "instance-1",
      method: "instance.start",
      params: {},
      idempotencyKey: "operation-key-1",
    });

    expect(calls).toMatchObject({ send: 1, execute: 1 });
  });

  it("uses one injected realtime session and filters locally without channel input", async () => {
    const { options, realtime } = makeOptions();
    const client = createMessageClient(options);
    const seen: AnyRealtimeEventV1[] = [];
    client.listen({ conversationIds: ["conversation-1"], eventTypes: ["message.created"] }, (event) => {
      seen.push(event);
    });

    await client.connect();
    expect(client.getSnapshot()).toBe(client.getSnapshot());
    expect(realtime.input?.cursor).toBeUndefined();
    realtime.input?.onEvent(created());
    expect(seen).toHaveLength(1);
    expect(client.getSnapshot().connection).toBe("connected");
  });

  it("isolates listener exceptions without blocking other listeners or cursor state", async () => {
    const { options, realtime } = makeOptions();
    const client = createMessageClient(options);
    const delivered: AnyRealtimeEventV1[] = [];
    client.listen({}, () => {
      throw new Error("consumer failure");
    });
    client.listen({}, (event) => delivered.push(event));
    await client.connect();

    expect(() => realtime.input?.onEvent(created("1"))).not.toThrow();
    expect(delivered).toHaveLength(1);
    expect(client.getSnapshot().cursor).toMatchObject({ streamSequence: "1" });
  });

  it("suppresses a matching HTTP/WSS message-create race", async () => {
    const { options, realtime } = makeOptions();
    const client = createMessageClient(options);
    const seen: AnyRealtimeEventV1[] = [];
    client.listen({}, (event) => seen.push(event));
    await client.connect();

    await client.messages.send({
      conversationId: "conversation-1",
      clientMessageId: "message-1",
      type: "chat_message",
      content: { text: "hello" },
    });
    realtime.input?.onEvent(created());
    expect(seen).toHaveLength(0);
  });

  it("ref-counts watches and singleflights the initial hydrate", async () => {
    const { options, calls } = makeOptions();
    const client = createMessageClient(options);
    const first = client.conversations.watch("conversation-1");
    const second = client.conversations.watch("conversation-1");
    await Promise.all([first.ready, second.ready]);

    expect(calls.hydrate).toBe(1);
    expect(client.getSnapshot().watchedConversationIds).toEqual(["conversation-1"]);
    first.release();
    expect(client.getSnapshot().watchedConversationIds).toEqual(["conversation-1"]);
    second.release();
    expect(client.getSnapshot().watchedConversationIds).toEqual([]);
  });

  it("rebases once rather than applying a delta sequence gap", async () => {
    const { options, realtime, calls } = makeOptions();
    const client = createMessageClient(options);
    await client.connect();
    realtime.input?.onEvent(created("1"));
    realtime.input?.onEvent(delta("3"));
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(calls.rebase).toBe(1);
    expect(client.getSnapshot().cursor).toMatchObject({ streamSequence: "3" });
  });
});
