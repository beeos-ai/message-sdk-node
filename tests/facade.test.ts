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

describe("realtime recovery checkpoint closure", () => {
  it("rebases on every sequence gap, including a full event", async () => {
    const { options, realtime, calls } = makeOptions();
    const client = createMessageClient(options);
    await client.connect();

    realtime.input?.onEvent(created("1"));
    realtime.input?.onEvent(created("3"));
    await tick();

    expect(calls.rebase).toBe(1);
    expect(client.getSnapshot().cursor).toMatchObject({ streamSequence: "3" });
  });

  it("restores and serializes opaque checkpoints without writing an older cursor", async () => {
    const writes: Array<{ eventCursor?: { streamSequence: string }; syncCursor?: string }> = [];
    let concurrentWrites = 0;
    let maximumConcurrentWrites = 0;
    const realtime: { input?: RealtimeConnectInput } = {};
    const options: MessageClientFacadeOptions = {
      transport: {
        async sendMessage(input) { return { messageId: input.clientMessageId, outcome: "created" }; },
        async executeMethod() { return { operationId: "operation-1", outcome: "accepted" }; },
        async hydrateConversation(input) { return { conversationId: input.conversationId, events: [] }; },
        async rebase() { return { events: [] }; },
      },
      realtime: {
        async connect(input) {
          realtime.input = input;
          return { syncCursor: "opaque-issued", close: () => undefined };
        },
      },
      storage: {
        async get() { return undefined; },
        async set() { throw new Error("checkpoint storage should be preferred"); },
        async getCheckpoint() {
          return {
            eventCursor: { streamSequence: "1", historyGeneration: "generation-1" },
            syncCursor: "opaque-restored",
          };
        },
        async setCheckpoint(_key, checkpoint) {
          concurrentWrites++;
          maximumConcurrentWrites = Math.max(maximumConcurrentWrites, concurrentWrites);
          await Promise.resolve();
          writes.push(checkpoint);
          concurrentWrites--;
        },
      },
    };
    const client = createMessageClient(options);
    await client.connect();
    expect(realtime.input?.cursor).toMatchObject({ streamSequence: "1" });
    expect(realtime.input?.syncCursor).toBe("opaque-restored");

    realtime.input?.onEvent(created("2"));
    realtime.input?.onEvent(delta("3"));
    realtime.input?.onEvent(created("2")); // stale; it must never overwrite sequence 3.
    await tick();
    await tick();

    expect(maximumConcurrentWrites).toBe(1);
    expect(writes.at(-1)).toMatchObject({
      eventCursor: { streamSequence: "3" },
      syncCursor: "opaque-restored",
    });
  });

  it("pauses, rebases, then replays buffered newer events in order", async () => {
    let resolveRebase!: (value: { cursor: { streamSequence: string; historyGeneration: string }; events: AnyRealtimeEventV1[] }) => void;
    const rebase = new Promise<{ cursor: { streamSequence: string; historyGeneration: string }; events: AnyRealtimeEventV1[] }>((resolve) => {
      resolveRebase = resolve;
    });
    const realtime: { input?: RealtimeConnectInput } = {};
    const options: MessageClientFacadeOptions = {
      transport: {
        async sendMessage(input) { return { messageId: input.clientMessageId, outcome: "created" }; },
        async executeMethod() { return { operationId: "operation-1", outcome: "accepted" }; },
        async hydrateConversation(input) { return { conversationId: input.conversationId, events: [] }; },
        async rebase() { return rebase; },
      },
      realtime: {
        async connect(input) {
          realtime.input = input;
          return { close: () => undefined };
        },
      },
    };
    const client = createMessageClient(options);
    const seen: string[] = [];
    client.listen({}, (event) => seen.push(event.eventId));
    await client.connect();

    realtime.input?.onEvent(created("1"));
    realtime.input?.onEvent(delta("3")); // starts recovery and is buffered
    realtime.input?.onEvent(delta("4")); // must not interleave with rebase
    resolveRebase({
      cursor: { streamSequence: "3", historyGeneration: "generation-1" },
      events: [delta("2"), delta("3")],
    });
    await tick();
    await tick();

    expect(seen).toEqual(["event-1", "delta-2", "delta-3", "delta-4"]);
    expect(client.getSnapshot().cursor).toMatchObject({ streamSequence: "4" });
  });

  it("fails explicitly rather than falling back when Centrifuge reports unrecovered history", async () => {
    const { options, realtime, calls } = makeOptions();
    const client = createMessageClient(options);
    await client.connect();

    realtime.input?.onRecovery?.({ recoverable: true, recovered: false, positioned: true });
    await tick();

    expect(calls.rebase).toBe(1);
    expect(client.getSnapshot().connection).toBe("connected");
  });
});

function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

describe("realtime recovery failures", () => {
  it("enters an explicit failed state and never selects another transport", async () => {
    const { options, realtime } = makeOptions();
    options.transport.rebase = async () => {
      throw new Error("authority sync unavailable");
    };
    const client = createMessageClient(options);
    await client.connect();

    realtime.input?.onRecovery?.({ recoverable: false, recovered: false, positioned: false });
    await tick();
    await tick();

    expect(client.getSnapshot()).toMatchObject({
      connection: "failed",
      recoveryError: "authority sync unavailable",
    });
  });
});
