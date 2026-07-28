import { describe, expect, it, vi } from "vitest";

import { OutcomeUnknownError } from "../src/errors.js";
import {
  createMessageClient,
  type MessageClient,
} from "../src/unified-client.js";
import type {
  DomainProjectionSnapshot,
  MessageClientComposition,
  MessageProjection,
  ProjectionCheckpoint,
  RealtimeConnectInput,
} from "../src/facade/contracts.js";
import type { RealtimeEventV1 } from "../src/protocol/index.js";

const at = "2026-07-28T00:00:00.000Z";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

function emptyProjection(): DomainProjectionSnapshot {
  return {
    conversations: {},
    messages: {},
    operations: {},
    latestOffsetByConversation: {},
    hydrationByConversation: {},
  };
}

function projectionMessage(id: string): MessageProjection {
  return {
    id, conversationId: "c1", senderId: "u1", type: "chat_message", body: id,
    state: "completed", historyGeneration: "1", offset: "1", revision: "1",
    createdAt: at, updatedAt: at,
  };
}

function operation(id: string) {
  return {
    id,
    instanceId: "i1",
    target: { scope: "instance" as const },
    method: "instance/start",
    capability: "instance",
    contractRevision: "2026-07-14.3" as const,
    transport: "service" as const,
    sequence: "1",
    status: "running" as const,
    effectState: "running" as const,
    terminal: false,
    createdAt: at,
    updatedAt: at,
    revision: "1",
  };
}

function created(
  id = "m2",
  sequence = "2",
  senderId = "u1",
): RealtimeEventV1<"message.created"> {
  return {
    schemaVersion: 1,
    eventId: `e-${id}`,
    type: "message.created",
    scope: { tenantId: "t1", conversationId: "c1", messageId: id },
    actor: { kind: "user", id: "u1" },
    ordering: {
      streamSequence: sequence, entityRevision: sequence, messageOffset: sequence,
      historyGeneration: "1", completeness: "full",
    },
    correlation: {},
    occurredAt: at,
    data: {
      message: {
        id, conversationId: "c1", senderId, type: "chat_message",
        body: id, state: "completed", createdAt: at, updatedAt: at, historyGeneration: "1",
      },
    },
  };
}

function terminal(
  id: string,
  sequence: string,
  revision: string,
  senderId = "u1",
): RealtimeEventV1<"message.terminal"> {
  return {
    ...created(id, sequence, senderId),
    eventId: `terminal-${id}`,
    type: "message.terminal",
    actor: { kind: "service", id: "message-service" },
    ordering: {
      ...created(id, sequence).ordering,
      entityRevision: revision,
      messageOffset: revision,
    },
    data: {
      message: {
        ...created(id, sequence, senderId).data.message,
        state: "completed",
      },
    },
  };
}

function instanceUpdated(sequence: string): RealtimeEventV1<"instance.updated"> {
  return {
    schemaVersion: 1,
    eventId: `instance-${sequence}`,
    type: "instance.updated",
    scope: { tenantId: "t1", instanceId: "i1" },
    actor: { kind: "service", id: "instance-service" },
    ordering: {
      streamSequence: sequence,
      entityRevision: sequence,
      completeness: "full",
    },
    correlation: {},
    occurredAt: at,
    data: { instanceId: "i1", status: "running" },
  };
}

function inboxControl(
  type: "inbox.conversation.available" | "inbox.conversation.unavailable",
  sequence: string,
): RealtimeEventV1<typeof type> {
  return {
    schemaVersion: 1,
    eventId: `${type}-${sequence}`,
    type,
    scope: { tenantId: "t1" },
    actor: { kind: "service", id: "message-service" },
    ordering: {
      streamSequence: sequence,
      entityRevision: sequence,
      completeness: "full",
    },
    correlation: {},
    occurredAt: at,
    data: { conversationId: "c1" },
  };
}

function runtimeDispatchFailed(eventId = "dispatch-failed-1"): RealtimeEventV1<"runtime.dispatch.failed"> {
  return {
    schemaVersion: 1,
    eventId,
    type: "runtime.dispatch.failed",
    scope: { tenantId: "t1", conversationId: "c1", messageId: "m1" },
    actor: { kind: "service", id: "message-service" },
    ordering: { streamSequence: "0", completeness: "delta" },
    correlation: { correlationId: "corr-1" },
    occurredAt: at,
    data: { status: "unconfirmed", code: "delivery_unconfirmed" },
  };
}

function createdForConversation(
  conversationId: string,
  messageId: string,
): RealtimeEventV1<"message.created"> {
  const value = created(messageId, "1", "u1");
  value.scope.conversationId = conversationId;
  value.scope.messageId = messageId;
  value.ordering.entityRevision = "2";
  value.ordering.messageOffset = "2";
  value.data.message.conversationId = conversationId;
  return value;
}

function composition(): {
  value: MessageClientComposition;
  realtime: { input?: RealtimeConnectInput; subscriptions: boolean[] };
  calls: { send: number; execute: number; reconcile: number };
} {
  const realtime: { input?: RealtimeConnectInput; subscriptions: boolean[] } = { subscriptions: [] };
  const calls = { send: 0, execute: 0, reconcile: 0 };
  return {
    realtime,
    calls,
    value: {
      conversationQuery: {
        async getConversation(id) {
          return { id, state: "open", historyGeneration: "1", revision: "1", updatedAt: at };
        },
        async listConversations() { return { conversations: [], hasMore: false }; },
      },
      conversationCommand: {
        async createConversation() {
          return { id: "c1", state: "open", historyGeneration: "1", revision: "1", updatedAt: at };
        },
        async updateConversation(command) {
          return { id: command.conversationId, state: "open", historyGeneration: "1", revision: "1", updatedAt: at };
        },
        async clearConversation(id) {
          return { id, state: "open", historyGeneration: "2", revision: "2", updatedAt: at };
        },
        async deleteConversation() {},
      },
      messageQuery: {
        async listMessages() {
          return {
            messages: [projectionMessage("m1")],
            historyGeneration: "1",
            historyBoundaryOffset: "0",
            latestOffset: "1",
            hasMore: false,
          };
        },
        async reconcileMessage() { calls.reconcile++; return undefined; },
      },
      messageCommand: {
        async sendMessage(command) {
          calls.send++;
          return { messageId: command.clientMessageId, outcome: "created" };
        },
        async cancelMessage() {},
      },
      messageStream: {
        async startStream(command) { return { messageId: command.clientMessageId, outcome: "created" }; },
        async append() {},
        async finalize() {},
      },
      runtimeMethods: {
        async executeMethod(command) {
          calls.execute++;
          return {
            operationId: command.operationId,
            outcome: "accepted",
            contractRevision: "2026-07-14.3",
          };
        },
        async listActiveOperations() { return { operations: [operation("op1")] }; },
        async getOperation(id) { return operation(id); },
        async cancelOperation(id) {
          return {
            ...operation(id),
            sequence: "2",
            status: "cancelled",
            effectState: "cancelled",
            terminal: true,
            revision: "2",
          };
        },
      },
      currentPrincipal: {
        currentPrincipalId: () => "u1",
      },
      realtime: {
        async connect(input) {
          realtime.input = input;
          return {
            setConversationWatched: (_id, watched) => { realtime.subscriptions.push(watched); },
            close: () => undefined,
          };
        },
      },
    },
  };
}

describe("UnifiedMessageClient", () => {
  it("queues an initial offline connection request and opens exactly once when active online", async () => {
    const fixture = composition();
    let state: "active" | "background" | "inactive" = "inactive";
    let online = false;
    let lifecycleListener: (() => void) | undefined;
    const connect = vi.fn(fixture.value.realtime.connect.bind(fixture.value.realtime));
    const closeStore = vi.fn(async () => undefined);
    fixture.value.realtime.connect = connect;
    fixture.value.lifecycle = {
      current: () => state,
      isOnline: () => online,
      subscribe(listener) {
        lifecycleListener = listener;
        return () => { lifecycleListener = undefined; };
      },
    };
    fixture.value.projectionStore = {
      async loadCheckpoint() { return undefined; },
      async commitCheckpoint() {},
      close: closeStore,
    };
    const client = createMessageClient(fixture.value);

    await expect(client.connect()).resolves.toBeUndefined();
    expect(client.getSnapshot().connection).toBe("disconnected");
    expect(connect).not.toHaveBeenCalled();

    online = true;
    state = "background";
    lifecycleListener?.();
    await Promise.resolve();
    expect(connect).not.toHaveBeenCalled();

    state = "active";
    lifecycleListener?.();
    lifecycleListener?.();
    await client.connect();
    await vi.waitFor(() => expect(client.getSnapshot().connection).toBe("connected"));
    expect(connect).toHaveBeenCalledOnce();

    await client.disconnect();
    expect(closeStore).toHaveBeenCalledOnce();
    lifecycleListener?.();
    await Promise.resolve();
    expect(connect).toHaveBeenCalledOnce();
    await client.disconnect();
    expect(closeStore).toHaveBeenCalledOnce();
  });

  it("queues an initial background connection and opens on foreground without replacing the client", async () => {
    const fixture = composition();
    let state: "active" | "background" | "inactive" = "background";
    let lifecycleListener: (() => void) | undefined;
    const connect = vi.fn(fixture.value.realtime.connect.bind(fixture.value.realtime));
    fixture.value.realtime.connect = connect;
    fixture.value.lifecycle = {
      current: () => state,
      isOnline: () => true,
      subscribe(listener) {
        lifecycleListener = listener;
        return () => { lifecycleListener = undefined; };
      },
    };
    const client = createMessageClient(fixture.value);

    await client.connect();
    expect(connect).not.toHaveBeenCalled();
    state = "active";
    lifecycleListener?.();
    await vi.waitFor(() => expect(client.getSnapshot().connection).toBe("connected"));
    expect(connect).toHaveBeenCalledOnce();

    await client.disconnect();
  });

  it("closes an in-flight session that arrives after backgrounding and opens one fresh foreground session", async () => {
    const fixture = composition();
    let state: "active" | "background" | "inactive" = "active";
    let lifecycleListener: (() => void) | undefined;
    const firstSession = deferred<{
      setConversationWatched: () => void;
      close: () => Promise<void>;
    }>();
    const firstClose = vi.fn(async () => undefined);
    const secondClose = vi.fn(async () => undefined);
    const connect = vi.fn()
      .mockImplementationOnce(() => firstSession.promise)
      .mockResolvedValueOnce({
        setConversationWatched: () => undefined,
        close: secondClose,
      });
    fixture.value.realtime.connect = connect;
    fixture.value.lifecycle = {
      current: () => state,
      isOnline: () => true,
      subscribe(listener) {
        lifecycleListener = listener;
        return () => { lifecycleListener = undefined; };
      },
    };
    const client = createMessageClient(fixture.value);

    const connecting = client.connect();
    await vi.waitFor(() => expect(connect).toHaveBeenCalledOnce());
    state = "background";
    lifecycleListener?.();
    firstSession.resolve({
      setConversationWatched: () => undefined,
      close: firstClose,
    });
    await connecting;

    expect(firstClose).toHaveBeenCalledOnce();
    expect(client.getSnapshot().connection).toBe("disconnected");

    state = "active";
    lifecycleListener?.();
    await vi.waitFor(() => expect(client.getSnapshot().connection).toBe("connected"));
    expect(connect).toHaveBeenCalledTimes(2);
    expect(secondClose).not.toHaveBeenCalled();

    await client.disconnect();
    expect(secondClose).toHaveBeenCalledOnce();
  });

  it("owns one session, watch recovery, projection, stable snapshot and listeners", async () => {
    const fixture = composition();
    const client: MessageClient = createMessageClient(fixture.value);
    let notifications = 0;
    client.subscribe(() => { notifications++; });
    const events: string[] = [];
    client.listen({ conversationIds: ["c1"] }, (event) => events.push(event.eventId));

    await client.connect();
    const watch = client.conversations.watch("c1");
    await watch.ready;
    expect(fixture.realtime.subscriptions).toEqual([true]);
    expect(client.getSnapshot().messages.m1).toBeDefined();
    const stable = client.getSnapshot();
    expect(client.getSnapshot()).toBe(stable);

    fixture.realtime.input?.onEvent(
      created(),
      { kind: "conversation", conversationId: "c1" },
    );
    expect(client.getSnapshot().messages.m2).toBeDefined();
    expect(events).toEqual(["e-m2"]);
    expect(notifications).toBeGreaterThan(0);

    watch.release();
    await Promise.resolve();
    expect(fixture.realtime.subscriptions).toEqual([true, false]);
    await client.disconnect();
    expect(client.getSnapshot().connection).toBe("disconnected");
  });

  it("keeps messages and methods as distinct commands", async () => {
    const fixture = composition();
    const client = createMessageClient(fixture.value);
    await client.messages.send({
      conversationId: "c1", clientMessageId: "cm1", idempotencyKey: "cm1",
      type: "chat_message", content: { text: "hello" },
    });
    await client.methods.execute({
      operationId: "op1",
      instanceId: "i1",
      target: { scope: "instance" },
      method: "instance/start",
      params: {},
      idempotencyKey: "op-key",
    });
    expect(fixture.calls).toMatchObject({ send: 1, execute: 1 });
  });

  it("delivers ephemeral runtime dispatch failure once without cursor, projection or checkpoint mutation", async () => {
    const fixture = composition();
    const commits: ProjectionCheckpoint[] = [];
    fixture.value.projectionStore = {
      async loadCheckpoint() { return undefined; },
      async commitCheckpoint(checkpoint) { commits.push(checkpoint); },
    };
    const client = createMessageClient(fixture.value);
    await client.connect();
    const watch = client.conversations.watch("c1");
    await watch.ready;
    await vi.waitFor(() => expect(commits.length).toBeGreaterThan(0));
    commits.length = 0;
    const before = client.getSnapshot();
    const listener = vi.fn();
    client.listen({
      conversationIds: ["c1"],
      eventTypes: ["runtime.dispatch.failed"],
    }, listener);

    const event = runtimeDispatchFailed();
    fixture.realtime.input?.onEvent(event, {
      kind: "conversation",
      conversationId: "c1",
    });
    fixture.realtime.input?.onEvent(event, {
      kind: "conversation",
      conversationId: "c1",
    });
    await Promise.resolve();

    expect(listener).toHaveBeenCalledOnce();
    expect(listener).toHaveBeenCalledWith(event);
    expect(client.getSnapshot()).toBe(before);
    expect(commits).toHaveLength(0);
    watch.release();
    await client.disconnect();
  });

  it("uses only senderId plus current principal for optimistic/echo authorship", async () => {
    const fixture = composition();
    const client = createMessageClient(fixture.value);
    await client.connect();
    const watch = client.conversations.watch("c1");
    await watch.ready;
    await client.messages.send({
      conversationId: "c1",
      clientMessageId: "own-message",
      idempotencyKey: "own-message",
      type: "chat_message",
      content: { text: "hello" },
    });
    expect(client.getSnapshot().messages["own-message"]).toMatchObject({
      senderId: "u1",
      state: "optimistic",
    });
    expect(client.messages.isMine("own-message")).toBe(true);

    fixture.realtime.input!.onEvent(
      created("own-message", "1", "u1"),
      { kind: "conversation", conversationId: "c1" },
    );
    expect(
      Object.keys(client.getSnapshot().messages).filter((id) => id === "own-message"),
    ).toEqual(["own-message"]);
    expect(client.messages.isMine(client.getSnapshot().messages["own-message"])).toBe(true);

    fixture.realtime.input!.onEvent(
      created("agent-reply", "2", "agent-a"),
      { kind: "conversation", conversationId: "c1" },
    );
    expect(client.getSnapshot().messages["agent-reply"].senderId).toBe("agent-a");
    expect(client.messages.isMine("agent-reply")).toBe(false);
    watch.release();
  });

  it("treats the same senderId as mine on multiple devices of one principal", async () => {
    const leftFixture = composition();
    const rightFixture = composition();
    const left = createMessageClient(leftFixture.value);
    const right = createMessageClient(rightFixture.value);
    await Promise.all([left.connect(), right.connect()]);
    const [leftWatch, rightWatch] = [
      left.conversations.watch("c1"),
      right.conversations.watch("c1"),
    ];
    await Promise.all([leftWatch.ready, rightWatch.ready]);
    const echo = created("multi-device", "1", "u1");
    leftFixture.realtime.input!.onEvent(echo, {
      kind: "conversation",
      conversationId: "c1",
    });
    rightFixture.realtime.input!.onEvent(echo, {
      kind: "conversation",
      conversationId: "c1",
    });
    expect(left.messages.isMine("multi-device")).toBe(true);
    expect(right.messages.isMine("multi-device")).toBe(true);
    leftWatch.release();
    rightWatch.release();
  });

  it("fails send closed when the current principal is unresolved", async () => {
    const fixture = composition();
    fixture.value.currentPrincipal = { currentPrincipalId: () => "" };
    const client = createMessageClient(fixture.value);
    await expect(client.messages.send({
      conversationId: "c1",
      clientMessageId: "must-not-send",
      idempotencyKey: "must-not-send",
      type: "chat_message",
      content: {},
    })).rejects.toThrow("current principal is not resolved");
    expect(fixture.calls.send).toBe(0);
    expect(client.getSnapshot().messages).toEqual({});
  });

  it("marks outcome_unknown and never replays automatically", async () => {
    const fixture = composition();
    fixture.value.messageCommand.sendMessage = async (command) => {
      fixture.calls.send++;
      throw new OutcomeUnknownError({
        phase: "open",
        conversationId: command.conversationId,
        messageId: command.clientMessageId,
        idempotencyKey: command.idempotencyKey,
        cause: new Error("connection reset"),
      });
    };
    const client = createMessageClient(fixture.value);
    const command = {
      conversationId: "c1", clientMessageId: "same-key", idempotencyKey: "same-key",
      type: "chat_message", content: { text: "hello" },
    } as const;
    await expect(client.messages.send(command)).rejects.toBeInstanceOf(OutcomeUnknownError);
    expect(fixture.calls.send).toBe(1);
    expect(client.getSnapshot().messages["same-key"]).toMatchObject({
      state: "outcome_unknown",
      idempotencyKey: "same-key",
    });
    await Promise.resolve();
    expect(fixture.calls.send).toBe(1);
  });

  it("explicit retry reconciles before reusing the same key", async () => {
    const fixture = composition();
    fixture.value.messageQuery.reconcileMessage = async (_id, key) => {
      fixture.calls.reconcile++;
      expect(key).toBe("same-key");
      return projectionMessage("server-message");
    };
    const client = createMessageClient(fixture.value);
    const result = await client.messages.retry({
      conversationId: "c1", clientMessageId: "same-key", idempotencyKey: "same-key",
      type: "chat_message", content: { text: "hello" },
    });
    expect(result).toEqual({ messageId: "server-message", outcome: "duplicate" });
    expect(fixture.calls).toMatchObject({ reconcile: 1, send: 0 });
  });

  it("fails mismatched caller and authoritative message identifiers closed", async () => {
    const fixture = composition();
    const client = createMessageClient(fixture.value);
    await expect(client.messages.send({
      conversationId: "c1",
      clientMessageId: "client-id",
      idempotencyKey: "different-key",
      type: "chat_message",
      content: {},
    })).rejects.toThrow("must be identical");
    expect(fixture.calls.send).toBe(0);

    fixture.value.messageCommand.sendMessage = async () => {
      fixture.calls.send++;
      return { messageId: "different-server-id", outcome: "created" };
    };
    await expect(client.messages.send({
      conversationId: "c1",
      clientMessageId: "same-id",
      idempotencyKey: "same-id",
      type: "chat_message",
      content: {},
    })).rejects.toThrow("must use clientMessageId");
    expect(fixture.calls.send).toBe(1);
    expect(client.getSnapshot().messages["same-id"]).toBeUndefined();
  });

  it("collapses optimistic, WSS echo before HTTP receipt, and receipt into one row", async () => {
    const fixture = composition();
    let resolveReceipt!: (receipt: { messageId: string; outcome: "created" }) => void;
    fixture.value.messageCommand.sendMessage = async () => {
      fixture.calls.send++;
      return new Promise((resolve) => { resolveReceipt = resolve; });
    };
    const client = createMessageClient(fixture.value);
    await client.connect();
    const watch = client.conversations.watch("c1");
    await watch.ready;
    const sending = client.messages.send({
      conversationId: "c1",
      clientMessageId: "race-id",
      idempotencyKey: "race-id",
      type: "chat_message",
      content: { text: "race" },
    });
    fixture.realtime.input!.onEvent(
      created("race-id", "2", "u1"),
      { kind: "conversation", conversationId: "c1" },
    );
    resolveReceipt({ messageId: "race-id", outcome: "created" });
    await sending;
    expect(
      Object.keys(client.getSnapshot().messages).filter((id) => id === "race-id"),
    ).toEqual(["race-id"]);
    expect(client.getSnapshot().messages["race-id"]).toMatchObject({
      senderId: "u1",
      state: "completed",
    });
    watch.release();
  });

  it("keeps an HTTP-first receipt optimistic until the authoritative WSS echo", async () => {
    const fixture = composition();
    const client = createMessageClient(fixture.value);
    await client.connect();
    const watch = client.conversations.watch("c1");
    await watch.ready;
    await client.messages.send({
      conversationId: "c1",
      clientMessageId: "http-first",
      idempotencyKey: "http-first",
      type: "chat_message",
      content: { text: "hello" },
    });
    expect(client.getSnapshot().messages["http-first"]).toMatchObject({
      state: "optimistic",
      offset: "0",
      revision: "0",
      historyGeneration: "1",
    });

    fixture.realtime.input!.onEvent(
      created("http-first", "2", "u1"),
      { kind: "conversation", conversationId: "c1" },
    );
    expect(
      Object.keys(client.getSnapshot().messages).filter((id) => id === "http-first"),
    ).toEqual(["http-first"]);
    expect(client.getSnapshot().messages["http-first"]).toMatchObject({
      state: "completed",
      offset: "2",
      revision: "2",
      historyGeneration: "1",
    });
    watch.release();
  });

  it("reconciles outcome_unknown with a later authoritative WSS echo without replay", async () => {
    const fixture = composition();
    fixture.value.messageCommand.sendMessage = async (command) => {
      fixture.calls.send++;
      throw new OutcomeUnknownError({
        phase: "send",
        conversationId: command.conversationId,
        messageId: command.clientMessageId,
        idempotencyKey: command.idempotencyKey,
        cause: new Error("response lost"),
      });
    };
    const client = createMessageClient(fixture.value);
    await client.connect();
    const watch = client.conversations.watch("c1");
    await watch.ready;
    await expect(client.messages.send({
      conversationId: "c1",
      clientMessageId: "unknown-id",
      idempotencyKey: "unknown-id",
      type: "chat_message",
      content: {},
    })).rejects.toBeInstanceOf(OutcomeUnknownError);
    fixture.realtime.input!.onEvent(
      created("unknown-id", "2", "u1"),
      { kind: "conversation", conversationId: "c1" },
    );
    expect(client.getSnapshot().messages["unknown-id"].state).toBe("completed");
    expect(fixture.calls.send).toBe(1);
    watch.release();
  });

  it("publishes one stable projection when durable hydrate and buffered WSS contain the same message", async () => {
    const fixture = composition();
    fixture.value.conversationQuery.listConversations = async () => ({
      conversations: [{
        id: "c1", state: "open", historyGeneration: "1", revision: "1", updatedAt: at,
      }],
      hasMore: false,
    });
    fixture.value.privateConversationDirectoryQuery = {
      listPrivateConversations: async (state) => ({
        conversations: state === "open"
          ? [{
              id: "c1", state: "open", historyGeneration: "1", revision: "1", updatedAt: at,
            }]
          : [],
        hasMore: false,
      }),
    };
    fixture.value.realtime.connect = async (input) => {
      fixture.realtime.input = input;
      input.onEvent(
        created("m1", "1"),
        { kind: "conversation", conversationId: "c1" },
      );
      return { setConversationWatched: () => undefined, close: () => undefined };
    };
    const client = createMessageClient(fixture.value);
    let appearances = 0;
    let visible = false;
    client.subscribe(() => {
      const next = Boolean(client.getSnapshot().messages.m1);
      if (next && !visible) appearances++;
      visible = next;
    });
    await client.connect();
    expect(client.getSnapshot().messages.m1).toBeDefined();
    expect(Object.keys(client.getSnapshot().messages)).toEqual(["m1"]);
    expect(appearances).toBe(1);
  });

  it("discovers an offline-created conversation through SDK-owned durable recovery on reconnect", async () => {
    const fixture = composition();
    let offlineConversationExists = false;
    fixture.value.conversationQuery.listConversations = async () => ({
      conversations: offlineConversationExists
        ? [{ id: "c2", state: "open", historyGeneration: "3", revision: "4", updatedAt: at }]
        : [],
      hasMore: false,
    });
    fixture.value.privateConversationDirectoryQuery = {
      listPrivateConversations: async (state) => ({
        conversations: state === "open" && offlineConversationExists
          ? [{ id: "c2", state: "open", historyGeneration: "3", revision: "4", updatedAt: at }]
          : [],
        hasMore: false,
      }),
    };
    fixture.value.conversationQuery.getConversation = async (id) => ({
      id, state: "open", historyGeneration: "3", revision: "4", updatedAt: at,
    });
    fixture.value.messageQuery.listMessages = async (id) => ({
      messages: [{
        id: "offline-m1", conversationId: id, type: "chat_message", body: "missed",
        state: "completed", historyGeneration: "3", offset: "8", revision: "9",
        createdAt: at, updatedAt: at,
      }],
      historyGeneration: "3",
      historyBoundaryOffset: "0",
      latestOffset: "8",
      hasMore: false,
    });
    const client = createMessageClient(fixture.value);
    await client.connect();
    expect(client.getSnapshot().conversations.c2).toBeUndefined();
    await client.disconnect();

    offlineConversationExists = true;
    await client.connect();
    expect(client.getSnapshot().conversations.c2).toBeDefined();
    expect(client.getSnapshot().messages["offline-m1"]).toMatchObject({
      conversationId: "c2",
      revision: "9",
    });
  });

  it("fails connect when an invalid event arrives in the startup recovery buffer", async () => {
    const fixture = composition();
    fixture.value.realtime.connect = async (input) => {
      input.onEvent({ schemaVersion: 99 }, { kind: "private-control" });
      return { setConversationWatched: () => undefined, close: () => undefined };
    };
    const client = createMessageClient(fixture.value);
    await expect(client.connect()).rejects.toThrow();
    expect(client.getSnapshot().connection).toBe("failed");
    expect(client.getSnapshot().recoveryError).toBeTruthy();
  });

  it("fails closed on a private-control sequence gap buffered during startup", async () => {
    const fixture = composition();
    fixture.value.projectionStore = {
      async loadCheckpoint() {
        return {
          projection: emptyProjection(),
          cursors: {
            privateControl: { streamSequence: "1", historyGeneration: "1" },
            conversations: {},
          },
        };
      },
      async commitCheckpoint() {},
    };
    fixture.value.realtime.connect = async (input) => {
      input.onEvent(instanceUpdated("3"), { kind: "private-control" });
      return { setConversationWatched: () => undefined, close: () => undefined };
    };
    const client = createMessageClient(fixture.value);
    await expect(client.connect()).rejects.toThrow("buffered realtime gap");
    expect(client.getSnapshot().connection).toBe("failed");
    expect(client.getSnapshot().recoveryError).toContain("sequence_gap");
  });

  it("persists private-control and conversation cursors in atomic projection checkpoints", async () => {
    const fixture = composition();
    const committed: ProjectionCheckpoint[] = [];
    fixture.value.projectionStore = {
      async loadCheckpoint() {
        return {
          projection: emptyProjection(),
          cursors: {
            privateControl: { streamSequence: "9", historyGeneration: "1" },
            conversations: {
              c1: { streamSequence: "1", historyGeneration: "1" },
            },
          },
        };
      },
      async commitCheckpoint(checkpoint) { committed.push(checkpoint); },
    };
    const client = createMessageClient(fixture.value);
    await client.connect();
    const watch = client.conversations.watch("c1");
    await watch.ready;

    const privateControl = instanceUpdated("10");
    const conversation = created("same-id", "2");
    fixture.realtime.input!.onEvent(privateControl, { kind: "private-control" });
    fixture.realtime.input!.onEvent(conversation, { kind: "conversation", conversationId: "c1" });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(committed.at(-1)?.cursors).toMatchObject({
      privateControl: { streamSequence: "10" },
      conversations: { c1: { streamSequence: "2" } },
    });
    expect(committed.at(-1)?.projection.messages["same-id"]).toBeDefined();

    const nextPrivateControl = instanceUpdated("11");
    const nextConversation = created("conversation-next", "3");
    fixture.realtime.input!.onEvent(nextPrivateControl, { kind: "private-control" });
    fixture.realtime.input!.onEvent(nextConversation, {
      kind: "conversation",
      conversationId: "c1",
    });
    expect(client.getSnapshot().connection).toBe("connected");
  });

  it("restores projection and cursor from the same crash-consistent checkpoint", async () => {
    let durable: ProjectionCheckpoint | undefined;
    const first = composition();
    first.value.projectionStore = {
      async loadCheckpoint() { return durable; },
      async commitCheckpoint(checkpoint) { durable = checkpoint; },
    };
    const firstClient = createMessageClient(first.value);
    await firstClient.connect();
    const watch = firstClient.conversations.watch("c1");
    await watch.ready;
    await vi.waitFor(() => { expect(durable?.projection.messages.m1).toBeDefined(); });

    const commitsBeforeEvent: ProjectionCheckpoint[] = [];
    first.value.projectionStore.commitCheckpoint = async (checkpoint) => {
      commitsBeforeEvent.push(checkpoint);
      durable = checkpoint;
    };
    first.realtime.input!.onEvent(created("atomic-m2", "2"), {
      kind: "conversation",
      conversationId: "c1",
    });
    await vi.waitFor(() => { expect(commitsBeforeEvent).toHaveLength(1); });
    expect(durable?.projection.messages["atomic-m2"]).toBeDefined();
    expect(durable?.cursors.conversations.c1?.streamSequence).toBe("2");
    await firstClient.disconnect();

    const second = composition();
    const restoredCommits: ProjectionCheckpoint[] = [];
    second.value.projectionStore = {
      async loadCheckpoint() { return durable; },
      async commitCheckpoint(checkpoint) {
        restoredCommits.push(checkpoint);
        durable = checkpoint;
      },
    };
    const secondClient = createMessageClient(second.value);
    const delivered: string[] = [];
    secondClient.listen({ conversationIds: ["c1"] }, (event) => delivered.push(event.eventId));
    await secondClient.connect();
    expect(secondClient.getSnapshot().messages["atomic-m2"]).toBeDefined();
    const commitsAfterConnect = restoredCommits.length;

    second.realtime.input!.onEvent(created("atomic-m2", "2"), {
      kind: "conversation",
      conversationId: "c1",
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(delivered).toEqual([]);
    expect(restoredCommits).toHaveLength(commitsAfterConnect);
  });

  it("starts private inbox HTTP recovery only after the WSS connect gate resolves", async () => {
    const fixture = composition();
    const directoryStates: string[] = [];
    fixture.value.privateConversationDirectoryQuery = {
      listPrivateConversations: async (state) => {
        directoryStates.push(state);
        return { conversations: [], hasMore: false };
      },
    };
    let resolveWss!: (session: {
      setConversationWatched(): void;
      close(): void;
    }) => void;
    const wssReady = new Promise<{
      setConversationWatched(): void;
      close(): void;
    }>((resolve) => { resolveWss = resolve; });
    fixture.value.realtime.connect = async (input) => {
      fixture.realtime.input = input;
      return wssReady;
    };
    const client = createMessageClient(fixture.value);
    const opening = client.connect();
    await Promise.resolve();
    await Promise.resolve();
    expect(directoryStates).toEqual([]);

    resolveWss({ setConversationWatched() {}, close() {} });
    await opening;
    expect(directoryStates).toEqual(["open", "closed"]);
  });

  it("fails closed when an event arrives on an incompatible physical audience", async () => {
    const privateFixture = composition();
    const privateClient = createMessageClient(privateFixture.value);
    await privateClient.connect();
    privateFixture.realtime.input!.onEvent(
      created("wrong-private-audience", "1"),
      { kind: "private-control" },
    );
    expect(privateClient.getSnapshot()).toMatchObject({
      connection: "failed",
      recoveryError: expect.stringContaining("audience contract violation"),
    });

    const conversationFixture = composition();
    const conversationClient = createMessageClient(conversationFixture.value);
    await conversationClient.connect();
    conversationFixture.realtime.input!.onEvent(
      instanceUpdated("1"),
      { kind: "conversation", conversationId: "c1" },
    );
    expect(conversationClient.getSnapshot()).toMatchObject({
      connection: "failed",
      recoveryError: expect.stringContaining("audience contract violation"),
    });
  });

  it("uses private directory controls to add and remove authorized conv subscriptions", async () => {
    const fixture = composition();
    let available = false;
    fixture.value.privateConversationDirectoryQuery = {
      listPrivateConversations: async (state) => ({
        conversations: available && state === "open"
          ? [{ id: "c1", state: "open", historyGeneration: "1", revision: "1", updatedAt: at }]
          : [],
        hasMore: false,
      }),
    };
    const client = createMessageClient(fixture.value);
    const lateEvents: string[] = [];
    client.listen({ conversationIds: ["c1"] }, (event) => lateEvents.push(event.eventId));
    await client.connect();
    expect(fixture.realtime.subscriptions).toEqual([]);

    available = true;
    fixture.realtime.input!.onEvent(
      inboxControl("inbox.conversation.available", "1"),
      { kind: "private-control" },
    );
    await vi.waitFor(() => {
      expect(client.getSnapshot().conversations.c1).toBeDefined();
    });
    expect(fixture.realtime.subscriptions).toEqual([true]);

    available = false;
    fixture.realtime.input!.onEvent(
      inboxControl("inbox.conversation.unavailable", "2"),
      { kind: "private-control" },
    );
    fixture.realtime.input!.onEvent(
      createdForConversation("c1", "late-after-unavailable"),
      { kind: "conversation", conversationId: "c1" },
    );
    await vi.waitFor(() => {
      expect(client.getSnapshot().conversations.c1).toBeUndefined();
    });
    expect(fixture.realtime.subscriptions).toEqual([true, false]);
    expect(client.getSnapshot().messages["late-after-unavailable"]).toBeUndefined();
    expect(lateEvents).toEqual([]);
  });

  it("buffers newer per-conv events after directory subscribe until all hydrates commit", async () => {
    const fixture = composition();
    fixture.value.privateConversationDirectoryQuery = {
      listPrivateConversations: async (state) => ({
        conversations: state === "open"
          ? ["c1", "c2"].map((id) => ({
              id,
              state: "open" as const,
              historyGeneration: "1",
              revision: "1",
              updatedAt: at,
            }))
          : [],
        hasMore: false,
      }),
    };
    fixture.value.messageQuery.listMessages = async (conversationId) => ({
      messages: [{
        id: `${conversationId}-http`,
        conversationId,
        senderId: "u1",
        type: "chat_message",
        body: "http",
        state: "completed",
        historyGeneration: "1",
        offset: "1",
        revision: "1",
        createdAt: at,
        updatedAt: at,
      }],
      historyGeneration: "1",
      historyBoundaryOffset: "0",
      latestOffset: "1",
      hasMore: false,
    });
    fixture.value.realtime.connect = async (input) => {
      fixture.realtime.input = input;
      return {
        setConversationWatched: (conversationId, watched) => {
          fixture.realtime.subscriptions.push(watched);
          if (watched) {
            input.onEvent(
              createdForConversation(conversationId, `${conversationId}-live`),
              { kind: "conversation", conversationId },
            );
          }
        },
        close: () => undefined,
      };
    };
    const client = createMessageClient(fixture.value);
    await client.connect();
    expect(Object.keys(client.getSnapshot().messages).sort()).toEqual([
      "c1-http",
      "c1-live",
      "c2-http",
      "c2-live",
    ]);
    expect(client.getSnapshot().latestOffsetByConversation).toEqual({
      c1: "2",
      c2: "2",
    });
  });

  it("singleflights a directory-only generation rebase and dispatches a duplicate gap event once", async () => {
    const fixture = composition();
    let generation = "1";
    let laterGetCalls = 0;
    let unblockLaterGet!: () => void;
    const laterGet = new Promise<void>((resolve) => { unblockLaterGet = resolve; });
    fixture.value.privateConversationDirectoryQuery = {
      listPrivateConversations: async (state) => ({
        conversations: state === "open"
          ? [{ id: "c1", state: "open", historyGeneration: generation, revision: generation, updatedAt: at }]
          : [],
        hasMore: false,
      }),
    };
    fixture.value.conversationQuery.getConversation = async (id) => {
      if (generation === "2" && laterGetCalls++ === 0) await laterGet;
      return { id, state: "open", historyGeneration: generation, revision: generation, updatedAt: at };
    };
    fixture.value.messageQuery.listMessages = async (conversationId) => ({
      messages: [{
        id: generation === "1" ? "old-generation" : "new-http",
        conversationId,
        senderId: "u1",
        type: "chat_message",
        body: generation,
        state: "completed",
        historyGeneration: generation,
        offset: "1",
        revision: generation,
        createdAt: at,
        updatedAt: at,
      }],
      historyGeneration: generation,
      historyBoundaryOffset: "0",
      latestOffset: "1",
      hasMore: false,
    });
    const committedCheckpoints: ProjectionCheckpoint[] = [];
    fixture.value.projectionStore = {
      async loadCheckpoint() {
        return {
          projection: emptyProjection(),
          cursors: {
            conversations: {
              c1: { streamSequence: "1", historyGeneration: "1" },
            },
          },
        };
      },
      async commitCheckpoint(checkpoint) { committedCheckpoints.push(checkpoint); },
    };
    const client = createMessageClient(fixture.value);
    const dispatched: string[] = [];
    client.listen({ conversationIds: ["c1"] }, (event) => dispatched.push(event.eventId));
    await client.connect();
    expect(client.getSnapshot().messages["old-generation"]).toBeDefined();

    generation = "2";
    const gap = created("new-live", "3", "u1");
    gap.ordering.historyGeneration = "2";
    gap.ordering.messageOffset = "2";
    gap.data.message.historyGeneration = "2";
    fixture.realtime.input!.onEvent(gap, {
      kind: "conversation",
      conversationId: "c1",
    });
    await vi.waitFor(() => { expect(laterGetCalls).toBe(1); });
    fixture.realtime.input!.onEvent(gap, {
      kind: "conversation",
      conversationId: "c1",
    });
    unblockLaterGet();
    await vi.waitFor(() => {
      expect(client.getSnapshot().messages["new-live"]).toBeDefined();
    });
    expect(client.getSnapshot().messages["old-generation"]).toBeUndefined();
    expect(client.getSnapshot().messages["new-http"]).toBeDefined();
    expect(dispatched).toEqual([gap.eventId]);
    expect(laterGetCalls).toBe(2);
    expect(committedCheckpoints.at(-1)?.cursors.conversations.c1).toMatchObject({
      streamSequence: "3",
      historyGeneration: "2",
    });
    expect(committedCheckpoints.at(-1)?.projection.messages["new-live"]).toBeDefined();
    expect(committedCheckpoints.at(-1)?.projection.messages["old-generation"]).toBeUndefined();

    const delayedOldGeneration = created("old-late", "4", "u1");
    delayedOldGeneration.ordering.historyGeneration = "1";
    delayedOldGeneration.ordering.messageOffset = "9";
    delayedOldGeneration.data.message.historyGeneration = "1";
    fixture.realtime.input!.onEvent(delayedOldGeneration, {
      kind: "conversation",
      conversationId: "c1",
    });
    expect(client.getSnapshot().messages["old-late"]).toBeUndefined();
    expect(committedCheckpoints.at(-1)?.cursors.conversations.c1).toMatchObject({
      streamSequence: "3",
      historyGeneration: "2",
    });
  });

  it("waits for reconnect re-subscription authorization before HTTP hydrate and buffer drain", async () => {
    const fixture = composition();
    fixture.value.privateConversationDirectoryQuery = {
      listPrivateConversations: async (state) => ({
        conversations: state === "open"
          ? [{ id: "c1", state: "open", historyGeneration: "1", revision: "1", updatedAt: at }]
          : [],
        hasMore: false,
      }),
    };
    let conversationGets = 0;
    fixture.value.conversationQuery.getConversation = async (id) => {
      conversationGets++;
      return { id, state: "open", historyGeneration: "1", revision: "1", updatedAt: at };
    };
    let watchCalls = 0;
    let resolveReauthorized!: () => void;
    const reauthorized = new Promise<void>((resolve) => { resolveReauthorized = resolve; });
    fixture.value.realtime.connect = async (input) => {
      fixture.realtime.input = input;
      return {
        setConversationWatched: (_id, watched) => {
          if (!watched) return;
          watchCalls++;
          if (watchCalls > 1) return reauthorized;
        },
        close() {},
      };
    };
    const client = createMessageClient(fixture.value);
    await client.connect();
    expect(conversationGets).toBe(2);

    fixture.realtime.input!.onState("disconnected");
    fixture.realtime.input!.onState("connected");
    await vi.waitFor(() => { expect(watchCalls).toBe(2); });
    fixture.realtime.input!.onEvent(
      created("during-reauthorization", "2", "u1"),
      { kind: "conversation", conversationId: "c1" },
    );
    expect(conversationGets).toBe(2);
    expect(client.getSnapshot().messages["during-reauthorization"]).toBeUndefined();

    resolveReauthorized();
    await vi.waitFor(() => {
      expect(conversationGets).toBe(4);
      expect(client.getSnapshot().messages["during-reauthorization"]).toBeDefined();
    });
  });

  it("dispatches an ordered lifecycle event even when its projection is a semantic no-op", async () => {
    const fixture = composition();
    const client = createMessageClient(fixture.value);
    const terminals: string[] = [];
    let projectionNotifications = 0;
    client.listen({ eventTypes: ["message.terminal"] }, (event) => terminals.push(event.eventId));
    client.subscribe(() => { projectionNotifications++; });
    await client.connect();
    const watch = client.conversations.watch("c1");
    await watch.ready;

    const initialTerminalSnapshot = created("m-terminal", "1", "agent-a");
    initialTerminalSnapshot.data.message.state = "completed";
    initialTerminalSnapshot.ordering.entityRevision = "1";
    initialTerminalSnapshot.ordering.messageOffset = "1";
    fixture.realtime.input!.onEvent(initialTerminalSnapshot, {
      kind: "conversation",
      conversationId: "c1",
    });
    const afterCreated = client.getSnapshot();
    const notificationsAfterCreated = projectionNotifications;

    fixture.realtime.input!.onEvent(
      terminal("m-terminal", "2", "1", "agent-a"),
      { kind: "conversation", conversationId: "c1" },
    );
    expect(terminals).toEqual(["terminal-m-terminal"]);
    expect(client.getSnapshot().messages["m-terminal"].senderId).toBe("agent-a");
    expect(client.messages.isMine("m-terminal")).toBe(false);
    expect(client.getSnapshot()).toBe(afterCreated);
    expect(projectionNotifications).toBe(notificationsAfterCreated);
    watch.release();
  });

});
