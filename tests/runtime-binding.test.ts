import { describe, expect, it } from "vitest";

import {
  createRuntimeBindingMessageClient,
  type RuntimeBindingDurableRecoveryPort,
  type RuntimeBindingIdentity,
  type RuntimeBindingWakeTransportConnectInput,
  type RuntimeBindingWakeTransportPort,
} from "../src/node.js";

const binding: RuntimeBindingIdentity = {
  runtimeBindingId: "binding-1",
  runtimeEpoch: "epoch-7",
};

class FakeWakeTransport implements RuntimeBindingWakeTransportPort {
  readonly inputs: RuntimeBindingWakeTransportConnectInput[] = [];
  readonly sessions: Array<{ closed: boolean }> = [];

  async connect(input: RuntimeBindingWakeTransportConnectInput) {
    this.inputs.push(input);
    const session = { closed: false };
    this.sessions.push(session);
    input.onState("connected");
    return {
      close: () => {
        session.closed = true;
      },
    };
  }
}

function recoveryPort(): RuntimeBindingDurableRecoveryPort {
  return {
    async listOpenConversations() {
      return { conversations: [], hasMore: false };
    },
    async listUnhandledBy() {
      return { messages: [], hasMore: false };
    },
  };
}

describe("runtime-binding message facade", () => {
  it("owns one physical WSS across wake listeners, hides transport material, and cleans up by refcount", async () => {
    const transport = new FakeWakeTransport();
    const client = createRuntimeBindingMessageClient({
      binding,
      wakeTransport: transport,
      durableRecovery: recoveryPort(),
    });
    const events: string[] = [];
    const first = client.wakes.watch((event) => events.push(event.type));
    const second = client.wakes.watch((event) => events.push(event.type));

    await Promise.all([first.ready, second.ready]);
    expect(transport.inputs).toHaveLength(1);
    expect(transport.sessions).toHaveLength(1);
    expect(events).toEqual(["recovery.required"]);
    expect(client.getSnapshot().connection).toBe("recovering");
    expect(client.getSnapshot().wakeListenerCount).toBe(2);

    const input = transport.inputs[0] as unknown as Record<string, unknown>;
    expect(input).not.toHaveProperty("token");
    expect(input).not.toHaveProperty("channel");
    expect(input).not.toHaveProperty("channels");
    expect(input).not.toHaveProperty("subscription");
    expect(client as unknown as { publish?: unknown }).not.toHaveProperty("publish");
    expect(client as unknown as { options?: unknown }).not.toHaveProperty("options");
    expect(client.wakes as unknown as { subscribe?: unknown }).not.toHaveProperty("subscribe");

    client.markDurableRecoveryComplete();
    expect(client.getSnapshot().connection).toBe("connected");
    first.release();
    expect(transport.sessions[0].closed).toBe(false);
    expect(client.getSnapshot().wakeListenerCount).toBe(1);
    second.release();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(transport.sessions[0].closed).toBe(true);
    expect(client.getSnapshot().connection).toBe("disconnected");
    transport.inputs[0].onState("connected");
    expect(client.getSnapshot().connection).toBe("disconnected");
  });

  it("fails closed and retires the WSS for stale-epoch or malformed wakes", async () => {
    const transport = new FakeWakeTransport();
    const client = createRuntimeBindingMessageClient({
      binding,
      wakeTransport: transport,
      durableRecovery: recoveryPort(),
    });
    const wakes: string[] = [];
    const watch = client.wakes.watch((event) => wakes.push(event.type));
    await watch.ready;
    client.markDurableRecoveryComplete();

    transport.inputs[0].onWake({ type: "message.available", runtimeEpoch: "old-epoch" });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(wakes).toEqual(["recovery.required"]);
    expect(client.getSnapshot()).toMatchObject({
      connection: "failed",
      recoveryError: expect.stringContaining("stale wake epoch"),
    });
    expect(transport.sessions[0].closed).toBe(true);

    const malformedTransport = new FakeWakeTransport();
    const malformed = createRuntimeBindingMessageClient({
      binding,
      wakeTransport: malformedTransport,
      durableRecovery: recoveryPort(),
    });
    const malformedWatch = malformed.wakes.watch(() => undefined);
    await malformedWatch.ready;
    malformedTransport.inputs[0].onWake({ type: "message.available" } as never);
    expect(malformed.getSnapshot()).toMatchObject({
      connection: "failed",
      recoveryError: expect.stringContaining("malformed wake event"),
    });

    watch.release();
    malformedWatch.release();
  });

  it("emits one durable-recovery trigger per reconnect, even if connected is repeated", async () => {
    const transport = new FakeWakeTransport();
    const client = createRuntimeBindingMessageClient({
      binding,
      wakeTransport: transport,
      durableRecovery: recoveryPort(),
    });
    const recoveries: string[] = [];
    const watch = client.wakes.watch((event) => {
      if (event.type === "recovery.required") recoveries.push(event.reason);
    });
    await watch.ready;
    client.markDurableRecoveryComplete();

    transport.inputs[0].onState("reconnecting");
    transport.inputs[0].onState("connected");
    transport.inputs[0].onState("connected");
    expect(recoveries).toEqual(["startup", "reconnect"]);
    expect(client.getSnapshot().connection).toBe("recovering");

    // The same in-progress durable scan remains authoritative across another
    // WSS interruption; a second WSS callback cannot start a duplicate scan.
    transport.inputs[0].onState("reconnecting");
    transport.inputs[0].onState("connected");
    expect(recoveries).toEqual(["startup", "reconnect"]);

    client.markDurableRecoveryComplete();
    expect(client.getSnapshot().connection).toBe("connected");
    watch.release();
  });

  it("scopes durable reads to the fixed binding without calling a guessed HTTP route", async () => {
    const transport = new FakeWakeTransport();
    const calls: unknown[] = [];
    const client = createRuntimeBindingMessageClient({
      binding,
      wakeTransport: transport,
      durableRecovery: {
        async listOpenConversations(receivedBinding, input) {
          calls.push(["open", receivedBinding, input]);
          return { conversations: [], hasMore: false };
        },
        async listUnhandledBy(receivedBinding, input) {
          calls.push(["unhandled", receivedBinding, input]);
          return { messages: [], hasMore: false };
        },
      },
    });

    await client.recovery.listOpenConversations({ limit: 10 });
    await client.recovery.listUnhandledBy({ conversationId: "conversation-1", limit: 20 });

    expect(calls).toEqual([
      ["open", binding, { limit: 10 }],
      ["unhandled", binding, { conversationId: "conversation-1", limit: 20 }],
    ]);
  });
});
