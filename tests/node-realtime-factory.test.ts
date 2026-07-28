import { beforeEach, describe, expect, it, vi } from "vitest";

const mock = vi.hoisted(() => ({
  instances: [] as Array<{
    handlers: Map<string, (ctx: any) => void>;
    emit(name: string, ctx?: any): void;
  }>,
  subscriptions: [] as Array<{
    channel: string;
    handlers: Map<string, (ctx: any) => void>;
    emit(name: string, ctx?: any): void;
  }>,
}));

vi.mock("centrifuge", () => ({
  Centrifuge: class {
    readonly handlers = new Map<string, (ctx: any) => void>();
    constructor() {
      const instance = {
        handlers: this.handlers,
        emit: (name: string, ctx: any = {}) => this.handlers.get(name)?.(ctx),
      };
      mock.instances.push(instance);
    }
    on(name: string, handler: (ctx: any) => void) {
      this.handlers.set(name, handler);
      return this;
    }
    connect() {}
    disconnect() {}
    setToken() {}
    newSubscription(channel: string) {
      const handlers = new Map<string, (ctx: any) => void>();
      const value = {
        channel,
        handlers,
        emit: (name: string, ctx: any = {}) => handlers.get(name)?.(ctx),
      };
      mock.subscriptions.push(value);
      return {
        on(name: string, handler: (ctx: any) => void) { handlers.set(name, handler); },
        subscribe() {},
        unsubscribe() {},
        removeAllListeners() {},
      };
    }
  },
}));

import { createNodeCentrifugeFactory } from "../src/node-runtime.js";

beforeEach(() => {
  mock.instances.splice(0);
  mock.subscriptions.splice(0);
});

function options() {
  const states: string[] = [];
  const errors: unknown[] = [];
  return {
    states,
    errors,
    value: {
      url: "wss://realtime.example/connection/websocket",
      token: "token",
      onEvent() {},
      onState(state: "connecting" | "connected" | "disconnected" | "reconnecting" | "failed") {
        states.push(state);
      },
      async onRefresh() {},
      onError(error: unknown) { errors.push(error); },
    },
  };
}

describe("Node Centrifugo physical connection contract", () => {
  it("does not resolve connect until the first real connected event", async () => {
    const fixture = options();
    const client = createNodeCentrifugeFactory().create(fixture.value);
    let resolved = false;
    const opening = Promise.resolve(client.connect()).then(() => { resolved = true; });
    await Promise.resolve();
    expect(resolved).toBe(false);

    mock.instances[0].emit("connected");
    await opening;
    expect(resolved).toBe(true);
    expect(fixture.states).toEqual(["connected"]);

    // Later reconnect notifications do not re-settle the first-connect gate.
    mock.instances[0].emit("connected");
    expect(fixture.states).toEqual(["connected", "connected"]);
  });

  it("does not turn a post-connect recoverable error into a fatal close", async () => {
    const fixture = options();
    const client = createNodeCentrifugeFactory().create(fixture.value);
    const opening = Promise.resolve(client.connect());
    mock.instances[0].emit("connected");
    await opening;

    mock.instances[0].emit("error", { error: new Error("recoverable transport advisory") });
    mock.instances[0].emit("connecting");
    mock.instances[0].emit("connected");
    expect(fixture.errors).toEqual([]);
    expect(fixture.states).toEqual(["connected", "reconnecting", "connected"]);
  });

  it("rejects and forwards an error received before first connected", async () => {
    const fixture = options();
    const client = createNodeCentrifugeFactory().create(fixture.value);
    const opening = Promise.resolve(client.connect());
    const failure = new Error("handshake rejected");
    mock.instances[0].emit("error", { error: failure });
    await expect(opening).rejects.toBe(failure);
    expect(fixture.errors).toEqual([failure]);
  });

  it("rejects a disconnect received before first connected", async () => {
    const fixture = options();
    const client = createNodeCentrifugeFactory().create(fixture.value);
    const opening = Promise.resolve(client.connect());
    mock.instances[0].emit("disconnected", { reason: "transport closed" });
    await expect(opening).rejects.toThrow("disconnected before connected");
  });

  it("uses the hidden conv namespace and fails a server authorization rejection closed", async () => {
    const fixture = options();
    const client = createNodeCentrifugeFactory().create(fixture.value);
    const opening = Promise.resolve(client.connect());
    mock.instances[0].emit("connected");
    await opening;

    const watch = Promise.resolve(client.setConversationWatched("guessed-id", true));
    expect(mock.subscriptions[0].channel).toBe("conv:guessed-id");
    const denied = new Error("permission denied");
    mock.subscriptions[0].emit("error", { error: denied });
    await expect(watch).rejects.toBe(denied);
  });

  it("cancels a pending authorization wait when the logical watch is released", async () => {
    const fixture = options();
    const client = createNodeCentrifugeFactory().create(fixture.value);
    const opening = Promise.resolve(client.connect());
    mock.instances[0].emit("connected");
    await opening;

    const watch = Promise.resolve(client.setConversationWatched("pending-id", true));
    await client.setConversationWatched("pending-id", false);
    await expect(watch).rejects.toThrow("cancelled before authorization");
  });

  it("requires a fresh subscribed authorization barrier after physical reconnect", async () => {
    const fixture = options();
    const client = createNodeCentrifugeFactory().create(fixture.value);
    const opening = Promise.resolve(client.connect());
    mock.instances[0].emit("connected");
    await opening;

    const initial = Promise.resolve(client.setConversationWatched("c1", true));
    mock.subscriptions[0].emit("subscribed");
    await initial;

    mock.instances[0].emit("disconnected", { reason: "network" });
    const reauthorized = Promise.resolve(client.setConversationWatched("c1", true));
    let ready = false;
    void reauthorized.then(() => { ready = true; });
    mock.instances[0].emit("connected");
    await Promise.resolve();
    expect(ready).toBe(false);

    mock.subscriptions[0].emit("subscribed");
    await reauthorized;
    expect(ready).toBe(true);
  });
});
