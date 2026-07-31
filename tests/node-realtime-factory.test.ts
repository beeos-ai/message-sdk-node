import { beforeEach, describe, expect, it, vi } from "vitest";

const mock = vi.hoisted(() => ({
  instances: [] as Array<{
    handlers: Map<string, (ctx: any) => void>;
    newSubscriptionCalls: string[];
    emit(name: string, ctx?: any): void;
  }>,
}));

vi.mock("centrifuge", () => ({
  Centrifuge: class {
    readonly handlers = new Map<string, (ctx: any) => void>();
    readonly newSubscriptionCalls: string[] = [];
    constructor() {
      mock.instances.push({
        handlers: this.handlers,
        newSubscriptionCalls: this.newSubscriptionCalls,
        emit: (name: string, ctx: any = {}) => this.handlers.get(name)?.(ctx),
      });
    }
    on(name: string, handler: (ctx: any) => void) {
      this.handlers.set(name, handler);
      return this;
    }
    connect() {}
    disconnect() {}
    setToken() {}
    newSubscription(channel: string) {
      this.newSubscriptionCalls.push(channel);
      throw new Error("dynamic subscriptions are forbidden");
    }
  },
}));

import { createNodeCentrifugeFactory } from "../src/node-runtime.js";

beforeEach(() => mock.instances.splice(0));

function fixture() {
  const states: string[] = [];
  const events: unknown[] = [];
  const errors: unknown[] = [];
  return {
    states,
    events,
    errors,
    value: {
      url: "wss://realtime.example/connection/websocket",
      token: "server-bound-personal-token",
      onEvent(event: unknown) { events.push(event); },
      onState(state: "connecting" | "connected" | "disconnected" | "reconnecting" | "failed") {
        states.push(state);
      },
      async onRefresh() {},
      onError(error: unknown) { errors.push(error); },
    },
  };
}

describe("personal Centrifugo transport", () => {
  it("waits for the physical connected event and consumes connection publications", async () => {
    const state = fixture();
    const client = createNodeCentrifugeFactory().create(state.value);
    let resolved = false;
    const opening = Promise.resolve(client.connect()).then(() => { resolved = true; });
    await Promise.resolve();
    expect(resolved).toBe(false);
    mock.instances[0].emit("connected");
    await opening;
    mock.instances[0].emit("publication", { data: { eventId: "evt-1" } });
    expect(state.events).toEqual([{ eventId: "evt-1" }]);
    expect(mock.instances[0].newSubscriptionCalls).toEqual([]);
    expect("setConversationWatched" in client).toBe(false);
    expect("publish" in client).toBe(false);
  });

  it("reports reconnect state without creating logical subscriptions", async () => {
    const state = fixture();
    const client = createNodeCentrifugeFactory().create(state.value);
    const opening = Promise.resolve(client.connect());
    mock.instances[0].emit("connected");
    await opening;
    mock.instances[0].emit("connecting");
    mock.instances[0].emit("connected");
    expect(state.states).toEqual(["connected", "reconnecting", "connected"]);
    expect(mock.instances[0].newSubscriptionCalls).toEqual([]);
  });

  it("fails the first-connect gate closed", async () => {
    const state = fixture();
    const client = createNodeCentrifugeFactory().create(state.value);
    const opening = Promise.resolve(client.connect());
    const failure = new Error("handshake rejected");
    mock.instances[0].emit("error", { error: failure });
    await expect(opening).rejects.toBe(failure);
    expect(state.errors).toEqual([failure]);
  });
});
