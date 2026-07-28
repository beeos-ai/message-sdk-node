import { describe, expect, it, vi } from "vitest";

import { CentrifugoSessionAdapter } from "../src/adapters/centrifugo.js";

function connectInput() {
  return {
    onEvent() {},
    onState() {},
  };
}

describe("CentrifugoSessionAdapter credential refresh", () => {
  it("singleflights concurrent token refresh on the same WSS endpoint", async () => {
    let credentialsCalls = 0;
    let callbacks!: { onRefresh(): Promise<void> };
    const updateToken = vi.fn();
    const adapter = new CentrifugoSessionAdapter({
      credentials: {
        async getCredentials() {
          credentialsCalls++;
          return {
            token: credentialsCalls === 1 ? "initial" : "refreshed",
            realtimeUrl: "wss://realtime.example/connection/websocket",
          };
        },
      },
      factory: {
        create(options) {
          callbacks = options;
          return {
            connect() {},
            close() {},
            updateToken,
            setConversationWatched() {},
          };
        },
      },
    });
    await adapter.connect(connectInput());
    await Promise.all([callbacks.onRefresh(), callbacks.onRefresh()]);
    expect(credentialsCalls).toBe(2);
    expect(updateToken).toHaveBeenCalledTimes(1);
    expect(updateToken).toHaveBeenCalledWith("refreshed");
  });

  it("fails refresh closed when credentials change the WSS endpoint", async () => {
    let credentialsCalls = 0;
    let callbacks!: { onRefresh(): Promise<void> };
    const updateToken = vi.fn();
    const adapter = new CentrifugoSessionAdapter({
      credentials: {
        async getCredentials() {
          credentialsCalls++;
          return {
            token: credentialsCalls === 1 ? "initial" : "refreshed",
            realtimeUrl: credentialsCalls === 1
              ? "wss://realtime.example/connection/websocket"
              : "wss://other.example/connection/websocket",
          };
        },
      },
      factory: {
        create(options) {
          callbacks = options;
          return {
            connect() {},
            close() {},
            updateToken,
            setConversationWatched() {},
          };
        },
      },
    });
    await adapter.connect(connectInput());
    await expect(callbacks.onRefresh()).rejects.toThrow("changed the WSS endpoint");
    expect(updateToken).not.toHaveBeenCalled();
  });
});
