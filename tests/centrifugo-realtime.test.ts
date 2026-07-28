import { describe, expect, it } from "vitest";

import {
  createCentrifugoRealtimeTransport,
  createMessageClient,
  type AnyRealtimeEventV1,
  type CentrifugeClient,
  type CentrifugeClientFactory,
  type CentrifugeClientOptions,
  type RealtimeConnectionState,
  type MessageClientFacadeOptions,
  type RealtimeConnectInput,
} from "../src/facade/index.js";
import {
  createSingleWssCentrifugeFactory,
  type CentrifugeConnection,
} from "../src/facade/centrifugo-factory.js";

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

class FakeCentrifugeClient implements CentrifugeClient {
  connected = false;
  closed = false;
  readonly updatedTokens: string[] = [];

  constructor(readonly options: CentrifugeClientOptions) {}

  connect(): void {
    this.connected = true;
    this.options.onState("connected");
  }

  close(): void {
    this.closed = true;
  }

  updateToken(token: string): void {
    this.updatedTokens.push(token);
  }
}

class FakeCentrifugeFactory implements CentrifugeClientFactory {
  readonly clients: FakeCentrifugeClient[] = [];

  create(options: CentrifugeClientOptions): CentrifugeClient {
    const client = new FakeCentrifugeClient(options);
    this.clients.push(client);
    return client;
  }
}

class FakeNativeCentrifuge implements CentrifugeConnection {
  static instances: FakeNativeCentrifuge[] = [];
  readonly listeners = new Map<string, (context: any) => void>();
  connected = false;
  disconnected = false;
  readonly setTokens: string[] = [];

  constructor(
    readonly endpoint: string,
    readonly nativeOptions: { token: string; getToken: () => Promise<string>; websocket: unknown },
  ) {
    FakeNativeCentrifuge.instances.push(this);
  }

  on(event: "connecting" | "connected" | "disconnected" | "publication" | "subscribed" | "error", listener: (context: any) => void): unknown {
    this.listeners.set(event, listener);
  }

  connect(): void {
    this.connected = true;
  }

  disconnect(): void {
    this.disconnected = true;
  }

  setToken(token: string): void {
    this.setTokens.push(token);
  }

  emit(event: "connecting" | "connected" | "disconnected" | "publication" | "subscribed" | "error", context: any = {}): void {
    this.listeners.get(event)?.(context);
  }
}

function response(
  token: string,
  cursor = "opaque-sync-cursor",
  options: { realtimeUrl?: string; expiresAt?: number } = {},
): Response {
  return new Response(
    JSON.stringify({
      token,
      realtime_url: options.realtimeUrl ?? "wss://msg-ws.example/connection/websocket",
      sync_cursor: cursor,
      expires_at: options.expiresAt ?? Math.floor(Date.now() / 1_000) + 900,
    }),
    { status: 200, headers: { "Content-Type": "application/json" } },
  );
}

function facadeOptions(realtime: ReturnType<typeof createCentrifugoRealtimeTransport>): MessageClientFacadeOptions {
  return {
    realtime,
    transport: {
      async sendMessage(input) {
        return { messageId: input.clientMessageId, outcome: "created" };
      },
      async executeMethod() {
        return { operationId: "operation-1", outcome: "accepted" };
      },
      async hydrateConversation(input) {
        return { conversationId: input.conversationId, events: [] };
      },
      async rebase() {
        return { events: [] };
      },
    },
  };
}

describe("SDK-owned Centrifugo realtime transport", () => {
  it("binds centrifuge-js to one WSS-only internal client and lets Centrifuge own transient reconnects", async () => {
    FakeNativeCentrifuge.instances = [];
    const nativeWebSocket = function NativeWebSocket() {};
    const factory = createSingleWssCentrifugeFactory(
      FakeNativeCentrifuge as never,
      nativeWebSocket,
    );
    const states: RealtimeConnectionState[] = [];
    const publications: unknown[] = [];
    let handle!: CentrifugeClient;
    handle = factory.create({
      url: "wss://msg-ws.example/connection/websocket",
      token: "initial-token",
      onPublication: (event) => publications.push(event),
      onState: (state) => states.push(state),
      onRefreshRequired: async () => handle.updateToken("refreshed-token"),
      onError: () => states.push("failed"),
    });
    const native = FakeNativeCentrifuge.instances[0];
    expect(native.endpoint).toBe("wss://msg-ws.example/connection/websocket");
    expect(native.nativeOptions.websocket).toBe(nativeWebSocket);
    expect(Array.isArray(native.endpoint)).toBe(false);
    expect(typeof (handle as unknown as { publish?: unknown }).publish).toBe("undefined");
    expect(typeof (handle as unknown as { subscribe?: unknown }).subscribe).toBe("undefined");

    native.emit("connecting");
    native.emit("connected");
    native.emit("connecting");
    native.emit("publication", { data: { eventId: "event-1" } });
    native.emit("error", { error: { message: "temporary" } });
    expect(await native.nativeOptions.getToken()).toBe("refreshed-token");
    native.emit("disconnected");
    await handle.close();

    expect(states).toEqual(["connecting", "connected", "reconnecting", "disconnected"]);
    expect(publications).toEqual([{ eventId: "event-1" }]);
    expect(native.setTokens).toEqual(["refreshed-token"]);
    expect(native.disconnected).toBe(true);
  });

  it("reduces subscribed recovery metadata without exposing stream or channel details", async () => {
    FakeNativeCentrifuge.instances = [];
    const factory = createSingleWssCentrifugeFactory(FakeNativeCentrifuge as never, function NativeWebSocket() {});
    const statuses: unknown[] = [];
    factory.create({
      url: "wss://msg-ws.example/connection/websocket",
      token: "initial-token",
      onPublication: () => undefined,
      onState: () => undefined,
      onRecovery: (status) => statuses.push(status),
      onRefreshRequired: async () => undefined,
      onError: () => undefined,
    });
    FakeNativeCentrifuge.instances[0].emit("subscribed", {
      recoverable: true,
      wasRecovering: true,
      recovered: false,
      streamPosition: { offset: 7, epoch: "not-exposed" },
      channel: "not-exposed",
    });

    expect(statuses).toEqual([{ recoverable: true, recovered: false, positioned: true }]);
  });

  it("uses one physical client across facade listeners and watches without exposing channels", async () => {
    const factory = new FakeCentrifugeFactory();
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const realtime = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example/",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async (url, init) => {
        calls.push({ url: String(url), init });
        return response("session-token");
      }) as typeof fetch,
    });
    const client = createMessageClient(facadeOptions(realtime));
    const seen: AnyRealtimeEventV1[] = [];
    client.listen({}, (event) => seen.push(event));
    const first = client.conversations.watch("conversation-1");
    const second = client.conversations.watch("conversation-1");
    await Promise.all([first.ready, second.ready]);

    await client.connect();
    expect(factory.clients).toHaveLength(1);
    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({ url: "https://msg.example/api/v2/realtime/session" });
    expect(calls[0].init?.method).toBe("POST");
    expect(calls[0].init?.headers).toEqual({ Authorization: "Bearer user-bearer" });
    expect(calls[0].init?.body).toBeUndefined();

    const config = factory.clients[0].options as unknown as Record<string, unknown>;
    expect(config).not.toHaveProperty("channel");
    expect(config).not.toHaveProperty("channels");
    expect(config).not.toHaveProperty("subscription");
    expect(typeof (realtime as unknown as { subscribe?: unknown }).subscribe).toBe("undefined");
    expect(typeof (realtime as unknown as { publish?: unknown }).publish).toBe("undefined");

    factory.clients[0].options.onPublication(created());
    expect(seen).toHaveLength(1);
    expect(client.getSnapshot().connection).toBe("connected");
    first.release();
    second.release();
  });

  it("refreshes the same physical WSS client through the authenticated refresh endpoint", async () => {
    const factory = new FakeCentrifugeFactory();
    const paths: string[] = [];
    let issue = 0;
    const realtime = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async (url) => {
        paths.push(String(url));
        issue++;
        return response(`session-token-${issue}`, `opaque-cursor-${issue}`);
      }) as typeof fetch,
    });
    const client = createMessageClient(facadeOptions(realtime));
    await client.connect();
    await Promise.all([
      factory.clients[0].options.onRefreshRequired(),
      factory.clients[0].options.onRefreshRequired(),
    ]);

    expect(factory.clients).toHaveLength(1);
    expect(paths).toEqual([
      "https://msg.example/api/v2/realtime/session",
      "https://msg.example/api/v2/realtime/session/refresh",
    ]);
    expect(factory.clients[0].updatedTokens).toEqual(["session-token-2"]);
  });

  it("reports reconnecting, failed, and explicit close without an EventSource or fallback", async () => {
    const factory = new FakeCentrifugeFactory();
    const states: string[] = [];
    const realtime = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async () => response("session-token")) as typeof fetch,
    });
    const input: RealtimeConnectInput = {
      onEvent: () => undefined,
      onState: (state) => states.push(state),
    };
    const session = await realtime.connect(input);
    expect(session.syncCursor).toBe("opaque-sync-cursor");
    factory.clients[0].options.onState("reconnecting");
    factory.clients[0].options.onError(new Error("network"));
    await session.close();
    // A late native callback after an explicit close cannot revive the session.
    factory.clients[0].options.onState("connected");

    expect(states).toEqual(["connected", "reconnecting", "failed"]);
    expect(factory.clients[0].closed).toBe(true);
  });

  it("keeps one physical client across transient disconnect and reconnect", async () => {
    const factory = new FakeCentrifugeFactory();
    const realtime = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async () => response("session-token")) as typeof fetch,
    });
    const client = createMessageClient(facadeOptions(realtime));
    await client.connect();
    factory.clients[0].options.onState("disconnected");
    factory.clients[0].options.onState("reconnecting");
    factory.clients[0].options.onState("connected");
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(factory.clients).toHaveLength(1);
    expect(factory.clients[0].closed).toBe(false);
    expect(client.getSnapshot().connection).toBe("connected");
  });

  it("fails closed on a non-WSS endpoint or server audience leakage", async () => {
    const factory = new FakeCentrifugeFactory();
    const badEndpoint = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async () => response("x", "x", { realtimeUrl: "https://not-wss.example" })) as typeof fetch,
    });
    await expect(badEndpoint.connect({ onEvent: () => undefined, onState: () => undefined })).rejects.toThrow("wss://");

    const leakingEndpoint = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async () => new Response(JSON.stringify({
        token: "x",
        realtime_url: "wss://msg.example",
        sync_cursor: "x",
        channels: ["usr:leak"],
        expires_at: Math.floor(Date.now() / 1_000) + 900,
      }))) as typeof fetch,
    });
    await expect(leakingEndpoint.connect({ onEvent: () => undefined, onState: () => undefined })).rejects.toThrow("leaked channels");
    expect(factory.clients).toHaveLength(0);
  });

  it("retires the one WSS client when refresh returns an unusable or mismatched session", async () => {
    const factory = new FakeCentrifugeFactory();
    const states: RealtimeConnectionState[] = [];
    let issue = 0;
    const realtime = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async () => {
        issue++;
        return issue === 1
          ? response("initial-token")
          : response("refresh-token", "opaque-sync-cursor", { realtimeUrl: "wss://other.example/connection/websocket" });
      }) as typeof fetch,
    });
    const input: RealtimeConnectInput = {
      onEvent: () => undefined,
      onState: (state) => states.push(state),
    };

    await realtime.connect(input);
    await expect(factory.clients[0].options.onRefreshRequired()).rejects.toThrow("changed the WSS endpoint");

    expect(factory.clients).toHaveLength(1);
    expect(factory.clients[0].closed).toBe(true);
    expect(factory.clients[0].updatedTokens).toEqual([]);
    expect(states).toEqual(["connected", "failed"]);
  });

  it("rejects expired or unsafe issued sessions without logging their token", async () => {
    const factory = new FakeCentrifugeFactory();
    const expiredToken = "do-not-log-session-token";
    const expired = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async () => response(expiredToken, "x", { expiresAt: 1 })) as typeof fetch,
    });
    await expect(expired.connect({ onEvent: () => undefined, onState: () => undefined }))
      .rejects.toThrow("future expires_at");

    const unsafe = createCentrifugoRealtimeTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return "user-bearer"; } },
      centrifuge: factory,
      fetchImpl: (async () => response("x", "x", { realtimeUrl: "wss://token@msg.example/#fragment" })) as typeof fetch,
    });
    await expect(unsafe.connect({ onEvent: () => undefined, onState: () => undefined }))
      .rejects.toThrow("unsafe realtime_url");
    expect(factory.clients).toHaveLength(0);
  });
});
