import { beforeEach, describe, expect, it, vi } from "vitest";

const realtimeMock = vi.hoisted(() => ({
  instances: [] as Array<{
    handlers: Map<string, (ctx: any) => void>;
    emit(name: string, ctx?: any): void;
  }>,
}));

vi.mock("centrifuge", () => ({
  Centrifuge: class {
    readonly handlers = new Map<string, (ctx: any) => void>();
    constructor() {
      realtimeMock.instances.push({
        handlers: this.handlers,
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
  },
}));

import { createGatewayMessageClientComposition } from "../src/gateway-runtime.js";
import { createMessageClient } from "../src/unified-client.js";

const at = "2026-08-02T00:00:00.000Z";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function conversation() {
  return {
    conversationId: "c1",
    agentId: "agent-a",
    title: "hi",
    state: "active",
    historyGeneration: "1",
    metadataVersion: "1",
    createdAt: at,
    lastActivityAt: at,
  };
}

beforeEach(() => {
  realtimeMock.instances.splice(0);
});

describe("Gateway composition — web/desktop credentials", () => {
  it("always sends credentials: include and omits Authorization when no token provider is configured", async () => {
    const calls: Array<{ init?: RequestInit }> = [];
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      calls.push({ init });
      return json({ success: true, data: conversation() });
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    await composition.conversationQuery.getConversation("c1");

    expect(calls[0].init?.credentials).toBe("include");
    expect(new Headers(calls[0].init?.headers).has("Authorization")).toBe(false);
  });

  it("omits Authorization when the token provider resolves an empty string, without throwing", async () => {
    const calls: Array<{ init?: RequestInit }> = [];
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      calls.push({ init });
      return json({ success: true, data: conversation() });
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      accessTokenProvider: async () => "   ",
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    await composition.conversationQuery.getConversation("c1");

    expect(calls[0].init?.credentials).toBe("include");
    expect(new Headers(calls[0].init?.headers).has("Authorization")).toBe(false);
  });

  it("adds a Bearer token alongside credentials: include when a non-empty token is supplied", async () => {
    const calls: Array<{ init?: RequestInit }> = [];
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      calls.push({ init });
      return json({ success: true, data: conversation() });
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      accessTokenProvider: async () => "web-token",
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    await composition.conversationQuery.getConversation("c1");

    expect(calls[0].init?.credentials).toBe("include");
    expect(new Headers(calls[0].init?.headers).get("Authorization")).toBe("Bearer web-token");
  });

  it("forwards platform: \"web\" in the messaging-token request body", async () => {
    const bodies: unknown[] = [];
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      bodies.push(init?.body ? JSON.parse(String(init.body)) : undefined);
      return json({
        token: "realtime-token",
        centrifugo_url: "wss://realtime.example/connection/websocket",
        principal_id: "user:u1",
      });
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    const opening = composition.realtime.connect({ onEvent() {}, onState() {} });
    await vi.waitFor(() => { expect(realtimeMock.instances).toHaveLength(1); });
    realtimeMock.instances[0].emit("connected");
    await opening;
    expect(bodies[0]).toEqual({ platform: "web" });
  });
});

describe("Gateway composition — mobile compatibility", () => {
  it("never sends credentials: include and still requires a non-empty Bearer token", async () => {
    const calls: Array<{ init?: RequestInit }> = [];
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      calls.push({ init });
      return json({ success: true, data: conversation() });
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "mobile",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      accessTokenProvider: async () => "mobile-token",
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    await composition.conversationQuery.getConversation("c1");

    expect(calls[0].init?.credentials).toBeUndefined();
    expect(new Headers(calls[0].init?.headers).get("Authorization")).toBe("Bearer mobile-token");
  });

  it("throws instead of silently omitting Authorization when the mobile token is empty", async () => {
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "mobile",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      accessTokenProvider: async () => "",
      fetch: vi.fn(async () => json({ success: true, data: conversation() })),
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    await expect(composition.conversationQuery.getConversation("c1"))
      .rejects.toThrow("empty token");
  });

  it("throws when platform is mobile and no accessTokenProvider was supplied", async () => {
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "mobile",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: vi.fn(async () => json({ success: true, data: conversation() })),
    } as any);
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    await expect(composition.conversationQuery.getConversation("c1"))
      .rejects.toThrow("required for platform \"mobile\"");
  });
});

describe("Gateway composition — personal.notification normalization", () => {
  it("passes a well-formed RealtimeEventV1 through unchanged", async () => {
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: async () => json({
        token: "realtime-token",
        centrifugo_url: "wss://realtime.example/connection/websocket",
        principal_id: "user:u1",
      }),
    });
    const observed: unknown[] = [];
    const opening = composition.realtime.connect({
      onEvent: (event) => observed.push(event),
      onState() {},
    });
    await vi.waitFor(() => { expect(realtimeMock.instances).toHaveLength(1); });
    realtimeMock.instances[0].emit("connected");
    await opening;

    const wellFormed = {
      schemaVersion: 1,
      eventId: "evt-1",
      type: "message.created",
      scope: { tenantId: "t1", conversationId: "c1" },
      actor: { kind: "agent", id: "agent-1" },
      correlation: {},
      occurredAt: at,
      data: { message: {} },
    };
    realtimeMock.instances[0].emit("publication", { data: wellFormed });
    expect(observed).toEqual([wellFormed]);
  });

  it("normalizes a thin/unenveloped frame into a canonical personal.notification consumable via listen()", async () => {
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: async () => json({
        token: "realtime-token",
        centrifugo_url: "wss://realtime.example/connection/websocket",
        principal_id: "user:u1",
      }),
    });
    const client = createMessageClient(composition);
    const notifications: unknown[] = [];
    client.listen({ eventTypes: ["personal.notification"] }, (event) => notifications.push(event));

    const opening = client.connect();
    await vi.waitFor(() => { expect(realtimeMock.instances).toHaveLength(1); });
    realtimeMock.instances[0].emit("connected");
    await opening;

    realtimeMock.instances[0].emit("publication", { data: { conversationId: "c1" } });

    await vi.waitFor(() => { expect(notifications).toHaveLength(1); });
    expect(notifications[0]).toMatchObject({
      type: "personal.notification",
      scope: { conversationId: "c1" },
      data: { conversationId: "c1" },
    });
    expect(client.getSnapshot().connection).toBe("connected");
    expect(client.getSnapshot().recoveryError).toBeUndefined();
  });
});
