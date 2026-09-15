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

function clearOperation(status: "running" | "succeeded" | "failed") {
  return {
    id: "op-clear",
    instanceId: "instance-1",
    target: { scope: "conversation", platformAgentId: "agent-a", conversationId: "c1" },
    method: "session/clear",
    capability: "conversation",
    contractRevision: "2026-07-14.3",
    transport: "service",
    sequence: "1",
    status,
    effectState: status === "succeeded" ? "committed" : status,
    terminal: status !== "running",
    createdAt: at,
    updatedAt: at,
    revision: "1",
  };
}

beforeEach(() => {
  realtimeMock.instances.splice(0);
});

describe("Gateway composition — web/desktop credentials", () => {
  it("uses the explicit root X-Request-ID without changing message identity", async () => {
    const headers: Headers[] = [];
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      headers.push(new Headers(init?.headers));
      return json({ success: true, data: { message_id: "request-1" } }, 202);
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });

    await composition.messageCommand.sendMessage({
      conversationId: "c1",
      agentId: "agent-a",
      clientMessageId: "request-1",
      idempotencyKey: "request-1",
      requestId: "root-request-1",
      type: "chat_message",
      content: { text: "hello" },
    });

    expect(headers[0].get("X-Request-ID")).toBe("root-request-1");
    expect(headers[0].get("Idempotency-Key")).toBe("request-1");
  });

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

  it("lets the host refresh a rejected access token and replays the request once", async () => {
    let currentToken = "access-old";
    const authorization: string[] = [];
    const refresh = vi.fn(async (staleAccessToken: string) => {
      expect(staleAccessToken).toBe("access-old");
      currentToken = "access-new";
      return "ok" as const;
    });
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      const value = new Headers(init?.headers).get("Authorization") ?? "";
      authorization.push(value);
      return value === "Bearer access-old"
        ? json({ error: "unauthorized" }, 401)
        : json({ success: true, data: conversation() });
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "mobile",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      accessTokenProvider: async () => currentToken,
      refreshAccessTokenOnUnauthorized: refresh,
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");

    await expect(composition.conversationQuery.getConversation("c1"))
      .resolves.toMatchObject({ id: "c1" });

    expect(refresh).toHaveBeenCalledOnce();
    expect(authorization).toEqual([
      "Bearer access-old",
      "Bearer access-new",
    ]);
  });

  it.each([
    ["transient", "access_token_refresh_transient"],
    ["invalid", "access_token_invalid"],
  ] as const)("does not replay when host recovery reports %s", async (outcome, code) => {
    const refresh = vi.fn(async () => outcome);
    const fetchMock = vi.fn(async () => json({ error: "unauthorized" }, 401));
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "mobile",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      accessTokenProvider: async () => "access-old",
      refreshAccessTokenOnUnauthorized: refresh,
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");

    await expect(composition.conversationQuery.getConversation("c1"))
      .rejects.toMatchObject({ status: 401, code });
    expect(refresh).toHaveBeenCalledOnce();
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("never loops when the replay is also rejected", async () => {
    const refresh = vi.fn(async () => "ok" as const);
    const fetchMock = vi.fn(async () => json({ error: "unauthorized" }, 401));
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "mobile",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      accessTokenProvider: async () => "access-token",
      refreshAccessTokenOnUnauthorized: refresh,
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");

    await expect(composition.conversationQuery.getConversation("c1"))
      .rejects.toMatchObject({ status: 401, code: "access_token_retry_rejected" });
    expect(refresh).toHaveBeenCalledOnce();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("does not invoke access-token recovery for HTTP 403", async () => {
    const refresh = vi.fn(async () => "ok" as const);
    const fetchMock = vi.fn(async () => json({ error: "forbidden" }, 403));
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "mobile",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      accessTokenProvider: async () => "access-token",
      refreshAccessTokenOnUnauthorized: refresh,
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");

    await expect(composition.conversationQuery.getConversation("c1"))
      .rejects.toMatchObject({ status: 403 });
    expect(refresh).not.toHaveBeenCalled();
    expect(fetchMock).toHaveBeenCalledOnce();
  });
});

describe("Gateway composition — asynchronous conversation clear", () => {
  it("waits for the clear operation to succeed before reading the conversation", async () => {
    let operationReads = 0;
    const calls: string[] = [];
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      calls.push(`${init?.method ?? "GET"} ${url}`);
      if (init?.method === "POST" && url.endsWith("/agents/agent-a/conversations/c1/clear")) {
        return json({ status: "accepted", operationId: "op-clear", conversationId: "c1" }, 202);
      }
      if (url.endsWith("/instances/instance-1/operations/op-clear")) {
        operationReads += 1;
        return json(clearOperation(operationReads === 1 ? "running" : "succeeded"));
      }
      if (url.endsWith("/agents/agent-a/conversations/c1")) {
        return json({ ...conversation(), historyGeneration: "2", metadataVersion: "2" });
      }
      throw new Error(`unexpected Gateway request: ${url}`);
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");

    await expect(composition.conversationCommand.clearConversation("c1", "clear-key", "instance-1"))
      .resolves.toMatchObject({ id: "c1", historyGeneration: "2" });

    expect(operationReads).toBe(2);
    expect(calls).toEqual([
      "POST https://gateway.example/api/v1/agents/agent-a/conversations/c1/clear",
      "GET https://gateway.example/api/v1/instances/instance-1/operations/op-clear",
      "GET https://gateway.example/api/v1/instances/instance-1/operations/op-clear",
      "GET https://gateway.example/api/v1/agents/agent-a/conversations/c1",
    ]);
  });

  it("rejects when the clear operation reaches a failed terminal state", async () => {
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      if (init?.method === "POST") {
        return json({ status: "accepted", operationId: "op-clear", conversationId: "c1" }, 202);
      }
      if (url.endsWith("/instances/instance-1/operations/op-clear")) {
        return json({ ...clearOperation("failed"), error: { code: "RUNTIME_EXECUTION_FAILED" } });
      }
      throw new Error(`unexpected Gateway request: ${url}`);
    });
    const composition = createGatewayMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");

    await expect(composition.conversationCommand.clearConversation("c1", "clear-key", "instance-1"))
      .rejects.toThrow("ended with failed");
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
