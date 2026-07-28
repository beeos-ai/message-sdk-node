import { beforeEach, describe, expect, it, vi } from "vitest";

const realtimeMock = vi.hoisted(() => ({
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
    newSubscription(channel: string) {
      const handlers = new Map<string, (ctx: any) => void>();
      const value = {
        channel,
        handlers,
        emit: (name: string, ctx: any = {}) => handlers.get(name)?.(ctx),
      };
      realtimeMock.subscriptions.push(value);
      return {
        on(name: string, handler: (ctx: any) => void) {
          handlers.set(name, handler);
        },
        subscribe() {},
        unsubscribe() {},
        removeAllListeners() {},
      };
    }
  },
}));

import { createReactNativeMessageClientComposition } from "../src/react-native-runtime.js";
import { createMessageClient } from "../src/unified-client.js";

const at = "2026-07-28T00:00:00.000Z";

function json(
  value: unknown,
  status = 200,
  headers: Readonly<Record<string, string>> = {},
): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

function conversation(
  title = "Renamed",
  agentId = "agent-a",
  conversationId = "c1",
) {
  return {
    conversationId,
    agentId,
    title,
    state: "active",
    historyGeneration: "1",
    metadataVersion: "2",
    createdAt: at,
    lastActivityAt: at,
  };
}

function operation(status: "running" | "cancelled" = "running") {
  const terminal = status === "cancelled";
  return {
    id: "op1",
    instanceId: "i1",
    target: { scope: "instance" },
    method: "instance/start",
    capability: "instance",
    contractRevision: "2026-07-14.3",
    transport: "service",
    sequence: terminal ? "2" : "1",
    status,
    effectState: terminal ? "cancelled" : "running",
    terminal,
    createdAt: at,
    updatedAt: at,
    revision: terminal ? "2" : "1",
  };
}

beforeEach(() => {
  realtimeMock.instances.splice(0);
  realtimeMock.subscriptions.splice(0);
});

describe("React Native Gateway composition", () => {
  it("shares one composition across explicit agent routes and rejects route aliasing", async () => {
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = String(input);
      const agentId = url.includes("/agents/agent-b/") ? "agent-b" : "agent-a";
      if (url.includes("state=closed")) {
        return json({ success: true, data: { conversations: [] } });
      }
      return json({
        success: true,
        data: {
          conversations: [conversation(
            agentId,
            agentId,
            agentId === "agent-a" ? "c1" : "c2",
          )],
        },
      });
    });
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    expect(composition.privateConversationDirectoryQuery).toBeUndefined();
    await expect(composition.conversationQuery.listConversations())
      .rejects.toThrow("explicit agentId");
    const [agentA, agentB] = await Promise.all([
      composition.conversationQuery.listConversationsForAgent!("agent-a"),
      composition.conversationQuery.listConversationsForAgent!("agent-b"),
    ]);
    expect(agentA.conversations[0]).toMatchObject({ id: "c1", agentId: "agent-a" });
    expect(agentB.conversations[0]).toMatchObject({ id: "c2", agentId: "agent-b" });
    expect(() => composition.conversationRoutes!.bindConversation("c1", "agent-b"))
      .toThrow("different agent");
  });

  it("decodes model override set and explicit clear from authoritative Gateway conversations", async () => {
    const rows = [
      { ...conversation(), modelOverrideId: "provider/model" },
      { ...conversation(), model_override_id: null },
    ];
    const fetchMock = vi.fn(async () => json({ success: true, data: rows.shift() }));
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    await expect(composition.conversationQuery.getConversation("c1"))
      .resolves.toMatchObject({ modelOverrideId: "provider/model" });
    await expect(composition.conversationQuery.getConversation("c1"))
      .resolves.toMatchObject({ modelOverrideId: null });
  });

  it("maps rename, send and typed method routes with caller-owned identities", async () => {
    const calls: Array<{
      url: string;
      method: string;
      headers: Headers;
      body?: unknown;
    }> = [];
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const value = {
        url: String(input),
        method: init?.method ?? "GET",
        headers: new Headers(init?.headers),
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
      };
      calls.push(value);
      if (value.url.endsWith("/messages")) {
        return json({
          success: true,
          data: {
            messageId: "message-key",
            runtime_dispatch: { status: "accepted" },
          },
        }, 202);
      }
      if (value.url.endsWith("/methods")) {
        return json({
          jsonrpc: "2.0",
          id: "op1",
          result: {
            status: "accepted",
            operationId: "op1",
            contractRevision: "2026-07-14.3",
            transport: "service",
          },
        }, 202, { "X-BeeOS-Operation-Id": "op1" });
      }
      return json({ success: true, data: conversation() });
    });
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");

    await composition.conversationCommand.updateConversation({
      conversationId: "c1",
      title: "Renamed",
      idempotencyKey: "rename-key",
    });
    const receipt = await composition.messageCommand.sendMessage({
      conversationId: "c1",
      agentId: "agent-a",
      clientMessageId: "message-key",
      idempotencyKey: "message-key",
      type: "chat_message",
      content: { text: "hello" },
    });
    const method = await composition.runtimeMethods.executeMethod({
      operationId: "op1",
      instanceId: "i1",
      target: { scope: "instance" },
      method: "instance/start",
      params: {},
      idempotencyKey: "operation-key",
    });

    expect(receipt).toEqual({
      messageId: "message-key",
      outcome: "accepted",
      runtimeDispatch: { status: "accepted" },
    });
    expect(method).toMatchObject({ operationId: "op1", outcome: "accepted" });
    expect(calls[0]).toMatchObject({
      url: "https://gateway.example/api/v1/agents/agent-a/conversations/c1",
      method: "PATCH",
      body: { title: "Renamed" },
    });
    expect(calls[0].headers.get("Idempotency-Key")).toBe("rename-key");
    expect(calls[1]).toMatchObject({
      url: "https://gateway.example/api/v1/agents/agent-a/conversations/c1/messages",
      method: "POST",
      body: { message: "hello", idempotency_key: "message-key" },
    });
    expect(calls[2]).toMatchObject({
      url: "https://gateway.example/api/v1/instances/i1/methods",
      method: "POST",
      body: {
        jsonrpc: "2.0",
        id: "op1",
        method: "instance/start",
        params: {},
      },
    });
    expect(calls[2].headers.get("X-BeeOS-Operation-Id")).toBe("op1");
    expect(calls[2].headers.get("Idempotency-Key")).toBe("operation-key");
    expect(calls.every((call) => call.headers.get("Authorization") === "Bearer access-token"))
      .toBe(true);
  });

  it("surfaces an invalid authoritative runtime_dispatch receipt as a protocol error", async () => {
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: vi.fn(async () => json({
        success: true,
        data: {
          messageId: "message-key",
          runtime_dispatch: {
            status: "failed",
            code: "delivery_unconfirmed",
          },
        },
      }, 202)),
    });

    await expect(composition.messageCommand.sendMessage({
      conversationId: "c1",
      agentId: "agent-a",
      clientMessageId: "message-key",
      idempotencyKey: "message-key",
      type: "chat_message",
      content: { text: "hello" },
    })).rejects.toMatchObject({ name: "RuntimeDispatchContractError" });
  });

  it("routes session/set_model only through its typed conversation target", async () => {
    const calls: Array<{ url: string; method: string; body?: unknown; headers: Headers }> = [];
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({
        url: String(input),
        method: init?.method ?? "GET",
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        headers: new Headers(init?.headers),
      });
      return json({
        status: "accepted",
        operationId: "model-op",
        contractRevision: "2026-07-14.3",
        transport: "service",
      }, 202, { "X-BeeOS-Operation-Id": "model-op" });
    });
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    await expect(composition.runtimeMethods.executeMethod({
      operationId: "wrong-target",
      instanceId: "i1",
      target: { scope: "instance" },
      method: "session/set_model",
      params: { modelOverrideId: "openai/gpt-4.1" },
      idempotencyKey: "wrong-target-key",
    })).rejects.toThrow("conversation target");
    await expect(composition.runtimeMethods.executeMethod({
      operationId: "unsupported",
      instanceId: "i1",
      target: {
        scope: "conversation",
        platformAgentId: "agent-a",
        conversationId: "c1",
      },
      method: "session/clear",
      params: {},
      idempotencyKey: "unsupported-key",
    })).rejects.toThrow("conversation_route_not_supported");
    expect(fetchMock).not.toHaveBeenCalled();

    const receipt = await composition.runtimeMethods.executeMethod({
      operationId: "model-op",
      instanceId: "i1",
      target: {
        scope: "conversation",
        platformAgentId: "agent-a",
        conversationId: "c1",
      },
      method: "session/set_model",
      params: { modelOverrideId: "openai/gpt-4.1" },
      idempotencyKey: "model-key",
    });
    expect(receipt).toMatchObject({ operationId: "model-op", outcome: "accepted" });
    expect(calls[0]).toMatchObject({
      url: "https://gateway.example/api/v1/agents/agent-a/conversations/c1/model",
      method: "PUT",
      body: { modelOverrideId: "openai/gpt-4.1" },
    });
    expect(calls[0].headers.get("X-BeeOS-Operation-Id")).toBe("model-op");
    expect(calls[0].headers.get("Idempotency-Key")).toBe("model-key");
  });

  it("strictly decodes active operations and carries ownership into get/cancel", async () => {
    let cancelled = false;
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/operations?")) {
        return json({ operations: [operation()], nextCursor: "next" });
      }
      if (url.endsWith("/cancel")) {
        cancelled = true;
        return json({ status: "accepted", operationId: "op1" }, 202);
      }
      if (url.endsWith("/operations/op1")) {
        return json(operation(cancelled ? "cancelled" : "running"));
      }
      return json({});
    });
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: fetchMock,
    });
    const active = await composition.runtimeMethods.listActiveOperations("i1");
    expect(active.operations[0]).toMatchObject({
      id: "op1",
      instanceId: "i1",
      status: "running",
      terminal: false,
    });
    expect(await composition.runtimeMethods.getOperation("op1"))
      .toMatchObject({ status: "running" });
    expect(await composition.runtimeMethods.cancelOperation("op1", "cancel-key"))
      .toMatchObject({ status: "cancelled", terminal: true });
    const cancelCall = fetchMock.mock.calls.find(([input]) => String(input).endsWith("/cancel"));
    expect(new Headers(cancelCall?.[1]?.headers).get("Idempotency-Key")).toBe("cancel-key");
  });

  it("rejects partial operation snapshots instead of retaining the SSE contract", async () => {
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: async () => json({
        operations: [{ id: "op1", method: "instance/start", state: "running" }],
      }),
    });
    await expect(composition.runtimeMethods.listActiveOperations("i1"))
      .rejects.toThrow();
  });

  it("rejects a foreign conversation row in a requested message hydrate page", async () => {
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: async () => json({
        success: true,
        data: {
          historyGeneration: "1",
          historyBoundaryOffset: "0",
          latestOffset: "1",
          hasMore: false,
          messages: [{
            messageId: "m1",
            conversationId: "foreign",
            publisherId: "user:u1",
            type: "chat_message",
            body: "wrong",
            state: "completed",
            offset: "1",
            historyGeneration: "1",
            createdAt: at,
            updatedAt: at,
          }],
        },
      }),
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    await expect(composition.messageQuery.listMessages("c1"))
      .rejects.toThrow("foreign conversation");
  });

  it("keeps Gateway history boundary separate from the latest message offset", async () => {
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: async () => json({
        success: true,
        data: {
          historyGeneration: "4",
          history_boundary_offset: "3",
          latestOffset: "9",
          hasMore: false,
          messages: [],
        },
      }),
    });
    composition.conversationRoutes!.bindConversation("c1", "agent-a");
    const page = await composition.messageQuery.listMessages("c1");
    expect(page).toMatchObject({
      historyGeneration: "4",
      historyBoundaryOffset: "3",
      latestOffset: "9",
      hasMore: false,
    });
  });

  it("pins currentPrincipal to the token response and hides raw subscribe/publish", async () => {
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: async () => json({
        token: "realtime-token",
        centrifugo_url: "wss://realtime.example/connection/websocket",
        principal_id: "user:u1",
      }),
    });
    const opening = composition.realtime.connect({ onEvent() {}, onState() {} });
    await vi.waitFor(() => { expect(realtimeMock.instances).toHaveLength(1); });
    realtimeMock.instances[0].emit("connected");
    const session = await opening;
    expect(composition.currentPrincipal.currentPrincipalId()).toBe("user:u1");

    const watching = Promise.resolve(session.setConversationWatched("c1", true));
    expect(realtimeMock.subscriptions[0].channel).toBe("conv:c1");
    expect("publish" in session).toBe(false);
    expect("subscribe" in session).toBe(false);
    realtimeMock.subscriptions[0].emit("subscribed");
    await watching;
  });

  it("keeps an unmapped private inbox hint pending until an explicit watch binds it", async () => {
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:u1" },
      fetch: async () => json({
        token: "realtime-token",
        centrifugo_url: "wss://realtime.example/connection/websocket",
        principal_id: "user:u1",
      }),
    });
    const client = createMessageClient(composition);
    const opening = client.connect();
    await vi.waitFor(() => { expect(realtimeMock.instances).toHaveLength(1); });
    realtimeMock.instances[0].emit("connected");
    await opening;
    realtimeMock.instances[0].emit("publication", {
      data: {
        schemaVersion: 1,
        eventId: "available-c1",
        type: "inbox.conversation.available",
        scope: { tenantId: "t1" },
        actor: { kind: "service", id: "message-service" },
        ordering: {
          streamSequence: "1",
          entityRevision: "1",
          completeness: "full",
        },
        correlation: {},
        occurredAt: at,
        data: { conversationId: "c1" },
      },
    });
    expect(client.getSnapshot().connection).toBe("connected");
    expect(realtimeMock.subscriptions).toHaveLength(0);
  });

  it("fails the WSS handshake when token identity and login identity differ", async () => {
    const composition = createReactNativeMessageClientComposition({
      gatewayUrl: "https://gateway.example",
      accessTokenProvider: async () => "access-token",
      currentPrincipal: { currentPrincipalId: () => "user:expected" },
      fetch: async () => json({
        token: "realtime-token",
        centrifugo_url: "wss://realtime.example/connection/websocket",
        principal_id: "user:other",
      }),
    });
    await expect(composition.realtime.connect({ onEvent() {}, onState() {} }))
      .rejects.toThrow("does not match");
    expect(realtimeMock.instances).toHaveLength(0);
  });
});
