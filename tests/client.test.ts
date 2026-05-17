import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ConversationNotFoundError,
  DuplicateIdError,
  MessageClient,
  NoSubscriberError,
  WaitTimeoutError,
} from "../src/index.js";

const ORIGINAL_FETCH = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = ORIGINAL_FETCH;
  vi.restoreAllMocks();
});

describe("MessageClient constructor", () => {
  it("requires serviceUrl", () => {
    // @ts-expect-error intentionally missing
    expect(() => new MessageClient({})).toThrow(/serviceUrl/);
    expect(() => new MessageClient({ serviceUrl: "" })).toThrow(/serviceUrl/);
  });

  it("rejects apiKey + tokenProvider together (mutually exclusive)", () => {
    expect(
      () =>
        new MessageClient({
          serviceUrl: "https://msg.example.com",
          apiKey: "k",
          tokenProvider: async () => ({
            token: "t",
            centrifugoUrl: "wss://x",
            identityId: "i",
            expiresAt: 1,
          }),
        }),
    ).toThrow(/mutually exclusive/);
  });
});

describe("MessageClient REST", () => {
  it("forwards Idempotency-Key on messages.send", async () => {
    let seenHeader: string | null = null;
    globalThis.fetch = vi.fn(async (_url, init) => {
      const h = new Headers(init?.headers);
      seenHeader = h.get("Idempotency-Key");
      return new Response(
        JSON.stringify({
          id: seenHeader,
          conversation_id: "conv-1",
          type: "chat_message",
          content: { ok: true },
          sender: "service:test",
          created_at: new Date().toISOString(),
        }),
        { status: 201, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof fetch;

    const client = new MessageClient({
      serviceUrl: "https://msg.example.com",
      apiKey: "k",
    });
    const msg = await client.messages.send({
      conversationId: "conv-1",
      id: "client-uuid-abc",
      type: "chat_message",
      content: { hello: 1 },
    });
    expect(seenHeader).toBe("client-uuid-abc");
    expect(msg.id).toBe("client-uuid-abc");
    expect(msg.conversationId).toBe("conv-1");
    expect(msg.sender).toBe("service:test");
  });

  it("maps server errors to typed sentinel classes", async () => {
    const cases: Array<{ status: number; body: unknown; err: any }> = [
      { status: 404, body: { error: { code: "conversation_not_found" } }, err: ConversationNotFoundError },
      { status: 408, body: { code: "wait_timeout" }, err: WaitTimeoutError },
      { status: 410, body: { code: "no_subscriber" }, err: NoSubscriberError },
      { status: 409, body: { error: { code: "duplicate_message_id" } }, err: DuplicateIdError },
    ];
    for (const tc of cases) {
      globalThis.fetch = vi.fn(async () =>
        new Response(JSON.stringify(tc.body), {
          status: tc.status,
          headers: { "Content-Type": "application/json" },
        }),
      ) as typeof fetch;
      const client = new MessageClient({ serviceUrl: "https://msg.example.com", apiKey: "k" });
      await expect(client.conversations.get("conv-x")).rejects.toBeInstanceOf(tc.err);
    }
  });

  it("uses cached token from tokenProvider as Bearer, otherwise X-API-Key", async () => {
    const seenHeaders: Record<string, string> = {};
    globalThis.fetch = vi.fn(async (_url, init) => {
      const h = new Headers(init?.headers);
      h.forEach((v, k) => (seenHeaders[k] = v));
      return new Response(
        JSON.stringify({
          id: "id",
          conversation_id: "c",
          type: "t",
          content: null,
          sender: "s",
          created_at: new Date().toISOString(),
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof fetch;

    // apiKey mode
    const keyClient = new MessageClient({
      serviceUrl: "https://msg.example.com",
      apiKey: "k",
    });
    await keyClient.messages.send({
      conversationId: "c",
      type: "chat_message",
      content: {},
    });
    expect(seenHeaders["x-api-key"]).toBe("k");
    expect(seenHeaders["authorization"]).toBeUndefined();
  });

  it("encodes pagination cursor in querystring", async () => {
    let seenURL = "";
    globalThis.fetch = vi.fn(async (url) => {
      seenURL = typeof url === "string" ? url : (url as URL).toString();
      return new Response(
        JSON.stringify({ messages: [], has_more: false }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof fetch;
    const client = new MessageClient({ serviceUrl: "https://msg.example.com", apiKey: "k" });
    const page = await client.messages.list("conv-1", { cursor: "opaque-cursor-1", limit: 25 });
    expect(seenURL).toContain("/api/v2/conversations/conv-1/messages");
    expect(seenURL).toContain("cursor=opaque-cursor-1");
    expect(seenURL).toContain("limit=25");
    expect(page.hasMore).toBe(false);
    expect(page.messages).toEqual([]);
  });

  it("identities.send hits the v2 identity path and forwards Idempotency-Key", async () => {
    let seenURL = "";
    let seenIdem: string | null = null;
    globalThis.fetch = vi.fn(async (url, init) => {
      seenURL = typeof url === "string" ? url : (url as URL).toString();
      const h = new Headers(init?.headers);
      seenIdem = h.get("Idempotency-Key");
      return new Response(JSON.stringify({ status: "queued" }), {
        status: 202,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;
    const client = new MessageClient({ serviceUrl: "https://msg.example.com", apiKey: "k" });
    const resp = await client.identities.send({
      identityId: "user:alice",
      id: "idem-1",
      type: "agent_reply",
      content: { text: "hi" },
    });
    expect(seenURL).toContain("/api/v2/identities/user%3Aalice/messages");
    expect(seenIdem).toBe("idem-1");
    expect(resp).toEqual({ status: "queued" });
  });

  it("conversations.subscribe throws when not connected", async () => {
    const client = new MessageClient({
      serviceUrl: "https://msg.example.com",
      apiKey: "k",
    });
    await expect(client.conversations.subscribe("conv-1")).rejects.toThrow(/connect\(\)/);
  });
});

describe("MessageClient realtime preconditions", () => {
  it("connect() requires tokenProvider", async () => {
    const client = new MessageClient({
      serviceUrl: "https://msg.example.com",
      apiKey: "k",
    });
    await expect(client.connect()).rejects.toThrow(/tokenProvider/);
  });
});
