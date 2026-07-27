import { describe, expect, it } from "vitest";

import {
  createMessageServiceHttpTransport,
  type AnyRealtimeEventV1,
} from "../src/facade/index.js";

function event(sequence: string): AnyRealtimeEventV1 {
  return {
    schemaVersion: 1,
    eventId: `event-${sequence}`,
    type: "message.created",
    scope: { tenantId: "tenant-1", conversationId: "conversation-1", messageId: `message-${sequence}` },
    actor: { kind: "service", id: "message-service" },
    ordering: { streamSequence: sequence, completeness: "full", historyGeneration: "1" },
    correlation: {},
    occurredAt: "2026-07-28T00:00:00.000Z",
    data: {
      message: {
        id: `message-${sequence}`,
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

function transport(fetchImpl: typeof fetch) {
  return createMessageServiceHttpTransport({
    apiBaseUrl: "https://msg.example/",
    authProvider: { async getAccessToken() { return "user-bearer"; } },
    fetchImpl,
    async executeMethod() { return { operationId: "operation-1", outcome: "accepted" }; },
    async hydrateConversation(input) { return { conversationId: input.conversationId, events: [] }; },
  });
}

describe("Message Service v2 HTTPS transport", () => {
  it("sends one generic content request with a caller-owned idempotency key and no retry", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const client = transport((async (url, init) => {
      calls.push({ url: String(url), init });
      return new Response(JSON.stringify({ id: "client-message-1" }), {
        status: 201,
        headers: { "content-type": "application/json", "x-request-id": "request-1" },
      });
    }) as typeof fetch);

    await expect(client.sendMessage({
      conversationId: "conversation/a",
      clientMessageId: "client-message-1",
      type: "chat_message",
      content: { text: "trace" },
      replyTo: "reply-1",
    })).resolves.toEqual({ messageId: "client-message-1", outcome: "created", correlationId: "request-1" });

    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe("https://msg.example/api/v2/conversations/conversation%2Fa/messages");
    expect(calls[0].init?.method).toBe("POST");
    const headers = new Headers(calls[0].init?.headers);
    expect(headers.get("authorization")).toBe("Bearer user-bearer");
    expect(headers.get("content-type")).toBe("application/json");
    expect(headers.get("idempotency-key")).toBe("client-message-1");
    expect(calls[0].init?.body).toBe(JSON.stringify({ type: "chat_message", content: { text: "trace" }, reply_to: "reply-1" }));
  });

  it("maps approved text and optional parts to the confirmed v2 envelope fields", async () => {
    const bodies: string[] = [];
    const headers: Headers[] = [];
    const client = transport((async (_url, init) => {
      bodies.push(String(init?.body));
      headers.push(new Headers(init?.headers));
      return new Response(JSON.stringify({ id: "text-message-1" }), { status: 201 });
    }) as typeof fetch);

    await expect(client.sendMessage({
      conversationId: "conversation-1",
      clientMessageId: "text-message-1",
      text: "canonical body",
      parts: [{ type: "custom", kind: "trace", data: { value: 1 } }],
    })).resolves.toMatchObject({ messageId: "text-message-1", outcome: "created" });
    await client.sendMessage({
      conversationId: "conversation-1",
      clientMessageId: "text-message-2",
      text: "body only",
    });

    expect(bodies).toEqual([
      JSON.stringify({
        type: "chat_message",
        body: "canonical body",
        parts: [{ type: "custom", kind: "trace", data: { value: 1 } }],
      }),
      JSON.stringify({ type: "chat_message", body: "body only" }),
    ]);
    expect(headers[0].get("idempotency-key")).toBe("text-message-1");
    expect(headers[1].get("idempotency-key")).toBe("text-message-2");
  });

  it("never synthesizes duplicate semantics or retries a failed send", async () => {
    let calls = 0;
    const client = transport((async () => {
      calls++;
      return new Response("ignored", { status: 503 });
    }) as typeof fetch);

    await expect(client.sendMessage({
      conversationId: "conversation-1",
      clientMessageId: "stable-key-1",
      type: "chat_message",
      content: { text: "trace" },
    })).rejects.toThrow("HTTP 503");
    expect(calls).toBe(1);
  });

  it("collects ordered, deduped delta pages with exact sequential opaque cursor requests", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const client = transport((async (url, init) => {
      calls.push({ url: String(url), init });
      const page = new URL(String(url)).searchParams.get("cursor");
      return new Response(JSON.stringify({
        events: page === "opaque previous+/="
          ? Array.from({ length: 100 }, (_, index) => event(String(index + 1)))
          : [event("101"), event("100")],
        next_cursor: page === "opaque previous+/=" ? "opaque-page-2" : "opaque-final-cursor",
        completeness: "delta",
        has_more: page === "opaque previous+/=",
      }), { status: 200, headers: { "content-type": "application/json" } });
    }) as typeof fetch);

    await expect(client.rebase({
      syncCursor: "opaque previous+/=",
      cursor: { streamSequence: "0", historyGeneration: "1" },
      reason: "sequence_gap",
    })).resolves.toMatchObject({
      syncCursor: "opaque-final-cursor",
      cursor: { streamSequence: "101", historyGeneration: "1" },
      events: Array.from({ length: 101 }, (_, index) => expect.objectContaining({ eventId: `event-${index + 1}` })),
    });
    expect(calls).toHaveLength(2);
    expect(calls[0].url).toBe("https://msg.example/api/v2/sync?cursor=opaque+previous%2B%2F%3D");
    expect(calls[1].url).toBe("https://msg.example/api/v2/sync?cursor=opaque-page-2");
    expect(calls[0].init?.method).toBe("GET");
    expect(new Headers(calls[0].init?.headers).get("authorization")).toBe("Bearer user-bearer");
  });

  it("returns an unchanged opaque cursor for an empty terminal delta page", async () => {
    const client = transport((async () => new Response(JSON.stringify({
      events: [], next_cursor: "opaque", completeness: "delta", has_more: false,
    }), { status: 200 })) as typeof fetch);

    await expect(client.rebase({
      syncCursor: "opaque",
      cursor: { streamSequence: "9", historyGeneration: "1" },
      reason: "sequence_gap",
    })).resolves.toMatchObject({
      events: [],
      cursor: { streamSequence: "9", historyGeneration: "1" },
      syncCursor: "opaque",
    });
  });

  it.each([
    ["a cursor loop", { events: [event("1")], next_cursor: "opaque", completeness: "delta", has_more: true }, "cursor must advance"],
    ["an empty continuation cursor", { events: [event("1")], next_cursor: "", completeness: "delta", has_more: true }, "requires next_cursor"],
    ["a non-delta completeness", { events: [event("1")], next_cursor: "next", completeness: "full", has_more: false }, "requires delta pages"],
  ])("fails closed on %s without exposing a partial page", async (_case, responseBody, message) => {
    const calls: string[] = [];
    const client = transport((async (url) => {
      calls.push(String(url));
      return new Response(JSON.stringify(responseBody), { status: 200 });
    }) as typeof fetch);

    await expect(client.rebase({ syncCursor: "opaque", reason: "sequence_gap" })).rejects.toThrow(message);
    expect(calls).toEqual(["https://msg.example/api/v2/sync?cursor=opaque"]);
  });

  it("does not expose collected earlier pages when a later page is invalid", async () => {
    const calls: string[] = [];
    const client = transport((async (url) => {
      calls.push(String(url));
      const cursor = new URL(String(url)).searchParams.get("cursor");
      return new Response(JSON.stringify(cursor === "opaque"
        ? { events: [event("1")], next_cursor: "page-2", completeness: "delta", has_more: true }
        : { events: [event("2")], next_cursor: "final", completeness: "full", has_more: false }), { status: 200 });
    }) as typeof fetch);

    await expect(client.rebase({ syncCursor: "opaque", reason: "sequence_gap" })).rejects.toThrow("requires delta pages");
    expect(calls).toEqual([
      "https://msg.example/api/v2/sync?cursor=opaque",
      "https://msg.example/api/v2/sync?cursor=page-2",
    ]);
  });

  it("fails closed on generation or projection epoch recovery and missing bearer tokens", async () => {
    const client = transport((async () => new Response(JSON.stringify({
      events: [], next_cursor: "next", completeness: "delta", has_more: false,
    }), { status: 200 })) as typeof fetch);
    await expect(client.rebase({ reason: "sequence_gap" })).rejects.toThrow("server-issued sync cursor");
    await expect(client.rebase({ syncCursor: "opaque", reason: "history_generation_changed" })).rejects.toThrow("cannot recover a history generation");
    await expect(client.rebase({ syncCursor: "opaque", reason: "projection_epoch_changed" })).rejects.toThrow("cannot recover a history generation or projection epoch");

    const noToken = createMessageServiceHttpTransport({
      apiBaseUrl: "https://msg.example",
      authProvider: { async getAccessToken() { return ""; } },
      fetchImpl: (async () => { throw new Error("must not fetch"); }) as typeof fetch,
      async executeMethod() { return { operationId: "operation-1", outcome: "accepted" }; },
      async hydrateConversation(input) { return { conversationId: input.conversationId, events: [] }; },
    });
    await expect(noToken.sendMessage({
      conversationId: "conversation-1", clientMessageId: "key-1", type: "chat_message", content: { text: "trace" },
    })).rejects.toThrow("empty bearer token");
  });

  it("delegates methods and hydrate only through explicit typed seams", async () => {
    const executed: string[] = [];
    const hydrated: string[] = [];
    const client = createMessageServiceHttpTransport({
      apiBaseUrl: "http://127.0.0.1:8787",
      authProvider: { async getAccessToken() { return "test-token"; } },
      fetchImpl: (async () => new Response("unexpected", { status: 500 })) as typeof fetch,
      async executeMethod(input) {
        executed.push(input.method);
        return { operationId: "operation-1", outcome: "accepted" };
      },
      async hydrateConversation(input) {
        hydrated.push(input.conversationId);
        return { conversationId: input.conversationId, events: [] };
      },
    });
    await client.executeMethod({ instanceId: "instance-1", method: "instance.start", params: {}, idempotencyKey: "key-1" });
    await client.hydrateConversation({ conversationId: "conversation-1" });
    expect(executed).toEqual(["instance.start"]);
    expect(hydrated).toEqual(["conversation-1"]);

    expect(() => createMessageServiceHttpTransport({
      apiBaseUrl: "http://msg.example",
      authProvider: { async getAccessToken() { return "token"; } },
      async executeMethod() { return { operationId: "operation-1", outcome: "accepted" }; },
      async hydrateConversation(input) { return { conversationId: input.conversationId, events: [] }; },
    })).toThrow("https:// outside localhost tests");
  });

  it("rejects ambiguous text/content inputs and non-JSON-safe parts before fetch", async () => {
    let calls = 0;
    const client = transport((async () => {
      calls++;
      return new Response(JSON.stringify({ id: "unexpected" }), { status: 201 });
    }) as typeof fetch);
    await expect(client.sendMessage({
      conversationId: "conversation-1",
      clientMessageId: "key-1",
      text: "trace",
      parts: [{ type: "custom", kind: "trace", data: { value: 1 } }],
      content: { text: "duplicate" },
    } as never)).rejects.toThrow("cannot include content or type");
    await expect(client.sendMessage({
      conversationId: "conversation-1",
      clientMessageId: "key-ambiguous",
      text: "trace",
      content: { text: "duplicate" },
    } as never)).rejects.toThrow("cannot include content or type");
    await expect(client.sendMessage({
      conversationId: "conversation-1",
      clientMessageId: "key-2",
      text: "trace",
      parts: [Number.NaN],
    } as never)).rejects.toThrow("JSON-safe array");
    expect(calls).toBe(0);
  });
});
