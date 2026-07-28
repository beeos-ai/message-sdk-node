import { afterEach, describe, expect, it, vi } from "vitest";

import { OutcomeUnknownError } from "../src/errors.js";
import { createNodeMessageClientComposition } from "../src/node-runtime.js";
import { RuntimeDispatchContractError } from "../src/protocol/index.js";

const base = "https://message.example";
const at = "2026-07-28T00:00:00.123456Z";

function options() {
  return {
    identityId: "agent 1",
    serviceUrl: base,
    tokenProvider: async () => ({
      token: "token",
      centrifugoUrl: "wss://realtime.example/connection/websocket",
      serviceUrl: base,
      identityId: "agent 1",
      expiresAt: 2_000_000_000,
    }),
  };
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Node Message Service composition route matrix", () => {
  it("uses authoritative identity directories and cursor message pages", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request) => {
      const url = String(input);
      requests.push(url);
      if (url.includes("/identities/agent%201/conversations")) {
        return json({
          conversations: [{
            id: "c1",
            state: "open",
            history_generation: 4,
            metadata_version: 7,
            updated_at: at,
          }],
          has_more: false,
        });
      }
      if (url.endsWith("/api/v2/conversations/c1")) {
        return json({
          id: "c1",
          state: "open",
          history_generation: 4,
          metadata_version: 7,
          updated_at: at,
        });
      }
      if (url.includes("/api/v2/conversations/c1/messages?cursor=page-1")) {
        return json({
          messages: [
            {
              id: "m1",
              conversation_id: "c1",
              sender: "agent-a",
              type: "agent_reply",
              content: {
                metadata: {
                  delivery_principal: "agent-a",
                  target_agent_id: "agent-a",
                  runtime_binding_id: "binding-1",
                  runtime_epoch: "12",
                },
              },
              body: "stream",
              state: "streaming",
              history_generation: 4,
              offset: 9,
              created_at: at,
              updated_at: "2026-07-28T00:00:00.123456Z",
            },
            {
              id: "m1-terminal",
              conversation_id: "c1",
              sender: "agent-a",
              type: "agent_reply",
              body: "done",
              state: "completed",
              history_generation: 4,
              offset: 9,
              created_at: at,
              updated_at: "2026-07-28T00:00:00.123457Z",
            },
          ],
          latest_offset: 9,
          next_cursor: "page-2",
          has_more: true,
        });
      }
      throw new Error(`unexpected request ${url}`);
    }));

    const composition = createNodeMessageClientComposition(options());
    await composition.privateConversationDirectoryQuery!.listPrivateConversations("open", "dir-1");
    await composition.privateConversationDirectoryQuery!.listPrivateConversations("closed");
    await composition.conversationQuery.getConversation("c1");
    const page = await composition.messageQuery.listMessages("c1", "page-1");

    expect(requests[0]).toBe(
      `${base}/api/v2/identities/agent%201/conversations?state=open&cursor=dir-1`,
    );
    expect(requests[1]).toBe(
      `${base}/api/v2/identities/agent%201/conversations?state=closed`,
    );
    expect(requests[3]).toBe(`${base}/api/v2/conversations/c1/messages?cursor=page-1`);
    expect(page).toMatchObject({ latestOffset: "9", nextSince: "page-2", hasMore: true });
    expect(BigInt(page.messages[1].revision)).toBeGreaterThan(BigInt(page.messages[0].revision));
    expect(page.messages[0]).toMatchObject({
      senderId: "agent-a",
      historyGeneration: "4",
      offset: "9",
      content: {
        metadata: {
          delivery_principal: "agent-a",
          target_agent_id: "agent-a",
          runtime_binding_id: "binding-1",
          runtime_epoch: "12",
        },
      },
      revision: (
        BigInt(Date.parse("2026-07-28T00:00:00Z")) * 1000n + 123456n
      ).toString(),
    });
  });

  it("routes v3 stream writes with conversation, UTF-8 body offset and stable keys", async () => {
    const calls: Array<{ url: string; method: string; body?: unknown; key?: string }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({
        url: String(input),
        method: init?.method ?? "GET",
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        key: new Headers(init?.headers).get("Idempotency-Key") ?? undefined,
      });
      return json({
        id: "open-key",
        idempotent: false,
        conversation_id: "c1",
        history_generation: 1,
        offset: 1,
        updated_at: at,
      }, init?.method === "POST" ? 201 : 200);
    }));
    const composition = createNodeMessageClientComposition(options());
    const receipt = await composition.messageStream.startStream({
      conversationId: "c1",
      clientMessageId: "open-key",
      idempotencyKey: "open-key",
      type: "agent_reply",
      content: {},
    });
    await composition.messageStream.append("c1", receipt.messageId, "🐝", 4, "append-key");
    await composition.messageStream.finalize(
      "c1",
      receipt.messageId,
      "completed",
      "terminal-key",
      "end_turn",
    );

    expect(receipt).toMatchObject({ messageId: "open-key", outcome: "created", idempotent: false });
    expect(calls).toEqual([
      {
        url: `${base}/api/v3/conversations/c1/messages`,
        method: "POST",
        body: {
          id: "open-key",
          type: "agent_reply",
          content: {},
          state: "streaming",
        },
        key: "open-key",
      },
      {
        url: `${base}/api/v3/conversations/c1/messages/open-key`,
        method: "PATCH",
        body: { body_append: "🐝", body_from: 4 },
        key: "append-key",
      },
      {
        url: `${base}/api/v3/conversations/c1/messages/open-key`,
        method: "PATCH",
        body: { state: "completed", stop_reason: "end_turn" },
        key: "terminal-key",
      },
    ]);
  });

  it("updates title through the existing control route and verifies authoritative convergence", async () => {
    const calls: Array<{ url: string; method: string; body?: unknown; key?: string }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      calls.push({
        url,
        method: init?.method ?? "GET",
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        key: new Headers(init?.headers).get("Idempotency-Key") ?? undefined,
      });
      if (init?.method === "PATCH") {
        return json({ data: { title: "new title", metadata_version: 8 } });
      }
      return json({
        id: "c1",
        title: "new title",
        state: "open",
        history_generation: 4,
        metadata_version: 8,
        updated_at: at,
      });
    }));

    const composition = createNodeMessageClientComposition(options());
    const result = await composition.conversationCommand.updateConversation({
      conversationId: "c1",
      title: "new title",
      idempotencyKey: "title-key",
    });

    expect(result).toMatchObject({ id: "c1", title: "new title", revision: "8" });
    expect(calls).toEqual([
      {
        url: `${base}/api/v1/control/conversations/c1`,
        method: "PATCH",
        body: { ownerIdentityId: "agent 1", title: "new title" },
        key: "title-key",
      },
      {
        url: `${base}/api/v2/conversations/c1`,
        method: "GET",
        body: undefined,
        key: undefined,
      },
    ]);
  });

  it("shares credential singleflight and retries one 401 with a refreshed token", async () => {
    let issued = 0;
    const tokenProvider = vi.fn(async () => {
      issued += 1;
      return {
        token: `token-${issued}`,
        centrifugoUrl: "wss://realtime.example/connection/websocket",
        serviceUrl: base,
        identityId: "agent 1",
        expiresAt: 2_000_000_000,
      };
    });
    const seen: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      const authorization = new Headers(init?.headers).get("Authorization") ?? "";
      seen.push(authorization);
      if (authorization === "Bearer token-1") return json({ code: "unauthorized" }, 401);
      return json({ conversations: [], has_more: false });
    }));
    const composition = createNodeMessageClientComposition({
      ...options(),
      tokenProvider,
    });

    await Promise.all([
      composition.privateConversationDirectoryQuery!.listPrivateConversations("open"),
      composition.privateConversationDirectoryQuery!.listPrivateConversations("closed"),
    ]);

    expect(tokenProvider).toHaveBeenCalledTimes(2);
    expect(seen.filter((value) => value === "Bearer token-1")).toHaveLength(2);
    expect(seen.filter((value) => value === "Bearer token-2")).toHaveLength(2);
  });

  it.each([
    undefined,
    { status: "accepted" },
    { status: "failed", code: "runtime_rejected" },
    { status: "unconfirmed", code: "delivery_unconfirmed" },
  ] as const)("decodes an optional exact runtime_dispatch receipt: %j", async (runtimeDispatch) => {
    vi.stubGlobal("fetch", vi.fn(async () => json({
      id: "m1",
      idempotent: false,
      ...(runtimeDispatch === undefined ? {} : { runtime_dispatch: runtimeDispatch }),
    }, 201)));
    const receipt = await createNodeMessageClientComposition(options()).messageCommand.sendMessage({
      conversationId: "c1",
      clientMessageId: "m1",
      idempotencyKey: "m1",
      type: "chat_message",
      content: {},
    });
    expect(receipt.runtimeDispatch).toEqual(runtimeDispatch);
  });

  it.each([
    { status: "accepted", code: "runtime_rejected" },
    { status: "failed", code: "delivery_unconfirmed" },
    { status: "failed", code: "runtime_rejected", provider: "forbidden" },
  ])("propagates invalid authoritative runtime_dispatch without outcome wrapping: %j", async (value) => {
    vi.stubGlobal("fetch", vi.fn(async () => json({
      id: "m1",
      idempotent: false,
      runtime_dispatch: value,
    }, 201)));
    const promise = createNodeMessageClientComposition(options()).messageStream.startStream({
      conversationId: "c1",
      clientMessageId: "m1",
      idempotencyKey: "m1",
      type: "chat_message",
      content: {},
    });
    await expect(promise).rejects.toBeInstanceOf(RuntimeDispatchContractError);
    await expect(promise).rejects.not.toBeInstanceOf(OutcomeUnknownError);
  });

  it("fails unsupported MS routes explicitly and distinguishes HTTP response from no response", async () => {
    const fetchMock = vi.fn(async () => new Response("unavailable", { status: 503 }));
    vi.stubGlobal("fetch", fetchMock);
    const composition = createNodeMessageClientComposition(options());
    await expect(composition.messageCommand.cancelMessage("c1", "m1", "cancel-key"))
      .rejects.toThrow("does not expose");
    expect(fetchMock).not.toHaveBeenCalled();

    const command = {
      conversationId: "c1",
      clientMessageId: "send-key",
      idempotencyKey: "send-key",
      type: "chat_message",
      content: {},
    } as const;
    await expect(composition.messageCommand.sendMessage(command))
      .rejects.not.toBeInstanceOf(OutcomeUnknownError);

    vi.stubGlobal("fetch", vi.fn(async () => {
      throw new TypeError("socket reset before response");
    }));
    await expect(composition.messageCommand.sendMessage(command))
      .rejects.toBeInstanceOf(OutcomeUnknownError);
  });
});
