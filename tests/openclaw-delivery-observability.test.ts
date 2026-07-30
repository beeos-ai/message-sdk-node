import { afterEach, describe, expect, it, vi } from "vitest";

import { createNodeMessageClientComposition } from "../src/node-runtime.js";
import { formatOpenClawDeliveryBoundary, hashIdentifier } from "../src/openclaw-delivery-observability.js";

const base = "https://message.example";

function options(logger?: { info?: (m: string) => void; warn?: (m: string) => void }) {
  return {
    identityId: "agent 1",
    serviceUrl: base,
    logger,
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

describe("hashIdentifier / formatOpenClawDeliveryBoundary", () => {
  it("is deterministic, 64 hex chars, namespace-scoped, and never leaks the raw value", () => {
    const a = hashIdentifier("secret-conversation-id", "ns-a");
    const b = hashIdentifier("secret-conversation-id", "ns-a");
    const c = hashIdentifier("secret-conversation-id", "ns-b");
    expect(a).toBe(b);
    expect(a).not.toBe(c);
    expect(a).toMatch(/^[0-9a-f]{64}$/);
    expect(a).not.toContain("secret-conversation-id");
  });

  it("formats only hashes and fixed enums", () => {
    const line = formatOpenClawDeliveryBoundary({
      stage: "sdk_stream_open",
      status: "accepted",
      code: "created",
      conversationId: "raw-conversation-id",
      messageId: "raw-message-id",
    });
    expect(line).toContain("stage=sdk_stream_open");
    expect(line).toContain("status=accepted");
    expect(line).toContain("code=created");
    expect(line).toContain("conversation_id_hash=");
    expect(line).toContain("message_id_hash=");
    expect(line).not.toContain("raw-conversation-id");
    expect(line).not.toContain("raw-message-id");
  });
});

describe("NodeMessageHttpAdapter openclaw_delivery_boundary logging", () => {
  it("logs stream-open, delta-append, and terminal-finalize as hash-only, never leaking raw ids or body", async () => {
    vi.stubGlobal("fetch", vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      return json({
        id: "open-key-secret",
        idempotent: false,
        conversation_id: "c1-secret",
        history_generation: 1,
        offset: 1,
        updated_at: "2026-07-28T00:00:00.000Z",
      }, init?.method === "POST" ? 201 : 200);
    }));

    const info: string[] = [];
    const warn: string[] = [];
    const composition = createNodeMessageClientComposition(options({
      info: (m) => info.push(m),
      warn: (m) => warn.push(m),
    }));

    const receipt = await composition.messageStream.startStream({
      conversationId: "c1-secret",
      clientMessageId: "open-key-secret",
      idempotencyKey: "open-key-secret",
      type: "agent_reply",
      content: { message: "the secret reply body" },
    });
    await composition.messageStream.append("c1-secret", receipt.messageId, "secret-chunk", 0, "append-key");
    await composition.messageStream.finalize("c1-secret", receipt.messageId, "completed", "terminal-key", "end_turn");

    const boundaryLines = info.filter((line) => line.includes("openclaw_delivery_boundary"));
    expect(boundaryLines.some((l) => l.includes("stage=sdk_stream_open") && l.includes("status=accepted"))).toBe(true);
    expect(boundaryLines.some((l) => l.includes("stage=sdk_delta_append") && l.includes("status=accepted"))).toBe(true);
    expect(boundaryLines.some((l) => l.includes("stage=sdk_terminal_finalize") && l.includes("status=accepted") && l.includes("reason=completed"))).toBe(true);
    expect(warn).toHaveLength(0);

    const allLogs = [...info, ...warn].join("\n");
    for (const raw of ["c1-secret", "open-key-secret", "secret-chunk", "the secret reply body"]) {
      expect(allLogs).not.toContain(raw);
    }
  });

  it("logs a failed finalize as hash-only warn and still rethrows the original error", async () => {
    vi.stubGlobal("fetch", vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === "PATCH") return json({ error: "boom" }, 500);
      return json({
        id: "open-key-secret",
        idempotent: false,
        conversation_id: "c1-secret",
        history_generation: 1,
        offset: 1,
        updated_at: "2026-07-28T00:00:00.000Z",
      }, 201);
    }));

    const info: string[] = [];
    const warn: string[] = [];
    const composition = createNodeMessageClientComposition(options({
      info: (m) => info.push(m),
      warn: (m) => warn.push(m),
    }));

    const receipt = await composition.messageStream.startStream({
      conversationId: "c1-secret",
      clientMessageId: "open-key-secret",
      idempotencyKey: "open-key-secret",
      type: "agent_reply",
      content: {},
    });

    await expect(
      composition.messageStream.finalize("c1-secret", receipt.messageId, "completed", "terminal-key", "end_turn"),
    ).rejects.toThrow();

    const boundaryLines = warn.filter((line) => line.includes("openclaw_delivery_boundary"));
    expect(boundaryLines.some((l) =>
      l.includes("stage=sdk_terminal_finalize") && l.includes("status=failed") && l.includes("code=http_500"),
    )).toBe(true);

    const allLogs = [...info, ...warn].join("\n");
    expect(allLogs).not.toContain("c1-secret");
    expect(allLogs).not.toContain("open-key-secret");
  });
});
