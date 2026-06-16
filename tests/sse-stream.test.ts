import { describe, expect, it, vi } from "vitest";

import {
  MessageStreamReader,
  type BackfillTruncatedEvent,
  type ReplayCompleteEvent,
  type StreamResponse,
} from "../src/sse-stream.js";
import { MessageClient } from "../src/client.js";
import type { Message, MessageEnvelope } from "../src/index.js";

const ENC = new TextEncoder();

/** Build an SSE frame: optional event name + JSON data payload. */
function frame(data: unknown, event?: string): string {
  const lines: string[] = [];
  if (event) lines.push(`event: ${event}`);
  lines.push(`data: ${JSON.stringify(data)}`);
  return lines.join("\n") + "\n\n";
}

/** A byte source that emits the given string chunks verbatim. */
function bytes(...chunks: string[]): AsyncIterable<Uint8Array> {
  return {
    async *[Symbol.asyncIterator]() {
      for (const c of chunks) yield ENC.encode(c);
    },
  };
}

/** A reader fed a fixed set of SSE frames (one byte chunk). */
function readerFor(
  sse: string,
  rebase: (id: string) => Promise<MessageEnvelope | null> = async () => null,
): MessageStreamReader {
  return new MessageStreamReader({
    connect: async (): Promise<StreamResponse> => ({ status: 200, body: bytes(sse) }),
    rebase,
  });
}

async function collect(reader: MessageStreamReader): Promise<Message[]> {
  const out: Message[] = [];
  for await (const m of reader) out.push(m);
  return out;
}

describe("MessageStreamReader — HTTP SSE delta-wire (ADR-0025)", () => {
  const base = { message_id: "m1", channel_id: "conv-1", type: "agent_reply" };

  it("folds delta frames into cumulative body + per-frame bodyDelta", async () => {
    const sse =
      frame({ ...base, event: "message.created", state: "streaming", body: "", offset: 1 }, "message") +
      frame({ ...base, event: "message.delta", state: "streaming", body_from: 0, body_chunk: "Hello", offset: 1 }) +
      frame({ ...base, event: "message.delta", state: "streaming", body_from: 5, body_chunk: " world", offset: 1 }) +
      frame({ ...base, event: "message.updated", state: "completed", stop_reason: "end_turn", body: "Hello world", offset: 1 });

    const msgs = await collect(readerFor(sse));

    expect(msgs.map((m) => m.body)).toEqual(["", "Hello", "Hello world", "Hello world"]);
    expect(msgs.map((m) => m.bodyDelta)).toEqual(["", "Hello", " world", ""]);
    expect(msgs.at(-1)?.state).toBe("completed");
    expect(msgs.at(-1)?.stopReason).toBe("end_turn");
  });

  it("aligns body_from on multi-byte UTF-8 chunks", async () => {
    const b = { ...base, state: "streaming" };
    const sse =
      frame({ ...b, event: "message.created", body: "" }) +
      frame({ ...b, event: "message.delta", body_from: 0, body_chunk: "héllo" }) + // 6 bytes
      frame({ ...b, event: "message.delta", body_from: 6, body_chunk: " 世界" });

    const msgs = await collect(readerFor(sse));
    expect(msgs.at(-1)?.body).toBe("héllo 世界");
    expect(msgs.at(-1)?.bodyDelta).toBe(" 世界");
  });

  it("drops typed ping heartbeats (named and default-event) without surfacing them", async () => {
    // A gateway may inject pings during long silent windows. Neither a
    // named `event: ping` nor a default-event `data: {"type":"ping"}`
    // carries a message_id/body, so neither must surface as a Message;
    // real frames on either side still come through in order.
    const sse =
      frame({ ...base, event: "message.created", state: "completed", body: "first", offset: 1 }, "message") +
      frame({ type: "ping" }, "ping") +
      frame({ type: "ping" }) +
      frame({ ...base, message_id: "m2", event: "message.created", state: "completed", body: "second", offset: 2 }, "message");

    const msgs = await collect(readerFor(sse));
    expect(msgs.map((m) => m.id)).toEqual(["m1", "m2"]);
    expect(msgs.map((m) => m.body)).toEqual(["first", "second"]);
  });

  it("rebases via snapshot fetch when a delta gaps (no local base)", async () => {
    const rebase = vi.fn(async (): Promise<MessageEnvelope> => ({
      id: "m1",
      conversationId: "conv-1",
      type: "agent_reply",
      sender: "agent",
      body: "Hello world",
      state: "streaming",
      createdAt: "2026-01-01T00:00:00Z",
    }));
    // First frame is a delta at offset 5 with no created/base → gap → rebase.
    const sse = frame({ ...base, event: "message.delta", state: "streaming", body_from: 5, body_chunk: "!" });

    const msgs = await collect(readerFor(sse, rebase));
    expect(rebase).toHaveBeenCalledOnce();
    expect(msgs.at(-1)?.body).toBe("Hello world");
    expect(msgs.at(-1)?.bodyDelta).toBe("");
  });

  it("degrades to last-known body when rebase fails (never blocks)", async () => {
    const sse =
      frame({ ...base, event: "message.created", state: "streaming", body: "Hi" }) +
      frame({ ...base, event: "message.delta", state: "streaming", body_from: 99, body_chunk: "x" });
    const msgs = await collect(readerFor(sse, async () => null));
    // Gap delta with failed rebase keeps the prior cumulative body.
    expect(msgs.at(-1)?.body).toBe("Hi");
  });

  it("surfaces backfill_truncated and replay_complete control frames", async () => {
    const sse =
      frame({ oldest_redis_offset: 50, since: 10, hint: "resubscribe" }, "backfill_truncated") +
      frame({ ...base, event: "message.created", state: "completed", body: "done", offset: 7 }, "message") +
      frame({ latest_offset: 7 }, "replay_complete");

    const reader = readerFor(sse);
    const backfills: BackfillTruncatedEvent[] = [];
    const replays: ReplayCompleteEvent[] = [];
    reader.on("backfill_truncated", (e) => backfills.push(e));
    reader.on("replay_complete", (e) => replays.push(e));

    const msgs = await collect(reader);

    expect(backfills).toEqual([{ oldest_redis_offset: 50, since: 10, hint: "resubscribe" }]);
    expect(replays).toEqual([{ latest_offset: 7 }]);
    expect(msgs).toHaveLength(1);
    expect(msgs[0]!.body).toBe("done");
  });

  it("tracks lastCursor from frame offsets", async () => {
    const sse =
      frame({ ...base, event: "message.created", state: "streaming", body: "", offset: 3 }) +
      frame({ ...base, event: "message.updated", state: "completed", body: "ok", offset: 9 });
    const reader = readerFor(sse);
    await collect(reader);
    expect(reader.lastCursor()).toBe(9);
  });

  it("parses frames split across byte-chunk boundaries", async () => {
    const full =
      frame({ ...base, event: "message.created", state: "streaming", body: "" }) +
      frame({ ...base, event: "message.updated", state: "completed", body: "chunked" });
    // Split mid-line at an arbitrary byte boundary.
    const mid = Math.floor(full.length / 2);
    const reader = new MessageStreamReader({
      connect: async () => ({ status: 200, body: bytes(full.slice(0, mid), full.slice(mid)) }),
      rebase: async () => null,
    });
    const msgs = await collect(reader);
    expect(msgs.at(-1)?.body).toBe("chunked");
  });

  it("ends the iterator on clean server close (EOF)", async () => {
    const sse = frame({ ...base, event: "message.created", state: "completed", body: "x" });
    const msgs = await collect(readerFor(sse));
    expect(msgs).toHaveLength(1);
  });

  it("propagates a dial error to the consumer", async () => {
    const reader = new MessageStreamReader({
      connect: async () => {
        throw new Error("boom: 403 not_member");
      },
      rebase: async () => null,
    });
    await expect(collect(reader)).rejects.toThrow(/boom: 403/);
  });
});

describe("ConversationsAPI.stream — wiring", () => {
  it("passes the since cursor + delta-wire Accept header through the dialer", async () => {
    let capturedUrl = "";
    let capturedHeaders: Record<string, string> = {};
    const dialer = async (
      url: string,
      headers: Record<string, string>,
    ): Promise<StreamResponse> => {
      capturedUrl = url;
      capturedHeaders = headers;
      return {
        status: 200,
        body: bytes(
          frame({ message_id: "m1", channel_id: "conv-9", type: "agent_reply", event: "message.created", state: "completed", body: "hi", offset: 13 }),
        ),
      };
    };

    const client = new MessageClient({ serviceUrl: "http://ms.local", apiKey: "k1" });
    const reader = client.conversations.stream("conv-9", { since: 12, dialer });
    const msgs = await collect(reader);

    expect(capturedUrl).toBe("http://ms.local/api/v2/conversations/conv-9/stream?since=12");
    expect(capturedHeaders["Accept"]).toContain("application/vnd.beeos.message-delta");
    expect(capturedHeaders["X-API-Key"]).toBe("k1");
    expect(msgs.at(-1)?.body).toBe("hi");
    expect(reader.lastCursor()).toBe(13);
  });
});
