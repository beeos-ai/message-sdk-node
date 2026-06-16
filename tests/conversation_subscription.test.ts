import { describe, expect, it } from "vitest";

import { ConversationSubscription, type Message, type MessageEnvelope } from "../src/index.js";

// Minimal fake Centrifuge subscription that lets the test drive the
// `subscribed` / `publication` callbacks ConversationSubscription wires.
class FakeSub {
  handlers: Record<string, (arg?: unknown) => void> = {};
  subscribed = false;
  on(event: string, h: (arg?: unknown) => void): this {
    this.handlers[event] = h;
    return this;
  }
  subscribe(): void {
    this.subscribed = true;
  }
  unsubscribe(): void {
    this.subscribed = false;
  }
  emitPublication(data: unknown): void {
    this.handlers["publication"]?.({ data } as unknown);
  }
  emitSubscribed(ctx: { recovered?: boolean }): void {
    this.handlers["subscribed"]?.(ctx as unknown);
  }
}

class FakeCentrifuge {
  sub = new FakeSub();
  newSubscription(): FakeSub {
    return this.sub;
  }
}

function makeSub(
  fetchSnapshot?: (id: string) => Promise<MessageEnvelope | null>,
): { sub: ConversationSubscription; cent: FakeCentrifuge; msgs: Message[] } {
  const cent = new FakeCentrifuge();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const sub = new ConversationSubscription(cent as any, "conv-1", fetchSnapshot);
  const msgs: Message[] = [];
  sub.on("message", (m) => msgs.push(m));
  return { sub, cent, msgs };
}

describe("ConversationSubscription — delta-wire folding (ADR-0025)", () => {
  it("folds message.delta frames into cumulative body + per-frame bodyDelta", () => {
    const { cent, msgs } = makeSub();
    const base = { message_id: "m1", channel_id: "conv-1", type: "agent_reply" };

    cent.sub.emitPublication({ ...base, event: "message.created", state: "streaming", body: "" });
    cent.sub.emitPublication({ ...base, event: "message.delta", state: "streaming", body_from: 0, body_chunk: "Hello" });
    cent.sub.emitPublication({ ...base, event: "message.delta", state: "streaming", body_from: 5, body_chunk: " world" });
    cent.sub.emitPublication({ ...base, event: "message.updated", state: "completed", stop_reason: "end_turn", body: "Hello world" });

    expect(msgs.map((m) => m.body)).toEqual(["", "Hello", "Hello world", "Hello world"]);
    expect(msgs.map((m) => m.bodyDelta)).toEqual(["", "Hello", " world", ""]);
    expect(msgs.at(-1)?.state).toBe("completed");
    expect(msgs.at(-1)?.stopReason).toBe("end_turn");
  });

  it("aligns body_from on multi-byte UTF-8 chunks", () => {
    const { cent, msgs } = makeSub();
    const base = { message_id: "m1", channel_id: "conv-1", type: "agent_reply", state: "streaming" };
    cent.sub.emitPublication({ ...base, event: "message.created", body: "" });
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 0, body_chunk: "héllo" }); // 6 bytes
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 6, body_chunk: " 世界" });

    expect(msgs.at(-1)?.body).toBe("héllo 世界");
    expect(msgs.at(-1)?.bodyDelta).toBe(" 世界");
  });

  it("rebases via snapshot fetch when a delta gaps", async () => {
    let fetched = 0;
    const fetcher = async (id: string): Promise<MessageEnvelope> => {
      fetched++;
      return {
        id,
        conversationId: "conv-1",
        type: "agent_reply",
        sender: "agent",
        body: "full server body",
        state: "streaming",
        createdAt: "2026-05-20T00:00:00Z",
      };
    };
    const { cent, msgs } = makeSub(fetcher);
    const base = { message_id: "m1", channel_id: "conv-1", type: "agent_reply", state: "streaming" };

    // First delta lands at offset 50 with no base → gap → rebase.
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 50, body_chunk: "late" });
    // Allow the async refetch to resolve.
    await new Promise((r) => setTimeout(r, 5));

    expect(fetched).toBe(1);
    expect(msgs.at(-1)?.body).toBe("full server body");
  });

  it("emits empty bodyDelta on a duplicate (idempotent) delta", () => {
    const { cent, msgs } = makeSub();
    const base = { message_id: "m1", channel_id: "conv-1", type: "agent_reply", state: "streaming" };

    cent.sub.emitPublication({ ...base, event: "message.created", body: "Hello" });
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 5, body_chunk: " world" });
    expect(msgs.at(-1)?.body).toBe("Hello world");
    expect(msgs.at(-1)?.bodyDelta).toBe(" world");

    // Replay the same delta: fully contained → body unchanged, no delta.
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 5, body_chunk: " world" });
    expect(msgs.at(-1)?.body).toBe("Hello world");
    expect(msgs.at(-1)?.bodyDelta).toBe("");
  });

  it("emits empty bodyDelta on a non-prefix snapshot replace", () => {
    const { cent, msgs } = makeSub();
    const base = { message_id: "m1", channel_id: "conv-1", type: "agent_reply", state: "streaming" };

    cent.sub.emitPublication({ ...base, event: "message.created", body: "Hello" });
    // Longer but divergent (non-prefix) snapshot → no clean increment.
    cent.sub.emitPublication({ ...base, event: "message.updated", body: "Goodbye world" });
    expect(msgs.at(-1)?.body).toBe("Goodbye world");
    expect(msgs.at(-1)?.bodyDelta).toBe("");
  });

  it("fires at most one snapshot fetch while a rebase is in flight", async () => {
    let resolveFetch: (env: MessageEnvelope) => void = () => {};
    let fetched = 0;
    const fetcher = (id: string): Promise<MessageEnvelope> => {
      fetched++;
      return new Promise<MessageEnvelope>((res) => {
        resolveFetch = res;
      }).then(() => ({
        id,
        conversationId: "conv-1",
        type: "agent_reply",
        sender: "agent",
        body: "rebased body",
        state: "streaming",
        createdAt: "2026-05-20T00:00:00Z",
      }));
    };
    const { cent, msgs } = makeSub(fetcher);
    const base = { message_id: "m1", channel_id: "conv-1", type: "agent_reply", state: "streaming" };

    // Three gapping deltas arrive before the first fetch resolves.
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 50, body_chunk: "a" });
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 51, body_chunk: "b" });
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 52, body_chunk: "c" });

    // Only one GET should have fired despite three gapping deltas.
    expect(fetched).toBe(1);

    resolveFetch({} as MessageEnvelope);
    await new Promise((r) => setTimeout(r, 5));
    expect(msgs.at(-1)?.body).toBe("rebased body");

    // After the in-flight fetch settles, a new gap can fetch again.
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 99, body_chunk: "z" });
    expect(fetched).toBe(2);
  });

  it("clears folded state on an unrecovered reconnect", () => {
    const { cent, msgs } = makeSub();
    const base = { message_id: "m1", channel_id: "conv-1", type: "agent_reply", state: "streaming" };

    cent.sub.emitPublication({ ...base, event: "message.created", body: "" });
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 0, body_chunk: "abc" });
    expect(msgs.at(-1)?.body).toBe("abc");

    // Reconnect without recovery → state dropped.
    cent.sub.emitSubscribed({ recovered: false });

    // A fresh snapshot re-establishes the base; a delta at 0 appends.
    cent.sub.emitPublication({ ...base, event: "message.created", body: "" });
    cent.sub.emitPublication({ ...base, event: "message.delta", body_from: 0, body_chunk: "xyz" });
    expect(msgs.at(-1)?.body).toBe("xyz");
    expect(msgs.at(-1)?.bodyDelta).toBe("xyz");
  });
});
