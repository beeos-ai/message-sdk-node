import { afterEach, describe, expect, it, vi } from "vitest";

import { MessageClient, StreamTerminatedError } from "../src/index.js";

const ORIGINAL_FETCH = globalThis.fetch;

interface RecordedRequest {
  method: string;
  url: string;
  headers: Record<string, string>;
  body: unknown;
}

function captureFetch(
  handler: (req: RecordedRequest) => { status: number; body: unknown },
): { fetch: typeof fetch; calls: RecordedRequest[] } {
  const calls: RecordedRequest[] = [];
  const f = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    const headers: Record<string, string> = {};
    new Headers(init?.headers).forEach((v, k) => {
      headers[k] = v;
    });
    let body: unknown;
    if (typeof init?.body === "string") {
      try {
        body = JSON.parse(init.body);
      } catch {
        body = init.body;
      }
    }
    const req: RecordedRequest = {
      method: init?.method ?? "GET",
      url,
      headers,
      body,
    };
    calls.push(req);
    const resp = handler(req);
    return new Response(JSON.stringify(resp.body), {
      status: resp.status,
      headers: { "Content-Type": "application/json" },
    });
  }) as unknown as typeof fetch;
  return { fetch: f, calls };
}

afterEach(() => {
  globalThis.fetch = ORIGINAL_FETCH;
  vi.restoreAllMocks();
});

function makeClient(): MessageClient {
  return new MessageClient({
    serviceUrl: "https://msg.test",
    apiKey: "test-key",
  });
}

describe("MessageStream — v3 envelope flow", () => {
  it("issues a single POST then ships appends and a terminal snapshot PATCH", async () => {
    const initial = {
      id: "msg-1",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:alice",
      reply_to: "msg-0",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") {
        return { status: 201, body: initial };
      }
      return {
        status: 200,
        body: { ...initial, body: "hello world", state: "completed" },
      };
    });
    globalThis.fetch = f;

    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "msg-1",
      replyTo: "msg-0",
    });
    expect(stream.id).toBe("msg-1");
    // Plan sink-streaming-to-sdk: startStream returns synchronously
    // and the open POST resolves in the background. envelope is
    // populated once `opened()` settles.
    await stream.opened();
    expect(stream.envelope?.state).toBe("streaming");

    stream.appendBody("hello ");
    stream.appendBody("world");

    const final = await stream.finalize({ stopReason: "end_turn" });

    // Append-only wire: each appendBody ships a body_append PATCH
    // immediately (coalesced only while a PATCH is in flight); finalize()
    // ships the terminal snapshot PATCH. So: 1 POST + >=1 PATCH.
    const methods = calls.map((c) => c.method);
    expect(methods.filter((m) => m === "POST")).toHaveLength(1);
    expect(methods.filter((m) => m === "PATCH").length).toBeGreaterThanOrEqual(1);

    // The last call must be a terminal PATCH carrying the snapshot.
    const last = calls.at(-1);
    expect(last?.method).toBe("PATCH");
    const patchBody = last?.body as { body?: string; state?: string; stop_reason?: string };
    expect(patchBody.body).toBe("hello world");
    expect(patchBody.state).toBe("completed");
    expect(patchBody.stop_reason).toBe("end_turn");

    expect(final.state).toBe("completed");
    expect(stream.isTerminated).toBe(true);
  });

  it("forwards Idempotency-Key when id is supplied", async () => {
    const { fetch: f, calls } = captureFetch(() => ({
      status: 201,
      body: {
        id: "msg-9",
        conversation_id: "conv-1",
        type: "agent_reply",
        sender: "agent:bob",
        body: "",
        state: "streaming",
        created_at: "2026-05-20T00:00:00Z",
      },
    }));
    globalThis.fetch = f;
    const client = makeClient();
    await client.messages.startStream({ conversationId: "conv-1", id: "msg-9" });
    expect(calls[0].headers["idempotency-key"]).toBe("msg-9");
  });

  it("throws StreamTerminatedError when mutating after finalize", async () => {
    const initial = {
      id: "msg-2",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    const { fetch: f } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      return { status: 200, body: { ...initial, state: "completed" } };
    });
    globalThis.fetch = f;
    const client = makeClient();
    const stream = await client.messages.startStream({
      conversationId: "conv-1",
      id: "msg-2",
    });
    await stream.finalize({ stopReason: "end_turn" });
    expect(() => stream.appendBody("late")).toThrow(StreamTerminatedError);
  });

  it("fail() sets state=failed and stop_reason=error by default", async () => {
    const initial = {
      id: "msg-3",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      return { status: 200, body: { ...initial, state: "failed", stop_reason: "error" } };
    });
    globalThis.fetch = f;
    const client = makeClient();
    const stream = await client.messages.startStream({
      conversationId: "conv-1",
      id: "msg-3",
    });
    await stream.fail({ body: "rate limit hit" });
    const last = calls.at(-1);
    const body = last?.body as { state?: string; stop_reason?: string; body?: string };
    expect(body.state).toBe("failed");
    expect(body.stop_reason).toBe("error");
    expect(body.body).toBe("rate limit hit");
  });

  it("cancel() sets state=cancelled and stop_reason=user_stop by default", async () => {
    const initial = {
      id: "msg-4",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      return { status: 200, body: { ...initial, state: "cancelled", stop_reason: "user_stop" } };
    });
    globalThis.fetch = f;
    const client = makeClient();
    const stream = await client.messages.startStream({
      conversationId: "conv-1",
      id: "msg-4",
    });
    await stream.cancel();
    const last = calls.at(-1);
    const body = last?.body as { state?: string; stop_reason?: string };
    expect(body.state).toBe("cancelled");
    expect(body.stop_reason).toBe("user_stop");
  });

  it("coalesces addPart calls made in one tick into a single parts PATCH", async () => {
    const initial = {
      id: "msg-5",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      return { status: 200, body: { ...initial, state: "completed" } };
    });
    globalThis.fetch = f;
    const client = makeClient();
    const stream = await client.messages.startStream({ conversationId: "conv-1", id: "msg-5" });
    // Four synchronous addPart calls accumulate into the parts array
    // before the first queued drain runs, so they fold into one PATCH
    // carrying the full array (parts always ship as a full snapshot).
    stream.appendThinking("a", "done");
    stream.appendThinking("b", "done");
    stream.appendThinking("c", "done");
    stream.appendThinking("d", "done");
    await new Promise((r) => setTimeout(r, 20));

    const patches = calls.filter((c) => c.method === "PATCH");
    expect(patches.length).toBeGreaterThanOrEqual(1);
    const body = patches[0].body as { parts?: unknown[] };
    expect(body.parts).toHaveLength(4);

    await stream.finalize();
  });

  it("setBody overwrites the cumulative snapshot and ships as a snapshot PATCH", async () => {
    const initial = {
      id: "msg-set",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      return { status: 200, body: { ...initial, state: "completed" } };
    });
    globalThis.fetch = f;
    const client = makeClient();
    const stream = await client.messages.startStream({
      conversationId: "conv-1",
      id: "msg-set",
    });

    stream.appendBody("partial");
    stream.setBody("Hello world");

    await stream.finalize({ stopReason: "end_turn" });

    const last = calls.at(-1);
    const patchBody = last?.body as { body?: string; state?: string };
    expect(patchBody.body).toBe("Hello world");
    expect(patchBody.state).toBe("completed");
  });

  it("setBody is a no-op when text equals current body", async () => {
    const initial = {
      id: "msg-setnoop",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      return { status: 200, body: { ...initial, state: "completed" } };
    });
    globalThis.fetch = f;
    const client = makeClient();
    const stream = await client.messages.startStream({
      conversationId: "conv-1",
      id: "msg-setnoop",
    });
    stream.appendBody("Hello world");
    // Same value → must NOT mark dirty / arm a spurious PATCH, but the
    // pending append is still in flight so a single PATCH fires.
    stream.setBody("Hello world");
    await stream.finalize({ stopReason: "end_turn" });
    const patches = calls.filter((c) => c.method === "PATCH");
    // 1 terminal PATCH minimum; may also see 1 mid-stream append PATCH.
    // Critical: setBody itself did not duplicate the snapshot.
    expect(patches.length).toBeGreaterThanOrEqual(1);
    expect(patches.length).toBeLessThanOrEqual(2);
    const last = calls.at(-1);
    const patchBody = last?.body as { body?: string };
    expect(patchBody.body).toBe("Hello world");
  });

  // PR2: mid-stream PATCH failures must NOT propagate to the caller
  // (snapshot semantics self-heal on the next write), but MUST fire
  // the onError("midstream") observer hook so loggers / metrics can
  // see the transient failure.
  it("PR2: onError('midstream') fires on a mid-stream PATCH failure; stream self-heals on the next write", async () => {
    const initial = {
      id: "msg-pr2-mid",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    let patchCount = 0;
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      patchCount++;
      // First mid-stream PATCH fails with a 503; subsequent PATCHes
      // succeed.
      if (patchCount === 1) {
        return {
          status: 503,
          body: { error: { code: "service_unavailable", message: "boom" } },
        };
      }
      return { status: 200, body: { ...initial, body: "ok", state: "completed" } };
    });
    globalThis.fetch = f;

    const errors: Array<{ phase: string; status?: number }> = [];
    const client = makeClient();
    const stream = await client.messages.startStream(
      { conversationId: "conv-1", id: "msg-pr2-mid" },
      {
        onError: (err, phase) => {
          const status = (err as { status?: number }).status;
          errors.push({ phase, status });
        },
      },
    );

    // appendBody ships a body_append PATCH immediately (which 503s).
    stream.appendBody("oops");
    // Give the PATCH + its chain failure a chance to settle.
    await new Promise((r) => setTimeout(r, 60));

    // Stream MUST still be active — snapshot semantics self-heal.
    expect(stream.isTerminated).toBe(false);

    // Add the rest of the body and finalize. The terminal PATCH must
    // succeed (no further failures injected) and carry the cumulative
    // snapshot.
    stream.appendBody(" world");
    const final = await stream.finalize({ stopReason: "end_turn" });
    expect(final.state).toBe("completed");

    // Observer saw exactly one midstream error.
    const midErrors = errors.filter((e) => e.phase === "midstream");
    expect(midErrors).toHaveLength(1);
    expect(midErrors[0].status).toBe(503);
    // No terminal errors — the final PATCH succeeded.
    expect(errors.filter((e) => e.phase === "terminal")).toHaveLength(0);

    // Sanity: at least 2 PATCHes (the failed one + the terminal one).
    const patches = calls.filter((c) => c.method === "PATCH");
    expect(patches.length).toBeGreaterThanOrEqual(2);
  });

  // PR2: terminal PATCH retries exactly once after a 200ms delay when
  // the first attempt fails with a retryable (non-4xx) error. The
  // caller's finalize() resolves on the retry's success.
  //
  // Contract: when retry RESCUES the stream, onError MUST NOT fire —
  // the stream ended cleanly from the caller's perspective and the
  // observer signal is reserved for genuine terminal failures. This
  // keeps logger.warn / metric.failure wirings on phase="terminal"
  // free of false positives from transient 5xx blips.
  it("PR2: terminal PATCH retries once on 5xx and resolves on retry success WITHOUT firing onError", async () => {
    const initial = {
      id: "msg-pr2-term-ok",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    let patchCount = 0;
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      patchCount++;
      // First terminal PATCH fails 5xx; the retry succeeds.
      if (patchCount === 1) {
        return { status: 503, body: { error: { code: "service_unavailable" } } };
      }
      return {
        status: 200,
        body: { ...initial, body: "done", state: "completed", stop_reason: "end_turn" },
      };
    });
    globalThis.fetch = f;

    const errors: Array<{ phase: string; status?: number }> = [];
    const client = makeClient();
    const stream = await client.messages.startStream(
      { conversationId: "conv-1", id: "msg-pr2-term-ok" },
      {
        onError: (err, phase) => {
          const status = (err as { status?: number }).status;
          errors.push({ phase, status });
        },
      },
    );

    const start = Date.now();
    const final = await stream.finalize({ stopReason: "end_turn" });
    const elapsed = Date.now() - start;

    expect(final.state).toBe("completed");
    // Retry rescued the stream → onError("terminal") MUST NOT fire.
    // Observers tied to logger.warn / failure metrics should not see
    // transient retry-recovered blips as incidents.
    expect(errors.filter((e) => e.phase === "terminal")).toHaveLength(0);
    // Sanity: terminal PATCH counted twice (initial + retry).
    expect(calls.filter((c) => c.method === "PATCH")).toHaveLength(2);
    // The 200ms retry delay must have actually elapsed.
    expect(elapsed).toBeGreaterThanOrEqual(150);
  });

  // PR2: 4xx errors from terminal PATCH (channel closed, ACL denied,
  // row already terminal, …) MUST NOT trigger a retry — they are
  // semantic failures and retrying changes nothing. The caller sees
  // the error immediately.
  it("PR2: terminal PATCH skips retry on 4xx and rethrows immediately", async () => {
    const initial = {
      id: "msg-pr2-term-4xx",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    let patchCount = 0;
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      patchCount++;
      return {
        status: 410,
        body: { error: { code: "conversation_closed", message: "closed" } },
      };
    });
    globalThis.fetch = f;

    const errors: Array<{ phase: string; status?: number }> = [];
    const client = makeClient();
    const stream = await client.messages.startStream(
      { conversationId: "conv-1", id: "msg-pr2-term-4xx" },
      {
        onError: (err, phase) => {
          const status = (err as { status?: number }).status;
          errors.push({ phase, status });
        },
      },
    );

    await expect(stream.finalize({ stopReason: "end_turn" })).rejects.toMatchObject({
      status: 410,
    });

    // Exactly one terminal PATCH attempt — no retry.
    expect(calls.filter((c) => c.method === "PATCH")).toHaveLength(1);
    expect(patchCount).toBe(1);
    // Exactly one onError call for the 4xx (no second one after a
    // retry, because there was no retry).
    expect(errors.filter((e) => e.phase === "terminal")).toHaveLength(1);
    expect(errors[0].status).toBe(410);
  });

  it("getEnvelope returns the camelCase v3 snapshot", async () => {
    const { fetch: f } = captureFetch(() => ({
      status: 200,
      body: {
        id: "msg-6",
        conversation_id: "conv-1",
        type: "agent_reply",
        sender: "agent:c",
        reply_to: "msg-0",
        body: "hello",
        state: "completed",
        stop_reason: "end_turn",
        created_at: "2026-05-20T00:00:00Z",
        updated_at: "2026-05-20T00:00:01Z",
      },
    }));
    globalThis.fetch = f;
    const client = makeClient();
    const env = await client.messages.getEnvelope("conv-1", "msg-6");
    expect(env.id).toBe("msg-6");
    expect(env.conversationId).toBe("conv-1");
    expect(env.state).toBe("completed");
    expect(env.stopReason).toBe("end_turn");
    expect(env.replyTo).toBe("msg-0");
    expect(env.updatedAt).toBe("2026-05-20T00:00:01Z");
  });
});

// ============================================================================
// Plan `sink-streaming-to-sdk` — sync startStream + open-failure fallback +
// stopReason whitelist. These tests cover the behaviors that USED to live in
// beeos-claw's SessionStream / maybeSendTerminalFallback / KNOWN_COMPLETED_-
// STOP_REASONS and now live inside MessageStream itself.
// ============================================================================

describe("MessageStream — sync construct + opening queue (sink plan)", () => {
  it("returns synchronously and buffers appends before the open POST resolves", async () => {
    const initial = {
      id: "msg-sync-1",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    let postResolved = false;
    let resolvePost: (env: typeof initial) => void = () => {};
    const postPromise = new Promise<typeof initial>((r) => {
      resolvePost = r;
    });
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") {
        // Mark the POST as in-flight; the test below resolves it on demand.
        // We can't stash the promise directly in fetch's sync return, so
        // we use a sentinel via the outer captureFetch's mock.
        return { status: 201, body: initial };
      }
      return { status: 200, body: { ...initial, state: "completed" } };
    });
    // Wrap fetch to delay the POST response until we explicitly resolve.
    const wrapped = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      if (method === "POST" && !postResolved) {
        postResolved = true;
        const env = await postPromise;
        return new Response(JSON.stringify(env), {
          status: 201,
          headers: { "Content-Type": "application/json" },
        });
      }
      return (f as unknown as typeof fetch)(input, init);
    }) as unknown as typeof fetch;
    globalThis.fetch = wrapped;

    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "msg-sync-1",
    });

    // SYNC return: id is set immediately, envelope NOT yet populated.
    expect(stream.id).toBe("msg-sync-1");
    expect(stream.envelope).toBeUndefined();

    // Appends BEFORE POST resolves — must be buffered, NOT throw.
    stream.appendBody("hello ");
    stream.appendBody("world");

    // Confirm no PATCH was sent yet — the open POST hasn't resolved.
    expect(calls.filter((c) => c.method === "PATCH")).toHaveLength(0);

    // Release the open POST. Wait for opened() to confirm.
    resolvePost(initial);
    await stream.opened();
    expect(stream.envelope?.state).toBe("streaming");

    // Finalize — terminal PATCH must carry the buffered cumulative body.
    const final = await stream.finalize({ stopReason: "end_turn" });
    expect(final.state).toBe("completed");
    const lastPatch = calls.filter((c) => c.method === "PATCH").at(-1);
    expect((lastPatch?.body as { body?: string }).body).toBe("hello world");
  });
});

describe("MessageStream — open-failure fallback to sendV3 (sink plan)", () => {
  function makeOpenFailFetch(
    onFallbackPost?: (req: RecordedRequest) => { status: number; body: unknown },
  ): { fetch: typeof fetch; calls: RecordedRequest[] } {
    let postCount = 0;
    return captureFetch((req) => {
      if (req.method === "POST") {
        postCount++;
        if (postCount === 1) {
          // First POST = open. Fail it.
          return {
            status: 503,
            body: { error: { code: "service_unavailable", message: "boom" } },
          };
        }
        // Second POST = sendV3 fallback. Caller can customize.
        return (
          onFallbackPost?.(req) ?? {
            status: 201,
            body: {
              id: "fallback-id",
              conversation_id: "conv-1",
              type: "agent_reply",
              sender: "agent:c",
              body: (req.body as { body?: string }).body ?? "",
              state: (req.body as { state?: string }).state ?? "completed",
              stop_reason: (req.body as { stop_reason?: string }).stop_reason,
              created_at: "2026-05-20T00:00:00Z",
            },
          }
        );
      }
      // No PATCHes expected on the open-failure path.
      throw new Error(`unexpected non-POST request after open failure: ${req.method}`);
    });
  }

  it("finalize falls back to sendV3 with state=completed when open POST fails", async () => {
    const { fetch: f, calls } = makeOpenFailFetch();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "open-fail-1",
      replyTo: "user-msg-0",
    });
    stream.appendBody("partial reply");

    const final = await stream.finalize({ stopReason: "end_turn" });

    // Exactly TWO POSTs: failed open + fallback sendV3. NO PATCHes.
    expect(calls.filter((c) => c.method === "POST")).toHaveLength(2);
    expect(calls.filter((c) => c.method === "PATCH")).toHaveLength(0);

    // The fallback POST is the second one — carries terminal state.
    const fallback = calls.filter((c) => c.method === "POST")[1];
    const fbBody = fallback.body as { body?: string; state?: string; stop_reason?: string; reply_to?: string };
    expect(fbBody.state).toBe("completed");
    expect(fbBody.stop_reason).toBe("end_turn");
    expect(fbBody.body).toBe("partial reply");
    expect(fbBody.reply_to).toBe("user-msg-0");

    // Stream id is updated to the fallback's id so downstream getEnvelope works.
    expect(stream.id).toBe("fallback-id");
    expect(final.id).toBe("fallback-id");
  });

  it("fail falls back to sendV3 with state=failed", async () => {
    const { fetch: f, calls } = makeOpenFailFetch();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "open-fail-2",
    });
    await stream.fail({ body: "rate limited" });

    const fallback = calls.filter((c) => c.method === "POST")[1];
    const fbBody = fallback.body as { body?: string; state?: string; stop_reason?: string };
    expect(fbBody.state).toBe("failed");
    expect(fbBody.stop_reason).toBe("error");
    expect(fbBody.body).toBe("rate limited");
  });

  it("refuse falls back to sendV3 with state=refused", async () => {
    const { fetch: f, calls } = makeOpenFailFetch();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "open-fail-3",
    });
    await stream.refuse({ body: "I can't help with that." });

    const fallback = calls.filter((c) => c.method === "POST")[1];
    const fbBody = fallback.body as { body?: string; state?: string; stop_reason?: string };
    expect(fbBody.state).toBe("refused");
    expect(fbBody.stop_reason).toBe("refused");
    expect(fbBody.body).toBe("I can't help with that.");
  });

  it("cancel falls back to sendV3 with state=cancelled", async () => {
    const { fetch: f, calls } = makeOpenFailFetch();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "open-fail-4",
    });
    await stream.cancel();

    const fallback = calls.filter((c) => c.method === "POST")[1];
    const fbBody = fallback.body as { state?: string; stop_reason?: string };
    expect(fbBody.state).toBe("cancelled");
    expect(fbBody.stop_reason).toBe("user_stop");
  });

  it("fallback POST failure fires onError('terminal') but does NOT re-throw", async () => {
    const { fetch: f } = makeOpenFailFetch(() => ({
      status: 500,
      body: { error: { code: "internal", message: "still down" } },
    }));
    globalThis.fetch = f;
    const errors: Array<{ phase: string; status?: number }> = [];
    const client = makeClient();
    const stream = client.messages.startStream(
      { conversationId: "conv-1", id: "open-fail-5" },
      {
        onError: (err, phase) => {
          const status = (err as { status?: number }).status;
          errors.push({ phase, status });
        },
      },
    );
    // Beeos-claw parity: terminal fallback failure is warn-only, so
    // finalize MUST NOT throw.
    await expect(stream.finalize({ stopReason: "end_turn" })).resolves.not.toThrow();
    // Observer saw exactly one terminal error.
    expect(errors.filter((e) => e.phase === "terminal")).toHaveLength(1);
    expect(errors[0].status).toBe(500);
  });

  it("opened() rejects when open POST fails (caller-observable error)", async () => {
    const { fetch: f } = makeOpenFailFetch();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "open-fail-6",
    });
    await expect(stream.opened()).rejects.toMatchObject({ status: 503 });
    // Stream still works — finalize falls back to sendV3.
    await stream.finalize({ stopReason: "end_turn" });
  });
});

// ============================================================================
// ADR-0025 — append-only delta wire. appendBody must ship a
// {body_append, body_from} PATCH immediately (no 256/50 buffer), with
// body_from being the UTF-8 byte offset the chunk attaches at. Bursts
// while a PATCH is in flight coalesce into one append; setBody / finalize
// still ship the full body; a 409 mismatch self-heals via a full-body
// snapshot replace.
// ============================================================================

describe("MessageStream — append-only delta wire (ADR-0025)", () => {
  function setupAppendCapture(): {
    fetch: typeof fetch;
    calls: RecordedRequest[];
    serverBody: () => string;
  } {
    const initial = {
      id: "d-1",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    let body = "";
    const cap = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      const b = req.body as {
        body?: string;
        body_append?: string;
        body_from?: number;
        state?: string;
      };
      if (typeof b.body_append === "string") {
        body += b.body_append;
      } else if (typeof b.body === "string") {
        body = b.body;
      }
      return {
        status: 200,
        body: { ...initial, body, state: b.state ?? "streaming" },
      };
    });
    return { fetch: cap.fetch, calls: cap.calls, serverBody: () => body };
  }

  it("appendBody ships body_append + body_from immediately (no full body)", async () => {
    const { fetch: f, calls, serverBody } = setupAppendCapture();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "d-1",
    });
    await stream.opened();

    // Space the appends with awaits so each drain fires its own PATCH.
    stream.appendBody("Hello");
    await new Promise((r) => setTimeout(r, 5));
    stream.appendBody(" world");
    await new Promise((r) => setTimeout(r, 5));

    const appendPatches = calls.filter(
      (c) => c.method === "PATCH" && (c.body as { body_append?: string }).body_append !== undefined,
    );
    expect(appendPatches.length).toBeGreaterThanOrEqual(2);
    const first = appendPatches[0].body as { body_append?: string; body_from?: number; body?: string };
    expect(first.body_append).toBe("Hello");
    expect(first.body_from).toBe(0);
    // Must NOT carry the full cumulative body on an append PATCH.
    expect(first.body).toBeUndefined();
    const second = appendPatches[1].body as { body_append?: string; body_from?: number };
    expect(second.body_append).toBe(" world");
    expect(second.body_from).toBe(5);

    await stream.finalize({ stopReason: "end_turn" });
    expect(serverBody()).toBe("Hello world");
  });

  it("body_from uses UTF-8 byte offsets for multi-byte chars", async () => {
    const { fetch: f, calls } = setupAppendCapture();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "d-1",
    });
    await stream.opened();

    stream.appendBody("世界"); // 6 UTF-8 bytes
    await new Promise((r) => setTimeout(r, 5));
    stream.appendBody("!");
    await new Promise((r) => setTimeout(r, 5));

    const appendPatches = calls.filter(
      (c) => c.method === "PATCH" && (c.body as { body_append?: string }).body_append !== undefined,
    );
    const second = appendPatches.at(-1)?.body as { body_from?: number };
    expect(second?.body_from).toBe(6);

    await stream.finalize();
  });

  it("coalesces appends queued while a PATCH is in flight into one body_append", async () => {
    // Hold the first PATCH open so the next appends accumulate, then
    // release — they must flush as ONE append covering all of them.
    const initial = {
      id: "d-coalesce",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    let releaseFirstPatch: () => void = () => {};
    const gate = new Promise<void>((r) => {
      releaseFirstPatch = r;
    });
    let patchCount = 0;
    const calls: RecordedRequest[] = [];
    const f = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      const url = typeof input === "string" ? input : input.toString();
      let body: unknown;
      if (typeof init?.body === "string") {
        try {
          body = JSON.parse(init.body);
        } catch {
          body = init.body;
        }
      }
      calls.push({ method, url, headers: {}, body });
      if (method === "POST") {
        return new Response(JSON.stringify(initial), {
          status: 201,
          headers: { "Content-Type": "application/json" },
        });
      }
      patchCount++;
      if (patchCount === 1) {
        await gate; // hold the first append PATCH open
      }
      return new Response(JSON.stringify({ ...initial, state: "streaming" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as unknown as typeof fetch;
    globalThis.fetch = f;

    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "d-coalesce",
    });
    await stream.opened();

    stream.appendBody("aaa"); // drain #1 starts, blocks on gate
    await new Promise((r) => setTimeout(r, 5));
    // These accumulate in pendingBody while drain #1 is blocked.
    stream.appendBody("bbb");
    stream.appendBody("ccc");
    await new Promise((r) => setTimeout(r, 5));
    releaseFirstPatch();
    await new Promise((r) => setTimeout(r, 10));

    const appendPatches = calls.filter(
      (c) => c.method === "PATCH" && (c.body as { body_append?: string }).body_append !== undefined,
    );
    // Exactly two appends: "aaa"@0, then coalesced "bbbccc"@3.
    expect(appendPatches).toHaveLength(2);
    expect((appendPatches[0].body as { body_append?: string }).body_append).toBe("aaa");
    expect((appendPatches[1].body as { body_append?: string; body_from?: number }).body_append).toBe("bbbccc");
    expect((appendPatches[1].body as { body_from?: number }).body_from).toBe(3);

    await stream.finalize();
  });

  it("setBody + concurrent appendBody anchors the next body_from at the snapshot length (no 409)", async () => {
    // Race guard (ADR-0025 P1b): an appendBody that lands while the
    // setBody snapshot PATCH is in flight must anchor at the SNAPSHOT's
    // byte length, not the post-append length. We model a stateful
    // server that 409s on any body_from mismatch, then assert it never
    // fires and the body assembles correctly.
    const enc = new TextEncoder();
    let serverBody = "";
    let saw409 = false;
    let releaseSnapshotPatch: () => void = () => {};
    const gate = new Promise<void>((r) => {
      releaseSnapshotPatch = r;
    });
    let patchCount = 0;
    const calls: RecordedRequest[] = [];
    const f = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      const url = typeof input === "string" ? input : input.toString();
      let body: unknown;
      if (typeof init?.body === "string") {
        try {
          body = JSON.parse(init.body);
        } catch {
          body = init.body;
        }
      }
      calls.push({ method, url, headers: {}, body });
      if (method === "POST") {
        return new Response(
          JSON.stringify({
            id: "d-race",
            conversation_id: "conv-1",
            type: "agent_reply",
            sender: "agent:r",
            body: "",
            state: "streaming",
            created_at: "2026-05-20T00:00:00Z",
          }),
          { status: 201, headers: { "Content-Type": "application/json" } },
        );
      }
      patchCount++;
      const b = body as { body?: string; body_append?: string; body_from?: number; state?: string };
      if (patchCount === 1) {
        await gate; // hold the setBody snapshot PATCH open
      }
      if (typeof b.body === "string") {
        serverBody = b.body; // full snapshot replace
      } else if (typeof b.body_append === "string") {
        const cur = enc.encode(serverBody).length;
        if (b.body_from !== cur) {
          saw409 = true;
          return new Response(
            JSON.stringify({ error: { code: "append_offset_mismatch", server_body_length: cur } }),
            { status: 409, headers: { "Content-Type": "application/json" } },
          );
        }
        serverBody += b.body_append;
      }
      return new Response(
        JSON.stringify({
          id: "d-race",
          conversation_id: "conv-1",
          type: "agent_reply",
          sender: "agent:r",
          body: serverBody,
          state: b.state ?? "streaming",
          created_at: "2026-05-20T00:00:00Z",
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as unknown as typeof fetch;
    globalThis.fetch = f;

    const client = makeClient();
    const stream = client.messages.startStream({ conversationId: "conv-1", id: "d-race" });
    await stream.opened();

    stream.setBody("Hello"); // drain #1 (snapshot) starts, blocks on gate
    await new Promise((r) => setTimeout(r, 5));
    // Lands DURING the snapshot PATCH await — must anchor at len("Hello")=5.
    stream.appendBody(" world");
    await new Promise((r) => setTimeout(r, 5));
    releaseSnapshotPatch();
    await new Promise((r) => setTimeout(r, 10));

    const final = await stream.finalize();

    expect(saw409).toBe(false);
    const appendPatches = calls.filter(
      (c) => c.method === "PATCH" && (c.body as { body_append?: string }).body_append !== undefined,
    );
    expect(appendPatches).toHaveLength(1);
    expect((appendPatches[0].body as { body_from?: number }).body_from).toBe(5);
    expect(serverBody).toBe("Hello world");
    expect(final.state).toBe("completed");
  });

  it("self-heals on a 409 append_offset_mismatch by resending the full body", async () => {
    const initial = {
      id: "d-409",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    let patchCount = 0;
    const { fetch: f, calls } = captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      patchCount++;
      // First append PATCH returns a 409 mismatch.
      if (patchCount === 1) {
        return {
          status: 409,
          body: {
            error: {
              code: "append_offset_mismatch",
              message: "mismatch",
              server_body_length: 0,
            },
          },
        };
      }
      const b = req.body as { body?: string; state?: string };
      return { status: 200, body: { ...initial, body: b.body ?? "", state: b.state ?? "streaming" } };
    });
    globalThis.fetch = f;

    const errors: string[] = [];
    const client = makeClient();
    const stream = client.messages.startStream(
      { conversationId: "conv-1", id: "d-409" },
      { onError: (_e, phase) => errors.push(phase) },
    );
    await stream.opened();

    stream.appendBody("Hello");
    await new Promise((r) => setTimeout(r, 10));

    // The 409 must have triggered a silent full-body resync PATCH.
    const resync = calls.filter(
      (c) => c.method === "PATCH" && (c.body as { body?: string }).body === "Hello",
    );
    expect(resync.length).toBeGreaterThanOrEqual(1);
    // 409 mismatch is expected/self-healing → NOT surfaced as onError.
    expect(errors.filter((p) => p === "midstream")).toHaveLength(0);

    await stream.finalize({ stopReason: "end_turn" });
  });
});

describe("MessageStream — stopReason whitelist (sink plan)", () => {
  function setupOk(): { fetch: typeof fetch; calls: RecordedRequest[] } {
    const initial = {
      id: "wl-1",
      conversation_id: "conv-1",
      type: "agent_reply",
      sender: "agent:c",
      body: "",
      state: "streaming",
      created_at: "2026-05-20T00:00:00Z",
    };
    return captureFetch((req) => {
      if (req.method === "POST") return { status: 201, body: initial };
      return { status: 200, body: { ...initial, state: "completed" } };
    });
  }

  it.each([
    "end_turn",
    "max_tokens",
    "tool_use",
    "timeout",
    "content_filter",
  ] as const)("allows known finalize stopReason %s through unchanged", async (known) => {
    const { fetch: f, calls } = setupOk();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: `wl-${known}`,
    });
    await stream.finalize({ stopReason: known });
    const last = calls.at(-1);
    expect((last?.body as { stop_reason?: string }).stop_reason).toBe(known);
  });

  it("falls back to end_turn for unknown finalize stopReason", async () => {
    const { fetch: f, calls } = setupOk();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "wl-unknown",
    });
    // Cast to bypass the StopReason union type so we exercise the
    // runtime whitelist (beeos-claw used to pre-filter via
    // KNOWN_COMPLETED_STOP_REASONS; now it's the SDK's job).
    await stream.finalize({ stopReason: "stop_sequence" as unknown as never });
    const last = calls.at(-1);
    expect((last?.body as { stop_reason?: string }).stop_reason).toBe("end_turn");
  });

  it("defaults missing stopReason to end_turn", async () => {
    const { fetch: f, calls } = setupOk();
    globalThis.fetch = f;
    const client = makeClient();
    const stream = client.messages.startStream({
      conversationId: "conv-1",
      id: "wl-default",
    });
    await stream.finalize();
    const last = calls.at(-1);
    expect((last?.body as { stop_reason?: string }).stop_reason).toBe("end_turn");
  });
});
