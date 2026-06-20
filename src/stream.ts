/**
 * MessageStream — ergonomic SDK wrapper around the BeeOS Message
 * Service Envelope v3 wire (POST/PATCH/GET /api/v3/conversations/{cid}/messages).
 *
 * Wire protocol (ADR-0025 — append-only delta):
 *   - `appendBody(chunk)` ships the chunk *immediately* as a
 *     `{body_append, body_from}` PATCH — `body_from` is the UTF-8 byte
 *     offset the chunk attaches at. No 256-char / 50ms batching buffer.
 *   - `setBody(text)` and `finalize/fail/...` still ship the full
 *     cumulative `body` (snapshot replace) — used for whole-turn
 *     rewrites and the terminal write.
 *   - `addPart` / `replacePart` ship the full `parts` array (parts are
 *     small and change rarely; the body is the bandwidth hot path).
 *
 * Why no buffer: keeping the SDK bufferless preserves the agent's
 * native output cadence and means an openclaw/beeos-claw restart loses
 * at most the single chunk in flight, not a 50ms/256-char backlog.
 *
 * Send pacing (back-pressure coalescing, no timer): while a PATCH is in
 * flight, further appended chunks accumulate in `pendingBody`; the next
 * chain step flushes them as ONE `body_append`. When the link is idle
 * each chunk goes out on its own. The SDK's only streaming state is two
 * integers — `sentBytes` (UTF-8 bytes the server has) and the pending
 * buffer's start offset — so it is effectively stateless across the
 * cumulative body it also keeps for finalize().
 *
 * Lifecycle (since v2 — `sink-streaming-to-sdk` plan):
 *
 *   1. `client.messages.startStream(...)` returns a `MessageStream`
 *      **synchronously**. The underlying POST runs in the background
 *      and is exposed via `stream.opened()`.
 *   2. Callers may `appendBody` / `addPart` / `appendThinking` /
 *      `appendToolUse` / `appendToolResult` immediately — writes are
 *      enqueued and dispatched after the open POST resolves.
 *   3. `finalize/fail/refuse/cancel` block until the terminal write
 *      lands. If the open POST FAILED, the terminal call falls back
 *      to a single-shot `sendV3` POST (different id, same replyTo)
 *      so consumers still see a terminal envelope row instead of an
 *      indefinite "still streaming" gap.
 *
 * Concurrency model: a MessageStream is single-writer. The SDK
 * serializes the PATCH chain so the server sees writes in append
 * order even when the caller's appends are interleaved with awaits.
 */

import { randomUUID } from "node:crypto";

import { MessagingError } from "./errors.js";
import type {
  MessageEnvelope,
  MessageState,
  MessageStreamOptions,
  Part,
  PartState,
  StartStreamInput,
  StopReason,
} from "./types.js";

/** Internal request interface — kept narrow so tests can fake it. */
export interface MessageStreamTransport {
  request<T>(
    method: string,
    path: string,
    body?: unknown,
    extraHeaders?: Record<string, string>,
    reqOpts?: RequestOptions,
  ): Promise<T>;
}

/** Subset of MessageClient request options exposed to the stream. */
export interface RequestOptions {
  token?: string;
  sender?: string;
  timeoutMs?: number;
}

/**
 * Fixed retry delay for the terminal PATCH. Sized for same-DC
 * Message Service deployments where round-trip is ~10ms — a single
 * 200ms retry covers ~99% of transient blips without dragging the
 * finalize() critical path. We deliberately do NOT do exponential
 * backoff: if 200ms isn't enough, MS is genuinely unreachable and
 * more retries just delay the caller's error handling.
 */
const TERMINAL_RETRY_DELAY_MS = 200;

/**
 * StopReason values that map cleanly onto v3 `state="completed"`.
 * `finalize({stopReason})` rejects anything outside this set with a
 * silent fallback to `"end_turn"` so MS's `ValidateStopReason` whitelist
 * never 400s a finalize PATCH for an upstream vocabulary mismatch (e.g.
 * Anthropic's `stop_sequence` which has no direct v3 mapping).
 *
 * `error` / `refused` / `user_stop` / `agent_lost` are excluded because
 * they belong to the `fail` / `refuse` / `cancel` / server-reaper
 * terminal paths respectively — completed has its own narrow vocabulary.
 */
const KNOWN_COMPLETED_STOP_REASONS: ReadonlySet<StopReason> =
  new Set<StopReason>([
    "end_turn",
    "max_tokens",
    "tool_use",
    "timeout",
    "content_filter",
  ]);

/**
 * MessageStream handle. Construct via `client.messages.startStream(...)`
 * — never directly via `new`.
 *
 * Lifecycle:
 *
 *   1. Constructed synchronously; POST runs in the background via
 *      `openPromise`. Writes accumulate locally.
 *   2. POST resolves → `openState="open"`; queued appends flush as
 *      `body_append` PATCHes on the next chain step (back-pressure
 *      coalescing, no timer).
 *   3. POST rejects → `openState="failed"`; future PATCHes no-op;
 *      `finalize/fail/refuse/cancel` falls back to a single-shot
 *      `sendV3` POST with terminal state preset.
 *   4. After any terminal call, further appends throw `StreamTerminatedError`.
 */
export class MessageStream {
  private readonly transport: MessageStreamTransport;
  private readonly reqOpts?: RequestOptions;
  private readonly conversationId: string;
  /** Pinned StartStream input — reused by the open-failure sendV3 fallback. */
  private readonly input: StartStreamInput;
  /**
   * Stable UUID. Populated synchronously from `input.id`; refreshed
   * from the POST response on success (typically equal). On open
   * failure + sendV3 fallback the id is updated to the new row's id
   * so `stream.id` always reflects what's actually in MS.
   */
  private messageId: string;

  /** Snapshot fields kept in sync with the wire. */
  private body = "";
  private parts: Part[] = [];
  private state: MessageState = "streaming";
  private stopReason?: StopReason;

  /** Set after first POST resolves (success path). */
  private initialEnvelope?: MessageEnvelope;

  /**
   * Append-only delta state (ADR-0025).
   *   - sentBytes: UTF-8 byte length the server is known to hold. It is
   *     the single source of truth for the next append's `body_from` —
   *     derived at drain time (NOT captured when appendBody runs), so a
   *     setBody/heal that lands mid-flight can't desync the cursor.
   *   - pendingBody: chunks appended while a PATCH is in flight,
   *     coalesced into the next `body_append`. Because the write chain
   *     is serial, on each drain pendingBody is exactly the bytes that
   *     follow sentBytes, so `from = sentBytes` is always correct.
   *   - dirtyParts: a parts change is queued (shipped as a full array).
   */
  private sentBytes = 0;
  private pendingBody = "";
  private dirtyParts = false;
  /** Set by setBody(): the next drain ships a full-body snapshot replace. */
  private snapshotPending = false;

  /** Open lifecycle. PATCH/flush only fires while `openState === "open"`. */
  private openState: "opening" | "open" | "failed" = "opening";
  private openError?: Error;
  /** Exposed via `opened()` so callers can await POST landing if needed. */
  private readonly openPromise: Promise<MessageEnvelope>;

  /** Set when finalize/fail/cancel is called. After this, no more writes. */
  private terminated = false;
  /**
   * Sequential promise chain so PATCHes are serialized. Initial value
   * is gated on the open POST settling (success OR failure — failures
   * are absorbed so the chain stays alive; downstream operations
   * branch on `openState` to decide whether to PATCH or sendV3).
   */
  private chain: Promise<void>;

  private readonly onError?: (err: Error, phase: "midstream" | "terminal") => void;

  constructor(
    transport: MessageStreamTransport,
    input: StartStreamInput,
    opts: MessageStreamOptions | undefined,
    reqOpts: RequestOptions | undefined,
    openPromise: Promise<MessageEnvelope>,
  ) {
    this.transport = transport;
    this.input = input;
    this.conversationId = input.conversationId;
    this.messageId = input.id ?? "";
    this.reqOpts = reqOpts;
    this.onError = opts?.onError;
    // Seed local snapshot with whatever the caller pre-populated on
    // the POST — the server stores these on create, so they must
    // match the wire view. The seed body is already persisted by the
    // POST, so the server starts at its byte length.
    this.body = input.body ?? "";
    this.sentBytes = utf8Len(this.body);
    if (input.parts) this.parts = [...input.parts];

    // Wrap openPromise so neither branch escapes as an unhandled
    // rejection AND the chain can be initialized to wait on it.
    this.openPromise = openPromise;
    this.chain = openPromise.then(
      (env) => {
        this.openState = "open";
        this.initialEnvelope = env;
        if (env.id) this.messageId = env.id;
        // DO NOT clobber body/parts/state from `env`: writes appended
        // between sync construct and POST resolve must survive. The
        // first PATCH ships the cumulative snapshot anyway.
      },
      (err) => {
        this.openState = "failed";
        this.openError = err instanceof Error ? err : new Error(String(err));
        // Absorb the rejection so the chain stays usable. The
        // terminate() path inspects openState and falls back to
        // sendV3. We deliberately do NOT call onError here for the
        // open-POST failure — the SDK's contract is that onError
        // fires on PATCH failures; the open-POST result is surfaced
        // via `await stream.opened()` for callers that care.
      },
    );
  }

  /** The stable v3 message id. */
  get id(): string {
    return this.messageId;
  }

  /** The first POST response. Defined after the open POST resolves. */
  get envelope(): MessageEnvelope | undefined {
    return this.initialEnvelope;
  }

  /** True after finalize / fail / cancel has been awaited. */
  get isTerminated(): boolean {
    return this.terminated;
  }

  /**
   * Awaits the underlying open POST. Resolves with the initial
   * envelope on success or rejects with the POST error on failure.
   * Most callers can ignore this — appends are queued and the
   * snapshot semantics handle ordering. Useful for tests / SSE
   * consumers that need the server-side row before continuing.
   */
  async opened(): Promise<MessageEnvelope> {
    return this.openPromise;
  }

  /**
   * Append text to the cumulative body. Multiple consecutive
   * appendBody calls accumulate locally and flush as a single PATCH
   * — so a thousand-token stream incurs ~20 PATCHes, not a thousand.
   *
   * Safe to call before the open POST has resolved; the appended
   * content is buffered locally and shipped in the first PATCH after
   * `openState` transitions to `"open"`.
   */
  appendBody(chunk: string): void {
    this.assertActive();
    if (!chunk) return;
    this.body += chunk;
    // Accumulate into the pending append buffer. The drain computes
    // body_from from sentBytes at send time, so appendBody just grows
    // the buffer — no cursor to anchor here (avoids the setBody/heal
    // mid-flight desync, see drainOnce).
    this.pendingBody += chunk;
    this.kick();
  }

  /**
   * Replace the cumulative body with a snapshot value. Useful when
   * the caller already holds the canonical full-turn text. Safe to
   * call before open resolves. Ships as a full-body snapshot PATCH
   * (not an append) and re-anchors the append cursor to the new length.
   */
  setBody(text: string): void {
    this.assertActive();
    if (text === this.body) return;
    this.body = text;
    // Any queued append is now superseded by the full snapshot.
    this.pendingBody = "";
    this.snapshotPending = true;
    this.kick();
  }

  /**
   * Append a new part to the parts array. Safe to call before open
   * resolves; the parts array is shipped in full on the next PATCH.
   */
  addPart(part: Part): void {
    this.assertActive();
    this.parts.push(part);
    this.dirtyParts = true;
    this.kick();
  }

  /**
   * Replace a part by index. Useful for transitioning thinking /
   * tool_use parts from `state="streaming"` to `state="done"`.
   */
  replacePart(index: number, part: Part): void {
    this.assertActive();
    if (index < 0 || index >= this.parts.length) {
      throw new Error(
        `MessageStream.replacePart: index ${index} out of range [0,${this.parts.length})`,
      );
    }
    this.parts[index] = part;
    this.dirtyParts = true;
    this.kick();
  }

  /** Sugar: append a thinking part. */
  appendThinking(text: string, state: PartState = "done"): void {
    this.addPart({ type: "thinking", text, state });
  }

  /** Sugar: append a tool_use part. */
  appendToolUse(
    id: string,
    name: string,
    args: unknown,
    state: PartState = "streaming",
  ): void {
    this.addPart({ type: "tool_use", id, name, arguments: args, state });
  }

  /** Sugar: append a tool_result part. */
  appendToolResult(
    toolUseId: string,
    content: string | object,
    isError = false,
  ): void {
    this.addPart({
      type: "tool_result",
      tool_use_id: toolUseId,
      content,
      ...(isError ? { is_error: true } : {}),
    });
  }

  /**
   * Send the terminal write with state="completed". The `stopReason`
   * is whitelisted to MS's `ValidateStopReason` "completed" vocabulary
   * — unknown / upstream-specific values silently fall back to
   * `"end_turn"` so we never 400 the terminal PATCH on a vocabulary
   * mismatch. Pass `body` to overwrite the cumulative body with a
   * final snapshot (e.g. the runtime's canonical full-turn transcript).
   *
   * On open-POST failure, falls back to a single-shot `sendV3` POST
   * with state="completed".
   */
  async finalize(opts?: {
    stopReason?: StopReason;
    body?: string;
  }): Promise<MessageEnvelope> {
    const raw = opts?.stopReason;
    const reason: StopReason =
      raw && KNOWN_COMPLETED_STOP_REASONS.has(raw) ? raw : "end_turn";
    if (opts?.body !== undefined) {
      this.body = opts.body;
    }
    return this.terminate("completed", reason);
  }

  /**
   * Send the terminal write with state="failed" (default) or a
   * caller-supplied non-success state. Use `state` to write
   * "refused" / "cancelled" — but prefer the dedicated `refuse()` /
   * `cancel()` methods for clarity.
   *
   * On open-POST failure, falls back to a single-shot `sendV3` POST
   * with the chosen terminal state.
   */
  async fail(opts?: {
    body?: string;
    state?: Exclude<MessageState, "streaming" | "completed">;
    stopReason?: StopReason;
  }): Promise<MessageEnvelope> {
    if (opts?.body !== undefined) {
      this.body = opts.body;
    }
    const state = opts?.state ?? "failed";
    const stop = opts?.stopReason ?? "error";
    return this.terminate(state, stop);
  }

  /**
   * Sugar for `fail({state:"refused", stopReason:"refused"})`. Use when
   * the agent declined to answer (content filter trip, policy, …).
   * `body` carries the refusal message that consumers see on
   * `/wait` / SSE.
   */
  async refuse(opts?: {
    body?: string;
    stopReason?: StopReason;
  }): Promise<MessageEnvelope> {
    if (opts?.body !== undefined) {
      this.body = opts.body;
    }
    return this.terminate("refused", opts?.stopReason ?? "refused");
  }

  /** Sugar for `fail({state:"cancelled", stopReason:"user_stop"})`. */
  async cancel(opts?: { stopReason?: StopReason }): Promise<MessageEnvelope> {
    return this.terminate("cancelled", opts?.stopReason ?? "user_stop");
  }

  // --- internals ----------------------------------------------------

  private assertActive(): void {
    if (this.terminated) {
      throw new StreamTerminatedError(this.messageId, this.state);
    }
  }

  /**
   * Enqueue a drain on the serialized chain. No timer: each mutator
   * calls kick(), and drainOnce() ships whatever is pending. Multiple
   * kicks while a PATCH is in flight collapse — the queued drains after
   * the first find the pending buffer already empty and no-op, so a
   * burst of appendBody calls coalesces into the next PATCH naturally.
   */
  private kick(): void {
    if (this.terminated) return;
    this.chain = this.chain
      .then(() => this.drainOnce())
      .catch((err) => this.notifyError(err, "midstream"));
  }

  private async drainOnce(): Promise<void> {
    if (this.terminated) return;
    // Open POST never landed → no row to PATCH. Pending writes stay in
    // memory and ride out on the terminal sendV3 fallback.
    if (this.openState !== "open") return;

    // Full-body snapshot replace (setBody) takes precedence — it makes
    // any queued append moot and re-anchors the append cursor.
    if (this.snapshotPending) {
      this.snapshotPending = false;
      // Capture the body we actually send BEFORE awaiting: appends that
      // land during the PATCH grow this.body but are NOT in this
      // snapshot, so sentBytes must reflect the captured length only.
      const sentBody = this.body;
      const patch: Record<string, unknown> = { body: sentBody };
      if (this.dirtyParts) {
        patch.parts = this.parts;
        this.dirtyParts = false;
      }
      await this.patchSnapshot(patch);
      this.sentBytes = utf8Len(sentBody);
      return;
    }

    const hasBody = this.pendingBody !== "";
    if (!hasBody && !this.dirtyParts) return;

    const chunk = this.pendingBody;
    // from is derived from the confirmed server length at send time. The
    // serial write chain guarantees pendingBody is exactly the bytes
    // following sentBytes, so this is always the correct offset.
    const from = this.sentBytes;
    this.pendingBody = "";

    const patch: Record<string, unknown> = {};
    if (hasBody) {
      patch.body_append = chunk;
      patch.body_from = from;
    }
    if (this.dirtyParts) {
      patch.parts = this.parts;
      this.dirtyParts = false;
    }

    try {
      await this.patchSnapshot(patch);
      if (hasBody) {
        this.sentBytes = from + utf8Len(chunk);
      }
    } catch (err) {
      // An append_offset_mismatch (409) means our body_from drifted from
      // the server's length (e.g. a dropped/duplicated PATCH). The
      // producer is the sole writer and holds the authoritative full
      // body, so self-heal silently by re-sending the whole body as a
      // snapshot replace — no need to parse the server length.
      if (isAppendOffsetMismatch(err)) {
        // Capture before await for the same reason as the snapshot
        // branch: concurrent appends grow this.body but aren't in the
        // body we resend, so sentBytes tracks the captured length only.
        const healBody = this.body;
        await this.patchSnapshot({ body: healBody });
        this.sentBytes = utf8Len(healBody);
        return;
      }
      // Any other failure (5xx / network): the chunk we just consumed
      // from pendingBody now lives only in this.body. Flag a full-body
      // resync so the NEXT write self-heals (the same snapshot recovery
      // the pre-ADR-0025 model relied on), then surface the error to
      // onError("midstream") via kick()'s catch. The terminal write
      // also ships the full body, so a stream that ends here loses
      // nothing.
      this.snapshotPending = true;
      throw err;
    }
  }

  private async terminate(
    state: MessageState,
    stopReason: StopReason,
  ): Promise<MessageEnvelope> {
    if (this.terminated) {
      return this.initialEnvelope ?? this.localSnapshot();
    }
    this.terminated = true;
    // The terminal write ships the full cumulative body (snapshot
    // replace), so drop any queued append — it is already covered.
    this.pendingBody = "";
    this.snapshotPending = false;

    this.chain = this.chain.then(async () => {
      if (this.openState === "failed") {
        await this.sendV3Fallback(state, stopReason);
      } else {
        const patch: Record<string, unknown> = { state };
        if (stopReason) patch.stop_reason = stopReason;
        patch.body = this.body;
        if (this.parts.length > 0) patch.parts = this.parts;
        await this.patchSnapshotWithTerminalRetry(patch);
      }
      this.state = state;
      this.stopReason = stopReason;
    });
    await this.chain;
    return this.initialEnvelope
      ? { ...this.initialEnvelope, ...this.localSnapshot() }
      : this.localSnapshot();
  }

  /**
   * Single-shot fallback when the open POST failed. Posts a NEW v3
   * envelope with terminal state pre-applied so consumers see a
   * terminal row even though the streaming row never existed.
   *
   * Trade-offs:
   *   - The fallback row uses a fresh UUID rather than the original
   *     `input.id`. This avoids races against any partial server
   *     state the failed POST may have created. `stream.id` is
   *     updated to the new id so callers reading it post-terminate
   *     see what's actually in MS.
   *   - Failure of the fallback itself is reported via
   *     `onError("terminal")` but does NOT re-throw. Beeos-claw
   *     parity: a terminal-write failure is observability-only; the
   *     awaiting `finalize/fail/...` caller's logic should not branch
   *     on whether the fallback row landed (in practice the server-
   *     side stale-streaming reaper handles the gap).
   */
  private async sendV3Fallback(
    state: MessageState,
    stopReason: StopReason,
  ): Promise<void> {
    const newId = randomUUID();
    try {
      const env = await postStreamingEnvelope(
        this.transport,
        {
          conversationId: this.conversationId,
          id: newId,
          type: this.input.type ?? "agent_reply",
          replyTo: this.input.replyTo,
          body: this.body,
          parts: this.parts.length > 0 ? this.parts : undefined,
          state,
          stopReason,
        },
        this.reqOpts,
      );
      this.initialEnvelope = env;
      if (env.id) this.messageId = env.id;
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      this.notifyError(e, "terminal");
      // Intentionally NOT re-thrown — fallback failure is warn-only.
    }
  }

  private async patchSnapshot(patch: Record<string, unknown>): Promise<void> {
    const raw = await this.transport.request<Record<string, unknown>>(
      "PATCH",
      `/api/v3/conversations/${encodeURIComponent(this.conversationId)}/messages/${encodeURIComponent(this.messageId)}`,
      patch,
      undefined,
      this.reqOpts,
    );
    if (raw && typeof raw === "object") {
      this.initialEnvelope = rawToEnvelope(raw);
    }
  }

  /**
   * Terminal PATCH wrapper with a single short-delay retry. Protects
   * against the only failure mode that v3 snapshot semantics cannot
   * self-heal: if the terminal PATCH never lands, the row stays in
   * state="streaming" forever and downstream consumers (SSE, wait)
   * never see the stream end.
   */
  private async patchSnapshotWithTerminalRetry(patch: Record<string, unknown>): Promise<void> {
    try {
      await this.patchSnapshot(patch);
      return;
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      if (isNonRetryable4xx(e)) {
        this.notifyError(e, "terminal");
        throw e;
      }
      await sleep(TERMINAL_RETRY_DELAY_MS);
      try {
        await this.patchSnapshot(patch);
      } catch (retryErr) {
        const re = retryErr instanceof Error ? retryErr : new Error(String(retryErr));
        this.notifyError(re, "terminal");
        throw re;
      }
    }
  }

  /**
   * Invokes the user-supplied onError hook with full fault isolation:
   * observer exceptions MUST NOT alter stream lifecycle.
   */
  private notifyError(err: Error, phase: "midstream" | "terminal"): void {
    if (!this.onError) return;
    try {
      this.onError(err, phase);
    } catch {
      // intentionally empty — observer faults are not our concern
    }
  }

  private localSnapshot(): MessageEnvelope {
    return {
      id: this.messageId,
      conversationId: this.conversationId,
      type: this.initialEnvelope?.type ?? this.input.type ?? "agent_reply",
      sender: this.initialEnvelope?.sender ?? "",
      replyTo: this.initialEnvelope?.replyTo ?? this.input.replyTo,
      body: this.body,
      parts: this.parts.length > 0 ? this.parts : undefined,
      state: this.state,
      stopReason: this.stopReason,
      createdAt: this.initialEnvelope?.createdAt ?? new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
  }
}

/**
 * Returns true when the error is a 4xx HTTP failure that should NOT
 * be retried on terminal PATCH.
 */
function isNonRetryable4xx(err: Error): boolean {
  if (err instanceof MessagingError) {
    return err.status >= 400 && err.status < 500;
  }
  return false;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * UTF-8 byte length of a string. Matches PostgreSQL `octet_length(body)`
 * and Go `len(string)` so the `body_from` offset lines up across the
 * producer SDK, the Message Service, and consumers. Uses Buffer (Node)
 * with a TextEncoder fallback for non-Node runtimes.
 */
function utf8Len(s: string): number {
  if (typeof Buffer !== "undefined") return Buffer.byteLength(s, "utf8");
  return new TextEncoder().encode(s).length;
}

/**
 * True when the error is the Message Service's 409 append-offset
 * mismatch (ADR-0025) — the signal to resync via a full-body replace.
 */
function isAppendOffsetMismatch(err: unknown): boolean {
  return (
    err instanceof MessagingError &&
    err.status === 409 &&
    err.code === "append_offset_mismatch"
  );
}

/** Thrown when a caller mutates a stream after finalize/fail/cancel. */
export class StreamTerminatedError extends Error {
  constructor(public readonly messageId: string, public readonly state: MessageState) {
    super(
      `MessageStream ${messageId} is already terminal (state=${state}); cannot mutate further`,
    );
    this.name = "StreamTerminatedError";
  }
}

/**
 * @internal — converts a raw v3 wire object into a typed
 * MessageEnvelope. Tolerates unknown fields (forward-compat).
 */
export function rawToEnvelope(raw: Record<string, unknown>): MessageEnvelope {
  return {
    id: String(raw.id ?? ""),
    conversationId: String(raw.conversation_id ?? ""),
    type: String(raw.type ?? ""),
    sender: String(raw.sender ?? ""),
    replyTo: typeof raw.reply_to === "string" ? raw.reply_to : undefined,
    body: typeof raw.body === "string" ? raw.body : "",
    parts: Array.isArray(raw.parts) ? (raw.parts as Part[]) : undefined,
    state: (raw.state as MessageState) ?? "completed",
    stopReason:
      typeof raw.stop_reason === "string"
        ? (raw.stop_reason as StopReason)
        : undefined,
    content: raw.content,
    idempotent: raw.idempotent === true,
    createdAt: String(raw.created_at ?? ""),
    updatedAt:
      typeof raw.updated_at === "string" ? raw.updated_at : undefined,
  };
}

/**
 * @internal — POST helper used by client.messages.startStream /
 * sendV3 / MessageStream.sendV3Fallback.
 */
export async function postStreamingEnvelope(
  transport: MessageStreamTransport,
  input: StartStreamInput,
  reqOpts?: RequestOptions,
): Promise<MessageEnvelope> {
  const headers: Record<string, string> = {};
  if (input.id) headers["Idempotency-Key"] = input.id;
  const body: Record<string, unknown> = {
    type: input.type ?? "agent_reply",
    state: input.state ?? "streaming",
  };
  if (input.replyTo) body.reply_to = input.replyTo;
  if (input.body) body.body = input.body;
  if (input.parts) body.parts = input.parts;
  if (input.stopReason) body.stop_reason = input.stopReason;
  if (input.requireSubscriber) body.require_subscriber = true;
  const raw = await transport.request<Record<string, unknown>>(
    "POST",
    `/api/v3/conversations/${encodeURIComponent(input.conversationId)}/messages`,
    body,
    headers,
    reqOpts,
  );
  return rawToEnvelope(raw ?? {});
}
