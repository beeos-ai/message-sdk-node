// HTTP SSE stream consumer (ADR-0025) — TypeScript port of the Go SDK
// `Conversations.Stream` / `pumpSSE`
// (backend/sdks/message-sdk-go/stream.go).
//
// While `ConversationSubscription` (client.ts) folds the delta wire off
// a Centrifugo WSS subscription, this reader consumes the same wire over
// the Message Service HTTP SSE edge
// (`GET /api/v2/conversations/{id}/stream`). It is the path a Node-side
// server consumer uses when it has no realtime WSS identity — symmetric
// with how the Go gateways (chatinvoke / a2a) consume MS.
//
// Behaviour mirrors the Go pump rule-for-rule:
//   - opt into the delta wire via `Accept: text/event-stream,
//     application/vnd.beeos.message-delta`;
//   - fold `message.delta` / `message.updated` / `message.created`
//     frames through the shared reducer (reducer.ts) keyed by
//     `message_id`, so every emitted Message carries the full cumulative
//     `body` plus the per-frame `bodyDelta`;
//   - on a delta for an in-flight row with no local base (typically the
//     first frame after a reconnect) rebase via REST `getEnvelope`,
//     bounded so a stalled GET can't hang the pump;
//   - surface `backfill_truncated` (typed) and `replay_complete` control
//     frames; ignore other control / heartbeat lines;
//   - track the latest offset so the caller can resume a dropped
//     connection via `stream({ since: reader.lastCursor() })`.
//
// Reconnect is caller-driven (same as the Go SSE Stream): the reader
// ends on a clean server close; the caller re-opens with the last
// cursor. The reducer + `since` resume make that lossless.

import { EventEmitter } from "node:events";

import { MessagingError, mapHttpError } from "./errors.js";
import { envelopeToMessage, type WireEnvelope } from "./wire.js";
import {
  applyWireFrame,
  appendedText,
  emptySnapshot,
  snapshotBody,
  snapshotFromBody,
  type ReducedSnapshot,
} from "./reducer.js";
import type { Message, MessageEnvelope } from "./types.js";

/** Terminal v3 states — once a row reaches one, its reducer entry is freed. */
const TERMINAL_STATES = new Set(["completed", "failed", "refused", "cancelled"]);

/** Default high-water mark for the internal pull queue. When the
 * consumer falls this far behind, the pump stops reading the socket
 * (TCP back-pressure) rather than dropping frames or growing unbounded. */
const DEFAULT_HIGH_WATER_MARK = 256;

/** Bounds each in-pump rebase GET so a stalled snapshot fetch can't hang
 * the stream — matches the Go `streamRebaseTimeout`. */
const REBASE_TIMEOUT_MS = 10_000;

/** Typed `backfill_truncated` control frame (ADR-0022). Emitted when a
 * resuming consumer's `since` predates the oldest retained delta chunk. */
export interface BackfillTruncatedEvent {
  oldest_redis_offset?: number;
  since?: number;
  hint?: string;
}

/** Typed `replay_complete` control frame — backfill is done, live frames follow. */
export interface ReplayCompleteEvent {
  latest_offset?: number;
}

/**
 * One open SSE connection, as returned by a {@link StreamDialer}. `body`
 * is the raw byte stream of the `text/event-stream` response; the reader
 * does its own SSE line framing.
 */
export interface StreamResponse {
  status: number;
  body: AsyncIterable<Uint8Array>;
}

/**
 * Opens the SSE connection. Injectable so tests can feed synthetic
 * frames without a live Message Service (mirrors the Go `streamDialer`).
 */
export type StreamDialer = (
  url: string,
  headers: Record<string, string>,
  signal: AbortSignal,
) => Promise<StreamResponse>;

export interface StreamOptions {
  /** Resume from this offset (exclusive) — maps to the MS `since` query param. */
  since?: number;
  /** Per-request auth token override (else the client's cached token / API key). */
  token?: string;
  /** Caller AbortSignal; aborting closes the connection. */
  signal?: AbortSignal;
  /** Override the HTTP transport (tests / non-fetch runtimes). */
  dialer?: StreamDialer;
  /** Internal back-pressure tuning; defaults to {@link DEFAULT_HIGH_WATER_MARK}. */
  highWaterMark?: number;
}

/** Dependencies the reader needs, wired by `ConversationsAPI.stream`. */
export interface MessageStreamReaderDeps {
  /** Open (or re-open) the SSE connection at the given resume offset. */
  connect: (since: number, signal: AbortSignal) => Promise<StreamResponse>;
  /** Authoritative snapshot refetch for rebase; returns null on any failure. */
  rebase: (messageId: string) => Promise<MessageEnvelope | null>;
  since?: number;
  signal?: AbortSignal;
  highWaterMark?: number;
}

export interface MessageStreamReaderEvents {
  backfill_truncated: (ev: BackfillTruncatedEvent) => void;
  replay_complete: (ev: ReplayCompleteEvent) => void;
  error: (err: Error) => void;
  close: () => void;
}

/**
 * Async-iterable SSE reader. Consume via `for await (const msg of
 * reader)`; each Message carries the reconstructed cumulative `body`
 * plus the incremental `bodyDelta`. Listen for `backfill_truncated` /
 * `replay_complete` control frames via `on(...)`. Call `close()` (or
 * abort the supplied signal) to tear down.
 */
export class MessageStreamReader
  extends EventEmitter
  implements AsyncIterable<Message>
{
  private readonly deps: MessageStreamReaderDeps;
  private readonly abort: AbortController;
  private readonly reducers = new Map<string, ReducedSnapshot>();
  private readonly hwm: number;

  // Pull-queue state.
  private readonly buffer: Message[] = [];
  private waiter: {
    resolve: (r: IteratorResult<Message>) => void;
    reject: (e: Error) => void;
  } | null = null;
  private drainWaiters: (() => void)[] = [];
  private failure: Error | null = null;
  private ended = false;
  private started = false;

  private lastOffset: number;

  constructor(deps: MessageStreamReaderDeps) {
    super();
    this.deps = deps;
    this.lastOffset = deps.since && deps.since > 0 ? deps.since : 0;
    this.hwm = deps.highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    this.abort = new AbortController();
    if (deps.signal) {
      if (deps.signal.aborted) this.abort.abort();
      else deps.signal.addEventListener("abort", () => this.abort.abort(), { once: true });
    }
  }

  /** Latest offset observed — pass as `since` to resume after a drop. */
  lastCursor(): number {
    return this.lastOffset;
  }

  /** Tear down the connection. Idempotent. */
  close(): void {
    this.abort.abort();
  }

  override on<K extends keyof MessageStreamReaderEvents>(
    event: K,
    listener: MessageStreamReaderEvents[K],
  ): this;
  override on(event: string | symbol, listener: (...args: unknown[]) => void): this;
  override on(event: string | symbol, listener: (...args: unknown[]) => void): this {
    return super.on(event, listener);
  }

  [Symbol.asyncIterator](): AsyncIterator<Message> {
    if (!this.started) {
      this.started = true;
      void this.run();
    }
    return {
      next: () => this.pull(),
      return: async () => {
        this.close();
        return { value: undefined, done: true };
      },
    };
  }

  // --- pull-queue plumbing ------------------------------------------

  private pull(): Promise<IteratorResult<Message>> {
    if (this.buffer.length > 0) {
      const value = this.buffer.shift()!;
      this.maybeDrain();
      return Promise.resolve({ value, done: false });
    }
    if (this.failure) {
      const err = this.failure;
      this.failure = null;
      return Promise.reject(err);
    }
    if (this.ended) return Promise.resolve({ value: undefined as never, done: true });
    return new Promise<IteratorResult<Message>>((resolve, reject) => {
      this.waiter = { resolve, reject };
    });
  }

  private push(msg: Message): void {
    if (this.ended) return;
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w.resolve({ value: msg, done: false });
      return;
    }
    this.buffer.push(msg);
  }

  private finish(): void {
    if (this.ended) return;
    this.ended = true;
    this.emit("close");
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w.resolve({ value: undefined as never, done: true });
    }
    this.wakeDrain();
  }

  private fail(err: Error): void {
    if (this.ended) return;
    this.ended = true;
    // Reject the awaiting consumer FIRST — that is the primary error
    // surface for `for await`. Only emit the "error" event when a
    // listener exists; EventEmitter throws on an unhandled "error".
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w.reject(err);
    } else {
      this.failure = err;
    }
    this.wakeDrain();
    if (this.listenerCount("error") > 0) this.emit("error", err);
  }

  /** Resolve when the buffer has drained below the high-water mark. */
  private whenDrained(): Promise<void> {
    if (this.buffer.length < this.hwm || this.ended) return Promise.resolve();
    return new Promise<void>((resolve) => this.drainWaiters.push(resolve));
  }

  private maybeDrain(): void {
    if (this.buffer.length < this.hwm) this.wakeDrain();
  }

  private wakeDrain(): void {
    const waiters = this.drainWaiters;
    this.drainWaiters = [];
    for (const w of waiters) w();
  }

  // --- the pump -----------------------------------------------------

  private async run(): Promise<void> {
    try {
      const resp = await this.deps.connect(this.lastOffset, this.abort.signal);
      await this.pump(resp.body);
      this.finish();
    } catch (err) {
      if (this.abort.signal.aborted) {
        this.finish();
        return;
      }
      this.fail(err instanceof Error ? err : new Error(String(err)));
    }
  }

  private async pump(body: AsyncIterable<Uint8Array>): Promise<void> {
    let dataBuf = "";
    let eventName = "";

    for await (const line of iterLines(body)) {
      if (this.abort.signal.aborted) return;

      if (line === "") {
        // Blank line terminates an event.
        if (dataBuf === "") {
          eventName = "";
          continue;
        }
        const payload = dataBuf;
        const currentEvent = eventName;
        dataBuf = "";
        eventName = "";
        await this.dispatch(currentEvent, payload);
        await this.whenDrained();
        continue;
      }
      if (line.startsWith(":")) continue; // comment / heartbeat
      if (line.startsWith("event:")) {
        eventName = line.slice("event:".length).trim();
        continue;
      }
      if (line.startsWith("data:")) {
        let chunk = line.slice("data:".length);
        if (chunk.startsWith(" ")) chunk = chunk.slice(1);
        dataBuf += chunk;
        continue;
      }
      // id: / retry: and any other field — the v3 envelope carries the
      // authoritative offset, so SSE-protocol fields are not load-bearing.
    }
  }

  private async dispatch(eventName: string, payload: string): Promise<void> {
    // Control frames: event name is non-empty and not the default
    // "message". Surface the two typed ones; drop the rest.
    if (eventName !== "" && eventName !== "message") {
      if (eventName === "backfill_truncated") {
        try {
          this.emit("backfill_truncated", JSON.parse(payload) as BackfillTruncatedEvent);
        } catch {
          /* ignore malformed control frame */
        }
      } else if (eventName === "replay_complete") {
        try {
          this.emit("replay_complete", JSON.parse(payload) as ReplayCompleteEvent);
        } catch {
          /* ignore */
        }
      }
      return;
    }

    let env: WireEnvelope;
    try {
      env = JSON.parse(payload) as WireEnvelope;
    } catch {
      return; // unparseable data frame — skip silently (matches Go)
    }

    // Heartbeat ping on the default `message` event
    // (`data: {"type":"ping"}`). A gateway may inject these during long
    // silent windows to keep proxies alive; it has no message_id / body,
    // so it must never surface as a Message. (A named `event: ping` is
    // already dropped by the control-frame branch above.) Reaching here
    // is itself the liveness signal — there is no internal idle watchdog
    // to reset (the pump relies on the AbortSignal + TCP), so we just
    // drop it.
    if ((env as { type?: string }).type === "ping" && !env.message_id) {
      return;
    }

    const id = env.message_id ?? "";
    if (typeof env.offset === "number" && env.offset > this.lastOffset) {
      this.lastOffset = env.offset;
    }

    if (!id) {
      // Legacy / control envelope with no message id — pass through unfolded.
      this.push(envelopeToMessage(env));
      return;
    }

    const prev = this.reducers.get(id) ?? emptySnapshot();
    const { snapshot, result } = applyWireFrame(prev, env);

    if (result === "rebase") {
      // First delta for an in-flight row we have no base for (typically
      // the first frame after a reconnect). Fetch the authoritative
      // snapshot and fold onto it; degrade to "keep last-known body" on
      // any failure — never block the pump.
      const snap = await this.tryRebase(id);
      const msg = envelopeToMessage(env);
      if (snap) {
        this.reducers.set(
          id,
          snapshotFromBody(snap.body, snap.parts, snap.state, snap.stopReason),
        );
        msg.body = snap.body;
        msg.parts = snap.parts;
        msg.state = snap.state;
        msg.stopReason = snap.stopReason;
        msg.bodyDelta = "";
        if (snap.state && TERMINAL_STATES.has(snap.state)) this.reducers.delete(id);
      } else {
        msg.body = snapshotBody(prev);
        msg.bodyDelta = "";
      }
      this.push(msg);
      return;
    }

    if (result === "ignored") {
      this.push(envelopeToMessage(env));
      return;
    }

    // ok — surface the reconstructed cumulative body + the increment.
    const bodyDelta = appendedText(prev.bodyBytes, snapshot);
    this.reducers.set(id, snapshot);

    const msg = envelopeToMessage(env);
    msg.body = snapshotBody(snapshot);
    msg.bodyDelta = bodyDelta;
    if (snapshot.parts !== undefined) msg.parts = snapshot.parts;
    if (snapshot.state) msg.state = snapshot.state as Message["state"];
    if (snapshot.stopReason) msg.stopReason = snapshot.stopReason as Message["stopReason"];

    if (snapshot.state && TERMINAL_STATES.has(snapshot.state)) this.reducers.delete(id);
    this.push(msg);
  }

  private async tryRebase(messageId: string): Promise<MessageEnvelope | null> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const timeout = new Promise<null>((resolve) => {
        timer = setTimeout(() => resolve(null), REBASE_TIMEOUT_MS);
      });
      return await Promise.race([this.deps.rebase(messageId), timeout]);
    } catch {
      return null;
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
}

/**
 * Stream the bytes of an `AsyncIterable<Uint8Array>` as SSE lines
 * (split on `\n`, trailing `\r` trimmed). A trailing partial line at
 * end-of-stream is flushed.
 */
export async function* iterLines(
  source: AsyncIterable<Uint8Array>,
): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let buf = "";
  for await (const chunk of source) {
    buf += decoder.decode(chunk, { stream: true });
    let idx: number;
    while ((idx = buf.indexOf("\n")) >= 0) {
      let line = buf.slice(0, idx);
      if (line.endsWith("\r")) line = line.slice(0, -1);
      yield line;
      buf = buf.slice(idx + 1);
    }
  }
  buf += decoder.decode();
  if (buf.length > 0) {
    if (buf.endsWith("\r")) buf = buf.slice(0, -1);
    yield buf;
  }
}

/**
 * Default fetch-based dialer. Uses `Accept: text/event-stream,
 * application/vnd.beeos.message-delta` (the caller's `connect` closure
 * adds it). Maps a non-2xx open to the SDK error sentinels.
 */
export const defaultStreamDialer: StreamDialer = async (url, headers, signal) => {
  const resp = await fetch(url, { method: "GET", headers, signal });
  if (!resp.ok) {
    let data: unknown;
    try {
      data = await resp.json();
    } catch {
      /* body may not be JSON */
    }
    throw mapHttpError(resp.status, data);
  }
  if (!resp.body) {
    throw new MessagingError("stream: empty response body", resp.status);
  }
  return { status: resp.status, body: readableToBytes(resp.body) };
};

/** Adapt a web ReadableStream<Uint8Array> to an AsyncIterable<Uint8Array>. */
function readableToBytes(
  stream: ReadableStream<Uint8Array>,
): AsyncIterable<Uint8Array> {
  // Node 20+ ReadableStream is async-iterable, but not all runtimes
  // expose it — wrap the reader explicitly for portability.
  return {
    async *[Symbol.asyncIterator]() {
      const reader = stream.getReader();
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) return;
          if (value) yield value;
        }
      } finally {
        reader.releaseLock();
      }
    },
  };
}
