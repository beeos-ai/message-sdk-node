/**
 * MessageClient — single unified client for the BeeOS Message Service.
 *
 *   - Namespaced REST: `client.conversations.*`, `client.messages.*`,
 *     `client.identities.*` (4-concept frame: Conversation, Message,
 *     Identity, Subscription). All requests go to `/api/v2/*`.
 *
 *   - Realtime: `await client.connect()` opens a Centrifugo
 *     WebSocket, auto-subscribes the caller's personal channel, and
 *     dispatches incoming publications as v2 `Message<T>` via
 *     EventEmitter (`client.on("message", ...)`).
 *
 *   - Group subscriptions: `await client.conversations.subscribe(id)`
 *     returns a `ConversationSubscription` handle whose `.on("message")`
 *     emits messages from `ch:{id}`.
 *
 * Auth: pick one mode (mutually exclusive).
 *
 *   - Service-side: `apiKey` (forwarded as `X-API-Key`).
 *   - Identity-bound (agent / user): `tokenProvider` async function.
 *     The provider returns a `TokenResponse` containing the JWT, the
 *     Centrifugo WSS URL, and (optionally) the MS REST base URL.
 *     The client caches the JWT and uses it as `Authorization: Bearer`
 *     on REST + hands it to Centrifuge for WebSocket auth.
 */

import { EventEmitter } from "node:events";

import {
  Centrifuge,
  type PublicationContext,
  type ServerPublicationContext,
  type SubscribedContext,
} from "centrifuge";
import WebSocket from "ws";

import { mapHttpError } from "./errors.js";
import { envelopeToMessage, type WireEnvelope } from "./wire.js";
import {
  applyWireFrame,
  appendedText,
  emptySnapshot,
  snapshotBody,
  snapshotFromBody,
  MEDIA_TYPE_DELTA_WIRE,
  type ReducedSnapshot,
} from "./reducer.js";
import {
  MessageStreamReader,
  defaultStreamDialer,
  type StreamDialer,
  type StreamOptions,
  type StreamResponse,
} from "./sse-stream.js";
import type {
  Conversation,
  ConversationPage,
  CreateConversationInput,
  IdentitySendInput,
  ListConversationsOptions,
  ListOptions,
  Logger,
  Message,
  MessageEnvelope,
  MessagePage,
  MessageStreamOptions,
  Presence,
  SendInput,
  StartStreamInput,
  TokenProvider,
  WaitInput,
} from "./types.js";
import {
  MessageStream,
  postStreamingEnvelope,
  rawToEnvelope,
  type MessageStreamTransport,
} from "./stream.js";

export interface MessageClientOptions {
  /**
   * Base URL of the Message Service (e.g. `https://msg.beeos.ai`).
   *
   * Optional at construction time. When absent, the client enters
   * **delayed-connect** mode: every REST request awaits an internal
   * `configReady` promise until `setConfig({ serviceUrl, ... })` is
   * called. This lets agent runtimes (beeos-claw, device-agent, …)
   * build the client at process start and wire the actual MS
   * endpoint in once their bootstrap handshake resolves.
   */
  serviceUrl?: string;

  /**
   * Optional Centrifugo WebSocket URL. Only needed for realtime
   * (`connect()`). When omitted, the SDK reads it from the
   * `tokenProvider` response.
   */
  centrifugoUrl?: string;

  /** Service-to-service API key (X-API-Key). Mutually exclusive with `tokenProvider`. */
  apiKey?: string;

  /**
   * Async function returning a fresh Centrifugo JWT + URL. Required
   * for identity-bound REST + realtime auth (agent / user clients).
   * Centrifuge's internal refresh loop calls this; the SDK also
   * caches the latest JWT and forwards it as `Authorization: Bearer`
   * on REST.
   */
  tokenProvider?: TokenProvider;

  /**
   * Service-to-service `Sender-Identity` header. When the caller has
   * an `apiKey` and wants to send-as another identity, set this.
   * Token-mode callers leave this unset — the server derives sender
   * from the token's `sub`.
   */
  senderIdentity?: string;

  /** Per-request timeout in ms. Default 30000. */
  requestTimeoutMs?: number;

  /** Logger (pino, console, …). Optional. */
  logger?: Logger;
}

export interface RequestOptions {
  /** Override the auth header for this single request. */
  token?: string;
  /** Override sender for this single s2s request. */
  sender?: string;
  /** Override default request timeout. */
  timeoutMs?: number;
}

export interface ConnectOptions {
  /**
   * Pin the identity id this client will operate as. Forwarded to the
   * token provider on first fetch. When omitted, the SDK uses the
   * `identityId` field on the token response.
   */
  identityId?: string;
}

// Typed event signatures.
export interface MessageClientEvents {
  message: (msg: Message) => void;
  error: (err: Error) => void;
  connect: () => void;
  disconnect: (info: { code: number; reason: string }) => void;
}

export class MessageClient extends EventEmitter {
  readonly conversations: ConversationsAPI;
  readonly messages: MessagesAPI;
  readonly identities: IdentitiesAPI;

  private opts: MessageClientOptions;
  private logger: Logger;

  // Realtime state
  private centrifuge: Centrifuge | null = null;
  private identityId = "";
  private cachedToken: string | null = null;
  private serviceUrl: string;
  private tokenRefreshCount = 0;

  /**
   * Resolves once `serviceUrl` AND one of `{apiKey, tokenProvider}` are
   * available. All REST requests await this so callers can construct
   * the client BEFORE the MS bootstrap handshake has resolved (plan
   * `sink-streaming-to-sdk`, replaces the standalone
   * `createLazyMessageStreamFactory` in beeos-claw).
   */
  private configReady: Promise<void>;
  private resolveConfigReady: () => void = () => {
    /* assigned below */
  };

  constructor(opts: Partial<MessageClientOptions> = {}) {
    super();
    if (opts.apiKey && opts.tokenProvider) {
      throw new Error("message-sdk: apiKey and tokenProvider are mutually exclusive");
    }
    this.opts = { ...opts } as MessageClientOptions;
    this.serviceUrl = opts.serviceUrl ?? "";
    this.logger = opts.logger ?? {};
    this.conversations = new ConversationsAPI(this);
    this.messages = new MessagesAPI(this);
    this.identities = new IdentitiesAPI(this);

    this.configReady = new Promise<void>((resolve) => {
      this.resolveConfigReady = resolve;
    });
    if (this.isConfigured()) this.resolveConfigReady();
  }

  /**
   * Populate (or complete) the client's runtime config after
   * construction. Used by agent runtimes that build the client at
   * process start and learn the MS endpoint / auth later (e.g.
   * beeos-claw fetches a Centrifugo token via Agent Gateway before
   * the MS URL is known).
   *
   * Subsequent calls may add MISSING fields. Calling setConfig with
   * a value that conflicts with an already-set field throws — the
   * SDK does not support hot-swapping serviceUrl / apiKey /
   * tokenProvider mid-flight.
   */
  setConfig(cfg: Partial<MessageClientOptions>): void {
    if (cfg.serviceUrl !== undefined) {
      if (this.serviceUrl && this.serviceUrl !== cfg.serviceUrl) {
        throw new Error(
          "message-sdk: setConfig cannot change serviceUrl once set",
        );
      }
      this.serviceUrl = cfg.serviceUrl;
    }
    if (cfg.apiKey !== undefined) {
      if (this.opts.apiKey && this.opts.apiKey !== cfg.apiKey) {
        throw new Error(
          "message-sdk: setConfig cannot change apiKey once set",
        );
      }
      this.opts.apiKey = cfg.apiKey;
    }
    if (cfg.tokenProvider !== undefined) {
      if (this.opts.tokenProvider && this.opts.tokenProvider !== cfg.tokenProvider) {
        throw new Error(
          "message-sdk: setConfig cannot change tokenProvider once set",
        );
      }
      this.opts.tokenProvider = cfg.tokenProvider;
    }
    if (cfg.centrifugoUrl !== undefined) {
      if (this.opts.centrifugoUrl && this.opts.centrifugoUrl !== cfg.centrifugoUrl) {
        throw new Error(
          "message-sdk: setConfig cannot change centrifugoUrl once set",
        );
      }
      this.opts.centrifugoUrl = cfg.centrifugoUrl;
    }
    if (cfg.senderIdentity !== undefined) this.opts.senderIdentity = cfg.senderIdentity;
    if (cfg.requestTimeoutMs !== undefined) this.opts.requestTimeoutMs = cfg.requestTimeoutMs;
    if (cfg.logger !== undefined) this.logger = cfg.logger;
    if (this.opts.apiKey && this.opts.tokenProvider) {
      throw new Error(
        "message-sdk: apiKey and tokenProvider are mutually exclusive",
      );
    }
    if (this.isConfigured()) this.resolveConfigReady();
  }

  private isConfigured(): boolean {
    if (!this.serviceUrl) return false;
    return Boolean(this.opts.apiKey) || Boolean(this.opts.tokenProvider);
  }

  // --- typed event overloads ---
  override on<K extends keyof MessageClientEvents>(event: K, listener: MessageClientEvents[K]): this;
  override on(event: string | symbol, listener: (...args: unknown[]) => void): this;
  override on(event: string | symbol, listener: (...args: unknown[]) => void): this {
    return super.on(event, listener);
  }

  override off<K extends keyof MessageClientEvents>(event: K, listener: MessageClientEvents[K]): this;
  override off(event: string | symbol, listener: (...args: unknown[]) => void): this;
  override off(event: string | symbol, listener: (...args: unknown[]) => void): this {
    return super.off(event, listener);
  }

  override emit<K extends keyof MessageClientEvents>(
    event: K,
    ...args: Parameters<MessageClientEvents[K]>
  ): boolean;
  override emit(event: string | symbol, ...args: unknown[]): boolean;
  override emit(event: string | symbol, ...args: unknown[]): boolean {
    return super.emit(event, ...args);
  }

  // --- Realtime ---

  /**
   * Open a Centrifugo WebSocket and auto-subscribe the caller's
   * personal channel (`personal:{identityId}`). Incoming publications
   * are translated to v2 `Message<T>` and emitted via `on("message")`.
   *
   * Requires a `tokenProvider` to be configured on the constructor.
   * Service-side (apiKey-only) callers don't have a realtime identity
   * and calling `connect()` throws.
   */
  async connect(opts?: ConnectOptions): Promise<void> {
    if (this.centrifuge) {
      await this.disconnect();
    }
    if (!this.opts.tokenProvider) {
      throw new Error(
        "message-sdk: connect() requires tokenProvider; service-side (apiKey) callers cannot use realtime",
      );
    }
    const provider = this.opts.tokenProvider;
    this.identityId = opts?.identityId ?? "";
    this.tokenRefreshCount = 0;

    // Probe once to discover identity + WSS URL when not pinned.
    let wssUrl = this.opts.centrifugoUrl ?? "";
    if (!wssUrl || !this.identityId) {
      const probe = await provider(this.identityId);
      this.cachedToken = probe.token;
      if (probe.serviceUrl) this.serviceUrl = probe.serviceUrl;
      if (!this.identityId) this.identityId = probe.identityId;
      if (!wssUrl) wssUrl = probe.centrifugoUrl;
    }
    if (!wssUrl) {
      throw new Error(
        "message-sdk: no Centrifugo URL — set centrifugoUrl on options or have tokenProvider return centrifugoUrl",
      );
    }

    const channel = `personal:${this.identityId}`;
    this.logger.info?.(
      `[message-sdk] connecting identity=${this.identityId} url=${wssUrl}`,
    );

    const getToken = async (): Promise<string> => {
      this.tokenRefreshCount++;
      this.logger.debug?.(
        `[message-sdk] token fetch #${this.tokenRefreshCount} identity=${this.identityId}`,
      );
      const resp = await provider(this.identityId);
      this.cachedToken = resp.token;
      if (resp.serviceUrl) this.serviceUrl = resp.serviceUrl;
      return resp.token;
    };

    const cent = new Centrifuge(wssUrl, {
      getToken,
      websocket: WebSocket as unknown as typeof globalThis.WebSocket,
    });

    cent.on("connecting", (ctx) => {
      this.logger.debug?.(
        `[message-sdk] connecting code=${ctx.code} reason=${ctx.reason}`,
      );
    });
    cent.on("connected", () => {
      this.logger.info?.(`[message-sdk] connected identity=${this.identityId}`);
      this.emit("connect");
    });
    cent.on("disconnected", (ctx) => {
      this.logger.warn?.(
        `[message-sdk] disconnected code=${ctx.code} reason=${ctx.reason}`,
      );
      this.emit("disconnect", { code: ctx.code, reason: ctx.reason });
    });
    cent.on("error", (ctx) => {
      this.logger.error?.("[message-sdk] error:", ctx.error);
      this.emit("error", ctx.error instanceof Error ? ctx.error : new Error(String(ctx.error)));
    });
    cent.on("subscribed", (ctx: SubscribedContext) => {
      if (ctx.channel === channel) {
        this.logger.info?.(
          `[message-sdk] subscribed personal channel=${channel} recoverable=${ctx.recoverable} recovered=${ctx.recovered}`,
        );
      }
    });
    cent.on("publication", (ctx: ServerPublicationContext) => {
      if (ctx.channel === channel) {
        this.dispatchPublication(ctx.data);
      }
    });

    this.centrifuge = cent;
    cent.connect();
  }

  /** Tear down the Centrifugo connection. Safe to call when not connected. */
  async disconnect(): Promise<void> {
    if (this.centrifuge) {
      this.centrifuge.disconnect();
      this.centrifuge = null;
    }
    this.cachedToken = null;
  }

  get isConnected(): boolean {
    return this.centrifuge?.state === "connected";
  }

  get currentIdentityId(): string {
    return this.identityId;
  }

  /** @internal — used by ConversationSubscription. */
  get _centrifuge(): Centrifuge | null {
    return this.centrifuge;
  }

  private dispatchPublication(data: unknown): void {
    if (!data || typeof data !== "object") return;
    try {
      const msg = envelopeToMessage(data as WireEnvelope);
      this.emit("message", msg);
    } catch (err) {
      this.logger.error?.("[message-sdk] dispatch failed:", err);
      this.emit("error", err instanceof Error ? err : new Error(String(err)));
    }
  }

  // --- REST plumbing (used by API namespaces) ---

  /** @internal */
  async request<T>(
    method: string,
    path: string,
    body?: unknown,
    extraHeaders?: Record<string, string>,
    reqOpts?: RequestOptions,
  ): Promise<T> {
    // In delayed-connect mode, block this REST call until setConfig
    // has been called with at least serviceUrl + (apiKey | tokenProvider).
    // Pre-configured clients hit a resolved promise and proceed
    // immediately (zero added latency).
    await this.configReady;
    const url = new URL(path, this.serviceUrl).toString();
    const headers: Record<string, string> = {
      Accept: "application/json",
      ...extraHeaders,
    };

    // Auth precedence:
    //   1. per-request token override (RequestOptions.token)
    //   2. cachedToken (populated by tokenProvider during connect / refresh)
    //   3. options.apiKey (X-API-Key)
    const reqToken = reqOpts?.token ?? this.cachedToken;
    if (reqToken) {
      headers["Authorization"] = `Bearer ${reqToken}`;
    } else if (this.opts.apiKey) {
      headers["X-API-Key"] = this.opts.apiKey;
    }

    const sender = reqOpts?.sender ?? this.opts.senderIdentity;
    if (sender) headers["Sender-Identity"] = sender;

    let payload: BodyInit | undefined;
    if (body !== undefined && body !== null) {
      headers["Content-Type"] = "application/json";
      payload = JSON.stringify(body);
    }

    const timeoutMs =
      reqOpts?.timeoutMs ?? this.opts.requestTimeoutMs ?? 30_000;
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const resp = await fetch(url, {
        method,
        headers,
        body: payload,
        signal: ac.signal,
      });
      if (!resp.ok) {
        let data: unknown;
        try {
          data = await resp.json();
        } catch {
          /* body may not be JSON */
        }
        throw mapHttpError(resp.status, data);
      }
      if (resp.status === 204) return undefined as T;
      const text = await resp.text();
      if (!text) return undefined as T;
      return JSON.parse(text) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * @internal — open an SSE stream connection for `ConversationsAPI.stream`.
   * Awaits delayed-connect config, builds the URL + auth headers (opting
   * into the ADR-0025 delta wire), and hands off to the dialer. No
   * per-request timeout: the SSE connection is long-lived and torn down
   * via the AbortSignal, not a deadline.
   */
  async _openSSE(
    path: string,
    opts: { since?: number; token?: string; signal: AbortSignal },
    dialer: StreamDialer = defaultStreamDialer,
  ): Promise<StreamResponse> {
    await this.configReady;
    const u = new URL(path, this.serviceUrl);
    if (opts.since && opts.since > 0) u.searchParams.set("since", String(opts.since));
    const headers: Record<string, string> = {
      Accept: `text/event-stream, ${MEDIA_TYPE_DELTA_WIRE}`,
    };
    const reqToken = opts.token ?? this.cachedToken;
    if (reqToken) {
      headers["Authorization"] = `Bearer ${reqToken}`;
    } else if (this.opts.apiKey) {
      headers["X-API-Key"] = this.opts.apiKey;
    }
    return dialer(u.toString(), headers, opts.signal);
  }
}

// ============================================================================
// REST namespaces
// ============================================================================

export class ConversationsAPI {
  constructor(private client: MessageClient) {}

  async create(
    input: CreateConversationInput,
    opts?: RequestOptions,
  ): Promise<Conversation> {
    const body: Record<string, unknown> = {
      participants: input.participants,
    };
    if (input.metadata) body.metadata = input.metadata;
    if (input.singleShot !== undefined) body.single_shot = input.singleShot;
    if (input.deadlineMs !== undefined) body.deadline_ms = input.deadlineMs;
    return this.client.request<Conversation>(
      "POST",
      "/api/v2/conversations",
      body,
      undefined,
      opts,
    );
  }

  async get(conversationId: string, opts?: RequestOptions): Promise<Conversation> {
    return this.client.request<Conversation>(
      "GET",
      `/api/v2/conversations/${encodeURIComponent(conversationId)}`,
      undefined,
      undefined,
      opts,
    );
  }

  async close(
    conversationId: string,
    reason?: string,
    opts?: RequestOptions,
  ): Promise<void> {
    const path = reason
      ? `/api/v2/conversations/${encodeURIComponent(conversationId)}?reason=${encodeURIComponent(reason)}`
      : `/api/v2/conversations/${encodeURIComponent(conversationId)}`;
    return this.client.request<void>("DELETE", path, undefined, undefined, opts);
  }

  async wait<TContent = unknown>(
    input: WaitInput,
    opts?: RequestOptions,
  ): Promise<Message<TContent>> {
    const body: Record<string, unknown> = {
      reply_to: input.replyTo,
    };
    if (input.timeoutMs !== undefined) body.timeout_ms = input.timeoutMs;
    if (input.expectTypes && input.expectTypes.length > 0) {
      body.expect_types = input.expectTypes;
    }
    const raw = await this.client.request<Record<string, unknown>>(
      "POST",
      `/api/v2/conversations/${encodeURIComponent(input.conversationId)}/wait`,
      body,
      undefined,
      opts,
    );
    return snakeMessageToCamel<TContent>(raw);
  }

  /**
   * Subscribe to a conversation channel (`ch:{conversationId}`) via the
   * realtime transport. Requires `connect()` to have been called.
   * Returns a `ConversationSubscription` handle whose `on("message")`
   * emits v2 `Message<T>` objects.
   */
  async subscribe(conversationId: string): Promise<ConversationSubscription> {
    const cent = this.client._centrifuge;
    if (!cent) {
      throw new Error(
        "message-sdk: subscribe() requires connect() — no active realtime client",
      );
    }
    // Pass a snapshot fetcher so the subscription can rebase via REST
    // when the delta wire signals a gap (ADR-0025).
    return new ConversationSubscription(cent, conversationId, (messageId) =>
      this.client.messages.getEnvelope(conversationId, messageId).catch(() => null),
    );
  }

  /**
   * Consume a conversation's message stream over HTTP SSE
   * (`GET /api/v2/conversations/{id}/stream`). Unlike `subscribe()`
   * (which needs a live Centrifugo WSS connection via `connect()`),
   * this works for service-side (API-key) callers and folds the
   * ADR-0025 delta wire so every emitted Message carries the full
   * cumulative `body` plus the incremental `bodyDelta`.
   *
   * Returns a `MessageStreamReader` — `for await (const msg of reader)`.
   * Reconnect is caller-driven: on a clean close, re-open with
   * `stream(id, { since: reader.lastCursor() })` to resume losslessly.
   */
  stream(conversationId: string, opts: StreamOptions = {}): MessageStreamReader {
    const dialer = opts.dialer;
    const connect = (since: number, signal: AbortSignal): Promise<StreamResponse> =>
      this.client._openSSE(
        `/api/v2/conversations/${encodeURIComponent(conversationId)}/stream`,
        { since, token: opts.token, signal },
        dialer,
      );
    const rebase = (messageId: string): Promise<MessageEnvelope | null> =>
      this.client.messages
        .getEnvelope(conversationId, messageId, opts.token ? { token: opts.token } : undefined)
        .catch(() => null);
    return new MessageStreamReader({
      connect,
      rebase,
      since: opts.since,
      signal: opts.signal,
      highWaterMark: opts.highWaterMark,
    });
  }
}

export class MessagesAPI {
  constructor(private client: MessageClient) {}

  async send<TContent = unknown>(
    input: SendInput<TContent>,
    opts?: RequestOptions,
  ): Promise<Message<TContent>> {
    const headers: Record<string, string> = {};
    if (input.id) headers["Idempotency-Key"] = input.id;
    const body: Record<string, unknown> = {
      type: input.type,
      content: input.content,
    };
    if (input.replyTo) body.reply_to = input.replyTo;
    if (input.requireSubscriber !== undefined) {
      body.require_subscriber = input.requireSubscriber;
    }
    const raw = await this.client.request<Record<string, unknown>>(
      "POST",
      `/api/v2/conversations/${encodeURIComponent(input.conversationId)}/messages`,
      body,
      headers,
      opts,
    );
    return snakeMessageToCamel<TContent>(raw);
  }

  /**
   * Send a message, then block until the first reply arrives. Equivalent to
   *   const sent = await messages.send(input);
   *   return conversations.wait({ conversationId, replyTo: sent.id, timeoutMs });
   * Callers SHOULD set `input.id` for safe retries.
   */
  async sendAndWait<TContent = unknown, TReply = unknown>(
    input: SendInput<TContent>,
    timeoutMs?: number,
    opts?: RequestOptions,
  ): Promise<Message<TReply>> {
    const sent = await this.send(input, opts);
    return (this.client as unknown as { conversations: ConversationsAPI }).conversations.wait<TReply>(
      { conversationId: input.conversationId, replyTo: sent.id, timeoutMs },
      opts,
    );
  }

  async list<TContent = unknown>(
    conversationId: string,
    lo?: ListOptions,
    opts?: RequestOptions,
  ): Promise<MessagePage<TContent>> {
    const params = new URLSearchParams();
    if (lo?.cursor) params.set("cursor", lo.cursor);
    if (lo?.limit) params.set("limit", String(lo.limit));
    const qs = params.toString();
    const path = qs
      ? `/api/v2/conversations/${encodeURIComponent(conversationId)}/messages?${qs}`
      : `/api/v2/conversations/${encodeURIComponent(conversationId)}/messages`;
    const raw = await this.client.request<{
      messages: Array<Record<string, unknown>>;
      next_cursor?: string;
      has_more?: boolean;
    }>("GET", path, undefined, undefined, opts);
    return {
      messages: raw.messages.map((m) => snakeMessageToCamel<TContent>(m)),
      nextCursor: raw.next_cursor,
      hasMore: raw.has_more ?? false,
    };
  }

  // -------------------------------------------------------------
  // v3 — Envelope v3 streaming API.
  //
  // Backed by `POST/PATCH/GET /api/v3/conversations/{cid}/messages`
  // on the Message Service. The returned `MessageStream` ships each
  // append as an immediate `{ body_append, body_from }` PATCH (no
  // buffer, no debounce timer — ADR-0025 append-only delta wire); see
  // `stream.ts`.
  // -------------------------------------------------------------

  /**
   * Open a streaming v3 message envelope. Returns a `MessageStream`
   * **synchronously** — the underlying POST runs in the background and
   * is exposed via `stream.opened()`. Each `appendBody` ships a
   * `body_append` delta PATCH immediately (coalesced only while a PATCH
   * is in flight); appends issued before the POST resolves ride out on
   * the first PATCH after open. `setBody` / `finalize` ship the full
   * cumulative body as a snapshot replace.
   *
   *   const s = client.messages.startStream({
   *     conversationId,
   *     id: crypto.randomUUID(),
   *     replyTo: incoming.id,
   *   });
   *   s.appendBody("Hello ");          // → body_append PATCH
   *   s.appendBody("world");           // → body_append PATCH (or coalesced)
   *   await s.finalize({ stopReason: "end_turn" });
   *
   * If the open POST fails (network / 5xx / 4xx), the terminal call
   * automatically falls back to a single-shot `sendV3` POST so
   * consumers still see a terminal envelope row.
   *
   * `input.id` is RECOMMENDED — when present it's forwarded as the
   * `Idempotency-Key` HTTP header and used as the local `stream.id`.
   */
  startStream(
    input: StartStreamInput,
    streamOpts?: MessageStreamOptions,
    opts?: RequestOptions,
  ): MessageStream {
    const transport: MessageStreamTransport = {
      request: (method, path, body, extraHeaders, reqOpts) =>
        this.client.request(method, path, body, extraHeaders, reqOpts),
    };
    const openPromise = postStreamingEnvelope(transport, input, opts);
    return new MessageStream(transport, input, streamOpts, opts, openPromise);
  }

  /**
   * Post a v3 envelope in a single shot — terminal state goes in on
   * the very first POST, no subsequent PATCHes. Useful as a fallback
   * when the streaming open POST failed transiently (plan 1.5) and
   * the caller wants to land SOMETHING in MS so polls / SSE replays
   * see a terminal row.
   *
   * Functionally equivalent to `startStream(input)` followed by
   * `setBody(input.body) + finalize({ stop_reason })`, but with a
   * single HTTP roundtrip and no in-memory MessageStream object.
   * The caller picks `state` (typically `completed` / `failed` /
   * `refused` / `cancelled`) — the underlying MS handler accepts
   * the row directly on create.
   */
  async sendV3(
    input: StartStreamInput,
    opts?: RequestOptions,
  ): Promise<MessageEnvelope> {
    const transport: MessageStreamTransport = {
      request: (method, path, body, extraHeaders, reqOpts) =>
        this.client.request(method, path, body, extraHeaders, reqOpts),
    };
    return postStreamingEnvelope(transport, input, opts);
  }

  /**
   * Fetch a v3 envelope snapshot. Useful for SSE reconnect (caller
   * GETs to backfill the latest body+parts+state before resuming
   * subscription).
   */
  async getEnvelope(
    conversationId: string,
    messageId: string,
    opts?: RequestOptions,
  ): Promise<MessageEnvelope> {
    const raw = await this.client.request<Record<string, unknown>>(
      "GET",
      `/api/v3/conversations/${encodeURIComponent(conversationId)}/messages/${encodeURIComponent(messageId)}`,
      undefined,
      undefined,
      opts,
    );
    return rawToEnvelope(raw ?? {});
  }
}

export class IdentitiesAPI {
  constructor(private client: MessageClient) {}

  /**
   * Send a fire-and-forget message to an identity's personal channel.
   * Useful for service-side fan-out (notifications, outbox relay) and
   * cross-agent reply where no conversation exists yet.
   */
  async send<TContent = unknown>(
    input: IdentitySendInput<TContent>,
    opts?: RequestOptions,
  ): Promise<{ status: string }> {
    const headers: Record<string, string> = {};
    if (input.id) headers["Idempotency-Key"] = input.id;
    const body: Record<string, unknown> = {
      type: input.type,
      content: input.content,
    };
    if (input.requireSubscriber !== undefined) {
      body.require_subscriber = input.requireSubscriber;
    }
    return this.client.request<{ status: string }>(
      "POST",
      `/api/v2/identities/${encodeURIComponent(input.identityId)}/messages`,
      body,
      headers,
      opts,
    );
  }

  async presence(identityId: string, opts?: RequestOptions): Promise<Presence> {
    return this.client.request<Presence>(
      "GET",
      `/api/v2/identities/${encodeURIComponent(identityId)}/presence`,
      undefined,
      undefined,
      opts,
    );
  }

  async conversations(
    identityId: string,
    lo?: ListConversationsOptions,
    opts?: RequestOptions,
  ): Promise<ConversationPage> {
    const params = new URLSearchParams();
    if (lo?.state) params.set("state", lo.state);
    if (lo?.limit) params.set("limit", String(lo.limit));
    if (lo?.cursor) params.set("cursor", lo.cursor);
    const qs = params.toString();
    const path = qs
      ? `/api/v2/identities/${encodeURIComponent(identityId)}/conversations?${qs}`
      : `/api/v2/identities/${encodeURIComponent(identityId)}/conversations`;
    const raw = await this.client.request<{
      conversations: Conversation[];
      next_cursor?: string;
      has_more?: boolean;
    }>("GET", path, undefined, undefined, opts);
    return {
      conversations: raw.conversations,
      nextCursor: raw.next_cursor,
      hasMore: raw.has_more ?? false,
    };
  }
}

// ============================================================================
// Conversation subscription handle
// ============================================================================

export interface ConversationSubscriptionEvents {
  message: (msg: Message) => void;
  error: (err: Error) => void;
  subscribed: () => void;
  unsubscribed: () => void;
}

/**
 * Fetches the authoritative full snapshot of a message, used to rebase
 * after a delta-wire gap. Returns null when the row can't be fetched.
 */
type SnapshotFetcher = (messageId: string) => Promise<MessageEnvelope | null>;

const TERMINAL_STATES = new Set(["completed", "failed", "refused", "cancelled"]);

export class ConversationSubscription extends EventEmitter {
  private sub: ReturnType<Centrifuge["newSubscription"]>;
  private closed = false;

  /**
   * Per-message delta-wire reducer state (ADR-0025), keyed by
   * message_id. Folds append-only {body_from, body_chunk} frames back
   * into a cumulative body so consumers always see the full text plus
   * the per-frame `bodyDelta`. When MS broadcasts full snapshots
   * (flag off / terminal frames) the reducer simply adopts them.
   */
  private reducers = new Map<string, ReducedSnapshot>();

  /**
   * Per-message guard against a rebase storm: while a snapshot refetch
   * is in flight for a message, the reducer still has no base, so every
   * subsequent delta for that id would re-trigger "rebase" and fire a
   * redundant GET. We record the id here when a fetch starts and clear
   * it when the fetch settles (the reducer is re-seeded by then), so at
   * most one snapshot fetch per message is in flight at a time.
   */
  private rebaseInFlight = new Set<string>();

  constructor(
    centrifuge: Centrifuge,
    public readonly conversationId: string,
    private readonly fetchSnapshot?: SnapshotFetcher,
  ) {
    super();
    this.sub = centrifuge.newSubscription(`ch:${conversationId}`);
    this.sub.on("subscribed", (ctx: SubscribedContext) => {
      // ADR-0025 reconnect realign: when Centrifugo could NOT recover
      // the missed publications (recovered === false), any partially
      // folded body is now stale — drop it so subsequent snapshot /
      // terminal frames (and rebase refetches) re-establish the base.
      if (ctx && ctx.recovered === false) {
        this.reducers.clear();
        this.rebaseInFlight.clear();
      }
      this.emit("subscribed");
    });
    this.sub.on("unsubscribed", () => this.emit("unsubscribed"));
    this.sub.on("publication", (ctx: PublicationContext) => {
      try {
        this.handlePublication(ctx.data as WireEnvelope);
      } catch (err) {
        this.emit("error", err instanceof Error ? err : new Error(String(err)));
      }
    });
    this.sub.subscribe();
  }

  private handlePublication(env: WireEnvelope): void {
    const id = env.message_id ?? "";
    // No message id (legacy / control) → pass through unfolded.
    if (!id) {
      this.emit("message", envelopeToMessage(env));
      return;
    }

    const prev = this.reducers.get(id) ?? emptySnapshot();
    const { snapshot, result } = applyWireFrame(prev, env);

    if (result === "rebase") {
      // Missed a chunk (uncovered reconnect / edge drop). Refetch the
      // authoritative snapshot and re-emit; the terminal frame would
      // also correct it, but rebasing now keeps the typewriter live.
      // Coalesce: skip if a fetch for this id is already in flight, else
      // every queued delta would fire its own redundant GET.
      if (this.fetchSnapshot && !this.rebaseInFlight.has(id)) {
        this.rebaseInFlight.add(id);
        this.fetchSnapshot(id)
          .then((snap) => {
            if (!snap || this.closed) return;
            this.reducers.set(
              id,
              snapshotFromBody(snap.body, snap.parts, snap.state, snap.stopReason),
            );
            const msg = envelopeToMessage(env);
            msg.body = snap.body;
            msg.parts = snap.parts;
            msg.state = snap.state;
            msg.stopReason = snap.stopReason;
            msg.bodyDelta = "";
            this.emit("message", msg);
          })
          .catch((e) => this.emit("error", e instanceof Error ? e : new Error(String(e))))
          .finally(() => {
            this.rebaseInFlight.delete(id);
          });
      }
      return;
    }

    if (result === "ignored") {
      this.emit("message", envelopeToMessage(env));
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

    if (snapshot.state && TERMINAL_STATES.has(snapshot.state)) {
      this.reducers.delete(id);
    }
    this.emit("message", msg);
  }

  override on<K extends keyof ConversationSubscriptionEvents>(
    event: K,
    listener: ConversationSubscriptionEvents[K],
  ): this;
  override on(event: string | symbol, listener: (...args: unknown[]) => void): this;
  override on(event: string | symbol, listener: (...args: unknown[]) => void): this {
    return super.on(event, listener);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.reducers.clear();
    this.rebaseInFlight.clear();
    this.sub.unsubscribe();
    this.removeAllListeners();
  }
}

// ============================================================================
// helpers
// ============================================================================

function snakeMessageToCamel<TContent>(raw: Record<string, unknown>): Message<TContent> {
  return {
    id: String(raw.id ?? ""),
    conversationId: String(raw.conversation_id ?? ""),
    type: String(raw.type ?? ""),
    content: (raw.content ?? null) as TContent,
    sender: String(raw.sender ?? ""),
    replyTo: typeof raw.reply_to === "string" ? raw.reply_to : undefined,
    createdAt: String(raw.created_at ?? ""),
  };
}
