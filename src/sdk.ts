/**
 * MessageSDK — Node.js client for the BeeOS Message Service IM channel-primitives.
 *
 * Originally extracted from beeos-claw (`agents/beeos-claw/src/messaging/message-sdk.ts`)
 * so multiple agent runtimes (beeos-claw, device-agent, …) can share a single
 * battle-tested implementation.
 *
 * Two transports:
 *
 *   1. Centrifugo WebSocket (real-time inbound + fire-and-forget publish)
 *      — required for receiving `personal:{principalId}` publications.
 *
 *   2. Message Service REST (durable publish, `/wait`, history, webhooks)
 *      — only available when `serviceUrl` is configured. Agents under the
 *      P6 architecture should leave this OFF and obtain Centrifugo tokens
 *      via Agent Gateway proxy instead.
 *
 * The class is logger-agnostic — pass any object exposing the optional
 * `info`/`warn`/`error`/`debug` methods (pino, console, etc.).
 */

import {
  Centrifuge,
  type Subscription,
  type PublicationContext,
  type ServerPublicationContext,
} from "centrifuge";
import WebSocket from "ws";

import type {
  ChannelMessage,
  ConnectOptions,
  ListChannelMessagesResponse,
  MessageEnvelope,
  MessageHandler,
  MessageSDKConfig,
  MessageSDKLogger,
  SendChannelMessageInput,
  SendChannelMessageResponse,
  TokenProvider,
  TokenResponse,
  WaitForReplyInput,
} from "./envelope.js";

export class MessageSDK {
  private config: MessageSDKConfig;
  private client: Centrifuge | null = null;
  private personalSub: Subscription | null = null;
  private principalId = "";
  private handlers: Map<string, MessageHandler> = new Map();
  private _resubscribeTimer: ReturnType<typeof setTimeout> | null = null;
  private _resubscribeAttempts = 0;
  private static readonly MAX_RESUBSCRIBE_ATTEMPTS = 5;
  private _reconnectCount = 0;
  private _lastDisconnectTs = 0;
  private _tokenRefreshCount = 0;
  private _onConnectedCallback: (() => void) | null = null;
  private logger: MessageSDKLogger;

  constructor(config: MessageSDKConfig, logger: MessageSDKLogger) {
    this.config = config;
    this.logger = logger;
  }

  // --- REST API (requires serviceUrl) ---

  async getToken(
    principalId: string,
    claims?: Record<string, string>,
    ttlSeconds?: number,
  ): Promise<TokenResponse> {
    const body: Record<string, unknown> = { principal_id: principalId };
    if (claims) body.claims = claims;
    if (ttlSeconds) body.ttl_seconds = ttlSeconds;
    return this.restRequest<TokenResponse>("POST", "/api/v1/tokens", body);
  }

  async send(
    targetId: string,
    messageType: string,
    payload: unknown,
  ): Promise<void> {
    await this.restRequest("POST", "/api/v1/messages/send", {
      target_id: targetId,
      message_type: messageType,
      payload,
    });
  }

  async isOnline(principalId: string): Promise<boolean> {
    const resp = await this.restRequest<{ online: boolean }>(
      "GET",
      `/api/v1/connections/${encodeURIComponent(principalId)}/online`,
    );
    return resp.online;
  }

  async registerWebhook(
    url: string,
    events: string[],
    secret?: string,
  ): Promise<{ id: string }> {
    const body: Record<string, unknown> = { url, events };
    if (secret) body.secret = secret;
    return this.restRequest<{ id: string }>("POST", "/api/v1/webhooks", body);
  }

  async removeWebhook(id: string): Promise<void> {
    await this.restRequest(
      "DELETE",
      `/api/v1/webhooks/${encodeURIComponent(id)}`,
    );
  }

  // --- Real-time connection (centrifuge-js) ---

  /**
   * Connect to Centrifugo. Token acquisition is fully delegated to the
   * Centrifuge SDK via getToken — if the token provider fails (e.g. Message
   * Service is down), Centrifuge handles retry with exponential backoff
   * internally. The caller does NOT need to implement retry logic.
   */
  async connect(principalId: string, opts?: ConnectOptions): Promise<void> {
    if (this.client) {
      this.logger.info?.(
        "[message-sdk] Disconnecting previous client before reconnect",
      );
      await this.disconnect();
    }

    this.principalId = principalId;
    this._reconnectCount = 0;
    this._lastDisconnectTs = 0;
    this._tokenRefreshCount = 0;

    const provider: TokenProvider =
      opts?.tokenProvider ?? ((pid) => this.getToken(pid));

    let wsUrl = opts?.centrifugoUrl || this.config.centrifugoUrl;

    if (!wsUrl) {
      try {
        const probe = await provider(principalId);
        wsUrl = probe.centrifugo_url;
      } catch (err) {
        this.logger.warn?.(
          `[message-sdk] Token probe failed, cannot discover Centrifugo URL: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }

    if (!wsUrl) {
      throw new Error(
        "No Centrifugo URL available. Set centrifugoUrl in config, ConnectOptions, " +
          "or ensure the token provider is reachable at startup.",
      );
    }

    const channel = `personal:${principalId}`;

    this.logger.info?.(
      `[message-sdk] Connecting as principal=${principalId} url=${wsUrl}`,
    );

    const getToken = async (): Promise<string> => {
      this._tokenRefreshCount++;
      this.logger.info?.(
        `[message-sdk] Token fetch #${this._tokenRefreshCount} for principal=${principalId}`,
      );
      const resp = await provider(principalId);
      return resp.token;
    };

    this.client = new Centrifuge(wsUrl, {
      getToken,
      websocket: WebSocket as unknown as typeof globalThis.WebSocket,
    });

    this.client.on("connecting", (ctx) => {
      this.logger.info?.(
        `[message-sdk] Connecting: code=${ctx.code} reason=${ctx.reason}`,
      );
    });

    this.client.on("connected", (ctx) => {
      if (this._reconnectCount > 0) {
        const downtime =
          this._lastDisconnectTs > 0 ? Date.now() - this._lastDisconnectTs : 0;
        this.logger.info?.(
          `[message-sdk] Reconnected (client=${ctx.client}) reconnects=${this._reconnectCount} downtime=${downtime}ms`,
        );
      } else {
        this.logger.info?.(`[message-sdk] Connected (client=${ctx.client})`);
      }

      if (this._onConnectedCallback) {
        try {
          this._onConnectedCallback();
        } catch {
          /* best-effort */
        }
        this._onConnectedCallback = null;
      }
    });

    this.client.on("disconnected", (ctx) => {
      this._reconnectCount++;
      this._lastDisconnectTs = Date.now();
      this.logger.warn?.(
        `[message-sdk] Disconnected: code=${ctx.code} reason=${ctx.reason} totalDisconnects=${this._reconnectCount}`,
      );
    });

    this.client.on("error", (ctx) => {
      this.logger.error?.("[message-sdk] Error:", ctx.error);
    });

    // Server-side subscription events — connect proxy returns
    // personal:{principalId} in the connect response, so Centrifugo
    // automatically manages the subscription. We just listen for messages.
    this.client.on("subscribed", (ctx) => {
      if (ctx.channel === channel) {
        this.logger.info?.(
          `[message-sdk] Subscribed channel=${channel} recoverable=${ctx.recoverable} recovered=${ctx.recovered}`,
        );
      }
    });
    this.client.on("subscribing", (ctx) => {
      if (ctx.channel === channel) {
        this.logger.info?.(`[message-sdk] Subscribing channel=${channel}`);
      }
    });
    this.client.on("unsubscribed", (ctx) => {
      if (ctx.channel === channel) {
        this.logger.warn?.(`[message-sdk] Unsubscribed channel=${channel}`);
      }
    });
    this.client.on("publication", (ctx: ServerPublicationContext) => {
      if (ctx.channel === channel) {
        this.dispatch(ctx.data as MessageEnvelope).catch((err) => {
          // Format err into the message string — some downstream loggers
          // (e.g. openclaw) swallow the second argument on `.error(msg, err)`,
          // which historically masked real handler failures with an empty
          // "dispatch error:" line.
          const envType =
            (ctx?.data as { type?: string } | undefined)?.type ?? "(unknown)";
          const msg = err instanceof Error ? err.message : String(err);
          this.logger.error?.(
            `[message-sdk] dispatch failed envType=${envType}: ${msg}`,
          );
        });
      }
    });

    this.client.connect();
  }

  async disconnect(): Promise<void> {
    if (this._resubscribeTimer) {
      clearTimeout(this._resubscribeTimer);
      this._resubscribeTimer = null;
    }
    this._resubscribeAttempts = 0;
    if (this.personalSub) {
      this.personalSub.unsubscribe();
      this.personalSub = null;
    }
    this.client?.disconnect();
    this.client = null;
  }

  onMessage(type: string, handler: MessageHandler): void {
    this.handlers.set(type, handler);
  }

  /**
   * Register a one-shot callback that fires when the SDK successfully
   * connects (including after Centrifuge's internal retry succeeds).
   */
  onceConnected(cb: () => void): void {
    this._onConnectedCallback = cb;
  }

  async joinGroup(
    groupId: string,
    onMessage: MessageHandler,
  ): Promise<Subscription | null> {
    if (!this.client) throw new Error("Not connected");
    const sub = this.client.newSubscription(`group:${groupId}`);
    sub.on("publication", (ctx: PublicationContext) => {
      onMessage(ctx.data as MessageEnvelope).catch((err) => {
        this.logger.error?.(
          `[message-sdk] group handler error (${groupId}):`,
          err,
        );
      });
    });
    sub.subscribe();
    return sub;
  }

  async publishToGroup(groupId: string, content: string): Promise<void> {
    if (!this.client) throw new Error("Not connected");
    await this.client.publish(`group:${groupId}`, {
      from: this.principalId,
      content,
      timestamp: Date.now(),
    });
  }

  /**
   * Publish a message directly to a channel (ch:{channelId}) via Centrifugo.
   * Fire-and-forget: does NOT persist via Message Service — callers that
   * need durability, a server-assigned message_id, or the ability to be
   * /wait'ed on must use sendChannelMessage instead.
   *
   * `opts` are generic IM primitives carried inside the envelope so live
   * subscribers (e.g. L0 Invoke waiters, SSE pumps) can correlate request
   * and reply. They are purely IM-level — the caller has zero knowledge
   * of A2A / MCP / invocation / task semantics. Fields are only set on
   * the wire when provided.
   */
  async sendToChannel(
    channelId: string,
    type: string,
    payload: unknown,
    opts?: { inReplyTo?: string; messageId?: string; idempotencyKey?: string },
  ): Promise<void> {
    const centrifugoChannel = `ch:${channelId}`;
    const envelope: MessageEnvelope = {
      type,
      payload,
      channel_id: channelId,
      metadata: {
        from: this.principalId,
        channel_id: channelId,
        timestamp: String(Date.now()),
      },
    };
    if (opts?.inReplyTo) envelope.in_reply_to = opts.inReplyTo;
    if (opts?.messageId) envelope.message_id = opts.messageId;
    if (opts?.idempotencyKey) envelope.idempotency_key = opts.idempotencyKey;

    if (this.client?.state === "connected") {
      await this.client.publish(centrifugoChannel, envelope);
      this.logger.debug?.(
        `[message-sdk] Published type=${type} to ${centrifugoChannel}`,
      );
      return;
    }

    if (this.config.serviceUrl) {
      this.logger.warn?.(
        `[message-sdk] Centrifugo not connected, falling back to REST for ch:${channelId}`,
      );
      const body: Record<string, unknown> = {
        sender_id: this.principalId,
        type,
        payload,
      };
      if (opts?.inReplyTo) body.in_reply_to = opts.inReplyTo;
      if (opts?.messageId) body.message_id = opts.messageId;
      if (opts?.idempotencyKey) body.idempotency_key = opts.idempotencyKey;
      await this.restRequest(
        "POST",
        `/api/v1/channels/${encodeURIComponent(channelId)}/messages`,
        body,
      );
      return;
    }

    throw new Error(
      `Cannot send to channel ${channelId}: no Centrifugo connection and no serviceUrl`,
    );
  }

  /**
   * Persistent publish via Message Service REST. Writes into
   * channel_messages (idempotent on message_id / idempotency_key) and
   * fans out to ch:{id} and personal:{pid}. Callers that need the
   * assigned offset, server-generated message_id, or plan to be
   * /wait'ed on from L0 / A2A MUST use this method.
   */
  async sendChannelMessage(
    channelId: string,
    input: SendChannelMessageInput,
  ): Promise<SendChannelMessageResponse> {
    if (!channelId) throw new Error("channelId is required");
    if (!input.type) throw new Error("type is required");
    const body: Record<string, unknown> = {
      sender_id: input.senderId ?? this.principalId,
      type: input.type,
      payload: input.payload,
    };
    if (!body.sender_id) {
      throw new Error("senderId required (and no principalId is known)");
    }
    if (input.messageId) body.message_id = input.messageId;
    if (input.inReplyTo) body.in_reply_to = input.inReplyTo;
    if (input.idempotencyKey) body.idempotency_key = input.idempotencyKey;
    return this.restRequest<SendChannelMessageResponse>(
      "POST",
      `/api/v1/channels/${encodeURIComponent(channelId)}/messages`,
      body,
    );
  }

  /**
   * Convenience: reply to an incoming envelope by pulling channel_id +
   * message_id off the envelope metadata. Use inside handlers attached
   * via onMessage.
   */
  async replyToMessage(
    envelope: MessageEnvelope,
    type: string,
    payload: unknown,
    opts?: { idempotencyKey?: string; messageId?: string },
  ): Promise<SendChannelMessageResponse> {
    const channelId = envelope.channel_id ?? envelope.metadata?.channel_id;
    if (!channelId) {
      throw new Error("replyToMessage: envelope has no channel_id");
    }
    const inReplyTo =
      envelope.message_id ??
      envelope.metadata?.message_id ??
      envelope.metadata?.invocation_id ?? // legacy shim: L0 invocation → reply target
      envelope.correlationId;
    if (!inReplyTo) {
      throw new Error(
        "replyToMessage: envelope has no message_id / invocation_id / correlationId",
      );
    }
    return this.sendChannelMessage(channelId, {
      type,
      payload,
      inReplyTo,
      idempotencyKey: opts?.idempotencyKey,
      messageId: opts?.messageId,
    });
  }

  /** Page historical messages in a channel. */
  async listChannelMessages(
    channelId: string,
    opts?: { since?: number; limit?: number; type?: string },
  ): Promise<ListChannelMessagesResponse> {
    if (!channelId) throw new Error("channelId is required");
    const q = new URLSearchParams();
    if (opts?.since !== undefined) q.set("since", String(opts.since));
    if (opts?.limit !== undefined) q.set("limit", String(opts.limit));
    if (opts?.type) q.set("type", opts.type);
    const qs = q.toString();
    const path = `/api/v1/channels/${encodeURIComponent(channelId)}/messages${qs ? `?${qs}` : ""}`;
    return this.restRequest<ListChannelMessagesResponse>("GET", path);
  }

  /**
   * Block until a message with in_reply_to === input.inReplyTo is
   * published into the channel, or the server-side timeout fires
   * (returns a 408). Uses the MS /wait long-poll endpoint so callers
   * don't have to spin up their own Centrifugo subscription.
   */
  async waitForReply(
    channelId: string,
    input: WaitForReplyInput,
  ): Promise<ChannelMessage> {
    if (!channelId) throw new Error("channelId is required");
    if (!input.inReplyTo) throw new Error("inReplyTo is required");
    const body: Record<string, unknown> = { in_reply_to: input.inReplyTo };
    if (input.timeoutMs !== undefined) body.timeout_ms = input.timeoutMs;
    if (input.since !== undefined) body.since = input.since;
    return this.restRequest<ChannelMessage>(
      "POST",
      `/api/v1/channels/${encodeURIComponent(channelId)}/wait`,
      body,
    );
  }

  /**
   * Publish a message to a target principal's personal channel.
   * Goes directly through the Centrifugo WSS connection — no REST hop.
   * Symmetric with ACP's sendBridgeMessage: same connection for send and receive.
   */
  async sendToPersonal(
    targetId: string,
    type: string,
    payload: unknown,
  ): Promise<void> {
    if (!this.client) throw new Error("Not connected");
    const channel = `personal:${targetId}`;
    const envelope: MessageEnvelope = {
      type,
      payload,
      metadata: {
        from: this.principalId,
        timestamp: String(Date.now()),
      },
    };
    try {
      await this.client.publish(channel, envelope);
      this.logger.debug?.(
        `[message-sdk] Published type=${type} to ${channel}`,
      );
    } catch (err) {
      this.logger.warn?.(
        `[message-sdk] Publish failed type=${type} channel=${channel}: ${err instanceof Error ? err.message : String(err)}`,
      );
      throw err;
    }
  }

  get isConnected(): boolean {
    return this.client?.state === "connected";
  }

  get currentPrincipalId(): string {
    return this.principalId;
  }

  // --- Private ---

  private async dispatch(envelope: MessageEnvelope): Promise<void> {
    const handler = this.handlers.get(envelope.type);
    if (!handler) {
      this.logger.debug?.(
        `[message-sdk] No handler for type=${envelope.type}`,
      );
      return;
    }
    await handler(envelope);
  }

  private async restRequest<T = unknown>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    if (!this.config.serviceUrl) {
      throw new Error(
        "Message Service REST API requires serviceUrl in config",
      );
    }
    const url = `${this.config.serviceUrl}${path}`;
    const headers: Record<string, string> = {};
    const init: RequestInit = { method, headers };
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      init.body = JSON.stringify(body);
    }

    const resp = await fetch(url, init);
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(
        `Message Service ${method} ${path} returned ${resp.status}: ${text}`,
      );
    }

    if (resp.status === 204) return undefined as T;
    return (await resp.json()) as T;
  }
}
