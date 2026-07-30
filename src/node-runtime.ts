import WebSocket from "ws";

import { OutcomeUnknownError } from "./errors.js";
import { CentrifugoSessionAdapter } from "./adapters/centrifugo.js";
import { createCentrifugeFactory } from "./adapters/centrifuge-factory.js";
import type {
  ConversationProjection,
  ExecuteMethodReceipt,
  MessageClientComposition,
  MessageListPage,
  MessageProjection,
  OperationProjection,
  SendMessageCommand,
  SendMessageReceipt,
  UpdateConversationCommand,
} from "./facade/contracts.js";
import type { JsonValue, RealtimeDeliveryAudience } from "./protocol/index.js";
import {
  decodeRuntimeDispatchReceipt,
  RuntimeDispatchContractError,
} from "./protocol/runtime-dispatch.js";
import {
  NodeRuntimeDeliveryPort,
  type RuntimeDeliveryAuthorityPort,
} from "./runtime-delivery.js";
import type { Logger, TokenProvider, TokenResponse } from "./types.js";
import { formatOpenClawDeliveryBoundary } from "./openclaw-delivery-observability.js";

export interface NodeMessageClientOptions {
  readonly tokenProvider: TokenProvider;
  readonly identityId: string;
  readonly serviceUrl?: string;
  readonly logger?: Logger;
  /** Explicit MS route. This factory never selects Gateway/ACP fallback. */
  readonly route?: "message-service";
  /** Node-only durable runtime execution composition. No credential reaches handlers. */
  readonly runtimeDelivery?: {
    readonly authority: RuntimeDeliveryAuthorityPort;
    readonly scopedDeliveryKey: string;
  };
}

/** Node-only infrastructure transport sharing the MessageClient credential owner. */
export interface NodeMessageServiceTransport {
  ready(): Promise<{ serviceUrl: string; identityId: string }>;
  fetch(url: string | URL, init?: RequestInit): Promise<Response>;
  fetchRuntimeChannel(
    url: string | URL,
    authority: {
      runtimeLeaseCredential: string;
      executionGrant?: string;
    },
    init?: RequestInit,
    resilience?: { retryTransientStatus?: boolean },
  ): Promise<Response>;
}

export interface NodeMessageClientComposition extends MessageClientComposition {
  /** Composition-root only. Feature code receives only MessageClient. */
  readonly nodeTransport: NodeMessageServiceTransport;
}

/**
 * Node/MS infrastructure composition. Callers pass this to the one root
 * createMessageClient factory; this helper does not create a second client.
 */
export function createNodeMessageClientComposition(
  options: NodeMessageClientOptions,
): NodeMessageClientComposition {
  if (options.route && options.route !== "message-service") {
    throw new Error("node message client requires the explicit message-service route");
  }
  const credentials = new SharedCredentials(options);
  const http = new NodeMessageHttpAdapter(credentials, options.logger);
  const nodeTransport = new SharedNodeMessageServiceTransport(
    credentials,
    options.runtimeDelivery?.scopedDeliveryKey,
  );
  const realtime = new CentrifugoSessionAdapter({
    credentials: { getCredentials: () => credentials.realtime() },
    factory: createNodeCentrifugeFactory(),
  });
  const composition: NodeMessageClientComposition = {
    conversationQuery: http,
    privateConversationDirectoryQuery: http,
    conversationCommand: http,
    messageQuery: http,
    messageCommand: http,
    messageStream: http,
    runtimeMethods: http,
    realtime,
    currentPrincipal: credentials,
    nodeTransport,
    ...(options.runtimeDelivery
      ? {
          runtimeDelivery: new NodeRuntimeDeliveryPort(
            credentials,
            options.runtimeDelivery.authority,
            options.runtimeDelivery.scopedDeliveryKey,
          ),
        }
      : {}),
  };
  return composition;
}

class SharedNodeMessageServiceTransport implements NodeMessageServiceTransport {
  constructor(
    private readonly credentials: SharedCredentials,
    private readonly scopedDeliveryKey?: string,
  ) {}

  async ready(): Promise<{ serviceUrl: string; identityId: string }> {
    const auth = await this.credentials.http();
    return { serviceUrl: auth.serviceUrl, identityId: auth.identityId };
  }

  async fetch(url: string | URL, init: RequestInit = {}): Promise<Response> {
    return await this.fetchAuthenticated(url, init, await this.credentials.http(), true);
  }

  async fetchRuntimeChannel(
    url: string | URL,
    authority: { runtimeLeaseCredential: string; executionGrant?: string },
    init: RequestInit = {},
    _resilience?: { retryTransientStatus?: boolean },
  ): Promise<Response> {
    if (!this.scopedDeliveryKey) {
      throw new Error("runtime delivery transport is unavailable on this composition");
    }
    const auth = await this.credentials.http();
    const requested = typeof url === "string" ? new URL(url) : url;
    assertSameOrigin(requested, auth.serviceUrl, "runtime authority");
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${authority.runtimeLeaseCredential}`);
    headers.set("x-runtime-delivery-key", this.scopedDeliveryKey);
    if (authority.executionGrant) {
      headers.set("x-beeos-execution-grant", authority.executionGrant);
    }
    const response = await fetch(requested, { ...init, headers, redirect: "error" });
    assertResponseOrigin(response, requested.origin);
    return response;
  }

  private async fetchAuthenticated(
    url: string | URL,
    init: RequestInit,
    auth: { token: string; serviceUrl: string; identityId: string },
    allowRefresh: boolean,
  ): Promise<Response> {
    const requested = typeof url === "string" ? new URL(url) : url;
    assertSameOrigin(requested, auth.serviceUrl, "Message Service credential");
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${auth.token}`);
    headers.set("sender-identity", auth.identityId);
    const response = await fetch(requested, { ...init, headers, redirect: "error" });
    assertResponseOrigin(response, requested.origin);
    if (response.status === 401 && allowRefresh) {
      this.credentials.invalidate(auth.token);
      return await this.fetchAuthenticated(
        requested,
        init,
        await this.credentials.http(),
        false,
      );
    }
    return response;
  }
}

class SharedCredentials {
  private identityId: string;
  private serviceUrl?: string;
  private cached?: TokenResponse;
  private inflight?: Promise<TokenResponse>;

  constructor(private readonly options: NodeMessageClientOptions) {
    this.identityId = options.identityId;
    this.serviceUrl = options.serviceUrl;
  }

  async issue(force = false): Promise<TokenResponse> {
    const now = Math.floor(Date.now() / 1_000);
    if (!force && this.cached && this.cached.expiresAt > now + 15) {
      return this.cached;
    }
    if (!this.inflight) {
      this.inflight = this.options.tokenProvider(this.identityId).then((value) => {
        if (!value.token.trim()) throw new Error("node message tokenProvider returned an empty token");
        if (!Number.isSafeInteger(value.expiresAt) || value.expiresAt <= now) {
          throw new Error("node message tokenProvider returned an expired token");
        }
        this.identityId = value.identityId || this.identityId;
        this.pinServiceUrl(value.serviceUrl);
        this.cached = value;
        return value;
      }).finally(() => {
        this.inflight = undefined;
      });
    }
    return await this.inflight;
  }

  async http(): Promise<{ token: string; serviceUrl: string; identityId: string }> {
    const value = await this.issue();
    const serviceUrl = this.serviceUrl ?? value.serviceUrl;
    if (!serviceUrl) throw new Error("node message client requires serviceUrl from options or tokenProvider");
    const identityId = value.identityId || this.identityId;
    if (!identityId) throw new Error("node message client requires an identityId");
    return { token: value.token, serviceUrl: normalizeBase(serviceUrl), identityId };
  }

  async serviceOrigin(): Promise<string> {
    return (await this.http()).serviceUrl;
  }

  async realtime(): Promise<{ token: string; realtimeUrl: string }> {
    const value = await this.issue();
    if (!value.centrifugoUrl) throw new Error("node message tokenProvider returned no realtime URL");
    return { token: value.token, realtimeUrl: value.centrifugoUrl };
  }

  currentPrincipalId(): string {
    if (!this.identityId) throw new Error("node message client has not resolved its current principal");
    return this.identityId;
  }

  invalidate(token: string): void {
    if (this.cached?.token === token) this.cached = undefined;
  }

  private pinServiceUrl(issued?: string): void {
    if (!issued) return;
    if (this.serviceUrl &&
        new URL(this.serviceUrl).origin !== new URL(issued).origin) {
      throw new Error("configured and issued Message Service origins do not match");
    }
    this.serviceUrl ??= issued;
  }
}

class NodeMessageHttpAdapter {
  private readonly generations = new Map<string, string>();

  constructor(
    private readonly credentials: SharedCredentials,
    private readonly logger?: Logger,
  ) {}

  async getConversation(id: string): Promise<ConversationProjection> {
    const raw = await this.request("GET", `/api/v2/conversations/${encodeURIComponent(id)}`);
    const value = conversation(raw);
    this.generations.set(id, value.historyGeneration);
    return value;
  }

  async listConversations(cursor?: string) {
    return this.listPrivateConversations("open", cursor);
  }

  async listPrivateConversations(state: "open" | "closed", cursor?: string) {
    const query = cursor ? `&cursor=${encodeURIComponent(cursor)}` : "";
    const auth = await this.credentials.http();
    const raw = record(await this.request(
      "GET",
      `/api/v2/identities/${encodeURIComponent(auth.identityId)}/conversations?state=${state}${query}`,
      undefined,
      undefined,
      auth,
    ));
    const values = array(raw.conversations).map(conversation);
    for (const value of values) this.generations.set(value.id, value.historyGeneration);
    return {
      conversations: values,
      nextCursor: optionalString(raw.next_cursor ?? raw.nextCursor),
      hasMore: Boolean(raw.has_more ?? raw.hasMore),
    };
  }

  async createConversation(
    command: { participants: readonly string[]; title?: string; metadata?: Readonly<Record<string, JsonValue>> },
  ): Promise<ConversationProjection> {
    void command;
    throw new Error("MS Node composition does not create conversations without a caller-owned idempotency key");
  }

  async updateConversation(command: UpdateConversationCommand): Promise<ConversationProjection> {
    const auth = await this.credentials.http();
    const raw = record(await this.request(
      "PATCH",
      `/api/v1/control/conversations/${encodeURIComponent(command.conversationId)}`,
      { ownerIdentityId: auth.identityId, title: command.title },
      command.idempotencyKey,
      auth,
    ));
    const envelope = raw.data !== undefined ? record(raw.data) : raw;
    if (requiredString(envelope.title) !== command.title) {
      throw new Error("conversation title update receipt does not match the command");
    }
    const receiptRevision = decimal(envelope.metadata_version ?? envelope.metadataVersion);
    const authoritative = await this.getConversation(command.conversationId);
    if (authoritative.title !== command.title ||
        BigInt(authoritative.revision) < BigInt(receiptRevision)) {
      throw new Error("conversation title projection did not converge to the update receipt");
    }
    return authoritative;
  }

  async clearConversation(id: string, key: string): Promise<ConversationProjection> {
    void id;
    void key;
    throw new Error("MS v2 does not expose a conversation clear route");
  }

  async deleteConversation(id: string, key: string): Promise<void> {
    await this.request("DELETE", `/api/v2/conversations/${encodeURIComponent(id)}`, undefined, key);
  }

  async listMessages(conversationId: string, cursor?: string): Promise<MessageListPage> {
    const query = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const raw = record(await this.request(
      "GET",
      `/api/v2/conversations/${encodeURIComponent(conversationId)}/messages${query}`,
    ));
    const generation = this.generations.get(conversationId);
    if (!generation) throw new Error("message list requires a preceding generation-fenced conversation read");
    return {
      messages: array(raw.messages).map(message),
      historyGeneration: decimal(raw.history_generation ?? raw.historyGeneration),
      historyBoundaryOffset: decimal(
        raw.history_boundary_offset ?? raw.historyBoundaryOffset,
      ),
      latestOffset: decimal(raw.latest_offset ?? raw.latestOffset),
      nextSince: optionalString(raw.next_cursor ?? raw.nextCursor),
      hasMore: Boolean(raw.has_more ?? raw.hasMore),
    };
  }

  async getMessage(conversationId: string, messageId: string): Promise<MessageProjection> {
    const raw = await this.request(
      "GET",
      `/api/v3/conversations/${encodeURIComponent(conversationId)}/messages/${encodeURIComponent(messageId)}`,
    );
    return message(raw);
  }

  async reconcileMessage(conversationId: string, idempotencyKey: string): Promise<MessageProjection | undefined> {
    try {
      return await this.getMessage(conversationId, idempotencyKey);
    } catch (error) {
      if (isStatus(error, 404)) return undefined;
      throw error;
    }
  }

  async sendMessage(command: SendMessageCommand): Promise<SendMessageReceipt> {
    try {
      const response = await this.requestResponse(
        "POST",
        `/api/v2/conversations/${encodeURIComponent(command.conversationId)}/messages`,
        { type: command.type, content: command.content, reply_to: command.replyTo },
        command.idempotencyKey,
      );
      const raw = record(await response.json());
      return {
        messageId: requiredString(raw.id),
        outcome: raw.idempotent ? "duplicate" : response.status === 201 ? "created" : "accepted",
        idempotent: Boolean(raw.idempotent),
        correlationId: response.headers.get("x-request-id") ?? undefined,
        ...decodeOptionalRuntimeDispatch(raw),
      };
    } catch (error) {
      if (error instanceof NodeHttpError || error instanceof RuntimeDispatchContractError) throw error;
      throw new OutcomeUnknownError({
        phase: "open",
        conversationId: command.conversationId,
        messageId: command.clientMessageId,
        idempotencyKey: command.idempotencyKey,
        cause: asError(error),
      });
    }
  }

  async cancelMessage(conversationId: string, messageId: string, key: string): Promise<void> {
    void conversationId;
    void messageId;
    void key;
    throw new Error("MS v2 does not expose a message cancel route");
  }

  async startStream(command: SendMessageCommand): Promise<SendMessageReceipt> {
    try {
      const response = await this.requestResponse(
        "POST",
        `/api/v3/conversations/${encodeURIComponent(command.conversationId)}/messages`,
        {
          id: command.clientMessageId,
          type: command.type,
          content: command.content,
          reply_to: command.replyTo,
          state: "streaming",
        },
        command.idempotencyKey,
      );
      const raw = record(await response.json());
      const outcome = raw.idempotent ? "duplicate" : response.status === 201 ? "created" : "accepted";
      this.logger?.info?.(formatOpenClawDeliveryBoundary({
        stage: "sdk_stream_open",
        status: "accepted",
        code: String(outcome),
        conversationId: command.conversationId,
        messageId: requiredString(raw.id),
      }));
      return {
        messageId: requiredString(raw.id),
        outcome,
        idempotent: Boolean(raw.idempotent),
        correlationId: response.headers.get("x-request-id") ?? undefined,
        ...decodeOptionalRuntimeDispatch(raw),
      };
    } catch (error) {
      this.logger?.warn?.(formatOpenClawDeliveryBoundary({
        stage: "sdk_stream_open",
        status: "failed",
        code: error instanceof NodeHttpError ? `http_${error.status}` : "outcome_unknown",
        conversationId: command.conversationId,
        messageId: command.clientMessageId,
      }));
      if (error instanceof NodeHttpError || error instanceof RuntimeDispatchContractError) throw error;
      throw new OutcomeUnknownError({
        phase: "open", conversationId: command.conversationId,
        messageId: command.clientMessageId, idempotencyKey: command.idempotencyKey, cause: asError(error),
      });
    }
  }

  append(
    conversationId: string,
    messageId: string,
    bodyAppend: string,
    bodyFrom: number,
    key: string,
  ): Promise<void> {
    return this.patchMessage(
      conversationId,
      messageId,
      { body_append: bodyAppend, body_from: bodyFrom },
      key,
      "sdk_delta_append",
    );
  }

  setBody(conversationId: string, messageId: string, body: string, key: string): Promise<void> {
    return this.patchMessage(conversationId, messageId, { body }, key, "sdk_delta_append");
  }

  setParts(conversationId: string, messageId: string, parts: readonly JsonValue[], key: string): Promise<void> {
    return this.patchMessage(conversationId, messageId, { parts }, key, "sdk_delta_append");
  }

  finalize(
    conversationId: string,
    messageId: string,
    state: "completed" | "failed" | "refused" | "cancelled",
    key: string,
    stopReason?: string,
  ): Promise<void> {
    return this.patchMessage(
      conversationId,
      messageId,
      { state, stop_reason: stopReason },
      key,
      "sdk_terminal_finalize",
      state,
    );
  }

  async executeMethod(): Promise<ExecuteMethodReceipt> {
    throw new Error("runtime methods are not available on the explicit message-service route");
  }
  async listActiveOperations(): Promise<never> {
    throw new Error("runtime methods are not available on the explicit message-service route");
  }
  async getOperation(): Promise<OperationProjection> {
    throw new Error("runtime methods are not available on the explicit message-service route");
  }
  async cancelOperation(): Promise<OperationProjection> {
    throw new Error("runtime methods are not available on the explicit message-service route");
  }

  private patchMessage(
    conversationId: string,
    messageId: string,
    body: unknown,
    key: string,
    stage: "sdk_delta_append" | "sdk_terminal_finalize",
    reason?: string,
  ): Promise<void> {
    return this.request(
      "PATCH",
      `/api/v3/conversations/${encodeURIComponent(conversationId)}/messages/${encodeURIComponent(messageId)}`,
      body,
      key,
    ).then(
      () => {
        this.logger?.info?.(formatOpenClawDeliveryBoundary({
          stage, status: "accepted", reason, conversationId, messageId,
        }));
      },
      (error: unknown) => {
        this.logger?.warn?.(formatOpenClawDeliveryBoundary({
          stage, status: "failed", reason,
          code: error instanceof NodeHttpError ? `http_${error.status}` : "outcome_unknown",
          conversationId, messageId,
        }));
        throw error;
      },
    );
  }

  private async request(
    method: string,
    path: string,
    body?: unknown,
    key?: string,
    auth?: { token: string; serviceUrl: string },
  ): Promise<unknown> {
    const response = await this.requestResponse(method, path, body, key, auth);
    if (response.status === 204) return undefined;
    return response.json();
  }

  private async requestResponse(
    method: string,
    path: string,
    body?: unknown,
    key?: string,
    suppliedAuth?: { token: string; serviceUrl: string },
  ): Promise<Response> {
    const auth = suppliedAuth ?? await this.credentials.http();
    return await this.requestWithAuth(method, path, body, key, auth, true);
  }

  private async requestWithAuth(
    method: string,
    path: string,
    body: unknown,
    key: string | undefined,
    auth: { token: string; serviceUrl: string },
    allowRefresh: boolean,
  ): Promise<Response> {
    const headers: Record<string, string> = {
      Accept: "application/json",
      Authorization: `Bearer ${auth.token}`,
    };
    if (key) headers["Idempotency-Key"] = key;
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const response = await fetch(`${auth.serviceUrl}${path}`, {
      method,
      headers,
      redirect: "error",
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (response.url && new URL(response.url).origin !== new URL(auth.serviceUrl).origin) {
      throw new Error("Message Service authenticated response origin changed");
    }
    if (response.status === 401 && allowRefresh) {
      this.credentials.invalidate(auth.token);
      const refreshed = await this.credentials.http();
      return await this.requestWithAuth(method, path, body, key, refreshed, false);
    }
    if (!response.ok) throw new NodeHttpError(response.status);
    return response;
  }
}

/** @internal Exported only for transport contract tests; not a package subpath. */
export function createNodeCentrifugeFactory() {
  return createCentrifugeFactory({
    websocket: WebSocket as unknown as typeof globalThis.WebSocket,
  });
}

class NodeHttpError extends Error {
  constructor(readonly status: number) {
    super(`message service request failed with HTTP ${status}`);
  }
}

function conversation(value: unknown): ConversationProjection {
  const raw = record(value);
  return {
    id: requiredString(raw.id),
    title: optionalString(raw.title),
    state: raw.state === "closed" ? "closed" : "open",
    historyGeneration: decimal(raw.history_generation ?? raw.historyGeneration),
    revision: decimal(raw.metadata_version ?? raw.metadataVersion),
    lastMessageId: optionalString(raw.last_message_id ?? raw.lastMessageId),
    lastActivityAt: optionalString(raw.last_activity_at ?? raw.lastActivityAt),
    updatedAt: optionalString(raw.updated_at ?? raw.updatedAt ?? raw.created_at ?? raw.createdAt) ?? new Date(0).toISOString(),
  };
}

function message(value: unknown): MessageProjection {
  const raw = record(value);
  const offset = decimal(raw.offset);
  const authoritativeGeneration = decimal(raw.history_generation ?? raw.historyGeneration);
  const updatedAt = requiredString(raw.updated_at ?? raw.updatedAt);
  const replyTo = optionalString(
    raw.reply_to ?? raw.replyTo ?? raw.in_reply_to ?? raw.inReplyTo,
  );
  const stopReason = optionalString(raw.stop_reason ?? raw.stopReason);
  return {
    id: requiredString(raw.id),
    conversationId: requiredString(raw.conversation_id ?? raw.conversationId),
    senderId: requiredString(raw.sender_id ?? raw.senderId ?? raw.sender),
    type: requiredString(raw.type),
    body: optionalString(raw.body) ?? "",
    content: (raw.content ?? null) as JsonValue,
    ...(replyTo ? { replyTo } : {}),
    state: messageState(raw.state),
    ...(stopReason ? { stopReason } : {}),
    historyGeneration: authoritativeGeneration,
    offset,
    revision: raw.revision === undefined
      ? unixMicroseconds(updatedAt)
      : decimal(raw.revision),
    createdAt: optionalString(raw.created_at ?? raw.createdAt) ?? new Date(0).toISOString(),
    updatedAt,
  };
}

function messageState(value: unknown): MessageProjection["state"] {
  return ["streaming", "completed", "failed", "refused", "cancelled"].includes(String(value))
    ? value as MessageProjection["state"]
    : "completed";
}
function normalizeBase(value: string): string { return value.replace(/\/+$/, ""); }
function assertSameOrigin(requested: URL, serviceUrl: string, authority: string): void {
  if (requested.origin !== new URL(serviceUrl).origin) {
    throw new Error(`${authority} cannot be sent to another origin`);
  }
}
function assertResponseOrigin(response: Response, expectedOrigin: string): void {
  if (response.url && new URL(response.url).origin !== expectedOrigin) {
    throw new Error("Message Service authenticated response origin changed");
  }
}
function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("message service response must be an object");
  return value as Record<string, unknown>;
}
function array(value: unknown): unknown[] { return Array.isArray(value) ? value : []; }
function requiredString(value: unknown): string {
  if (typeof value !== "string" || !value) throw new Error("message service response requires a string");
  return value;
}
function optionalString(value: unknown): string | undefined { return typeof value === "string" && value ? value : undefined; }
function decimal(value: unknown): string {
  const text = String(value);
  if (!/^\d+$/.test(text)) throw new Error("message service response requires a decimal offset/revision");
  return text;
}
function unixMicroseconds(value: string): string {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match) throw new Error("message updated_at must be RFC3339");
  const milliseconds = Date.parse(`${match[1]}${match[3]}`);
  if (!Number.isFinite(milliseconds)) throw new Error("message updated_at must be RFC3339");
  const micros = (match[2] ?? "").padEnd(6, "0").slice(0, 6);
  return (BigInt(milliseconds) * 1000n + BigInt(micros || "0")).toString();
}
function isStatus(error: unknown, status: number): boolean { return error instanceof NodeHttpError && error.status === status; }
function asError(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }
function decodeOptionalRuntimeDispatch(
  raw: Record<string, unknown>,
): { runtimeDispatch?: ReturnType<typeof decodeRuntimeDispatchReceipt> } {
  const value = raw.runtime_dispatch ?? raw.runtimeDispatch;
  return value === undefined
    ? {}
    : { runtimeDispatch: decodeRuntimeDispatchReceipt(value) };
}
