import { CentrifugoSessionAdapter } from "./adapters/centrifugo.js";
import { createCentrifugeFactory } from "./adapters/centrifuge-factory.js";
import { OutcomeUnknownError } from "./errors.js";
import type {
  ActiveOperationListPage,
  ConversationListPage,
  ConversationProjection,
  CurrentPrincipalPort,
  ExecuteMethodCommand,
  ExecuteMethodReceipt,
  LifecyclePort,
  MessageClientComposition,
  MessageListPage,
  MessageProjection,
  OperationProjection,
  ProjectionStore,
  SendMessageCommand,
  SendMessageReceipt,
  UpdateConversationCommand,
} from "./facade/contracts.js";
import type { JsonValue } from "./protocol/index.js";
import {
  decodeRuntimeDispatchReceipt,
  RuntimeDispatchContractError,
} from "./protocol/index.js";

const CONTRACT_REVISION = "2026-07-14.3" as const;

export interface ReactNativeMessageClientOptions {
  readonly gatewayUrl: string;
  readonly accessTokenProvider: () => Promise<string>;
  /**
   * Login-owned identity is verified against the token endpoint's canonical
   * principal_id before it can author optimistic rows.
   */
  readonly currentPrincipal: CurrentPrincipalPort;
  readonly projectionStore?: ProjectionStore;
  readonly lifecycle?: LifecyclePort;
  readonly fetch?: typeof globalThis.fetch;
}

/** React Native/Gateway-v1 composition with one hidden Centrifugo WSS. */
export function createReactNativeMessageClientComposition(
  options: ReactNativeMessageClientOptions,
): MessageClientComposition {
  const credentials = new GatewayCredentials(options);
  const http = new GatewayHttpAdapter(options, credentials);
  return {
    conversationQuery: http,
    conversationCommand: http,
    conversationRoutes: http,
    messageQuery: http,
    messageCommand: http,
    messageStream: http,
    runtimeMethods: http,
    realtime: new CentrifugoSessionAdapter({
      credentials: { getCredentials: () => credentials.realtime() },
      factory: createCentrifugeFactory(),
    }),
    currentPrincipal: credentials,
    ...(options.projectionStore ? { projectionStore: options.projectionStore } : {}),
    ...(options.lifecycle ? { lifecycle: options.lifecycle } : {}),
  };
}

class GatewayCredentials implements CurrentPrincipalPort {
  private canonicalPrincipal?: string;

  constructor(private readonly options: ReactNativeMessageClientOptions) {}

  async accessToken(): Promise<string> {
    const token = await this.options.accessTokenProvider();
    if (!token.trim()) throw new Error("Gateway access token provider returned an empty token");
    return token;
  }

  async realtime(): Promise<{ token: string; realtimeUrl: string }> {
    const response = await request(
      this.options,
      this,
      "POST",
      "/api/v1/messaging/token",
      { platform: "mobile" },
    );
    const raw = record(response);
    const principal = requiredString(raw.principal_id ?? raw.principalId);
    const expected = this.options.currentPrincipal.currentPrincipalId().trim();
    if (!expected || expected !== principal) {
      throw new Error("messaging token principal does not match the authenticated principal");
    }
    this.canonicalPrincipal = principal;
    return {
      token: requiredString(raw.token),
      realtimeUrl: requiredString(raw.centrifugo_url ?? raw.realtime_url ?? raw.realtimeUrl),
    };
  }

  currentPrincipalId(): string {
    const expected = this.options.currentPrincipal.currentPrincipalId().trim();
    if (!this.canonicalPrincipal || expected !== this.canonicalPrincipal) {
      throw new Error("message-sdk current principal is not pinned by messaging credentials");
    }
    return this.canonicalPrincipal;
  }
}

class GatewayHttpAdapter {
  private readonly instancesByOperation = new Map<string, string>();
  private readonly agentsByConversation = new Map<string, string>();

  constructor(
    private readonly options: ReactNativeMessageClientOptions,
    private readonly credentials: GatewayCredentials,
  ) {}

  bindConversation(conversationId: string, agentId: string): void {
    if (!conversationId.trim() || !agentId.trim()) {
      throw new Error("conversation route requires conversationId and agentId");
    }
    const current = this.agentsByConversation.get(conversationId);
    if (current && current !== agentId) {
      throw new Error("conversation is already bound to a different agent");
    }
    this.agentsByConversation.set(conversationId, agentId);
  }

  async getConversation(id: string): Promise<ConversationProjection> {
    const agentId = this.agentFor(id);
    return conversation(unwrap(await this.call(
      "GET",
      `/api/v1/agents/${segment(agentId)}/conversations/${segment(id)}`,
    )), agentId);
  }

  async listConversations(): Promise<ConversationListPage> {
    throw new Error("React Native conversation listing requires explicit agentId");
  }

  async listConversationsForAgent(
    agentId: string,
    cursor?: string,
  ): Promise<ConversationListPage> {
    if (!agentId.trim()) throw new Error("agentId is required");
    const parsed = decodeDirectoryCursor(cursor);
    const page = await this.listDirectoryPage(agentId, parsed.state, parsed.since);
    if (page.hasMore) {
      return {
        ...page,
        nextCursor: encodeDirectoryCursor(parsed.state, page.nextCursor),
      };
    }
    if (parsed.state === "closed") return page;
    const closed = await this.listDirectoryPage(agentId, "closed");
    return {
      conversations: [...page.conversations, ...closed.conversations],
      hasMore: closed.hasMore,
      ...(closed.hasMore
        ? { nextCursor: encodeDirectoryCursor("closed", closed.nextCursor) }
        : {}),
    };
  }

  async createConversation(command: {
    agentId?: string;
    participants: readonly string[];
    title?: string;
    metadata?: Readonly<Record<string, JsonValue>>;
  }): Promise<ConversationProjection> {
    const agentId = command.agentId?.trim();
    if (!agentId || !command.participants.includes(agentId)) {
      throw new Error("Gateway conversation target does not match the composed agent");
    }
    const metadata = command.metadata
      ? Object.fromEntries(Object.entries(command.metadata).map(([key, value]) => {
          if (typeof value !== "string") {
            throw new Error("Gateway conversation metadata values must be strings");
          }
          return [key, value];
        }))
      : undefined;
    const value = conversation(unwrap(await this.call(
      "POST",
      `/api/v1/agents/${segment(agentId)}/conversations`,
      { title: command.title ?? "", ...(metadata ? { metadata } : {}) },
    )), agentId);
    this.bindConversation(value.id, agentId);
    return value;
  }

  async updateConversation(command: UpdateConversationCommand): Promise<ConversationProjection> {
    const agentId = this.agentFor(command.conversationId);
    const value = unwrap(await this.call(
      "PATCH",
      `/api/v1/agents/${segment(agentId)}/conversations/${segment(command.conversationId)}`,
      { title: command.title },
      { "Idempotency-Key": command.idempotencyKey },
    ));
    return conversation(value, agentId);
  }

  async clearConversation(id: string, key: string): Promise<ConversationProjection> {
    const agentId = this.agentFor(id);
    await this.call(
      "POST",
      `/api/v1/agents/${segment(agentId)}/conversations/${segment(id)}/clear`,
      {},
      { "Idempotency-Key": key, "X-BeeOS-Operation-Id": key },
    );
    return this.getConversation(id);
  }

  async deleteConversation(id: string, key: string): Promise<void> {
    const agentId = this.agentFor(id);
    await this.call(
      "DELETE",
      `/api/v1/agents/${segment(agentId)}/conversations/${segment(id)}`,
      undefined,
      { "Idempotency-Key": key },
    );
  }

  async listMessages(conversationId: string, since?: string): Promise<MessageListPage> {
    const agentId = this.agentFor(conversationId);
    const query = new URLSearchParams({ limit: "500", include_deltas: "false" });
    if (since) query.set("since", since);
    const raw = record(unwrap(await this.call(
      "GET",
      `/api/v1/agents/${segment(agentId)}/conversations/${segment(conversationId)}/messages?${query}`,
    )));
    const generation = decimal(raw.historyGeneration ?? raw.history_generation);
    return {
      messages: array(raw.messages).map((value) => message(value, conversationId, generation)),
      historyGeneration: generation,
      historyBoundaryOffset: decimal(
        raw.historyBoundaryOffset ?? raw.history_boundary_offset,
      ),
      latestOffset: decimal(
        raw.latestOffsetStr ?? raw.latest_offset_str ?? raw.latestOffset ?? raw.latest_offset,
      ),
      nextSince: optionalString(raw.nextSince ?? raw.next_since),
      hasMore: Boolean(raw.hasMore ?? raw.has_more),
    };
  }

  async reconcileMessage(
    conversationId: string,
    idempotencyKey: string,
  ): Promise<MessageProjection | undefined> {
    let cursor: string | undefined;
    do {
      const page = await this.listMessages(conversationId, cursor);
      const found = page.messages.find((value) => value.id === idempotencyKey);
      if (found) return found;
      cursor = page.hasMore ? page.nextSince : undefined;
      if (page.hasMore && !cursor) throw new Error("Gateway message page omitted nextSince");
    } while (cursor);
    return undefined;
  }

  async sendMessage(command: SendMessageCommand): Promise<SendMessageReceipt> {
    if (!command.agentId) throw new Error("React Native message send requires explicit agentId");
    this.bindConversation(command.conversationId, command.agentId);
    const agentId = this.agentFor(command.conversationId);
    const text = messageText(command.content);
    try {
      const response = await this.call(
        "POST",
        `/api/v1/agents/${segment(agentId)}/conversations/${segment(command.conversationId)}/messages`,
        { message: text, idempotency_key: command.idempotencyKey },
        { "Idempotency-Key": command.idempotencyKey },
      );
      const raw = record(unwrap(response));
      return {
        messageId: requiredString(raw.messageId ?? raw.message_id),
        outcome: "accepted",
        ...((raw.runtime_dispatch ?? raw.runtimeDispatch) === undefined
          ? {}
          : { runtimeDispatch: decodeRuntimeDispatchReceipt(raw.runtime_dispatch ?? raw.runtimeDispatch) }),
      };
    } catch (error) {
      if (error instanceof GatewayHttpError || error instanceof RuntimeDispatchContractError) throw error;
      throw new OutcomeUnknownError({
        phase: "open",
        conversationId: command.conversationId,
        messageId: command.clientMessageId,
        idempotencyKey: command.idempotencyKey,
        cause: asError(error),
      });
    }
  }

  async cancelMessage(
    conversationId: string,
    messageId: string,
    key: string,
  ): Promise<void> {
    const agentId = this.agentFor(conversationId);
    await this.call(
      "POST",
      `/api/v1/agents/${segment(agentId)}/conversations/${segment(conversationId)}/cancel`,
      { target_message_id: messageId, reason: "user_stop" },
      { "Idempotency-Key": key, "X-BeeOS-Operation-Id": key },
    );
  }

  async startStream(): Promise<never> {
    throw new Error("Gateway composition does not expose agent reply stream writes");
  }
  async append(): Promise<never> {
    throw new Error("Gateway composition does not expose agent reply stream writes");
  }
  async finalize(): Promise<never> {
    throw new Error("Gateway composition does not expose agent reply stream writes");
  }

  async executeMethod(command: ExecuteMethodCommand): Promise<ExecuteMethodReceipt> {
    if (command.method === "session/clear" || command.method === "session/cancel") {
      throw new Error("conversation_route_not_supported");
    }
    if (command.method === "session/set_model") {
      return this.executeSetModel(command);
    }
    const response = await this.callResponse(
      "POST",
      `/api/v1/instances/${segment(command.instanceId)}/methods`,
      { jsonrpc: "2.0", id: command.operationId, method: command.method, params: command.params },
      {
        "Idempotency-Key": command.idempotencyKey,
        "X-BeeOS-Operation-Id": command.operationId,
      },
    );
    if (response.headers.get("X-BeeOS-Operation-Id") !== command.operationId) {
      throw new Error("Gateway runtime response operationId is invalid");
    }
    const envelope = record(await response.json());
    if (envelope.error !== undefined) throw new GatewayHttpError(response.status);
    this.instancesByOperation.set(command.operationId, command.instanceId);
    if (response.status === 202) {
      const result = record(envelope.result);
      if (
        result.status !== "accepted"
        || result.operationId !== command.operationId
        || result.contractRevision !== CONTRACT_REVISION
      ) throw new Error("Gateway runtime accepted response is invalid");
      return {
        operationId: command.operationId,
        outcome: "accepted",
        contractRevision: CONTRACT_REVISION,
      };
    }
    if (response.status !== 200) throw new GatewayHttpError(response.status);
    return {
      operationId: command.operationId,
      outcome: "completed",
      result: jsonValue(envelope.result),
      contractRevision: CONTRACT_REVISION,
    };
  }

  private async executeSetModel(
    command: ExecuteMethodCommand,
  ): Promise<ExecuteMethodReceipt> {
    if (
      command.target.scope !== "conversation"
    ) {
      throw new Error("session/set_model requires a conversation target");
    }
    this.bindConversation(command.target.conversationId, command.target.platformAgentId);
    const params = record(command.params);
    const modelOverrideId = params.modelOverrideId;
    if (modelOverrideId !== null && typeof modelOverrideId !== "string") {
      throw new Error("session/set_model requires modelOverrideId string or null");
    }
    const response = await this.callResponse(
      "PUT",
      `/api/v1/agents/${segment(command.target.platformAgentId)}/conversations/${segment(command.target.conversationId)}/model`,
      { modelOverrideId },
      {
        "Idempotency-Key": command.idempotencyKey,
        "X-BeeOS-Operation-Id": command.operationId,
      },
    );
    if (response.headers.get("X-BeeOS-Operation-Id") !== command.operationId) {
      throw new Error("Gateway model response operationId is invalid");
    }
    const value = await response.json();
    if (response.status === 202) {
      const accepted = record(value);
      if (
        accepted.status !== "accepted"
        || accepted.operationId !== command.operationId
        || accepted.contractRevision !== CONTRACT_REVISION
      ) throw new Error("Gateway model accepted response is invalid");
      this.instancesByOperation.set(command.operationId, command.instanceId);
      return {
        operationId: command.operationId,
        outcome: "accepted",
        contractRevision: CONTRACT_REVISION,
      };
    }
    if (response.status !== 200) throw new GatewayHttpError(response.status);
    this.instancesByOperation.set(command.operationId, command.instanceId);
    return {
      operationId: command.operationId,
      outcome: "completed",
      result: jsonValue(value),
      contractRevision: CONTRACT_REVISION,
    };
  }

  async listActiveOperations(
    instanceId: string,
    cursor?: string,
  ): Promise<ActiveOperationListPage> {
    const query = new URLSearchParams({ status: "active", limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const raw = record(await this.call(
      "GET",
      `/api/v1/instances/${segment(instanceId)}/operations?${query}`,
    ));
    const operations = array(raw.operations).map(operation);
    for (const value of operations) {
      if (value.instanceId !== instanceId) {
        throw new Error("Gateway operation list returned a foreign instance");
      }
      this.instancesByOperation.set(value.id, instanceId);
    }
    return {
      operations,
      nextCursor: optionalString(raw.nextCursor ?? raw.next_cursor),
    };
  }

  async getOperation(operationId: string): Promise<OperationProjection> {
    const instanceId = this.instancesByOperation.get(operationId);
    if (!instanceId) {
      throw new Error("operation instance is unknown; execute or listActive must establish ownership");
    }
    const value = operation(await this.call(
      "GET",
      `/api/v1/instances/${segment(instanceId)}/operations/${segment(operationId)}`,
    ));
    if (value.id !== operationId || value.instanceId !== instanceId) {
      throw new Error("Gateway operation GET returned a foreign operation");
    }
    return value;
  }

  async cancelOperation(operationId: string, key: string): Promise<OperationProjection> {
    const instanceId = this.instancesByOperation.get(operationId);
    if (!instanceId) {
      throw new Error("operation instance is unknown; execute or listActive must establish ownership");
    }
    await this.call(
      "POST",
      `/api/v1/instances/${segment(instanceId)}/operations/${segment(operationId)}/cancel`,
      {},
      { "Idempotency-Key": key, "X-BeeOS-Operation-Id": operationId },
    );
    return this.getOperation(operationId);
  }

  private async listDirectoryPage(
    agentId: string,
    state: "active" | "closed",
    since?: string,
  ): Promise<ConversationListPage> {
    const query = new URLSearchParams({ state, limit: "100" });
    if (since) query.set("since", since);
    const raw = record(unwrap(await this.call(
      "GET",
      `/api/v1/agents/${segment(agentId)}/conversations?${query}`,
    )));
    const next = optionalString(raw.nextSince ?? raw.next_since);
    return {
      conversations: array(raw.conversations).map((value) => {
        const projection = conversation(value, agentId);
        this.bindConversation(projection.id, agentId);
        return projection;
      }),
      nextCursor: next,
      hasMore: Boolean(next),
    };
  }

  private call(
    method: string,
    path: string,
    body?: unknown,
    headers?: Readonly<Record<string, string>>,
  ): Promise<unknown> {
    return this.callResponse(method, path, body, headers).then(async (response) => {
      if (response.status === 204) return undefined;
      return response.json();
    });
  }

  private callResponse(
    method: string,
    path: string,
    body?: unknown,
    headers?: Readonly<Record<string, string>>,
  ): Promise<Response> {
    return requestResponse(this.options, this.credentials, method, path, body, headers);
  }

  private agentFor(conversationId: string): string {
    const agentId = this.agentsByConversation.get(conversationId);
    if (!agentId) {
      throw new Error("conversation route is unknown; listForAgent/create/watch/send must bind it");
    }
    return agentId;
  }
}

async function request(
  options: ReactNativeMessageClientOptions,
  credentials: GatewayCredentials,
  method: string,
  path: string,
  body?: unknown,
): Promise<unknown> {
  const response = await requestResponse(options, credentials, method, path, body);
  return response.json();
}

async function requestResponse(
  options: ReactNativeMessageClientOptions,
  credentials: GatewayCredentials,
  method: string,
  path: string,
  body?: unknown,
  extraHeaders: Readonly<Record<string, string>> = {},
): Promise<Response> {
  const token = await credentials.accessToken();
  const headers: Record<string, string> = {
    Accept: "application/json",
    Authorization: `Bearer ${token}`,
    ...extraHeaders,
  };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const response = await fetchImpl(`${normalizeBase(options.gatewayUrl)}${path}`, {
    method,
    headers,
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  if (!response.ok) throw new GatewayHttpError(response.status);
  return response;
}

class GatewayHttpError extends Error {
  constructor(readonly status: number) {
    super(`Gateway request failed with HTTP ${status}`);
  }
}

function conversation(value: unknown, expectedAgentId?: string): ConversationProjection {
  const raw = record(value);
  const agentId = optionalString(raw.agentId ?? raw.agent_id);
  const modelOverrideId = optionalNullableString(
    Object.prototype.hasOwnProperty.call(raw, "modelOverrideId")
      ? raw.modelOverrideId
      : raw.model_override_id,
  );
  if (expectedAgentId && agentId !== expectedAgentId) {
    throw new Error("Gateway conversation agent does not match the explicit route");
  }
  return {
    id: requiredString(raw.conversationId ?? raw.conversation_id ?? raw.id),
    ...(agentId ? { agentId } : {}),
    title: optionalString(raw.title),
    ...(modelOverrideId === undefined ? {} : { modelOverrideId }),
    state: raw.state === "closed" ? "closed" : "open",
    historyGeneration: decimal(raw.historyGeneration ?? raw.history_generation),
    revision: decimal(raw.metadataVersion ?? raw.metadata_version),
    lastActivityAt: optionalString(raw.lastActivityAt ?? raw.last_activity_at),
    updatedAt: optionalString(
      raw.lastActivityAt ?? raw.last_activity_at ?? raw.closedAt ?? raw.closed_at
      ?? raw.createdAt ?? raw.created_at,
    ) ?? new Date(0).toISOString(),
  };
}

function message(
  value: unknown,
  conversationId: string,
  fallbackGeneration: string,
): MessageProjection {
  const raw = record(value);
  const updatedAt = optionalString(raw.updatedAt ?? raw.updated_at)
    ?? requiredString(raw.createdAt ?? raw.created_at);
  const id = requiredString(raw.messageId ?? raw.message_id ?? raw.id);
  const responseConversationId = optionalString(
    raw.conversationId ?? raw.conversation_id,
  );
  if (responseConversationId && responseConversationId !== conversationId) {
    throw new Error("Gateway message belongs to a foreign conversation");
  }
  const content = raw.payload === undefined ? undefined : jsonValue(raw.payload);
  return {
    id,
    conversationId: responseConversationId ?? conversationId,
    senderId: requiredString(raw.publisherId ?? raw.publisher_id ?? raw.senderId ?? raw.sender_id),
    type: requiredString(raw.type),
    body: optionalString(raw.body) ?? "",
    ...(raw.parts === undefined ? {} : { parts: jsonValue(raw.parts) }),
    ...(content === undefined ? {} : { content }),
    ...(optionalString(raw.inReplyTo ?? raw.in_reply_to)
      ? { replyTo: optionalString(raw.inReplyTo ?? raw.in_reply_to) }
      : {}),
    state: messageState(raw.state),
    ...(optionalString(raw.stopReason ?? raw.stop_reason)
      ? { stopReason: optionalString(raw.stopReason ?? raw.stop_reason) }
      : {}),
    historyGeneration: decimal(
      raw.historyGeneration ?? raw.history_generation ?? fallbackGeneration,
    ),
    offset: decimal(raw.offsetStr ?? raw.offset_str ?? raw.offset),
    revision: raw.revision === undefined
      ? unixMicroseconds(updatedAt)
      : decimal(raw.revision),
    clientMessageId: id,
    idempotencyKey: id,
    createdAt: optionalString(raw.createdAt ?? raw.created_at) ?? updatedAt,
    updatedAt,
  };
}

function operation(value: unknown): OperationProjection {
  const raw = record(value);
  const target = runtimeTarget(raw.target);
  const result = raw.result === undefined ? undefined : jsonValue(raw.result);
  const error = raw.error === undefined ? undefined : jsonValue(raw.error);
  const projection = raw.projection === undefined ? undefined : jsonValue(raw.projection);
  const progress = raw.progress === undefined ? undefined : jsonValue(raw.progress);
  const status = requiredString(raw.status) as OperationProjection["status"];
  const effectState = requiredString(
    raw.effectState ?? raw.effect_state,
  ) as OperationProjection["effectState"];
  const terminal = raw.terminal;
  if (!operationStateCompatible(status, effectState, terminal)) {
    throw new Error("Gateway operation snapshot has inconsistent terminal state");
  }
  if (raw.contractRevision !== CONTRACT_REVISION || raw.transport !== "service") {
    throw new Error("Gateway operation snapshot contract/transport is invalid");
  }
  return {
    id: requiredString(raw.id),
    instanceId: requiredString(raw.instanceId ?? raw.instance_id),
    target,
    method: requiredString(raw.method),
    capability: requiredString(raw.capability),
    contractRevision: CONTRACT_REVISION,
    transport: "service",
    sequence: decimal(raw.sequence),
    cursor: optionalString(raw.cursor),
    status,
    effectState,
    terminal,
    ...(progress === undefined ? {} : { progress }),
    ...(result === undefined ? {} : { result }),
    ...(error === undefined ? {} : { error }),
    ...(projection === undefined ? {} : { projection }),
    createdAt: requiredString(raw.createdAt ?? raw.created_at),
    updatedAt: requiredString(raw.updatedAt ?? raw.updated_at),
    revision: decimal(raw.revision ?? raw.sequence),
  };
}

function runtimeTarget(value: unknown): OperationProjection["target"] {
  const raw = record(value);
  if (raw.scope === "instance" && Object.keys(raw).length === 1) return { scope: "instance" };
  const platformAgentId = requiredString(raw.platformAgentId ?? raw.platform_agent_id);
  if (raw.scope === "agent") return { scope: "agent", platformAgentId };
  if (raw.scope === "conversation") {
    return {
      scope: "conversation",
      platformAgentId,
      conversationId: requiredString(raw.conversationId ?? raw.conversation_id),
    };
  }
  throw new Error("Gateway operation target is invalid");
}

function operationStateCompatible(
  status: string,
  effect: string,
  terminal: unknown,
): terminal is boolean {
  if (typeof terminal !== "boolean") return false;
  if (status === "queued") return effect === "queued" && !terminal;
  if (status === "running") return effect === "running" && !terminal;
  if (status === "runtime_committed" || status === "projection_pending") {
    return effect === "committed" && !terminal;
  }
  if (status === "succeeded") return effect === "committed" && terminal;
  if (status === "failed") return effect === "failed" && terminal;
  if (status === "cancelled") return effect === "cancelled" && terminal;
  if (status === "expired") return effect === "expired" && terminal;
  if (status === "outcome_unknown") return effect === "outcome_unknown" && terminal;
  return status === "projection_blocked" && effect === "committed";
}

function decodeDirectoryCursor(cursor?: string): {
  state: "active" | "closed";
  since?: string;
} {
  if (!cursor) return { state: "active" };
  const separator = cursor.indexOf(":");
  if (separator < 0) throw new Error("invalid Gateway conversation directory cursor");
  const state = cursor.slice(0, separator);
  const since = cursor.slice(separator + 1);
  if ((state !== "active" && state !== "closed") || !since) {
    throw new Error("invalid Gateway conversation directory cursor");
  }
  return { state, since };
}

function encodeDirectoryCursor(state: "active" | "closed", since?: string): string {
  if (!since) throw new Error("Gateway conversation page omitted cursor");
  return `${state}:${since}`;
}

function unwrap(value: unknown): unknown {
  const raw = record(value);
  if ("success" in raw) {
    if (raw.success !== true || raw.data === undefined) {
      throw new Error(optionalString(raw.error) ?? "Gateway response failed");
    }
    return raw.data;
  }
  return value;
}

function messageText(content: JsonValue): string {
  if (
    typeof content === "object" && content !== null && !Array.isArray(content)
    && typeof content.text === "string"
  ) return content.text;
  throw new Error("Gateway chat_message content requires a text field");
}

function messageState(value: unknown): MessageProjection["state"] {
  return ["streaming", "completed", "failed", "refused", "cancelled"].includes(String(value))
    ? value as MessageProjection["state"]
    : "completed";
}

function jsonValue(value: unknown): JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return value.map(jsonValue);
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, jsonValue(item)]));
  }
  throw new Error("Gateway response contains a non-JSON value");
}

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Gateway response must be an object");
  }
  return value as Record<string, unknown>;
}
function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error("Gateway response requires an array");
  return value;
}
function requiredString(value: unknown): string {
  if (typeof value !== "string" || !value) throw new Error("Gateway response requires a string");
  return value;
}
function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}
function optionalNullableString(value: unknown): string | null | undefined {
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (typeof value === "string" && value.length > 0) return value;
  throw new Error("Gateway conversation modelOverrideId must be a non-empty string or null");
}
function decimal(value: unknown): string {
  const text = String(value);
  if (!/^\d+$/.test(text)) throw new Error("Gateway response requires a decimal string");
  return text;
}
function segment(value: string): string { return encodeURIComponent(value); }
function normalizeBase(value: string): string {
  const base = value.replace(/\/+$/, "");
  const url = new URL(base);
  if (url.protocol !== "https:" && !isLoopback(url)) {
    throw new Error("React Native Gateway composition requires HTTPS");
  }
  return base;
}
function isLoopback(url: URL): boolean {
  return url.protocol === "http:"
    && ["127.0.0.1", "localhost", "::1"].includes(url.hostname);
}
function unixMicroseconds(value: string): string {
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) throw new Error("Gateway message timestamp must be RFC3339");
  return (BigInt(milliseconds) * 1000n).toString();
}
function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
