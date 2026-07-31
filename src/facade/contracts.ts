import type {
  AnyRealtimeEventV1,
  JsonValue,
  RealtimeEventType,
  RuntimeDispatchReceipt,
} from "../protocol/index.js";

export type CommandOutcome = "accepted" | "created" | "duplicate" | "outcome_unknown";

export interface ConversationProjection {
  readonly id: string;
  readonly agentId?: string;
  readonly title?: string;
  /** Canonical server projection: absent=unknown, null=explicitly cleared. */
  readonly modelOverrideId?: string | null;
  readonly state: "open" | "closed";
  readonly historyGeneration: string;
  readonly revision: string;
  readonly lastMessageId?: string;
  readonly lastActivityAt?: string;
  readonly updatedAt: string;
}

export interface MessageProjection {
  readonly id: string;
  readonly conversationId: string;
  readonly senderId: string;
  readonly type: string;
  readonly body: string;
  readonly parts?: JsonValue;
  readonly content?: JsonValue;
  readonly replyTo?: string;
  readonly state: "optimistic" | "streaming" | "completed" | "failed" | "refused" | "cancelled" | "outcome_unknown";
  readonly stopReason?: string;
  readonly historyGeneration: string;
  readonly offset: string;
  readonly revision: string;
  readonly clientMessageId?: string;
  readonly idempotencyKey?: string;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface OperationProjection {
  readonly id: string;
  readonly instanceId: string;
  readonly target: RuntimeMethodTarget;
  readonly method: string;
  readonly capability: string;
  readonly contractRevision: "2026-07-14.3";
  readonly transport: "service";
  readonly sequence: string;
  readonly cursor?: string;
  readonly status:
    | "queued" | "running" | "runtime_committed" | "projection_pending"
    | "succeeded" | "failed" | "cancelled" | "expired"
    | "outcome_unknown" | "projection_blocked";
  readonly effectState:
    | "queued" | "running" | "committed" | "failed" | "cancelled"
    | "expired" | "outcome_unknown";
  readonly terminal: boolean;
  readonly progress?: JsonValue;
  readonly result?: JsonValue;
  readonly error?: JsonValue;
  readonly projection?: JsonValue;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly revision: string;
}

export interface SessionNewResult {
  readonly sessionId: string;
  readonly conversationId: string;
  readonly [key: string]: JsonValue;
}

export interface SessionNewOperationProjection extends OperationProjection {
  readonly method: "session.new";
  readonly result?: SessionNewResult;
}

export interface ConversationHydrationProof {
  readonly conversationId: string;
  readonly historyGeneration: string;
  /** Durable lower fence committed by the latest history clear. */
  readonly historyBoundaryOffset: string;
  readonly conversationRevision: string;
  readonly latestOffset: string;
  /**
   * Client-owned monotonic revision of the authoritative projection covered
   * by this proof. Unlike latestOffset it changes for same-offset terminal
   * updates and conversation-only mutations.
   */
  readonly projectionRevision: string;
  readonly complete: true;
}

export interface DomainProjectionSnapshot {
  readonly conversations: Readonly<Record<string, ConversationProjection>>;
  readonly messages: Readonly<Record<string, MessageProjection>>;
  readonly operations: Readonly<Record<string, OperationProjection>>;
  readonly latestOffsetByConversation: Readonly<Record<string, string>>;
  readonly hydrationByConversation: Readonly<Record<string, ConversationHydrationProof>>;
}

export interface ConversationListPage {
  readonly conversations: readonly ConversationProjection[];
  readonly nextCursor?: string;
  readonly hasMore: boolean;
}

export interface MessageListPage {
  readonly messages: readonly MessageProjection[];
  readonly historyGeneration: string;
  readonly historyBoundaryOffset: string;
  readonly latestOffset: string;
  readonly nextSince?: string;
  readonly hasMore: boolean;
}

export interface CreateConversationCommand {
  readonly agentId?: string;
  readonly participants: readonly string[];
  readonly title?: string;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface UpdateConversationCommand {
  readonly conversationId: string;
  readonly title: string;
  readonly idempotencyKey: string;
}

export interface SendMessageCommand {
  readonly conversationId: string;
  readonly agentId?: string;
  readonly clientMessageId: string;
  readonly idempotencyKey: string;
  readonly type: string;
  readonly content: JsonValue;
  readonly replyTo?: string;
}

export interface SendMessageReceipt {
  readonly messageId: string;
  readonly outcome: CommandOutcome;
  readonly idempotent?: boolean;
  readonly correlationId?: string;
  readonly runtimeDispatch?: RuntimeDispatchReceipt;
}

export type RuntimeMethodTarget =
  | { readonly scope: "instance" }
  | { readonly scope: "agent"; readonly platformAgentId: string }
  | {
      readonly scope: "conversation";
      readonly platformAgentId: string;
      readonly conversationId: string;
    };

export interface ExecuteMethodCommand {
  readonly operationId: string;
  readonly instanceId: string;
  readonly target: RuntimeMethodTarget;
  readonly method: string;
  readonly params: JsonValue;
  readonly idempotencyKey: string;
}

export type ExecuteMethodReceipt = {
  readonly operationId: string;
  readonly outcome: "accepted" | "duplicate" | "outcome_unknown";
  readonly contractRevision: "2026-07-14.3";
  readonly correlationId?: string;
} | {
  readonly operationId: string;
  readonly outcome: "completed";
  readonly result: JsonValue;
  readonly contractRevision: "2026-07-14.3";
  readonly correlationId?: string;
};

export interface ActiveOperationListPage {
  readonly operations: readonly OperationProjection[];
  readonly nextCursor?: string;
}

export interface ConversationQueryPort {
  getConversation(conversationId: string): Promise<ConversationProjection>;
  listConversations(cursor?: string): Promise<ConversationListPage>;
  listConversationsForAgent?(
    agentId: string,
    cursor?: string,
  ): Promise<ConversationListPage>;
}

/** Composition-only explicit Gateway route registry; never exposed by MessageClient. */
export interface ConversationRoutePort {
  bindConversation(conversationId: string, agentId: string): void;
}

/** Optional authoritative directory used by SDK-owned durable inbox recovery. */
export interface PrivateConversationDirectoryQueryPort {
  listPrivateConversations(
    state: "open" | "closed",
    cursor?: string,
  ): Promise<ConversationListPage>;
}

export interface ConversationCommandPort {
  createConversation(command: CreateConversationCommand): Promise<ConversationProjection>;
  updateConversation(command: UpdateConversationCommand): Promise<ConversationProjection>;
  clearConversation(conversationId: string, idempotencyKey: string): Promise<ConversationProjection>;
  deleteConversation(conversationId: string, idempotencyKey: string): Promise<void>;
}

export interface MessageQueryPort {
  listMessages(conversationId: string, since?: string): Promise<MessageListPage>;
  reconcileMessage(conversationId: string, idempotencyKey: string): Promise<MessageProjection | undefined>;
  getMessage?(conversationId: string, messageId: string): Promise<MessageProjection>;
}

export interface MessageCommandPort {
  sendMessage(command: SendMessageCommand): Promise<SendMessageReceipt>;
  cancelMessage(conversationId: string, messageId: string, idempotencyKey: string): Promise<void>;
}

export interface MessageStreamWriter {
  startStream(command: SendMessageCommand): Promise<SendMessageReceipt>;
  append(
    conversationId: string,
    messageId: string,
    bodyAppend: string,
    bodyFrom: number,
    idempotencyKey: string,
  ): Promise<void>;
  finalize(
    conversationId: string,
    messageId: string,
    state: "completed" | "failed" | "refused" | "cancelled",
    idempotencyKey: string,
    stopReason?: string,
  ): Promise<void>;
  setBody?(conversationId: string, messageId: string, body: string, idempotencyKey: string): Promise<void>;
  setParts?(
    conversationId: string,
    messageId: string,
    parts: readonly JsonValue[],
    idempotencyKey: string,
  ): Promise<void>;
}

export interface RuntimeMethodPort {
  executeMethod(command: ExecuteMethodCommand): Promise<ExecuteMethodReceipt>;
  listActiveOperations(instanceId: string, cursor?: string): Promise<ActiveOperationListPage>;
  getOperation(operationId: string): Promise<OperationProjection>;
  cancelOperation(operationId: string, idempotencyKey: string): Promise<OperationProjection>;
}

/** Lease authority is supplied by the Node composition caller, never serialized. */
export interface RuntimeDeliveryLease {
  readonly instanceId: string;
  readonly handlerIdentity: string;
  readonly runtimeEpoch: string;
  readonly leaseId: string;
  readonly leaseExpiresAt: string;
  readonly journalStoreId: string;
  readonly journalGeneration: string;
}

export interface RuntimeMethodDelivery {
  readonly deliveryId: string;
  readonly redelivered: boolean;
  readonly idleMs: number;
  readonly message: JsonValue;
  readonly executionGrant?: string;
}

export type RuntimeDeliveryHistory =
  | { readonly status: "not_found" | "expired" }
  | { readonly status: "found"; readonly snapshot: JsonValue };

export type RuntimeDeliveryAppendReceipt =
  | {
      readonly outcome: "created" | "duplicate" | "reconciled";
      readonly message?: JsonValue;
      readonly history?: JsonValue;
    }
  | { readonly outcome: "outcome_unknown"; readonly history?: JsonValue };

export interface RuntimeDeliveryConsumer {
  start(): void;
  stop(signal?: AbortSignal): Promise<void>;
  history(operationId: string): Promise<RuntimeDeliveryHistory>;
  append(
    operationId: string,
    type: string,
    payload: JsonValue,
    executionGrant?: string,
  ): Promise<RuntimeDeliveryAppendReceipt>;
  acknowledge(deliveryIds: readonly string[]): Promise<void>;
}

export interface RuntimeDeliveryConsumeOptions {
  readonly onDelivery: (
    delivery: RuntimeMethodDelivery,
    context: { readonly lease: RuntimeDeliveryLease; readonly signal: AbortSignal },
  ) => Promise<void>;
  readonly onError?: (error: unknown) => void;
  /** A consumer whose authority has no lease issues no reads and raises no
   * error, so a stalled runtime is bounded only by someone happening to look.
   * Fires at most once per 30s while starved, and once more with `recovered`
   * when a lease reappears. Starvation shorter than one interval is never
   * reported, so startup and renewal gaps stay silent. */
  readonly onLeaseStarvation?: (
    input: { readonly starvedForMs: number; readonly recovered: boolean },
  ) => void;
  readonly readCount?: number;
  readonly blockMs?: number;
  readonly idleDelayMs?: number;
  readonly renewIntervalMs?: number;
}

/** Composition-only durable runtime-delivery transport. */
export interface RuntimeDeliveryPort {
  consume(options: RuntimeDeliveryConsumeOptions): RuntimeDeliveryConsumer;
}

export type RealtimeConnectionState =
  | "connecting"
  | "connected"
  | "disconnected"
  | "reconnecting"
  | "failed";

export interface RealtimeSession {
  close(): Promise<void> | void;
}

export interface RealtimeConnectInput {
  onEvent(event: unknown): void;
  onState(state: RealtimeConnectionState): void;
}

export interface RealtimeSessionPort {
  connect(input: RealtimeConnectInput): Promise<RealtimeSession>;
}

export interface LifecyclePort {
  current(): "active" | "background" | "inactive";
  isOnline(): boolean;
  subscribe(listener: () => void): () => void;
}

/** Viewer-local identity. It is never serialized into projection storage. */
export interface CurrentPrincipalPort {
  currentPrincipalId(): string;
}

export interface RealtimeListenFilter {
  readonly conversationIds?: readonly string[];
  readonly instanceIds?: readonly string[];
  readonly agentIds?: readonly string[];
  readonly eventTypes?: readonly RealtimeEventType[];
}

export type RealtimeListener = (event: AnyRealtimeEventV1) => void;

export interface ConversationWatch {
  readonly conversationId: string;
  readonly ready: Promise<void>;
  release(): void;
}

export interface MessageClientSnapshot extends DomainProjectionSnapshot {
  readonly connection: RealtimeConnectionState;
  readonly watchedConversationIds: readonly string[];
  readonly recoveryError?: string;
}

export interface MessageClientComposition {
  readonly conversationQuery: ConversationQueryPort;
  readonly privateConversationDirectoryQuery?: PrivateConversationDirectoryQueryPort;
  readonly conversationCommand: ConversationCommandPort;
  readonly conversationRoutes?: ConversationRoutePort;
  readonly messageQuery: MessageQueryPort;
  readonly messageCommand: MessageCommandPort;
  readonly messageStream: MessageStreamWriter;
  readonly runtimeMethods: RuntimeMethodPort;
  readonly runtimeDelivery?: RuntimeDeliveryPort;
  readonly realtime: RealtimeSessionPort;
  readonly currentPrincipal: CurrentPrincipalPort;
  readonly lifecycle?: LifecyclePort;
}
