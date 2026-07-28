import { OutcomeUnknownError } from "./errors.js";
import { UnifiedMessageStream } from "./message-stream.js";
import {
  decodeRealtimeEvent,
  evaluateScopedRealtimeEvent,
  RealtimeDedupe,
  withScopedRealtimeCursor,
  type AnyRealtimeEventV1,
  type RealtimeDeliveryAudience,
  type ScopedRealtimeCursors,
} from "./protocol/index.js";
import { ConversationWatchRegistry } from "./facade/watch-registry.js";
import { ProjectionEngine } from "./facade/projection.js";
import { RecoveryCoordinator } from "./facade/recovery-coordinator.js";
import type {
  ConversationListPage,
  ConversationProjection,
  ConversationWatch,
  CreateConversationCommand,
  DomainProjectionSnapshot,
  ExecuteMethodCommand,
  ExecuteMethodReceipt,
  ActiveOperationListPage,
  MessageClientComposition,
  MessageClientSnapshot,
  MessageListPage,
  MessageProjection,
  OperationProjection,
  RealtimeConnectionState,
  RealtimeListenFilter,
  RealtimeSession,
  RuntimeDeliveryConsumeOptions,
  RuntimeDeliveryConsumer,
  SendMessageCommand,
  SendMessageReceipt,
  UpdateConversationCommand,
} from "./facade/contracts.js";

type StoreListener = () => void;
type EventListener = (event: AnyRealtimeEventV1) => void;

export interface ConversationsNamespace {
  watch(conversationId: string, agentId?: string): ConversationWatch;
  get(conversationId: string): Promise<ConversationProjection>;
  list(cursor?: string): Promise<ConversationListPage>;
  listForAgent(agentId: string, cursor?: string): Promise<ConversationListPage>;
  create(command: CreateConversationCommand): Promise<ConversationProjection>;
  update(command: UpdateConversationCommand): Promise<ConversationProjection>;
  clear(conversationId: string, idempotencyKey: string): Promise<ConversationProjection>;
  delete(conversationId: string, idempotencyKey: string): Promise<void>;
}

export interface MessagesNamespace {
  send(command: SendMessageCommand): Promise<SendMessageReceipt>;
  /**
   * Explicit retry only: reconcile first and then reuse the exact caller-owned
   * idempotency key. The SDK never invokes this automatically.
   */
  retry(command: SendMessageCommand): Promise<SendMessageReceipt>;
  list(conversationId: string, since?: string): Promise<MessageListPage>;
  getEnvelope(conversationId: string, messageId: string): Promise<MessageProjection>;
  isMine(messageOrId: MessageProjection | string): boolean;
  cancel(conversationId: string, messageId: string, idempotencyKey: string): Promise<void>;
  startStream(command: SendMessageCommand): UnifiedMessageStream;
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
}

export interface MethodsNamespace {
  execute(command: ExecuteMethodCommand): Promise<ExecuteMethodReceipt>;
  listActive(instanceId: string, cursor?: string): Promise<ActiveOperationListPage>;
  get(operationId: string): Promise<OperationProjection>;
  cancel(operationId: string, idempotencyKey: string): Promise<OperationProjection>;
  /** Node runtime binding only; MS durable HTTP remains the truth source. */
  consume(options: RuntimeDeliveryConsumeOptions): RuntimeDeliveryConsumer;
}

export interface MessageClient {
  readonly conversations: ConversationsNamespace;
  readonly messages: MessagesNamespace;
  readonly methods: MethodsNamespace;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  listen(filter: RealtimeListenFilter, listener: EventListener): () => void;
  getSnapshot(): MessageClientSnapshot;
  subscribe(listener: StoreListener): () => void;
}

/**
 * The one MessageClient implementation. Platform composition supplies narrow
 * HTTPS/WSS/storage/lifecycle ports; feature code only receives this object.
 */
export class UnifiedMessageClient implements MessageClient {
  readonly conversations: ConversationsNamespace;
  readonly messages: MessagesNamespace;
  readonly methods: MethodsNamespace;

  private readonly projection = new ProjectionEngine();
  private readonly recovery: RecoveryCoordinator;
  private readonly watches: ConversationWatchRegistry;
  private readonly listeners = new Map<number, { filter: RealtimeListenFilter; listener: EventListener }>();
  private readonly ephemeralDedupe = new RealtimeDedupe();
  private readonly storeListeners = new Set<StoreListener>();
  private readonly runtimeConsumers = new Set<RuntimeDeliveryConsumer>();
  private nextListenerId = 1;
  private connection: RealtimeConnectionState = "disconnected";
  private session?: RealtimeSession;
  private connectPromise?: Promise<void>;
  private requestedConnection = false;
  private stopLifecycle?: () => void;
  private restored = false;
  private recoveryError?: string;
  private checkpointWrite: Promise<void> = Promise.resolve();
  private projectionStoreClosePending = false;
  private scopedCursors: ScopedRealtimeCursors = { conversations: {} };
  private transportInterrupted = false;
  private recoveryBuffering = false;
  private recoveryBatching = false;
  private recoveryDirty = false;
  private privateRecoveryPromise?: Promise<void>;
  private privateDirectoryRefreshRequested = false;
  private readonly bufferedInbound: Array<{
    raw: unknown;
    audience: RealtimeDeliveryAudience;
  }> = [];
  private readonly deferredListenerEvents: AnyRealtimeEventV1[] = [];
  private snapshot: MessageClientSnapshot;

  constructor(private readonly composition: MessageClientComposition) {
    this.recovery = new RecoveryCoordinator({
      conversations: composition.conversationQuery,
      privateDirectory: composition.privateConversationDirectoryQuery,
      messages: composition.messageQuery,
      projection: this.projection,
    });
    this.watches = new ConversationWatchRegistry({
      recovery: this.recovery,
      projection: this.projection,
      getSession: () => this.session,
    });
    this.snapshot = this.makeSnapshot();

    this.conversations = {
      watch: (id, agentId) => this.watch(id, agentId),
      get: (id) => composition.conversationQuery.getConversation(id),
      list: (cursor) => composition.conversationQuery.listConversations(cursor),
      listForAgent: (agentId, cursor) => {
        const list = composition.conversationQuery.listConversationsForAgent;
        if (!list) throw new Error("conversation query adapter does not support agent-scoped listing");
        return list.call(composition.conversationQuery, agentId, cursor);
      },
      create: (command) => composition.conversationCommand.createConversation(command),
      update: (command) => composition.conversationCommand.updateConversation(command),
      clear: async (id, key) => {
        const result = await composition.conversationCommand.clearConversation(id, key);
        await this.recovery.recoverConversation(id);
        this.publishProjectionChange();
        return result;
      },
      delete: (id, key) => composition.conversationCommand.deleteConversation(id, key),
    };
    this.messages = {
      send: (command) => this.send(command, false),
      retry: (command) => this.send(command, true),
      list: (id, since) => composition.messageQuery.listMessages(id, since),
      getEnvelope: (id, messageId) => {
        if (!composition.messageQuery.getMessage) throw new Error("message query adapter does not implement getMessage");
        return composition.messageQuery.getMessage(id, messageId);
      },
      isMine: (messageOrId) => {
        const message = typeof messageOrId === "string"
          ? this.projection.getSnapshot().messages[messageOrId]
          : messageOrId;
        return Boolean(
          message?.senderId
          && message.senderId === requiredCurrentPrincipalId(composition),
        );
      },
      cancel: (id, messageId, key) => composition.messageCommand.cancelMessage(id, messageId, key),
      startStream: (command) => {
        assertSend(command);
        return new UnifiedMessageStream(composition.messageStream, command);
      },
      append: (conversationId, messageId, bodyAppend, bodyFrom, key) =>
        composition.messageStream.append(conversationId, messageId, bodyAppend, bodyFrom, key),
      finalize: (conversationId, messageId, state, key, stopReason) =>
        composition.messageStream.finalize(conversationId, messageId, state, key, stopReason),
    };
    this.methods = {
      execute: (command) => this.execute(command),
      listActive: (instanceId, cursor) =>
        composition.runtimeMethods.listActiveOperations(instanceId, cursor),
      get: (id) => composition.runtimeMethods.getOperation(id),
      cancel: (id, key) => composition.runtimeMethods.cancelOperation(id, key),
      consume: (options) => this.consumeRuntimeDeliveries(options),
    };
  }

  async connect(): Promise<void> {
    this.requestedConnection = true;
    this.projectionStoreClosePending = true;
    this.startLifecycle();
    if (!this.lifecycleAllowsConnection()) return;
    return this.open();
  }

  async disconnect(): Promise<void> {
    this.requestedConnection = false;
    this.stopLifecycle?.();
    this.stopLifecycle = undefined;
    const inflight = this.connectPromise;
    try {
      if (inflight) await inflight.catch(() => undefined);
      await Promise.allSettled(
        [...this.runtimeConsumers].map((consumer) => consumer.stop()),
      );
      this.runtimeConsumers.clear();
      await this.retireSession();
      await this.checkpointWrite;
    } finally {
      if (this.projectionStoreClosePending) {
        this.projectionStoreClosePending = false;
        await this.composition.projectionStore?.close?.();
      }
    }
  }

  private consumeRuntimeDeliveries(
    options: RuntimeDeliveryConsumeOptions,
  ): RuntimeDeliveryConsumer {
    const port = this.composition.runtimeDelivery;
    if (!port) {
      throw new Error("runtime delivery consumption is unavailable on this composition");
    }
    const inner = port.consume(options);
    let stopped = false;
    const wrapped: RuntimeDeliveryConsumer = {
      start: () => {
        if (stopped) throw new Error("runtime delivery consumer is stopped");
        inner.start();
      },
      stop: async (signal) => {
        if (stopped) return;
        stopped = true;
        this.runtimeConsumers.delete(wrapped);
        await inner.stop(signal);
      },
      history: (operationId) => inner.history(operationId),
      append: (operationId, type, payload, executionGrant) =>
        inner.append(operationId, type, payload, executionGrant),
      acknowledge: (deliveryIds) => inner.acknowledge(deliveryIds),
    };
    this.runtimeConsumers.add(wrapped);
    return wrapped;
  }

  private async open(): Promise<void> {
    if (this.connection === "connected") return;
    if (this.connectPromise) return this.connectPromise;
    if (!this.requestedConnection || !this.lifecycleAllowsConnection()) return;
    this.connectPromise = (async () => {
      await this.restore();
      if (!this.requestedConnection || !this.lifecycleAllowsConnection()) return;
      this.setConnection("connecting");
      this.beginRecoveryBuffer();
      try {
        const session = await this.composition.realtime.connect({
          onEvent: (raw, audience) => this.handleInbound(raw, audience),
          onState: (state) => this.handleRealtimeState(state),
        });
        if (!this.requestedConnection || !this.lifecycleAllowsConnection()) {
          try {
            await session.close();
          } finally {
            this.abortRecoveryBuffer();
            this.setConnection("disconnected");
          }
          return;
        }
        this.session = session;
        await this.recoverPrivateDirectoryProjection();
        if (
          this.session !== session ||
          !this.requestedConnection ||
          !this.lifecycleAllowsConnection()
        ) {
          if (this.session === session) {
            this.session = undefined;
            await session.close();
          }
          this.abortRecoveryBuffer();
          this.setConnection("disconnected");
          return;
        }
        this.recoveryError = undefined;
        this.setConnection("connected");
      } catch (error) {
        this.abortRecoveryBuffer();
        this.recoveryError = errorMessage(error);
        this.setConnection("failed");
        throw error;
      }
    })().finally(() => { this.connectPromise = undefined; });
    return this.connectPromise;
  }

  private lifecycleAllowsConnection(): boolean {
    const lifecycle = this.composition.lifecycle;
    return !lifecycle || (lifecycle.current() === "active" && lifecycle.isOnline());
  }

  private startLifecycle(): void {
    if (this.stopLifecycle || !this.composition.lifecycle) return;
    this.stopLifecycle = this.composition.lifecycle.subscribe(() => {
      const lifecycle = this.composition.lifecycle;
      if (!lifecycle || !this.requestedConnection) return;
      if (lifecycle.current() === "active" && lifecycle.isOnline()) {
        void this.open().catch(() => undefined);
      } else {
        void this.retireSession();
      }
    });
  }

  private async retireSession(): Promise<void> {
    const session = this.session;
    this.session = undefined;
    if (session) await session.close();
    this.setConnection("disconnected");
  }

  listen(filter: RealtimeListenFilter, listener: EventListener): () => void {
    const id = this.nextListenerId++;
    this.listeners.set(id, { filter, listener });
    return () => this.listeners.delete(id);
  }

  getSnapshot(): MessageClientSnapshot {
    return this.snapshot;
  }

  subscribe(listener: StoreListener): () => void {
    this.storeListeners.add(listener);
    return () => this.storeListeners.delete(listener);
  }

  private watch(conversationId: string, agentId?: string): ConversationWatch {
    if (agentId) this.composition.conversationRoutes?.bindConversation(conversationId, agentId);
    const watch = this.watches.watch(conversationId);
    this.publishSnapshot();
    let released = false;
    return {
      ...watch,
      ready: watch.ready.then(() => {
        this.publishProjectionChange();
      }, (error) => {
        this.publishSnapshot();
        throw error;
      }),
      release: () => {
        if (released) return;
        released = true;
        watch.release();
        this.publishSnapshot();
      },
    };
  }

  private async send(command: SendMessageCommand, explicitRetry: boolean): Promise<SendMessageReceipt> {
    assertSend(command);
    if (command.agentId) {
      this.composition.conversationRoutes?.bindConversation(
        command.conversationId,
        command.agentId,
      );
    }
    if (explicitRetry) {
      const reconciled = await this.composition.messageQuery.reconcileMessage(command.conversationId, command.idempotencyKey);
      if (reconciled) {
        if (this.projection.reconcileOptimistic(command.clientMessageId, reconciled)) this.publishProjectionChange();
        return { messageId: reconciled.id, outcome: "duplicate" };
      }
    }
    const localId = this.putLocalMessage(command, "optimistic");
    try {
      const receipt = await this.composition.messageCommand.sendMessage(command);
      if (receipt.messageId !== command.clientMessageId) {
        throw new Error("message service must use clientMessageId/idempotencyKey as messageId");
      }
      if (receipt.outcome === "outcome_unknown") {
        this.putLocalMessage(command, "outcome_unknown");
      } else if (this.projection.reconcileOptimistic(localId, { id: receipt.messageId })) {
        this.publishProjectionChange();
      }
      return receipt;
    } catch (error) {
      if (error instanceof OutcomeUnknownError || errorName(error) === "OutcomeUnknownError") {
        this.putLocalMessage(command, "outcome_unknown");
      } else if (this.projection.removeMessage(localId)) {
        this.publishProjectionChange();
      }
      throw error;
    }
  }

  private async execute(command: ExecuteMethodCommand): Promise<ExecuteMethodReceipt> {
    if (!command.operationId || !command.instanceId || !command.method || !command.idempotencyKey) {
      throw new Error("operationId, instanceId, method and idempotencyKey are required");
    }
    const receipt = await this.composition.runtimeMethods.executeMethod(command);
    if (receipt.operationId !== command.operationId) {
      throw new Error("runtime method response operationId does not match the caller-owned operationId");
    }
    if (receipt.outcome === "outcome_unknown" && this.projection.putOperation({
      id: receipt.operationId,
      instanceId: command.instanceId,
      target: command.target,
      method: command.method,
      capability: command.method.split("/", 1)[0] || command.method,
      contractRevision: receipt.contractRevision,
      transport: "service",
      sequence: "0",
      status: "outcome_unknown",
      effectState: "outcome_unknown",
      terminal: true,
      error: { code: "OUTCOME_UNKNOWN" },
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      revision: "0",
    })) this.publishProjectionChange();
    return receipt;
  }

  private putLocalMessage(command: SendMessageCommand, state: "optimistic" | "outcome_unknown"): string {
    const now = new Date().toISOString();
    const conversation = this.projection.getSnapshot().conversations[command.conversationId];
    const content = command.content;
    const body = typeof content === "object" && content !== null && !Array.isArray(content)
      && typeof content.text === "string"
      ? content.text
      : "";
    const optimistic: MessageProjection = {
      id: command.clientMessageId,
      conversationId: command.conversationId,
      senderId: requiredCurrentPrincipalId(this.composition),
      type: command.type,
      body,
      content,
      ...(command.replyTo ? { replyTo: command.replyTo } : {}),
      state,
      historyGeneration: conversation?.historyGeneration ?? "0",
      offset: "0",
      revision: "0",
      clientMessageId: command.clientMessageId,
      idempotencyKey: command.idempotencyKey,
      createdAt: now,
      updatedAt: now,
    };
    if (this.projection.putOptimisticMessage(optimistic)) this.publishProjectionChange();
    return optimistic.id;
  }

  private handleInbound(
    raw: unknown,
    audience: RealtimeDeliveryAudience = { kind: "private-control" },
  ): void {
    if (this.recoveryBuffering) {
      this.bufferedInbound.push({ raw, audience });
      return;
    }
    this.processInbound(raw, audience);
  }

  private processInbound(raw: unknown, audience: RealtimeDeliveryAudience): void {
    let event: AnyRealtimeEventV1;
    try {
      event = decodeRealtimeEvent(raw);
    } catch (error) {
      this.failInbound(errorMessage(error));
      return;
    }
    if (!isAudienceCompatible(event, audience)) {
      this.failInbound(
        `realtime audience contract violation: ${event.type} on ${audience.kind}`,
      );
      return;
    }
    if (event.type === "runtime.dispatch.failed") {
      if (!this.ephemeralDedupe.accept(event)) return;
      if (this.recoveryBatching) {
        this.deferredListenerEvents.push(event);
      } else {
        this.dispatchEvent(event);
      }
      return;
    }
    const decision = evaluateScopedRealtimeEvent(this.scopedCursors, event, audience);
    if (decision.action === "ignore_stale") return;
    if (decision.action === "rebase") {
      if (this.recoveryBatching) {
        this.failInbound(`buffered realtime gap requires reconnect: ${decision.reason}`);
        return;
      }
      if (audience.kind === "private-control") {
        this.failInbound(`private-control realtime gap requires a fresh authoritative reconnect: ${decision.reason}`);
        return;
      }
      const conversationId = event.scope.conversationId;
      if (!conversationId) {
        this.recoveryError = `cannot recover realtime gap outside a conversation: ${decision.reason}`;
        this.setConnection("failed");
        return;
      }
      void this.watches.recoverScope(conversationId, event).then(({ commit, eventAccepted }) => {
        if (!commit || !eventAccepted) return;
        this.scopedCursors = withScopedRealtimeCursor(this.scopedCursors, audience, {
          streamSequence: event.ordering.streamSequence,
          historyGeneration: commit.conversation.historyGeneration,
          projectionUid: event.ordering.projectionUid,
          projectionEpoch: event.ordering.projectionEpoch,
        });
        // Recovery has already installed the authoritative projection. Persist
        // it with the admitted event cursor as one crash-consistent checkpoint.
        this.publishProjectionChange();
        if (
          !event.ordering.historyGeneration
          || event.ordering.historyGeneration === commit.conversation.historyGeneration
        ) {
          this.dispatchEvent(event);
        }
      }, (error) => {
        this.recoveryError = errorMessage(error);
        this.setConnection("failed");
      });
      return;
    }
    const before = this.projection.getSnapshot();
    this.scopedCursors = withScopedRealtimeCursor(this.scopedCursors, audience, decision.cursor);
    const accepted = this.watches.accept(event);
    if (this.projection.getSnapshot() !== before) {
      this.publishProjectionChange();
    } else {
      // Cursor admission precedes dedupe logically, but durability is never
      // split from the projection represented by that cursor.
      this.persistCheckpoint();
    }
    if (!accepted) return;
    if (
      event.type === "inbox.conversation.available"
      || event.type === "inbox.conversation.unavailable"
    ) {
      this.requestPrivateDirectoryRefresh();
    }
    if (this.recoveryBatching) {
      this.deferredListenerEvents.push(event);
      return;
    }
    this.dispatchEvent(event);
  }

  private dispatchEvent(event: AnyRealtimeEventV1): void {
    for (const { filter, listener } of this.listeners.values()) {
      if (!matches(event, filter)) continue;
      try {
        listener(event);
      } catch {
        // Observers cannot block another observer or SDK state progression.
      }
    }
  }

  private handleRealtimeState(state: RealtimeConnectionState): void {
    if (state === "disconnected" || state === "reconnecting") this.transportInterrupted = true;
    if (state !== "connected") this.setConnection(state);
    if (state === "connected" && this.recoveryBuffering) return;
    if (state === "connected" && this.transportInterrupted) {
      this.transportInterrupted = false;
      this.beginRecoveryBuffer();
      void this.recoverPrivateDirectoryProjection().then(
        () => this.setConnection("connected"),
        (error) => {
          this.abortRecoveryBuffer();
          this.recoveryError = errorMessage(error);
          this.setConnection("failed");
        },
      );
      return;
    }
    if (state === "connected") this.setConnection("connected");
  }

  private beginRecoveryBuffer(): void {
    this.recoveryBuffering = true;
    this.recoveryBatching = true;
  }

  private recoverPrivateDirectoryProjection(): Promise<void> {
    if (this.privateRecoveryPromise) return this.privateRecoveryPromise;
    this.privateRecoveryPromise = (async () => {
      do {
        this.privateDirectoryRefreshRequested = false;
        if (this.composition.privateConversationDirectoryQuery) {
          const conversations = await this.recovery.discoverPrivateDirectory();
          await this.watches.setDirectoryConversations(
            conversations.map((conversation) => conversation.id),
          );
          await this.recovery.recoverPrivateDirectoryProjection(conversations);
        } else {
          await this.watches.reauthorizeAll();
          await this.watches.recoverAll();
        }
        this.recoveryDirty = true;
        while (this.bufferedInbound.length > 0) {
          const batch = this.bufferedInbound.splice(0);
          for (const { raw, audience } of batch) this.processInbound(raw, audience);
        }
      } while (this.privateDirectoryRefreshRequested);
      this.recoveryBuffering = false;
      this.recoveryBatching = false;
      if (this.recoveryDirty) {
        this.recoveryDirty = false;
        this.publishProjectionChange();
      }
      for (const event of this.deferredListenerEvents.splice(0)) this.dispatchEvent(event);
    })().finally(() => { this.privateRecoveryPromise = undefined; });
    return this.privateRecoveryPromise;
  }

  private requestPrivateDirectoryRefresh(): void {
    if (!this.composition.privateConversationDirectoryQuery) {
      if (this.composition.conversationRoutes) {
        // Mobile has no login-wide agent directory. The control remains a
        // discovery hint until listForAgent/watch supplies explicit routing.
        return;
      }
      this.failInbound("private directory control requires an authoritative directory port");
      return;
    }
    this.privateDirectoryRefreshRequested = true;
    if (this.privateRecoveryPromise) return;
    this.beginRecoveryBuffer();
    void this.recoverPrivateDirectoryProjection().catch((error) => {
      this.abortRecoveryBuffer();
      this.recoveryError = errorMessage(error);
      this.setConnection("failed");
    });
  }

  private abortRecoveryBuffer(): void {
    this.recoveryBuffering = false;
    this.recoveryBatching = false;
    this.recoveryDirty = false;
    this.bufferedInbound.splice(0);
    this.deferredListenerEvents.splice(0);
  }

  private failInbound(message: string): never | void {
    if (this.recoveryBatching) throw new Error(message);
    this.recoveryError = message;
    this.setConnection("failed");
  }

  private async restore(): Promise<void> {
    if (this.restored) return;
    this.restored = true;
    const persisted = await this.composition.projectionStore?.loadCheckpoint();
    if (!persisted) return;
    this.scopedCursors = persisted.cursors;
    this.projection.replace(persisted.projection);
    this.publishSnapshot();
  }

  private persistCheckpoint(): void {
    if (this.recoveryBatching) {
      this.recoveryDirty = true;
      return;
    }
    const store = this.composition.projectionStore;
    if (!store) return;
    const checkpoint = {
      projection: this.projection.getSnapshot(),
      cursors: this.scopedCursors,
    };
    this.checkpointWrite = this.checkpointWrite
      .then(() => store.commitCheckpoint(checkpoint))
      .catch((error) => {
        this.recoveryError = errorMessage(error);
        this.setConnection("failed");
      });
  }

  private publishProjectionChange(): void {
    if (this.recoveryBatching) {
      this.recoveryDirty = true;
      return;
    }
    this.persistCheckpoint();
    this.publishSnapshot();
  }

  private setConnection(connection: RealtimeConnectionState): void {
    if (this.connection === connection) return;
    this.connection = connection;
    this.publishSnapshot();
  }

  private publishSnapshot(): void {
    const next = this.makeSnapshot();
    if (samePublicSnapshot(this.snapshot, next)) return;
    this.snapshot = next;
    for (const listener of this.storeListeners) listener();
  }

  private makeSnapshot(): MessageClientSnapshot {
    const projection = this.projection.getSnapshot();
    return Object.freeze({
      ...projection,
      connection: this.connection,
      watchedConversationIds: Object.freeze(this.watches?.watchedConversationIds() ?? []),
      ...(this.recoveryError ? { recoveryError: this.recoveryError } : {}),
    });
  }
}

export function createMessageClient(composition: MessageClientComposition): MessageClient {
  return new UnifiedMessageClient(composition);
}

function matches(event: AnyRealtimeEventV1, filter: RealtimeListenFilter): boolean {
  if (filter.eventTypes && !filter.eventTypes.includes(event.type)) return false;
  if (filter.conversationIds && (!event.scope.conversationId || !filter.conversationIds.includes(event.scope.conversationId))) return false;
  if (filter.instanceIds && (!event.scope.instanceId || !filter.instanceIds.includes(event.scope.instanceId))) return false;
  if (filter.agentIds && (!event.scope.agentId || !filter.agentIds.includes(event.scope.agentId))) return false;
  return true;
}

function isAudienceCompatible(
  event: AnyRealtimeEventV1,
  audience: RealtimeDeliveryAudience,
): boolean {
  if (audience.kind === "private-control") {
    return event.scope.conversationId === undefined;
  }
  return event.scope.conversationId === audience.conversationId;
}

function samePublicSnapshot(left: MessageClientSnapshot, right: MessageClientSnapshot): boolean {
  return left.connection === right.connection
    && left.recoveryError === right.recoveryError
    && left.conversations === right.conversations
    && left.messages === right.messages
    && left.operations === right.operations
    && left.latestOffsetByConversation === right.latestOffsetByConversation
    && left.hydrationByConversation === right.hydrationByConversation
    && left.watchedConversationIds.length === right.watchedConversationIds.length
    && left.watchedConversationIds.every((id, index) => id === right.watchedConversationIds[index]);
}

function assertSend(command: SendMessageCommand): void {
  if (!command.conversationId || !command.clientMessageId || !command.idempotencyKey || !command.type) {
    throw new Error("conversationId, clientMessageId, idempotencyKey and type are required");
  }
  if (command.clientMessageId !== command.idempotencyKey) {
    throw new Error("clientMessageId and idempotencyKey must be identical");
  }
}

function errorName(error: unknown): string | undefined {
  return typeof error === "object" && error !== null && "name" in error
    ? String((error as { name: unknown }).name)
    : undefined;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "message-sdk operation failed";
}

function requiredCurrentPrincipalId(composition: MessageClientComposition): string {
  const principalId = composition.currentPrincipal.currentPrincipalId().trim();
  if (!principalId) throw new Error("message-sdk current principal is not resolved");
  return principalId;
}
