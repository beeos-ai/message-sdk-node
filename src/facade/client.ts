import {
  decodeRealtimeEvent,
  evaluateRealtimeEvent,
  RecoveryOwnership,
  RealtimeDedupe,
  SingleflightHydrator,
  type AnyRealtimeEventV1,
  type RealtimeCursor,
} from "../protocol/index.js";
import type {
  AppStatePort,
  ConversationHydration,
  ConversationWatch,
  ExecuteMethodInput,
  ExecuteMethodResult,
  MessageClientFacadeOptions,
  MessageClientFacadeSnapshot,
  NetworkPort,
  RealtimeCheckpoint,
  RealtimeConnectionState,
  RealtimeListenFilter,
  RealtimeRecoveryStatus,
  RealtimeRebaseReason,
  RealtimeSession,
  SendMessageInput,
  SendMessageResult,
} from "./types.js";

type Listener = (event: AnyRealtimeEventV1) => void;
type StoreListener = () => void;

/**
 * Cross-platform message/realtime orchestration.
 *
 * It owns local filtering, dedupe, watch ref-counting, the single recovery
 * lease, and the private recovery checkpoint. It intentionally cannot
 * construct a URL, WebSocket, Centrifuge channel, token, fetch request,
 * EventSource, ACP client, or fallback path.
 */
export class MessageClientFacade {
  readonly messages: {
    send: (input: SendMessageInput) => Promise<SendMessageResult>;
  };
  readonly methods: {
    execute: (input: ExecuteMethodInput) => Promise<ExecuteMethodResult>;
  };
  readonly conversations: {
    watch: (conversationId: string) => ConversationWatch;
  };

  private readonly dedupe = new RealtimeDedupe();
  private readonly hydration = new SingleflightHydrator();
  private readonly recoveryOwnership = new RecoveryOwnership();
  private readonly listeners = new Map<number, { filter: RealtimeListenFilter; listener: Listener }>();
  private readonly storeListeners = new Set<StoreListener>();
  private readonly watches = new Map<string, { refs: number; ready: Promise<ConversationHydration> }>();
  private readonly cursorKey: string;
  private nextListenerId = 1;
  private cursor?: RealtimeCursor;
  private syncCursor?: string;
  private checkpoint?: RealtimeCheckpoint;
  private checkpointRestored = false;
  private checkpointWrite: Promise<void> = Promise.resolve();
  private connection: RealtimeConnectionState = "disconnected";
  private session?: RealtimeSession;
  private connectPromise?: Promise<void>;
  private rebasePromise?: Promise<void>;
  private requestedConnection = false;
  private lifecycleStarted = false;
  private rebasePaused = false;
  private pendingTransportRebase = false;
  private readonly bufferedInbound: AnyRealtimeEventV1[] = [];
  private recoveryError?: string;
  private stopAppState?: () => void;
  private stopNetwork?: () => void;
  private snapshot: MessageClientFacadeSnapshot = {
    connection: "disconnected",
    watchedConversationIds: [],
  };

  constructor(private readonly options: MessageClientFacadeOptions) {
    this.cursorKey = options.cursorStorageKey ?? "beeos.message-sdk.realtime.cursor.v1";
    this.messages = { send: (input) => this.sendMessage(input) };
    this.methods = { execute: (input) => this.executeMethod(input) };
    this.conversations = { watch: (conversationId) => this.watchConversation(conversationId) };
  }

  async connect(): Promise<void> {
    this.requestedConnection = true;
    this.startLifecyclePorts();
    if (this.connection === "connected") return;
    if (this.connectPromise) return this.connectPromise;
    if (!this.isOnline()) throw new Error("message-sdk realtime connect requires an online network");

    this.connectPromise = (async () => {
      await this.restoreCheckpoint();
      this.setConnection("reconnecting");
      this.session = await this.options.realtime.connect({
        cursor: this.cursor,
        syncCursor: this.syncCursor,
        onEvent: (event) => this.handleInbound(event),
        onState: (state) => this.handleRealtimeState(state),
        onRecovery: (status) => this.handleTransportRecovery(status),
      });
      // A reconnect must not replace a persisted opaque cursor with a server
      // bootstrap cursor. The backend may issue a new token while its sync
      // endpoint still expects the prior cursor; only a completed authoritative
      // rebase advances that cursor.
      await this.updateCheckpoint(this.cursor, this.syncCursor ?? this.session.syncCursor);
      this.recoveryError = undefined;
      this.setConnection("connected");
      if (this.pendingTransportRebase) {
        this.pendingTransportRebase = false;
        this.requestRebase("transport_unrecovered");
      }
    })().finally(() => {
      this.connectPromise = undefined;
    });
    return this.connectPromise;
  }

  async disconnect(): Promise<void> {
    this.requestedConnection = false;
    await this.closeSession();
    this.stopAppState?.();
    this.stopAppState = undefined;
    this.stopNetwork?.();
    this.stopNetwork = undefined;
    this.lifecycleStarted = false;
  }

  listen(filter: RealtimeListenFilter, listener: Listener): () => void {
    const id = this.nextListenerId++;
    this.listeners.set(id, { filter, listener });
    return () => this.listeners.delete(id);
  }

  /** Stable object identity until a state change, suitable for useSyncExternalStore. */
  getSnapshot(): MessageClientFacadeSnapshot {
    return this.snapshot;
  }

  subscribe(listener: StoreListener): () => void {
    this.storeListeners.add(listener);
    return () => this.storeListeners.delete(listener);
  }

  private async sendMessage(input: SendMessageInput): Promise<SendMessageResult> {
    assertRequired(input.conversationId, "conversationId");
    assertRequired(input.clientMessageId, "clientMessageId");
    assertValidSendMessageInput(input);
    const result = await this.options.transport.sendMessage(input);
    // HTTP/WSS create race: suppress only the matching created echo, never
    // future deltas or terminal events for the same message.
    this.dedupe.recordHttpMessageCreated(result.messageId);
    return result;
  }

  private async executeMethod(input: ExecuteMethodInput): Promise<ExecuteMethodResult> {
    assertRequired(input.instanceId, "instanceId");
    assertRequired(input.method, "method");
    assertRequired(input.idempotencyKey, "idempotencyKey");
    return this.options.transport.executeMethod(input);
  }

  private watchConversation(conversationId: string): ConversationWatch {
    assertRequired(conversationId, "conversationId");
    const existing = this.watches.get(conversationId);
    if (existing) {
      existing.refs++;
      this.publishSnapshot();
      return { conversationId, ready: existing.ready, release: () => this.releaseWatch(conversationId) };
    }
    const ready = this.hydrateConversation(conversationId);
    this.watches.set(conversationId, { refs: 1, ready });
    this.publishSnapshot();
    return { conversationId, ready, release: () => this.releaseWatch(conversationId) };
  }

  private releaseWatch(conversationId: string): void {
    const entry = this.watches.get(conversationId);
    if (!entry) return;
    entry.refs--;
    if (entry.refs <= 0) this.watches.delete(conversationId);
    this.publishSnapshot();
  }

  private hydrateConversation(conversationId: string): Promise<ConversationHydration> {
    return this.hydration.hydrate(`conversation:${conversationId}`, async () => {
      const hydrated = await this.options.transport.hydrateConversation({
        conversationId,
        cursor: this.cursor,
      });
      for (const event of orderEvents(hydrated.events)) this.applyEvent(event, true);
      if (hydrated.cursor) await this.updateCheckpoint(hydrated.cursor, this.syncCursor);
      return hydrated;
    });
  }

  private handleInbound(raw: unknown): void {
    let event: AnyRealtimeEventV1;
    try {
      event = decodeRealtimeEvent(raw);
    } catch (error) {
      this.failRecovery(error);
      return;
    }
    if (this.rebasePaused) {
      this.bufferedInbound.push(event);
      return;
    }
    this.processInbound(event);
  }

  private processInbound(event: AnyRealtimeEventV1): void {
    const decision = evaluateRealtimeEvent(this.cursor, event);
    if (decision.action === "ignore_stale") return;
    if (decision.action === "rebase") {
      // Do not mark a gap event as seen before an authoritative rebase: it
      // must be replayed only if that rebase did not already contain it.
      this.bufferedInbound.push(event);
      this.requestRebase(decision.reason);
      return;
    }
    if (!this.dedupe.accept(event)) return;
    this.applyEvent(event, true);
    void this.updateCheckpoint(decision.cursor, this.syncCursor).catch((error) => this.failRecovery(error));
    if (event.scope.conversationId && !this.watches.has(event.scope.conversationId)) {
      // GetStream-style unknown-channel notification handling: one hydrate,
      // regardless of concurrent message/member events.
      void this.hydrateConversation(event.scope.conversationId).catch(() => undefined);
    }
  }

  private requestRebase(reason: RealtimeRebaseReason): void {
    void this.rebase(reason).catch((error) => this.failRecovery(error));
  }

  private async rebase(reason: RealtimeRebaseReason): Promise<void> {
    if (this.rebasePromise) return this.rebasePromise;
    this.rebasePaused = true;
    const release = this.recoveryOwnership.acquire("message-sdk-facade");
    const run = (async () => {
      try {
        const rebase = await this.hydration.hydrate("realtime:rebase", () =>
          this.options.transport.rebase({ cursor: this.cursor, syncCursor: this.syncCursor, reason }),
        );
        // The HTTP transport returns only a fully collected, validated delta
        // chain. Generation and epoch changes still require a separately
        // documented authoritative snapshot and fail closed rather than being
        // guessed from delta fields.
        const events = orderEvents(rebase.events);
        const cursor = rebase.cursor ?? cursorFromEvents(events) ?? this.cursor;
        const syncCursor = rebase.syncCursor ?? this.syncCursor;

        // No realtime event can interleave with this checkpoint+projection
        // batch. Consumers see the authoritative event order before buffered
        // newer events are evaluated and deduplicated.
        await this.updateCheckpoint(cursor, syncCursor);
        this.dedupe.clear();
        for (const event of events) {
          if (this.dedupe.accept(event)) this.applyEvent(event, true);
        }
      } finally {
        release();
      }
    })();
    this.rebasePromise = run;
    try {
      await run;
      this.rebasePaused = false;
      this.drainBufferedInbound();
    } finally {
      if (this.rebasePromise === run) this.rebasePromise = undefined;
    }
  }

  private drainBufferedInbound(): void {
    // Preserve transport arrival order. Each item is evaluated against the
    // freshly committed authoritative cursor; a remaining gap starts exactly
    // one subsequent singleflight rebase.
    const buffered = this.bufferedInbound.splice(0);
    for (const event of buffered) {
      if (this.rebasePaused) {
        this.bufferedInbound.push(event);
        continue;
      }
      this.processInbound(event);
    }
  }

  private handleTransportRecovery(status: RealtimeRecoveryStatus): void {
    if (status.recoverable && status.recovered && status.positioned) return;
    // Centrifuge may emit subscribed before connect() resolves. Hold the
    // signal until the private session cursor is available.
    if (!this.session) {
      this.pendingTransportRebase = true;
      return;
    }
    this.requestRebase("transport_unrecovered");
  }

  private applyEvent(event: AnyRealtimeEventV1, bypassDedupe: boolean): void {
    if (!bypassDedupe && !this.dedupe.accept(event)) return;
    for (const { filter, listener } of this.listeners.values()) {
      if (!matchesFilter(event, filter)) continue;
      // A consumer is observational only. Its exception must not prevent
      // another consumer from seeing the event or prevent cursor persistence.
      try {
        listener(event);
      } catch {
        // Intentionally isolated; platform adapters can add logging outside
        // this pure facade without coupling state progression to observers.
      }
    }
  }

  private async restoreCheckpoint(): Promise<void> {
    if (this.checkpointRestored) return;
    this.checkpointRestored = true;
    if (!this.options.storage) return;
    const checkpoint = await this.options.storage.getCheckpoint?.(this.cursorKey);
    if (checkpoint) {
      this.checkpoint = cloneCheckpoint(checkpoint);
      this.cursor = checkpoint.eventCursor;
      this.syncCursor = checkpoint.syncCursor;
      this.publishSnapshot();
      return;
    }
    const legacyCursor = await this.options.storage.get(this.cursorKey);
    if (legacyCursor) {
      this.checkpoint = { eventCursor: legacyCursor };
      this.cursor = legacyCursor;
      this.publishSnapshot();
    }
  }

  private async updateCheckpoint(eventCursor: RealtimeCursor | undefined, syncCursor: string | undefined): Promise<void> {
    const current = this.checkpoint ?? {};
    const nextCursor = newerCursor(current.eventCursor, eventCursor);
    const nextSyncCursor = chooseSyncCursor(current, nextCursor, syncCursor);
    const next: RealtimeCheckpoint = {
      ...(nextCursor ? { eventCursor: nextCursor } : {}),
      ...(nextSyncCursor ? { syncCursor: nextSyncCursor } : {}),
    };
    if (sameCheckpoint(current, next)) return this.checkpointWrite;

    this.checkpoint = cloneCheckpoint(next);
    this.cursor = next.eventCursor;
    this.syncCursor = next.syncCursor;
    this.publishSnapshot();
    this.checkpointWrite = this.checkpointWrite.then(async () => {
      if (!this.options.storage) return;
      if (this.options.storage.setCheckpoint) {
        await this.options.storage.setCheckpoint(this.cursorKey, cloneCheckpoint(next));
        return;
      }
      if (next.eventCursor) await this.options.storage.set(this.cursorKey, next.eventCursor);
    });
    return this.checkpointWrite;
  }

  private handleRealtimeState(state: RealtimeConnectionState): void {
    this.setConnection(state);
    // `disconnected` is a Centrifuge transient state. The same WSS client
    // owns reconnect; replacing it here races its recovery and creates a
    // second physical connection. Only an explicit close or terminal failed
    // state retires the transport.
    if (state === "failed") this.session = undefined;
  }

  private failRecovery(error: unknown): void {
    this.recoveryError = error instanceof Error ? error.message : "message-sdk realtime recovery failed";
    this.rebasePaused = true;
    this.setConnection("failed");
  }

  private startLifecyclePorts(): void {
    if (this.lifecycleStarted) return;
    this.lifecycleStarted = true;
    this.stopAppState = subscribeAppState(this.options.appState, (state) => {
      if (state === "active" && this.requestedConnection && this.isOnline()) {
        void this.connect().catch(() => undefined);
      }
      if (state !== "active") void this.closeSession();
    });
    this.stopNetwork = subscribeNetwork(this.options.network, (online) => {
      if (!online) {
        void this.closeSession();
      } else if (this.requestedConnection) {
        void this.connect().catch(() => undefined);
      }
    });
  }

  private async closeSession(): Promise<void> {
    const session = this.session;
    this.session = undefined;
    if (session) await session.close();
    this.setConnection("disconnected");
  }

  private isOnline(): boolean {
    return this.options.network?.isOnline() ?? true;
  }

  private setConnection(connection: RealtimeConnectionState): void {
    if (this.connection === connection) return;
    this.connection = connection;
    this.publishSnapshot();
  }

  private publishSnapshot(): void {
    this.snapshot = {
      connection: this.connection,
      cursor: this.cursor,
      recoveryError: this.recoveryError,
      watchedConversationIds: [...this.watches.keys()].sort(),
    };
    for (const listener of this.storeListeners) listener();
  }
}

export function createMessageClient(options: MessageClientFacadeOptions): MessageClientFacade {
  return new MessageClientFacade(options);
}

function matchesFilter(event: AnyRealtimeEventV1, filter: RealtimeListenFilter): boolean {
  if (filter.eventTypes && !filter.eventTypes.includes(event.type)) return false;
  if (filter.conversationIds && (!event.scope.conversationId || !filter.conversationIds.includes(event.scope.conversationId))) return false;
  if (filter.instanceIds && (!event.scope.instanceId || !filter.instanceIds.includes(event.scope.instanceId))) return false;
  if (filter.agentIds && (!event.scope.agentId || !filter.agentIds.includes(event.scope.agentId))) return false;
  return true;
}

function assertRequired(value: string, name: string): void {
  if (!value) throw new Error(`${name} is required`);
}

function assertValidSendMessageInput(input: SendMessageInput): void {
  const raw = input as SendMessageInput & { text?: unknown; content?: unknown; type?: unknown; parts?: unknown };
  const hasText = Object.prototype.hasOwnProperty.call(raw, "text");
  const hasContent = Object.prototype.hasOwnProperty.call(raw, "content");
  const hasType = Object.prototype.hasOwnProperty.call(raw, "type");
  const hasParts = Object.prototype.hasOwnProperty.call(raw, "parts");
  if (hasText) {
    if (typeof raw.text !== "string") throw new Error("text must be a string");
    if (hasContent || hasType) throw new Error("text send cannot include content or type");
    if (hasParts && !Array.isArray(raw.parts)) throw new Error("text send parts must be an array");
    return;
  }
  if (!hasContent || !hasType || typeof raw.type !== "string" || !raw.type) {
    throw new Error("content send requires explicit non-empty type and content");
  }
  if (hasParts) throw new Error("content send cannot include parts; include them inside content");
}

function subscribeAppState(port: AppStatePort | undefined, listener: (state: "active" | "background" | "inactive") => void): (() => void) | undefined {
  return port?.subscribe(listener);
}

function subscribeNetwork(port: NetworkPort | undefined, listener: (online: boolean) => void): (() => void) | undefined {
  return port?.subscribe(listener);
}

function orderEvents(events: readonly AnyRealtimeEventV1[]): AnyRealtimeEventV1[] {
  return [...events].sort((left, right) => {
    const leftSequence = BigInt(left.ordering.streamSequence);
    const rightSequence = BigInt(right.ordering.streamSequence);
    return leftSequence < rightSequence ? -1 : leftSequence > rightSequence ? 1 : 0;
  });
}

function cursorFromEvents(events: readonly AnyRealtimeEventV1[]): RealtimeCursor | undefined {
  const latest = orderEvents(events).at(-1);
  if (!latest) return undefined;
  return {
    streamSequence: latest.ordering.streamSequence,
    historyGeneration: latest.ordering.historyGeneration,
    projectionUid: latest.ordering.projectionUid,
    projectionEpoch: latest.ordering.projectionEpoch,
  };
}

function newerCursor(current: RealtimeCursor | undefined, incoming: RealtimeCursor | undefined): RealtimeCursor | undefined {
  if (!current) return incoming;
  if (!incoming) return current;
  return BigInt(incoming.streamSequence) >= BigInt(current.streamSequence) ? incoming : current;
}

function chooseSyncCursor(
  current: RealtimeCheckpoint,
  selectedCursor: RealtimeCursor | undefined,
  incomingSyncCursor: string | undefined,
): string | undefined {
  if (!incomingSyncCursor) return current.syncCursor;
  if (!current.eventCursor || !selectedCursor) return incomingSyncCursor;
  // Never associate an older incoming cursor with a newer durable event
  // checkpoint. Equal/newer cursors are allowed to roll the opaque token.
  if (BigInt(selectedCursor.streamSequence) >= BigInt(current.eventCursor.streamSequence)) return incomingSyncCursor;
  return current.syncCursor;
}

function sameCheckpoint(left: RealtimeCheckpoint, right: RealtimeCheckpoint): boolean {
  return left.syncCursor === right.syncCursor
    && left.eventCursor?.streamSequence === right.eventCursor?.streamSequence
    && left.eventCursor?.historyGeneration === right.eventCursor?.historyGeneration
    && left.eventCursor?.projectionUid === right.eventCursor?.projectionUid
    && left.eventCursor?.projectionEpoch === right.eventCursor?.projectionEpoch;
}

function cloneCheckpoint(checkpoint: RealtimeCheckpoint): RealtimeCheckpoint {
  return {
    ...(checkpoint.eventCursor ? { eventCursor: { ...checkpoint.eventCursor } } : {}),
    ...(checkpoint.syncCursor ? { syncCursor: checkpoint.syncCursor } : {}),
  };
}
