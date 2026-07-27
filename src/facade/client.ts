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
  RealtimeConnectionState,
  RealtimeListenFilter,
  RealtimeSession,
  SendMessageInput,
  SendMessageResult,
} from "./types.js";

type Listener = (event: AnyRealtimeEventV1) => void;
type StoreListener = () => void;

/**
 * Cross-platform message/realtime orchestration.
 *
 * It owns local filtering, dedupe, watch ref-counting and the single recovery
 * lease. It intentionally cannot construct a URL, WebSocket, Centrifuge
 * channel, token, fetch request, EventSource, ACP client, or fallback path.
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
  private connection: RealtimeConnectionState = "disconnected";
  private session?: RealtimeSession;
  private connectPromise?: Promise<void>;
  private rebasePromise?: Promise<void>;
  private requestedConnection = false;
  private lifecycleStarted = false;
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
      if (!this.cursor && this.options.storage) {
        this.cursor = await this.options.storage.get(this.cursorKey);
      }
      this.setConnection("reconnecting");
      this.session = await this.options.realtime.connect({
        cursor: this.cursor,
        onEvent: (event) => this.handleInbound(event),
        onState: (state) => this.handleRealtimeState(state),
      });
      this.syncCursor = this.session.syncCursor ?? this.syncCursor;
      this.setConnection("connected");
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
    assertRequired(input.type, "type");
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
      for (const event of hydrated.events) this.applyEvent(event, true);
      if (hydrated.cursor) await this.setCursor(hydrated.cursor);
      return hydrated;
    });
  }

  private handleInbound(raw: unknown): void {
    const event = decodeRealtimeEvent(raw);
    if (!this.dedupe.accept(event)) return;
    const decision = evaluateRealtimeEvent(this.cursor, event);
    if (decision.action === "ignore_stale") return;
    if (decision.action === "rebase") {
      void this.rebase(decision.reason);
      return;
    }
    this.applyEvent(event, true);
    void this.setCursor(decision.cursor);
    if (event.scope.conversationId && !this.watches.has(event.scope.conversationId)) {
      // GetStream-style unknown-channel notification handling: one hydrate,
      // regardless of concurrent message/member events.
      void this.hydrateConversation(event.scope.conversationId).catch(() => undefined);
    }
  }

  private async rebase(reason: "sequence_gap" | "history_generation_changed" | "projection_epoch_changed"): Promise<void> {
    if (this.rebasePromise) return this.rebasePromise;
    const release = this.recoveryOwnership.acquire("message-sdk-facade");
    const run = (async () => {
      try {
        const rebase = await this.hydration.hydrate("realtime:rebase", () =>
          this.options.transport.rebase({ cursor: this.cursor, syncCursor: this.syncCursor, reason }),
        );
        for (const event of rebase.events) this.applyEvent(event, true);
        if (rebase.cursor) await this.setCursor(rebase.cursor);
        this.syncCursor = rebase.syncCursor ?? this.syncCursor;
      } finally {
        release();
      }
    })();
    this.rebasePromise = run.finally(() => {
      this.rebasePromise = undefined;
    });
    return this.rebasePromise;
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

  private async setCursor(cursor: RealtimeCursor): Promise<void> {
    this.cursor = cursor;
    this.publishSnapshot();
    if (this.options.storage) await this.options.storage.set(this.cursorKey, cursor);
  }

  private handleRealtimeState(state: RealtimeConnectionState): void {
    this.setConnection(state);
    if (state === "disconnected") {
      this.session = undefined;
      if (this.requestedConnection && this.isOnline()) void this.connect().catch(() => undefined);
    }
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

function subscribeAppState(port: AppStatePort | undefined, listener: (state: "active" | "background" | "inactive") => void): (() => void) | undefined {
  return port?.subscribe(listener);
}

function subscribeNetwork(port: NetworkPort | undefined, listener: (online: boolean) => void): (() => void) | undefined {
  return port?.subscribe(listener);
}
