import type { MessageJson } from "./types.js";

/**
 * The server-authoritative fence for one runtime process binding.
 *
 * This is intentionally not a user identity, provider session identifier, or
 * Centrifugo audience. A replacement runtime must use a new epoch.
 */
export interface RuntimeBindingIdentity {
  readonly runtimeBindingId: string;
  readonly runtimeEpoch: string;
}

/** A server wake is only a hint to perform durable HTTPS recovery. */
export type RuntimeBindingWakeNotification = Readonly<{
  type: "message.available" | "operation.available";
  runtimeEpoch: string;
}>;

/**
 * A recovery wake is emitted locally after initial WSS connect or reconnect.
 * It contains no transport material and requires the holder to use the
 * durable recovery port before acknowledging it.
 */
export type RuntimeBindingRecoveryWake = Readonly<{
  type: "recovery.required";
  runtimeEpoch: string;
  reason: "startup" | "reconnect";
}>;

/** The only events a runtime-binding consumer can observe. */
export type RuntimeBindingWakeEvent = RuntimeBindingWakeNotification | RuntimeBindingRecoveryWake;

export type RuntimeBindingWakeListener = (event: RuntimeBindingWakeEvent) => void;

/**
 * Lifecycle of the one SDK-owned WSS for a runtime binding.
 *
 * `recovering` remains visible until the caller has completed durable HTTPS
 * recovery and calls markDurableRecoveryComplete(). There is no alternate
 * transport, retry loop, or fallback state.
 */
export type RuntimeBindingConnectionState =
  | "disconnected"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "recovering"
  | "failed";

/** A WSS session exposes only close; token, URL and channel stay private. */
export interface RuntimeBindingWakeSession {
  close(): Promise<void> | void;
}

/**
 * Internal WSS transport seam for a future server-issued runtime session.
 *
 * The transport receives the binding fence, but cannot expose a raw client,
 * subscription, publish method, token, channel, audience, or private stream
 * through this API. It must retain one physical native WSS while reporting a
 * transient reconnect.
 */
export interface RuntimeBindingWakeTransportPort {
  connect(input: RuntimeBindingWakeTransportConnectInput): Promise<RuntimeBindingWakeSession>;
}

export interface RuntimeBindingWakeTransportConnectInput {
  readonly binding: RuntimeBindingIdentity;
  onWake(event: RuntimeBindingWakeNotification): void;
  onState(state: RuntimeBindingConnectionState): void;
}

/** Backend V2ConversationResponse mapped to the public TypeScript casing. */
export interface RuntimeBindingConversation {
  readonly id: string;
  readonly participants: readonly string[];
  readonly metadata?: Readonly<Record<string, string>>;
  readonly ownerIdentityId?: string;
  readonly targetIdentityId?: string;
  readonly targetKind?: string;
  readonly title?: string;
  readonly metadataVersion: number;
  readonly lastActivityAt?: string;
  readonly state: "open" | "closed";
  readonly closedReason?: string;
  readonly singleShot?: boolean;
  readonly deadlineAt?: string;
  readonly createdAt: string;
  readonly closedAt?: string;
}

/** Backend V2MessageResponse mapped to the public TypeScript casing. */
export interface RuntimeBindingMessage {
  readonly id: string;
  readonly conversationId: string;
  readonly type: string;
  readonly content: MessageJson;
  readonly sender: string;
  readonly replyTo?: string;
  readonly createdAt: string;
  readonly updatedAt?: string;
  readonly state?: string;
  readonly stopReason?: string;
  readonly body?: string;
  readonly parts?: readonly MessageJson[];
}

export interface RuntimeBindingConversationPage {
  readonly conversations: readonly RuntimeBindingConversation[];
  readonly nextCursor?: string;
  readonly hasMore: boolean;
}

export interface RuntimeBindingMessagePage {
  readonly messages: readonly RuntimeBindingMessage[];
  readonly nextCursor?: string;
  readonly hasMore: boolean;
}

export interface RuntimeBindingListOpenConversationsInput {
  readonly cursor?: string;
  readonly limit: number;
}

export interface RuntimeBindingListUnhandledMessagesInput {
  readonly conversationId: string;
  readonly cursor?: string;
  readonly limit: number;
}

/**
 * The binding-scoped durable recovery HTTPS seam.
 *
 * There is deliberately no MessageServiceHttpTransport implementation today:
 * backend/services/message/pkg/infrastructure/server/http/rest_handler_v2.go
 * registers only identity-scoped conversation and message list routes, and
 * realtime_handler.go issues sessions only from IdentityIDFromCtx. Although
 * backend/services/message/pkg/infrastructure/centrifugo/realtime_session.go
 * has runtime token primitives, no authenticated runtime session/recovery
 * HTTP contract exists to bind these reads to runtimeBindingId + runtimeEpoch.
 * Implementing guessed paths or using an arbitrary identity route here would
 * be a confused-deputy fallback. Add the server contract first, then provide
 * a concrete HTTPS adapter for this port.
 */
export interface RuntimeBindingDurableRecoveryPort {
  listOpenConversations(
    binding: RuntimeBindingIdentity,
    input: RuntimeBindingListOpenConversationsInput,
  ): Promise<RuntimeBindingConversationPage>;

  listUnhandledBy(
    binding: RuntimeBindingIdentity,
    input: RuntimeBindingListUnhandledMessagesInput,
  ): Promise<RuntimeBindingMessagePage>;
}

export interface RuntimeBindingWakeWatch {
  /** Resolves once the one WSS has been opened; recovery may still be pending. */
  readonly ready: Promise<void>;
  /** Idempotent. The final release closes the WSS and removes the listener. */
  release(): void;
}

export interface RuntimeBindingMessageClientSnapshot {
  readonly binding: RuntimeBindingIdentity;
  readonly connection: RuntimeBindingConnectionState;
  readonly recoveryPending: boolean;
  readonly wakeListenerCount: number;
  readonly recoveryError?: string;
}

export interface RuntimeBindingMessageClientOptions {
  /** Explicit, epoch-fenced identity of this one client instance. */
  readonly binding: RuntimeBindingIdentity;
  readonly wakeTransport: RuntimeBindingWakeTransportPort;
  readonly durableRecovery: RuntimeBindingDurableRecoveryPort;
}

type StoreListener = () => void;

/**
 * Runtime-binding v2 facade.
 *
 * Each instance manages at most one physical WSS at a time. WSS publications
 * are reduced to typed wake hints; the caller performs all durable reads via
 * recovery and acknowledges the result. Replies intentionally stay on the
 * existing Node MessageClient HTTPS writer and are not routed through here.
 */
export class RuntimeBindingMessageClient {
  readonly wakes: { watch: (listener: RuntimeBindingWakeListener) => RuntimeBindingWakeWatch };
  readonly recovery: {
    listOpenConversations: (input: RuntimeBindingListOpenConversationsInput) => Promise<RuntimeBindingConversationPage>;
    listUnhandledBy: (input: RuntimeBindingListUnhandledMessagesInput) => Promise<RuntimeBindingMessagePage>;
  };

  private readonly wakeListeners = new Map<number, RuntimeBindingWakeListener>();
  private readonly storeListeners = new Set<StoreListener>();
  private nextListenerId = 1;
  private nextConnectionId = 1;
  private activeConnectionId?: number;
  private session?: RuntimeBindingWakeSession;
  private connectPromise?: Promise<void>;
  private connection: RuntimeBindingConnectionState = "disconnected";
  private transportConnected = false;
  private transportInterrupted = false;
  private recoveryPending = false;
  private recoveryError?: string;
  private snapshot: RuntimeBindingMessageClientSnapshot;
  readonly #options: RuntimeBindingMessageClientOptions;

  constructor(options: RuntimeBindingMessageClientOptions) {
    this.#options = options;
    assertBinding(options.binding);
    this.snapshot = snapshotFor(options.binding, this.connection, this.recoveryPending, 0);
    this.wakes = { watch: (listener) => this.watchWake(listener) };
    this.recovery = {
      listOpenConversations: (input) => this.listOpenConversations(input),
      listUnhandledBy: (input) => this.listUnhandledBy(input),
    };
  }

  /** Stable object identity until externally observable state changes. */
  getSnapshot(): RuntimeBindingMessageClientSnapshot {
    return this.snapshot;
  }

  subscribe(listener: StoreListener): () => void {
    this.storeListeners.add(listener);
    return () => this.storeListeners.delete(listener);
  }

  /**
   * Completes the currently signalled durable recovery generation. The SDK
   * never assumes success from a WSS reconnect alone.
   */
  markDurableRecoveryComplete(): void {
    if (!this.recoveryPending || this.connection === "failed") return;
    this.recoveryPending = false;
    this.recoveryError = undefined;
    this.setConnection(this.session ? "connected" : "disconnected");
  }

  /** Fail closed when the caller cannot complete the required HTTPS recovery. */
  failDurableRecovery(error: unknown): void {
    this.fail(error);
  }

  /** Explicitly retires this WSS. It never retries or selects another route. */
  async disconnect(): Promise<void> {
    await this.closeSession("disconnected");
  }

  private watchWake(listener: RuntimeBindingWakeListener): RuntimeBindingWakeWatch {
    const id = this.nextListenerId++;
    this.wakeListeners.set(id, listener);
    this.publishSnapshot();
    const ready = this.open();
    let released = false;
    return {
      ready,
      release: () => {
        if (released) return;
        released = true;
        this.wakeListeners.delete(id);
        this.publishSnapshot();
        if (this.wakeListeners.size === 0 && this.connection !== "failed") {
          void this.closeSession("disconnected").catch(() => undefined);
        }
      },
    };
  }

  private async open(): Promise<void> {
    if (this.session) return;
    if (this.connectPromise) return this.connectPromise;
    if (this.connection === "failed") {
      throw new Error("message-sdk runtime binding wake transport is failed; create a new binding client after recovery");
    }
    this.setConnection("connecting");
    const connectionId = this.nextConnectionId++;
    this.activeConnectionId = connectionId;
    const run = (async () => {
      try {
        const session = await this.#options.wakeTransport.connect({
          binding: this.#options.binding,
          onWake: (event) => {
            if (this.activeConnectionId === connectionId) this.handleWake(event);
          },
          onState: (state) => {
            if (this.activeConnectionId === connectionId) this.handleTransportState(state);
          },
        });
        if (this.activeConnectionId !== connectionId || this.connection === "failed" || this.wakeListeners.size === 0) {
          await session.close();
          return;
        }
        this.session = session;
        if (!this.transportConnected) this.handleTransportState("connected");
      } catch (error) {
        if (this.activeConnectionId === connectionId) this.fail(error);
        throw error;
      }
    })();
    this.connectPromise = run.finally(() => {
      this.connectPromise = undefined;
    });
    return this.connectPromise;
  }

  private handleWake(event: unknown): void {
    if (!isWakeNotification(event)) {
      this.fail(new Error("message-sdk runtime binding received a malformed wake event"));
      return;
    }
    if (event.runtimeEpoch !== this.#options.binding.runtimeEpoch) {
      this.fail(new Error("message-sdk runtime binding received a stale wake epoch"));
      return;
    }
    this.emit(event);
  }

  private handleTransportState(state: RuntimeBindingConnectionState): void {
    if (this.connection === "failed" || this.connection === "disconnected" && this.wakeListeners.size === 0) return;
    switch (state) {
      case "failed":
        this.fail(new Error("message-sdk runtime binding wake transport failed"));
        return;
      case "disconnected":
      case "reconnecting":
        if (this.transportConnected) {
          this.transportInterrupted = true;
          this.setConnection("reconnecting");
        } else {
          this.setConnection("connecting");
        }
        return;
      case "connecting":
        this.setConnection(this.transportConnected ? "reconnecting" : "connecting");
        return;
      case "recovering":
        // A transport cannot declare HTTPS recovery complete. Preserve the
        // local durable-recovery gate instead of trusting a raw state signal.
        if (this.recoveryPending) this.setConnection("recovering");
        return;
      case "connected":
        if (!this.transportConnected) {
          this.transportConnected = true;
          this.requestDurableRecovery("startup");
          return;
        }
        if (this.transportInterrupted) {
          this.transportInterrupted = false;
          this.requestDurableRecovery("reconnect");
          return;
        }
        if (this.recoveryPending) this.setConnection("recovering");
        else this.setConnection("connected");
        return;
    }
  }

  private requestDurableRecovery(reason: "startup" | "reconnect"): void {
    if (this.recoveryPending) {
      this.setConnection("recovering");
      return;
    }
    this.recoveryPending = true;
    this.recoveryError = undefined;
    this.setConnection("recovering");
    this.emit({ type: "recovery.required", runtimeEpoch: this.#options.binding.runtimeEpoch, reason });
  }

  private async listOpenConversations(input: RuntimeBindingListOpenConversationsInput): Promise<RuntimeBindingConversationPage> {
    assertPageInput(input);
    return this.#options.durableRecovery.listOpenConversations(this.#options.binding, input);
  }

  private async listUnhandledBy(input: RuntimeBindingListUnhandledMessagesInput): Promise<RuntimeBindingMessagePage> {
    assertPageInput(input);
    if (!input.conversationId) throw new Error("conversationId is required");
    return this.#options.durableRecovery.listUnhandledBy(this.#options.binding, input);
  }

  private fail(error: unknown): void {
    if (this.connection === "failed") return;
    this.recoveryError = error instanceof Error ? error.message : "message-sdk runtime binding recovery failed";
    this.recoveryPending = false;
    this.transportInterrupted = false;
    this.setConnection("failed");
    const session = this.session;
    this.activeConnectionId = undefined;
    this.session = undefined;
    if (session) void Promise.resolve(session.close()).catch(() => undefined);
  }

  private async closeSession(state: "disconnected"): Promise<void> {
    const session = this.session;
    this.activeConnectionId = undefined;
    this.session = undefined;
    this.transportConnected = false;
    this.transportInterrupted = false;
    this.recoveryPending = false;
    this.recoveryError = undefined;
    this.setConnection(state);
    if (session) await session.close();
  }

  private setConnection(connection: RuntimeBindingConnectionState): void {
    if (this.connection === connection) return;
    this.connection = connection;
    this.publishSnapshot();
  }

  private emit(event: RuntimeBindingWakeEvent): void {
    for (const listener of this.wakeListeners.values()) {
      try {
        listener(event);
      } catch {
        // Wakes are hints. A consumer exception must not change binding state.
      }
    }
  }

  private publishSnapshot(): void {
    this.snapshot = snapshotFor(
      this.#options.binding,
      this.connection,
      this.recoveryPending,
      this.wakeListeners.size,
      this.recoveryError,
    );
    for (const listener of this.storeListeners) listener();
  }
}

export function createRuntimeBindingMessageClient(
  options: RuntimeBindingMessageClientOptions,
): RuntimeBindingMessageClient {
  return new RuntimeBindingMessageClient(options);
}

function assertBinding(binding: RuntimeBindingIdentity): void {
  if (!binding.runtimeBindingId) throw new Error("runtimeBindingId is required");
  if (!binding.runtimeEpoch) throw new Error("runtimeEpoch is required");
}

function assertPageInput(input: { cursor?: string; limit: number }): void {
  if (!Number.isInteger(input.limit) || input.limit < 1 || input.limit > 500) {
    throw new Error("runtime binding recovery limit must be an integer between 1 and 500");
  }
}

function isWakeNotification(value: unknown): value is RuntimeBindingWakeNotification {
  if (!isRecord(value)) return false;
  return (value.type === "message.available" || value.type === "operation.available")
    && typeof value.runtimeEpoch === "string" && value.runtimeEpoch.length > 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function snapshotFor(
  binding: RuntimeBindingIdentity,
  connection: RuntimeBindingConnectionState,
  recoveryPending: boolean,
  wakeListenerCount: number,
  recoveryError?: string,
): RuntimeBindingMessageClientSnapshot {
  return {
    binding: { runtimeBindingId: binding.runtimeBindingId, runtimeEpoch: binding.runtimeEpoch },
    connection,
    recoveryPending,
    wakeListenerCount,
    ...(recoveryError ? { recoveryError } : {}),
  };
}
