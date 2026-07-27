import type {
  AnyRealtimeEventV1,
  RealtimeCursor,
} from "../protocol/index.js";

/** JSON-safe request body supplied to the HTTPS transport adapter. */
export type MessageJson =
  | string
  | number
  | boolean
  | null
  | MessageJson[]
  | { [key: string]: MessageJson };

/**
 * The only command transport consumed by the platform-neutral facade.
 * Implementations serialize these calls over HTTPS + JSON; this core never
 * knows a URL, fetch implementation, WebSocket implementation, or token.
 */
export interface MessageHttpTransportPort {
  sendMessage(input: SendMessageInput): Promise<SendMessageResult>;
  executeMethod(input: ExecuteMethodInput): Promise<ExecuteMethodResult>;
  hydrateConversation(input: HydrateConversationInput): Promise<ConversationHydration>;
  rebase(input: RebaseInput): Promise<RealtimeRebase>;
}

export interface SendMessageInput {
  conversationId: string;
  /** Caller-owned stable idempotency key. The facade never generates one. */
  clientMessageId: string;
  type: string;
  content: MessageJson;
  replyTo?: string;
}

export interface SendMessageResult {
  messageId: string;
  /** A response may be accepted/created/duplicate, but never synthetic. */
  outcome: "accepted" | "created" | "duplicate";
  correlationId?: string;
}

export interface ExecuteMethodInput {
  instanceId: string;
  method: string;
  params: MessageJson;
  /** Caller-owned stable idempotency key. */
  idempotencyKey: string;
}

export interface ExecuteMethodResult {
  operationId: string;
  outcome: "accepted" | "duplicate";
  correlationId?: string;
}

export interface HydrateConversationInput {
  conversationId: string;
  cursor?: RealtimeCursor;
}

export interface ConversationHydration {
  conversationId: string;
  cursor?: RealtimeCursor;
  events: AnyRealtimeEventV1[];
}

export interface RebaseInput {
  cursor?: RealtimeCursor;
  reason: "sequence_gap" | "history_generation_changed" | "projection_epoch_changed";
}

export interface RealtimeRebase {
  cursor?: RealtimeCursor;
  events: AnyRealtimeEventV1[];
}

export interface RealtimeSession {
  close(): Promise<void> | void;
}

export interface RealtimeConnectInput {
  cursor?: RealtimeCursor;
  onEvent(event: unknown): void;
  onState(state: RealtimeConnectionState): void;
}

export type RealtimeConnectionState = "connected" | "disconnected" | "reconnecting";

/** The realtime adapter owns the physical WSS implementation and token flow. */
export interface RealtimeTransportPort {
  connect(input: RealtimeConnectInput): Promise<RealtimeSession>;
}

/** Optional persisted sync cursor; implementations may target SQLite/IndexedDB. */
export interface RealtimeStoragePort {
  get(key: string): Promise<RealtimeCursor | undefined>;
  set(key: string, cursor: RealtimeCursor): Promise<void>;
  remove?(key: string): Promise<void>;
}

export interface AppStatePort {
  current(): "active" | "background" | "inactive";
  subscribe(listener: (state: "active" | "background" | "inactive") => void): () => void;
}

export interface NetworkPort {
  isOnline(): boolean;
  subscribe(listener: (online: boolean) => void): () => void;
}

export interface MessageClientFacadeOptions {
  transport: MessageHttpTransportPort;
  realtime: RealtimeTransportPort;
  storage?: RealtimeStoragePort;
  appState?: AppStatePort;
  network?: NetworkPort;
  /** Namespace cursor persistence without exposing server channels. */
  cursorStorageKey?: string;
}

export interface RealtimeListenFilter {
  conversationIds?: readonly string[];
  instanceIds?: readonly string[];
  agentIds?: readonly string[];
  eventTypes?: readonly AnyRealtimeEventV1["type"][];
}

export interface ConversationWatch {
  conversationId: string;
  ready: Promise<ConversationHydration>;
  release(): void;
}

export interface MessageClientFacadeSnapshot {
  connection: RealtimeConnectionState;
  cursor?: RealtimeCursor;
  watchedConversationIds: readonly string[];
}
