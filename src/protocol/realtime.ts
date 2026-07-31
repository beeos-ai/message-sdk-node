/**
 * Platform-neutral realtime notification contract.
 *
 * Realtime is deliberately transport-only: the durable HTTP/DB projection is
 * authoritative. Consumers use entity revisions to ignore stale notifications
 * and hydrate over HTTP when a message body delta cannot be applied.
 */
export const REALTIME_EVENT_TYPES = [
  "message.created",
  "message.delta",
  "message.updated",
  "message.terminal",
  "message.deleted",
  "conversation.created",
  "conversation.updated",
  "conversation.deleted",
  "conversation.member.added",
  "conversation.member.removed",
  "conversation.read.updated",
  "conversation.unread.updated",
  "instance.updated",
  "agent.updated",
  "inbox.conversation.available",
  "inbox.conversation.unavailable",
  "operation.available",
  "operation.started",
  "operation.progress",
  "operation.terminal",
  "runtime.dispatch.failed",
  "typing.started",
  "typing.stopped",
] as const;

export type RealtimeEventType = (typeof REALTIME_EVENT_TYPES)[number];
export type RealtimeActorKind = "user" | "agent" | "service" | "system";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export interface RealtimeScope {
  tenantId: string;
  conversationId?: string;
  messageId?: string;
  instanceId?: string;
  agentId?: string;
  operationId?: string;
  runtimeEpoch?: string;
  [key: string]: JsonValue | undefined;
}

export interface RealtimeActor {
  kind: RealtimeActorKind;
  id: string;
  [key: string]: JsonValue;
}

export interface RealtimeCorrelation {
  requestId?: string;
  correlationId?: string;
  causationId?: string;
  traceId?: string;
  idempotencyKeyHash?: string;
  [key: string]: JsonValue | undefined;
}

/** Immutable identity used by delta events, where the full snapshot is not repeated. */
export interface RealtimeMessageIdentity {
  id: string;
  conversationId: string;
  senderId: string;
  /** Durable entity revision, not a realtime transport cursor. */
  revision: string;
  offset?: number;
  [key: string]: JsonValue | undefined;
}

/** Durable message projection. Never contains a realtime transport token. */
export interface RealtimeMessage extends RealtimeMessageIdentity {
  type: string;
  replyTo?: string;
  body: string;
  parts?: JsonValue;
  state: "streaming" | "completed" | "failed" | "refused" | "cancelled";
  stopReason?: string;
  content?: JsonValue;
  createdAt: string;
  updatedAt: string;
  historyGeneration: string;
}

export interface RealtimeConversation {
  id: string;
  title?: string;
  modelOverrideId?: string | null;
  state: "open" | "closed";
  metadataVersion: string;
  historyGeneration: string;
  lastMessageId?: string;
  lastActivityAt?: string;
  updatedAt: string;
  [key: string]: JsonValue | undefined;
}

export interface RealtimeMember {
  identityId: string;
  role: string;
  joinedAt: string;
  [key: string]: JsonValue;
}

export interface RealtimeOperation {
  id: string;
  instanceId: string;
  target:
    | { scope: "instance"; [key: string]: JsonValue }
    | { scope: "agent"; platformAgentId: string; [key: string]: JsonValue }
    | { scope: "conversation"; platformAgentId: string; conversationId: string; [key: string]: JsonValue };
  method: string;
  capability: string;
  contractRevision: "2026-07-14.3";
  transport: "service";
  sequence: string;
  cursor?: string;
  status:
    | "queued" | "running" | "runtime_committed" | "projection_pending"
    | "succeeded" | "failed" | "cancelled" | "expired"
    | "outcome_unknown" | "projection_blocked";
  effectState:
    | "queued" | "running" | "committed" | "failed" | "cancelled"
    | "expired" | "outcome_unknown";
  terminal: boolean;
  progress?: JsonValue;
  result?: JsonValue;
  error?: JsonValue;
  projection?: JsonValue;
  createdAt: string;
  updatedAt: string;
  revision: string;
  [key: string]: JsonValue | undefined;
}

export interface SessionNewResult {
  sessionId: string;
  conversationId: string;
  [key: string]: JsonValue;
}

export interface SessionNewRealtimeOperation extends RealtimeOperation {
  method: "session.new";
  result?: SessionNewResult;
}

export interface RealtimeEventDataMap {
  "message.created": { message: RealtimeMessage; [key: string]: unknown };
  "message.delta": { message: RealtimeMessageIdentity; bodyAppend: string; bodyFrom: number; parts?: JsonValue; [key: string]: unknown };
  "message.updated": { message: RealtimeMessage; [key: string]: unknown };
  "message.terminal": { message: RealtimeMessage; [key: string]: unknown };
  "message.deleted": { messageId: string; [key: string]: unknown };
  "conversation.created": { conversation: RealtimeConversation; [key: string]: unknown };
  "conversation.updated": { conversation: RealtimeConversation; [key: string]: unknown };
  "conversation.deleted": { conversationId: string; [key: string]: unknown };
  "conversation.member.added": { member: RealtimeMember; [key: string]: unknown };
  "conversation.member.removed": { member: RealtimeMember; [key: string]: unknown };
  "conversation.read.updated": { identityId: string; lastReadMessageId?: string; [key: string]: unknown };
  "conversation.unread.updated": { identityId: string; unreadCount: number; [key: string]: unknown };
  "instance.updated": { instanceId: string; status: string; [key: string]: unknown };
  "agent.updated": { agentId: string; status: string; revision: string; [key: string]: unknown };
  "inbox.conversation.available": { conversationId: string; [key: string]: unknown };
  "inbox.conversation.unavailable": { conversationId: string; [key: string]: unknown };
  "operation.available": { operationId: string; [key: string]: unknown };
  "operation.started": { operation: RealtimeOperation; [key: string]: unknown };
  "operation.progress": { operation: RealtimeOperation; [key: string]: unknown };
  "operation.terminal": { operation: RealtimeOperation; [key: string]: unknown };
  "runtime.dispatch.failed": { [key: string]: unknown };
  "typing.started": { identityId: string; [key: string]: unknown };
  "typing.stopped": { identityId: string; [key: string]: unknown };
}

export interface RealtimeEventV1<T extends RealtimeEventType = RealtimeEventType> {
  schemaVersion: 1;
  eventId: string;
  type: T;
  scope: RealtimeScope;
  actor: RealtimeActor;
  correlation: RealtimeCorrelation;
  occurredAt: string;
  data: RealtimeEventDataMap[T];
  [key: string]: unknown;
}

export type AnyRealtimeEventV1 = {
  [T in RealtimeEventType]: RealtimeEventV1<T>;
}[RealtimeEventType];

export class RealtimeEventValidationError extends Error {
  constructor(readonly reason: string) {
    super(`invalid RealtimeEventV1: ${reason}`);
    this.name = "RealtimeEventValidationError";
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasString(record: Record<string, unknown>, key: string): boolean {
  return typeof record[key] === "string" && (record[key] as string).length > 0;
}

/**
 * Performs only the minimum transport-envelope decoding needed to keep the SDK
 * from crashing. Authorization and business validation remain server-owned.
 * Unknown event types and additive fields are intentionally preserved.
 */
export function validateRealtimeEvent(value: unknown): AnyRealtimeEventV1 {
  if (!isRecord(value)) throw new RealtimeEventValidationError("event must be an object");
  if (value.schemaVersion !== 1) throw new RealtimeEventValidationError("schemaVersion must be 1");
  if (!hasString(value, "eventId")) throw new RealtimeEventValidationError("eventId is required");
  if (!hasString(value, "type")) throw new RealtimeEventValidationError("type is required");
  if (!isRecord(value.scope) || !hasString(value.scope, "tenantId")) {
    throw new RealtimeEventValidationError("scope.tenantId is required");
  }
  if (!isRecord(value.actor) || !hasString(value.actor, "id") || !hasString(value.actor, "kind")) {
    throw new RealtimeEventValidationError("actor.kind and actor.id are required");
  }
  if (value.correlation !== undefined && !isRecord(value.correlation)) {
    throw new RealtimeEventValidationError("correlation must be an object");
  }
  if (typeof value.occurredAt !== "string") {
    throw new RealtimeEventValidationError("occurredAt must be a string");
  }
  if (!isRecord(value.data)) {
    throw new RealtimeEventValidationError("data must be an object");
  }
  return value as unknown as AnyRealtimeEventV1;
}

export function encodeRealtimeEvent(event: AnyRealtimeEventV1): string {
  validateRealtimeEvent(event);
  return JSON.stringify(event);
}

export function decodeRealtimeEvent(input: string | unknown): AnyRealtimeEventV1 {
  const value = typeof input === "string" ? JSON.parse(input) : input;
  return validateRealtimeEvent(value);
}
