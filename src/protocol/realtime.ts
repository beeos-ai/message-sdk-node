/**
 * Platform-neutral realtime contract.
 *
 * This module deliberately has no dependency on Node, WebSocket, Centrifuge,
 * React, or the REST client. Platform adapters only transport these events;
 * the protocol layer owns their shape and validation.
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
  "operation.started",
  "operation.progress",
  "operation.terminal",
  "typing.started",
  "typing.stopped",
] as const;

export type RealtimeEventType = (typeof REALTIME_EVENT_TYPES)[number];
export type RealtimeCompleteness = "full" | "delta";
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
}

export interface RealtimeActor {
  kind: RealtimeActorKind;
  id: string;
}

export interface RealtimeOrdering {
  /** Decimal integer, serialized as a string so JSON cannot lose precision. */
  streamSequence: string;
  entityRevision?: string;
  projectionUid?: string;
  projectionEpoch?: string;
  historyGeneration?: string;
  completeness: RealtimeCompleteness;
  runtimeEpoch?: string;
}

export interface RealtimeCorrelation {
  requestId?: string;
  correlationId?: string;
  causationId?: string;
  traceId?: string;
  idempotencyKeyHash?: string;
}

/** Immutable identity used by delta events, where the full snapshot is not repeated. */
export interface RealtimeMessageIdentity {
  id: string;
  conversationId: string;
  senderId: string;
}

/** Durable v3 message projection. Never contains a realtime transport token. */
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
  /** Decimal string; a generation change requires projection rebase. */
  historyGeneration: string;
}

export interface RealtimeConversation {
  id: string;
  title?: string;
  state: "open" | "closed";
  metadataVersion: string;
  historyGeneration: string;
  lastMessageId?: string;
  lastActivityAt: string;
  updatedAt: string;
}

export interface RealtimeMember {
  identityId: string;
  role: string;
  joinedAt: string;
}

export interface RealtimeOperation {
  id: string;
  method: string;
  state: "started" | "running" | "completed" | "failed" | "cancelled" | "outcome_unknown";
  result?: JsonValue;
  errorCode?: string;
}

export interface RealtimeEventDataMap {
  "message.created": { message: RealtimeMessage };
  "message.delta": { message: RealtimeMessageIdentity; bodyAppend: string; bodyFrom: number; parts?: JsonValue };
  "message.updated": { message: RealtimeMessage };
  "message.terminal": { message: RealtimeMessage };
  "message.deleted": { messageId: string };
  "conversation.created": { conversation: RealtimeConversation };
  "conversation.updated": { conversation: RealtimeConversation };
  "conversation.deleted": { conversationId: string };
  "conversation.member.added": { member: RealtimeMember };
  "conversation.member.removed": { member: RealtimeMember };
  "conversation.read.updated": { identityId: string; lastReadMessageId?: string };
  "conversation.unread.updated": { identityId: string; unreadCount: number };
  "instance.updated": { instanceId: string; status: string };
  "agent.updated": { agentId: string; status: string; revision: string };
  "operation.started": { operation: RealtimeOperation };
  "operation.progress": { operation: RealtimeOperation; progress?: number };
  "operation.terminal": { operation: RealtimeOperation };
  "typing.started": { identityId: string };
  "typing.stopped": { identityId: string };
}

export interface RealtimeEventV1<T extends RealtimeEventType = RealtimeEventType> {
  schemaVersion: 1;
  eventId: string;
  type: T;
  scope: RealtimeScope;
  actor: RealtimeActor;
  ordering: RealtimeOrdering;
  correlation: RealtimeCorrelation;
  occurredAt: string;
  data: RealtimeEventDataMap[T];
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

const EVENT_TYPE_SET = new Set<string>(REALTIME_EVENT_TYPES);
const ACTOR_KIND_SET = new Set<string>(["user", "agent", "service", "system"]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasString(record: Record<string, unknown>, key: string): boolean {
  return typeof record[key] === "string" && (record[key] as string).length > 0;
}

function isDecimalString(value: unknown): value is string {
  return typeof value === "string" && /^\d+$/.test(value);
}

function isRFC3339(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && !Number.isNaN(Date.parse(value));
}

function isJsonValue(value: unknown): value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") return true;
  if (Array.isArray(value)) return value.every(isJsonValue);
  return isRecord(value) && Object.values(value).every(isJsonValue);
}

function isMessageIdentity(value: unknown): value is Record<string, unknown> {
  return isRecord(value) && hasString(value, "id") && hasString(value, "conversationId") && hasString(value, "senderId");
}

function isMessageSnapshot(value: unknown): value is Record<string, unknown> {
  if (!isMessageIdentity(value)) return false;
  if (!hasString(value, "type") || typeof value.body !== "string") return false;
  if (!["streaming", "completed", "failed", "refused", "cancelled"].includes(String(value.state))) return false;
  if (!isRFC3339(value.createdAt) || !isRFC3339(value.updatedAt) || !isDecimalString(value.historyGeneration)) return false;
  return (value.replyTo === undefined || typeof value.replyTo === "string")
    && (value.stopReason === undefined || typeof value.stopReason === "string")
    && (value.content === undefined || isJsonValue(value.content))
    && (value.parts === undefined || isJsonValue(value.parts));
}

function isConversationSnapshot(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value) || !hasString(value, "id")) return false;
  if (!["open", "closed"].includes(String(value.state))) return false;
  if (!isDecimalString(value.metadataVersion) || !isDecimalString(value.historyGeneration)) return false;
  if (!isRFC3339(value.lastActivityAt) || !isRFC3339(value.updatedAt)) return false;
  return (value.title === undefined || typeof value.title === "string")
    && (value.lastMessageId === undefined || typeof value.lastMessageId === "string");
}

function isMemberSnapshot(value: unknown): value is Record<string, unknown> {
  return isRecord(value) && hasString(value, "identityId") && hasString(value, "role") && isRFC3339(value.joinedAt);
}

function isOperationSnapshot(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value) || !hasString(value, "id") || !hasString(value, "method")) return false;
  if (!["started", "running", "completed", "failed", "cancelled", "outcome_unknown"].includes(String(value.state))) return false;
  return (value.result === undefined || isJsonValue(value.result))
    && (value.errorCode === undefined || typeof value.errorCode === "string");
}

function hasDataForType(type: RealtimeEventType, scope: Record<string, unknown>, data: Record<string, unknown>): boolean {
  switch (type) {
    case "message.created":
    case "message.updated":
    case "message.terminal":
      return hasString(scope, "conversationId") && hasString(scope, "messageId") && isMessageSnapshot(data.message)
        && data.message.id === scope.messageId && data.message.conversationId === scope.conversationId;
    case "message.delta":
      return hasString(scope, "conversationId") && hasString(scope, "messageId") && isMessageIdentity(data.message)
        && data.message.id === scope.messageId && data.message.conversationId === scope.conversationId
        && typeof data.bodyAppend === "string" && Number.isSafeInteger(data.bodyFrom) && (data.bodyFrom as number) >= 0
        && (data.parts === undefined || isJsonValue(data.parts));
    case "message.deleted":
      return hasString(scope, "conversationId") && hasString(scope, "messageId") && data.messageId === scope.messageId;
    case "conversation.created":
    case "conversation.updated":
      return hasString(scope, "conversationId") && isConversationSnapshot(data.conversation) && data.conversation.id === scope.conversationId;
    case "conversation.deleted":
      return hasString(scope, "conversationId") && data.conversationId === scope.conversationId;
    case "conversation.member.added":
    case "conversation.member.removed":
      return hasString(scope, "conversationId") && isMemberSnapshot(data.member);
    case "conversation.read.updated":
      return hasString(scope, "conversationId") && hasString(data, "identityId");
    case "conversation.unread.updated":
      return hasString(scope, "conversationId") && hasString(data, "identityId") && Number.isSafeInteger(data.unreadCount) && (data.unreadCount as number) >= 0;
    case "instance.updated":
      return hasString(scope, "instanceId") && hasString(data, "instanceId") && hasString(data, "status");
    case "agent.updated":
      return hasString(scope, "agentId") && data.agentId === scope.agentId && hasString(data, "status") && isDecimalString(data.revision);
    case "operation.started":
    case "operation.progress":
    case "operation.terminal":
      return hasString(scope, "operationId") && isOperationSnapshot(data.operation) && data.operation.id === scope.operationId
        && (type !== "operation.progress" || data.progress === undefined || (Number.isSafeInteger(data.progress) && (data.progress as number) >= 0 && (data.progress as number) <= 100));
    case "typing.started":
    case "typing.stopped":
      return hasString(scope, "conversationId") && hasString(data, "identityId");
  }
}

/** Validates the transport envelope before it reaches platform state. */
export function validateRealtimeEvent(value: unknown): AnyRealtimeEventV1 {
  if (!isRecord(value)) throw new RealtimeEventValidationError("event must be an object");
  if (value.schemaVersion !== 1) throw new RealtimeEventValidationError("schemaVersion must be 1");
  if (!hasString(value, "eventId")) throw new RealtimeEventValidationError("eventId is required");
  if (!hasString(value, "type") || !EVENT_TYPE_SET.has(value.type as string)) {
    throw new RealtimeEventValidationError("type is unsupported");
  }
  if (!isRecord(value.scope) || !hasString(value.scope, "tenantId")) {
    throw new RealtimeEventValidationError("scope.tenantId is required");
  }
  if (!isRecord(value.actor) || !hasString(value.actor, "id") || !ACTOR_KIND_SET.has(String(value.actor.kind))) {
    throw new RealtimeEventValidationError("actor.kind and actor.id are required");
  }
  if (!isRecord(value.ordering) || !/^\d+$/.test(String(value.ordering.streamSequence ?? ""))) {
    throw new RealtimeEventValidationError("ordering.streamSequence must be a decimal integer");
  }
  if (value.ordering.completeness !== "full" && value.ordering.completeness !== "delta") {
    throw new RealtimeEventValidationError("ordering.completeness must be full or delta");
  }
  if (!isRecord(value.correlation)) throw new RealtimeEventValidationError("correlation is required");
  if (!hasString(value, "occurredAt") || Number.isNaN(Date.parse(value.occurredAt as string))) {
    throw new RealtimeEventValidationError("occurredAt must be an RFC3339 timestamp");
  }
  if (!isRecord(value.data) || !hasDataForType(value.type as RealtimeEventType, value.scope, value.data)) {
    throw new RealtimeEventValidationError("data does not satisfy event type contract");
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
