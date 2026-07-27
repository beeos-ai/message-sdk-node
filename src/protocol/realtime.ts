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

export interface RealtimeMessage {
  id: string;
  conversationId: string;
  senderId: string;
  body?: string;
  state?: "streaming" | "completed" | "failed" | "refused" | "cancelled";
  content?: JsonValue;
}

export interface RealtimeConversation {
  id: string;
  title?: string;
  state?: "open" | "closed";
}

export interface RealtimeMember {
  identityId: string;
  role?: string;
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
  "message.delta": { bodyAppend?: string; bodyFrom?: number; message: RealtimeMessage };
  "message.updated": { message: RealtimeMessage };
  "message.terminal": { message: RealtimeMessage; stopReason?: string };
  "message.deleted": { messageId: string };
  "conversation.created": { conversation: RealtimeConversation };
  "conversation.updated": { conversation: RealtimeConversation };
  "conversation.deleted": { conversationId: string };
  "conversation.member.added": { member: RealtimeMember };
  "conversation.member.removed": { member: RealtimeMember };
  "conversation.read.updated": { identityId: string; lastReadMessageId?: string };
  "conversation.unread.updated": { identityId: string; unreadCount: number };
  "instance.updated": { instanceId: string; status: string };
  "agent.updated": { agentId: string; status: string };
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

function hasDataForType(type: RealtimeEventType, scope: Record<string, unknown>, data: Record<string, unknown>): boolean {
  switch (type) {
    case "message.created":
    case "message.delta":
    case "message.updated":
    case "message.terminal":
      return hasString(scope, "conversationId") && hasString(scope, "messageId") && isRecord(data.message);
    case "message.deleted":
      return hasString(scope, "conversationId") && hasString(scope, "messageId") && hasString(data, "messageId");
    case "conversation.created":
    case "conversation.updated":
      return hasString(scope, "conversationId") && isRecord(data.conversation);
    case "conversation.deleted":
      return hasString(scope, "conversationId") && hasString(data, "conversationId");
    case "conversation.member.added":
    case "conversation.member.removed":
      return hasString(scope, "conversationId") && isRecord(data.member) && hasString(data.member, "identityId");
    case "conversation.read.updated":
      return hasString(scope, "conversationId") && hasString(data, "identityId");
    case "conversation.unread.updated":
      return hasString(scope, "conversationId") && hasString(data, "identityId") && typeof data.unreadCount === "number";
    case "instance.updated":
      return hasString(scope, "instanceId") && hasString(data, "instanceId") && hasString(data, "status");
    case "agent.updated":
      return hasString(scope, "agentId") && hasString(data, "agentId") && hasString(data, "status");
    case "operation.started":
    case "operation.progress":
    case "operation.terminal":
      return hasString(scope, "operationId") && isRecord(data.operation) && hasString(data.operation, "id") && hasString(data.operation, "method") && hasString(data.operation, "state");
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
