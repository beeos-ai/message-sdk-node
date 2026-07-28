/**
 * Platform-neutral realtime contract.
 *
 * This module deliberately has no dependency on Node, WebSocket, Centrifuge,
 * React, or the REST client. Platform adapters only transport these events;
 * the protocol layer owns their shape and validation.
 */
import {
  isRuntimeDispatchFailureData,
  type RuntimeDispatchReceipt,
} from "./runtime-dispatch.js";

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
  "operation.started",
  "operation.progress",
  "operation.terminal",
  "runtime.dispatch.failed",
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
  /** Durable conversation-local message offset, when the producer has one. */
  messageOffset?: string;
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
  offset?: number;
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
  modelOverrideId?: string | null;
  state: "open" | "closed";
  metadataVersion: string;
  historyGeneration: string;
  lastMessageId?: string;
  lastActivityAt?: string;
  updatedAt: string;
}

export interface RealtimeMember {
  identityId: string;
  role: string;
  joinedAt: string;
}

export interface RealtimeOperation {
  id: string;
  instanceId: string;
  target:
    | { scope: "instance" }
    | { scope: "agent"; platformAgentId: string }
    | { scope: "conversation"; platformAgentId: string; conversationId: string };
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
  "inbox.conversation.available": { conversationId: string };
  "inbox.conversation.unavailable": { conversationId: string };
  "operation.started": { operation: RealtimeOperation };
  "operation.progress": { operation: RealtimeOperation };
  "operation.terminal": { operation: RealtimeOperation };
  "runtime.dispatch.failed": Exclude<RuntimeDispatchReceipt, { status: "accepted" }>;
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
  if (typeof value !== "string") return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
  if (!match) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = Number(match[10] ?? 0);
  const offsetMinute = Number(match[11] ?? 0);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return month >= 1 && month <= 12
    && day >= 1 && day <= days[month - 1]
    && hour <= 23 && minute <= 59 && second <= 59
    && offsetHour <= 23 && offsetMinute <= 59;
}

function isJsonValue(value: unknown): value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") return true;
  if (Array.isArray(value)) return value.every(isJsonValue);
  return isRecord(value) && Object.values(value).every(isJsonValue);
}

function hasOnlyKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  return Object.keys(value).every((key) => allowed.includes(key));
}

function isMessageIdentity(value: unknown): value is Record<string, unknown> {
  return isRecord(value)
    && hasOnlyKeys(value, ["id", "conversationId", "offset", "senderId"])
    && hasString(value, "id")
    && hasString(value, "conversationId")
    && hasString(value, "senderId")
    && (value.offset === undefined || (Number.isSafeInteger(value.offset) && (value.offset as number) >= 0));
}

function isMessageSnapshot(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value) || !hasOnlyKeys(value, [
    "id", "conversationId", "offset", "senderId", "type", "replyTo", "body", "parts", "state", "stopReason", "content",
    "createdAt", "updatedAt", "historyGeneration",
  ])) return false;
  if (!hasString(value, "id") || !hasString(value, "conversationId") || !hasString(value, "senderId")) return false;
  if (!hasString(value, "type") || typeof value.body !== "string") return false;
  if (!["streaming", "completed", "failed", "refused", "cancelled"].includes(String(value.state))) return false;
  if (!isRFC3339(value.createdAt) || !isRFC3339(value.updatedAt) || !isDecimalString(value.historyGeneration)) return false;
  return (value.replyTo === undefined || typeof value.replyTo === "string")
    && (value.stopReason === undefined || typeof value.stopReason === "string")
    && (value.content === undefined || isJsonValue(value.content))
    && (value.parts === undefined || isJsonValue(value.parts));
}

function isConversationSnapshot(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value) || !hasOnlyKeys(value, [
    "id", "title", "modelOverrideId", "state", "metadataVersion", "historyGeneration", "lastMessageId", "lastActivityAt", "updatedAt",
  ]) || !hasString(value, "id")) return false;
  if (!["open", "closed"].includes(String(value.state))) return false;
  if (!isDecimalString(value.metadataVersion) || !isDecimalString(value.historyGeneration)) return false;
  if (!isRFC3339(value.updatedAt)) return false;
  return (value.title === undefined || typeof value.title === "string")
    && (value.modelOverrideId === undefined || value.modelOverrideId === null
      || (typeof value.modelOverrideId === "string" && value.modelOverrideId.length > 0))
    && (value.lastMessageId === undefined || typeof value.lastMessageId === "string")
    && (value.lastActivityAt === undefined || isRFC3339(value.lastActivityAt));
}

function isMemberSnapshot(value: unknown): value is Record<string, unknown> {
  return isRecord(value)
    && hasOnlyKeys(value, ["identityId", "role", "joinedAt"])
    && hasString(value, "identityId")
    && hasString(value, "role")
    && isRFC3339(value.joinedAt);
}

function isOperationSnapshot(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value) || !hasOnlyKeys(value, [
    "id", "instanceId", "target", "method", "capability", "contractRevision",
    "transport", "sequence", "cursor", "status", "effectState", "terminal",
    "progress", "result", "error", "projection", "createdAt", "updatedAt", "revision",
  ])) return false;
  if (!hasString(value, "id") || !hasString(value, "instanceId")
    || !hasString(value, "method") || !hasString(value, "capability")) return false;
  if (value.contractRevision !== "2026-07-14.3" || value.transport !== "service") return false;
  if (!isDecimalString(value.sequence) || !isDecimalString(value.revision)) return false;
  if (value.cursor !== undefined && !hasString(value, "cursor")) return false;
  if (!isRuntimeTarget(value.target) || !isRFC3339(value.createdAt) || !isRFC3339(value.updatedAt)) return false;
  if (typeof value.terminal !== "boolean" || !isOperationStateCompatible(
    String(value.status),
    String(value.effectState),
    value.terminal,
  )) return false;
  return (value.progress === undefined || isJsonValue(value.progress))
    && (value.result === undefined || isJsonValue(value.result))
    && (value.error === undefined || isJsonValue(value.error))
    && (value.projection === undefined || isJsonValue(value.projection));
}

function isRuntimeTarget(value: unknown): boolean {
  if (!isRecord(value) || !hasString(value, "scope")) return false;
  if (value.scope === "instance") return hasOnlyKeys(value, ["scope"]);
  if (value.scope === "agent") {
    return hasOnlyKeys(value, ["scope", "platformAgentId"])
      && hasString(value, "platformAgentId");
  }
  return value.scope === "conversation"
    && hasOnlyKeys(value, ["scope", "platformAgentId", "conversationId"])
    && hasString(value, "platformAgentId")
    && hasString(value, "conversationId");
}

function isOperationStateCompatible(status: string, effectState: string, terminal: boolean): boolean {
  switch (status) {
    case "queued": return effectState === "queued" && !terminal;
    case "running": return effectState === "running" && !terminal;
    case "runtime_committed":
    case "projection_pending":
      return effectState === "committed" && !terminal;
    case "succeeded": return effectState === "committed" && terminal;
    case "failed": return effectState === "failed" && terminal;
    case "cancelled": return effectState === "cancelled" && terminal;
    case "expired": return effectState === "expired" && terminal;
    case "outcome_unknown": return effectState === "outcome_unknown" && terminal;
    case "projection_blocked": return effectState === "committed";
    default: return false;
  }
}

function hasDataForType(type: RealtimeEventType, scope: Record<string, unknown>, data: Record<string, unknown>): boolean {
  switch (type) {
    case "message.created":
    case "message.updated":
    case "message.terminal":
      return hasOnlyKeys(data, ["message"]) && hasString(scope, "conversationId") && hasString(scope, "messageId") && isMessageSnapshot(data.message)
        && data.message.id === scope.messageId && data.message.conversationId === scope.conversationId;
    case "message.delta":
      return hasOnlyKeys(data, ["message", "bodyAppend", "bodyFrom", "parts"])
        && hasString(scope, "conversationId") && hasString(scope, "messageId") && isMessageIdentity(data.message)
        && data.message.id === scope.messageId && data.message.conversationId === scope.conversationId
        && typeof data.bodyAppend === "string" && Number.isSafeInteger(data.bodyFrom) && (data.bodyFrom as number) >= 0
        && (data.parts === undefined || isJsonValue(data.parts));
    case "message.deleted":
      return hasOnlyKeys(data, ["messageId"]) && hasString(scope, "conversationId") && hasString(scope, "messageId") && data.messageId === scope.messageId;
    case "conversation.created":
    case "conversation.updated":
      return hasOnlyKeys(data, ["conversation"]) && hasString(scope, "conversationId") && isConversationSnapshot(data.conversation) && data.conversation.id === scope.conversationId;
    case "conversation.deleted":
      return hasOnlyKeys(data, ["conversationId"]) && hasString(scope, "conversationId") && data.conversationId === scope.conversationId;
    case "conversation.member.added":
    case "conversation.member.removed":
      return hasOnlyKeys(data, ["member"]) && hasString(scope, "conversationId") && isMemberSnapshot(data.member);
    case "conversation.read.updated":
      return hasOnlyKeys(data, ["identityId", "lastReadMessageId"])
        && hasString(scope, "conversationId") && hasString(data, "identityId")
        && (data.lastReadMessageId === undefined || typeof data.lastReadMessageId === "string");
    case "conversation.unread.updated":
      return hasOnlyKeys(data, ["identityId", "unreadCount"])
        && hasString(scope, "conversationId") && hasString(data, "identityId") && Number.isSafeInteger(data.unreadCount) && (data.unreadCount as number) >= 0;
    case "instance.updated":
      return hasOnlyKeys(data, ["instanceId", "status"])
        && hasString(scope, "instanceId") && hasString(data, "instanceId") && hasString(data, "status");
    case "agent.updated":
      return hasOnlyKeys(data, ["agentId", "status", "revision"])
        && hasString(scope, "agentId") && data.agentId === scope.agentId && hasString(data, "status") && isDecimalString(data.revision);
    case "inbox.conversation.available":
    case "inbox.conversation.unavailable":
      return hasOnlyKeys(data, ["conversationId"]) && hasString(data, "conversationId")
        && scope.conversationId === undefined;
    case "operation.started":
    case "operation.progress":
    case "operation.terminal":
      return hasOnlyKeys(data, ["operation"])
        && hasString(scope, "operationId") && hasString(scope, "instanceId")
        && isOperationSnapshot(data.operation)
        && data.operation.id === scope.operationId
        && data.operation.instanceId === scope.instanceId;
    case "runtime.dispatch.failed":
      return hasOnlyKeys(scope, ["tenantId", "conversationId", "messageId"])
        && hasString(scope, "conversationId")
        && hasString(scope, "messageId")
        && isRuntimeDispatchFailureData(data);
    case "typing.started":
    case "typing.stopped":
      return hasOnlyKeys(data, ["identityId"]) && hasString(scope, "conversationId") && hasString(data, "identityId");
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
  if (value.type === "runtime.dispatch.failed" && (
    !hasOnlyKeys(value, [
      "schemaVersion", "eventId", "type", "scope", "actor",
      "ordering", "correlation", "occurredAt", "data",
    ])
    || !isRecord(value.actor)
    || !hasOnlyKeys(value.actor, ["kind", "id"])
    || value.actor.kind !== "service"
    || value.actor.id !== "message-service"
  )) {
    throw new RealtimeEventValidationError("runtime dispatch actor/envelope is invalid");
  }
  if (!isRecord(value.actor) || !hasString(value.actor, "id") || !ACTOR_KIND_SET.has(String(value.actor.kind))) {
    throw new RealtimeEventValidationError("actor.kind and actor.id are required");
  }
  if (!isRecord(value.ordering) || !/^\d+$/.test(String(value.ordering.streamSequence ?? ""))) {
    throw new RealtimeEventValidationError("ordering.streamSequence must be a decimal integer");
  }
  if (value.ordering.messageOffset !== undefined && !isDecimalString(value.ordering.messageOffset)) throw new RealtimeEventValidationError("ordering.messageOffset must be a decimal integer");
  if (["message.created", "message.delta", "message.updated", "message.terminal"].includes(String(value.type))
    && !isDecimalString(value.ordering.messageOffset)) {
    throw new RealtimeEventValidationError("ordering.messageOffset is required for message events");
  }
  if (value.ordering.completeness !== "full" && value.ordering.completeness !== "delta") {
    throw new RealtimeEventValidationError("ordering.completeness must be full or delta");
  }
  if (value.type === "runtime.dispatch.failed" && (
    !hasOnlyKeys(value.ordering, ["streamSequence", "completeness"])
    || value.ordering.streamSequence !== "0"
    || value.ordering.completeness !== "delta"
  )) {
    throw new RealtimeEventValidationError("runtime dispatch ordering must be ephemeral");
  }
  if (!isRecord(value.correlation) || !hasOnlyKeys(value.correlation, [
    "requestId", "correlationId", "causationId", "traceId", "idempotencyKeyHash",
  ]) || !Object.values(value.correlation).every(
    (item) => typeof item === "string" && item.length > 0,
  )) {
    throw new RealtimeEventValidationError("correlation values must be exact non-empty strings");
  }
  if (value.type === "runtime.dispatch.failed" && !hasOnlyKeys(value.correlation, [
    "requestId", "correlationId", "causationId", "traceId", "idempotencyKeyHash",
  ])) {
    throw new RealtimeEventValidationError("runtime dispatch correlation is invalid");
  }
  if (!isRFC3339(value.occurredAt)) {
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
