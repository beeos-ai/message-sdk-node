import { applyWireFrame, snapshotBody, snapshotFromBody } from "../reducer.js";
import type {
  AnyRealtimeEventV1,
  RealtimeConversation,
  RealtimeMessage,
  RealtimeOperation,
} from "../protocol/index.js";
import type {
  ConversationHydrationProof,
  ConversationProjection,
  DomainProjectionSnapshot,
  MessageProjection,
  OperationProjection,
} from "./contracts.js";

const EMPTY_SNAPSHOT: DomainProjectionSnapshot = Object.freeze({
  conversations: Object.freeze({}),
  messages: Object.freeze({}),
  operations: Object.freeze({}),
  latestOffsetByConversation: Object.freeze({}),
  hydrationByConversation: Object.freeze({}),
});

export interface ConversationHydrationCommit {
  readonly conversation: ConversationProjection;
  readonly messages: readonly MessageProjection[];
  readonly historyBoundaryOffset: string;
  readonly latestOffset: string;
}

export type ProjectionApplyResult =
  | "changed"
  | "stale"
  | "message_delta_gap"
  | "ignored";

export class ProjectionEngine {
  private snapshot: DomainProjectionSnapshot;

  constructor(initial: DomainProjectionSnapshot = EMPTY_SNAPSHOT) {
    this.snapshot = freezeSnapshot(initial);
  }

  getSnapshot(): DomainProjectionSnapshot {
    return this.snapshot;
  }

  replace(snapshot: DomainProjectionSnapshot): void {
    this.snapshot = freezeSnapshot(snapshot);
  }

  commitHydration(commit: ConversationHydrationCommit): boolean {
    const current = this.snapshot.conversations[commit.conversation.id];
    if (current && compareDecimal(current.revision, commit.conversation.revision) > 0) return false;
    if (isIdenticalHydration(this.snapshot, commit)) return false;

    const conversations = { ...this.snapshot.conversations, [commit.conversation.id]: freeze(commit.conversation) };
    const messages: Record<string, MessageProjection> = {};
    for (const [id, message] of Object.entries(this.snapshot.messages)) {
      if (message.conversationId !== commit.conversation.id) messages[id] = message;
    }
    for (const message of commit.messages) {
      if (message.conversationId !== commit.conversation.id) {
        throw new Error("hydration message belongs to another conversation");
      }
      if (message.historyGeneration !== commit.conversation.historyGeneration) continue;
      messages[message.id] = freeze(message);
    }
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      conversations,
      messages,
      latestOffsetByConversation: {
        ...this.snapshot.latestOffsetByConversation,
        [commit.conversation.id]: commit.latestOffset,
      },
      hydrationByConversation: {
        ...this.snapshot.hydrationByConversation,
        [commit.conversation.id]: Object.freeze({
          conversationId: commit.conversation.id,
          historyGeneration: commit.conversation.historyGeneration,
          historyBoundaryOffset: commit.historyBoundaryOffset,
          conversationRevision: commit.conversation.revision,
          latestOffset: commit.latestOffset,
          projectionRevision: nextProjectionRevision(
            this.snapshot.hydrationByConversation[commit.conversation.id]?.projectionRevision,
          ),
          complete: true as const,
        }),
      },
    });
    return true;
  }

  /** Atomically replaces the authoritative private inbox conversation directory. */
  replaceDirectoryHydrations(commits: readonly ConversationHydrationCommit[]): boolean {
    const authoritativeIds = new Set(commits.map((commit) => commit.conversation.id));
    const staleIds = Object.keys(this.snapshot.conversations)
      .filter((id) => !authoritativeIds.has(id));
    const base = staleIds.length === 0
      ? this.snapshot
      : freezeSnapshot({
          ...this.snapshot,
          conversations: Object.fromEntries(
            Object.entries(this.snapshot.conversations)
              .filter(([id]) => authoritativeIds.has(id)),
          ),
          messages: Object.fromEntries(
            Object.entries(this.snapshot.messages)
              .filter(([, message]) => authoritativeIds.has(message.conversationId)),
          ),
          latestOffsetByConversation: Object.fromEntries(
            Object.entries(this.snapshot.latestOffsetByConversation)
              .filter(([id]) => authoritativeIds.has(id)),
          ),
          hydrationByConversation: Object.fromEntries(
            Object.entries(this.snapshot.hydrationByConversation)
              .filter(([id]) => authoritativeIds.has(id)),
          ),
        });
    const staged = new ProjectionEngine(base);
    let changed = false;
    for (const commit of commits) {
      changed = staged.commitHydration(commit) || changed;
    }
    if (changed || staleIds.length > 0) this.snapshot = staged.getSnapshot();
    return changed || staleIds.length > 0;
  }

  apply(event: AnyRealtimeEventV1): ProjectionApplyResult {
    try {
      return this.applyKnown(event);
    } catch {
      // The transport decoder deliberately accepts additive and unknown
      // fields. A malformed known payload is ignored by the reducer instead
      // of crashing the application process.
      return "ignored";
    }
  }

  private applyKnown(event: AnyRealtimeEventV1): ProjectionApplyResult {
    let changed = false;
    switch (event.type) {
      case "conversation.created":
      case "conversation.updated":
        changed = this.applyConversation(event.data.conversation);
        break;
      case "conversation.deleted":
        changed = this.deleteConversation(event.data.conversationId);
        break;
      case "message.created":
      case "message.updated":
      case "message.terminal":
        changed = this.applyMessage(event.data.message);
        break;
      case "message.delta":
        return this.applyDelta(event);
      case "message.deleted":
        changed = this.deleteMessage(event.data.messageId, event.scope.conversationId);
        break;
      case "operation.started":
      case "operation.progress":
      case "operation.terminal":
        changed = this.applyOperation(event.data.operation, event.scope.instanceId);
        break;
      default:
        return "ignored";
    }

    return changed ? "changed" : "stale";
  }

  putOptimisticMessage(message: MessageProjection): boolean {
    if (message.state !== "optimistic" && message.state !== "outcome_unknown") {
      throw new Error("optimistic message must be optimistic or outcome_unknown");
    }
    const current = this.snapshot.messages[message.id];
    if (current && compareDecimal(current.revision, message.revision) > 0) return false;
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      messages: { ...this.snapshot.messages, [message.id]: freeze(message) },
    });
    return true;
  }

  putOperation(operation: OperationProjection): boolean {
    const current = this.snapshot.operations[operation.id];
    if (current && compareDecimal(current.revision, operation.revision) > 0) return false;
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      operations: { ...this.snapshot.operations, [operation.id]: freeze(operation) },
    });
    return true;
  }

  reconcileOptimistic(
    localId: string,
    authoritative: MessageProjection | { id: string },
  ): boolean {
    const local = this.snapshot.messages[localId];
    if (!local) return false;
    if (!("conversationId" in authoritative) && authoritative.id === localId) {
      // The POST receipt confirms identity but carries no authoritative
      // projection. Keep the optimistic row until the HTTP/WSS full echo.
      return false;
    }
    const current = this.snapshot.messages[authoritative.id];
    const messages = { ...this.snapshot.messages };
    delete messages[localId];
    if ("conversationId" in authoritative) {
      if (!current || compareDecimal(current.revision, authoritative.revision) < 0) {
        messages[authoritative.id] = freeze(authoritative);
      }
    } else if (!current) {
      messages[authoritative.id] = freeze({ ...local, id: authoritative.id });
    }
    this.snapshot = freezeSnapshot({ ...this.snapshot, messages });
    return true;
  }

  removeMessage(messageId: string): boolean {
    if (!this.snapshot.messages[messageId]) return false;
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      messages: withoutKey(this.snapshot.messages, messageId),
    });
    return true;
  }

  private applyConversation(value: RealtimeConversation, revision?: string): boolean {
    const incoming = conversationFromRealtime(value, revision);
    const current = this.snapshot.conversations[value.id];
    if (
      current
      && compareDecimal(incoming.historyGeneration, current.historyGeneration) < 0
    ) return false;
    if (current && compareDecimal(current.revision, incoming.revision) >= 0) return false;
    const generationChanged = current && current.historyGeneration !== incoming.historyGeneration;
    const messages = generationChanged
      ? filterMessages(this.snapshot.messages, (message) => message.conversationId !== value.id)
      : this.snapshot.messages;
    const latestOffsetByConversation = generationChanged
      ? withoutKey(this.snapshot.latestOffsetByConversation, value.id)
      : this.snapshot.latestOffsetByConversation;
    const hydrationByConversation = generationChanged
      ? withoutKey(this.snapshot.hydrationByConversation, value.id)
      : this.snapshot.hydrationByConversation;
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      conversations: { ...this.snapshot.conversations, [value.id]: freeze(incoming) },
      messages,
      latestOffsetByConversation,
      hydrationByConversation,
    });
    return true;
  }

  private deleteConversation(conversationId: string): boolean {
    if (!this.snapshot.conversations[conversationId]) return false;
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      conversations: withoutKey(this.snapshot.conversations, conversationId),
      messages: filterMessages(this.snapshot.messages, (message) => message.conversationId !== conversationId),
      latestOffsetByConversation: withoutKey(this.snapshot.latestOffsetByConversation, conversationId),
      hydrationByConversation: withoutKey(this.snapshot.hydrationByConversation, conversationId),
    });
    return true;
  }

  private applyMessage(value: RealtimeMessage): boolean {
    const conversation = this.snapshot.conversations[value.conversationId];
    if (conversation && conversation.historyGeneration !== value.historyGeneration) return false;
    const current = this.snapshot.messages[value.id];
    const offset = value.offset === undefined
      ? current?.offset ?? "0"
      : String(value.offset);
    const incoming = messageFromRealtime(value, offset);
    if (current && compareDecimal(current.revision, incoming.revision) >= 0) return false;
    const messages = { ...this.snapshot.messages };
    for (const [localId, message] of Object.entries(messages)) {
      if (localId !== value.id && message.clientMessageId === value.id) delete messages[localId];
    }
    messages[value.id] = freeze(incoming);
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      messages,
    });
    return true;
  }

  private applyDelta(
    event: Extract<AnyRealtimeEventV1, { type: "message.delta" }>,
  ): ProjectionApplyResult {
    const id = event.data.message.id;
    const current = this.snapshot.messages[id];
    if (!current) return "message_delta_gap";
    const offset = event.data.message.offset === undefined
      ? current.offset
      : String(event.data.message.offset);
    const revision = event.data.message.revision;
    if (compareDecimal(current.revision, revision) >= 0) return "stale";
    const reduced = applyWireFrame(snapshotFromBody(current.body), {
      event: "message.delta",
      body_from: event.data.bodyFrom,
      body_chunk: event.data.bodyAppend,
    });
    if (reduced.result === "rebase") return "message_delta_gap";
    const nextBody = snapshotBody(reduced.snapshot);
    const unchanged = nextBody === current.body
      && event.data.parts === undefined
      && current.offset === offset;
    if (unchanged) return "stale";
    const next: MessageProjection = {
      ...current,
      body: nextBody,
      ...(event.data.parts === undefined ? {} : { parts: event.data.parts }),
      revision,
      offset,
      updatedAt: event.occurredAt,
    };
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      messages: { ...this.snapshot.messages, [id]: freeze(next) },
    });
    return "changed";
  }

  private deleteMessage(messageId: string, conversationId?: string): boolean {
    if (!this.snapshot.messages[messageId]) return false;
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      messages: withoutKey(this.snapshot.messages, messageId),
      hydrationByConversation: this.snapshot.hydrationByConversation,
    });
    return true;
  }

  private applyOperation(value: RealtimeOperation, instanceId?: string): boolean {
    const incoming: OperationProjection = {
      ...value,
      instanceId: instanceId ?? value.instanceId,
      revision: value.revision,
    };
    const current = this.snapshot.operations[value.id];
    if (current && compareDecimal(current.revision, incoming.revision) >= 0) return false;
    this.snapshot = freezeSnapshot({
      ...this.snapshot,
      operations: { ...this.snapshot.operations, [value.id]: freeze(incoming) },
    });
    return true;
  }
}

export function conversationFromRealtime(value: RealtimeConversation, revision?: string): ConversationProjection {
  return {
    id: value.id,
    ...(value.title === undefined ? {} : { title: value.title }),
    ...(value.modelOverrideId === undefined ? {} : { modelOverrideId: value.modelOverrideId }),
    state: value.state,
    historyGeneration: value.historyGeneration,
    revision: revision ?? value.metadataVersion,
    ...(value.lastMessageId === undefined ? {} : { lastMessageId: value.lastMessageId }),
    ...(value.lastActivityAt === undefined ? {} : { lastActivityAt: value.lastActivityAt }),
    updatedAt: value.updatedAt,
  };
}

export function messageFromRealtime(value: RealtimeMessage, offset: string): MessageProjection {
  return {
    id: value.id,
    conversationId: value.conversationId,
    senderId: value.senderId,
    type: value.type,
    body: value.body,
    ...(value.parts === undefined ? {} : { parts: value.parts }),
    ...(value.content === undefined ? {} : { content: value.content }),
    ...(value.replyTo === undefined ? {} : { replyTo: value.replyTo }),
    state: value.state,
    ...(value.stopReason === undefined ? {} : { stopReason: value.stopReason }),
    historyGeneration: value.historyGeneration,
    offset,
    revision: value.revision,
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
  };
}

function compareDecimal(left: string, right: string): number {
  const a = BigInt(left);
  const b = BigInt(right);
  return a < b ? -1 : a > b ? 1 : 0;
}

function maxDecimal(left: string, right: string): string {
  return compareDecimal(left, right) >= 0 ? left : right;
}

function advanceHydrationProof(
  proofs: Readonly<Record<string, ConversationHydrationProof>>,
  conversationId: string,
  update: { conversationRevision?: string; latestOffset?: string } = {},
): Readonly<Record<string, ConversationHydrationProof>> {
  const current = proofs[conversationId];
  if (!current) return proofs;
  return {
    ...proofs,
    [conversationId]: Object.freeze({
      ...current,
      ...(update.conversationRevision ? { conversationRevision: update.conversationRevision } : {}),
      ...(update.latestOffset
        ? { latestOffset: maxDecimal(current.latestOffset, update.latestOffset) }
        : {}),
      projectionRevision: nextProjectionRevision(current.projectionRevision),
    }),
  };
}

function isIdenticalHydration(
  snapshot: DomainProjectionSnapshot,
  commit: ConversationHydrationCommit,
): boolean {
  const current = snapshot.conversations[commit.conversation.id];
  const proof = snapshot.hydrationByConversation[commit.conversation.id];
  if (
    !current
    || !proof
    || current.historyGeneration !== commit.conversation.historyGeneration
    || current.revision !== commit.conversation.revision
    || proof.historyGeneration !== commit.conversation.historyGeneration
    || proof.historyBoundaryOffset !== commit.historyBoundaryOffset
    || proof.conversationRevision !== commit.conversation.revision
    || proof.latestOffset !== commit.latestOffset
  ) return false;
  const existing = Object.values(snapshot.messages)
    .filter((message) => message.conversationId === commit.conversation.id)
    .map((message) => `${message.id}\u0000${message.revision}`)
    .sort();
  const incoming = commit.messages
    .filter((message) => message.historyGeneration === commit.conversation.historyGeneration)
    .map((message) => `${message.id}\u0000${message.revision}`)
    .sort();
  return existing.length === incoming.length
    && existing.every((value, index) => value === incoming[index]);
}

function nextProjectionRevision(current?: string): string {
  return (BigInt(current ?? "0") + 1n).toString();
}

function filterMessages(
  messages: Readonly<Record<string, MessageProjection>>,
  predicate: (message: MessageProjection) => boolean,
): Readonly<Record<string, MessageProjection>> {
  return Object.fromEntries(Object.entries(messages).filter(([, message]) => predicate(message)));
}

function withoutKey<T>(record: Readonly<Record<string, T>>, key: string): Readonly<Record<string, T>> {
  const copy = { ...record };
  delete copy[key];
  return copy;
}

function freeze<T extends object>(value: T): T {
  return Object.freeze({ ...value });
}

function freezeSnapshot(snapshot: DomainProjectionSnapshot): DomainProjectionSnapshot {
  return Object.freeze({
    conversations: Object.freeze({ ...snapshot.conversations }),
    messages: Object.freeze({ ...snapshot.messages }),
    operations: Object.freeze({ ...snapshot.operations }),
    latestOffsetByConversation: Object.freeze({ ...snapshot.latestOffsetByConversation }),
    hydrationByConversation: Object.freeze({ ...snapshot.hydrationByConversation }),
  });
}
