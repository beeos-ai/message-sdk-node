import type { AnyRealtimeEventV1 } from "./realtime.js";

/**
 * Bounded realtime event de-duplication. HTTP/WSS identity reconciliation is
 * owned by MessageClient's projection path, not by this transport-level set.
 */
export class RealtimeDedupe {
  private readonly seen = new Map<string, true>();

  constructor(private readonly maxEntries = 4096) {
    if (!Number.isInteger(maxEntries) || maxEntries < 1) {
      throw new Error("maxEntries must be a positive integer");
    }
  }

  accept(event: AnyRealtimeEventV1): boolean {
    const keys = [
      `event:${event.eventId}`,
      semanticKey(event),
    ].filter((key): key is string => key !== undefined);
    if (keys.some((key) => this.seen.has(key))) return false;
    for (const key of keys) this.remember(key);
    return true;
  }

  clear(): void {
    this.seen.clear();
  }

  private remember(key: string): void {
    this.seen.delete(key);
    this.seen.set(key, true);
    while (this.seen.size > this.maxEntries) {
      const oldest = this.seen.keys().next().value as string | undefined;
      if (oldest === undefined) return;
      this.seen.delete(oldest);
    }
  }
}

function semanticKey(event: AnyRealtimeEventV1): string | undefined {
  const revision = event.ordering.entityRevision ?? event.ordering.streamSequence;
  switch (event.type) {
    case "message.created":
      return `message.created:${event.scope.messageId}`;
    case "message.delta":
    case "message.updated":
    case "message.terminal":
    case "message.deleted":
      return `${event.type}:${event.scope.messageId}:${revision}`;
    case "conversation.created":
    case "conversation.updated":
    case "conversation.deleted":
      return `${event.type}:${event.scope.conversationId}:${revision}`;
    case "conversation.member.added":
    case "conversation.member.removed":
    case "conversation.read.updated":
    case "conversation.unread.updated":
    case "typing.started":
    case "typing.stopped":
      return `${event.type}:${event.scope.conversationId}:${revision}`;
    case "instance.updated":
      return `${event.type}:${event.scope.instanceId}:${revision}`;
    case "agent.updated":
      return `${event.type}:${event.scope.agentId}:${revision}`;
    case "inbox.conversation.available":
    case "inbox.conversation.unavailable":
      return `${event.type}:${event.data.conversationId}:${revision}`;
    case "operation.started":
    case "operation.progress":
    case "operation.terminal":
      return `${event.type}:${event.scope.operationId}:${revision}`;
    case "runtime.dispatch.failed":
      // Ephemeral dispatch failures dedupe only by stable eventId. They have
      // no durable entity revision/cursor and must not collapse distinct
      // attempts for the same message.
      return undefined;
  }
}
