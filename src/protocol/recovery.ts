import type { AnyRealtimeEventV1, RealtimeOrdering } from "./realtime.js";

export interface RealtimeCursor {
  streamSequence: string;
  /**
   * Monotonic version of the entity the event mutates. Required to order
   * events that share a `streamSequence` — see `evaluateRealtimeEvent`.
   */
  entityRevision?: string;
  historyGeneration?: string;
  projectionUid?: string;
  projectionEpoch?: string;
}

export type RealtimeDeliveryAudience =
  | { readonly kind: "private-control" }
  | { readonly kind: "conversation"; readonly conversationId: string };

export interface ScopedRealtimeCursors {
  readonly privateControl?: RealtimeCursor;
  readonly conversations: Readonly<Record<string, RealtimeCursor>>;
}

export type RecoveryDecision =
  | { action: "apply"; cursor: RealtimeCursor }
  | { action: "ignore_stale" }
  | { action: "rebase"; reason: "sequence_gap" | "history_generation_changed" | "projection_epoch_changed" };

/**
 * Exactly one layer may own connection recovery. UI adapters acquire this
 * lease from the SDK rather than registering independent reconnect reloads.
 */
export class RecoveryOwnership {
  private owner?: string;

  acquire(owner: string): () => void {
    if (!owner) throw new Error("recovery owner is required");
    if (this.owner && this.owner !== owner) {
      throw new Error(`recovery is already owned by ${this.owner}`);
    }
    this.owner = owner;
    return () => {
      if (this.owner === owner) this.owner = undefined;
    };
  }
}

export function evaluateScopedRealtimeEvent(
  cursors: ScopedRealtimeCursors,
  event: AnyRealtimeEventV1,
  audience?: RealtimeDeliveryAudience,
): RecoveryDecision {
  return evaluateRealtimeEvent(cursorForAudience(cursors, audience ?? inferredAudience(event)), event);
}

export function realtimeScopeKey(audience: RealtimeDeliveryAudience): string {
  return audience.kind === "private-control"
    ? "private-control"
    : `conversation:${audience.conversationId}`;
}

export function withScopedRealtimeCursor(
  cursors: ScopedRealtimeCursors,
  audience: RealtimeDeliveryAudience,
  cursor: RealtimeCursor,
): ScopedRealtimeCursors {
  if (audience.kind === "conversation") return {
    ...cursors,
    conversations: { ...cursors.conversations, [audience.conversationId]: cursor },
  };
  return { ...cursors, privateControl: cursor };
}

/**
 * An equal `streamSequence` is legitimate and must not be treated as a replay.
 *
 * Message Service derives `streamSequence` from the `channel_messages` row
 * offset, and Message Envelope v3 (ADR-0023) keeps one row per assistant turn
 * and advances it with PATCH. Every event of a streamed reply — the create,
 * each delta, the terminal — therefore reports the same sequence. Rejecting
 * `incoming <= previous` silently discarded all but the first frame of every
 * reply, and because the caller drops the event before dedupe, projection, and
 * listener dispatch, the loss was invisible end to end.
 *
 * `streamSequence` orders *rows*; `entityRevision` orders mutations *within* a
 * row. So a tie on sequence is decided by revision, which Message Service
 * advances on every PATCH. The pre-v3 SSE reader encoded exactly this rule and
 * the knowledge was lost in the WebSocket migration; keep the reason written
 * down so it is not "simplified" back to `<=`.
 *
 * Genuine duplicates still cost nothing: an event that repeats a revision is
 * refused here, and identical frames are caught downstream by `eventId`
 * dedupe. A tie that cannot be adjudicated — either side missing a revision —
 * fails closed rather than guessing.
 */
export function evaluateRealtimeEvent(cursor: RealtimeCursor | undefined, event: AnyRealtimeEventV1): RecoveryDecision {
  const next = cursorFromOrdering(event.ordering);
  if (!cursor) return { action: "apply", cursor: next };
  if (cursor.historyGeneration && next.historyGeneration && cursor.historyGeneration !== next.historyGeneration) {
    return { action: "rebase", reason: "history_generation_changed" };
  }
  if (cursor.projectionEpoch && next.projectionEpoch && cursor.projectionEpoch !== next.projectionEpoch) {
    return { action: "rebase", reason: "projection_epoch_changed" };
  }
  const previous = BigInt(cursor.streamSequence);
  const incoming = BigInt(next.streamSequence);
  if (incoming < previous) return { action: "ignore_stale" };
  if (incoming === previous) {
    return advancesEntityRevision(cursor.entityRevision, next.entityRevision)
      ? { action: "apply", cursor: next }
      : { action: "ignore_stale" };
  }
  // A missing full event can carry an irreversible delete, membership, or
  // security change. Never accept a sequence gap merely because the arriving
  // event describes a full entity projection.
  if (incoming > previous + 1n) {
    return { action: "rebase", reason: "sequence_gap" };
  }
  return { action: "apply", cursor: next };
}

/**
 * Both revisions must be present decimal integers. A missing one is not
 * "revision zero": it is an unorderable tie, and admitting it would let a
 * replayed frame re-enter the projection.
 */
function advancesEntityRevision(previous: string | undefined, incoming: string | undefined): boolean {
  if (!isDecimal(previous) || !isDecimal(incoming)) return false;
  return BigInt(incoming) > BigInt(previous);
}

function isDecimal(value: string | undefined): value is string {
  return value !== undefined && /^\d+$/.test(value);
}

function cursorForAudience(cursors: ScopedRealtimeCursors, audience: RealtimeDeliveryAudience): RealtimeCursor | undefined {
  return audience.kind === "private-control"
    ? cursors.privateControl
    : cursors.conversations[audience.conversationId];
}

function inferredAudience(event: AnyRealtimeEventV1): RealtimeDeliveryAudience {
  return event.scope.conversationId
    ? { kind: "conversation", conversationId: event.scope.conversationId }
    : { kind: "private-control" };
}

function cursorFromOrdering(ordering: RealtimeOrdering): RealtimeCursor {
  return {
    streamSequence: ordering.streamSequence,
    entityRevision: ordering.entityRevision,
    historyGeneration: ordering.historyGeneration,
    projectionUid: ordering.projectionUid,
    projectionEpoch: ordering.projectionEpoch,
  };
}
