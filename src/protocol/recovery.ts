import type { AnyRealtimeEventV1, RealtimeOrdering } from "./realtime.js";

export interface RealtimeCursor {
  streamSequence: string;
  historyGeneration?: string;
  projectionUid?: string;
  projectionEpoch?: string;
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
  if (incoming <= previous) return { action: "ignore_stale" };
  if (incoming > previous + 1n && event.ordering.completeness === "delta") {
    return { action: "rebase", reason: "sequence_gap" };
  }
  return { action: "apply", cursor: next };
}

function cursorFromOrdering(ordering: RealtimeOrdering): RealtimeCursor {
  return {
    streamSequence: ordering.streamSequence,
    historyGeneration: ordering.historyGeneration,
    projectionUid: ordering.projectionUid,
    projectionEpoch: ordering.projectionEpoch,
  };
}
