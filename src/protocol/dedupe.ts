import type { AnyRealtimeEventV1 } from "./realtime.js";

/**
 * Process-local bounded event-id de-duplication.
 *
 * This is intentionally not durable and is not a recovery cursor. Reconnect
 * recovery always hydrates authoritative HTTP projections.
 */
export class RealtimeDedupe {
  private readonly seen = new Map<string, true>();

  constructor(private readonly maxEntries = 4096) {
    if (!Number.isInteger(maxEntries) || maxEntries < 1) {
      throw new Error("maxEntries must be a positive integer");
    }
  }

  accept(event: AnyRealtimeEventV1): boolean {
    if (this.seen.has(event.eventId)) return false;
    this.seen.set(event.eventId, true);
    while (this.seen.size > this.maxEntries) {
      const oldest = this.seen.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.seen.delete(oldest);
    }
    return true;
  }

  clear(): void {
    this.seen.clear();
  }
}
