import { describe, expect, it } from "vitest";

import { decodeRealtimeEvent, RealtimeDedupe } from "../src/protocol/index.js";

const base = {
  schemaVersion: 1,
  eventId: "evt-1",
  type: "future.additive.event",
  scope: { tenantId: "tenant-1", conversationId: "conversation-1", futureScope: "kept" },
  actor: { kind: "service", id: "message-service", futureActor: true },
  correlation: { correlationId: "corr-1", futureCorrelation: "kept" },
  occurredAt: "2026-07-31T00:00:00.000Z",
  data: { future: { nested: true } },
  futureEnvelope: "kept",
};

describe("personal realtime envelope", () => {
  it("minimally decodes and preserves unknown event types and additive fields", () => {
    const decoded = decodeRealtimeEvent(JSON.stringify(base)) as unknown as typeof base;
    expect(decoded).toEqual(base);
    expect("ordering" in decoded).toBe(false);
  });

  it("passes through legacy and unknown frames without schema gating", () => {
    const legacy = {
      type: "notification.created",
      payload: { title: "kept", additive: { nested: true } },
      transportHint: "legacy",
    };
    const future = {
      ...base,
      schemaVersion: 2,
      type: "future.event.v2",
      additive: { nested: [1, false, null] },
    };
    expect(decodeRealtimeEvent(legacy)).toEqual(legacy);
    expect(decodeRealtimeEvent(JSON.stringify(future))).toEqual(future);
  });

  it("does not reject additive fields on known operation events", () => {
    const event = {
      ...base,
      type: "operation.terminal",
      scope: {
        tenantId: "tenant-1",
        instanceId: "instance-1",
        operationId: "operation-1",
        runtimeEpoch: "epoch-9",
      },
      data: {
        operation: {
          id: "operation-1",
          instanceId: "instance-1",
          method: "session/new",
          result: { sessionId: "session-1", conversationId: "conversation-1" },
        },
        additive: "preserved",
      },
    };
    expect(decodeRealtimeEvent(event)).toEqual(event);
  });

  it("keeps operation.available as a thin durable-claim wake signal", () => {
    const event = {
      ...base,
      type: "operation.available",
      scope: {
        tenantId: "tenant-1",
        instanceId: "instance-1",
        operationId: "operation-1",
        runtimeEpoch: "epoch-9",
      },
      data: { operationId: "operation-1" },
    };
    expect(decodeRealtimeEvent(event)).toEqual(event);
  });

  it("deduplicates only by eventId in a bounded process-local LRU", () => {
    const dedupe = new RealtimeDedupe(2);
    const first = decodeRealtimeEvent(base);
    const sameEntityNewEvent = decodeRealtimeEvent({ ...base, eventId: "evt-2" });
    const third = decodeRealtimeEvent({ ...base, eventId: "evt-3" });
    expect(dedupe.accept(first)).toBe(true);
    expect(dedupe.accept(first)).toBe(false);
    expect(dedupe.accept(sameEntityNewEvent)).toBe(true);
    expect(dedupe.accept(third)).toBe(true);
    expect(dedupe.accept(first)).toBe(true);
  });
});
