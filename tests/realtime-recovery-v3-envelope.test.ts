import { describe, expect, it } from "vitest";

import { ProjectionEngine } from "../src/facade/index.js";
import {
  evaluateScopedRealtimeEvent,
  withScopedRealtimeCursor,
  type RealtimeDeliveryAudience,
  type ScopedRealtimeCursors,
} from "../src/protocol/index.js";
import type { AnyRealtimeEventV1 } from "../src/protocol/index.js";
import {
  chunks,
  conversationId,
  createdEvent,
  deltaEvent,
  deltaRevision,
  fullBody,
  historyGeneration,
  messageId,
  messageOffset,
  ordering,
  streamSequence,
  terminalEvent,
  terminalRevision,
} from "./fixtures/v3-envelope.js";

/**
 * The live-connection gate. Every event of one streamed reply carries the same
 * `streamSequence` under the v3 single-row envelope, so a strict
 * greater-than comparison discards all of them but the first — before dedupe,
 * before projection, before listener dispatch.
 */

const audience: RealtimeDeliveryAudience = { kind: "conversation", conversationId };

/** Drives the gate exactly as UnifiedMessageClient.processInbound does. */
function runStream(events: readonly AnyRealtimeEventV1[]): {
  decisions: string[];
  projection: ProjectionEngine;
} {
  let cursors: ScopedRealtimeCursors = { conversations: {} };
  const projection = new ProjectionEngine();
  const decisions: string[] = [];
  for (const event of events) {
    const decision = evaluateScopedRealtimeEvent(cursors, event, audience);
    decisions.push(decision.action);
    if (decision.action !== "apply") continue;
    cursors = withScopedRealtimeCursor(cursors, audience, decision.cursor);
    projection.apply(event);
  }
  return { decisions, projection };
}

describe("evaluateScopedRealtimeEvent under the v3 single-row envelope", () => {
  it("applies every same-sequence event of one streamed reply", () => {
    expect(chunks).toHaveLength(29);

    const events: AnyRealtimeEventV1[] = [
      createdEvent(),
      ...chunks.map((_chunk, index) => deltaEvent(index)),
      terminalEvent(),
    ];
    const { decisions, projection } = runStream(events);

    expect(decisions).toEqual(events.map(() => "apply"));
    expect(projection.getSnapshot().messages[messageId]).toMatchObject({
      body: fullBody,
      offset: messageOffset,
      revision: terminalRevision,
      state: "completed",
    });
  });

  it("still ignores a replayed event whose entity revision does not advance", () => {
    const { decisions } = runStream([
      createdEvent(),
      deltaEvent(0),
      deltaEvent(0),
      deltaEvent(1),
    ]);

    expect(decisions).toEqual(["apply", "apply", "ignore_stale", "apply"]);
  });

  it("ignores a same-sequence event that regresses the entity revision", () => {
    const { decisions } = runStream([
      createdEvent(),
      deltaEvent(0),
      deltaEvent(1),
      deltaEvent(0),
    ]);

    expect(decisions).toEqual(["apply", "apply", "apply", "ignore_stale"]);
  });

  it("fails closed on a same-sequence event with no entity revision", () => {
    const bare = deltaEvent(3);
    const { entityRevision: _omitted, ...orderingWithoutRevision } = bare.ordering;
    const { decisions } = runStream([
      createdEvent(),
      { ...bare, ordering: orderingWithoutRevision } as AnyRealtimeEventV1,
    ]);

    expect(decisions).toEqual(["apply", "ignore_stale"]);
  });

  it("still rebases on a genuine sequence gap", () => {
    const gapped = {
      ...deltaEvent(0),
      ordering: ordering(deltaRevision(0), "20"),
    } as AnyRealtimeEventV1;

    expect(runStream([createdEvent(), gapped]).decisions).toEqual(["apply", "rebase"]);
  });

  it("still ignores an event whose sequence regresses", () => {
    const older = {
      ...deltaEvent(0),
      ordering: ordering(deltaRevision(0), "13"),
    } as AnyRealtimeEventV1;

    expect(runStream([createdEvent(), older]).decisions).toEqual(["apply", "ignore_stale"]);
  });

  it("carries the entity revision into the cursor it returns", () => {
    const decision = evaluateScopedRealtimeEvent(
      { conversations: {} },
      deltaEvent(0),
      audience,
    );

    expect(decision).toMatchObject({
      action: "apply",
      cursor: { streamSequence, entityRevision: deltaRevision(0), historyGeneration },
    });
  });
});
