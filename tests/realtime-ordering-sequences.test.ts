/**
 * Replay of the shared ordering-sequence corpus through this SDK's recovery
 * evaluator.
 *
 * The single-event vectors ask whether one event is well formed. They cannot ask
 * whether a well-formed event should be applied given what arrived before it,
 * and that is the rule that broke on 2026-07-29: `evaluateRealtimeEvent`
 * compared `streamSequence` alone, while Message Service assigns the durable row
 * offset to `streamSequence` for every PATCH of one reply row. A 30-event
 * streaming reply therefore looked like 29 stale duplicates, `failInbound` never
 * fired because nothing was malformed, and the bubble simply stayed empty.
 *
 * So the corpus carries a sequence dimension: an event list plus the decision
 * expected at each step. The Go SDK replays the same sequences through its own
 * reducer. Two independent implementations reading one file is the only thing
 * that stops them drifting apart again.
 *
 * Scope note: this suite asserts decisions, not projected message state. Body
 * accumulation lives in `src/reducer.ts` and has its own shared corpus
 * (`reducer_vectors.json`); `expectProjection` in the sequence vectors is
 * therefore consumed by the Go SDK, whose reducer owns both concerns.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  evaluateScopedRealtimeEvent,
  validateRealtimeEvent,
  withScopedRealtimeCursor,
} from "../src/protocol/index.js";
import type {
  AnyRealtimeEventV1,
  RealtimeDeliveryAudience,
  RecoveryDecision,
  ScopedRealtimeCursors,
} from "../src/protocol/index.js";

const VENDORED_VECTORS_PATH = join(
  dirname(fileURLToPath(import.meta.url)),
  "testdata",
  "realtime_event_v1_vectors.json",
);

interface OrderingStep {
  readonly expect: string;
  readonly reason?: string;
  readonly event: unknown;
}

interface OrderingSequence {
  readonly name: string;
  readonly why: string;
  readonly steps: readonly OrderingStep[];
}

interface OrderingSuite {
  readonly contract: {
    readonly orderingDecisions: readonly string[];
    readonly orderingRebaseReasons: readonly string[];
  };
  readonly orderingSequences: readonly OrderingSequence[];
}

function loadSequences(): OrderingSuite {
  const parsed: unknown = JSON.parse(readFileSync(VENDORED_VECTORS_PATH, "utf8"));
  const suite = parsed as Partial<OrderingSuite>;
  const contract = suite?.contract;
  if (!contract || !Array.isArray(contract.orderingDecisions) || !Array.isArray(contract.orderingRebaseReasons)) {
    throw new Error(`ordering vectors have no usable contract block: ${VENDORED_VECTORS_PATH}`);
  }
  if (!Array.isArray(suite.orderingSequences) || suite.orderingSequences.length === 0) {
    throw new Error(`ordering vectors carry no sequences: ${VENDORED_VECTORS_PATH}`);
  }
  return suite as OrderingSuite;
}

const suite = loadSequences();

/**
 * This SDK's decision names happen to be the corpus's names, but the mapping is
 * explicit so that renaming an action here fails loudly instead of silently
 * never matching an expectation.
 */
function orderingToken(decision: RecoveryDecision): string {
  switch (decision.action) {
    case "apply":
      return "apply";
    case "ignore_stale":
      return "ignore_stale";
    case "rebase":
      return "rebase";
    default: {
      const exhaustive: never = decision;
      throw new Error(`unmapped recovery decision: ${JSON.stringify(exhaustive)}`);
    }
  }
}

function audienceOf(event: AnyRealtimeEventV1): RealtimeDeliveryAudience {
  const conversationId = event.scope.conversationId;
  if (!conversationId) throw new Error(`sequence event ${event.eventId} has no conversation scope`);
  return { kind: "conversation", conversationId };
}

describe("RealtimeEventV1 ordering sequences", () => {
  it("agrees with the corpus on the decision vocabulary", () => {
    // Derived from the file rather than restated: a decision the corpus adds
    // and this evaluator cannot produce has to fail here.
    expect([...suite.contract.orderingDecisions].sort()).toEqual(["apply", "ignore_stale", "rebase"]);
  });

  for (const sequence of suite.orderingSequences) {
    it(`decides ${sequence.name}`, () => {
      let cursors: ScopedRealtimeCursors = { conversations: {} };
      sequence.steps.forEach((step, index) => {
        expect(suite.contract.orderingDecisions, `step ${index} expects an undeclared decision`)
          .toContain(step.expect);
        // A step the producer could never emit would prove nothing, so each one
        // goes through the same validator the transport uses.
        const event = validateRealtimeEvent(step.event);
        const audience = audienceOf(event);
        const decision = evaluateScopedRealtimeEvent(cursors, event, audience);
        const context = `step ${index} (${event.type} ${event.eventId}, `
          + `streamSequence=${event.ordering.streamSequence} `
          + `entityRevision=${event.ordering.entityRevision ?? "<absent>"})\n  why: ${sequence.why}`;
        expect(orderingToken(decision), context).toBe(step.expect);
        if (decision.action === "rebase") {
          expect(suite.contract.orderingRebaseReasons).toContain(decision.reason);
          expect(decision.reason, context).toBe(step.reason);
        }
        if (decision.action === "apply") {
          cursors = withScopedRealtimeCursor(cursors, audience, decision.cursor);
        }
      });
    });
  }
});
