/**
 * Replay of the shared ordering-sequence corpus through the *second* admission
 * gate: the drain of events buffered while a conversation hydrates.
 *
 * There were two doors, not one. `evaluateRealtimeEvent` guards the live
 * connection and `isNewerThanCommit` guards the buffered drain, and both used to
 * demand a strictly greater number — sequence in one case, `messageOffset` in
 * the other. Under the single-row envelope every event of a reply carries the
 * same offset, so a conversation recovered mid-reply lost the same 29 deltas the
 * live path lost. The terminal survived on an explicit exemption and, because it
 * carries a full body, papered over the loss instead of exposing it.
 *
 * A corpus sequence with a `replay` block is one list read two ways: steps below
 * `bufferedFromStep` are what the hydration page committed, steps from it on are
 * what sat in the buffer. The hydration payload is REST-shaped, so this test
 * builds the committed snapshot out of the committed steps rather than expecting
 * the realtime corpus to carry a foreign contract.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { describe, expect, it } from "vitest";

import { ConversationWatchRegistry, ProjectionEngine } from "../src/facade/index.js";
import type { ConversationHydrationCommit, MessageProjection } from "../src/facade/index.js";
import { validateRealtimeEvent } from "../src/protocol/index.js";
import type { AnyRealtimeEventV1 } from "../src/protocol/index.js";

const VENDORED_VECTORS_PATH = join(
  dirname(fileURLToPath(import.meta.url)),
  "testdata",
  "realtime_event_v1_vectors.json",
);

interface ReplayStep {
  readonly expect: string;
  readonly expectBuffered?: string;
  readonly event: unknown;
}

interface ReplaySequence {
  readonly name: string;
  readonly why: string;
  readonly steps: readonly ReplayStep[];
  readonly replay?: {
    readonly committedLatestOffset: string;
    readonly bufferedFromStep: number;
  };
  readonly expectProjection?: {
    readonly exists: boolean;
    readonly body: string;
    readonly state: string;
  };
}

interface ReplaySuite {
  readonly contract: { readonly replayDecisions: readonly string[] };
  readonly orderingSequences: readonly ReplaySequence[];
}

function loadSuite(): ReplaySuite {
  const parsed = JSON.parse(readFileSync(VENDORED_VECTORS_PATH, "utf8")) as Partial<ReplaySuite>;
  if (!Array.isArray(parsed?.contract?.replayDecisions) || parsed.contract.replayDecisions.length === 0) {
    throw new Error(`ordering vectors declare no replay decisions: ${VENDORED_VECTORS_PATH}`);
  }
  if (!Array.isArray(parsed.orderingSequences)) {
    throw new Error(`ordering vectors carry no sequences: ${VENDORED_VECTORS_PATH}`);
  }
  return parsed as ReplaySuite;
}

const suite = loadSuite();
const replaySequences = suite.orderingSequences.filter((sequence) => sequence.replay !== undefined);

/**
 * The message as the hydration page returns it. Derived from a committed step so
 * the two views of the sequence cannot describe different rows.
 */
function hydratedMessage(event: AnyRealtimeEventV1): MessageProjection {
  const snapshot = (event.data as { message?: Record<string, unknown> }).message;
  if (!snapshot) throw new Error(`committed step ${event.eventId} carries no message snapshot`);
  return {
    id: String(snapshot.id),
    conversationId: String(snapshot.conversationId),
    senderId: String(snapshot.senderId),
    type: String(snapshot.type),
    body: String(snapshot.body ?? ""),
    state: String(snapshot.state),
    offset: String(snapshot.offset),
    revision: String(event.ordering.entityRevision),
    historyGeneration: String(snapshot.historyGeneration),
    createdAt: String(snapshot.createdAt),
    updatedAt: String(snapshot.updatedAt),
  } as MessageProjection;
}

function hydrationCommit(
  conversationId: string,
  messages: readonly MessageProjection[],
  latestOffset: string,
  updatedAt: string,
): ConversationHydrationCommit {
  return {
    conversation: {
      id: conversationId,
      state: "open",
      historyGeneration: "0",
      revision: "1",
      updatedAt,
    },
    messages,
    historyBoundaryOffset: "0",
    latestOffset,
  } as ConversationHydrationCommit;
}

/** Records every event the registry admits past the replay gate. */
class RecordingProjection extends ProjectionEngine {
  readonly admitted: string[] = [];

  override apply(event: AnyRealtimeEventV1): boolean {
    this.admitted.push(event.eventId);
    return super.apply(event);
  }
}

function bufferedDuringHydrate(projection: ProjectionEngine) {
  let finish!: (commit: ConversationHydrationCommit) => void;
  const pending = new Promise<ConversationHydrationCommit>((resolve) => { finish = resolve; });
  // RecoveryCoordinator commits the snapshot before returning, so the drain runs
  // against a projection that already holds the page.
  const hydrated = pending.then((commit) => {
    projection.commitHydration(commit);
    return commit;
  });
  const registry = new ConversationWatchRegistry({
    recovery: { recoverConversation: () => hydrated },
    projection,
    getSession: () => ({
      connect: async () => undefined,
      disconnect: async () => undefined,
      setToken: async () => undefined,
      setConversationWatched: async () => undefined,
    }) as never,
  });
  return { registry, commit: finish };
}

describe("RealtimeEventV1 ordering sequences on the buffered replay gate", () => {
  it("finds replay coverage in the corpus", () => {
    // Not a formality: the live gate and the replay gate failed the same way, so
    // a corpus that only describes the live one leaves half the bug uncovered.
    expect(replaySequences.length).toBeGreaterThan(0);
  });

  for (const sequence of replaySequences) {
    it(`drains ${sequence.name}`, async () => {
      const replay = sequence.replay!;
      const events = sequence.steps.map((step) => validateRealtimeEvent(step.event));
      const committed = events.slice(0, replay.bufferedFromStep);
      const buffered = events.slice(replay.bufferedFromStep);
      const conversationId = events[0].scope.conversationId;
      if (!conversationId) throw new Error(`sequence ${sequence.name} has no conversation scope`);

      const projection = new RecordingProjection();
      const { registry, commit } = bufferedDuringHydrate(projection);
      const watch = registry.watch(conversationId);
      await Promise.resolve();

      for (const event of buffered) {
        expect(registry.accept(event), `the registry must buffer ${event.eventId} while hydrating`).toBe(true);
      }

      commit(hydrationCommit(
        conversationId,
        committed.map(hydratedMessage),
        replay.committedLatestOffset,
        events[0].occurredAt,
      ));
      await watch.ready;

      const wanted = buffered
        .filter((_, index) => sequence.steps[replay.bufferedFromStep + index].expectBuffered === "apply")
        .map((event) => event.eventId);
      expect(projection.admitted, `why: ${sequence.why}`).toEqual(wanted);

      if (!sequence.expectProjection) return;
      const message = projection.getSnapshot().messages[events[0].scope.messageId!];
      expect(message).toMatchObject({
        body: sequence.expectProjection.body,
        state: sequence.expectProjection.state,
      });
    });
  }
});
