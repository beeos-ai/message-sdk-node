import { describe, expect, it } from "vitest";

import { ConversationWatchRegistry, ProjectionEngine } from "../src/facade/index.js";
import type { ConversationHydrationCommit } from "../src/facade/index.js";
import type { AnyRealtimeEventV1 } from "../src/protocol/index.js";
import {
  chunks,
  conversationId,
  createdEvent,
  deltaEvent,
  fullBody,
  messageId,
  messageOffset,
  terminalEvent,
  terminalRevision,
} from "./fixtures/v3-envelope.js";

/**
 * The replay gate. Events that arrive while a conversation is hydrating are
 * buffered and drained against the committed snapshot, so this is the second
 * admission point a streamed reply must survive. Under the v3 single-row
 * envelope every event of a turn carries the same messageOffset, so an
 * offset-only comparison discards the whole turn.
 */

const at = "2026-07-28T00:00:00.000Z";

function hydrationCommit(messages: ConversationHydrationCommit["messages"]): ConversationHydrationCommit {
  return {
    conversation: {
      id: conversationId,
      state: "open",
      historyGeneration: "0",
      revision: "1",
      updatedAt: at,
    },
    messages,
    historyBoundaryOffset: "0",
    latestOffset: messageOffset,
  };
}

/** The message as the HTTP page returns it: created, not yet streamed. */
function hydratedMessage() {
  return {
    id: messageId,
    conversationId,
    senderId: "agent_a",
    type: "agent_reply",
    body: "",
    state: "streaming",
    offset: messageOffset,
    revision: "1785305995400000",
    historyGeneration: "0",
    createdAt: at,
    updatedAt: at,
  };
}

/** Records every event the registry admits past the replay gate. */
class RecordingProjection extends ProjectionEngine {
  readonly applied: string[] = [];

  override apply(event: AnyRealtimeEventV1): boolean {
    this.applied.push(event.type);
    return super.apply(event);
  }
}

function bufferedDuringHydrate(projection: ProjectionEngine) {
  let finish!: (commit: ConversationHydrationCommit) => void;
  const pending = new Promise<ConversationHydrationCommit>((resolve) => { finish = resolve; });
  // RecoveryCoordinator commits the hydration snapshot before returning, so
  // the buffered drain runs against a projection that already holds the page.
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

describe("ConversationWatchRegistry replay gate under the v3 envelope", () => {
  it("admits all 29 same-offset deltas and the terminal buffered across a hydrate", async () => {
    expect(chunks).toHaveLength(29);

    const projection = new RecordingProjection();
    const { registry, commit } = bufferedDuringHydrate(projection);

    const watch = registry.watch(conversationId);
    await Promise.resolve();

    for (let index = 0; index < chunks.length; index++) {
      expect(registry.accept(deltaEvent(index))).toBe(true);
    }
    expect(registry.accept(terminalEvent())).toBe(true);

    commit(hydrationCommit([hydratedMessage()]));
    await watch.ready;

    expect(projection.applied).toEqual([
      ...chunks.map(() => "message.delta"),
      "message.terminal",
    ]);
    expect(projection.getSnapshot().messages[messageId]).toMatchObject({
      body: fullBody,
      offset: messageOffset,
      revision: terminalRevision,
      state: "completed",
    });
  });

  it("accumulates a reply that is still streaming when the hydrate commits", async () => {
    // No terminal. The terminal used to be the only event with an
    // entityRevision exemption, so it masked the loss of every delta by
    // carrying a full body snapshot of its own.
    const projection = new RecordingProjection();
    const { registry, commit } = bufferedDuringHydrate(projection);

    const watch = registry.watch(conversationId);
    await Promise.resolve();

    for (let index = 0; index < 3; index++) {
      expect(registry.accept(deltaEvent(index))).toBe(true);
    }

    commit(hydrationCommit([hydratedMessage()]));
    await watch.ready;

    expect(projection.applied).toEqual(["message.delta", "message.delta", "message.delta"]);
    expect(projection.getSnapshot().messages[messageId]).toMatchObject({
      body: chunks.slice(0, 3).join(""),
      state: "streaming",
    });
  });

  it("drains same-sequence deltas in revision order regardless of arrival order", async () => {
    // All 29 deltas tie on streamSequence, so the drain has to break the tie
    // on entityRevision or applyDelta rejects chunks whose bodyFrom does not
    // meet the body accumulated so far.
    const projection = new RecordingProjection();
    const { registry, commit } = bufferedDuringHydrate(projection);

    const watch = registry.watch(conversationId);
    await Promise.resolve();

    for (const index of [4, 0, 3, 1, 2]) {
      expect(registry.accept(deltaEvent(index))).toBe(true);
    }

    commit(hydrationCommit([hydratedMessage()]));
    await watch.ready;

    expect(projection.getSnapshot().messages[messageId]).toMatchObject({
      body: chunks.slice(0, 5).join(""),
    });
  });

  it("refuses a buffered event for a message the hydration page omitted", async () => {
    // The offset boundary still owns this case: a page that deliberately did
    // not return a message must not have it resurrected from the buffer.
    const projection = new RecordingProjection();
    const { registry, commit } = bufferedDuringHydrate(projection);

    const watch = registry.watch(conversationId);
    await Promise.resolve();

    expect(registry.accept(createdEvent())).toBe(true);
    expect(registry.accept(deltaEvent(0))).toBe(true);

    commit(hydrationCommit([]));
    await watch.ready;

    expect(projection.applied).toEqual([]);
    expect(projection.getSnapshot().messages[messageId]).toBeUndefined();
  });
});
