import { RealtimeDedupe } from "../protocol/index.js";
import type { AnyRealtimeEventV1 } from "../protocol/index.js";
import type {
  ConversationWatch,
  RealtimeSession,
} from "./contracts.js";
import type { RecoveryCoordinator } from "./recovery-coordinator.js";
import type { ConversationHydrationCommit, ProjectionEngine } from "./projection.js";

interface WatchEntry {
  refs: number;
  readyState: boolean;
  readonly buffered: AnyRealtimeEventV1[];
  readonly ready: Promise<void>;
  recovering?: Promise<ConversationHydrationCommit>;
}

export interface ConversationWatchRegistryOptions {
  readonly recovery: Pick<RecoveryCoordinator, "recoverConversation">;
  readonly projection: ProjectionEngine;
  /** Returns the SDK-owned physical WSS session. */
  readonly getSession: () => RealtimeSession | undefined;
}

export interface ScopeRecoveryResult {
  readonly commit?: ConversationHydrationCommit;
  /**
   * True only for the caller that admitted the triggering event into the
   * recovery buffer. Concurrent duplicate deliveries share the hydrate but
   * must not share listener dispatch authority.
   */
  readonly eventAccepted: boolean;
}

/**
 * Ref-counted hidden logical subscriptions on one SDK-owned physical WSS.
 *
 * The registry is the only entry point for conversation realtime events:
 * eventId dedupe happens before fanout buffering, so the same server event
 * received more than once on an authorized conversation scope is projected once.
 */
export class ConversationWatchRegistry {
  private readonly entries = new Map<string, WatchEntry>();
  private readonly dedupe = new RealtimeDedupe();
  private directoryConversationIds = new Set<string>();
  private readonly directoryRecovery = new Map<string, {
    buffered: AnyRealtimeEventV1[];
    recovering?: Promise<ConversationHydrationCommit>;
  }>();

  constructor(private readonly options: ConversationWatchRegistryOptions) {}

  watch(conversationId: string): ConversationWatch {
    if (!conversationId) throw new Error("conversationId is required");
    const active = this.entries.get(conversationId);
    if (active) {
      active.refs++;
      return this.handle(conversationId, active);
    }

    let resolveReady!: () => void;
    let rejectReady!: (error: unknown) => void;
    const ready = new Promise<void>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    const entry: WatchEntry = { refs: 1, readyState: false, buffered: [], ready };
    this.entries.set(conversationId, entry);

    void this.start(conversationId, entry).then(resolveReady, rejectReady);
    return this.handle(conversationId, entry);
  }

  /**
   * Returns true when the event was accepted by this registry. A buffered
   * event is accepted but is not visible until the fenced hydrate commits.
   */
  accept(event: AnyRealtimeEventV1): boolean {
    if (!this.dedupe.accept(event)) return false;
    const conversationId = event.scope.conversationId;
    if (!conversationId) {
      this.options.projection.apply(event);
      return true;
    }
    const entry = this.entries.get(conversationId);
    if (!entry && !this.directoryConversationIds.has(conversationId)) return false;
    if (!entry) {
      this.options.projection.apply(event);
      return true;
    }
    if (!entry.readyState) {
      entry.buffered.push(event);
      return true;
    }
    this.options.projection.apply(event);
    return true;
  }

  refCount(conversationId: string): number {
    return this.entries.get(conversationId)?.refs ?? 0;
  }

  async setDirectoryConversations(conversationIds: readonly string[]): Promise<void> {
    const session = this.options.getSession();
    if (!session) throw new Error("directory subscriptions require the SDK realtime connection");
    const next = new Set(conversationIds);
    const added = [...next].filter((id) => !this.directoryConversationIds.has(id));
    const removed = [...this.directoryConversationIds].filter((id) => !next.has(id));
    const subscribed: string[] = [];
    try {
      for (const id of added) {
        await session.setConversationWatched(id, true);
        subscribed.push(id);
      }
      // Existing logical subscriptions receive a new server authorization
      // barrier after every physical reconnect. Never hydrate or drain the
      // recovery buffer until every current directory id is re-subscribed.
      await Promise.all(
        [...next].filter((id) => !added.includes(id))
          .map((id) => session.setConversationWatched(id, true)),
      );
    } catch (error) {
      await Promise.all(subscribed.map((id) =>
        Promise.resolve(session.setConversationWatched(id, false)).catch(() => undefined)
      ));
      throw error;
    }
    for (const id of removed) {
      // Directory authority revokes the internal subscription and invalidates
      // any already-ready UI watch. A caller must acquire a fresh watch after
      // authority becomes available again; a stale handle cannot keep access.
      this.entries.delete(id);
      this.directoryRecovery.delete(id);
      await session.setConversationWatched(id, false);
    }
    this.directoryConversationIds = next;
  }


  recoverScope(
    conversationId: string,
    event?: AnyRealtimeEventV1,
  ): Promise<ScopeRecoveryResult> {
    const entry = this.entries.get(conversationId);
    if (!entry && this.directoryConversationIds.has(conversationId)) {
      return this.recoverDirectoryScope(conversationId, event);
    }
    if (!entry) return Promise.resolve({ eventAccepted: false });
    let eventAccepted = false;
    if (event) {
      eventAccepted = this.dedupe.accept(event);
      if (!eventAccepted) {
        return entry.recovering
          ? entry.recovering.then((commit) => ({ commit, eventAccepted: false }))
          : Promise.resolve({ eventAccepted: false });
      }
      entry.buffered.push(event);
    }
    if (entry.recovering) {
      return entry.recovering.then((commit) => ({ commit, eventAccepted }));
    }
    entry.readyState = false;
    const run = (async () => {
      const commit = await this.options.recovery.recoverConversation(conversationId);
      entry.buffered.sort(compareScopedOrder);
      for (const buffered of entry.buffered.splice(0)) {
        if (isNewerThanCommit(buffered, commit.latestOffset, this.options.projection)) {
          this.options.projection.apply(buffered);
        }
      }
      entry.readyState = true;
      return commit;
    })();
    entry.recovering = run.finally(() => { entry.recovering = undefined; });
    return entry.recovering.then((commit) => ({ commit, eventAccepted }));
  }

  async recoverAll(): Promise<void> {
    await Promise.all([...this.entries.keys()].map((id) => this.recoverScope(id)));
  }

  async reauthorizeAll(): Promise<void> {
    const session = this.options.getSession();
    if (!session) throw new Error("conversation reauthorization requires the SDK realtime connection");
    await Promise.all(
      this.watchedConversationIds().map((id) => session.setConversationWatched(id, true)),
    );
  }
  watchedConversationIds(): string[] {
    return [...new Set([
      ...this.directoryConversationIds,
      ...this.entries.keys(),
    ])].sort();
  }

  isAuthorized(conversationId: string): boolean {
    return this.directoryConversationIds.has(conversationId) || this.entries.has(conversationId);
  }

  private recoverDirectoryScope(
    conversationId: string,
    event?: AnyRealtimeEventV1,
  ): Promise<ScopeRecoveryResult> {
    let state = this.directoryRecovery.get(conversationId);
    if (!state) {
      state = { buffered: [] };
      this.directoryRecovery.set(conversationId, state);
    }
    const eventAccepted = event ? this.dedupe.accept(event) : false;
    if (event && eventAccepted) state.buffered.push(event);
    if (state.recovering) {
      return state.recovering.then((commit) => ({ commit, eventAccepted }));
    }
    if (event && !eventAccepted) return Promise.resolve({ eventAccepted: false });
    const run = (async () => {
      const commit = await this.options.recovery.recoverConversation(conversationId);
      state!.buffered.sort(compareScopedOrder);
      for (const buffered of state!.buffered.splice(0)) {
        if (isNewerThanCommit(buffered, commit.latestOffset, this.options.projection)) {
          this.options.projection.apply(buffered);
        }
      }
      return commit;
    })();
    state.recovering = run.finally(() => { state!.recovering = undefined; });
    return state.recovering.then((commit) => ({ commit, eventAccepted }));
  }

  private async start(conversationId: string, entry: WatchEntry): Promise<void> {
    const session = this.options.getSession();
    if (!session) throw new Error("conversation watch requires the SDK realtime connection");
    let subscribed = false;
    try {
      // Subscribe/buffer first. No HTTP response can race ahead of registration.
      if (!this.directoryConversationIds.has(conversationId)) {
        await session.setConversationWatched(conversationId, true);
        subscribed = true;
      }
      const commit = await this.options.recovery.recoverConversation(conversationId);

      // Recovery committed the generation-fenced HTTP snapshot. A buffered
      // message at/below latestOffset is already represented by the snapshot,
      // except a same-offset terminal carrying a newer entity revision.
      entry.buffered.sort(compareScopedOrder);
      for (const event of entry.buffered.splice(0)) {
        if (isNewerThanCommit(event, commit.latestOffset, this.options.projection)) {
          this.options.projection.apply(event);
        }
      }
      entry.readyState = true;

      // A caller may release while recovery is in flight.
      if (entry.refs === 0) {
        this.entries.delete(conversationId);
        if (!this.directoryConversationIds.has(conversationId)) {
          await session.setConversationWatched(conversationId, false);
        }
      }
    } catch (error) {
      // A failed ready promise must not leak a hidden subscription or poison
      // later watch attempts for the same conversation.
      if (this.entries.get(conversationId) === entry) this.entries.delete(conversationId);
      if (subscribed) await session.setConversationWatched(conversationId, false);
      throw error;
    }
  }

  private handle(conversationId: string, entry: WatchEntry): ConversationWatch {
    let released = false;
    return {
      conversationId,
      ready: entry.ready,
      release: () => {
        if (released) return;
        released = true;
        entry.refs--;
        if (entry.refs > 0) return;
        if (!entry.readyState) {
          if (!this.directoryConversationIds.has(conversationId)) {
            void Promise.resolve(
              this.options.getSession()?.setConversationWatched(conversationId, false),
            ).catch(() => undefined);
          }
          return;
        }
        this.entries.delete(conversationId);
        if (!this.directoryConversationIds.has(conversationId)) {
          void Promise.resolve(
            this.options.getSession()?.setConversationWatched(conversationId, false),
          ).catch(() => undefined);
        }
      },
    };
  }
}

function compareScopedOrder(left: AnyRealtimeEventV1, right: AnyRealtimeEventV1): number {
  const a = BigInt(left.ordering.streamSequence);
  const b = BigInt(right.ordering.streamSequence);
  return a < b ? -1 : a > b ? 1 : 0;
}

function isNewerThanCommit(
  event: AnyRealtimeEventV1,
  latestOffset: string,
  projection: ProjectionEngine,
): boolean {
  if (!event.type.startsWith("message.")) return true;
  const offset = event.ordering.messageOffset;
  if (!offset) return false;
  const offsetOrder = compareDecimal(offset, latestOffset);
  if (offsetOrder > 0) return true;
  if (offsetOrder < 0 || event.type !== "message.terminal") return false;

  const current = event.scope.messageId
    ? projection.getSnapshot().messages[event.scope.messageId]
    : undefined;
  const revision = event.ordering.entityRevision;
  return Boolean(current && revision && compareDecimal(revision, current.revision) > 0);
}

function compareDecimal(left: string, right: string): number {
  return BigInt(left) < BigInt(right) ? -1 : BigInt(left) > BigInt(right) ? 1 : 0;
}
