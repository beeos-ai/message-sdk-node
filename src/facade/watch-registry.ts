import type { AnyRealtimeEventV1 } from "../protocol/index.js";
import type { ConversationWatch } from "./contracts.js";
import type { RecoveryCoordinator } from "./recovery-coordinator.js";
import type {
  ConversationHydrationCommit,
  ProjectionApplyResult,
  ProjectionEngine,
} from "./projection.js";

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
}

export interface ScopeRecoveryResult {
  readonly commit?: ConversationHydrationCommit;
}

/**
 * Local ref-counted conversation filtering.
 *
 * A watch never changes WSS subscriptions. The one physical connection is
 * already bound by the server to the caller's personal inbox.
 */
export class ConversationWatchRegistry {
  private readonly entries = new Map<string, WatchEntry>();
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

  accept(event: AnyRealtimeEventV1): ProjectionApplyResult {
    const conversationId = event.scope.conversationId;
    if (!conversationId) return this.options.projection.apply(event);
    const entry = this.entries.get(conversationId);
    if (entry && !entry.readyState) {
      entry.buffered.push(event);
      return "changed";
    }
    return this.options.projection.apply(event);
  }

  refCount(conversationId: string): number {
    return this.entries.get(conversationId)?.refs ?? 0;
  }

  setDirectoryConversations(conversationIds: readonly string[]): void {
    const requested = new Set(conversationIds);
    for (const id of this.directoryConversationIds) {
      if (requested.has(id)) continue;
      this.directoryRecovery.delete(id);
    }
    this.directoryConversationIds = requested;
  }

  includeDirectoryConversation(conversationId: string): void {
    if (!conversationId) return;
    this.directoryConversationIds.add(conversationId);
  }

  recoverScope(
    conversationId: string,
    event?: AnyRealtimeEventV1,
  ): Promise<ScopeRecoveryResult> {
    const entry = this.entries.get(conversationId);
    if (!entry) {
      return this.recoverDirectoryScope(conversationId, event);
    }
    if (event) entry.buffered.push(event);
    if (entry.recovering) return entry.recovering.then((commit) => ({ commit }));
    entry.readyState = false;
    const run = (async () => {
      const commit = await this.options.recovery.recoverConversation(conversationId);
      for (const buffered of entry.buffered.splice(0)) {
        this.options.projection.apply(buffered);
      }
      entry.readyState = true;
      return commit;
    })();
    entry.recovering = run.finally(() => { entry.recovering = undefined; });
    return entry.recovering.then((commit) => ({ commit }));
  }

  async recoverAll(): Promise<void> {
    await Promise.all(this.watchedConversationIds().map((id) => this.recoverScope(id)));
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
    if (event) state.buffered.push(event);
    if (state.recovering) return state.recovering.then((commit) => ({ commit }));
    const run = (async () => {
      const commit = await this.options.recovery.recoverConversation(conversationId);
      for (const buffered of state!.buffered.splice(0)) {
        this.options.projection.apply(buffered);
      }
      return commit;
    })();
    state.recovering = run.finally(() => { state!.recovering = undefined; });
    return state.recovering.then((commit) => ({ commit }));
  }

  private async start(conversationId: string, entry: WatchEntry): Promise<void> {
    try {
      await this.options.recovery.recoverConversation(conversationId);
      for (const event of entry.buffered.splice(0)) {
        this.options.projection.apply(event);
      }
      entry.readyState = true;
      if (entry.refs === 0) this.entries.delete(conversationId);
    } catch (error) {
      if (this.entries.get(conversationId) === entry) this.entries.delete(conversationId);
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
        if (entry.refs === 0 && entry.readyState) this.entries.delete(conversationId);
      },
    };
  }
}
