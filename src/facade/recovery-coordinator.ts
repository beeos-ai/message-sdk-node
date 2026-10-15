import { HistoryWindowTooLargeError } from "../errors.js";
import { SingleflightHydrator } from "../protocol/index.js";
import type {
  ConversationProjection,
  ConversationQueryPort,
  MessageListPage,
  MessageProjection,
  RecentMessagePage,
  MessageQueryPort,
  PrivateConversationDirectoryQueryPort,
} from "./contracts.js";
import type { ConversationHydrationCommit, ProjectionEngine } from "./projection.js";

export class HistoryGenerationChangedError extends Error {
  constructor(readonly conversationId: string) {
    super(`conversation ${conversationId} changed history generation during recovery`);
    this.name = "HistoryGenerationChangedError";
  }
}

export interface RecoveryCoordinatorOptions {
  readonly conversations: Pick<ConversationQueryPort, "getConversation">;
  readonly privateDirectory?: PrivateConversationDirectoryQueryPort;
  readonly messages: Pick<MessageQueryPort, "listMessages" | "listRecentMessages" | "getMessage">;
  readonly projection: ProjectionEngine;
  /** Total complete G1/messages/G2 attempts. Default and maximum: 2. */
  readonly maxAttempts?: number;
}

/**
 * The SDK's sole generation-fenced recovery owner.
 *
 * Existing conversation/message reads are sufficient: it deliberately has no
 * sync endpoint, delta fallback, transport switch, or retry of commands.
 */
export class RecoveryCoordinator {
  private readonly singleflight = new SingleflightHydrator();
  private readonly maxAttempts: number;

  constructor(private readonly options: RecoveryCoordinatorOptions) {
    this.maxAttempts = options.maxAttempts ?? 2;
    if (!Number.isInteger(this.maxAttempts) || this.maxAttempts < 1 || this.maxAttempts > 2) {
      throw new Error("recovery maxAttempts must be 1 or 2");
    }
  }

  recoverConversation(conversationId: string): Promise<ConversationHydrationCommit> {
    if (!conversationId) return Promise.reject(new Error("conversationId is required"));
    return this.singleflight.hydrate(`conversation:${conversationId}`, () => this.run(conversationId));
  }

  async loadEarlier(conversationId: string): Promise<RecentMessagePage | undefined> {
    const proof = this.options.projection.getSnapshot().hydrationByConversation[conversationId];
    const cursor = proof?.window?.nextOlderCursor;
    if (!cursor || !proof.window?.hasOlder) return undefined;
    if (!this.options.messages.listRecentMessages) throw new Error("recent history unsupported");
    return this.singleflight.hydrate(`older:${conversationId}:${cursor}`, async () => {
      try {
        const page = await this.options.messages.listRecentMessages!(conversationId, cursor);
        this.options.projection.mergeEarlierHistory(conversationId, page, cursor);
        return page;
      } catch (error) {
        if (!(error instanceof HistoryWindowTooLargeError)) throw error;
        const commit = await this.readConversation(conversationId, true);
        if (this.options.projection.getSnapshot().conversations[conversationId]?.historyGeneration !== proof.historyGeneration) throw new HistoryGenerationChangedError(conversationId);
        this.options.projection.commitHydration(commit);
        return { messages: commit.messages, historyGeneration: commit.conversation.historyGeneration,
          historyBoundaryOffset: commit.historyBoundaryOffset, latestOffset: commit.latestOffset,
          snapshotUpperOffset: commit.latestOffset, hasOlder: false };
      }
    });
  }

  discoverPrivateDirectory(): Promise<readonly ConversationProjection[]> {
    if (!this.options.privateDirectory) return Promise.resolve([]);
    return this.singleflight.hydrate(
      "private-directory-discovery",
      () => this.readAllConversations(),
    );
  }

  recoverPrivateDirectoryProjection(
    conversations: readonly ConversationProjection[],
  ): Promise<readonly ConversationHydrationCommit[]> {
    return this.singleflight.hydrate("private-directory-hydrate", async () => {
      const commits: ConversationHydrationCommit[] = [];
      for (const conversation of conversations) {
        commits.push(await this.readConversation(conversation.id));
      }
      this.options.projection.replaceDirectoryHydrations(commits);
      return commits;
    });
  }

  private async run(conversationId: string): Promise<ConversationHydrationCommit> {
    const commit = await this.readConversation(conversationId);
    this.options.projection.commitHydration(commit);
    return commit;
  }

  private async readConversation(conversationId: string, full = false): Promise<ConversationHydrationCommit> {
    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      const g1 = await this.options.conversations.getConversation(conversationId);
      const recent = !full && g1.historyCapabilities?.includes("tail-v1") && this.options.messages.listRecentMessages;
      let page: RecentMessagePage | Awaited<ReturnType<RecoveryCoordinator["readAllMessages"]>>;
      try {
        page = recent ? await this.options.messages.listRecentMessages!(conversationId) : await this.readAllMessages(conversationId);
      } catch (error) {
        if (!(error instanceof HistoryWindowTooLargeError)) throw error;
        page = await this.readAllMessages(conversationId);
      }
      const g2 = await this.options.conversations.getConversation(conversationId);
      if (g1.historyGeneration !== g2.historyGeneration) {
        if (attempt === this.maxAttempts) throw new HistoryGenerationChangedError(conversationId);
        continue;
      }
      if (page.historyGeneration !== g2.historyGeneration) {
        if (attempt === this.maxAttempts) throw new HistoryGenerationChangedError(conversationId);
        continue;
      }
      const messages = page.messages.filter((message) => message.historyGeneration === g2.historyGeneration);
      if ('activeMessageIds' in page && page.activeMessageIds) {
        for (const id of page.activeMessageIds) {
          if (messages.some(row => row.id === id)) continue;
          if (!this.options.messages.getMessage) throw new Error("active history detail unsupported");
          const active = await this.options.messages.getMessage(conversationId,id);
          if (active.conversationId !== conversationId || active.historyGeneration !== page.historyGeneration) throw new HistoryGenerationChangedError(conversationId);
          messages.push(active);
        }
      }
      const commit: ConversationHydrationCommit = {
        ...('hasOlder' in page ? { window: {
          snapshotUpperOffset: page.snapshotUpperOffset, hasOlder: page.hasOlder,
          ...(page.nextOlderCursor ? { nextOlderCursor: page.nextOlderCursor } : {}),
        } } : {}),
        conversation: g2,
        messages,
        historyBoundaryOffset: page.historyBoundaryOffset,
        latestOffset: page.latestOffset,
      };
      return commit;
    }
    throw new HistoryGenerationChangedError(conversationId);
  }

  private async readAllConversations(): Promise<ConversationProjection[]> {
    const conversations: ConversationProjection[] = [];
    const seenIds = new Set<string>();
    const seenCursors = new Set<string>();
    for (const state of ["open", "closed"] as const) {
      let cursor: string | undefined;
      seenCursors.clear();
      while (true) {
        const page = await this.options.privateDirectory!.listPrivateConversations(state, cursor);
        for (const conversation of page.conversations) {
          if (seenIds.has(conversation.id)) continue;
          seenIds.add(conversation.id);
          conversations.push(conversation);
        }
        if (!page.hasMore) break;
        if (!page.nextCursor || page.nextCursor === cursor || seenCursors.has(page.nextCursor)) {
          throw new Error(`${state} conversation pagination did not advance`);
        }
        seenCursors.add(page.nextCursor);
        cursor = page.nextCursor;
      }
    }
    return conversations;
  }

  private async readAllMessages(conversationId: string): Promise<{
    messages: MessageProjection[];
    historyGeneration: string;
    historyBoundaryOffset: string;
    latestOffset: string;
  }> {
    const messages: MessageProjection[] = [];
    let since: string | undefined;
    let historyGeneration: string | undefined;
    let historyBoundaryOffset: string | undefined;
    let latestOffset = "0";
    const seenCursors = new Set<string>();
    while (true) {
      const page: MessageListPage = await this.options.messages.listMessages(conversationId, since);
      if (
        historyGeneration !== undefined &&
        (
          page.historyGeneration !== historyGeneration ||
          page.historyBoundaryOffset !== historyBoundaryOffset
        )
      ) {
        throw new HistoryGenerationChangedError(conversationId);
      }
      historyGeneration ??= page.historyGeneration;
      historyBoundaryOffset ??= page.historyBoundaryOffset;
      messages.push(...page.messages);
      latestOffset = maxDecimal(latestOffset, page.latestOffset);
      if (!page.hasMore) {
        return {
          messages,
          historyGeneration,
          historyBoundaryOffset,
          latestOffset,
        };
      }
      if (!page.nextSince || page.nextSince === since || seenCursors.has(page.nextSince)) {
        throw new Error("message pagination did not advance");
      }
      seenCursors.add(page.nextSince);
      since = page.nextSince;
    }
  }
}

function maxDecimal(left: string, right: string): string {
  return BigInt(left) >= BigInt(right) ? left : right;
}
