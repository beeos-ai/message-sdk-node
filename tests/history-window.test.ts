import { describe, expect, it, vi } from "vitest";
import { HistoryWindowTooLargeError } from "../src/errors.js";
import { RecoveryCoordinator } from "../src/facade/recovery-coordinator.js";
import { ProjectionEngine } from "../src/facade/projection.js";
import type { ConversationProjection, MessageProjection, RecentMessagePage } from "../src/facade/contracts.js";
import { createMessageClient } from "../src/unified-client.js";
import { createGatewayMessageClientComposition } from "../src/gateway-runtime.js";

const conversation: ConversationProjection = { id: "c1", agentId: "a1", state: "open", historyGeneration: "2", revision: "3", updatedAt: "2026-10-15T00:00:00Z", historyCapabilities: ["tail-v1"] };
function row(id: string, offset: string, revision = "1"): MessageProjection {
  return { id, conversationId: "c1", senderId: "user:1", type: "chat_message", body: id, state: "completed", historyGeneration: "2", offset, revision, createdAt: conversation.updatedAt, updatedAt: conversation.updatedAt };
}
function page(messages: MessageProjection[], hasOlder: boolean, cursor?: string): RecentMessagePage {
  return { messages, historyGeneration: "2", historyBoundaryOffset: "0", latestOffset: "6000", snapshotUpperOffset: "6000", hasOlder, ...(cursor ? { nextOlderCursor: cursor } : {}) };
}

describe("bounded history recovery", () => {
  it("does not probe a recent endpoint on an old server", async () => {
    const fetch = vi.fn(async () => new Response(JSON.stringify({ success: true, data: { conversation_id: "c1", agent_id: "a1", state: "active", history_generation: "2", metadata_version: "3" } }), { status: 200 }));
    const composition = createGatewayMessageClientComposition({ gatewayUrl: "https://gateway.example", platform: "web", currentPrincipal: { currentPrincipalId: () => "user" }, fetch: fetch as typeof globalThis.fetch });
    composition.conversationRoutes!.bindConversation("c1", "a1");
    await expect(createMessageClient(composition).messages.recent("c1")).rejects.toThrow("unsupported");
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("recovers an active reply outside the recent page through its authorized detail entry", async () => {
    const projection = new ProjectionEngine();
    const getMessage = vi.fn(async () => ({ ...row("active", "1"), senderId: "agent", state: "streaming" as const }));
    const recent = vi.fn(async () => ({ ...page([row("last", "6000")], true, "older"), activeMessageIds: ["active"] }));
    const recovery = new RecoveryCoordinator({ conversations: { getConversation: async () => conversation }, messages: { listMessages: vi.fn(), listRecentMessages: recent, getMessage }, projection });
    await recovery.recoverConversation("c1");
    expect(getMessage).toHaveBeenCalledExactlyOnceWith("c1", "active");
    expect(projection.getSnapshot().messages.active.state).toBe("streaming");
  });

  it("keeps oversized legacy envelopes accessible by using the existing full recovery path", async () => {
    const projection = new ProjectionEngine();
    const listMessages = vi.fn(async () => ({ messages: [row("huge", "3")], historyGeneration: "2", historyBoundaryOffset: "0", latestOffset: "3", hasMore: false }));
    const recovery = new RecoveryCoordinator({ conversations: { getConversation: async () => conversation }, messages: { listMessages, listRecentMessages: vi.fn().mockRejectedValue(new HistoryWindowTooLargeError()) }, projection });
    await recovery.recoverConversation("c1");
    expect(listMessages).toHaveBeenCalledTimes(1);
    expect(projection.getSnapshot().messages.huge).toBeDefined();
    expect(projection.getSnapshot().hydrationByConversation.c1.complete).toBe(true);
  });

  it("hydrates only a recent page, then merges older history without replacing newer content", async () => {
    const projection = new ProjectionEngine();
    const listMessages = vi.fn();
    const recent = vi.fn().mockResolvedValueOnce(page([row("last", "6000", "5")], true, "older-1"))
      .mockResolvedValueOnce(page([row("first", "3"), row("last", "6000", "1")], false));
    const recovery = new RecoveryCoordinator({ conversations: { getConversation: async () => conversation }, messages: { listMessages, listRecentMessages: recent }, projection });
    await recovery.recoverConversation("c1");
    expect(listMessages).not.toHaveBeenCalled();
    expect(recent).toHaveBeenCalledTimes(1);
    expect(projection.getSnapshot().hydrationByConversation.c1).toMatchObject({ complete: false, window: { hasOlder: true, nextOlderCursor: "older-1" } });
    await recovery.loadEarlier("c1");
    expect(projection.getSnapshot().messages.last.revision).toBe("5");
    expect(Object.keys(projection.getSnapshot().messages)).toHaveLength(2);
    expect(projection.getSnapshot().hydrationByConversation.c1.complete).toBe(true);
  });

  it("rejects an older page when clear replaces the history generation", async () => {
    const projection = new ProjectionEngine();
    let resolve!: (value: RecentMessagePage) => void;
    const recent = vi.fn().mockResolvedValueOnce(page([row("last", "6000")], true, "older"))
      .mockImplementationOnce(() => new Promise<RecentMessagePage>(r => { resolve = r; }));
    const recovery = new RecoveryCoordinator({ conversations: { getConversation: async () => conversation }, messages: { listMessages: vi.fn(), listRecentMessages: recent }, projection });
    await recovery.recoverConversation("c1");
    const older = recovery.loadEarlier("c1");
    await Promise.resolve();
    projection.commitHydration({ conversation: { ...conversation, historyGeneration: "3", revision: "4" }, messages: [], historyBoundaryOffset: "6000", latestOffset: "6000" });
    resolve(page([row("old", "3")], false));
    await expect(older).rejects.toThrow("stale");
    expect(projection.getSnapshot().messages).toEqual({});
  });

  it("keeps legacy full recovery when the server does not advertise tail", async () => {
    const projection = new ProjectionEngine();
    const recent = vi.fn();
    const listMessages = vi.fn().mockResolvedValue({ messages: [row("only", "3")], historyGeneration: "2", historyBoundaryOffset: "0", latestOffset: "3", hasMore: false });
    const recovery = new RecoveryCoordinator({ conversations: { getConversation: async () => ({ ...conversation, historyCapabilities: undefined }) }, messages: { listMessages, listRecentMessages: recent }, projection });
    await recovery.recoverConversation("c1");
    expect(recent).not.toHaveBeenCalled();
    expect(projection.getSnapshot().hydrationByConversation.c1.complete).toBe(true);
  });

  it("decodes summary false and large sparse offsets without constructing a cursor", async () => {
    const fetch = vi.fn(async (url: string | URL | Request) => new Response(JSON.stringify({ success: true, data: String(url).includes("/tail") ? {
      messages: [{ message_id: "m", publisher_id: "agent", type: "agent_reply", body: "hello", created_at: conversation.updatedAt, offset_str: "9007199254740993" }],
      history_generation: "2", history_boundary_offset: "0", latest_offset_str: "9007199254740993", snapshot_upper_offset: "9007199254740993", has_older: true, next_older_cursor: "opaque.signed",
    } : { conversation_id: "c1", agent_id: "a1", state: "active", history_generation: "2", metadata_version: "3", has_messages: false, history_capabilities: ["tail-v1"] } }), { status: 200 }));
    const composition = createGatewayMessageClientComposition({ gatewayUrl: "https://gateway.example", platform: "web", currentPrincipal: { currentPrincipalId: () => "user" }, fetch: fetch as typeof globalThis.fetch });
    composition.conversationRoutes!.bindConversation("c1", "a1");
    expect((await composition.conversationQuery.getConversation("c1")).hasMessages).toBe(false);
    const result = await composition.messageQuery.listRecentMessages!("c1");
    expect(result.messages[0].offset).toBe("9007199254740993");
    expect(result.nextOlderCursor).toBe("opaque.signed");
    expect(String(fetch.mock.calls[1][0])).toContain("/messages/tail?limit=30");
  });
});
