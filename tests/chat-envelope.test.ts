import { describe, expect, it } from "vitest";

import {
  A2A_PROTOCOL_FIELDS,
  extractChatPrompt,
} from "../src/chat-envelope.js";
import type { Message } from "../src/types.js";

const LOCAL_AGENT_ID = "agent-local-test";
const FIXED_SUFFIX = () => "fixed-suffix";

function chat(content: Record<string, unknown>, overrides: Partial<Message> = {}): Message {
  return {
    id: "msg-id",
    conversationId: "",
    type: "chat_message",
    sender: "",
    createdAt: "",
    content,
    ...overrides,
  };
}

describe("extractChatPrompt — chat_message", () => {
  it("returns null when no message is present", () => {
    expect(
      extractChatPrompt(chat({ parts: [] }), {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      }),
    ).toBeNull();
  });

  it("extracts message, channelId and contextId from a native message", () => {
    const out = extractChatPrompt(
      chat(
        {
          message: "hello",
          context_id: "ctx-42",
          parts: [
            { type: "text", text: "hello" },
            { type: "file", file_url: "https://cdn/a.png" },
          ],
        },
        { conversationId: "ch-123" },
      ),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out).not.toBeNull();
    expect(out!.message).toBe("hello");
    expect(out!.files).toEqual(["https://cdn/a.png"]);
    expect(out!.channelId).toBe("ch-123");
    expect(out!.contextId).toBe("ctx-42");
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:ctx-ctx-42`);
  });

  it("generates a fresh session key when neither context_id nor channelId is supplied", () => {
    const out = extractChatPrompt(chat({ message: "hi" }), {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:fixed-suffix`);
    expect(out!.channelId).toBeUndefined();
  });

  it("uses ch-{channelId} suffix when only conversationId is present", () => {
    const out = extractChatPrompt(
      chat({ message: "hi" }, { conversationId: "ch-stable" }),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.channelId).toBe("ch-stable");
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:ch-ch-stable`);
    expect(out!.sessionKey.includes("fixed-suffix")).toBe(false);
  });

  it("prefers ctx-{contextId} over ch-{channelId} when both are present", () => {
    const out = extractChatPrompt(
      chat(
        { message: "hi", context_id: "ctx-pinned" },
        { conversationId: "ch-stable" },
      ),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:ctx-ctx-pinned`);
    expect(out!.channelId).toBe("ch-stable");
  });

  it("does NOT surface any A2A protocol header even if upstream leaks one", () => {
    const polluted: Record<string, unknown> = { message: "hello" };
    for (const field of A2A_PROTOCOL_FIELDS) {
      polluted[field] = `leaked:${field}`;
    }
    const out = extractChatPrompt(chat(polluted), {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out).not.toBeNull();
    const allowedKeys = new Set([
      "message",
      "files",
      "channelId",
      "contextId",
      "sessionKey",
      "messageId",
    ]);
    for (const key of Object.keys(out!)) {
      expect(allowedKeys.has(key)).toBe(true);
    }
    for (const field of A2A_PROTOCOL_FIELDS) {
      expect(out!.sessionKey.includes(`leaked:${field}`)).toBe(false);
      expect(out!.message.includes(`leaked:${field}`)).toBe(false);
    }
  });

  it("legacy agent_request messages still parse, but default channel stays `a2a`", () => {
    const out = extractChatPrompt(
      chat(
        { message: "legacy", files: ["/tmp/a.txt"] },
        { type: "agent_request" },
      ),
      {
        source: "agent_request",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.message).toBe("legacy");
    expect(out!.files).toEqual(["/tmp/a.txt"]);
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:a2a:fixed-suffix`);
  });

  it("respects an explicit session_key from legacy callers", () => {
    const out = extractChatPrompt(
      chat(
        { message: "legacy", session_key: "agent:legacy:pre-existing" },
        { type: "agent_request" },
      ),
      {
        source: "agent_request",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.sessionKey).toBe("agent:legacy:pre-existing");
  });

  it("uses message.id as the generic IM request/response correlator", () => {
    const out = extractChatPrompt(
      chat({ message: "hi" }, { id: "msg-envelope", conversationId: "ch-1" }),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.messageId).toBe("msg-envelope");
  });

  it("normalises empty string id to undefined", () => {
    const out = extractChatPrompt(
      chat({ message: "hi" }, { id: "", conversationId: "ch-1" }),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.messageId).toBeUndefined();
  });

  it("prefers message.conversationId over content.channel_id", () => {
    const out = extractChatPrompt(
      chat(
        { message: "m", channel_id: "payload-ch" },
        { conversationId: "envelope-ch" },
      ),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.channelId).toBe("envelope-ch");
  });
});

// PR-FIX-G: server-side producers stamp the wire protocol on
// `content.metadata.protocol` instead of embedding the platform agent
// UUID into content.session_key. The SDK reads it as an opaque string
// and composes `agent:{localAgentId}:{protocol}:{discriminator}` using
// its own localAgentId. These tests pin that contract.
describe("computeSessionKey — content.metadata.protocol", () => {
  it("uses content.metadata.protocol=a2a as the session tag with channelId", () => {
    const out = extractChatPrompt(
      chat(
        { message: "hi", metadata: { protocol: "a2a" } },
        { conversationId: "ch-abc" },
      ),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:a2a:ch-ch-abc`);
  });

  it("uses content.metadata.protocol=mcp with contextId precedence", () => {
    const out = extractChatPrompt(
      chat(
        {
          message: "hi",
          context_id: "conv-99",
          metadata: { protocol: "mcp" },
        },
        { conversationId: "ch-abc" },
      ),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:mcp:ctx-conv-99`);
  });

  it("uses content.metadata.protocol=openapi with random suffix fallback", () => {
    const out = extractChatPrompt(
      chat({ message: "hi", metadata: { protocol: "openapi" } }),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:openapi:fixed-suffix`);
  });

  it("falls back to legacy :invoke: prefix when metadata.protocol is absent", () => {
    const out = extractChatPrompt(
      chat(
        { message: "hi", context_id: "conv-99" },
        { conversationId: "ch-abc" },
      ),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:ctx-conv-99`);
  });

  it("legacy agent_request without metadata.protocol falls back to :a2a:{generateSuffix}", () => {
    const out = extractChatPrompt(
      chat(
        { message: "hi", context_id: "ctx-99" },
        { type: "agent_request", conversationId: "ch-abc" },
      ),
      {
        source: "agent_request",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:a2a:fixed-suffix`);
  });

  it("explicit content.session_key still wins over metadata.protocol", () => {
    const out = extractChatPrompt(
      chat(
        {
          message: "hi",
          session_key: "agent:pinned-foo:custom:bar",
          metadata: { protocol: "a2a" },
        },
        { conversationId: "ch-abc" },
      ),
      {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      },
    );
    expect(out!.sessionKey).toBe("agent:pinned-foo:custom:bar");
  });
});
