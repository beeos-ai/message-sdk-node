import { describe, expect, it } from "vitest";

import {
  A2A_PROTOCOL_FIELDS,
  extractChatPrompt,
} from "../src/chat-envelope.js";
import type { MessageEnvelope } from "../src/envelope.js";

const LOCAL_AGENT_ID = "agent-local-test";
const FIXED_SUFFIX = () => "fixed-suffix";

describe("extractChatPrompt — chat_message envelope", () => {
  it("returns null when no message is present", () => {
    const env: MessageEnvelope = { type: "chat_message", payload: { parts: [] } };
    expect(
      extractChatPrompt(env, {
        source: "chat_message",
        localAgentId: LOCAL_AGENT_ID,
        generateSuffix: FIXED_SUFFIX,
      }),
    ).toBeNull();
  });

  it("extracts message, channelId and contextId from a native envelope", () => {
    const env: MessageEnvelope = {
      type: "chat_message",
      channel_id: "ch-123",
      payload: {
        message: "hello",
        context_id: "ctx-42",
        parts: [
          { type: "text", text: "hello" },
          { type: "file", file_url: "https://cdn/a.png" },
        ],
      },
    };
    const out = extractChatPrompt(env, {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out).not.toBeNull();
    expect(out!.message).toBe("hello");
    expect(out!.files).toEqual(["https://cdn/a.png"]);
    expect(out!.channelId).toBe("ch-123");
    expect(out!.contextId).toBe("ctx-42");
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:ctx-ctx-42`);
  });

  it("generates a fresh session key when neither context_id nor channel_id is supplied", () => {
    const env: MessageEnvelope = {
      type: "chat_message",
      payload: { message: "hi" },
    };
    const out = extractChatPrompt(env, {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:fixed-suffix`);
    expect(out!.channelId).toBeUndefined();
  });

  it("uses ch-{channelId} suffix when only channel_id is present", () => {
    const env: MessageEnvelope = {
      type: "chat_message",
      channel_id: "ch-stable",
      payload: { message: "hi" },
    };
    const out = extractChatPrompt(env, {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.channelId).toBe("ch-stable");
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:ch-ch-stable`);
    expect(out!.sessionKey.includes("fixed-suffix")).toBe(false);
  });

  it("prefers ctx-{contextId} over ch-{channelId} when both are present", () => {
    const env: MessageEnvelope = {
      type: "chat_message",
      channel_id: "ch-stable",
      payload: { message: "hi", context_id: "ctx-pinned" },
    };
    const out = extractChatPrompt(env, {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:invoke:ctx-ctx-pinned`);
    expect(out!.channelId).toBe("ch-stable");
  });

  it("does NOT surface any A2A protocol header even if upstream leaks one", () => {
    const pollutedPayload: Record<string, unknown> = { message: "hello" };
    for (const field of A2A_PROTOCOL_FIELDS) {
      pollutedPayload[field] = `leaked:${field}`;
    }
    const env: MessageEnvelope = {
      type: "chat_message",
      payload: pollutedPayload,
      metadata: {
        task_id: "leaked-task",
        caller_protocol: "a2a",
        correlation_id: "leaked-corr",
      },
    };
    const out = extractChatPrompt(env, {
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

  it("legacy agent_request envelopes still parse, but default channel stays `a2a`", () => {
    const env: MessageEnvelope = {
      type: "agent_request",
      payload: {
        message: "legacy",
        files: ["/tmp/a.txt"],
      },
    };
    const out = extractChatPrompt(env, {
      source: "agent_request",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.message).toBe("legacy");
    expect(out!.files).toEqual(["/tmp/a.txt"]);
    expect(out!.sessionKey).toBe(`agent:${LOCAL_AGENT_ID}:a2a:fixed-suffix`);
  });

  it("respects an explicit session_key from legacy callers", () => {
    const env: MessageEnvelope = {
      type: "agent_request",
      payload: {
        message: "legacy",
        session_key: "agent:legacy:pre-existing",
      },
    };
    const out = extractChatPrompt(env, {
      source: "agent_request",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.sessionKey).toBe("agent:legacy:pre-existing");
  });

  it("surfaces envelope.message_id as the generic IM request/response correlator", () => {
    const env: MessageEnvelope = {
      type: "chat_message",
      channel_id: "ch-1",
      message_id: "msg-envelope",
      payload: { message: "hi", message_id: "msg-payload" },
      metadata: { message_id: "msg-meta" },
    };
    const out = extractChatPrompt(env, {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.messageId).toBe("msg-envelope");
  });

  it("falls back to metadata.message_id when the envelope field is absent", () => {
    const env: MessageEnvelope = {
      type: "chat_message",
      channel_id: "ch-1",
      payload: { message: "hi" },
      metadata: { message_id: "msg-meta" },
    };
    const out = extractChatPrompt(env, {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.messageId).toBe("msg-meta");
  });

  it("normalizes empty string message_id to undefined", () => {
    const env: MessageEnvelope = {
      type: "chat_message",
      channel_id: "ch-1",
      message_id: "",
      payload: { message: "hi" },
    };
    const out = extractChatPrompt(env, {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.messageId).toBeUndefined();
  });

  it("prefers envelope.channel_id over metadata over payload", () => {
    const env: MessageEnvelope = {
      type: "chat_message",
      channel_id: "envelope-ch",
      payload: { message: "m", channel_id: "payload-ch" },
      metadata: { channel_id: "meta-ch" },
    };
    const out = extractChatPrompt(env, {
      source: "chat_message",
      localAgentId: LOCAL_AGENT_ID,
      generateSuffix: FIXED_SUFFIX,
    });
    expect(out!.channelId).toBe("envelope-ch");
  });
});
