import { describe, expect, it } from "vitest";

import { envelopeToMessage, type WireEnvelope } from "../src/wire.js";

describe("envelopeToMessage", () => {
  it("translates v1 wire envelope to v2 Message shape", () => {
    const env: WireEnvelope = {
      type: "chat_message",
      payload: { message: "hi" },
      channel_id: "conv-1",
      message_id: "msg-7",
      in_reply_to: "msg-prev",
      publisher_id: "agent:alice",
      created_at: "2026-05-17T08:00:00Z",
    };
    expect(envelopeToMessage(env)).toEqual({
      id: "msg-7",
      conversationId: "conv-1",
      type: "chat_message",
      content: { message: "hi" },
      sender: "agent:alice",
      replyTo: "msg-prev",
      createdAt: "2026-05-17T08:00:00Z",
    });
  });

  it("falls back to metadata.from for sender when publisher_id is absent", () => {
    const env: WireEnvelope = {
      type: "chat_message",
      payload: null,
      metadata: { from: "agent:bob" },
    };
    const msg = envelopeToMessage(env);
    expect(msg.sender).toBe("agent:bob");
  });

  it("normalises missing optional fields to empty/undefined", () => {
    const env: WireEnvelope = {
      type: "x",
      payload: 0,
    };
    const msg = envelopeToMessage(env);
    expect(msg.id).toBe("");
    expect(msg.conversationId).toBe("");
    expect(msg.sender).toBe("");
    expect(msg.createdAt).toBe("");
    expect(msg.replyTo).toBeUndefined();
    expect(msg.content).toBe(0);
  });
});
