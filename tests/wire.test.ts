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
    // v3 fields default to undefined on a pre-v3 envelope.
    expect(msg.body).toBeUndefined();
    expect(msg.parts).toBeUndefined();
    expect(msg.state).toBeUndefined();
    expect(msg.stopReason).toBeUndefined();
    expect(msg.updatedAt).toBeUndefined();
  });

  it("surfaces v3 envelope fields end-to-end (body / parts / state / stop_reason / updated_at)", () => {
    const env: WireEnvelope = {
      type: "agent_reply",
      payload: { text: "hello world" },
      channel_id: "conv-v3",
      message_id: "msg-v3-1",
      in_reply_to: "user-1",
      publisher_id: "agent:beeos-claw",
      created_at: "2026-05-21T05:00:00Z",
      updated_at: "2026-05-21T05:00:01Z",
      event: "message.updated",
      body: "hello world",
      parts: [
        { type: "thinking", text: "deliberate ...", state: "done" },
      ],
      state: "completed",
      stop_reason: "end_turn",
    };
    const msg = envelopeToMessage(env);
    expect(msg.body).toBe("hello world");
    expect(msg.parts).toHaveLength(1);
    expect(msg.parts?.[0]).toMatchObject({
      type: "thinking",
      text: "deliberate ...",
    });
    expect(msg.state).toBe("completed");
    expect(msg.stopReason).toBe("end_turn");
    expect(msg.updatedAt).toBe("2026-05-21T05:00:01Z");
  });

  it("keeps streaming envelopes non-terminal (state='streaming')", () => {
    const env: WireEnvelope = {
      type: "agent_reply",
      payload: { text: "partial" },
      channel_id: "conv-v3",
      message_id: "msg-v3-2",
      state: "streaming",
      body: "partial",
    };
    const msg = envelopeToMessage(env);
    expect(msg.state).toBe("streaming");
    expect(msg.stopReason).toBeUndefined();
  });
});
