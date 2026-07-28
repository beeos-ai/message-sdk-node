import { describe, expect, it } from "vitest";

import { OutcomeUnknownError } from "../src/errors.js";
import {
  StreamTerminatedError,
  UnifiedMessageStream,
} from "../src/message-stream.js";
import type {
  MessageStreamWriter,
  SendMessageCommand,
  SendMessageReceipt,
} from "../src/facade/contracts.js";

const command: SendMessageCommand = {
  conversationId: "c1",
  clientMessageId: "stream-key",
  idempotencyKey: "stream-key",
  type: "agent_reply",
  content: {},
};

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((ok, fail) => { resolve = ok; reject = fail; });
  return { promise, resolve, reject };
}

describe("UnifiedMessageStream", () => {
  it("returns synchronously, queues pre-open appends, serializes and terminates", async () => {
    const opened = deferred<SendMessageReceipt>();
    const calls: string[] = [];
    const writer: MessageStreamWriter = {
      startStream: () => { calls.push("open"); return opened.promise; },
      append: async (cid, _id, body, from, key) => {
        calls.push(`append:${cid}:${body}:${from}:${key}`);
      },
      setBody: async (cid, _id, body, key) => { calls.push(`body:${cid}:${body}:${key}`); },
      setParts: async (cid, _id, parts, key) => { calls.push(`parts:${cid}:${parts.length}:${key}`); },
      finalize: async (cid, _id, state, key, reason) => {
        calls.push(`terminal:${cid}:${state}:${reason}:${key}`);
      },
    };
    const stream = new UnifiedMessageStream(writer, command);
    expect(stream.id).toBe("stream-key");
    stream.appendBody("a");
    stream.appendBody("b");
    stream.addPart({ type: "text", value: "p" });
    expect(calls).toEqual(["open"]);

    opened.resolve({ messageId: "stream-key", outcome: "created" });
    await stream.opened();
    await stream.finalize({ stopReason: "end_turn" });
    expect(stream.id).toBe("stream-key");
    expect(stream.envelope).toEqual({ messageId: "stream-key", outcome: "created" });
    expect(calls).toEqual([
      "open",
      "append:c1:a:0:stream-key:append:1",
      "append:c1:b:1:stream-key:append:2",
      "parts:c1:1:stream-key:parts:3",
      "terminal:c1:completed:end_turn:stream-key:terminal:completed:4",
    ]);
    expect(stream.isTerminated).toBe(true);
    expect(() => stream.appendBody("late")).toThrow(StreamTerminatedError);
  });

  it("supports body snapshots, part replacement, fail/refuse/cancel helpers", async () => {
    const terminalStates: string[] = [];
    const writer: MessageStreamWriter = {
      async startStream(input) { return { messageId: input.clientMessageId, outcome: "created" }; },
      async append() {},
      async setBody(cid, _id, body) { expect(cid).toBe("c1"); expect(body).toBe("snapshot"); },
      async setParts(cid, _id, parts) { expect(cid).toBe("c1"); expect(parts).toHaveLength(1); },
      async finalize(cid, _id, state, _key, reason) {
        expect(cid).toBe("c1");
        terminalStates.push(`${state}:${reason}`);
      },
    };
    const stream = new UnifiedMessageStream(writer, command);
    stream.setBody("snapshot");
    stream.addPart({ type: "tool_use", value: "a" });
    stream.replacePart(0, { type: "tool_result", value: "b" });
    await stream.fail();
    expect(terminalStates).toEqual(["failed:error"]);
    expect(stream.parts).toEqual([{ type: "tool_result", value: "b" }]);

    const refused = new UnifiedMessageStream(writer, command);
    await refused.refuse();
    const cancelled = new UnifiedMessageStream(writer, command);
    await cancelled.cancel();
    expect(terminalStates).toEqual(["failed:error", "refused:refused", "cancelled:user_stop"]);
  });

  it("surfaces unknown open outcome without a new id or automatic replay", async () => {
    let opens = 0;
    const unknown = new OutcomeUnknownError({
      phase: "open",
      conversationId: "c1",
      messageId: "m1",
      idempotencyKey: "stream-key",
      cause: new Error("reset"),
    });
    const writer: MessageStreamWriter = {
      async startStream() { opens++; throw unknown; },
      async append() { throw new Error("must not append"); },
      async finalize() { throw new Error("must not finalize"); },
    };
    const stream = new UnifiedMessageStream(writer, command);
    stream.appendBody("queued");
    await expect(stream.opened()).rejects.toBe(unknown);
    await expect(stream.finalize()).rejects.toBe(unknown);
    expect(opens).toBe(1);
    expect(stream.id).toBe("stream-key");
  });

  it("does not retry a failed append before terminal", async () => {
    let appends = 0;
    const writer: MessageStreamWriter = {
      async startStream() { return { messageId: "stream-key", outcome: "created" }; },
      async append() { appends++; throw new Error("append outcome unknown"); },
      async finalize() { throw new Error("must not finalize after append failure"); },
    };
    const stream = new UnifiedMessageStream(writer, command);
    stream.appendBody("x");
    await expect(stream.finalize()).rejects.toThrow("append outcome unknown");
    expect(appends).toBe(1);
  });
});
