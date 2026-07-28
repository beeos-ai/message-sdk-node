import type { JsonValue } from "./protocol/index.js";
import type {
  MessageStreamWriter,
  SendMessageCommand,
  SendMessageReceipt,
} from "./facade/contracts.js";

export class StreamTerminatedError extends Error {
  constructor() {
    super("message stream is already terminated");
    this.name = "StreamTerminatedError";
  }
}

/**
 * High-level stream handle backed by the same MessageClient's narrow writer.
 * It serializes writes, derives deterministic operation keys from the
 * caller-owned base key, and never retries an uncertain write.
 */
export class UnifiedMessageStream {
  private messageId: string;
  private receipt?: SendMessageReceipt;
  private body = "";
  private streamParts: JsonValue[] = [];
  private terminated = false;
  private sequence = 0;
  private chain: Promise<void>;
  private readonly openPromise: Promise<SendMessageReceipt>;

  constructor(
    private readonly writer: MessageStreamWriter,
    private readonly command: SendMessageCommand,
  ) {
    this.messageId = command.clientMessageId;
    this.openPromise = writer.startStream(command).then((receipt) => {
      if (receipt.messageId !== command.clientMessageId) {
        throw new Error("message service must use clientMessageId/idempotencyKey as messageId");
      }
      this.messageId = receipt.messageId;
      this.receipt = receipt;
      return receipt;
    });
    // Absorb only for queue construction. opened()/terminal still surface it.
    this.chain = this.openPromise.then(() => undefined, () => undefined);
  }

  get id(): string {
    return this.messageId;
  }

  get envelope(): SendMessageReceipt | undefined {
    return this.receipt;
  }

  get isTerminated(): boolean {
    return this.terminated;
  }

  get parts(): readonly JsonValue[] {
    return this.streamParts;
  }

  opened(): Promise<SendMessageReceipt> {
    return this.openPromise;
  }

  appendBody(chunk: string): void {
    this.assertActive();
    if (!chunk) return;
    const bodyFrom = new TextEncoder().encode(this.body).byteLength;
    this.body += chunk;
    const key = this.nextKey("append");
    this.enqueue(() => this.writer.append(
      this.command.conversationId,
      this.messageId,
      chunk,
      bodyFrom,
      key,
    ));
  }

  setBody(body: string): void {
    this.assertActive();
    if (body === this.body) return;
    if (!this.writer.setBody) throw new Error("message stream writer does not support body snapshots");
    this.body = body;
    const key = this.nextKey("body");
    this.enqueue(() => this.writer.setBody!(this.command.conversationId, this.messageId, body, key));
  }

  addPart(part: JsonValue): void {
    this.assertActive();
    this.streamParts = [...this.streamParts, part];
    this.flushParts();
  }

  replacePart(index: number, part: JsonValue): void {
    this.assertActive();
    if (!Number.isInteger(index) || index < 0 || index >= this.streamParts.length) {
      throw new Error("message stream part index is out of range");
    }
    const parts = [...this.streamParts];
    parts[index] = part;
    this.streamParts = parts;
    this.flushParts();
  }

  appendThinking(text: string, state: "streaming" | "done" = "done"): void {
    this.addPart({ type: "thinking", text, state });
  }

  appendToolUse(value: JsonValue): void;
  appendToolUse(
    id: string,
    name: string,
    args: JsonValue,
    state?: "streaming" | "done",
  ): void;
  appendToolUse(
    valueOrId: JsonValue,
    name?: string,
    args?: JsonValue,
    state: "streaming" | "done" = "streaming",
  ): void {
    if (typeof valueOrId === "string" && name !== undefined) {
      this.addPart({
        type: "tool_use",
        id: valueOrId,
        name,
        arguments: args ?? null,
        state,
      });
      return;
    }
    this.addPart({ type: "tool_use", value: valueOrId });
  }

  appendToolResult(value: JsonValue): void;
  appendToolResult(
    toolUseId: string,
    content: JsonValue,
    isError?: boolean,
    state?: "streaming" | "done",
  ): void;
  appendToolResult(
    valueOrId: JsonValue,
    content?: JsonValue,
    isError = false,
    state?: "streaming" | "done",
  ): void {
    if (typeof valueOrId === "string" && content !== undefined) {
      this.addPart({
        type: "tool_result",
        tool_use_id: valueOrId,
        content,
        is_error: isError,
        ...(state ? { state } : {}),
      });
      return;
    }
    this.addPart({ type: "tool_result", value: valueOrId });
  }

  finalize(options?: { stopReason?: string }): Promise<SendMessageReceipt> {
    return this.terminate("completed", options?.stopReason);
  }

  fail(options?: { body?: string; stopReason?: string }): Promise<SendMessageReceipt> {
    if (options?.body !== undefined) this.setBody(options.body);
    return this.terminate("failed", options?.stopReason ?? "error");
  }

  refuse(options?: { stopReason?: string }): Promise<SendMessageReceipt> {
    return this.terminate("refused", options?.stopReason ?? "refused");
  }

  cancel(options?: { stopReason?: string }): Promise<SendMessageReceipt> {
    return this.terminate("cancelled", options?.stopReason ?? "user_stop");
  }

  private flushParts(): void {
    if (!this.writer.setParts) throw new Error("message stream writer does not support parts");
    const parts = [...this.streamParts];
    const key = this.nextKey("parts");
    this.enqueue(() => this.writer.setParts!(this.command.conversationId, this.messageId, parts, key));
  }

  private enqueue(write: () => Promise<void>): void {
    this.chain = this.chain.then(async () => {
      await this.openPromise;
      await write();
    });
    // Do not create a retry or replacement stream. A later terminal/opened
    // await observes the exact failure on this chain.
    void this.chain.catch(() => undefined);
  }

  private async terminate(
    state: "completed" | "failed" | "refused" | "cancelled",
    stopReason?: string,
  ): Promise<SendMessageReceipt> {
    this.assertActive();
    this.terminated = true;
    await this.openPromise;
    await this.chain;
    await this.writer.finalize(
      this.command.conversationId,
      this.messageId,
      state,
      this.nextKey(`terminal:${state}`),
      stopReason,
    );
    return this.receipt!;
  }

  private nextKey(operation: string): string {
    this.sequence++;
    return `${this.command.idempotencyKey}:${operation}:${this.sequence}`;
  }

  private assertActive(): void {
    if (this.terminated) throw new StreamTerminatedError();
  }
}
