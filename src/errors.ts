// Error sentinels for `@beeos-ai/message-sdk`. Each maps onto a
// specific (HTTP status, server code) pair returned by the Message
// Service. SDK consumers should pattern-match via `err instanceof X`
// rather than parsing status / code strings themselves.

export class MessagingError extends Error {
  status: number;
  code?: string;
  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = "MessagingError";
    this.status = status;
    this.code = code;
  }
}

export class ConversationNotFoundError extends MessagingError {
  constructor(msg = "conversation not found") {
    super(msg, 404, "conversation_not_found");
    this.name = "ConversationNotFoundError";
  }
}

export class ConversationClosedError extends MessagingError {
  constructor(msg = "conversation closed") {
    super(msg, 410, "conversation_closed");
    this.name = "ConversationClosedError";
  }
}

export class NotMemberError extends MessagingError {
  constructor(msg = "not a conversation member") {
    super(msg, 403, "not_member");
    this.name = "NotMemberError";
  }
}

export class NoSubscriberError extends MessagingError {
  constructor(msg = "no active subscriber") {
    super(msg, 410, "no_subscriber");
    this.name = "NoSubscriberError";
  }
}

export class DuplicateIdError extends MessagingError {
  constructor(msg = "duplicate message id") {
    super(msg, 409, "duplicate_message_id");
    this.name = "DuplicateIdError";
  }
}

export class WaitTimeoutError extends MessagingError {
  constructor(msg = "wait timeout") {
    super(msg, 408, "wait_timeout");
    this.name = "WaitTimeoutError";
  }
}

/**
 * Maps an HTTP response into the most specific MessagingError subtype.
 * The server uses two error envelopes:
 *  - `{"error":{"code","message"}}` for structured errors
 *  - `{"code","message"}` for channel-primitives flat errors
 * We accept either.
 */
export function mapHttpError(status: number, body: unknown): MessagingError {
  let code: string | undefined;
  let message: string | undefined;
  if (body && typeof body === "object") {
    const b = body as {
      code?: string;
      message?: string;
      error?: { code?: string; message?: string };
    };
    code = b.code;
    message = b.message;
    if (!code && b.error) {
      code = b.error.code;
      message = b.error.message;
    }
  }
  const msg = message ?? `HTTP ${status}`;

  if (status === 404 && code === "conversation_not_found") return new ConversationNotFoundError(msg);
  if (status === 410 && code === "conversation_closed") return new ConversationClosedError(msg);
  if (status === 403 && code === "not_member") return new NotMemberError(msg);
  if (status === 410 && code === "no_subscriber") return new NoSubscriberError(msg);
  if (status === 409 && code === "duplicate_message_id") return new DuplicateIdError(msg);
  if (status === 408 && code === "wait_timeout") return new WaitTimeoutError(msg);

  return new MessagingError(msg, status, code);
}
