// `@beeos-ai/message-sdk` — single unified BeeOS Message Service SDK
// for Node.js.
//
// Root export: `MessageClient` + supporting types and error sentinels.
// Subpath exports for agent-side helpers:
//
//   import { MessageClient } from "@beeos-ai/message-sdk";
//   import {
//     extractChatPrompt,
//     A2A_PROTOCOL_FIELDS,
//   } from "@beeos-ai/message-sdk/chat-envelope";
//   import {
//     buildAgentAuthHeaders,
//     createTokenProvider,
//   } from "@beeos-ai/message-sdk/auth";

export {
  MessageClient,
  ConversationsAPI,
  MessagesAPI,
  IdentitiesAPI,
  ConversationSubscription,
} from "./client.js";

export type {
  ConnectOptions,
  ConversationSubscriptionEvents,
  MessageClientEvents,
  MessageClientOptions,
  RequestOptions,
} from "./client.js";

export type {
  Conversation,
  ConversationFocus,
  ConversationPage,
  CreateConversationInput,
  GetConversationFocusInput,
  IdentitySendInput,
  ListConversationsOptions,
  ListOptions,
  Logger,
  Message,
  MessageEnvelope,
  MessagePage,
  MessageState,
  MessageStreamOptions,
  Part,
  PartState,
  Presence,
  PutConversationFocusInput,
  SendInput,
  StartStreamInput,
  StopReason,
  TokenProvider,
  TokenResponse,
  WaitInput,
} from "./types.js";

export { MessageStream, StreamTerminatedError } from "./stream.js";

export {
  MessageStreamReader,
  defaultStreamDialer,
  iterLines,
} from "./sse-stream.js";

export type {
  BackfillTruncatedEvent,
  MessageStreamReaderEvents,
  ReplayCompleteEvent,
  StreamDialer,
  StreamOptions,
  StreamResponse,
} from "./sse-stream.js";

export {
  applyWireFrame,
  appendedText,
  emptySnapshot,
  snapshotBody,
  snapshotFromBody,
  MEDIA_TYPE_DELTA_WIRE,
  WIRE_EVENT_CREATED,
  WIRE_EVENT_UPDATED,
  WIRE_EVENT_DELTA,
} from "./reducer.js";

export type { ApplyResult, ReducedSnapshot, WireFrame } from "./reducer.js";

export {
  ConversationClosedError,
  ConversationNotFoundError,
  DuplicateIdError,
  MessagingError,
  NoSubscriberError,
  NotMemberError,
  WaitTimeoutError,
} from "./errors.js";
