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
  ConversationPage,
  CreateConversationInput,
  IdentitySendInput,
  ListConversationsOptions,
  ListOptions,
  Logger,
  Message,
  MessagePage,
  Presence,
  SendInput,
  TokenProvider,
  TokenResponse,
  WaitInput,
} from "./types.js";

export {
  ConversationClosedError,
  ConversationNotFoundError,
  DuplicateIdError,
  MessagingError,
  NoSubscriberError,
  NotMemberError,
  WaitTimeoutError,
} from "./errors.js";
