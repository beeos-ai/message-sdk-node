export {
  MessageSDK,
} from "./sdk.js";

export type {
  ChannelMessage,
  ConnectOptions,
  ListChannelMessagesResponse,
  MessageEnvelope,
  MessageHandler,
  MessageSDKConfig,
  MessageSDKLogger,
  SendChannelMessageInput,
  SendChannelMessageResponse,
  TokenProvider,
  TokenResponse,
  WaitForReplyInput,
} from "./envelope.js";

export {
  buildAgentAuthHeaders,
  buildSigningString,
  createMessagingTokenProvider,
} from "./auth.js";

export type {
  CreateMessagingTokenProviderOptions,
  MessagingIdentity,
} from "./auth.js";

export {
  A2A_PROTOCOL_FIELDS,
  extractChatPrompt,
} from "./chat-envelope.js";

export type {
  ChatPrompt,
  ExtractChatPromptOptions,
} from "./chat-envelope.js";
