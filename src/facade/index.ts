export { ProjectionEngine, conversationFromRealtime, messageFromRealtime } from "./projection.js";
export { HistoryGenerationChangedError, RecoveryCoordinator } from "./recovery-coordinator.js";
export { ConversationWatchRegistry } from "./watch-registry.js";
export { createMessageClient } from "../unified-client.js";
export type * from "./contracts.js";
export type { ConversationHydrationCommit } from "./projection.js";
export type { RecoveryCoordinatorOptions } from "./recovery-coordinator.js";
export type { ConversationWatchRegistryOptions } from "./watch-registry.js";
