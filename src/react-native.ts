/** React Native entrypoint. AppState, NetInfo and SQLite are injected by the host. */
export { createMessageClient, MessageClientFacade } from "./facade/index.js";
export type * from "./facade/index.js";
export * from "./protocol/index.js";
