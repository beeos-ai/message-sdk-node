/** Browser entrypoint. WebSocket, fetch and IndexedDB are injected by the host. */
export { createMessageClient, MessageClientFacade } from "./facade/index.js";
export type * from "./facade/index.js";
export * from "./protocol/index.js";
