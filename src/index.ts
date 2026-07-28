/** Unified public entrypoint. Runtime-specific transports stay in composition roots. */
export { createMessageClient } from "./unified-client.js";
export type { MessageClient } from "./unified-client.js";
