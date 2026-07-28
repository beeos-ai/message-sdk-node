/** Browser runtime entrypoint; its MessageClient API is identical on every platform. */
export { createMessageClient } from "./unified-client.js";
export type { MessageClient } from "./unified-client.js";
export type * from "./facade/contracts.js";
