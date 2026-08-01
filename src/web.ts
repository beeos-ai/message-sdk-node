/** Browser runtime entrypoint; its MessageClient API is identical on every platform. */
export { createMessageClient } from "./unified-client.js";
export type { MessageClient } from "./unified-client.js";
export type * from "./facade/contracts.js";
/**
 * Type-only: the private Gateway composition itself
 * (`createGatewayMessageClientComposition`) is never exported here or from
 * any other package entrypoint. Pass this shape straight to
 * `createMessageClient({ gatewayUrl, platform: "web", ... })`.
 */
export type { GatewayMessageClientOptions } from "./gateway-runtime.js";
