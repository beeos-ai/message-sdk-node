/** Node runtime entrypoint; its MessageClient API is identical on every platform. */
export { createMessageClient } from "./unified-client.js";
export type { MessageClient } from "./unified-client.js";
export type * from "./facade/contracts.js";
export { createNodeMessageClientComposition } from "./node-runtime.js";
export type {
  NodeMessageClientComposition,
  NodeMessageClientOptions,
  NodeMessageServiceTransport,
} from "./node-runtime.js";
export type {
  RuntimeDeliveryAuthorityLease,
  RuntimeDeliveryAuthorityPort,
} from "./runtime-delivery.js";
