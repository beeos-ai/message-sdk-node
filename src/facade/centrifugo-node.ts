import { Centrifuge } from "centrifuge";
import WebSocket from "ws";

import { createSingleWssCentrifugeFactory } from "./centrifugo-factory.js";
import {
  createCentrifugoRealtimeTransport,
  type CentrifugeClientFactory,
  type CentrifugoRealtimeTransport,
  type CentrifugoRealtimeTransportOptions,
} from "./centrifugo-realtime.js";

/** Node/beeos-claw factory. `ws` is bundled by this SDK, never host-provided. */
export function createNodeCentrifugeClientFactory(): CentrifugeClientFactory {
  return createSingleWssCentrifugeFactory(
    Centrifuge as never,
    WebSocket as unknown as typeof globalThis.WebSocket,
  );
}

/** Node/beeos-claw convenience constructor using this package's bundled ws. */
export function createNodeCentrifugoRealtimeTransport(
  options: Omit<CentrifugoRealtimeTransportOptions, "centrifuge">,
): CentrifugoRealtimeTransport {
  return createCentrifugoRealtimeTransport({
    ...options,
    centrifuge: createNodeCentrifugeClientFactory(),
  });
}
