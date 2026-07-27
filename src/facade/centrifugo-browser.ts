import { Centrifuge } from "centrifuge";

import { createSingleWssCentrifugeFactory } from "./centrifugo-factory.js";
import {
  createCentrifugoRealtimeTransport,
  type CentrifugeClientFactory,
  type CentrifugoRealtimeTransport,
  type CentrifugoRealtimeTransportOptions,
} from "./centrifugo-realtime.js";

/** Browser factory. It uses the platform's native WebSocket only. */
export function createWebCentrifugeClientFactory(): CentrifugeClientFactory {
  return createSingleWssCentrifugeFactory(
    Centrifuge as never,
    globalThis.WebSocket,
  );
}

/** React Native exposes the same native WebSocket contract as the browser. */
export function createReactNativeCentrifugeClientFactory(): CentrifugeClientFactory {
  return createSingleWssCentrifugeFactory(
    Centrifuge as never,
    globalThis.WebSocket,
  );
}

/** Web convenience constructor: hosts never import or construct Centrifuge. */
export function createWebCentrifugoRealtimeTransport(
  options: Omit<CentrifugoRealtimeTransportOptions, "centrifuge">,
): CentrifugoRealtimeTransport {
  return createCentrifugoRealtimeTransport({
    ...options,
    centrifuge: createWebCentrifugeClientFactory(),
  });
}

/** React Native convenience constructor using its global native WebSocket. */
export function createReactNativeCentrifugoRealtimeTransport(
  options: Omit<CentrifugoRealtimeTransportOptions, "centrifuge">,
): CentrifugoRealtimeTransport {
  return createCentrifugoRealtimeTransport({
    ...options,
    centrifuge: createReactNativeCentrifugeClientFactory(),
  });
}
