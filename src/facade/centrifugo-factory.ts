import type {
  CentrifugeClient,
  CentrifugeClientFactory,
  CentrifugeClientOptions,
} from "./centrifugo-realtime.js";

type CentrifugeEvent = "connecting" | "connected" | "disconnected" | "publication" | "subscribed" | "error";

/** Structural subset of centrifuge-js. It deliberately omits subscriptions and publish. */
export interface CentrifugeConnection {
  on(event: CentrifugeEvent, listener: (context: any) => void): unknown;
  connect(): void;
  disconnect(): void;
  setToken(token: string): void;
}

export type CentrifugeConstructor = new (
  endpoint: string,
  options: {
    token: string;
    getToken: () => Promise<string>;
    websocket: unknown;
  },
) => CentrifugeConnection;

/**
 * Binds centrifuge-js to the SDK's intentionally narrow internal port.
 *
 * Passing a single WSS string (never an endpoint array) and an explicit native
 * WebSocket constructor prevents Centrifuge's optional emulation/fallback
 * transports from entering this product path.
 */
export function createSingleWssCentrifugeFactory(
  Centrifuge: CentrifugeConstructor,
  websocket: unknown,
): CentrifugeClientFactory {
  if (!websocket) throw new Error("message-sdk realtime requires a native WebSocket constructor");
  return {
    create(options: CentrifugeClientOptions): CentrifugeClient {
      let token = options.token;
      let everConnected = false;
      let closed = false;
      const client = new Centrifuge(options.url, {
        token,
        websocket,
        getToken: async () => {
          // Centrifuge calls this only when it needs a renewed connection
          // token. The SDK transport fetches it from Message Service and
          // updates this same physical WSS client before we return it.
          await options.onRefreshRequired();
          return token;
        },
      });

      client.on("connecting", () => {
        if (!closed) options.onState(everConnected ? "reconnecting" : "connecting");
      });
      client.on("connected", () => {
        if (closed) return;
        everConnected = true;
        options.onState("connected");
      });
      client.on("disconnected", () => {
        if (!closed) options.onState("disconnected");
      });
      client.on("publication", (context) => {
        if (!closed) options.onPublication(context?.data);
      });
      client.on("subscribed", (context) => {
        if (closed) return;
        // Centrifuge exposes subscribed recovery metadata here. Reduce it to
        // booleans so no channel, stream, token, or audience data crosses the
        // SDK boundary.
        options.onRecovery?.({
          recoverable: context?.recoverable === true,
          recovered: context?.recovered === true,
          positioned: context?.positioned === true || context?.streamPosition !== undefined,
        });
      });
      client.on("error", () => {
        // centrifuge-js reports retryable transport/token errors before its
        // own reconnect state transition. Do not turn a transient error into
        // a false terminal failure; `disconnected` is the terminal signal.
      });

      return {
        connect: () => client.connect(),
        close: () => {
          closed = true;
          client.disconnect();
        },
        updateToken: (nextToken) => {
          token = nextToken;
          client.setToken(nextToken);
        },
      };
    },
  };
}
