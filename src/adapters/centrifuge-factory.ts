import { Centrifuge } from "centrifuge";

import type { RealtimeConnectionState } from "../facade/contracts.js";

export interface CentrifugeFactoryRuntimeOptions {
  readonly websocket?: typeof globalThis.WebSocket;
}

/**
 * Shared hidden personal-inbox transport for Node, browser and React Native.
 * The server token binds the sole channel; this client exposes no subscribe or
 * publish API and therefore cannot guess a conversation transport channel.
 */
export function createCentrifugeFactory(runtime: CentrifugeFactoryRuntimeOptions = {}) {
  return {
    create(options: {
      url: string;
      token: string;
      onEvent(event: unknown): void;
      onState(state: RealtimeConnectionState): void;
      onRefresh(): Promise<void>;
      onError(error: unknown): void;
    }) {
      let token = options.token;
      let connected = false;
      let connectStarted = false;
      let firstSettled = false;
      let resolveFirst!: () => void;
      let rejectFirst!: (error: Error) => void;
      const firstConnected = new Promise<void>((resolve, reject) => {
        resolveFirst = resolve;
        rejectFirst = reject;
      });
      const resolveConnection = () => {
        if (firstSettled) return;
        firstSettled = true;
        resolveFirst();
      };
      const rejectConnection = (error: unknown) => {
        if (firstSettled) return;
        firstSettled = true;
        rejectFirst(asError(error));
      };
      const client = new Centrifuge(options.url, {
        token,
        getToken: async () => {
          await options.onRefresh();
          return token;
        },
        ...(runtime.websocket ? { websocket: runtime.websocket } : {}),
      });
      client.on("connecting", () => {
        options.onState(connected ? "reconnecting" : "connecting");
      });
      client.on("connected", () => {
        connected = true;
        options.onState("connected");
        resolveConnection();
      });
      client.on("disconnected", (ctx) => {
        options.onState("disconnected");
        if (!connected) {
          rejectConnection(new Error(`realtime disconnected before connected: ${ctx.reason}`));
        }
      });
      client.on("publication", (ctx) => options.onEvent(ctx.data));
      client.on("error", (ctx) => {
        if (!connected) {
          rejectConnection(ctx.error);
          options.onError(ctx.error);
        }
      });

      return {
        connect: () => {
          if (!connectStarted) {
            connectStarted = true;
            try {
              client.connect();
            } catch (error) {
              rejectConnection(error);
              options.onError(error);
            }
          }
          return firstConnected;
        },
        close: () => client.disconnect(),
        updateToken(next: string) {
          token = next;
          client.setToken(next);
        },
      };
    },
  };
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
