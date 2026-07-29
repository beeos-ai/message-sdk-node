import { Centrifuge } from "centrifuge";

import type { RealtimeConnectionState } from "../facade/contracts.js";
import type { RealtimeDeliveryAudience } from "../protocol/index.js";

export interface CentrifugeFactoryRuntimeOptions {
  readonly websocket?: typeof globalThis.WebSocket;
}

/**
 * Shared hidden Centrifuge factory for Node, browser and React Native
 * composition roots. It never exposes raw subscribe or publish to features.
 */
export function createCentrifugeFactory(runtime: CentrifugeFactoryRuntimeOptions = {}) {
  return {
    create(options: {
      url: string;
      token: string;
      onEvent(event: unknown, audience: RealtimeDeliveryAudience): void;
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
      const subscriptions = new Map<string, SubscriptionState>();
      const resetSubscriptionBarriers = () => {
        for (const state of subscriptions.values()) state.reset();
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
        if (connected) resetSubscriptionBarriers();
        options.onState(connected ? "reconnecting" : "connecting");
      });
      client.on("connected", () => {
        connected = true;
        options.onState("connected");
        resolveConnection();
      });
      client.on("disconnected", (ctx) => {
        if (connected) resetSubscriptionBarriers();
        options.onState("disconnected");
        if (!connected) {
          rejectConnection(new Error(`realtime disconnected before connected: ${ctx.reason}`));
        }
      });
      client.on("publication", (ctx) => {
        options.onEvent(ctx.data, { kind: "private-control" });
      });
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
        async setConversationWatched(id: string, watched: boolean) {
          const current = subscriptions.get(id);
          if (!watched) {
            current?.cancel(new Error("conversation subscription cancelled before authorization"));
            if (current) {
              client.removeSubscription(current.subscription);
              current.subscription.removeAllListeners();
            }
            subscriptions.delete(id);
            return;
          }
          if (current) return current.ready;

          const subscription = client.newSubscription(`conv:${id}`);
          const state = makeSubscriptionState(subscription);
          subscription.on("subscribed", () => state.resolve());
          subscription.on("error", (ctx) => state.cancel(asError(ctx.error)));
          subscription.on("unsubscribed", (ctx) => {
            state.cancel(new Error(
              `conversation subscription rejected (${ctx.code}): ${ctx.reason}`,
            ));
          });
          subscription.on("publication", (ctx) => {
            options.onEvent(ctx.data, { kind: "conversation", conversationId: id });
          });
          subscriptions.set(id, state);
          subscription.subscribe();
          try {
            await state.ready;
          } catch (error) {
            // unsubscribe() only changes the subscription state. Centrifuge
            // deliberately keeps that channel in its internal registry, so a
            // later authoritative retry would throw "already exists".
            client.removeSubscription(subscription);
            subscription.removeAllListeners();
            subscriptions.delete(id);
            throw error;
          }
        },
      };
    },
  };
}

type CentrifugeSubscription = ReturnType<Centrifuge["newSubscription"]>;

interface SubscriptionState {
  readonly subscription: CentrifugeSubscription;
  ready: Promise<void>;
  settled: boolean;
  resolve(): void;
  reset(): void;
  cancel(error: Error): void;
}

function makeSubscriptionState(subscription: CentrifugeSubscription): SubscriptionState {
  const state: SubscriptionState = {
    subscription,
    ready: Promise.resolve(),
    settled: true,
    resolve: () => undefined,
    reset: () => undefined,
    cancel: () => undefined,
  };
  state.reset = () => {
    if (!state.settled) return;
    state.settled = false;
    state.ready = new Promise<void>((resolve, reject) => {
      state.resolve = () => {
        if (state.settled) return;
        state.settled = true;
        resolve();
      };
      state.cancel = (error) => {
        if (state.settled) return;
        state.settled = true;
        reject(error);
      };
    });
  };
  state.reset();
  return state;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
