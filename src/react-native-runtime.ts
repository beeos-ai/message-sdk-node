import {
  createGatewayMessageClientComposition,
} from "./gateway-runtime.js";
import type {
  CurrentPrincipalPort,
  LifecyclePort,
  MessageClientComposition,
} from "./facade/contracts.js";

/**
 * @deprecated Use the unified `createMessageClient({ platform: "mobile", ... })`
 * factory instead. Retained for existing React Native composition roots;
 * behavior is unchanged (same Gateway v1 HTTP/WSS routes, same required
 * `accessTokenProvider`, same "mobile" messaging-token platform).
 */
export interface ReactNativeMessageClientOptions {
  readonly gatewayUrl: string;
  readonly accessTokenProvider: () => Promise<string>;
  /**
   * Login-owned identity is verified against the token endpoint's canonical
   * principal_id before it can author optimistic rows.
   */
  readonly currentPrincipal: CurrentPrincipalPort;
  readonly lifecycle?: LifecyclePort;
  readonly fetch?: typeof globalThis.fetch;
}

/**
 * @deprecated React Native/Gateway-v1 composition. Use the unified
 * `createMessageClient({ platform: "mobile", gatewayUrl, currentPrincipal,
 * accessTokenProvider, lifecycle?, fetch? })` factory instead. This helper
 * now delegates to that same shared implementation with a fixed
 * `platform: "mobile"` and is kept only for source compatibility.
 */
export function createReactNativeMessageClientComposition(
  options: ReactNativeMessageClientOptions,
): MessageClientComposition {
  return createGatewayMessageClientComposition({ ...options, platform: "mobile" });
}
