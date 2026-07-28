/** React Native entrypoint; its MessageClient API is identical on every platform. */
export { createMessageClient } from "./unified-client.js";
export { createReactNativeMessageClientComposition } from "./react-native-runtime.js";
export type { MessageClient } from "./unified-client.js";
export type * from "./facade/contracts.js";
export type { ReactNativeMessageClientOptions } from "./react-native-runtime.js";
