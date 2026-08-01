import { describe, expect, it } from "vitest";

import * as root from "../src/index.js";
import * as node from "../src/node.js";
import * as reactNative from "../src/react-native.js";
import * as web from "../src/web.js";

describe("package entrypoint surface", () => {
  it("keeps the business root to the single unified factory", () => {
    expect(Object.keys(root).sort()).toEqual(["createMessageClient"]);
    for (const forbidden of [
      "MessageClient",
      "Centrifugo",
      "EventSource",
      "channel",
      "publish",
      "subscribeChannel",
      "token",
      "platform",
      "fallback",
    ]) {
      expect(root).not.toHaveProperty(forbidden);
    }
  });

  it("keeps the unified runtime API identical while Node adds only composition infrastructure", () => {
    expect(Object.keys(web).sort()).toEqual(["createMessageClient"]);
    expect(Object.keys(reactNative).sort()).toEqual([
      "createMessageClient",
      "createReactNativeMessageClientComposition",
    ]);
    expect(Object.keys(node).sort()).toEqual([
      "createMessageClient",
      "createNodeMessageClientComposition",
    ]);
    expect(web.createMessageClient).toBe(root.createMessageClient);
    expect(reactNative.createMessageClient).toBe(root.createMessageClient);
    expect(node.createMessageClient).toBe(root.createMessageClient);
  });

  it("never leaks the private Gateway composition factory from any entrypoint", () => {
    for (const mod of [root, web, node, reactNative]) {
      expect(mod).not.toHaveProperty("createGatewayMessageClientComposition");
    }
  });
});
