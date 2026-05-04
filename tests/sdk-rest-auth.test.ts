/**
 * Unit tests for MessageSDK.restRequest auth header forwarding.
 *
 * Three modes the SDK supports (in priority order):
 *
 *   1. config.apiKey is set → X-API-Key (service-to-service)
 *   2. config.apiKey unset, currentToken populated by connect() → Authorization: Bearer <jwt>
 *      (principal-level: agent pods using their Centrifugo JWT against MS REST)
 *   3. neither → request goes out unauthenticated (MS will 401 in production)
 *
 * Tests use a mock fetch to capture the outgoing request and assert the
 * headers without spinning up a real HTTP server.
 */

import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";

import { MessageSDK } from "../src/sdk.js";
import type { MessageSDKLogger, TokenResponse } from "../src/envelope.js";

const noopLogger: MessageSDKLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
};

interface CapturedRequest {
  url: string | URL | Request;
  init?: RequestInit;
}

function installMockFetch(captured: CapturedRequest[], status = 201, body: unknown = { ok: true }) {
  const mock = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
    captured.push({ url: url as string | URL, init });
    return new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  });
  globalThis.fetch = mock as unknown as typeof globalThis.fetch;
  return mock;
}

describe("MessageSDK restRequest auth", () => {
  const captured: CapturedRequest[] = [];
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    captured.length = 0;
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it("uses X-API-Key when config.apiKey is set", async () => {
    installMockFetch(captured, 201, {
      status: "delivered",
      message: { message_id: "m-1", offset: 1 },
    });
    const sdk = new MessageSDK(
      { serviceUrl: "https://msg.example", apiKey: "service-key" },
      noopLogger,
    );

    await sdk.sendChannelMessage("ch-1", {
      senderId: "alice",
      type: "agent_reply",
      payload: { text: "hi" },
    });

    expect(captured).toHaveLength(1);
    const headers = (captured[0].init?.headers ?? {}) as Record<string, string>;
    expect(headers["X-API-Key"]).toBe("service-key");
    expect(headers["Authorization"]).toBeUndefined();
  });

  it("uses Authorization Bearer <token> when no apiKey and connect cached a token", async () => {
    installMockFetch(captured, 201, { status: "delivered" });
    const sdk = new MessageSDK(
      { serviceUrl: "https://msg.example" }, // NO apiKey
      noopLogger,
    );

    // Simulate post-connect state: capture token via the same mechanism
    // production uses (custom tokenProvider feeds our internal getToken).
    // We piggyback on a no-op connect that bails before WS but still
    // primes currentToken — easiest path is to manually call the
    // internal hook. Since the SDK doesn't expose that, exercise via
    // setServiceUrl + a dedicated tokenProvider.
    //
    // Easier: directly test that calling sendChannelMessage without
    // connecting yields no Authorization header (no token cached); then
    // simulate token by using a tokenProvider through public API.

    // Step 1: no token cached → no Authorization header
    await sdk.sendChannelMessage("ch-1", {
      senderId: "alice",
      type: "agent_reply",
      payload: { text: "hi" },
    });
    let headers = (captured[0].init?.headers ?? {}) as Record<string, string>;
    expect(headers["Authorization"]).toBeUndefined();
    expect(headers["X-API-Key"]).toBeUndefined();

    // Step 2: prime currentToken via connect's internal token capture.
    // We exploit the public connect path with a fake centrifuge that
    // never actually opens a WS — we just need the getToken closure
    // to fire once.
    captured.length = 0;
    const tokenResp: TokenResponse = {
      token: "jwt-payload",
      centrifugo_url: "ws://unused",
      principal_id: "alice",
      channels: [],
      expires_at: Math.floor(Date.now() / 1000) + 3600,
    };
    // Skip the real connect; reach into the SDK by calling the
    // tokenProvider-driven path directly is non-trivial — instead add
    // a test seam: call the getToken via the public `connect` once,
    // catch the WS failure, and assert currentToken was set.
    try {
      await sdk.connect("alice", {
        centrifugoUrl: "ws://unreachable.test",
        tokenProvider: async () => tokenResp,
      });
    } catch {
      /* WS will fail — but getToken ran first and cached the JWT */
    }

    await sdk.sendChannelMessage("ch-1", {
      senderId: "alice",
      type: "agent_reply",
      payload: { text: "hi" },
    });
    headers = (captured[0].init?.headers ?? {}) as Record<string, string>;
    expect(headers["Authorization"]).toBe("Bearer jwt-payload");
    expect(headers["X-API-Key"]).toBeUndefined();
  });

  it("apiKey takes precedence over currentToken when both are set", async () => {
    installMockFetch(captured);
    const sdk = new MessageSDK(
      { serviceUrl: "https://msg.example", apiKey: "service-key" },
      noopLogger,
    );

    // Prime token cache
    const tokenResp: TokenResponse = {
      token: "jwt-payload",
      centrifugo_url: "ws://unused",
      principal_id: "alice",
      channels: [],
      expires_at: Math.floor(Date.now() / 1000) + 3600,
    };
    try {
      await sdk.connect("alice", {
        centrifugoUrl: "ws://unreachable.test",
        tokenProvider: async () => tokenResp,
      });
    } catch {
      /* expected */
    }

    captured.length = 0;
    await sdk.sendChannelMessage("ch-1", {
      senderId: "alice",
      type: "agent_reply",
      payload: { text: "hi" },
    });
    const headers = (captured[0].init?.headers ?? {}) as Record<string, string>;
    expect(headers["X-API-Key"]).toBe("service-key");
    expect(headers["Authorization"]).toBeUndefined();
  });

  it("setServiceUrl late-binds the REST URL after construction", async () => {
    installMockFetch(captured);
    const sdk = new MessageSDK(
      { /* no serviceUrl yet */ },
      noopLogger,
    );

    // Without serviceUrl, sendChannelMessage must throw rather than
    // fire an unauthenticated request.
    await expect(
      sdk.sendChannelMessage("ch-1", {
        senderId: "alice",
        type: "agent_reply",
        payload: {},
      }),
    ).rejects.toThrow(/serviceUrl/);

    sdk.setServiceUrl("https://msg.example");
    await sdk.sendChannelMessage("ch-1", {
      senderId: "alice",
      type: "agent_reply",
      payload: {},
    });
    expect(captured).toHaveLength(1);
    expect(String(captured[0].url)).toContain("https://msg.example");
  });

  it("setServiceUrl ignores empty values and is idempotent", async () => {
    installMockFetch(captured);
    const sdk = new MessageSDK(
      { serviceUrl: "https://msg.example" },
      noopLogger,
    );
    sdk.setServiceUrl(undefined);
    sdk.setServiceUrl("");
    sdk.setServiceUrl("https://msg.example"); // no-op (same)

    await sdk.sendChannelMessage("ch-1", {
      senderId: "alice",
      type: "agent_reply",
      payload: {},
    });
    expect(captured).toHaveLength(1);
    expect(String(captured[0].url)).toContain("https://msg.example");
  });
});
