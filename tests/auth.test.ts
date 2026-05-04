import { generateKeyPairSync, createPublicKey, sign as cryptoSign, verify as cryptoVerify } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  buildAgentAuthHeaders,
  buildSigningString,
  createMessagingTokenProvider,
  type MessagingIdentity,
} from "../src/auth.js";
import type { TokenResponse } from "../src/envelope.js";

function generateEd25519(): {
  publicKeyRaw: Uint8Array;
  privateKeyDer: Buffer;
} {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519", {
    publicKeyEncoding: { type: "spki", format: "der" },
    privateKeyEncoding: { type: "pkcs8", format: "der" },
  });
  const pubDer = publicKey as unknown as Buffer;
  return {
    // Strip the 12-byte SPKI prefix to recover raw 32-byte Ed25519 key.
    publicKeyRaw:
      pubDer.length === 44 ? new Uint8Array(pubDer.subarray(12)) : new Uint8Array(pubDer),
    privateKeyDer: privateKey as unknown as Buffer,
  };
}

function makeIdentity(): {
  identity: MessagingIdentity;
  publicKeyDer: Buffer;
} {
  const { publicKeyRaw, privateKeyDer } = generateEd25519();
  const keyObj = createPublicKey({
    key: Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), Buffer.from(publicKeyRaw)]),
    format: "der",
    type: "spki",
  });
  const publicKeyDer = keyObj.export({ type: "spki", format: "der" }) as Buffer;
  const identity: MessagingIdentity = {
    publicKeyBase64: () => Buffer.from(publicKeyRaw).toString("base64"),
    sign: (bytes) => {
      const privKeyObj = require("node:crypto").createPrivateKey({
        key: privateKeyDer,
        format: "der",
        type: "pkcs8",
      });
      return new Uint8Array(cryptoSign(null, Buffer.from(bytes), privKeyObj));
    },
  };
  return { identity, publicKeyDer };
}

describe("buildSigningString", () => {
  it("uppercases the method and joins with pipes", () => {
    expect(buildSigningString("post", "/api/v1/messaging/token", "1700000000", "abc"))
      .toBe("POST|/api/v1/messaging/token|1700000000|abc");
  });
});

describe("buildAgentAuthHeaders", () => {
  it("returns Ed25519 auth headers verifiable with the public key", async () => {
    const { identity, publicKeyDer } = makeIdentity();

    const headers = await buildAgentAuthHeaders("POST", "/api/v1/messaging/token", identity);

    expect(headers["X-Agent-Public-Key"]).toMatch(/^[A-Za-z0-9+/=]+$/);
    expect(headers["X-Agent-Signature"]).toMatch(/^[A-Za-z0-9+/=]+$/);
    expect(headers["X-Agent-Timestamp"]).toMatch(/^\d+$/);
    expect(headers["X-Agent-Nonce"]).toMatch(/^[0-9a-f-]{36}$/);

    const signing = buildSigningString(
      "POST",
      "/api/v1/messaging/token",
      headers["X-Agent-Timestamp"],
      headers["X-Agent-Nonce"],
    );
    const sigBytes = Buffer.from(headers["X-Agent-Signature"], "base64");
    const pubKeyObj = createPublicKey({ key: publicKeyDer, format: "der", type: "spki" });
    const ok = cryptoVerify(null, Buffer.from(signing), pubKeyObj, sigBytes);
    expect(ok).toBe(true);
  });
});

describe("createMessagingTokenProvider", () => {
  it("POSTs to /api/v1/messaging/token with Ed25519 headers", async () => {
    const { identity } = makeIdentity();
    const expected: TokenResponse = {
      token: "tok-123",
      centrifugo_url: "wss://msg/ws",
      channels: ["personal:instance-7"],
      principal_id: "instance-7",
      expires_at: 9999,
    };

    let captured: { url?: string; init?: RequestInit } = {};
    const fakeFetch: typeof fetch = async (url, init) => {
      captured = { url: String(url), init };
      return new Response(JSON.stringify(expected), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };

    const provider = createMessagingTokenProvider({
      agentGatewayUrl: "https://agent-gw.beeos.ai/",
      identity,
      fetchImpl: fakeFetch,
    });

    const tok = await provider("");
    expect(tok).toEqual(expected);

    expect(captured.url).toBe("https://agent-gw.beeos.ai/api/v1/messaging/token");
    expect(captured.init?.method).toBe("POST");
    const headers = new Headers(captured.init?.headers);
    expect(headers.get("X-Agent-Public-Key")).not.toBeNull();
    expect(headers.get("X-Agent-Signature")).not.toBeNull();
    expect(headers.get("X-Agent-Timestamp")).not.toBeNull();
    expect(headers.get("X-Agent-Nonce")).not.toBeNull();
    expect(headers.get("Content-Type")).toBe("application/json");
    expect(JSON.parse(String(captured.init?.body))).toEqual({});
  });

  it("forwards principal_id when caller passes one", async () => {
    const { identity } = makeIdentity();
    let body: unknown = null;
    const fakeFetch: typeof fetch = async (_url, init) => {
      body = JSON.parse(String(init?.body));
      return new Response(
        JSON.stringify({
          token: "t",
          centrifugo_url: "wss://x",
          channels: [],
          principal_id: "p",
          expires_at: 1,
        }),
        { status: 200 },
      );
    };
    const provider = createMessagingTokenProvider({
      agentGatewayUrl: "https://agent-gw.beeos.ai",
      identity,
      fetchImpl: fakeFetch,
    });
    await provider("custom-pid");
    expect(body).toEqual({ principal_id: "custom-pid" });
  });

  it("throws on non-2xx with the response body in the message", async () => {
    const { identity } = makeIdentity();
    const fakeFetch: typeof fetch = async () =>
      new Response("instance not found", { status: 404 });
    const provider = createMessagingTokenProvider({
      agentGatewayUrl: "https://agent-gw.beeos.ai",
      identity,
      fetchImpl: fakeFetch,
    });
    await expect(provider("")).rejects.toThrow(
      /messaging-token POST .* returned 404: instance not found/,
    );
  });
});
