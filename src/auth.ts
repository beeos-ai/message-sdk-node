/**
 * Ed25519-signed `messaging/token` provider for agents.
 *
 * Wraps a call to `POST {agentGatewayUrl}/api/v1/messaging/token` with the
 * BeeOS agent Ed25519 signature scheme (see
 * `services/agent-gateway/internal/http/agent_gateway.go` for the server
 * side). Returns a `TokenProvider` compatible with `MessageSDK.connect`.
 *
 * Signed message format: `METHOD|PATH|timestamp|nonce`
 * Headers:
 *   X-Agent-Public-Key: <base64 Ed25519 public key>
 *   X-Agent-Signature:  <base64 Ed25519 signature>
 *   X-Agent-Timestamp:  <unix seconds>
 *   X-Agent-Nonce:      <uuid>
 *
 * The `identity` interface is intentionally minimal so callers can plug in
 * their own keystore (file-backed, KMS, HSM, …) without taking a hard
 * dependency on a specific implementation.
 */

import { randomUUID } from "node:crypto";

import type { TokenProvider, TokenResponse } from "./envelope.js";

/**
 * Minimal identity contract required by the token provider.
 *
 *   - `publicKeyBase64` — base64-encoded raw 32-byte Ed25519 public key.
 *   - `sign(bytes)`     — Ed25519 signature over the canonical signing string.
 */
export interface MessagingIdentity {
  publicKeyBase64(): string | Promise<string>;
  sign(bytes: Uint8Array): Uint8Array | Promise<Uint8Array>;
}

export interface CreateMessagingTokenProviderOptions {
  /** Agent Gateway base URL (e.g. `https://agent-gw.beeos.ai`). */
  agentGatewayUrl: string;
  identity: MessagingIdentity;
  /** Override fetch (defaults to globalThis.fetch). Used by tests. */
  fetchImpl?: typeof fetch;
  /** Override path (defaults to `/api/v1/messaging/token`). */
  path?: string;
}

/**
 * Build the canonical Ed25519 signing string used by Agent Gateway.
 *
 * Exposed for unit tests and downstream code that needs to sign requests
 * to other Agent Gateway endpoints with the same scheme.
 */
export function buildSigningString(
  method: string,
  urlPath: string,
  timestamp: string,
  nonce: string,
): string {
  return `${method.toUpperCase()}|${urlPath}|${timestamp}|${nonce}`;
}

/**
 * Build Ed25519 auth headers for an Agent Gateway HTTP request.
 *
 * Public so callers reuse the same logic for non-messaging endpoints
 * (canvas/token, instances, etc.).
 */
export async function buildAgentAuthHeaders(
  method: string,
  urlPath: string,
  identity: MessagingIdentity,
): Promise<Record<string, string>> {
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const nonce = randomUUID();
  const signingString = buildSigningString(method, urlPath, timestamp, nonce);
  const signature = await identity.sign(
    new TextEncoder().encode(signingString),
  );
  const publicKey = await identity.publicKeyBase64();
  return {
    "X-Agent-Public-Key": publicKey,
    "X-Agent-Signature": Buffer.from(signature).toString("base64"),
    "X-Agent-Timestamp": timestamp,
    "X-Agent-Nonce": nonce,
  };
}

/**
 * Construct a `TokenProvider` that calls Agent Gateway with Ed25519 auth.
 *
 * The returned function ignores the `principalId` argument (the Agent
 * Gateway derives the canonical principal from the Ed25519 public key
 * — pod-is-principal). Callers should still pass the resolved principal
 * back to `MessageSDK.connect` after the first token fetch.
 */
export function createMessagingTokenProvider(
  opts: CreateMessagingTokenProviderOptions,
): TokenProvider {
  const baseUrl = opts.agentGatewayUrl.replace(/\/+$/, "");
  const path = opts.path ?? "/api/v1/messaging/token";
  const fetchImpl = opts.fetchImpl ?? fetch;
  const identity = opts.identity;

  return async (principalId: string): Promise<TokenResponse> => {
    const url = `${baseUrl}${path}`;
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...(await buildAgentAuthHeaders("POST", path, identity)),
    };
    const body: Record<string, unknown> = {};
    if (principalId) body.principal_id = principalId;
    const resp = await fetchImpl(url, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(
        `messaging-token POST ${path} returned ${resp.status}: ${text}`,
      );
    }
    return (await resp.json()) as TokenResponse;
  };
}
