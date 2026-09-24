import { afterEach, describe, expect, it, vi } from "vitest";
import { createNodeRuntimeOnlyDeliveryComposition } from "../src/node-runtime.js";
import type { RuntimeDeliveryAuthorityLease, RuntimeDeliveryToken } from "../src/runtime-delivery.js";

function lease(): RuntimeDeliveryAuthorityLease {
  return { instanceId: "inst-1", leaseId: "lease-1", handlerIdentity: "runtime-handler-1",
    runtimeEpoch: "7", leaseExpiresAt: new Date(Date.now() + 60_000).toISOString(),
    journalStoreId: "journal-1", journalGeneration: "2", runtimeLeaseCredential: "current-lease" };
}

const token = (current: RuntimeDeliveryAuthorityLease): RuntimeDeliveryToken => ({
  deliveryToken: "header.payload.signature", expiresAt: Math.floor(Date.now() / 1_000) + 30,
  instanceId: current.instanceId, leaseId: current.leaseId,
  handlerIdentity: current.handlerIdentity, runtimeEpoch: current.runtimeEpoch,
});

afterEach(() => vi.unstubAllGlobals());

describe("runtime-only Cloud Message delivery", () => {
  it("reads with only the current lease and token, without ordinary Message sessions", async () => {
    let current: RuntimeDeliveryAuthorityLease | null = lease();
    const issued = vi.fn(async (active: RuntimeDeliveryAuthorityLease) => token(active));
    const requests: Array<{ url: string; bearer: string | null; delivery: string | null }> = [];
    vi.stubGlobal("fetch", vi.fn(async (url: string | URL, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      requests.push({ url: String(url), bearer: headers.get("authorization"),
        delivery: headers.get("x-runtime-delivery-key") });
      return Response.json({ status: "found" });
    }));
    const composition = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example", authority: { currentLease: () => current },
      deliveryTokenProvider: issued,
    });
    const consumer = composition.runtimeDelivery.consume({ onDelivery: async () => undefined });
    expect(await consumer.history("op-1")).toEqual({ status: "found", snapshot: { status: "found" } });
    expect(requests).toEqual([{ url: "https://message.example/api/v1/runtime/operations/op-1/history",
      bearer: "Bearer current-lease", delivery: "header.payload.signature" }]);
    expect(issued).toHaveBeenCalledOnce();
    current = { ...current!, leaseId: "lease-2", runtimeLeaseCredential: "renewed-lease" };
    expect(await consumer.history("op-2")).toEqual({ status: "found", snapshot: { status: "found" } });
    expect(requests[1]).toEqual({ url: "https://message.example/api/v1/runtime/operations/op-2/history",
      bearer: "Bearer renewed-lease", delivery: "header.payload.signature" });
    expect(issued).toHaveBeenCalledTimes(2);
  });

  it("fences mismatched, expired, excessive-TTL and changed-lease tokens before HTTP", async () => {
    let current: RuntimeDeliveryAuthorityLease | null = lease();
    let response: RuntimeDeliveryToken = token(current);
    const fetcher = vi.fn(async () => Response.json({}));
    vi.stubGlobal("fetch", fetcher);
    const composition = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example", authority: { currentLease: () => current },
      deliveryTokenProvider: async () => response,
    });
    const consumer = composition.runtimeDelivery.consume({ onDelivery: async () => undefined });
    for (const invalid of [
      { ...response, instanceId: "other" },
      { ...response, leaseId: "old" },
      { ...response, handlerIdentity: "other" },
      { ...response, runtimeEpoch: "8" },
      { ...response, expiresAt: Math.floor(Date.now() / 1_000) - 1 },
      { ...response, expiresAt: Math.floor(Date.now() / 1_000) + 61 },
      { ...response, deliveryToken: "not-a-jwt" },
    ]) {
      response = invalid;
      await expect(consumer.history("op-1")).rejects.toThrow();
    }
    expect(fetcher).not.toHaveBeenCalled();
    response = token(current);
    const old = current;
    const changing = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example", authority: { currentLease: () => current },
      deliveryTokenProvider: async () => {
        current = { ...old!, leaseId: "lease-2" };
        return token(old!);
      },
    }).runtimeDelivery.consume({ onDelivery: async () => undefined });
    await expect(changing.history("op-2")).rejects.toThrow("lease changed");
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("stops a blocked token request without sending a Message request", async () => {
    let entered!: () => void;
    const providerEntered = new Promise<void>((resolve) => { entered = resolve; });
    let providerSignal: AbortSignal | undefined;
    const fetcher = vi.fn(async () => Response.json({}));
    vi.stubGlobal("fetch", fetcher);
    const consumer = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example",
      authority: { currentLease: () => lease() },
      deliveryTokenProvider: async (_active, signal) => {
        providerSignal = signal;
        entered();
        return await new Promise<RuntimeDeliveryToken>(() => undefined);
      },
    }).runtimeDelivery.consume({ onDelivery: async () => undefined });
    consumer.start();
    await providerEntered;
    await consumer.stop();
    expect(providerSignal?.aborted).toBe(true);
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("does not call the provider after an immediate stop", async () => {
    const provider = vi.fn(async (active: RuntimeDeliveryAuthorityLease) => token(active));
    const fetcher = vi.fn(async () => Response.json({}));
    vi.stubGlobal("fetch", fetcher);
    const consumer = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example", authority: { currentLease: lease },
      deliveryTokenProvider: provider,
    }).runtimeDelivery.consume({ onDelivery: async () => undefined });
    consumer.start();
    await consumer.stop();
    expect(provider).not.toHaveBeenCalled();
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("handles a provider rejection concurrent with stop without sending HTTP", async () => {
    let entered!: () => void;
    const providerEntered = new Promise<void>((resolve) => { entered = resolve; });
    const fetcher = vi.fn(async () => Response.json({}));
    vi.stubGlobal("fetch", fetcher);
    const consumer = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example", authority: { currentLease: lease },
      deliveryTokenProvider: async (_active, signal) => {
        entered();
        return await new Promise<RuntimeDeliveryToken>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("provider aborted")), { once: true });
        });
      },
    }).runtimeDelivery.consume({ onDelivery: async () => undefined });
    consumer.start();
    await providerEntered;
    await consumer.stop();
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("rejects cross-origin or credential-bearing configuration", () => {
    const authority = { currentLease: () => lease() };
    const deliveryTokenProvider = async (active: RuntimeDeliveryAuthorityLease) => token(active);
    expect(() => createNodeRuntimeOnlyDeliveryComposition({ serviceUrl: "file:///tmp/socket",
      authority, deliveryTokenProvider })).toThrow("invalid");
    expect(() => createNodeRuntimeOnlyDeliveryComposition({ serviceUrl: "https://user:pass@message.example",
      authority, deliveryTokenProvider })).toThrow("invalid");
  });
});
