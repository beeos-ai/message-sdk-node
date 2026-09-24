import { afterEach, describe, expect, it, vi } from "vitest";
import { createNodeRuntimeOnlyDeliveryComposition } from "../src/node-runtime.js";
import type { RuntimeDeliveryAuthorityLease } from "../src/runtime-delivery.js";

function lease(): RuntimeDeliveryAuthorityLease {
  return { instanceId: "inst-1", leaseId: "lease-1", handlerIdentity: "runtime-handler-1",
    runtimeEpoch: "7", leaseExpiresAt: new Date(Date.now() + 60_000).toISOString(),
    journalStoreId: "journal-1", journalGeneration: "2", runtimeLeaseCredential: "current-lease" };
}

afterEach(() => vi.unstubAllGlobals());

describe("runtime-only Cloud Message delivery", () => {
  it("reads with only the current lease, without personal sessions or a delivery key", async () => {
    let current: RuntimeDeliveryAuthorityLease | null = lease();
    const requests: Array<{ url: string; bearer: string | null; delivery: string | null }> = [];
    vi.stubGlobal("fetch", vi.fn(async (url: string | URL, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      requests.push({ url: String(url), bearer: headers.get("authorization"),
        delivery: headers.get("x-runtime-delivery-key") });
      return Response.json({ status: "found" });
    }));
    const composition = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example", authority: { currentLease: () => current },
    });
    const consumer = composition.runtimeDelivery.consume({ onDelivery: async () => undefined });
    expect(await consumer.history("op-1")).toEqual({ status: "found", snapshot: { status: "found" } });
    expect(requests).toEqual([{ url: "https://message.example/api/v1/runtime/operations/op-1/history",
      bearer: "Bearer current-lease", delivery: null }]);
    current = { ...current!, leaseId: "lease-2", runtimeLeaseCredential: "renewed-lease" };
    expect(await consumer.history("op-2")).toEqual({ status: "found", snapshot: { status: "found" } });
    expect(requests[1]).toEqual({ url: "https://message.example/api/v1/runtime/operations/op-2/history",
      bearer: "Bearer renewed-lease", delivery: null });
    current = null;
    await expect(consumer.history("op-3")).rejects.toThrow("lease is unavailable");
    expect(requests).toHaveLength(2);
  });

  it("rejects cross-origin or credential-bearing configuration", () => {
    const authority = { currentLease: () => lease() };
    expect(() => createNodeRuntimeOnlyDeliveryComposition({ serviceUrl: "file:///tmp/socket",
      authority })).toThrow("invalid");
    expect(() => createNodeRuntimeOnlyDeliveryComposition({ serviceUrl: "https://user:pass@message.example",
      authority })).toThrow("invalid");
  });

  it("does not fetch after the lease rotates during origin resolution", async () => {
    let current = lease();
    const fetcher = vi.fn(async () => Response.json({ status: "found" }));
    vi.stubGlobal("fetch", fetcher);
    const consumer = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example", authority: { currentLease: () => current },
    }).runtimeDelivery.consume({ onDelivery: async () => undefined });
    const history = consumer.history("op-1");
    current = { ...current, leaseId: "next-lease" };
    await expect(history).rejects.toThrow("lease changed");
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("does not fetch if stopped immediately after starting the reader", async () => {
    const fetcher = vi.fn(async () => Response.json({ status: "deliveries", deliveries: [] }));
    vi.stubGlobal("fetch", fetcher);
    const consumer = createNodeRuntimeOnlyDeliveryComposition({
      serviceUrl: "https://message.example", authority: { currentLease: lease },
    }).runtimeDelivery.consume({ onDelivery: async () => undefined });
    consumer.start();
    await consumer.stop();
    expect(fetcher).not.toHaveBeenCalled();
  });
});
