import { afterEach, describe, expect, it, vi } from "vitest";

import {
  NodeRuntimeDeliveryPort,
  type RuntimeDeliveryAuthorityLease,
  type RuntimeDeliveryAuthorityPort,
} from "../src/runtime-delivery.js";

const origin = "https://message.example";

function lease(
  overrides: Partial<RuntimeDeliveryAuthorityLease> = {},
): RuntimeDeliveryAuthorityLease {
  return {
    instanceId: "instance-1",
    handlerIdentity: "runtime:pod-a",
    runtimeEpoch: "7",
    leaseId: "lease-1",
    leaseExpiresAt: new Date(Date.now() + 60_000).toISOString(),
    journalStoreId: "journal-1",
    journalGeneration: "3",
    runtimeLeaseCredential: "secret-credential",
    ...overrides,
  };
}

function authority(initial: RuntimeDeliveryAuthorityLease | null): {
  port: RuntimeDeliveryAuthorityPort;
  set(value: RuntimeDeliveryAuthorityLease | null): void;
} {
  let current = initial;
  return {
    port: { currentLease: () => current },
    set(value) { current = value; },
  };
}

function json(value: unknown, status = 200): Response {
  return Response.json(value, { status });
}

function waitForAbort(signal?: AbortSignal): Promise<Response> {
  return new Promise((_resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("aborted", "AbortError"));
      return;
    }
    signal?.addEventListener(
      "abort",
      () => reject(new DOMException("aborted", "AbortError")),
      { once: true },
    );
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("Node durable runtime delivery", () => {
  it("keeps credentials and the scoped key inside transport while exposing only a lease fence", async () => {
    const active = authority(lease());
    const calls: Array<{ path: string; credential: string | null; key: string | null }> = [];
    let readCount = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(String(input));
      const headers = new Headers(init?.headers);
      calls.push({
        path: url.pathname,
        credential: headers.get("authorization"),
        key: headers.get("x-runtime-delivery-key"),
      });
      if (url.pathname.endsWith("/deliveries/read")) {
        readCount += 1;
        if (readCount > 1) return waitForAbort(init?.signal ?? undefined);
        return json({
          status: "deliveries",
          deliveries: [{
            deliveryId: "delivery-1",
            redelivered: false,
            idleMs: 0,
            message: { type: "runtime_rpc_request", payload: { operationId: "operation-1" } },
            executionGrant: "grant-1",
          }],
        });
      }
      if (url.pathname.endsWith("/history")) {
        return json({ terminal: false, terminalSource: "none" });
      }
      if (url.pathname.endsWith("/ack")) {
        return json({ status: "acknowledged", deliveryIds: ["delivery-1"] });
      }
      throw new Error(`unexpected ${url.pathname}`);
    }));

    let resolveHandled!: () => void;
    const handled = new Promise<void>((resolve) => { resolveHandled = resolve; });
    let consumer!: ReturnType<NodeRuntimeDeliveryPort["consume"]>;
    consumer = new NodeRuntimeDeliveryPort(
      { serviceOrigin: async () => origin },
      active.port,
      "scoped-key",
    ).consume({
      async onDelivery(delivery, context) {
        expect(delivery.deliveryId).toBe("delivery-1");
        expect(context.lease).toEqual({
          instanceId: "instance-1",
          handlerIdentity: "runtime:pod-a",
          runtimeEpoch: "7",
          leaseId: "lease-1",
          leaseExpiresAt: expect.any(String),
          journalStoreId: "journal-1",
          journalGeneration: "3",
        });
        expect(context.lease).not.toHaveProperty("runtimeLeaseCredential");
        expect(context.lease).not.toHaveProperty("scopedDeliveryKey");
        await consumer.history("operation-1");
        await consumer.acknowledge(["delivery-1"]);
        resolveHandled();
      },
      blockMs: 1,
      renewIntervalMs: 60_000,
    });
    consumer.start();
    await handled;
    await consumer.stop();

    expect(calls.map((call) => call.path)).toEqual(expect.arrayContaining([
      "/api/v1/runtime/deliveries/read",
      "/api/v1/runtime/operations/operation-1/history",
      "/api/v1/runtime/deliveries/ack",
    ]));
    expect(calls.filter((call) => call.path.endsWith("/deliveries/read"))).toHaveLength(2);
    expect(calls.every((call) => call.credential === "Bearer secret-credential")).toBe(true);
    expect(calls.every((call) => call.key === "scoped-key")).toBe(true);
  });

  it("reconciles an ambiguous append through history without replaying the append", async () => {
    const active = authority(lease());
    let appendCalls = 0;
    let historyCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/messages")) {
        appendCalls += 1;
        return json({ code: "upstream_unavailable" }, 503);
      }
      if (url.pathname.endsWith("/history")) {
        historyCalls += 1;
        return json({ terminal: true, terminalSource: "final_message", final: { id: "m-final" } });
      }
      throw new Error(`unexpected ${url.pathname}`);
    }));
    const consumer = new NodeRuntimeDeliveryPort(
      { serviceOrigin: async () => origin },
      active.port,
      "scoped-key",
    ).consume({ async onDelivery() {} });

    await expect(consumer.append(
      "operation-1",
      "runtime_rpc_response",
      { outcome: "succeeded" },
      "grant-1",
    )).resolves.toEqual({
      outcome: "reconciled",
      history: {
        terminal: true,
        terminalSource: "final_message",
        final: { id: "m-final" },
      },
    });
    expect(appendCalls).toBe(1);
    expect(historyCalls).toBe(1);
  });

  it("fails closed before transport when the authority lease is expired", async () => {
    const active = authority(lease({
      leaseExpiresAt: new Date(Date.now() - 1).toISOString(),
    }));
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const consumer = new NodeRuntimeDeliveryPort(
      { serviceOrigin: async () => origin },
      active.port,
      "scoped-key",
    ).consume({ async onDelivery() {} });

    await expect(consumer.history("operation-1")).rejects.toThrow("lease expired");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("aborts an active handler immediately when any lease fence field changes", async () => {
    const current = lease();
    const active = authority(current);
    let readCount = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/deliveries/read")) {
        readCount += 1;
        if (readCount > 1) return waitForAbort(init?.signal ?? undefined);
        return json({
          status: "deliveries",
          deliveries: [{
            deliveryId: "delivery-1",
            redelivered: false,
            idleMs: 0,
            message: { type: "runtime_rpc_request" },
          }],
        });
      }
      throw new Error(`unexpected ${url.pathname}`);
    }));

    let handlerStarted!: () => void;
    const started = new Promise<void>((resolve) => { handlerStarted = resolve; });
    let handlerAborted!: () => void;
    const aborted = new Promise<void>((resolve) => { handlerAborted = resolve; });
    const consumer = new NodeRuntimeDeliveryPort(
      { serviceOrigin: async () => origin },
      active.port,
      "scoped-key",
    ).consume({
      async onDelivery(_delivery, context) {
        handlerStarted();
        if (context.signal.aborted) handlerAborted();
        else context.signal.addEventListener("abort", handlerAborted, { once: true });
        await new Promise<void>((resolve) => {
          context.signal.addEventListener("abort", () => resolve(), { once: true });
        });
      },
      renewIntervalMs: 5,
      blockMs: 1,
    });
    consumer.start();
    await started;
    active.set({ ...current, journalGeneration: "4" });
    await aborted;
    await consumer.stop();
  });

  describe("lease starvation reporting", () => {
    const cases = [
      {
        name: "stays silent below one report interval",
        starvedMs: 29_000,
        expected: [] as Array<{ recovered: boolean }>,
      },
      {
        name: "reports once at the first full interval",
        starvedMs: 31_000,
        expected: [{ recovered: false }],
      },
      {
        name: "reports once per interval while starved",
        starvedMs: 91_000,
        expected: [{ recovered: false }, { recovered: false }, { recovered: false }],
      },
    ];

    for (const scenario of cases) {
      it(scenario.name, async () => {
        vi.useFakeTimers();
        vi.stubGlobal("fetch", vi.fn(async () => {
          throw new Error("a starved consumer must never reach the transport");
        }));
        const starved = authority(null);
        const reports: Array<{ starvedForMs: number; recovered: boolean }> = [];
        const consumer = new NodeRuntimeDeliveryPort(
          { serviceOrigin: async () => origin },
          starved.port,
          "scoped-key",
        ).consume({
          onDelivery: async () => { throw new Error("no delivery is possible without a lease"); },
          onLeaseStarvation: (input) => { reports.push(input); },
          idleDelayMs: 500,
          renewIntervalMs: 60_000,
        });
        consumer.start();
        await vi.advanceTimersByTimeAsync(scenario.starvedMs);

        expect(reports.map(({ recovered }) => ({ recovered }))).toEqual(scenario.expected);
        for (const report of reports) {
          expect(report.starvedForMs).toBeGreaterThanOrEqual(30_000);
        }
        await consumer.stop();
      });
    }

    it("reports recovery only when the starvation was announced", async () => {
      vi.useFakeTimers();
      // A failing read is the only lease-present path that backs off through
      // idleDelayMs. A blocking read would park the loop and an empty
      // successful read would spin it without ever yielding to fake timers,
      // so neither lets the test observe the lease going away again.
      vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("read unavailable"); }));
      const starved = authority(null);
      const reports: Array<{ starvedForMs: number; recovered: boolean }> = [];
      const consumer = new NodeRuntimeDeliveryPort(
        { serviceOrigin: async () => origin },
        starved.port,
        "scoped-key",
      ).consume({
        onDelivery: async () => {},
        onError: () => {},
        onLeaseStarvation: (input) => { reports.push(input); },
        idleDelayMs: 500,
        blockMs: 1,
        renewIntervalMs: 60_000,
      });
      consumer.start();

      // A gap shorter than one interval is never announced, so its recovery
      // would otherwise be the only line an operator ever sees.
      await vi.advanceTimersByTimeAsync(5_000);
      starved.set(lease());
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reports).toEqual([]);

      starved.set(null);
      await vi.advanceTimersByTimeAsync(45_000);
      expect(reports).toHaveLength(1);
      expect(reports[0]).toMatchObject({ recovered: false });

      starved.set(lease());
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reports).toHaveLength(2);
      expect(reports[1]).toMatchObject({ recovered: true });
      expect(reports[1]!.starvedForMs).toBeGreaterThanOrEqual(45_000);

      await consumer.stop();
    });
  });
});
