import type {
  RuntimeDeliveryAppendReceipt,
  RuntimeDeliveryConsumeOptions,
  RuntimeDeliveryConsumer,
  RuntimeDeliveryHistory,
  RuntimeDeliveryLease,
  RuntimeDeliveryPort,
  RuntimeMethodDelivery,
} from "./facade/contracts.js";
import type { JsonValue } from "./protocol/index.js";

export interface RuntimeDeliveryOriginPort {
  serviceOrigin(): Promise<string>;
}

export interface RuntimeDeliveryAuthorityLease extends RuntimeDeliveryLease {
  readonly runtimeLeaseCredential: string;
}

export interface RuntimeDeliveryAuthorityPort {
  currentLease(): RuntimeDeliveryAuthorityLease | null;
}

/**
 * Node durable runtime delivery. It owns the HTTP claim lifecycle; WSS may
 * wake a caller, but never becomes the command truth source.
 */
/** Throttle floor for lease-starvation reporting. The loop re-evaluates the
 * lease every idleDelayMs (500ms default), so unthrottled reporting would be
 * two events a second for the entire outage. */
const leaseStarvationReportIntervalMs = 30_000;

export class NodeRuntimeDeliveryPort implements RuntimeDeliveryPort {
  constructor(
    private readonly origin: RuntimeDeliveryOriginPort,
    private readonly authority: RuntimeDeliveryAuthorityPort,
    private readonly scopedDeliveryKey: string,
  ) {
    if (!scopedDeliveryKey) throw new Error("runtime delivery key is required");
  }

  consume(options: RuntimeDeliveryConsumeOptions): RuntimeDeliveryConsumer {
    return new NodeRuntimeDeliveryConsumer(
      this.origin,
      this.authority,
      this.scopedDeliveryKey,
      options,
    );
  }
}

class NodeRuntimeDeliveryConsumer implements RuntimeDeliveryConsumer {
  private controller?: AbortController;
  private running?: Promise<void>;
  private readonly workers = new Map<string, {
    lease: RuntimeDeliveryAuthorityLease;
    controller: AbortController;
    completion: Promise<void>;
    expiryTimer?: ReturnType<typeof setTimeout>;
  }>();
  private consecutiveRenewFailures = 0;
  private leaseStarvedSince?: number;
  private leaseStarvationReportedAt = 0;

  constructor(
    private readonly origin: RuntimeDeliveryOriginPort,
    private readonly authority: RuntimeDeliveryAuthorityPort,
    private readonly scopedDeliveryKey: string,
    private readonly options: RuntimeDeliveryConsumeOptions,
  ) {
    if (options.readCount !== undefined &&
        (!Number.isSafeInteger(options.readCount) || options.readCount <= 0)) {
      throw new Error("runtime delivery readCount must be a positive integer");
    }
  }

  start(): void {
    if (this.running) return;
    const controller = new AbortController();
    this.controller = controller;
    const running = this.loop(controller.signal)
      .catch((error) => {
        if (!controller.signal.aborted) this.report(error);
      })
      .finally(() => {
        if (this.running === running) this.running = undefined;
      });
    this.running = running;
  }

  async stop(signal?: AbortSignal): Promise<void> {
    this.controller?.abort();
    for (const worker of this.workers.values()) worker.controller.abort();
    if (!this.running) return;
    await abortable(this.running, signal);
  }

  async history(operationId: string): Promise<RuntimeDeliveryHistory> {
    return await this.historyWithLease(operationId, this.requireCurrentLease());
  }

  async append(
    operationId: string,
    type: string,
    payload: JsonValue,
    executionGrant?: string,
  ): Promise<RuntimeDeliveryAppendReceipt> {
    return await this.appendWithLease(
      operationId,
      type,
      payload,
      this.requireCurrentLease(),
      executionGrant,
    );
  }

  async acknowledge(deliveryIds: readonly string[]): Promise<void> {
    await this.acknowledgeWithLease(deliveryIds, this.requireCurrentLease());
  }

  private async loop(signal: AbortSignal): Promise<void> {
    const renewal = setInterval(() => {
      void this.renewActive().catch((error) => this.report(error));
    }, this.options.renewIntervalMs ?? 20_000);
    renewal.unref?.();
    try {
      while (!signal.aborted) {
        const lease = this.authority.currentLease();
        if (!lease) {
          this.reportLeaseStarvation();
          await delay(this.options.idleDelayMs ?? 500, signal);
          continue;
        }
        this.reportLeaseRestored();
        if (leaseExpired(lease)) {
          for (const worker of this.workers.values()) worker.controller.abort();
          this.report(new Error("runtime delivery lease expired"));
          await delay(this.options.idleDelayMs ?? 500, signal);
          continue;
        }
        let deliveries: readonly RuntimeMethodDelivery[];
        try {
          deliveries = await this.read(
            lease,
            this.options.readCount ?? 32,
            this.options.blockMs ?? 5_000,
            signal,
          );
        } catch (error) {
          if (signal.aborted) return;
          this.report(error);
          await delay(this.options.idleDelayMs ?? 500, signal);
          continue;
        }
        for (const delivery of deliveries) {
          if (signal.aborted || this.workers.has(delivery.deliveryId)) continue;
          const workerController = new AbortController();
          const abort = () => workerController.abort();
          signal.addEventListener("abort", abort, { once: true });
          const entry = {
            lease,
            controller: workerController,
            completion: Promise.resolve(),
            expiryTimer: undefined,
          };
          this.armWorkerExpiry(entry);
          entry.completion = this.options.onDelivery(delivery, {
            lease: sanitizedLease(lease),
            signal: workerController.signal,
          }).catch((error) => {
            if (!workerController.signal.aborted) this.report(error);
          }).finally(() => {
            if (entry.expiryTimer) clearTimeout(entry.expiryTimer);
            signal.removeEventListener("abort", abort);
            this.workers.delete(delivery.deliveryId);
          });
          this.workers.set(delivery.deliveryId, entry);
        }
      }
    } finally {
      clearInterval(renewal);
      await Promise.allSettled([...this.workers.values()].map((worker) => worker.completion));
    }
  }

  private async read(
    lease: RuntimeDeliveryAuthorityLease,
    maxCount: number,
    blockMs: number,
    signal: AbortSignal,
  ): Promise<readonly RuntimeMethodDelivery[]> {
    const response = await this.runtimeRequest(
      "/api/v1/runtime/deliveries/read",
      lease,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ schemaVersion: 1, maxCount, blockMs }),
        signal,
      },
    );
    const raw = asRecord(await responseJson(response));
    if (!response.ok) throw httpError(response, raw, "runtime delivery read failed");
    if (!raw || raw.status !== "deliveries" || !Array.isArray(raw.deliveries) ||
        raw.deliveries.length > maxCount) {
      throw new Error("Message Service returned invalid runtime deliveries");
    }
    const deliveries = raw.deliveries.map(runtimeDelivery);
    if (new Set(deliveries.map((delivery) => delivery.deliveryId)).size !== deliveries.length) {
      throw new Error("Message Service returned duplicate runtime delivery ids");
    }
    return deliveries;
  }

  private async renewActive(): Promise<void> {
    const current = this.authority.currentLease();
    if (!current || leaseExpired(current)) {
      for (const worker of this.workers.values()) worker.controller.abort();
      return;
    }
    const ids: string[] = [];
    for (const [deliveryId, worker] of this.workers) {
      if (!sameLeaseFence(current, worker.lease)) {
        worker.controller.abort();
      } else {
        this.refreshWorkerLease(worker, current);
        ids.push(deliveryId);
      }
    }
    if (!ids.length) return;
    try {
      const response = await this.runtimeRequest(
        "/api/v1/runtime/deliveries/renew",
        current,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ schemaVersion: 1, deliveryIds: ids }),
        },
      );
      const raw = asRecord(await responseJson(response));
      if (!response.ok) throw httpError(response, raw, "runtime delivery renewal failed");
      if (!raw || raw.status !== "renewed" ||
          !stringArray(raw.renewed) || !stringArray(raw.notPending)) {
        throw new Error("Message Service returned invalid runtime delivery renewal");
      }
      this.consecutiveRenewFailures = 0;
      const lost = new Set(raw.notPending);
      for (const [deliveryId, worker] of this.workers) {
        if (lost.has(deliveryId)) worker.controller.abort();
      }
    } catch (error) {
      this.consecutiveRenewFailures += 1;
      this.report(error);
      if (this.consecutiveRenewFailures >= 3) {
        for (const worker of this.workers.values()) worker.controller.abort();
      }
    }
  }

  private refreshWorkerLease(
    worker: {
      lease: RuntimeDeliveryAuthorityLease;
      controller: AbortController;
      expiryTimer?: ReturnType<typeof setTimeout>;
    },
    lease: RuntimeDeliveryAuthorityLease,
  ): void {
    worker.lease = lease;
    this.armWorkerExpiry(worker);
  }

  private armWorkerExpiry(worker: {
    lease: RuntimeDeliveryAuthorityLease;
    controller: AbortController;
    expiryTimer?: ReturnType<typeof setTimeout>;
  }): void {
    if (worker.expiryTimer) clearTimeout(worker.expiryTimer);
    worker.expiryTimer = setTimeout(() => {
      const current = this.authority.currentLease();
      if (!current || leaseExpired(current) || !sameLeaseFence(current, worker.lease)) {
        worker.controller.abort();
        return;
      }
      // Heartbeat renewal rotates the credential and expiry while retaining
      // the lease fence. Re-arm from the live authority instead of aborting at
      // the read-time snapshot's stale expiry.
      this.refreshWorkerLease(worker, current);
    }, Math.max(0, leaseExpiryMs(worker.lease) - Date.now()));
    worker.expiryTimer.unref?.();
  }

  private async acknowledgeWithLease(
    deliveryIds: readonly string[],
    lease: RuntimeDeliveryAuthorityLease,
  ): Promise<void> {
    this.assertCurrentLease(lease);
    const response = await this.runtimeRequest(
      "/api/v1/runtime/deliveries/ack",
      lease,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ schemaVersion: 1, deliveryIds }),
      },
    );
    const raw = asRecord(await responseJson(response));
    if (!response.ok) throw httpError(response, raw, "runtime delivery acknowledgement failed");
    if (!raw || raw.status !== "acknowledged" ||
        !stringArray(raw.deliveryIds) || !sameIds(raw.deliveryIds, deliveryIds)) {
      throw new Error("Message Service returned invalid runtime delivery acknowledgement");
    }
  }

  private async historyWithLease(
    operationId: string,
    lease: RuntimeDeliveryAuthorityLease,
  ): Promise<RuntimeDeliveryHistory> {
    this.assertCurrentLease(lease);
    const response = await this.runtimeRequest(
      `/api/v1/runtime/operations/${segment(operationId)}/history`,
      lease,
      { method: "GET" },
    );
    if (response.status === 404) return { status: "not_found" };
    if (response.status === 410) return { status: "expired" };
    const raw = await responseJson(response);
    if (!response.ok) throw httpError(response, asRecord(raw), "runtime history read failed");
    if (!isJsonValue(raw) || !asRecord(raw)) {
      throw new Error("Message Service returned invalid runtime history");
    }
    return { status: "found", snapshot: raw };
  }

  private async appendWithLease(
    operationId: string,
    type: string,
    payload: JsonValue,
    lease: RuntimeDeliveryAuthorityLease,
    executionGrant?: string,
  ): Promise<RuntimeDeliveryAppendReceipt> {
    this.assertCurrentLease(lease);
    let response: Response;
    try {
      response = await this.runtimeRequest(
        `/api/v1/runtime/operations/${segment(operationId)}/messages`,
        lease,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ schemaVersion: 1, type, payload }),
        },
        executionGrant,
      );
    } catch (error) {
      return await this.reconcileAmbiguousAppend(operationId, lease, error);
    }
    const raw = asRecord(await responseJson(response));
    if (!response.ok) {
      if (response.status === 502 || response.status === 503 || response.status === 504 ||
          raw?.code === "OUTCOME_UNKNOWN") {
        return await this.reconcileAmbiguousAppend(operationId, lease);
      }
      throw httpError(response, raw, "runtime append failed");
    }
    if (!raw || raw.status !== "persisted" ||
        (raw.disposition !== "created" && raw.disposition !== "reused") ||
        !isJsonValue(raw.message)) {
      throw new Error("Message Service returned invalid runtime append receipt");
    }
    return {
      outcome: raw.disposition === "created" ? "created" : "duplicate",
      message: raw.message,
    };
  }

  private async reconcileAmbiguousAppend(
    operationId: string,
    lease: RuntimeDeliveryAuthorityLease,
    _cause?: unknown,
  ): Promise<RuntimeDeliveryAppendReceipt> {
    try {
      const history = await this.historyWithLease(operationId, lease);
      if (history.status === "found") {
        const snapshot = asRecord(history.snapshot);
        if (snapshot?.terminal || snapshot?.terminalSource === "final_message") {
          return {
            outcome: "reconciled",
            history: history.snapshot,
          };
        }
        return { outcome: "outcome_unknown", history: history.snapshot };
      }
    } catch {
      // Preserve ambiguity. Never retry append and never switch transports.
    }
    return { outcome: "outcome_unknown" };
  }

  private async runtimeRequest(
    path: string,
    lease: RuntimeDeliveryAuthorityLease,
    init: RequestInit,
    executionGrant?: string,
  ): Promise<Response> {
    this.assertCurrentLease(lease);
    const serviceOrigin = await this.origin.serviceOrigin();
    const url = new URL(path, `${serviceOrigin}/`);
    if (url.origin !== new URL(serviceOrigin).origin) {
      throw new Error("runtime delivery request escaped the Message Service origin");
    }
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${lease.runtimeLeaseCredential}`);
    headers.set("x-runtime-delivery-key", this.scopedDeliveryKey);
    if (executionGrant) headers.set("x-beeos-execution-grant", executionGrant);
    const response = await fetch(url, { ...init, headers, redirect: "error" });
    if (response.url && new URL(response.url).origin !== url.origin) {
      throw new Error("runtime delivery response origin changed");
    }
    return response;
  }

  private requireCurrentLease(): RuntimeDeliveryAuthorityLease {
    const lease = this.authority.currentLease();
    if (!lease) throw new Error("runtime delivery lease is unavailable");
    if (leaseExpired(lease)) throw new Error("runtime delivery lease expired");
    return lease;
  }

  private assertCurrentLease(expected: RuntimeDeliveryAuthorityLease): void {
    const current = this.authority.currentLease();
    if (!current || !sameLease(current, expected)) {
      throw new Error("runtime delivery lease changed");
    }
    if (leaseExpired(current)) throw new Error("runtime delivery lease expired");
  }

  private report(error: unknown): void {
    try {
      this.options.onError?.(error);
    } catch {
      // Observers never own transport lifecycle.
    }
  }

  private reportLeaseStarvation(): void {
    const now = Date.now();
    this.leaseStarvedSince ??= now;
    const starvedForMs = now - this.leaseStarvedSince;
    // Startup and lease renewal both cross this branch briefly. Reporting the
    // first iteration would announce a 0ms starvation on every healthy boot.
    if (starvedForMs < leaseStarvationReportIntervalMs) return;
    if (this.leaseStarvationReportedAt &&
        now - this.leaseStarvationReportedAt < leaseStarvationReportIntervalMs) {
      return;
    }
    this.leaseStarvationReportedAt = now;
    this.reportStarvation(starvedForMs, false);
  }

  private reportLeaseRestored(): void {
    const starvedSince = this.leaseStarvedSince;
    if (starvedSince === undefined) return;
    this.leaseStarvedSince = undefined;
    const reported = this.leaseStarvationReportedAt;
    this.leaseStarvationReportedAt = 0;
    // A window shorter than one report interval was never announced, so its
    // recovery would be the only line in the log.
    if (!reported) return;
    this.reportStarvation(Date.now() - starvedSince, true);
  }

  private reportStarvation(starvedForMs: number, recovered: boolean): void {
    try {
      this.options.onLeaseStarvation?.({ starvedForMs, recovered });
    } catch {
      // Observers never own transport lifecycle.
    }
  }
}

function runtimeDelivery(value: unknown): RuntimeMethodDelivery {
  const raw = asRecord(value);
  if (!raw || typeof raw.deliveryId !== "string" || !raw.deliveryId ||
      typeof raw.redelivered !== "boolean" ||
      !Number.isSafeInteger(raw.idleMs) || Number(raw.idleMs) < 0 ||
      !isJsonValue(raw.message) || !asRecord(raw.message) ||
      (raw.executionGrant !== undefined &&
        (typeof raw.executionGrant !== "string" || !raw.executionGrant))) {
    throw new Error("Message Service returned an invalid runtime delivery");
  }
  return {
    deliveryId: raw.deliveryId,
    redelivered: raw.redelivered,
    idleMs: Number(raw.idleMs),
    message: raw.message,
    ...(typeof raw.executionGrant === "string"
      ? { executionGrant: raw.executionGrant }
      : {}),
  };
}

function sameLease(
  left: RuntimeDeliveryAuthorityLease,
  right: RuntimeDeliveryAuthorityLease,
): boolean {
  return left.instanceId === right.instanceId &&
    left.handlerIdentity === right.handlerIdentity &&
    left.runtimeEpoch === right.runtimeEpoch &&
    left.leaseId === right.leaseId &&
    left.leaseExpiresAt === right.leaseExpiresAt &&
    left.journalStoreId === right.journalStoreId &&
    left.journalGeneration === right.journalGeneration &&
    left.runtimeLeaseCredential === right.runtimeLeaseCredential;
}

function sameLeaseFence(
  left: RuntimeDeliveryAuthorityLease,
  right: RuntimeDeliveryAuthorityLease,
): boolean {
  return left.instanceId === right.instanceId &&
    left.handlerIdentity === right.handlerIdentity &&
    left.runtimeEpoch === right.runtimeEpoch &&
    left.leaseId === right.leaseId &&
    left.journalStoreId === right.journalStoreId &&
    left.journalGeneration === right.journalGeneration;
}

function sanitizedLease(lease: RuntimeDeliveryAuthorityLease): RuntimeDeliveryLease {
  return {
    instanceId: lease.instanceId,
    handlerIdentity: lease.handlerIdentity,
    runtimeEpoch: lease.runtimeEpoch,
    leaseId: lease.leaseId,
    leaseExpiresAt: lease.leaseExpiresAt,
    journalStoreId: lease.journalStoreId,
    journalGeneration: lease.journalGeneration,
  };
}

function leaseExpiryMs(lease: RuntimeDeliveryLease): number {
  const value = Date.parse(lease.leaseExpiresAt);
  if (!Number.isFinite(value)) throw new Error("runtime delivery lease expiry is invalid");
  return value;
}

function leaseExpired(lease: RuntimeDeliveryLease): boolean {
  return leaseExpiryMs(lease) <= Date.now();
}

function stringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function sameIds(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length &&
    [...left].sort().every((value, index) => value === [...right].sort()[index]);
}

function segment(value: string): string {
  if (!value) throw new Error("runtime delivery identity is required");
  return encodeURIComponent(value);
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function isJsonValue(value: unknown): value is JsonValue {
  if (value === null ||
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean") return true;
  if (Array.isArray(value)) return value.every(isJsonValue);
  const record = asRecord(value);
  return Boolean(record) && Object.values(record!).every(isJsonValue);
}

async function responseJson(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return undefined;
  }
}

function httpError(
  response: Response,
  raw: Record<string, unknown> | undefined,
  fallback: string,
): Error {
  const message = typeof raw?.message === "string" && raw.message
    ? raw.message
    : fallback;
  return new Error(`${message} (${response.status})`);
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    const abort = () => {
      clearTimeout(timer);
      resolve();
    };
    signal.addEventListener("abort", abort, { once: true });
  });
}

async function abortable(promise: Promise<void>, signal?: AbortSignal): Promise<void> {
  if (!signal) return await promise;
  if (signal.aborted) throw signal.reason;
  await Promise.race([
    promise,
    new Promise<never>((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    }),
  ]);
}
