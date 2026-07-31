import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  decodeRuntimeDispatchReceipt,
  RUNTIME_DISPATCH_CONTRACT_REVISION,
  RUNTIME_DISPATCH_CONTRACT_SHA256,
  RuntimeDispatchContractError,
  validateRealtimeEvent,
} from "../src/protocol/index.js";

const artifactPath = fileURLToPath(new URL(
  "../contracts/runtime-dispatch-contract-v1.schema.json",
  import.meta.url,
));
const artifactBytes = readFileSync(artifactPath);
const artifact = JSON.parse(artifactBytes.toString("utf8")) as {
  $id?: string;
  revision?: string;
  $defs?: Record<string, unknown>;
};

describe("runtime dispatch canonical contract artifact", () => {
  it("pins the shipped artifact revision and SHA256", () => {
    expect(artifact.$id).toBe(RUNTIME_DISPATCH_CONTRACT_REVISION);
    expect(artifact.revision).toBe(RUNTIME_DISPATCH_CONTRACT_REVISION);
    expect(artifact.$defs).toHaveProperty("realtimeEvent");
    expect(artifact.$defs).toHaveProperty("runtimeDispatch");
    expect(createHash("sha256").update(artifactBytes).digest("hex"))
      .toBe(RUNTIME_DISPATCH_CONTRACT_SHA256);
  });

  it("decodes only compatible exact HTTP runtime_dispatch objects", () => {
    expect(decodeRuntimeDispatchReceipt({ status: "accepted" }))
      .toEqual({ status: "accepted" });
    expect(decodeRuntimeDispatchReceipt({ status: "failed", code: "runtime_rejected" }))
      .toEqual({ status: "failed", code: "runtime_rejected" });
    expect(decodeRuntimeDispatchReceipt({
      status: "unconfirmed",
      code: "delivery_unconfirmed",
    })).toEqual({ status: "unconfirmed", code: "delivery_unconfirmed" });
    for (const invalid of [
      { status: "accepted", code: "runtime_rejected" },
      { status: "failed", code: "delivery_unconfirmed" },
      { status: "unconfirmed", code: "runtime_unavailable" },
      { status: "failed", code: "provider_error" },
      { status: "failed", code: "runtime_rejected", provider: "forbidden" },
    ]) {
      expect(() => decodeRuntimeDispatchReceipt(invalid))
        .toThrow(RuntimeDispatchContractError);
    }
  });

  it("decodes the minimal additive realtime envelope", () => {
    const event = {
      schemaVersion: 1,
      eventId: "dispatch-1",
      type: "runtime.dispatch.failed",
      scope: { tenantId: "tenant", conversationId: "c1", messageId: "m1" },
      actor: { kind: "service", id: "message-service" },
      correlation: { correlationId: "corr-1" },
      occurredAt: "2026-07-28T00:00:00.000Z",
      data: { status: "unconfirmed", code: "delivery_unconfirmed" },
    };
    expect(validateRealtimeEvent(event)).toEqual(event);
    expect(validateRealtimeEvent({
      ...event,
      additive: "preserved",
      correlation: { correlationId: "", provider: "preserved" },
    })).toMatchObject({ additive: "preserved" });
    expect(validateRealtimeEvent({
      ...event,
      occurredAt: "2026-07-28 00:00:00Z",
    })).toMatchObject({ occurredAt: "2026-07-28 00:00:00Z" });
  });
});
