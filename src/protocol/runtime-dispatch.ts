export const RUNTIME_DISPATCH_CONTRACT_REVISION = "beeos.runtime-dispatch.v1" as const;
export const RUNTIME_DISPATCH_CONTRACT_SHA256 = "6e4f6b6a60433ffe75ed2d2902c20ad651ac2058026819384d8a74a4a8b3525b" as const;

export const RUNTIME_DISPATCH_FAILED_CODES = [
  "runtime_target_invalid",
  "runtime_request_invalid",
  "runtime_rejected",
  "runtime_unavailable",
] as const;
export const RUNTIME_DISPATCH_UNCONFIRMED_CODES = [
  "delivery_unconfirmed",
] as const;

export type RuntimeDispatchFailedCode = (typeof RUNTIME_DISPATCH_FAILED_CODES)[number];
export type RuntimeDispatchUnconfirmedCode = (typeof RUNTIME_DISPATCH_UNCONFIRMED_CODES)[number];
export type RuntimeDispatchCode = RuntimeDispatchFailedCode | RuntimeDispatchUnconfirmedCode;
export type RuntimeDispatchStatus = "accepted" | "failed" | "unconfirmed";

export type RuntimeDispatchReceipt =
  | { readonly status: "accepted" }
  | { readonly status: "failed"; readonly code: RuntimeDispatchFailedCode }
  | { readonly status: "unconfirmed"; readonly code: RuntimeDispatchUnconfirmedCode };

export class RuntimeDispatchContractError extends Error {
  constructor(readonly reason: string) {
    super(`invalid ${RUNTIME_DISPATCH_CONTRACT_REVISION}: ${reason}`);
    this.name = "RuntimeDispatchContractError";
  }
}

export function decodeRuntimeDispatchReceipt(value: unknown): RuntimeDispatchReceipt {
  if (!isRecord(value) || !hasOnlyKeys(value, ["status", "code"])) {
    throw new RuntimeDispatchContractError("runtime_dispatch must be an exact object");
  }
  if (value.status === "accepted" && Object.keys(value).length === 1) {
    return { status: "accepted" };
  }
  if (
    value.status === "failed" &&
    typeof value.code === "string" &&
    (RUNTIME_DISPATCH_FAILED_CODES as readonly string[]).includes(value.code) &&
    Object.keys(value).length === 2
  ) {
    return value as RuntimeDispatchReceipt;
  }
  if (
    value.status === "unconfirmed" &&
    value.code === "delivery_unconfirmed" &&
    Object.keys(value).length === 2
  ) {
    return { status: "unconfirmed", code: "delivery_unconfirmed" };
  }
  throw new RuntimeDispatchContractError("runtime_dispatch status/code is incompatible");
}

export function isRuntimeDispatchFailureData(
  value: unknown,
): value is Exclude<RuntimeDispatchReceipt, { status: "accepted" }> {
  try {
    const decoded = decodeRuntimeDispatchReceipt(value);
    return decoded.status !== "accepted";
  } catch {
    return false;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  return Object.keys(value).every((key) => allowed.includes(key));
}
