/**
 * Shared business protocol for a failed assistant envelope.
 *
 * Message Service stores `parts` as opaque JSONB and does not validate
 * `custom.data`. This module is the typed contract for
 * `{ type: "custom", kind: "runtime_error" }` so Web can distinguish a
 * user-actionable billing failure from an ordinary runtime error without
 * a new top-level MS field.
 *
 * Wire shape:
 *   { "type": "custom", "kind": "runtime_error", "data": { "code": "insufficient_credits" } }
 *
 * Do not put HTTP status codes, provider error types, stack traces, or
 * API keys in `data`.
 */

export const RUNTIME_ERROR_PART_KIND = "runtime_error" as const;

export type RuntimeErrorCode = "insufficient_credits";

export interface RuntimeErrorPartData {
  code: RuntimeErrorCode;
}

export interface RuntimeErrorPart {
  type: "custom";
  kind: typeof RUNTIME_ERROR_PART_KIND;
  data: RuntimeErrorPartData;
}

const RUNTIME_ERROR_CODES: readonly RuntimeErrorCode[] = ["insufficient_credits"];

export function createRuntimeErrorPart(code: RuntimeErrorCode): RuntimeErrorPart {
  return {
    type: "custom",
    kind: RUNTIME_ERROR_PART_KIND,
    data: { code },
  };
}

/**
 * Strict parser for Web and agents. Requires `type=custom`,
 * `kind=runtime_error`, and a known `data.code`. Extra fields on `data`
 * are ignored so a future additive payload does not break readers.
 */
export function parseRuntimeErrorPart(part: unknown): RuntimeErrorPart | undefined {
  if (!part || typeof part !== "object" || Array.isArray(part)) return undefined;
  const record = part as Record<string, unknown>;
  if (record.type !== "custom" || record.kind !== RUNTIME_ERROR_PART_KIND) {
    return undefined;
  }
  const data = record.data;
  if (!data || typeof data !== "object" || Array.isArray(data)) return undefined;
  const code = (data as Record<string, unknown>).code;
  if (typeof code !== "string") return undefined;
  if (!RUNTIME_ERROR_CODES.includes(code as RuntimeErrorCode)) return undefined;
  return createRuntimeErrorPart(code as RuntimeErrorCode);
}
