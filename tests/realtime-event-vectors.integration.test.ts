import { existsSync, readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { RealtimeEventValidationError, validateRealtimeEvent } from "../src/protocol/index.js";

const canonicalPath = process.env.CANONICAL_REALTIME_EVENT_VECTORS;
const strict = process.env.STRICT_REALTIME_EVENT_VECTORS === "1";

interface CanonicalRealtimeVectors {
  valid: unknown[];
  invalid: unknown[];
}

function loadCanonicalVectors(): CanonicalRealtimeVectors | undefined {
  if (!canonicalPath) return undefined;
  if (!existsSync(canonicalPath)) {
    throw new Error(`canonical realtime vectors not found: ${canonicalPath}`);
  }
  const parsed: unknown = JSON.parse(readFileSync(canonicalPath, "utf8"));
  if (!parsed || typeof parsed !== "object" || !Array.isArray((parsed as { valid?: unknown }).valid)
    || !Array.isArray((parsed as { invalid?: unknown }).invalid)) {
    throw new Error(`canonical realtime vectors have invalid shape: ${canonicalPath}`);
  }
  return parsed as CanonicalRealtimeVectors;
}

const vectors = loadCanonicalVectors();

describe("RealtimeEventV1 canonical backend vectors", () => {
  if (!vectors) {
    if (strict) {
      it("fails when strict cross-repository conformance has no canonical path", () => {
        throw new Error(
          "STRICT_REALTIME_EVENT_VECTORS=1 requires CANONICAL_REALTIME_EVENT_VECTORS to point to the backend canonical JSON",
        );
      });
    } else {
      it.skip("requires CANONICAL_REALTIME_EVENT_VECTORS for cross-repository conformance");
    }
    return;
  }

  it("contains the complete v1 acceptance and rejection sets", () => {
    expect(vectors.valid).toHaveLength(20);
    expect(vectors.invalid).toHaveLength(9);
  });

  for (const event of vectors.valid) {
    it(`accepts ${(event as { eventId?: string }).eventId ?? "unnamed valid vector"}`, () => {
      expect(validateRealtimeEvent(event)).toEqual(event);
    });
  }

  for (const event of vectors.invalid) {
    it(`rejects ${(event as { eventId?: string }).eventId ?? "unnamed invalid vector"}`, () => {
      expect(() => validateRealtimeEvent(event)).toThrow(RealtimeEventValidationError);
    });
  }
});
