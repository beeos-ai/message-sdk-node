import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  applyWireFrame,
  emptySnapshot,
  snapshotBody,
  type ApplyResult,
  type ReducedSnapshot,
  type WireFrame,
} from "../src/index.js";

// Shared conformance vectors — read DIRECTLY from the single canonical
// MS file (no in-repo copy), so the Go MS reducer, Go SDK reducer, and
// this TS reducer can never silently diverge. The Go SDK does the same
// (backend/sdks/message-sdk-go/reducer_test.go). This is a test-only
// in-repo path; it is never bundled into the published npm package.
const __dirname = dirname(fileURLToPath(import.meta.url));

// tests/ -> repo root is ../../../ ; then into the backend MS testdata.
const CANONICAL_VECTORS =
  "../../../backend/services/message/pkg/domain/message/testdata/reducer_vectors.json";

interface VectorCase {
  name: string;
  frames: WireFrame[];
  results: ApplyResult[];
  final_body: string;
  final_state: string;
  final_stop_reason?: string;
  final_parts?: unknown;
}

interface VectorSuite {
  cases: VectorCase[];
}

const suite = JSON.parse(
  readFileSync(join(__dirname, CANONICAL_VECTORS), "utf8"),
) as VectorSuite;

describe("applyWireFrame — shared conformance vectors (ADR-0025)", () => {
  it("loads at least one case", () => {
    expect(suite.cases.length).toBeGreaterThan(0);
  });

  for (const c of suite.cases) {
    it(c.name, () => {
      let snap: ReducedSnapshot = emptySnapshot();
      c.frames.forEach((f, i) => {
        const { snapshot, result } = applyWireFrame(snap, f);
        snap = snapshot;
        expect(result, `frame ${i}`).toBe(c.results[i]);
      });
      expect(snapshotBody(snap)).toBe(c.final_body);
      expect(snap.state ?? "").toBe(c.final_state);
      expect(snap.stopReason ?? "").toBe(c.final_stop_reason ?? "");
      if (c.final_parts !== undefined) {
        expect(snap.parts).toEqual(c.final_parts);
      }
    });
  }
});
