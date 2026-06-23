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

// Shared cross-language conformance vectors (ADR-0025): the Go MS reducer,
// Go SDK reducer, and this TS reducer must never silently diverge.
//
// The single source of truth lives in the backend monorepo at
//   services/message/pkg/domain/message/testdata/reducer_vectors.json
// The Go SDK reads it directly because it ships inside that monorepo
// (backend/sdks/message-sdk-go/reducer_test.go). This SDK is published as a
// standalone npm package from its own repository, so it CANNOT reach across
// repos at test time — doing so broke the publish pipeline (the backend path
// does not exist in a standalone checkout). We therefore vendor a verbatim
// copy under tests/testdata/ so this package is self-contained and the
// conformance suite runs on every publish. Drift against the canonical file
// is guarded separately (scripts/check-reducer-vectors.mjs / CI), not by a
// runtime cross-repo read. The fixture lives under tests/ and is excluded
// from the published tarball by package.json "files".
const __dirname = dirname(fileURLToPath(import.meta.url));

const CANONICAL_VECTORS = "./testdata/reducer_vectors.json";

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
