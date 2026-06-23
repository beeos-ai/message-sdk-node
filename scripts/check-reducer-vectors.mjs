#!/usr/bin/env node
// Drift guard for the vendored reducer conformance vectors (ADR-0025).
//
// The single source of truth is the backend monorepo file:
//   services/message/pkg/domain/message/testdata/reducer_vectors.json
// This package ships a verbatim copy at tests/testdata/reducer_vectors.json so
// the conformance suite is self-contained and runs on every standalone publish.
// This script fails if the two copies have diverged, so the copy can never
// silently rot.
//
// Resolution order for the canonical file:
//   1. $CANONICAL_REDUCER_VECTORS (explicit path, used by CI)
//   2. ../backend/services/message/pkg/domain/message/testdata/reducer_vectors.json
//      relative to the repo root (the standard monorepo working-tree layout)
//
// If the canonical file cannot be found, the script reports that it could not
// verify and exits 0 — a standalone checkout with no backend tree present is
// not a drift, it is simply "nothing to compare against". Set STRICT=1 to turn
// a missing canonical file into a hard failure (use this in any CI job that is
// responsible for guaranteeing the canonical file is available).

import { readFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const localPath = join(repoRoot, "tests", "testdata", "reducer_vectors.json");

const canonicalPath =
  process.env.CANONICAL_REDUCER_VECTORS ||
  resolve(
    repoRoot,
    "..",
    "backend",
    "services",
    "message",
    "pkg",
    "domain",
    "message",
    "testdata",
    "reducer_vectors.json",
  );

const strict = process.env.STRICT === "1";

if (!existsSync(localPath)) {
  console.error(`[check-reducer-vectors] vendored copy missing: ${localPath}`);
  process.exit(1);
}

if (!existsSync(canonicalPath)) {
  const msg = `[check-reducer-vectors] canonical file not found: ${canonicalPath}`;
  if (strict) {
    console.error(`${msg}\n  STRICT=1 set — treating missing canonical as failure.`);
    process.exit(1);
  }
  console.warn(`${msg}\n  Skipping drift check (no backend tree to compare against).`);
  process.exit(0);
}

const local = readFileSync(localPath);
const canonical = readFileSync(canonicalPath);

if (!local.equals(canonical)) {
  console.error(
    "[check-reducer-vectors] DRIFT DETECTED: vendored copy differs from canonical source.\n" +
      `  canonical: ${canonicalPath}\n` +
      `  vendored:  ${localPath}\n` +
      "  Re-sync with:\n" +
      `    cp "${canonicalPath}" "${localPath}"`,
  );
  process.exit(1);
}

console.log("[check-reducer-vectors] OK — vendored copy matches canonical source.");
