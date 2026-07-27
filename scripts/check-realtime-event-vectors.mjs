#!/usr/bin/env node
// Runs the SDK parser against the backend-owned RealtimeEventV1 vectors.
//
// This package deliberately does not vendor the file: the backend is the
// canonical owner. A caller must provide its exact path, so a standalone npm
// checkout cannot claim cross-repository conformance it did not run.

import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const canonicalPath = process.env.CANONICAL_REALTIME_EVENT_VECTORS;

if (!canonicalPath) {
  console.error(
    "[check-realtime-event-vectors] CANONICAL_REALTIME_EVENT_VECTORS is required; refusing to report cross-repository conformance without the backend-owned JSON.",
  );
  process.exit(1);
}
if (!existsSync(canonicalPath)) {
  console.error(`[check-realtime-event-vectors] canonical file not found: ${canonicalPath}`);
  process.exit(1);
}

const vitest = join(repoRoot, "node_modules", "vitest", "vitest.mjs");
if (!existsSync(vitest)) {
  console.error(`[check-realtime-event-vectors] Vitest is unavailable: ${vitest}`);
  process.exit(1);
}

const result = spawnSync(process.execPath, [vitest, "run", "tests/realtime-event-vectors.integration.test.ts"], {
  cwd: repoRoot,
  env: {
    ...process.env,
    CANONICAL_REALTIME_EVENT_VECTORS: canonicalPath,
    STRICT_REALTIME_EVENT_VECTORS: "1",
  },
  stdio: "inherit",
});

process.exit(result.status ?? 1);
