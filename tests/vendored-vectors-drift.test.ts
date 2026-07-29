/**
 * Drift guard for the corpora this package vendors from Message Service.
 *
 * Message Service owns both files. This package keeps verbatim copies so its
 * conformance suites run in a standalone npm checkout, which means the copies can
 * rot. This suite is the counterweight, and it runs in the default `npm test`.
 *
 * It replaces `scripts/check-*-vectors.mjs`: a drift guard that has to be invoked
 * by hand, with an environment variable, is a guard nobody runs. The reducer
 * script also resolved the canonical file at `<repo>/../backend/...`, which is
 * `<meta>/sdks/backend` in the layout it was written for — a path that has never
 * existed, so it reported "nothing to compare against" on every machine.
 *
 * A standalone checkout of this repository alone has no backend tree to compare
 * against, and no test inside one repository can observe an edit made in another.
 * The mirror of this check lives in the backend repository, next to the files
 * being edited, which is where drift is introduced.
 */
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";

import { describe, expect, it } from "vitest";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

interface MirroredCorpus {
  /** Path inside this package. */
  readonly vendored: string;
  /** Path inside the backend repository. */
  readonly canonicalRelative: string;
  /** Environment variable that overrides canonical resolution for this corpus. */
  readonly overrideEnv: string;
}

const MIRRORED_CORPORA: readonly MirroredCorpus[] = [
  {
    vendored: join(repoRoot, "tests", "testdata", "realtime_event_v1_vectors.json"),
    canonicalRelative: join(
      "services", "message", "pkg", "domain", "realtime", "testdata", "realtime_event_v1_vectors.json",
    ),
    overrideEnv: "CANONICAL_REALTIME_EVENT_VECTORS",
  },
  {
    vendored: join(repoRoot, "tests", "testdata", "reducer_vectors.json"),
    canonicalRelative: join(
      "services", "message", "pkg", "domain", "message", "testdata", "reducer_vectors.json",
    ),
    overrideEnv: "CANONICAL_REDUCER_VECTORS",
  },
];

/**
 * Candidate locations of the canonical file. The override is authoritative when
 * set — a machine with several backend worktrees needs to say which one it means.
 * Otherwise both standard layouts are searched:
 *   - `<meta>/backend/…`, where this package sits at `<meta>/sdks/message-sdk-node`.
 *   - `<parent>/backend/…`, a flat layout with both repositories side by side.
 */
function canonicalCandidates(corpus: MirroredCorpus): string[] {
  const explicit = process.env[corpus.overrideEnv];
  if (explicit) {
    // A mistyped override must not degrade into "nothing to compare against".
    if (!existsSync(explicit)) {
      throw new Error(`${corpus.overrideEnv} points at a file that does not exist: ${explicit}`);
    }
    return [explicit];
  }
  return [
    resolve(repoRoot, "..", "..", "backend", corpus.canonicalRelative),
    resolve(repoRoot, "..", "backend", corpus.canonicalRelative),
  ];
}

describe("vendored Message Service corpora", () => {
  for (const corpus of MIRRORED_CORPORA) {
    const name = corpus.vendored.slice(repoRoot.length + 1);

    it(`vendors ${name}`, () => {
      expect(existsSync(corpus.vendored), `${corpus.vendored} is missing`).toBe(true);
    });

    // The assertion is over every canonical file this checkout can reach. In the
    // meta-repository layout, and on any developer machine with a backend tree,
    // that is one file and this is a real byte comparison. In a standalone npm
    // checkout the set is empty and the assertion is vacuous; that residual gap
    // is a property of splitting the contract across two repositories.
    it(`keeps ${name} byte-identical to every reachable canonical copy`, () => {
      const vendored = readFileSync(corpus.vendored).toString("utf8");
      for (const canonical of canonicalCandidates(corpus).filter((path) => existsSync(path))) {
        const message = `${name} differs from ${canonical}; re-sync with:\n`
          + `  cp "${canonical}" "${corpus.vendored}"`;
        expect(readFileSync(canonical).toString("utf8"), message).toBe(vendored);
      }
    });
  }
});
