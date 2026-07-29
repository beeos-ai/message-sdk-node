/**
 * Conformance against the shared RealtimeEventV1 corpus.
 *
 * The realtime wire contract has three independent definitions: the Message
 * Service domain struct, the Go SDK struct, and this package's hand-written
 * validator. The corpus is the only thing that keeps them in step, so this
 * suite runs on a plain `npm test` — no environment variable, no skip. The
 * previous version was gated behind CANONICAL_REALTIME_EVENT_VECTORS and
 * therefore never ran on any machine.
 *
 * Message Service owns the file. This package vendors a verbatim copy so a
 * standalone npm checkout still validates the real contract;
 * `vendored-vectors-drift.test.ts` is what keeps the copy honest.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  REALTIME_EVENT_TYPES,
  RealtimeEventValidationError,
  validateRealtimeEvent,
} from "../src/protocol/index.js";

const VENDORED_VECTORS_PATH = join(
  dirname(fileURLToPath(import.meta.url)),
  "testdata",
  "realtime_event_v1_vectors.json",
);

interface RealtimeVectorSuite {
  contract: {
    schemaVersion: number;
    eventTypes: string[];
    consumerOnlyEventTypes: string[];
  };
  /** Accepted by every definition. */
  valid: unknown[];
  /** Rejected by every definition. */
  invalid: unknown[];
  /** Still accepted by the producer, rejected here — recorded latent gaps. */
  producerLenient: unknown[];
  /** Accepted here only; the server vocabulary has no producer for them. */
  consumerOnly: unknown[];
}

function loadVectors(): RealtimeVectorSuite {
  const parsed: unknown = JSON.parse(readFileSync(VENDORED_VECTORS_PATH, "utf8"));
  if (!parsed || typeof parsed !== "object") {
    throw new Error(`realtime vectors are not an object: ${VENDORED_VECTORS_PATH}`);
  }
  const suite = parsed as Partial<RealtimeVectorSuite>;
  const contract = suite.contract;
  if (!contract || !Array.isArray(contract.eventTypes) || !Array.isArray(contract.consumerOnlyEventTypes)) {
    throw new Error(`realtime vectors have no usable contract block: ${VENDORED_VECTORS_PATH}`);
  }
  for (const key of ["valid", "invalid", "producerLenient", "consumerOnly"] as const) {
    if (!Array.isArray(suite[key])) {
      throw new Error(`realtime vectors are missing the \`${key}\` array: ${VENDORED_VECTORS_PATH}`);
    }
  }
  return suite as RealtimeVectorSuite;
}

const vectors = loadVectors();

function label(event: unknown, fallback: string): string {
  const id = (event as { eventId?: string } | null)?.eventId;
  return id && id.length > 0 ? id : fallback;
}

describe("RealtimeEventV1 shared conformance corpus", () => {
  it("declares a v1 contract with a non-empty vocabulary", () => {
    expect(vectors.contract.schemaVersion).toBe(1);
    expect(vectors.contract.eventTypes.length).toBeGreaterThan(0);
    expect(vectors.valid.length).toBeGreaterThan(0);
    expect(vectors.invalid.length).toBeGreaterThan(0);
  });

  // The vocabulary this SDK accepts must be exactly the server's plus the
  // declared consumer-only extras. A Node-only event type that is not declared
  // in the corpus is an undocumented fork of the contract, and a server type
  // this SDK does not know is a dropped event.
  it("accepts exactly the server vocabulary plus the declared consumer-only types", () => {
    const server = new Set(vectors.contract.eventTypes);
    const consumerOnly = new Set(vectors.contract.consumerOnlyEventTypes);
    for (const type of server) {
      expect(consumerOnly.has(type), `${type} cannot be both server and consumer-only`).toBe(false);
    }
    expect([...REALTIME_EVENT_TYPES].sort()).toEqual([...server, ...consumerOnly].sort());
  });

  it("covers every server event type with at least one valid vector", () => {
    const covered = new Set(vectors.valid.map((event) => (event as { type: string }).type));
    for (const type of vectors.contract.eventTypes) {
      expect(covered.has(type), `no valid vector covers ${type}`).toBe(true);
    }
  });

  for (const [index, event] of vectors.valid.entries()) {
    it(`accepts ${label(event, `valid[${index}]`)}`, () => {
      expect(validateRealtimeEvent(event)).toEqual(event);
    });
  }

  for (const [index, event] of vectors.invalid.entries()) {
    it(`rejects ${label(event, `invalid[${index}]`)}`, () => {
      expect(() => validateRealtimeEvent(event)).toThrow(RealtimeEventValidationError);
    });
  }

  // These are payloads Message Service's own validator still lets through.
  // Rejecting them here is correct — it is the same asymmetry that took agent
  // pods down when the server omitted ordering.messageOffset — but the
  // asymmetry has to stay visible rather than being discovered in production.
  //
  // The list is empty today: the message-state, conversation-state, and
  // metadataVersion cases were tightened at the producer and moved into
  // `invalid`. `loadVectors` still requires the category to be declared, so an
  // empty list reads as "no known divergence" rather than "nobody checked".
  it("declares the producer-lenient category even when there is nothing to record", () => {
    expect(Array.isArray(vectors.producerLenient)).toBe(true);
  });

  for (const [index, event] of vectors.producerLenient.entries()) {
    it(`rejects producer-lenient ${label(event, `producerLenient[${index}]`)}`, () => {
      expect(() => validateRealtimeEvent(event)).toThrow(RealtimeEventValidationError);
    });
  }

  // No producer emits these. They are accepted here because this SDK carries
  // client-side handling ahead of the server; if that handling is removed, the
  // vector must move out of the corpus in the same change.
  for (const [index, event] of vectors.consumerOnly.entries()) {
    it(`accepts consumer-only ${label(event, `consumerOnly[${index}]`)}`, () => {
      expect(validateRealtimeEvent(event)).toEqual(event);
    });
  }
});
