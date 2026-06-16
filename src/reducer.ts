// Streaming delta reducer (ADR-0025) — TypeScript port.
//
// Behavioural twin of the canonical Go reducer
// (services/message/pkg/domain/message/reducer.go). The two stay in
// lockstep via the shared JSON conformance vectors at
// backend/services/message/pkg/domain/message/testdata/reducer_vectors.json,
// which reducer.test.ts reads DIRECTLY (no in-repo copy) — same single
// source of truth the Go SDK reducer_test.go folds.
//
// A WSS-direct consumer (device-agent / beeos-claw observing a channel)
// folds append-only {body_from, body_chunk} frames back into the
// cumulative body. body_from is a UTF-8 *byte* offset, so the reducer
// works on encoded bytes — JS string .length is UTF-16 code units and
// would misalign on 中文 / emoji.

import type { Part } from "./types.js";

export const WIRE_EVENT_CREATED = "message.created";
export const WIRE_EVENT_UPDATED = "message.updated";
export const WIRE_EVENT_DELTA = "message.delta";

/** Accept-header token to opt into the raw delta wire. MUST match the
 * MS edge constant in services/message/.../channel_stream.go. */
export const MEDIA_TYPE_DELTA_WIRE = "application/vnd.beeos.message-delta";

/** Minimal projection of a wire envelope the reducer folds. */
export interface WireFrame {
  event?: string;
  body?: string;
  body_from?: number;
  body_chunk?: string;
  parts?: Part[];
  state?: string;
  stop_reason?: string;
}

/** Running reconstructed view. `bodyBytes` is the implicit cursor: the
 * next in-order delta must declare body_from === bodyBytes.length. */
export interface ReducedSnapshot {
  bodyBytes: Uint8Array;
  parts?: Part[];
  state?: string;
  stopReason?: string;
  haveBase: boolean;
}

export type ApplyResult = "ok" | "rebase" | "ignored";

const ENCODER = new TextEncoder();
const DECODER = new TextDecoder();

function utf8(s: string): Uint8Array {
  return ENCODER.encode(s);
}

function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

/** Decode a snapshot's accumulated bytes back to a string. */
export function snapshotBody(s: ReducedSnapshot): string {
  return DECODER.decode(s.bodyBytes);
}

export function emptySnapshot(): ReducedSnapshot {
  return { bodyBytes: new Uint8Array(0), haveBase: false };
}

/** Seed a snapshot from a full body (e.g. a REST rebase refetch). */
export function snapshotFromBody(
  body: string,
  parts?: Part[],
  state?: string,
  stopReason?: string,
): ReducedSnapshot {
  return { bodyBytes: utf8(body ?? ""), parts, state, stopReason, haveBase: true };
}

/** Decode the bytes `next` grew by relative to `prev` — i.e. the text
 * the latest frame appended — but ONLY when `next` is a true byte-prefix
 * extension of `prev`. If a snapshot replaced the body with a longer but
 * divergent string (non-prefix), there is no clean incremental delta and
 * this returns "". Mirrors the Go SDK's grewBy / HasPrefix rule so a
 * typewriter UI never prints garbage on a non-monotonic replace. */
export function appendedText(prev: Uint8Array, next: ReducedSnapshot): string {
  if (next.bodyBytes.length <= prev.length) return "";
  for (let i = 0; i < prev.length; i++) {
    if (next.bodyBytes[i] !== prev[i]) return "";
  }
  return DECODER.decode(next.bodyBytes.subarray(prev.length));
}

/**
 * Fold a single wire frame into the running snapshot. Pure: returns a
 * new snapshot and the outcome. Mirrors the Go reducer rule-for-rule.
 */
export function applyWireFrame(
  prev: ReducedSnapshot,
  f: WireFrame,
): { snapshot: ReducedSnapshot; result: ApplyResult } {
  const event = f.event ?? "";
  switch (event) {
    case WIRE_EVENT_DELTA: {
      const cur = prev.bodyBytes.length;
      const from = f.body_from ?? 0;
      const chunk = utf8(f.body_chunk ?? "");
      let bodyBytes = prev.bodyBytes;
      if (from === cur) {
        bodyBytes = concatBytes(prev.bodyBytes, chunk);
      } else if (from > cur) {
        return { snapshot: prev, result: "rebase" };
      } else {
        // Overlapping replay — append only the tail past the current end.
        const end = from + chunk.length;
        if (end > cur) {
          bodyBytes = concatBytes(prev.bodyBytes, chunk.subarray(cur - from));
        }
        // else fully contained → idempotent no-op for the body.
      }
      const next: ReducedSnapshot = {
        bodyBytes,
        parts: f.parts != null ? f.parts : prev.parts,
        state: f.state ? f.state : prev.state,
        stopReason: f.stop_reason ? f.stop_reason : prev.stopReason,
        haveBase: true,
      };
      return { snapshot: next, result: "ok" };
    }

    case WIRE_EVENT_CREATED:
    case WIRE_EVENT_UPDATED:
    case "": {
      const next: ReducedSnapshot = {
        bodyBytes: utf8(f.body ?? ""),
        parts: f.parts,
        state: f.state,
        stopReason: f.stop_reason,
        haveBase: true,
      };
      return { snapshot: next, result: "ok" };
    }

    default:
      return { snapshot: prev, result: "ignored" };
  }
}
