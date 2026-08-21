import { describe, expect, it } from "vitest";

import {
  RUNTIME_ERROR_PART_KIND,
  createRuntimeErrorPart,
  parseRuntimeErrorPart,
} from "../src/protocol/index.js";

describe("runtime_error custom part", () => {
  it("creates the wire shape Web will read", () => {
    expect(createRuntimeErrorPart("insufficient_credits")).toEqual({
      type: "custom",
      kind: RUNTIME_ERROR_PART_KIND,
      data: { code: "insufficient_credits" },
    });
  });

  it("does not embed HTTP status, provider codes, or secrets", () => {
    const json = JSON.stringify(createRuntimeErrorPart("insufficient_credits"));
    expect(json).not.toMatch(/402|403|insufficient_quota|api[_ ]?key|stack/i);
  });

  it("parses only custom/runtime_error with a known code", () => {
    expect(parseRuntimeErrorPart(createRuntimeErrorPart("insufficient_credits"))).toEqual({
      type: "custom",
      kind: "runtime_error",
      data: { code: "insufficient_credits" },
    });
    expect(parseRuntimeErrorPart({
      type: "custom",
      kind: "tool_progress",
      data: { code: "insufficient_credits" },
    })).toBeUndefined();
    expect(parseRuntimeErrorPart({
      type: "custom",
      kind: "runtime_error",
      data: { code: "provider_error" },
    })).toBeUndefined();
    expect(parseRuntimeErrorPart({
      type: "source",
      url: "https://example.com",
    })).toBeUndefined();
  });

  it("ignores additive data fields while keeping the known code", () => {
    expect(parseRuntimeErrorPart({
      type: "custom",
      kind: "runtime_error",
      data: { code: "insufficient_credits", extra: "ignored" },
    })).toEqual(createRuntimeErrorPart("insufficient_credits"));
  });
});
