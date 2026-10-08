import { describe, expect, it } from "vitest";
import { parseConversationReasoning } from "../src/facade/reasoning.js";

describe("durable reasoning projection", () => {
  it("distinguishes missing old-server fields from an explicit reset", () => {
    expect(parseConversationReasoning(undefined)).toBeUndefined();
    expect(parseConversationReasoning(null)).toBeNull();
    expect(parseConversationReasoning({ modelId: "p/m", reasoningOverrideId: null,
      effectiveReasoningId: "low", defaultReasoningId: "low" })?.reasoningOverrideId).toBeNull();
  });
  it("fails closed for missing, malformed or inconsistent receipts", () => {
    for (const value of [{ modelId: "m" }, { modelId: "m", reasoningOverrideId: "high",
      effectiveReasoningId: "low", defaultReasoningId: null }]) {
      expect(() => parseConversationReasoning(value)).toThrow();
    }
  });
});
