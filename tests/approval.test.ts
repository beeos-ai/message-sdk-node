import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { parseApprovalUserAction } from "../src/approval/index.js";
import { createGatewayMessageClientComposition } from "../src/gateway-runtime.js";

const vectors = JSON.parse(readFileSync(new URL("./testdata/approval.v1.vectors.json", import.meta.url), "utf8"));
describe("approval action wire and authenticated route", () => {
  for (const vector of vectors.actions) it(vector.name, () => expect(Boolean(parseApprovalUserAction(vector.value))).toBe(vector.valid));
  it("submits a control action without a chat prompt or optimistic authorization", async () => {
    const action = vectors.actions[0].value;
    const fetch = vi.fn(async (_url, init) => new Response(JSON.stringify({ success: true, data: {
      status: "accepted", requestId: action.requestId, actionId: action.actionId, messageId: action.actionId,
    } }), { status: 202, headers: { "content-type": "application/json" } }));
    const composition = createGatewayMessageClientComposition({ gatewayUrl: "https://gateway.example", platform: "web",
      currentPrincipal: { currentPrincipalId: () => "user:owner" }, fetch: fetch as typeof globalThis.fetch });
    const receipt = await composition.messageCommand.sendMessage({ agentId: "agent_1", conversationId: action.sessionId,
      clientMessageId: action.actionId, idempotencyKey: action.actionId, type: "user.continue", content: { payload: action } });
    expect(receipt).toEqual({ messageId: action.actionId, outcome: "accepted" });
    expect(fetch.mock.calls[0]![0]).toBe("https://gateway.example/api/v1/agents/agent_1/conversations/conv_1/approvals/apr_1/actions");
    expect(JSON.parse(fetch.mock.calls[0]![1].body)).toEqual(action);
    await expect(composition.messageCommand.sendMessage({ agentId: "agent_1", conversationId: action.sessionId,
      clientMessageId: action.actionId, idempotencyKey: action.actionId, type: "user.continue", content: { payload: "yes" } }))
      .rejects.toThrow("INVALID_APPROVAL_ACTION");
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
