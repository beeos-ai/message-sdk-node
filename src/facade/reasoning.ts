export type ConversationReasoning = {
 modelId: string;
 reasoningOverrideId: string | null;
 effectiveReasoningId: string | null;
 defaultReasoningId: string | null;
};

/** Unknown old-server state is omitted; explicit null clears a model-bound override. */
export function parseConversationReasoning(raw: unknown): ConversationReasoning | null | undefined {
 if (raw === undefined || raw === null) return raw;
 if (typeof raw !== "object" || Array.isArray(raw)) throw new Error("Invalid conversation reasoning");
 const value = raw as Record<string, unknown>;
 if (typeof value.modelId !== "string" || !value.modelId || value.modelId.length > 256 || /\s/.test(value.modelId)) throw new Error("Invalid reasoning model");
 const fields = ["reasoningOverrideId", "effectiveReasoningId", "defaultReasoningId"] as const;
 for (const key of fields) { if (value[key] !== null && (typeof value[key] !== "string" || !(value[key] as string) || (value[key] as string).length > 64 || /\s/.test(value[key] as string))) throw new Error("Invalid reasoning level"); }
 if (value.effectiveReasoningId !== (value.reasoningOverrideId ?? value.defaultReasoningId)) throw new Error("Inconsistent reasoning receipt");
 return {modelId:value.modelId,reasoningOverrideId:value.reasoningOverrideId as string|null,effectiveReasoningId:value.effectiveReasoningId as string|null,defaultReasoningId:value.defaultReasoningId as string|null};
}
