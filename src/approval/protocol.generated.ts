/** Provider-neutral approval protocol. Matchers and execution grants are host-private. */
export const APPROVAL_PROTOCOL = "beeos.approval.v1" as const;
/** Reserved settings method; advertise only after its durable implementation ships. */
export const APPROVAL_SETTINGS_RUNTIME_METHOD = "session/set_approval_settings" as const;
export const APPROVAL_SETTINGS_CANONICAL_COMMAND = "session.setApprovalSettings" as const;
export const APPROVAL_OPERATION_TYPES = ["process.exec", "filesystem.mutate", "network.request",
  "tool.invoke", "external.mutation", "privilege.change", "sensitive_data.read"] as const;
export const APPROVAL_POLICY_MODES = ["deny", "allowlist", "ask", "auto_review", "full"] as const;
export type ApprovalOperationType = typeof APPROVAL_OPERATION_TYPES[number];
export type ApprovalPolicyMode = typeof APPROVAL_POLICY_MODES[number];
export type ApprovalReviewer = "user" | "auto_review";
export type ApprovalDecision = "approve_once" | "deny" | "cancel";
export type ApprovalAction = ApprovalDecision | "allow_always";
export type ApprovalJson = null | boolean | number | string | ApprovalJson[] | { [key: string]: ApprovalJson };
export interface ApprovalSettings {
  defaultMode: ApprovalPolicyMode;
  operationModes: Partial<Record<ApprovalOperationType, ApprovalPolicyMode>>;
  reviewer: ApprovalReviewer;
}
/** PUT replaces this entire override; omitted members inherit the immutable baseline. */
export interface ApprovalSettingsOverride {
  defaultMode?: ApprovalPolicyMode;
  operationModes?: Partial<Record<ApprovalOperationType, ApprovalPolicyMode>>;
  reviewer?: ApprovalReviewer;
}
export interface ApprovalBaseline {
  source: "global_snapshot" | "system_default" | "legacy_migration";
  globalSettingsRevision: number;
  globalRulesRevision: number;
  copiedAt: string;
  approvalSettings: ApprovalSettings;
}
export interface ApprovalRuleSummary {
  ruleId: string;
  scope: "global" | "session";
  effect: "allow";
  operationType: ApprovalOperationType;
  origin: { type: "allow_always" | "promoted_session_rule" | "global_snapshot"; sourceRuleId?: string; requestId?: string };
  matchSummary: { [key: string]: ApprovalJson };
}
interface Protocol { protocol: "beeos.approval.v1" }
interface Correlation extends Protocol {
  requestId: string;
  requestRevision: number;
  sessionId: string;
  taskId: string;
}
export interface ApprovalRequest extends Correlation {
  kind: "approval_request";
  trigger: { type: "rule_miss" | "auto_review_escalated" | "policy_required"; reasonCode: string };
  operation: { type: ApprovalOperationType; summary: string; parameters: { [key: string]: ApprovalJson } };
  availableActions: ApprovalAction[];
  createdAt: string;
  expiresAt: string;
}
export interface ApprovalDecisionAction extends Correlation {
  kind: "approval_decision";
  actionId: string;
  decision: ApprovalDecision;
}
export interface ApprovalAllowAlwaysAction extends Correlation {
  kind: "approval_allow_always";
  actionId: string;
  scope: "session";
}
export type ApprovalUserAction = ApprovalDecisionAction | ApprovalAllowAlwaysAction;
export type ApprovalResolutionStatus = "approved" | "denied" | "cancelled" | "expired" |
  "withdrawn" | "task_cancelled" | "superseded" | "invalidated";
export interface ApprovalResolved extends Protocol {
  kind: "approval_resolved";
  requestId: string;
  sessionId: string;
  taskId: string;
  resolution: { status: ApprovalResolutionStatus; decision?: ApprovalDecision;
    responseAction?: ApprovalAction; source: "user" | "auto_review" | "system"; reasonCode: string };
  acceptedActionId?: string;
  /** Rule creation is a separate outcome: one-shot approval survives rule persistence failure. */
  ruleChange?: { status: "applied" | "failed"; ruleId?: string; reasonCode?: string };
  resolvedAt: string;
}
export interface GlobalApprovalSettingsSet extends Protocol {
  kind: "global_approval_settings_set";
  expectedSettingsRevision: number;
  approvalSettings: ApprovalSettings;
}
export interface SessionApprovalSettingsSet extends Protocol {
  kind: "session_approval_settings_set";
  expectedSettingsRevision: number;
  approvalSettingsOverride: ApprovalSettingsOverride | null;
}
export interface ApprovalRulePromote extends Protocol {
  kind: "approval_rule_promote";
  sessionId: string;
  ruleId: string;
  expectedGlobalRulesRevision: number;
  expectedSessionRulesRevision: number;
  actionId: string;
}
export interface ApprovalCapabilities extends Protocol {
  kind: "approval_capabilities";
  supportedActions: ApprovalAction[];
  policyModes: ApprovalPolicyMode[];
  runtimeMethods: string[];
  features: { globalSettingsGet: boolean; globalSettingsSet: boolean; sessionSettingsGet: boolean;
    sessionSettingsSet: boolean; sessionSettingsReset: boolean; rulePromotion: boolean; singlePendingPerTask: boolean };
}
export interface GlobalApprovalSettings extends Protocol {
  kind: "global_approval_settings";
  scope: "global";
  settingsRevision: number;
  rulesRevision: number;
  approvalSettings: ApprovalSettings;
  rules: ApprovalRuleSummary[];
  appliesTo: "future_sessions_only";
  existingSessionsAffected: false;
  updatedAt: string;
}
export interface ApprovalGuardrailEffects {
  tenantGuardrailRevision: number;
  instanceGuardrailRevision: number;
  forcedModes: Partial<Record<ApprovalOperationType, ApprovalPolicyMode>>;
  disabledActions: { operationType: ApprovalOperationType; action: ApprovalAction }[];
}
export interface SessionApprovalSettings extends Protocol {
  kind: "session_approval_settings";
  scope: "session";
  sessionId: string;
  settingsRevision: number;
  rulesRevision: number;
  baseline: ApprovalBaseline;
  approvalSettingsOverride: ApprovalSettingsOverride | null;
  rules: ApprovalRuleSummary[];
  effectiveApprovalSettings: ApprovalSettings;
  guardrailEffects: ApprovalGuardrailEffects;
  effectivePolicyRevision: { settingsRevision: number; rulesRevision: number;
    tenantGuardrailRevision: number; instanceGuardrailRevision: number };
  projection: { state: "applied" | "pending" | "blocked"; pendingOperationId: string | null };
  updatedAt: string;
}
export interface SessionApprovalSettingsChanged extends Protocol {
  kind: "session_approval_settings_changed";
  operationId: string;
  sessionId: string;
  status: "applied";
  settingsRevision: number;
  rulesRevision: number;
  approvalSettingsOverride: ApprovalSettingsOverride | null;
  effectiveApprovalSettings: ApprovalSettings;
  appliedAt: string;
}
export interface SessionApprovalSettingsFailed extends Protocol {
  kind: "session_approval_settings_changed";
  operationId: string;
  sessionId: string;
  status: "failed";
  settingsRevision: number;
  reasonCode: string;
  message: string;
  retryable: boolean;
  failedAt: string;
}
export interface GlobalApprovalSettingsChanged extends Protocol {
  kind: "global_approval_settings_changed";
  settingsRevision: number;
  rulesRevision: number;
  appliesTo: "future_sessions_only";
  existingSessionsAffected: false;
  changedAt: string;
}
export type ApprovalWireDocument = ApprovalRequest | ApprovalUserAction | ApprovalResolved |
  GlobalApprovalSettingsSet | SessionApprovalSettingsSet | ApprovalRulePromote | ApprovalCapabilities |
  GlobalApprovalSettings | SessionApprovalSettings | SessionApprovalSettingsChanged | SessionApprovalSettingsFailed | GlobalApprovalSettingsChanged;

export const APPROVAL_ERROR_CODES = ["INVALID_APPROVAL_ACTION", "APPROVAL_NOT_FOUND", "APPROVAL_ALREADY_RESOLVED",
  "APPROVAL_EXPIRED", "APPROVAL_REQUEST_MISMATCH", "APPROVAL_ACTION_CONFLICT", "APPROVAL_UNAVAILABLE",
  "APPROVAL_SETTINGS_UNAVAILABLE", "INVALID_APPROVAL_SETTINGS", "REVISION_CONFLICT", "IDEMPOTENCY_CONFLICT",
  "SETTINGS_UPDATE_IN_PROGRESS", "SETTINGS_PROJECTION_PENDING", "SETTINGS_PROJECTION_BLOCKED",
  "SESSION_BASELINE_NOT_FOUND", "RUNTIME_METHOD_NOT_SUPPORTED", "RUNTIME_EXECUTION_FAILED",
  "GUARDRAIL_CONFLICT", "UNKNOWN_OPERATION_TYPE", "UNKNOWN_POLICY_MODE", "RULE_SCOPE_FORBIDDEN"] as const;

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}
function text(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 128 && /^[A-Za-z0-9_.:-]+$/u.test(value);
}
function revision(value: unknown): value is number { return Number.isSafeInteger(value) && (value as number) >= 0; }
function exact(value: Record<string, unknown>, keys: readonly string[]): boolean {
  return Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
}
/** Only these two explicit messages can authorize a pending request. No free-text coercion. */
export function parseApprovalUserAction(value: unknown): ApprovalUserAction | null {
  if (!record(value) || value.protocol !== APPROVAL_PROTOCOL || !text(value.requestId) ||
      !revision(value.requestRevision) || value.requestRevision < 1 || !text(value.sessionId) ||
      !text(value.taskId) || !text(value.actionId)) return null;
  const common = ["protocol", "kind", "requestId", "requestRevision", "sessionId", "taskId", "actionId"];
  if (value.kind === "approval_decision" && exact(value, [...common, "decision"]) &&
      ["approve_once", "deny", "cancel"].includes(value.decision as string)) return structuredClone(value) as unknown as ApprovalDecisionAction;
  if (value.kind === "approval_allow_always" && exact(value, [...common, "scope"]) && value.scope === "session")
    return structuredClone(value) as unknown as ApprovalAllowAlwaysAction;
  return null;
}
export function parseApprovalSettings(value: unknown, partial = false): ApprovalSettings | ApprovalSettingsOverride | null {
  if (!record(value) || Object.keys(value).some(key => !["defaultMode", "operationModes", "reviewer"].includes(key))) return null;
  if (!partial && !exact(value, ["defaultMode", "operationModes", "reviewer"])) return null;
  if (Object.hasOwn(value, "defaultMode") && !APPROVAL_POLICY_MODES.includes(value.defaultMode as ApprovalPolicyMode)) return null;
  if (Object.hasOwn(value, "reviewer") && !["user", "auto_review"].includes(value.reviewer as string)) return null;
  if (Object.hasOwn(value, "operationModes") && (!record(value.operationModes) ||
      Object.entries(value.operationModes).some(([key, mode]) => !APPROVAL_OPERATION_TYPES.includes(key as ApprovalOperationType) ||
        !APPROVAL_POLICY_MODES.includes(mode as ApprovalPolicyMode)))) return null;
  return structuredClone(value) as ApprovalSettings | ApprovalSettingsOverride;
}
