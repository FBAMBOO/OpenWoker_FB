import type {
  AgentProfileDetail,
  AgentProfileSpec,
  AgentProfileSummary,
  AgentProfileVersion,
  ContextRef,
  ContextRefInput,
  AgentRun,
  AuditPage,
  AttentionGate,
  CreateOrchestrationTask,
  GateVerificationReport,
  ModelPolicyDraft,
  ModelPolicyDetail,
  ModelPolicySummary,
  ModelPolicyVersion,
  ModelRoutingPolicySpec,
  OrchestrationTaskDetail,
  OrchestrationHealth,
  HandoffRuntimeSettings,
  OrchestrationTaskSummary,
  OutboxDeadLetterPage,
  ResultQuestion,
  RoutingModelDescriptor,
  RuntimePresetDescriptor,
  RuntimePresetRoleAssignment,
  RoutingSimulationFacts,
  RoutingSimulationResult,
  RunActivityPage,
  RunTranscript,
  SubscriptionRuntimeDescriptor,
  SubscriptionRuntimeHealth,
  TaskActivity,
  TaskBrief,
  TaskBriefInput,
  TaskComment,
  TaskCommentDelta,
  TaskEvidence,
  TaskEvidencePage,
  TaskGatesPage,
  TaskNode,
  TaskRunsPage,
  TaskStageState,
  TaskRelation,
  TaskDraftAnalysisV2,
  TaskQualityArchetype,
  TaskQualityArtifactStatus,
  TaskQualityBudgetMode,
  TaskQualityBudgetStatus,
  TaskQualityContract,
  TaskQualityStatus,
  TaskQualityWorkflowStatus,
  TaskQualityBenchmarkComparison,
  TaskQualityBenchmarkRun,
  TaskQualityBenchmarkSuite,
  RepositorySnapshotV2,
  ExecutionStrategyV2,
  QualityBundleV2,
  VersionProvenance,
  WorkStatus,
  ValidationReport,
  WakeRequest,
  WorkProduct,
} from "./types";

export type ApiRequest = <T>(path: string, init?: RequestInit) => Promise<T>;
export type ApiDownload = (path: string, filename?: string) => Promise<void>;

export interface TaskListOptions {
  statuses?: Array<OrchestrationTaskSummary["status"]>;
  workflowStatuses?: TaskQualityWorkflowStatus[];
  qualityStatuses?: TaskQualityStatus[];
  artifactStatuses?: TaskQualityArtifactStatus[];
  budgetStatuses?: TaskQualityBudgetStatus[];
  archetypes?: TaskQualityArchetype[];
  repo?: string;
  snapshotRef?: string;
  budgetModes?: TaskQualityBudgetMode[];
  hasWaiver?: boolean;
  repairCount?: number;
  createdBy?: string;
  limit?: number;
  offset?: number;
}

export interface CreateTaskQualityDraft {
  title?: string;
  objective: string;
  domain: "code" | "knowledge";
  workspace?: string;
  read_only: boolean;
  source_workspace_write: boolean;
  task_artifact_write: true;
  network: boolean;
  quality_profile_id: "balanced" | "quality-first" | "custom" | string;
  input?: Record<string, unknown>;
}

export interface DeadLetterRequeueCommand {
  actor: string;
  reason: string;
  idempotencyKey: string;
}

let fallbackIdempotencySequence = 0;

/** Create an opaque command key; callers retain it for the lifetime of one UI intent. */
export function createClientIdempotencyKey(scope: string): string {
  const normalizedScope = scope.toLowerCase().replace(/[^a-z0-9_-]+/g, "-").replace(/^-|-$/g, "") || "command";
  const random = globalThis.crypto?.randomUUID?.()
    || `${Date.now().toString(36)}-${(++fallbackIdempotencySequence).toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
  return `gui:${normalizedScope}:${random}`;
}

const jsonRequest = (method: string, body?: unknown, headers?: HeadersInit): RequestInit => ({
  method,
  headers: { "Content-Type": "application/json", ...headers },
  ...(body === undefined ? {} : { body: JSON.stringify(body) }),
});

function listFrom<T>(value: T[] | Record<string, unknown>, key: string): T[] {
  if (Array.isArray(value)) return value;
  const nested = value[key];
  return Array.isArray(nested) ? (nested as T[]) : [];
}

type JsonRecord = Record<string, unknown>;
const record = (value: unknown): JsonRecord => value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};
const array = (value: unknown): unknown[] => Array.isArray(value) ? value : [];
const text = (value: unknown, fallback = "") => value == null ? fallback : String(value);
const number = (value: unknown, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};
const lower = (value: unknown, fallback = "") => text(value, fallback).toLowerCase();
const stringList = (value: unknown): string[] => array(value).map((item) => text(item).trim()).filter(Boolean);
const acceptanceCriteria = (value: unknown): Record<string, string> => Object.fromEntries(
  Object.entries(record(value)).map(([criterion, status]) => [criterion, lower(status, "unknown")]),
);

function normalizedStatus(value: unknown, fallback: WorkStatus = "pending"): WorkStatus {
  const status = lower(value);
  return (status || fallback) as WorkStatus;
}

function normalizeRun(value: unknown, node?: JsonRecord): AgentRun {
  const item = record(value);
  const output = record(item.output);
  const budget = record(item.budget);
  const usage = record(item.usage || output.usage);
  const errorMessage = text(item.error_message || item.error || output.error_message || output.error) || undefined;
  return {
    id: text(item.id || item.run_id),
    node_id: text(item.node_id || item.node_key) || undefined,
    parent_run_id: text(item.parent_run_id) || null,
    title: text(item.title || node?.title || item.node_key || node?.key, "Agent run"),
    agent_name: text(item.agent_name || item.agent || node?.agent) || undefined,
    profile_version: item.profile_version == null ? undefined : number(item.profile_version),
    role: text(item.role) || undefined,
    status: normalizedStatus(item.status),
    model_id: text(item.model_id || item.model || node?.model) || undefined,
    routing_reason: text(item.routing_reason || record(item.routing_decision).reason) || undefined,
    attempt: item.attempt == null ? undefined : number(item.attempt),
    started_at: text(item.started_at) || undefined,
    completed_at: text(item.completed_at || item.finished_at) || undefined,
    summary: text(item.summary || output.summary || errorMessage) || undefined,
    error_kind: text(item.error_kind || output.error_kind) || undefined,
    error_message: errorMessage,
    session_id: text(item.session_id) || null,
    usage: Object.keys(usage).length ? {
      model_calls: number(usage.model_calls),
      tool_calls: number(usage.tool_calls),
      tokens: number(usage.tokens),
      wall_seconds: number(usage.wall_seconds),
    } : undefined,
    budget: Object.keys(budget).length ? {
      model_calls: number(budget.model_calls),
      tool_calls: number(budget.tool_calls),
      tokens: number(budget.tokens),
      wall_seconds: number(budget.wall_seconds),
    } : null,
  };
}

function jsonRecord(value: unknown): JsonRecord {
  const direct = record(value);
  if (Object.keys(direct).length) return direct;
  if (typeof value !== "string" || value.length > 64_000) return {};
  try {
    return record(JSON.parse(value));
  } catch {
    return {};
  }
}

function normalizeAuditPage(value: unknown): AuditPage | undefined {
  const item = record(value);
  if (!Object.keys(item).length) return undefined;
  return {
    has_more: Boolean(item.has_more),
    page_size: item.page_size == null ? undefined : number(item.page_size),
    offset: item.offset == null ? undefined : number(item.offset),
    limit: item.limit == null ? undefined : number(item.limit),
    next_offset: item.next_offset == null ? null : number(item.next_offset),
    cursor: text(item.cursor) || null,
    next_cursor: text(item.next_cursor) || null,
    pagination: item.pagination === "cursor" ? "cursor" : item.pagination === "offset" ? "offset" : undefined,
    order: text(item.order) || undefined,
  };
}

function normalizeHealth(value: unknown): OrchestrationHealth {
  const item = record(value);
  const leader = record(item.leader);
  const outbox = record(item.outbox);
  const handoff = record(item.handoff);
  const handoffSettings = record(handoff.settings);
  const handoffMetrics = record(handoff.metrics);
  const taskQuality = record(item.task_quality);
  return {
    ready: Boolean(item.ready),
    state: text(item.state, "unknown"),
    loop_alive: Boolean(item.loop_alive),
    leader: Object.keys(leader).length ? {
      held: Boolean(leader.held),
      epoch: leader.epoch == null ? null : number(leader.epoch),
      heartbeat_alive: Boolean(leader.heartbeat_alive),
      last_heartbeat_at: text(leader.last_heartbeat_at) || null,
    } : undefined,
    outbox: Object.keys(outbox).length ? {
      loop_alive: Boolean(outbox.loop_alive),
      last_success_at: text(outbox.last_success_at) || null,
      last_error: text(outbox.last_error) || null,
      pending: number(outbox.pending),
      dead_letters: number(outbox.dead_letters),
      oldest_pending_at: text(outbox.oldest_pending_at) || null,
      stale: Boolean(outbox.stale),
      stale_after_seconds: outbox.stale_after_seconds == null ? undefined : number(outbox.stale_after_seconds),
    } : undefined,
    closing: item.closing == null ? undefined : Boolean(item.closing),
    started_at: text(item.started_at) || null,
    last_tick_started_at: text(item.last_tick_started_at) || null,
    last_success_at: text(item.last_success_at) || null,
    last_error_at: text(item.last_error_at) || null,
    last_error: text(item.last_error) || null,
    consecutive_failures: item.consecutive_failures == null ? undefined : number(item.consecutive_failures),
    handoff: Object.keys(handoff).length ? {
      settings: normalizeHandoffSettings(handoffSettings),
      metrics: {
        counters: Object.fromEntries(
          Object.entries(record(handoffMetrics.counters)).map(([key, value]) => [key, number(value)]),
        ),
        gauges: Object.fromEntries(
          Object.entries(record(handoffMetrics.gauges)).map(([key, value]) => [key, number(value)]),
        ),
        histograms: record(handoffMetrics.histograms),
      },
    } : undefined,
    task_quality: Object.keys(taskQuality).length ? {
      schema_version: number(taskQuality.schema_version, 1),
      metrics: record(taskQuality.metrics),
      series: record(taskQuality.series),
      alerts: array(taskQuality.alerts).map((raw) => {
        const alert = record(raw);
        return {
          code: text(alert.code),
          severity: text(alert.severity),
          observed: number(alert.observed),
          message: text(alert.message),
        };
      }),
      privacy: text(taskQuality.privacy, "content_free_metadata_only"),
    } : undefined,
  };
}

function normalizeHandoffSettings(value: unknown): HandoffRuntimeSettings {
  const item = record(value);
  return {
    structured_handoff_enabled: item.structured_handoff_enabled == null ? true : Boolean(item.structured_handoff_enabled),
    structured_handoff_required_for_new_tasks: Boolean(item.structured_handoff_required_for_new_tasks),
    legacy_spawn_agent_enabled: item.legacy_spawn_agent_enabled == null ? true : Boolean(item.legacy_spawn_agent_enabled),
    default_context_token_budget: number(item.default_context_token_budget, 8_000),
    max_context_refs: number(item.max_context_refs, 50),
    max_inline_bytes_per_ref: number(item.max_inline_bytes_per_ref, 8_192),
    max_inline_bytes_total: number(item.max_inline_bytes_total, 32_768),
    max_comment_batch: number(item.max_comment_batch, 100),
    wake_coalesce_window_ms: number(item.wake_coalesce_window_ms, 1_000),
    wake_max_attempts: number(item.wake_max_attempts, 5),
    wake_backoff_seconds: number(item.wake_backoff_seconds, 1),
    context_read_audit_enabled: item.context_read_audit_enabled == null ? true : Boolean(item.context_read_audit_enabled),
    transcript_sharing_default: Boolean(item.transcript_sharing_default),
  };
}

function normalizeTaskSummary(value: unknown): OrchestrationTaskSummary {
  const item = record(value);
  const gates = array(item.gates || item.attention || item.attention_gates);
  const primary = record(item.primary_deliverable);
  const verdict = record(item.quality_verdict);
  const budget = record(item.effective_budget);
  const target = record(item.target);
  const runSummary = record(item.run_summary);
  return {
    id: text(item.id || item.task_id),
    title: text(item.title, "Untitled task"),
    objective: text(item.objective) || undefined,
    status: lower(item.status, "draft") as OrchestrationTaskSummary["status"],
    stage: lower(item.stage || item.current_stage, "intake"),
    progress: item.progress == null ? undefined : number(item.progress),
    attention_count: item.attention_count == null
      ? gates.filter((gate) => ["open", "pending"].includes(lower(record(gate).status))).length
      : number(item.attention_count),
    updated_at: text(item.updated_at || item.created_at),
    created_at: text(item.created_at) || undefined,
    profile_name: text(item.profile_name) || undefined,
    profile_version: item.profile_version == null ? undefined : number(item.profile_version),
    parent_task_id: text(item.parent_task_id) || null,
    parent_run_id: text(item.parent_run_id) || null,
    terminal_outcome: text(item.terminal_outcome) as OrchestrationTaskSummary["terminal_outcome"] || undefined,
    task_quality_v2: item.task_quality_v2 == null ? undefined : Boolean(item.task_quality_v2),
    workflow_status: text(item.workflow_status) as OrchestrationTaskSummary["workflow_status"] || undefined,
    workflow_resume_status: text(item.workflow_resume_status) as OrchestrationTaskSummary["workflow_resume_status"] || null,
    quality_status: text(item.quality_status) as OrchestrationTaskSummary["quality_status"] || undefined,
    artifact_status: text(item.artifact_status) as OrchestrationTaskSummary["artifact_status"] || undefined,
    budget_status: text(item.budget_status) as OrchestrationTaskSummary["budget_status"] || undefined,
    quality_reason_code: text(item.quality_reason_code) || null,
    attention_reason: text(item.attention_reason || item.quality_reason_code) || null,
    archetype: text(item.archetype) as OrchestrationTaskSummary["archetype"] || null,
    primary_deliverable: Object.keys(primary).length ? primary as unknown as OrchestrationTaskSummary["primary_deliverable"] : null,
    quality_verdict: Object.keys(verdict).length ? verdict as unknown as OrchestrationTaskSummary["quality_verdict"] : null,
    quality_score: item.quality_score == null && verdict.total_score == null ? null : number(item.quality_score ?? verdict.total_score),
    hard_gate_status: text(item.hard_gate_status) || undefined,
    effective_budget: Object.keys(budget).length ? budget as unknown as OrchestrationTaskSummary["effective_budget"] : undefined,
    budget_utilization_percent: item.budget_utilization_percent == null ? null : number(item.budget_utilization_percent),
    target: Object.keys(target).length ? target as unknown as OrchestrationTaskSummary["target"] : null,
    has_waiver: item.has_waiver == null ? undefined : Boolean(item.has_waiver),
    run_summary: Object.keys(runSummary).length ? {
      nodes: number(runSummary.nodes),
      repairs: number(runSummary.repairs),
    } : undefined,
    created_by: text(item.created_by) || null,
    started_at: text(item.started_at) || null,
  };
}

function normalizeBrief(value: unknown): TaskBrief {
  const item = record(value);
  return {
    id: text(item.id),
    task_id: text(item.task_id),
    revision: number(item.revision),
    status: lower(item.status, "draft") as TaskBrief["status"],
    title: text(item.title),
    objective: text(item.objective),
    background: text(item.background),
    scope: record(item.scope),
    instructions: stringList(item.instructions),
    constraints: stringList(item.constraints),
    non_goals: stringList(item.non_goals),
    acceptance_criteria: array(item.acceptance_criteria).map((raw, index) => {
      const criterion = record(raw);
      return {
        id: text(criterion.id, `criterion-${index + 1}`),
        text: text(criterion.text || criterion.description),
        required: criterion.required == null ? true : Boolean(criterion.required),
        verification: text(criterion.verification) || undefined,
      };
    }),
    deliverables: array(item.deliverables).map((raw, index) => {
      const deliverable = record(raw);
      return {
        id: text(deliverable.id, `deliverable-${index + 1}`),
        kind: text(deliverable.kind, "other"),
        title: text(deliverable.title) || undefined,
        required: deliverable.required == null ? true : Boolean(deliverable.required),
      };
    }),
    result_contract: record(item.result_contract),
    content_hash: text(item.content_hash),
    created_by_task_id: text(item.created_by_task_id) || null,
    created_by_run_id: text(item.created_by_run_id) || null,
    created_at: text(item.created_at),
    published_at: text(item.published_at) || null,
  };
}

function normalizeContextRef(value: unknown): ContextRef {
  const item = record(value);
  return {
    id: text(item.id),
    task_id: text(item.task_id),
    brief_id: text(item.brief_id),
    requirement: lower(item.requirement, "optional") as ContextRef["requirement"],
    ref_type: lower(item.ref_type),
    display_name: text(item.display_name, "Context reference"),
    selection_reason: text(item.selection_reason),
    locator: record(item.locator),
    delivery_mode: lower(item.delivery_mode, "on_demand") as ContextRef["delivery_mode"],
    summary: text(item.summary),
    mime_type: text(item.mime_type) || null,
    content_hash: text(item.content_hash) || null,
    byte_size: item.byte_size == null ? null : number(item.byte_size),
    token_estimate: item.token_estimate == null ? null : number(item.token_estimate),
    provenance: record(item.provenance),
    trust_level: text(item.trust_level, "untrusted"),
    created_at: text(item.created_at),
    read_count: number(item.read_count),
    last_read_at: text(item.last_read_at) || null,
    last_read_by_run_id: text(item.last_read_by_run_id) || null,
  };
}

function normalizeRelation(value: unknown): TaskRelation {
  const item = record(value);
  return {
    id: text(item.id),
    from_task_id: text(item.from_task_id),
    to_task_id: text(item.to_task_id),
    relation_type: lower(item.relation_type, "related"),
    metadata: record(item.metadata),
    created_at: text(item.created_at),
    removed_at: text(item.removed_at) || null,
  };
}

function normalizeComment(value: unknown): TaskComment {
  const item = record(value);
  return {
    id: text(item.id),
    task_id: text(item.task_id),
    sequence: number(item.sequence),
    author_type: text(item.author_type),
    author_id: text(item.author_id),
    created_by_run_id: text(item.created_by_run_id) || null,
    body_markdown: text(item.body_markdown),
    metadata: record(item.metadata),
    reply_to_comment_id: text(item.reply_to_comment_id) || null,
    created_at: text(item.created_at),
  };
}

function normalizeWorkProduct(value: unknown): WorkProduct {
  const item = record(value);
  return {
    id: text(item.id),
    task_id: text(item.task_id),
    run_id: text(item.run_id) || null,
    kind: text(item.kind, "other"),
    title: text(item.title, "Work product"),
    summary: text(item.summary),
    evidence_id: text(item.evidence_id) || null,
    artifact_id: text(item.artifact_id) || null,
    uri: text(item.uri) || null,
    content_hash: text(item.content_hash) || null,
    metadata: record(item.metadata),
    verification_status: text(item.verification_status, "unverified"),
    created_by: text(item.created_by),
    created_at: text(item.created_at),
  };
}

function normalizeResultQuestion(value: unknown): ResultQuestion {
  const item = record(value);
  return {
    id: text(item.id || item.task_id),
    task_id: text(item.task_id || item.id),
    source_task_id: text(item.source_task_id),
    question: text(item.question),
    status: lower(item.status, "draft") as ResultQuestion["status"],
    terminal_outcome: text(item.terminal_outcome) as ResultQuestion["terminal_outcome"] || undefined,
    stage: lower(item.stage, "intake"),
    progress: item.progress == null ? undefined : number(item.progress),
    answer: item.answer == null ? null : text(item.answer),
    answer_work_product_id: text(item.answer_work_product_id) || null,
    answer_artifact_id: text(item.answer_artifact_id) || null,
    source_work_product_ids: stringList(item.source_work_product_ids),
    created_at: text(item.created_at),
    updated_at: text(item.updated_at),
  };
}

function normalizeWake(value: unknown): WakeRequest {
  const item = record(value);
  return {
    id: text(item.id),
    target_task_id: text(item.target_task_id),
    target_run_id: text(item.target_run_id) || null,
    reason: text(item.reason),
    source_task_id: text(item.source_task_id) || null,
    source_run_id: text(item.source_run_id) || null,
    source_event_id: text(item.source_event_id) || null,
    payload: record(item.payload),
    status: lower(item.status, "pending"),
    attempts: number(item.attempts),
    coalesced_count: number(item.coalesced_count),
    last_error: text(item.last_error) || null,
    not_before: text(item.not_before) || null,
    claimed_by: text(item.claimed_by) || null,
    claimed_until: text(item.claimed_until) || null,
    delivered_at: text(item.delivered_at) || null,
    completed_at: text(item.completed_at) || null,
    created_at: text(item.created_at),
    updated_at: text(item.updated_at),
  };
}

function normalizeStage(value: unknown): TaskStageState {
  const item = record(value);
  const disposition = lower(item.status || item.disposition, "pending");
  const status: WorkStatus = disposition === "active"
    ? "running"
    : disposition === "request_changes"
      ? "waiting"
      : disposition === "canceled"
        ? "canceled"
        : normalizedStatus(disposition);
  return {
    stage: lower(item.stage),
    status,
    sequence: item.sequence == null ? undefined : number(item.sequence),
    attempt: item.attempt == null ? undefined : number(item.attempt),
    started_at: text(item.started_at || item.entered_at) || undefined,
    completed_at: text(item.completed_at || item.exited_at) || undefined,
  };
}

function normalizeGate(value: unknown): AttentionGate {
  const item = record(value);
  const prompt = record(item.prompt);
  const status = lower(item.status);
  // The core read model exposes normalized action objects at the top level while
  // retaining the original prompt actions (usually strings) for audit fidelity.
  // Prefer the normalized objects so response requirements and destructive tones
  // are not lost on their way to the operator.
  const rawActions = array(item.actions || prompt.actions || prompt.options);
  const criteria = acceptanceCriteria(item.criteria || prompt.criteria);
  const verification: GateVerificationReport[] = array(item.verification || prompt.verification).map((raw) => {
    const report = record(raw);
    return {
      node_id: text(report.node_id || report.node_key),
      node_key: text(report.node_key) || undefined,
      run_id: text(report.run_id) || null,
      role: text(report.role) || undefined,
      status: lower(report.status, "unknown"),
      criteria: acceptanceCriteria(report.criteria),
      summary: text(report.summary) || undefined,
      findings: stringList(report.findings),
      source: text(report.source) || undefined,
    };
  });
  const failedRuns = array(item.failed_runs || prompt.failed_runs).map((raw) => normalizeRun(raw));
  const workspaceCommitFailures = array(
    item.workspace_commit_failures || prompt.workspace_commit_failures,
  ).map((raw) => normalizeRun(raw));
  return {
    id: text(item.id || item.gate_id),
    kind: lower(item.kind, "approval"),
    title: text(prompt.title || prompt.question || item.title, humanTitle(item.kind || "attention")),
    description: text(prompt.description || prompt.body || prompt.reason || item.description) || undefined,
    status: status === "open" || status === "pending" ? "pending" : "resolved",
    actions: rawActions.map((raw, index) => {
      const action = record(raw);
      const label = typeof raw === "string" ? raw : text(action.label || action.title || action.id, `Option ${index + 1}`);
      const id = typeof raw === "string" ? slug(raw) : text(action.id || action.value, slug(label));
      return {
        id,
        label,
        tone: (lower(action.tone) as "primary" | "neutral" | "danger")
          || (["reject", "cancel"].includes(id) ? "danger" : ["accept_current", "approve", "accept", "submit", "retry"].includes(id) ? "primary" : "neutral"),
        requires_response: action.requires_response == null
          ? ["accept_current", "submit", "request_changes"].includes(id)
          : Boolean(action.requires_response),
      };
    }),
    response_placeholder: text(prompt.response_placeholder || item.response_placeholder) || undefined,
    created_at: text(item.created_at || item.opened_at) || undefined,
    resolved_at: text(item.resolved_at) || undefined,
    resolution: item.resolution == null ? null : typeof item.resolution === "string" ? item.resolution : JSON.stringify(item.resolution),
    version: item.version == null ? undefined : number(item.version),
    criteria: Object.keys(criteria).length ? criteria : undefined,
    verification: verification.length ? verification : undefined,
    policy_reasons: stringList(item.policy_reasons || prompt.policy_reasons),
    failed_runs: failedRuns.length ? failedRuns : undefined,
    workspace_commit_failures: workspaceCommitFailures.length ? workspaceCommitFailures : undefined,
  };
}

function normalizeTaskDetail(value: unknown): OrchestrationTaskDetail {
  const envelope = record(value);
  const taskRaw = record(envelope.task && typeof envelope.task === "object" ? envelope.task : envelope);
  const summary = normalizeTaskSummary(taskRaw);
  const plan = record(envelope.plan || envelope.graph || taskRaw.plan);
  const rawNodes = array(envelope.nodes || plan.nodes);
  const rawEdges = array(envelope.edges || plan.edges);
  const rawRuns = array(envelope.runs || envelope.agent_runs);
  const canonicalNodeIds = new Map<string, string>();
  for (const raw of rawNodes) {
    const item = record(raw);
    const canonical = text(item.id || item.node_id || item.key);
    for (const alias of [item.id, item.node_id, item.key].map((candidate) => text(candidate)).filter(Boolean)) {
      canonicalNodeIds.set(alias, canonical);
    }
  }
  const canonicalNodeId = (value: unknown) => {
    const candidate = text(value);
    return canonicalNodeIds.get(candidate) || candidate;
  };
  const runs = rawRuns.map((raw) => {
    const item = record(raw);
    const runNodeId = canonicalNodeId(item.node_id || item.node_key);
    const node = rawNodes
      .map(record)
      .find((candidate) => text(candidate.id || candidate.node_id || candidate.key) === runNodeId);
    return { ...normalizeRun(item, node), node_id: runNodeId || undefined };
  });
  const nodes: TaskNode[] = rawNodes.map((raw) => {
    const item = record(raw);
    const id = text(item.id || item.node_id || item.key);
    const key = text(item.key);
    const nodeRuns = runs.filter((run) => run.node_id === id);
    const latest = nodeRuns[nodeRuns.length - 1];
    const dependencies = rawEdges
      .map(record)
      .filter((edge) => canonicalNodeId(edge.to_node_id || edge.to_node || edge.to) === id)
      .map((edge) => canonicalNodeId(edge.from_node_id || edge.from_node || edge.from))
      .filter(Boolean);
    const explicitDependencies = stringList(item.depends_on || item.dependencies).map(canonicalNodeId);
    return {
      id,
      key: key || undefined,
      title: text(item.title || item.key, "Work item"),
      description: text(item.description || item.instructions) || undefined,
      kind: lower(item.kind) || undefined,
      status: normalizedStatus(item.status || latest?.status, latest ? latest.status : "pending"),
      depends_on: explicitDependencies.length ? explicitDependencies : dependencies,
      profile_name: text(item.profile_name || item.agent) || undefined,
      profile_version: item.profile_version == null ? undefined : number(item.profile_version),
      run_ids: nodeRuns.map((run) => run.id),
    };
  });
  const evidence: TaskEvidence[] = array(envelope.evidence).map((raw) => {
    const item = record(raw);
    const payload = record(item.payload);
    return {
      id: text(item.id || item.evidence_id),
      title: text(item.title || payload.title, humanTitle(item.kind || "evidence")),
      kind: lower(item.kind, "other"),
      summary: text(item.summary || payload.summary || payload.message) || undefined,
      uri: text(item.uri || item.blob_uri || payload.uri) || undefined,
      run_id: text(item.run_id) || undefined,
      created_at: text(item.created_at) || undefined,
      content_hash: text(item.content_hash || payload.content_hash || record(payload.blob).sha256) || undefined,
      payload,
      subject: Object.keys(record(item.subject || payload.subject)).length
        ? record(item.subject || payload.subject)
        : undefined,
      subject_matches: item.subject_matches == null && payload.subject_matches == null
        ? undefined
        : Boolean(item.subject_matches ?? payload.subject_matches),
      missing_criteria: stringList(item.missing_criteria || payload.missing_criteria),
      actor: text(item.actor || item.created_by || payload.created_by || payload.resolved_by) || undefined,
    };
  });
  const activity: TaskActivity[] = array(envelope.activity || envelope.events).map((raw, index) => {
    const item = record(raw);
    const payload = record(item.payload);
    const detailPayload = jsonRecord(item.detail);
    return {
      id: text(item.id, `event-${index}`),
      type: lower(item.type || item.event_type, "event"),
      summary: text(item.summary || payload.summary || payload.message, humanTitle(item.event_type || item.type || "event")),
      detail: text(item.detail || payload.detail) || undefined,
      error_kind: text(item.error_kind || payload.error_kind || detailPayload.error_kind) || undefined,
      error_message: text(item.error_message || payload.error_message || payload.error || detailPayload.error_message || detailPayload.error) || undefined,
      actor: text(item.actor || payload.actor || item.created_by) || undefined,
      created_at: text(item.created_at),
      stage: lower(item.stage || payload.stage) || undefined,
      sequence: item.sequence == null ? undefined : number(item.sequence),
      event_hash: text(item.event_hash) || undefined,
    };
  });
  return {
    ...summary,
    result: Object.keys(record(envelope.result || taskRaw.result)).length
      ? record(envelope.result || taskRaw.result)
      : null,
    brief: Object.keys(record(envelope.brief)).length ? normalizeBrief(envelope.brief) : undefined,
    handoff_summary: Object.keys(record(envelope.handoff_summary)).length
      ? record(envelope.handoff_summary) as OrchestrationTaskDetail["handoff_summary"]
      : undefined,
    stages: array(envelope.stages || envelope.stage_history).map(normalizeStage),
    attention: array(envelope.attention || envelope.attention_gates || envelope.gates).map(normalizeGate),
    attention_page: normalizeAuditPage(envelope.attention_page),
    nodes,
    edges: rawEdges.map((raw) => { const item = record(raw); return { from: canonicalNodeId(item.from || item.from_node_id || item.from_node), to: canonicalNodeId(item.to || item.to_node_id || item.to_node) }; }),
    runs,
    runs_page: normalizeAuditPage(envelope.runs_page),
    evidence,
    evidence_page: normalizeAuditPage(envelope.evidence_page),
    activity,
    activity_page: Object.keys(record(envelope.activity_page)).length ? {
      has_more: Boolean(record(envelope.activity_page).has_more),
      next_sequence: record(envelope.activity_page).next_sequence == null
        ? null
        : number(record(envelope.activity_page).next_sequence),
      next_parameter: text(record(envelope.activity_page).next_parameter) || undefined,
      page_size: record(envelope.activity_page).page_size == null
        ? undefined
        : number(record(envelope.activity_page).page_size),
    } : undefined,
    detail_limits: Object.keys(record(envelope.detail_limits)).length ? {
      child_depth: record(envelope.detail_limits).child_depth == null ? undefined : number(record(envelope.detail_limits).child_depth),
      attention: record(envelope.detail_limits).attention == null ? undefined : number(record(envelope.detail_limits).attention),
      runs: record(envelope.detail_limits).runs == null ? undefined : number(record(envelope.detail_limits).runs),
      evidence: record(envelope.detail_limits).evidence == null ? undefined : number(record(envelope.detail_limits).evidence),
      activity: record(envelope.detail_limits).activity == null ? undefined : number(record(envelope.detail_limits).activity),
    } : undefined,
    profile_snapshot: normalizeSnapshot(envelope.profile_snapshot || envelope.agent_profile_snapshot),
    model_policy_snapshot: normalizeSnapshot(envelope.model_policy_snapshot || envelope.routing_policy_snapshot),
    children: array(envelope.children).map(normalizeTaskSummary),
    children_details: array(envelope.children_details || envelope.child_details).map(normalizeTaskDetail),
    children_page: Object.keys(record(envelope.children_page)).length ? {
      truncated: Boolean(record(envelope.children_page).truncated),
      tree_truncated: Boolean(record(envelope.children_page).tree_truncated),
      depth_limit_reached: Boolean(record(envelope.children_page).depth_limit_reached),
      returned: number(record(envelope.children_page).returned),
      total: number(record(envelope.children_page).total),
      tree_row_limit: number(record(envelope.children_page).tree_row_limit),
    } : undefined,
    runtime_page: Object.keys(record(envelope.runtime_page)).length ? {
      truncated: Boolean(record(envelope.runtime_page).truncated),
      returned: number(record(envelope.runtime_page).returned),
      limit: number(record(envelope.runtime_page).limit),
    } : undefined,
    quality_refs: Object.keys(record(envelope.quality_refs)).length
      ? record(envelope.quality_refs) as OrchestrationTaskDetail["quality_refs"]
      : undefined,
    legacy_quality_projection: envelope.legacy_quality_projection == null
      ? undefined
      : Boolean(envelope.legacy_quality_projection),
    quality_projection_warning: text(envelope.quality_projection_warning) || undefined,
  };
}

function normalizeSnapshot(value: unknown) {
  const item = record(value);
  if (!Object.keys(item).length) return undefined;
  return { id: text(item.id || item.profile_id || item.policy_id), version: number(item.version), content_hash: text(item.content_hash) || undefined, name: text(item.name || item.display_name) || undefined };
}

function normalizeProfileSpec(value: unknown): AgentProfileSpec {
  const item = record(value);
  const base = record(item.base || item.cloned_from);
  const communication = record(item.communication_policy);
  return {
    schema_version: number(item.schema_version, Object.keys(communication).length ? 2 : 1) === 2 ? 2 : 1,
    profile_id: text(item.profile_id || item.id),
    display_name: text(item.display_name || item.name),
    role: lower(item.role, "worker") as AgentProfileSpec["role"],
    instructions: text(item.instructions),
    allowed_tools: stringList(item.allowed_tools),
    allowed_child_roles: stringList(item.allowed_child_roles).map((role) => role.toLowerCase()) as AgentProfileSpec["allowed_child_roles"],
    permission_mode: lower(item.permission_mode, "interactive") as AgentProfileSpec["permission_mode"],
    model_policy: text(item.model_policy, "quality-first"),
    max_iterations: number(item.max_iterations, 12),
    max_children: number(item.max_children),
    base: Object.keys(base).length ? { profile_id: text(base.profile_id), version: number(base.version) } : null,
    metadata: record(item.metadata),
    communication_policy: Object.keys(communication).length ? {
      can_delegate: Boolean(communication.can_delegate),
      allowed_child_roles: stringList(communication.allowed_child_roles).map((role) => role.toLowerCase()) as AgentProfileSpec["allowed_child_roles"],
      required_brief_fields: stringList(communication.required_brief_fields),
      max_initial_context_tokens: number(communication.max_initial_context_tokens, 4_000),
      max_context_refs: number(communication.max_context_refs, 20),
      max_inline_bytes_per_ref: number(communication.max_inline_bytes_per_ref, 8_192),
      max_inline_bytes_total: number(communication.max_inline_bytes_total, 32_768),
      allowed_context_ref_types: stringList(communication.allowed_context_ref_types),
      allow_full_transcript_reference: Boolean(communication.allow_full_transcript_reference),
      allowed_relation_types: stringList(communication.allowed_relation_types),
      can_comment: communication.can_comment == null ? true : Boolean(communication.can_comment),
      can_mention: communication.can_mention == null ? true : Boolean(communication.can_mention),
      can_mention_receive: communication.can_mention_receive == null ? true : Boolean(communication.can_mention_receive),
      result_contract_id: text(communication.result_contract_id, "task_result_v2"),
    } : undefined,
  };
}

function normalizeProfileVersion(value: unknown): AgentProfileVersion {
  const item = record(value);
  const spec = normalizeProfileSpec(item.spec || item);
  return {
    profile_id: spec.profile_id,
    version: number(item.version, 1),
    spec,
    content_hash: text(item.content_hash),
    builtin: Boolean(item.builtin),
    cloned_from: Object.keys(record(item.cloned_from)).length ? normalizeProfileSpec({ ...spec, base: item.cloned_from }).base : null,
    published_at: text(item.published_at || item.created_at) || undefined,
  };
}

function normalizeProfileSummary(value: unknown): AgentProfileSummary {
  const item = record(value);
  const current = record(item.current || item.latest);
  return {
    id: text(item.id || item.profile_id || current.profile_id),
    name: text(item.name || item.display_name || current.display_name || item.profile_id),
    role: (lower(item.role || current.role) || undefined) as AgentProfileSummary["role"],
    description: text(item.description) || undefined,
    builtin: Boolean(item.builtin ?? current.builtin),
    archived: Boolean(item.archived),
    current_version: item.current_version == null ? (item.version == null && current.version == null ? null : number(item.version || current.version)) : number(item.current_version),
    has_draft: Boolean(item.has_draft || item.draft),
    updated_at: text(item.updated_at) || undefined,
    derived_from: normalizeProvenance(item.derived_from || item.cloned_from || current.cloned_from, "profile_id"),
  };
}

function normalizeProfileDetail(value: unknown): AgentProfileDetail {
  const envelope = record(value);
  const root = record(envelope.profile || envelope);
  const rawVersions = array(root.versions || envelope.versions);
  const versions = rawVersions.length ? rawVersions.map(normalizeProfileVersion) : root.version ? [normalizeProfileVersion(root)] : [];
  const current = root.current ? normalizeProfileVersion(root.current) : versions[versions.length - 1] || null;
  const rawDraft = record(root.draft || envelope.draft);
  const draft = Object.keys(rawDraft).length ? {
    profile_id: text(rawDraft.profile_id || root.id || root.profile_id),
    base_version: rawDraft.base_version == null ? (record(rawDraft.base).version == null ? null : number(record(rawDraft.base).version)) : number(rawDraft.base_version),
    etag: text(rawDraft.etag, `v${current?.version || 0}`),
    spec: normalizeProfileSpec(rawDraft.spec || rawDraft),
    validation: rawDraft.validation as ValidationReport | undefined,
    updated_at: text(rawDraft.updated_at) || undefined,
  } : null;
  return { ...normalizeProfileSummary({ ...root, current, draft }), versions, current, draft };
}

function normalizePolicySpec(value: unknown): ModelRoutingPolicySpec {
  const item = record(value);
  return {
    schema_version: 1,
    policy_id: text(item.policy_id || item.id, "quality-first"),
    require_verified: item.require_verified !== false,
    allow_unknown_cost: item.allow_unknown_cost !== false,
    allowed_providers: stringList(item.allowed_providers),
    allowed_models: stringList(item.allowed_models),
    blocked_models: stringList(item.blocked_models),
    fallback_limit: number(item.fallback_limit, 2),
    fallback_for_explicit: Boolean(item.fallback_for_explicit),
  };
}

function normalizePolicyVersion(value: unknown): ModelPolicyVersion {
  const item = record(value);
  const spec = normalizePolicySpec(item.spec || item);
  return { policy_id: spec.policy_id, version: number(item.version, 1), spec, content_hash: text(item.content_hash), published_at: text(item.published_at || item.created_at) || undefined };
}

function normalizePolicySummary(value: unknown): ModelPolicySummary {
  const item = record(value);
  const current = record(item.current || item.latest);
  return {
    id: text(item.id || item.policy_id || current.policy_id),
    name: text(item.name || item.display_name || item.policy_id || current.policy_id),
    description: text(item.description) || undefined,
    builtin: Boolean(item.builtin),
    archived: Boolean(item.archived),
    current_version: item.current_version == null ? (item.version == null && current.version == null ? null : number(item.version || current.version)) : number(item.current_version),
    has_draft: Boolean(item.has_draft || item.draft),
    updated_at: text(item.updated_at) || undefined,
    derived_from: normalizeProvenance(item.derived_from, "policy_id"),
  };
}

function normalizePolicyDetail(value: unknown): ModelPolicyDetail {
  const envelope = record(value);
  const root = record(envelope.policy || envelope);
  const rawVersions = array(root.versions || envelope.versions);
  const versions = rawVersions.length ? rawVersions.map(normalizePolicyVersion) : root.version ? [normalizePolicyVersion(root)] : [];
  const current = root.current ? normalizePolicyVersion(root.current) : versions[versions.length - 1] || null;
  const rawDraft = record(root.draft || envelope.draft);
  const draft: ModelPolicyDraft | null = Object.keys(rawDraft).length ? {
    policy_id: text(rawDraft.policy_id || root.id || root.policy_id),
    base_version: rawDraft.base_version == null ? null : number(rawDraft.base_version),
    etag: text(rawDraft.etag, `v${current?.version || 0}`),
    spec: normalizePolicySpec(rawDraft.spec || rawDraft),
    validation: rawDraft.validation as ValidationReport | undefined,
    updated_at: text(rawDraft.updated_at) || undefined,
  } : null;
  return { ...normalizePolicySummary({ ...root, current, draft }), versions, current, draft };
}

function normalizeCatalogModel(value: unknown): RoutingModelDescriptor {
  const item = record(value);
  const rawCaps = item.capabilities;
  const capabilities = Array.isArray(rawCaps)
    ? stringList(rawCaps)
    : Object.entries(record(rawCaps)).filter(([, enabled]) => Boolean(enabled)).map(([name]) => name);
  const configured = item.configured !== false;
  const available = item.available !== false;
  const rawRuntime = record(item.runtime);
  return {
    id: text(item.id || item.model_id),
    label: text(item.label || item.model_id || item.id),
    provider: text(item.provider, text(item.model_id || item.id).includes(":") ? text(item.model_id || item.id).split(":", 1)[0] : "openai"),
    source: item.source === "custom" || item.source === "subscription-runtime" ? item.source : "curated",
    quality: number(item.quality),
    configured,
    in_composer_picker: item.in_composer_picker == null ? undefined : Boolean(item.in_composer_picker),
    availability: lower(item.availability, configured && available ? "configured" : configured ? "offline" : "unconfigured") as RoutingModelDescriptor["availability"],
    availability_reason: text(item.availability_reason) || undefined,
    verified: item.verified !== false,
    capabilities,
    context_window: item.context_window == null && item.context_window_tokens == null ? null : number(item.context_window || item.context_window_tokens),
    input_microusd_per_million: item.input_microusd_per_million == null ? null : number(item.input_microusd_per_million),
    output_microusd_per_million: item.output_microusd_per_million == null ? null : number(item.output_microusd_per_million),
    latency_rank: number(item.latency_rank),
    catalog_revision: text(item.catalog_revision) || undefined,
    runtime: Object.keys(rawRuntime).length ? {
      protocol: text(rawRuntime.protocol),
      model: text(rawRuntime.model),
      reasoning_effort: lower(rawRuntime.reasoning_effort),
      local_owner_only: Boolean(rawRuntime.local_owner_only),
      interactive_only: Boolean(rawRuntime.interactive_only),
    } : undefined,
  };
}

function normalizeSubscriptionRuntimeHealth(value: unknown, runtimeId: string, provider: string): SubscriptionRuntimeHealth {
  const item = record(value);
  return {
    runtime_id: text(item.runtime_id, runtimeId),
    provider: text(item.provider, provider),
    installed: Boolean(item.installed),
    authenticated: Boolean(item.authenticated),
    available: Boolean(item.available),
    policy_eligible: Boolean(item.policy_eligible),
    version: text(item.version),
    auth_kind: lower(item.auth_kind, "unknown"),
    executable: text(item.executable),
    reason: text(item.reason),
    checked_at: item.checked_at == null ? null : number(item.checked_at),
  };
}

function normalizeSubscriptionRuntime(value: unknown): SubscriptionRuntimeDescriptor {
  const item = record(value);
  const runtimeId = text(item.runtime_id || item.id);
  const provider = text(item.provider, runtimeId.includes(":") ? runtimeId.split(":", 1)[0] : "");
  const health = normalizeSubscriptionRuntimeHealth(item.health, runtimeId, provider);
  const availability = health.available && health.policy_eligible
    ? "available"
    : health.authenticated && !health.policy_eligible
      ? "blocked_by_policy"
      : Object.keys(record(item.health)).length
        ? "unavailable"
        : "unknown";
  return {
    runtime_id: runtimeId,
    provider,
    display_name: text(item.display_name || item.label, runtimeId),
    command: text(item.command),
    model: text(item.model || item.vendor_model),
    reasoning_effort: lower(item.reasoning_effort || item.effort),
    quality: number(item.quality),
    context_window: item.context_window == null ? null : number(item.context_window),
    minimum_cli_version: text(item.minimum_cli_version),
    protocol: text(item.protocol),
    interactive_only: Boolean(item.interactive_only),
    local_owner_only: item.local_owner_only !== false,
    capabilities: stringList(item.capabilities),
    health,
    availability,
    availability_reason: health.reason,
  };
}

function normalizeRuntimePresetRole(value: unknown, fallbackRole = ""): RuntimePresetRoleAssignment | null {
  if (typeof value === "string") {
    return fallbackRole && value.trim() ? { role: fallbackRole, runtime_id: value.trim() } : null;
  }
  const item = record(value);
  const role = text(item.role || item.agent_role, fallbackRole).trim();
  const runtimeId = text(item.runtime_id || item.model_id || item.model).trim();
  if (!role || !runtimeId) return null;
  return {
    role,
    runtime_id: runtimeId,
    access: text(item.access || item.permission_mode) || undefined,
    fresh_session: item.fresh_session == null ? undefined : Boolean(item.fresh_session),
    required: item.required == null ? undefined : Boolean(item.required),
  };
}

function normalizeRuntimePreset(value: unknown): RuntimePresetDescriptor {
  const item = record(value);
  const rawRoles = item.roles || item.assignments || item.role_assignments;
  const roles = Array.isArray(rawRoles)
    ? rawRoles.map((raw) => normalizeRuntimePresetRole(raw)).filter((role): role is RuntimePresetRoleAssignment => role !== null)
    : Object.entries(record(rawRoles)).map(([role, raw]) => normalizeRuntimePresetRole(raw, role)).filter((assignment): assignment is RuntimePresetRoleAssignment => assignment !== null);
  const requiredRuntimeIds = stringList(item.required_runtime_ids);
  const availability = lower(item.availability);
  const explicitAvailable = typeof item.available === "boolean"
    ? item.available
    : availability
      ? ["available", "configured", "ready"].includes(availability)
      : undefined;
  return {
    id: text(item.preset_id || item.id),
    name: text(item.display_name || item.name, humanTitle(item.preset_id || item.id)),
    description: text(item.description) || undefined,
    builtin: item.builtin !== false,
    is_default: Boolean(item.is_default || item.default),
    default_for_domains: stringList(item.default_for_domains || item.domains),
    roles,
    required_runtime_ids: requiredRuntimeIds.length
      ? requiredRuntimeIds
      : [...new Set(roles.filter((role) => role.required !== false).map((role) => role.runtime_id))],
    available: explicitAvailable,
    unavailable_runtime_ids: stringList(item.unavailable_runtime_ids),
    availability_reason: text(item.availability_reason || item.reason) || undefined,
  };
}

function normalizeSimulation(value: unknown): RoutingSimulationResult {
  const envelope = record(value);
  const item = record(envelope.decision || envelope);
  return {
    decision_id: text(item.decision_id),
    selected_model: text(item.selected_model) || null,
    fallback_models: stringList(item.fallback_models),
    reason: text(item.reason),
    catalog_hash: text(item.catalog_hash) || undefined,
    evaluations: array(item.evaluations || item.candidates).map((raw) => {
      const candidate = record(raw);
      return {
        model_id: text(candidate.model_id),
        provider: text(candidate.provider) || undefined,
        eligible: Boolean(candidate.eligible),
        reasons: stringList(candidate.reasons || candidate.reason_codes),
        quality: number(candidate.quality),
        estimated_cost_microusd: candidate.estimated_cost_microusd == null ? null : number(candidate.estimated_cost_microusd),
        latency_rank: candidate.latency_rank == null ? undefined : number(candidate.latency_rank),
        rank: candidate.rank == null ? null : number(candidate.rank),
      };
    }),
  };
}

function normalizeProvenance(value: unknown, idField: "profile_id" | "policy_id"): VersionProvenance | undefined {
  const item = record(value);
  if (!Object.keys(item).length) return undefined;
  return {
    [idField]: text(item[idField]),
    version: number(item.version),
    content_hash: text(item.content_hash),
  };
}

function humanTitle(value: unknown) {
  return text(value).replace(/[_-]+/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
const slug = (value: string) => value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");

export function createOrchestrationApi(apiRequest: ApiRequest) {
  const root = "/v1/orchestration";
  const createKeys = new WeakMap<CreateOrchestrationTask, string>();
  const resolutionKeys = new Map<string, string>();
  const mutationRequest = (
    method: "POST" | "PUT" | "PATCH" | "DELETE",
    scope: string,
    body?: unknown,
    headers: Record<string, string> = {},
  ) => jsonRequest(method, body, {
    ...headers,
    "Idempotency-Key": createClientIdempotencyKey(scope),
  });
  return {
    async getHandoffSettings(): Promise<HandoffRuntimeSettings> {
      return normalizeHandoffSettings(
        await apiRequest<unknown>("/v1/orchestration/handoff-settings"),
      );
    },

    async updateHandoffSettings(settings: HandoffRuntimeSettings): Promise<HandoffRuntimeSettings> {
      return normalizeHandoffSettings(await apiRequest<unknown>(
        "/v1/orchestration/handoff-settings",
        mutationRequest("PUT", "handoff-settings", settings),
      ));
    },

    async createTask(spec: CreateOrchestrationTask): Promise<OrchestrationTaskDetail> {
      const idempotencyKey = spec.idempotency_key
        || createKeys.get(spec)
        || createClientIdempotencyKey("task-create");
      createKeys.set(spec, idempotencyKey);
      return normalizeTaskDetail(await apiRequest(
        `${root}/tasks`,
        jsonRequest(
          "POST",
          { ...spec, read_only: Boolean(spec.read_only), idempotency_key: idempotencyKey },
          { "Idempotency-Key": idempotencyKey },
        ),
      ));
    },
    async listTasks(options: TaskListOptions = {}): Promise<OrchestrationTaskSummary[]> {
      const query = new URLSearchParams();
      for (const status of options.statuses || []) query.append("status", status);
      for (const status of options.workflowStatuses || []) query.append("workflow_status", status);
      for (const status of options.qualityStatuses || []) query.append("quality_status", status);
      for (const status of options.artifactStatuses || []) query.append("artifact_status", status);
      for (const status of options.budgetStatuses || []) query.append("budget_status", status);
      for (const value of options.archetypes || []) query.append("archetype", value);
      for (const value of options.budgetModes || []) query.append("budget_mode", value);
      if (options.repo) query.set("repo", options.repo);
      if (options.snapshotRef) query.set("snapshot_ref", options.snapshotRef);
      if (options.hasWaiver !== undefined) query.set("has_waiver", String(options.hasWaiver));
      if (options.repairCount !== undefined) query.set("repair_count", String(options.repairCount));
      if (options.createdBy) query.set("created_by", options.createdBy);
      if (options.limit !== undefined) query.set("limit", String(options.limit));
      if (options.offset !== undefined) query.set("offset", String(options.offset));
      const suffix = query.toString();
      const out = await apiRequest<OrchestrationTaskSummary[] | Record<string, unknown>>(
        `${root}/tasks${suffix ? `?${suffix}` : ""}`,
      );
      return listFrom<unknown>(out, "tasks").map(normalizeTaskSummary);
    },
    async createTaskQualityDraft(
      spec: CreateTaskQualityDraft,
      idempotencyKey = createClientIdempotencyKey("quality-draft-create"),
    ): Promise<{ task_id: string; id: string; workflow_status: TaskQualityWorkflowStatus; prompt_hash: string; created_at: string }> {
      return apiRequest(`${root}/task-drafts`, jsonRequest(
        "POST",
        spec,
        { "Idempotency-Key": idempotencyKey },
      ));
    },
    async analyzeTaskQualityDraft(
      taskId: string,
      payload: Record<string, unknown> = {},
      idempotencyKey = createClientIdempotencyKey(`quality-analyze-${taskId}`),
    ): Promise<TaskDraftAnalysisV2> {
      return apiRequest(`${root}/task-drafts/${encodeURIComponent(taskId)}:analyze`, jsonRequest(
        "POST",
        payload,
        { "Idempotency-Key": idempotencyKey },
      ));
    },
    getTaskQualityDraftAnalysis(taskId: string): Promise<TaskDraftAnalysisV2> {
      return apiRequest(`${root}/task-drafts/${encodeURIComponent(taskId)}/analysis`);
    },
    updateTaskQualityContract(
      taskId: string,
      contract: TaskQualityContract,
      etag: string,
    ): Promise<TaskQualityContract> {
      return apiRequest(`${root}/task-drafts/${encodeURIComponent(taskId)}/contract`, jsonRequest(
        "PUT",
        contract,
        { "If-Match": etag },
      ));
    },
    resolveTaskQualityTarget(
      taskId: string,
      payload: Record<string, unknown> = {},
    ): Promise<Record<string, unknown>> {
      return apiRequest(`${root}/task-drafts/${encodeURIComponent(taskId)}/target:resolve`, jsonRequest("POST", payload));
    },
    freezeTaskQualitySnapshot(
      taskId: string,
      payload: Record<string, unknown> = {},
    ): Promise<RepositorySnapshotV2> {
      return apiRequest(`${root}/task-drafts/${encodeURIComponent(taskId)}/snapshots`, jsonRequest("POST", payload));
    },
    publishTaskQualityContract(taskId: string, etag: string): Promise<TaskQualityContract> {
      return apiRequest(`${root}/task-drafts/${encodeURIComponent(taskId)}/contract:publish`, jsonRequest(
        "POST",
        undefined,
        { "If-Match": etag },
      ));
    },
    generateTaskQualityStrategy(
      taskId: string,
      payload: Record<string, unknown> = {},
    ): Promise<ExecutionStrategyV2> {
      return apiRequest(`${root}/task-drafts/${encodeURIComponent(taskId)}/strategy:generate`, jsonRequest("POST", payload));
    },
    async startTaskQualityDraft(taskId: string): Promise<OrchestrationTaskDetail> {
      return normalizeTaskDetail(await apiRequest(
        `${root}/task-drafts/${encodeURIComponent(taskId)}:start`,
        jsonRequest("POST"),
      ));
    },
    async getTask(id: string): Promise<OrchestrationTaskDetail> {
      const out = await apiRequest<OrchestrationTaskDetail | Record<string, unknown>>(
        `${root}/tasks/${encodeURIComponent(id)}`,
      );
      return normalizeTaskDetail(out);
    },
    getTaskQualityContract(taskId: string): Promise<TaskQualityContract> {
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/contract`);
    },
    getTaskQualitySnapshot(taskId: string): Promise<RepositorySnapshotV2> {
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/snapshot`);
    },
    getTaskQualityStrategy(taskId: string): Promise<ExecutionStrategyV2> {
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/strategy`);
    },
    getTaskQualityCoverage(taskId: string, offset = 0, limit = 200, cursor?: string): Promise<Record<string, unknown>> {
      const query = new URLSearchParams({ limit: String(limit) });
      if (cursor) query.set("cursor", cursor); else query.set("offset", String(offset));
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/coverage?${query.toString()}`);
    },
    getTaskQualityClaims(taskId: string, offset = 0, limit = 200, cursor?: string): Promise<Record<string, unknown>> {
      const query = new URLSearchParams({ limit: String(limit) });
      if (cursor) query.set("cursor", cursor); else query.set("offset", String(offset));
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/claims?${query.toString()}`);
    },
    getTaskQualityEvidence(taskId: string, offset = 0, limit = 200, cursor?: string): Promise<Record<string, unknown>> {
      const query = new URLSearchParams({ limit: String(limit) });
      if (cursor) query.set("cursor", cursor); else query.set("offset", String(offset));
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/evidence?${query.toString()}`);
    },
    getTaskQuality(
      taskId: string,
      offset = 0,
      limit = 200,
      cursors: Partial<Record<"gate_cursor" | "finding_cursor" | "evaluation_cursor" | "waiver_cursor", string>> = {},
    ): Promise<QualityBundleV2> {
      const query = new URLSearchParams({ limit: String(limit) });
      if (Object.keys(cursors).length) {
        Object.entries(cursors).forEach(([key, value]) => { if (value) query.set(key, value); });
      } else query.set("offset", String(offset));
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/quality?${query.toString()}`);
    },
    getTaskDeliverables(taskId: string, offset = 0, limit = 200, cursor?: string): Promise<Record<string, unknown>> {
      const query = new URLSearchParams({ limit: String(limit) });
      if (cursor) query.set("cursor", cursor); else query.set("offset", String(offset));
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/deliverables?${query.toString()}`);
    },
    getArtifactMetadata(artifactId: string): Promise<Record<string, unknown>> {
      return apiRequest(`${root}/artifacts/${encodeURIComponent(artifactId)}`);
    },
    async getArtifactContentRange(artifactId: string, start: number, endInclusive: number): Promise<string> {
      const out = await apiRequest<unknown>(
        `${root}/artifacts/${encodeURIComponent(artifactId)}/content`,
        { headers: { Range: `bytes=${start}-${endInclusive}` } },
      );
      return typeof out === "string" ? out : text(record(out).message);
    },
    async getArtifactDiff(artifactId: string, baseArtifactId: string): Promise<string> {
      const out = await apiRequest<unknown>(`${root}/artifacts/${encodeURIComponent(artifactId)}/diff?base=${encodeURIComponent(baseArtifactId)}`);
      return typeof out === "string" ? out : text(record(out).message);
    },
    requestTaskRepair(taskId: string, payload: Record<string, unknown>): Promise<Record<string, unknown>> {
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/repairs`, jsonRequest("POST", payload));
    },
    createTaskQualityWaiver(taskId: string, payload: Record<string, unknown>): Promise<Record<string, unknown>> {
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/waivers`, jsonRequest("POST", payload));
    },
    async resumeTaskQuality(taskId: string, payload: Record<string, unknown> = {}): Promise<OrchestrationTaskDetail> {
      return normalizeTaskDetail(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}:resume`,
        jsonRequest("POST", payload),
      ));
    },
    listTaskQualityBenchmarkSuites(): Promise<TaskQualityBenchmarkSuite[]> {
      return apiRequest(`${root}/benchmarks/suites`);
    },
    runTaskQualityBenchmark(suiteId: string, candidateId = "v2"): Promise<TaskQualityBenchmarkRun> {
      return apiRequest(`${root}/benchmarks/runs`, jsonRequest("POST", {
        suite_id: suiteId,
        candidate_id: candidateId,
      }));
    },
    getTaskQualityBenchmarkRun(runId: string): Promise<TaskQualityBenchmarkRun> {
      return apiRequest(`${root}/benchmarks/runs/${encodeURIComponent(runId)}`);
    },
    getTaskQualityBenchmarkComparison(runId: string): Promise<TaskQualityBenchmarkComparison> {
      return apiRequest(`${root}/benchmarks/runs/${encodeURIComponent(runId)}/comparison`);
    },
    promoteTaskQualityBenchmarkBaseline(
      suiteId: string,
      payload: { run_id: string; reason: string },
    ): Promise<Record<string, unknown>> {
      return apiRequest(`${root}/benchmarks/suites/${encodeURIComponent(suiteId)}:promote-baseline`, jsonRequest("POST", payload));
    },
    async listTaskBriefs(taskId: string): Promise<TaskBrief[]> {
      const out = await apiRequest<unknown[]>(`${root}/tasks/${encodeURIComponent(taskId)}/briefs`);
      return array(out).map(normalizeBrief);
    },
    async getTaskBrief(taskId: string, revision: number): Promise<TaskBrief> {
      return normalizeBrief(await apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/briefs/${revision}`));
    },
    validateTaskBrief(taskId: string, brief: TaskBriefInput): Promise<ValidationReport & { content_hash?: string }> {
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/briefs/validate`, jsonRequest("POST", brief));
    },
    async createTaskBrief(taskId: string, brief: TaskBriefInput): Promise<TaskBrief> {
      return normalizeBrief(await apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/briefs`, mutationRequest("POST", "brief-create", brief)));
    },
    async updateTaskBrief(taskId: string, brief: TaskBrief): Promise<TaskBrief> {
      const payload: TaskBriefInput = {
        title: brief.title,
        objective: brief.objective,
        background: brief.background,
        scope: brief.scope,
        instructions: brief.instructions,
        constraints: brief.constraints,
        non_goals: brief.non_goals,
        acceptance_criteria: brief.acceptance_criteria,
        deliverables: brief.deliverables,
        result_contract: brief.result_contract,
      };
      return normalizeBrief(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/briefs/${brief.revision}`,
        mutationRequest("PATCH", "brief-update", payload, { "If-Match": brief.content_hash }),
      ));
    },
    async publishTaskBrief(taskId: string, brief: Pick<TaskBrief, "revision" | "content_hash">): Promise<TaskBrief> {
      return normalizeBrief(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/briefs/${brief.revision}/publish`,
        mutationRequest("POST", "brief-publish", undefined, { "If-Match": brief.content_hash }),
      ));
    },
    async listContextRefs(taskId: string, briefId?: string): Promise<ContextRef[]> {
      const query = briefId ? `?brief_id=${encodeURIComponent(briefId)}` : "";
      const out = await apiRequest<unknown[]>(`${root}/tasks/${encodeURIComponent(taskId)}/context-refs${query}`);
      return array(out).map(normalizeContextRef);
    },
    async addContextRef(taskId: string, contextRef: ContextRefInput, briefId?: string): Promise<ContextRef> {
      return normalizeContextRef(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/context-refs`,
        mutationRequest("POST", "context-ref-add", { brief_id: briefId, context_ref: contextRef }),
      ));
    },
    readContextRef(refId: string, startLine?: number, endLine?: number): Promise<Record<string, unknown>> {
      const query = new URLSearchParams();
      if (startLine !== undefined) query.set("start_line", String(startLine));
      if (endLine !== undefined) query.set("end_line", String(endLine));
      const suffix = query.toString();
      return apiRequest(`${root}/context-refs/${encodeURIComponent(refId)}/content${suffix ? `?${suffix}` : ""}`);
    },
    verifyContextRef(refId: string): Promise<Record<string, unknown>> {
      return apiRequest(`${root}/context-refs/${encodeURIComponent(refId)}/verify`, mutationRequest("POST", "context-ref-verify"));
    },
    async listTaskRelations(taskId: string): Promise<TaskRelation[]> {
      const out = await apiRequest<unknown[]>(`${root}/tasks/${encodeURIComponent(taskId)}/relations`);
      return array(out).map(normalizeRelation);
    },
    async addTaskRelation(
      taskId: string,
      relation: Pick<TaskRelation, "from_task_id" | "to_task_id" | "relation_type"> & { metadata?: Record<string, unknown> },
    ): Promise<TaskRelation> {
      return normalizeRelation(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/relations`,
        mutationRequest("POST", "relation-add", relation),
      ));
    },
    async removeTaskRelation(taskId: string, relationId: string): Promise<TaskRelation> {
      return normalizeRelation(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/relations/${encodeURIComponent(relationId)}`,
        mutationRequest("DELETE", "relation-remove"),
      ));
    },
    async replaceTaskBlockers(
      taskId: string,
      taskIds: string[],
      reason = "Operator updated blockers",
    ): Promise<TaskRelation[]> {
      const out = await apiRequest<unknown[]>(
        `${root}/tasks/${encodeURIComponent(taskId)}/blockers`,
        mutationRequest("PUT", "blockers-replace", {
          task_ids: taskIds,
          reason,
          owner: "local-user",
          required_action: "Complete all blocker tasks",
        }),
      );
      return array(out).map(normalizeRelation);
    },
    async listTaskComments(taskId: string, afterSequence = 0): Promise<TaskCommentDelta> {
      const out = record(await apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/comments?after_sequence=${afterSequence}`));
      return {
        task_id: text(out.task_id, taskId),
        after_sequence: number(out.after_sequence, afterSequence),
        latest_sequence: number(out.latest_sequence),
        new_count: number(out.new_count),
        comments: array(out.comments).map(normalizeComment),
        fallback_fetch_needed: Boolean(out.fallback_fetch_needed),
      };
    },
    async postTaskComment(taskId: string, bodyMarkdown: string, metadata: Record<string, unknown> = {}): Promise<TaskComment> {
      return normalizeComment(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/comments`,
        mutationRequest("POST", "comment-post", { body_markdown: bodyMarkdown, metadata }),
      ));
    },
    async listTaskWorkProducts(taskId: string): Promise<WorkProduct[]> {
      const out = await apiRequest<unknown[]>(`${root}/tasks/${encodeURIComponent(taskId)}/work-products`);
      return array(out).map(normalizeWorkProduct);
    },
    async listResultQuestions(taskId: string): Promise<ResultQuestion[]> {
      const out = await apiRequest<unknown[]>(`${root}/tasks/${encodeURIComponent(taskId)}/result-questions`);
      return array(out).map(normalizeResultQuestion);
    },
    async askResultQuestion(taskId: string, question: string): Promise<ResultQuestion> {
      return normalizeResultQuestion(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/result-questions`,
        mutationRequest("POST", "result-question", { question }),
      ));
    },
    async createTaskWorkProduct(
      taskId: string,
      product: Pick<WorkProduct, "kind" | "title" | "summary"> & Partial<Pick<WorkProduct, "uri" | "content_hash">>,
    ): Promise<WorkProduct> {
      return normalizeWorkProduct(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/work-products`,
        mutationRequest("POST", "work-product-create", product),
      ));
    },
    verifyWorkProduct(productId: string): Promise<Record<string, unknown>> {
      return apiRequest(`${root}/work-products/${encodeURIComponent(productId)}/verify`, mutationRequest("POST", "work-product-verify"));
    },
    async listTaskWakes(taskId: string): Promise<WakeRequest[]> {
      const out = await apiRequest<unknown[]>(`${root}/tasks/${encodeURIComponent(taskId)}/wakes`);
      return array(out).map(normalizeWake);
    },
    async retryWake(wakeId: string): Promise<WakeRequest> {
      return normalizeWake(await apiRequest(`${root}/wakes/${encodeURIComponent(wakeId)}/retry`, mutationRequest("POST", "wake-retry")));
    },
    async cancelWake(wakeId: string): Promise<WakeRequest> {
      return normalizeWake(await apiRequest(`${root}/wakes/${encodeURIComponent(wakeId)}/cancel`, mutationRequest("POST", "wake-cancel")));
    },
    getHeartbeatContext(taskId: string, afterSequence = 0): Promise<Record<string, unknown>> {
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/heartbeat-context?after_sequence=${afterSequence}`);
    },
    async getHealth(): Promise<OrchestrationHealth> {
      try {
        return normalizeHealth(await apiRequest(`${root}/health`));
      } catch (error) {
        // Readiness failures intentionally use HTTP 503 while still carrying the
        // complete operational snapshot. Preserve that snapshot for recovery UI.
        const payload = record((error as { payload?: unknown } | null)?.payload);
        if (Object.prototype.hasOwnProperty.call(payload, "ready")) return normalizeHealth(payload);
        throw error;
      }
    },
    async listTaskRuns(taskId: string, offset = 0, limit = 200): Promise<TaskRunsPage> {
      const query = new URLSearchParams({ offset: String(offset), limit: String(limit) });
      const out = record(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/runs?${query.toString()}`,
      ));
      const page = normalizeAuditPage(out) || { has_more: false };
      return {
        task_id: text(out.task_id, taskId),
        runs: array(out.runs).map((raw) => normalizeRun(raw)),
        ...page,
      };
    },
    async listTaskGates(taskId: string, offset = 0, limit = 200): Promise<TaskGatesPage> {
      const query = new URLSearchParams({ offset: String(offset), limit: String(limit) });
      const out = record(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/gates?${query.toString()}`,
      ));
      const page = normalizeAuditPage(out) || { has_more: false };
      return {
        task_id: text(out.task_id, taskId),
        gates: array(out.gates || out.attention).map(normalizeGate),
        ...page,
      };
    },
    async listTaskEvidence(taskId: string, offset = 0, limit = 200): Promise<TaskEvidencePage> {
      const query = new URLSearchParams({ offset: String(offset), limit: String(limit) });
      const out = record(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/evidence?${query.toString()}`,
      ));
      const page = normalizeAuditPage(out) || { has_more: false };
      return {
        task_id: text(out.task_id, taskId),
        evidence: array(out.evidence).map((raw) => {
          const item = record(raw);
          const payload = record(item.payload);
          return {
            id: text(item.id || item.evidence_id),
            title: text(item.title || payload.title, humanTitle(item.kind || "evidence")),
            kind: lower(item.kind, "other"),
            summary: text(item.summary || payload.summary || payload.message) || undefined,
            uri: text(item.uri || item.blob_uri || payload.uri) || undefined,
            run_id: text(item.run_id) || undefined,
            created_at: text(item.created_at) || undefined,
            content_hash: text(item.content_hash || payload.content_hash || record(payload.blob).sha256) || undefined,
            payload,
            subject: Object.keys(record(item.subject || payload.subject)).length
              ? record(item.subject || payload.subject)
              : undefined,
            subject_matches: item.subject_matches == null && payload.subject_matches == null
              ? undefined
              : Boolean(item.subject_matches ?? payload.subject_matches),
            missing_criteria: stringList(item.missing_criteria || payload.missing_criteria),
            actor: text(item.actor || item.created_by || payload.created_by || payload.resolved_by) || undefined,
          };
        }),
        ...page,
      };
    },
    async listDeadLetters(offset = 0, limit = 100): Promise<OutboxDeadLetterPage> {
      const query = new URLSearchParams({ offset: String(offset), limit: String(limit) });
      const out = record(await apiRequest(`${root}/outbox/dead-letters?${query.toString()}`));
      const page = normalizeAuditPage(out) || { has_more: false };
      return {
        items: array(out.items).map((raw) => {
          const item = record(raw);
          return {
            id: text(item.id),
            event_id: text(item.event_id),
            topic: text(item.topic),
            attempts: number(item.attempts),
            last_error: text(item.last_error) || null,
            dead_lettered_at: text(item.dead_lettered_at) || null,
            payload: record(item.payload),
          };
        }),
        ...page,
      };
    },
    requeueDeadLetter(
      outboxId: string,
      command: DeadLetterRequeueCommand,
    ): Promise<{ id: string; event_id: string; status: string; attempts: number; replayed?: boolean }> {
      return apiRequest(
        `${root}/outbox/dead-letters/${encodeURIComponent(outboxId)}/requeue`,
        jsonRequest(
          "POST",
          { actor: command.actor, reason: command.reason },
          { "Idempotency-Key": command.idempotencyKey },
        ),
      );
    },
    async getRunTranscript(taskId: string, runId: string): Promise<RunTranscript> {
      const out = record(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/runs/${encodeURIComponent(runId)}/transcript`,
      ));
      return {
        task_id: text(out.task_id, taskId),
        run_id: text(out.run_id, runId),
        session_id: text(out.session_id) || null,
        available: Boolean(out.available),
        title: text(out.title) || undefined,
        messages: array(out.messages).map((message) => record(message)),
        message_count: number(out.message_count),
        offset: number(out.offset),
        limit: number(out.limit, 500),
        has_more: Boolean(out.has_more),
        next_offset: out.next_offset == null ? null : number(out.next_offset),
        updated_at: text(out.updated_at) || null,
      };
    },
    async getRunActivity(
      taskId: string,
      runId: string,
      options: { afterSequence?: number; beforeSequence?: number; latest?: boolean; limit?: number } = {},
    ): Promise<RunActivityPage> {
      const query = new URLSearchParams({
        limit: String(options.limit ?? 500),
        latest: String(options.latest ?? true),
      });
      if (options.afterSequence != null) query.set("after_sequence", String(options.afterSequence));
      if (options.beforeSequence != null) query.set("before_sequence", String(options.beforeSequence));
      const out = record(await apiRequest(
        `${root}/tasks/${encodeURIComponent(taskId)}/runs/${encodeURIComponent(runId)}/activity?${query.toString()}`,
      ));
      return {
        task_id: text(out.task_id, taskId),
        run_id: text(out.run_id, runId),
        activity: array(out.activity).map((raw) => {
          const item = record(raw);
          return {
            sequence: number(item.sequence),
            id: text(item.id),
            event_key: text(item.event_key),
            source_id: text(item.source_id),
            kind: text(item.kind, "lifecycle") as RunActivityPage["activity"][number]["kind"],
            status: text(item.status, "info") as RunActivityPage["activity"][number]["status"],
            title: text(item.title, "Agent activity"),
            summary: text(item.summary),
            detail: record(item.detail),
            created_at: text(item.created_at),
          };
        }),
        has_more: Boolean(out.has_more),
        next_sequence: out.next_sequence == null ? null : number(out.next_sequence),
        next_parameter: text(out.next_parameter) || undefined,
        order: text(out.order, "oldest_to_newest"),
        privacy: {
          reasoning: text(record(out.privacy).reasoning, "provider_summary_only"),
          tool_output: text(record(out.privacy).tool_output, "metadata_only"),
        },
      };
    },
    async submitTask(id: string): Promise<OrchestrationTaskDetail> {
      return normalizeTaskDetail(await apiRequest(
        `${root}/tasks/${encodeURIComponent(id)}/submit`,
        jsonRequest("POST"),
      ));
    },
    async pauseTask(id: string): Promise<OrchestrationTaskDetail> {
      return normalizeTaskDetail(await apiRequest(
        `${root}/tasks/${encodeURIComponent(id)}/pause`,
        jsonRequest("POST"),
      ));
    },
    async resumeTask(id: string): Promise<OrchestrationTaskDetail> {
      return normalizeTaskDetail(await apiRequest(
        `${root}/tasks/${encodeURIComponent(id)}/resume`,
        jsonRequest("POST"),
      ));
    },
    async cancelTask(id: string): Promise<OrchestrationTaskDetail> {
      return normalizeTaskDetail(await apiRequest(
        `${root}/tasks/${encodeURIComponent(id)}/cancel`,
        jsonRequest("POST"),
      ));
    },
    async archiveTask(id: string): Promise<OrchestrationTaskDetail> {
      return normalizeTaskDetail(await apiRequest(
        `${root}/tasks/${encodeURIComponent(id)}/archive`,
        jsonRequest("POST"),
      ));
    },
    async restoreTask(id: string): Promise<OrchestrationTaskDetail> {
      return normalizeTaskDetail(await apiRequest(
        `${root}/tasks/${encodeURIComponent(id)}/restore`,
        jsonRequest("POST"),
      ));
    },
    async resolveAttention(
      taskId: string,
      gateId: string,
      decision: string,
      response?: string,
      expectedVersion?: number,
      idempotencyKey?: string,
    ): Promise<{ ok: boolean; gate?: AttentionGate }> {
      const fingerprint = JSON.stringify([taskId, gateId, decision, response || "", expectedVersion ?? null]);
      const stableKey = idempotencyKey
        || resolutionKeys.get(fingerprint)
        || createClientIdempotencyKey(`gate-${gateId}`);
      if (!resolutionKeys.has(fingerprint) && resolutionKeys.size >= 256) {
        const oldest = resolutionKeys.keys().next().value;
        if (oldest !== undefined) resolutionKeys.delete(oldest);
      }
      resolutionKeys.set(fingerprint, stableKey);
      return apiRequest(`${root}/tasks/${encodeURIComponent(taskId)}/attention/${encodeURIComponent(gateId)}/resolve`,
        jsonRequest("POST", {
          decision,
          ...(response ? { response } : {}),
          ...(expectedVersion === undefined ? {} : { expected_version: expectedVersion }),
          idempotency_key: stableKey,
        }));
    },

    async listAgentProfiles(): Promise<AgentProfileSummary[]> {
      const out = await apiRequest<AgentProfileSummary[] | Record<string, unknown>>(`${root}/agent-profiles`);
      return listFrom<unknown>(out, "profiles").map(normalizeProfileSummary);
    },
    async getAgentProfile(id: string): Promise<AgentProfileDetail> {
      const out = await apiRequest<AgentProfileDetail | Record<string, unknown>>(
        `${root}/agent-profiles/${encodeURIComponent(id)}`,
      );
      return normalizeProfileDetail(out);
    },
    async createAgentProfile(spec: AgentProfileSpec): Promise<AgentProfileDetail> {
      return normalizeProfileDetail(await apiRequest(`${root}/agent-profiles`, jsonRequest("POST", { spec })));
    },
    async cloneAgentProfile(id: string, newProfileId: string, displayName: string): Promise<AgentProfileDetail> {
      return normalizeProfileDetail(await apiRequest(
        `${root}/agent-profiles/${encodeURIComponent(id)}/clone`,
        jsonRequest("POST", { new_profile_id: newProfileId, overrides: { display_name: displayName } }),
      ));
    },
    async createAgentProfileDraft(id: string, baseVersion?: number): Promise<AgentProfileDetail> {
      return normalizeProfileDetail(await apiRequest(
        `${root}/agent-profiles/${encodeURIComponent(id)}/draft`,
        jsonRequest("POST", baseVersion === undefined ? {} : { base_version: baseVersion }),
      ));
    },
    async saveAgentProfileDraft(id: string, spec: AgentProfileSpec, etag: string): Promise<AgentProfileDetail> {
      return normalizeProfileDetail(await apiRequest(
        `${root}/agent-profiles/${encodeURIComponent(id)}/draft`,
        jsonRequest("PUT", { spec }, { "If-Match": etag }),
      ));
    },
    validateAgentProfile(id: string, spec: AgentProfileSpec): Promise<ValidationReport> {
      return apiRequest(
        `${root}/agent-profiles/${encodeURIComponent(id)}/draft/validate`,
        jsonRequest("POST", { spec }),
      );
    },
    async publishAgentProfile(id: string, etag: string): Promise<AgentProfileDetail> {
      return normalizeProfileDetail(await apiRequest(
        `${root}/agent-profiles/${encodeURIComponent(id)}/draft/publish`,
        jsonRequest("POST", {}, { "If-Match": etag }),
      ));
    },

    async listModelPolicies(): Promise<ModelPolicySummary[]> {
      const out = await apiRequest<ModelPolicySummary[] | Record<string, unknown>>(`${root}/model-policies`);
      return listFrom<unknown>(out, "policies").map(normalizePolicySummary);
    },
    async getModelPolicy(id: string): Promise<ModelPolicyDetail> {
      const out = await apiRequest<ModelPolicyDetail | Record<string, unknown>>(
        `${root}/model-policies/${encodeURIComponent(id)}`,
      );
      return normalizePolicyDetail(out);
    },
    async createModelPolicy(spec: ModelRoutingPolicySpec): Promise<ModelPolicyDetail> {
      return normalizePolicyDetail(await apiRequest(`${root}/model-policies`, jsonRequest("POST", { spec })));
    },
    async cloneModelPolicy(id: string, newPolicyId: string): Promise<ModelPolicyDetail> {
      return normalizePolicyDetail(await apiRequest(`${root}/model-policies/${encodeURIComponent(id)}/clone`, jsonRequest("POST", { new_policy_id: newPolicyId })));
    },
    async createModelPolicyDraft(id: string, baseVersion?: number): Promise<ModelPolicyDetail> {
      return normalizePolicyDetail(await apiRequest(
        `${root}/model-policies/${encodeURIComponent(id)}/draft`,
        jsonRequest("POST", baseVersion === undefined ? {} : { base_version: baseVersion }),
      ));
    },
    async saveModelPolicyDraft(id: string, spec: ModelRoutingPolicySpec, etag: string): Promise<ModelPolicyDetail> {
      return normalizePolicyDetail(await apiRequest(
        `${root}/model-policies/${encodeURIComponent(id)}/draft`,
        jsonRequest("PUT", { spec }, { "If-Match": etag }),
      ));
    },
    validateModelPolicy(id: string, spec: ModelRoutingPolicySpec): Promise<ValidationReport> {
      return apiRequest(
        `${root}/model-policies/${encodeURIComponent(id)}/draft/validate`,
        jsonRequest("POST", { spec }),
      );
    },
    async publishModelPolicy(id: string, etag: string): Promise<ModelPolicyDetail> {
      return normalizePolicyDetail(await apiRequest(
        `${root}/model-policies/${encodeURIComponent(id)}/draft/publish`,
        jsonRequest("POST", {}, { "If-Match": etag }),
      ));
    },
    simulateModelPolicy(
      id: string,
      spec: ModelRoutingPolicySpec,
      facts: RoutingSimulationFacts,
    ): Promise<RoutingSimulationResult> {
      return apiRequest<unknown>(
        `${root}/model-policies/${encodeURIComponent(id)}/draft/simulate`,
        jsonRequest("POST", { policy: spec, request: facts }),
      ).then(normalizeSimulation);
    },
    async getModelCatalog(): Promise<RoutingModelDescriptor[]> {
      const out = await apiRequest<RoutingModelDescriptor[] | Record<string, unknown>>(`${root}/model-catalog`);
      return listFrom<unknown>(out, "models").map(normalizeCatalogModel);
    },
    async getSubscriptionRuntimes(refresh = false): Promise<SubscriptionRuntimeDescriptor[]> {
      const suffix = refresh ? "?refresh=true" : "";
      const out = await apiRequest<unknown[] | Record<string, unknown>>(`${root}/subscription-runtimes${suffix}`);
      return listFrom<unknown>(out, "runtimes").map(normalizeSubscriptionRuntime);
    },
    async getRuntimePresets(): Promise<RuntimePresetDescriptor[]> {
      const out = await apiRequest<unknown[] | Record<string, unknown>>(`${root}/runtime-presets`);
      const envelope = record(out);
      const defaultPresetId = text(envelope.default_preset_id);
      const rows = Array.isArray(out)
        ? out
        : Array.isArray(envelope.items)
          ? envelope.items
          : listFrom<unknown>(envelope, "presets");
      return rows.map(normalizeRuntimePreset).map((preset) => ({
        ...preset,
        is_default: preset.is_default || Boolean(defaultPresetId && preset.id === defaultPresetId),
      })).filter((preset) => Boolean(preset.id));
    },
  };
}

export type OrchestrationApi = ReturnType<typeof createOrchestrationApi>;
