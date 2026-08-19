export const ORCHESTRATION_STAGES = [
  "intake",
  "complexity_assessment",
  "clarification",
  "planning",
  "execution_review_test",
  "inter_step_evaluation",
  "final_acceptance",
  "archive",
] as const;

export type OrchestrationStage = (typeof ORCHESTRATION_STAGES)[number];

export * from "./taskQuality.generated";
import type {
  TaskQualityArchetype,
  TaskQualityArtifactStatus,
  TaskQualityBudgetMode,
  TaskQualityBudgetStatus,
  TaskQualityStatus,
  TaskQualityWorkflowStatus,
} from "./taskQuality.generated";

export interface PrimaryDeliverable {
  artifact_id: string;
  deliverable_id: string;
  filename: string;
  mime_type: string;
  sha256: string;
  byte_size: number;
  version: number;
  status: TaskQualityArtifactStatus;
}

export interface QualityVerdict {
  evaluation_id: string;
  decision: "publish" | "repair" | "needs_attention" | "reject" | string;
  rubric_score_id?: string | null;
  total_score?: number | null;
  finding_ids: string[];
  content_hash: string;
}

export interface EffectiveBudget {
  ledger_id?: string;
  mode: TaskQualityBudgetMode | "unconfigured";
  source?: string | null;
  used: Record<string, number>;
  reserved: Record<string, number>;
  remaining: Record<string, number>;
  limit: Record<string, number | null>;
  provider_usage?: Record<string, unknown>;
  over_budget?: boolean;
  fencing_token?: number;
}

export interface RepositoryTargetSummary {
  repo: string;
  repo_root: string;
  snapshot_ref?: string | null;
  short_sha?: string | null;
  dirty: boolean;
  snapshot_id: string;
}

export interface TaskQualityRequirement {
  id: string;
  category: string;
  text: string;
  required: boolean;
  hard_gate: boolean;
  source: string;
  confidence?: number | null;
  verification_method: string;
}

export interface TaskQualityDeliverableSpec {
  id: string;
  kind: string;
  filename: string;
  mime_type: string;
  required: boolean;
  primary: boolean;
  required_sections: string[];
  result_schema_id: string;
}

export interface TaskQualityContract {
  id: string;
  task_id: string;
  version: number;
  status: "draft" | "published" | "superseded";
  title: string;
  objective: string;
  archetype: TaskQualityArchetype;
  quality_profile_id: string;
  original_prompt_hash: string;
  requirements: TaskQualityRequirement[];
  deliverables: TaskQualityDeliverableSpec[];
  constraints: Array<Record<string, unknown>>;
  non_goals: string[];
  content_hash: string;
  etag: string;
}

export interface RepositorySnapshotV2 extends Record<string, unknown> {
  id: string;
  task_id: string;
  version: number;
  status: string;
  repo_root: string;
  project_root: string;
  snapshot_kind: string;
  selected_ref?: string | null;
  commit_oid?: string | null;
  dirty: boolean;
  manifest_hash: string;
  resolution_confidence: number;
  resolution_reason: string;
}

export interface ExecutionStrategyV2 extends Record<string, unknown> {
  id: string;
  task_id: string;
  version: number;
  archetype: TaskQualityArchetype;
  template_id: string;
  assessment: {
    cognitive_complexity: number;
    operational_risk: number;
    evidence_workload: number;
    rationale: string[];
  };
  nodes: Array<Record<string, unknown>>;
  edges: Array<Record<string, unknown>>;
  effective_policy: Record<string, unknown>;
  policy_provenance?: Record<string, unknown>;
  budget_profile: Record<string, unknown>;
  max_repair_attempts: number;
  content_hash: string;
}

export interface TaskDraftAnalysisV2 {
  id: string;
  task_id: string;
  status: string;
  contract: TaskQualityContract;
  contract_etag: string;
  target_resolution: Record<string, unknown>;
  request_hash: string;
}

export interface TaskQualityPage<T> {
  items: T[];
  offset: number;
  limit: number;
  has_more: boolean;
  next_offset?: number | null;
  cursor?: string | null;
  next_cursor?: string | null;
  pagination?: "offset" | "cursor";
}

export interface QualityFinding extends Record<string, unknown> {
  id: string;
  severity: string;
  category: string;
  message: string;
  blocking: boolean;
  repairable: boolean;
  status: string;
  section_id?: string | null;
  suggested_fix?: string | null;
}

export interface QualityBundleV2 {
  task_id: string;
  quality_status: TaskQualityStatus;
  quality_reason_code?: string | null;
  quality_verdict?: QualityVerdict | null;
  gates: TaskQualityPage<Record<string, unknown>>;
  findings: TaskQualityPage<QualityFinding>;
  evaluations: TaskQualityPage<Record<string, unknown>>;
  waivers: TaskQualityPage<Record<string, unknown>>;
}

export interface TaskQualityApiError {
  error: {
    code: string;
    message: string;
    retryable: boolean;
    details: Record<string, unknown>;
    correlation_id: string;
  };
}

export interface TaskQualityBenchmarkSuite {
  id: string;
  name: string;
  stack: string;
  version: number;
  snapshot_artifact_id: string;
  prompt_hash: string;
  candidate_ids: string[];
  baseline_candidate: string;
  thresholds: Record<string, number>;
  content_hash: string;
  promoted_baseline?: Record<string, unknown>;
}

export interface TaskQualityBenchmarkRun {
  id: string;
  suite_id: string;
  suite_version: number;
  suite_hash: string;
  snapshot_artifact_id: string;
  prompt_hash: string;
  candidate_id: string;
  status: "pass" | "fail";
  metrics: Record<string, unknown>;
  failures: Array<Record<string, unknown>>;
  created_at: string;
  completed_at: string;
  content_hash: string;
}

export interface TaskQualityBenchmarkComparison {
  run_id: string;
  suite_id: string;
  candidate_id: string;
  baseline: Record<string, unknown>;
  current_metrics: Record<string, unknown>;
  baseline_metrics: Record<string, unknown>;
  deltas: Record<string, number>;
  quality_score_regression: boolean;
}
export type WorkStatus =
  | "pending"
  | "ready"
  | "queued"
  | "claimed"
  | "running"
  | "waiting"
  | "waiting_human"
  | "waiting_child"
  | "waiting_gate"
  | "paused"
  | "blocked"
  | "needs_reconciliation"
  | "canceling"
  | "failed"
  | "completed"
  | "succeeded"
  | "skipped"
  | "cancelled"
  | "canceled"
  | "timed_out"
  | "lost"
  | "archived";

export interface TaskStageState {
  stage: OrchestrationStage | string;
  status: WorkStatus;
  sequence?: number;
  attempt?: number;
  started_at?: string | null;
  completed_at?: string | null;
}

export interface OrchestrationTaskSummary {
  id: string;
  title: string;
  objective?: string;
  status: WorkStatus | "draft";
  stage: OrchestrationStage | string;
  progress?: number;
  attention_count?: number;
  updated_at: string;
  created_at?: string;
  profile_name?: string;
  profile_version?: number;
  parent_task_id?: string | null;
  parent_run_id?: string | null;
  terminal_outcome?: WorkStatus | "draft";
  task_quality_v2?: boolean;
  workflow_status?: TaskQualityWorkflowStatus;
  workflow_resume_status?: TaskQualityWorkflowStatus | null;
  quality_status?: TaskQualityStatus;
  artifact_status?: TaskQualityArtifactStatus;
  budget_status?: TaskQualityBudgetStatus;
  quality_reason_code?: string | null;
  attention_reason?: string | null;
  archetype?: TaskQualityArchetype | null;
  primary_deliverable?: PrimaryDeliverable | null;
  quality_verdict?: QualityVerdict | null;
  quality_score?: number | null;
  hard_gate_status?: "pending" | "pass" | "fail" | "unknown" | string;
  effective_budget?: EffectiveBudget;
  budget_utilization_percent?: number | null;
  target?: RepositoryTargetSummary | null;
  has_waiver?: boolean;
  run_summary?: { nodes: number; repairs: number };
  created_by?: string | null;
  started_at?: string | null;
}

export interface CreateOrchestrationTask {
  /** Stable for one create intent so a retry cannot create a duplicate task. */
  idempotency_key?: string;
  title?: string;
  objective: string;
  domain: "code" | "knowledge";
  /** Hard execution boundary; never inferred from objective text. */
  read_only: boolean;
  workspace?: string;
  acceptance_criteria: string[];
  constraints?: string[];
  profile_id?: string;
  model_policy_id?: string;
  /** Role-aware runtime assignment. Mutually exclusive with requested_model. */
  runtime_preset_id?: string;
  requested_model?: string;
  require_review?: boolean;
  require_tests?: boolean;
  auto_start?: boolean;
  publish_brief?: boolean;
  brief?: TaskBriefInput;
  context_refs?: ContextRefInput[];
}

export interface AcceptanceCriterion {
  id: string;
  text: string;
  required?: boolean;
  verification?: string;
}

export interface BriefDeliverable {
  id: string;
  kind: string;
  title?: string;
  required?: boolean;
}

export interface TaskBriefInput {
  title: string;
  objective: string;
  background: string;
  scope: Record<string, unknown>;
  instructions: string[];
  constraints: string[];
  non_goals: string[];
  acceptance_criteria: AcceptanceCriterion[];
  deliverables: BriefDeliverable[];
  result_contract: Record<string, unknown>;
}

export interface TaskBrief extends TaskBriefInput {
  id: string;
  task_id: string;
  revision: number;
  status: "draft" | "published" | "superseded";
  content_hash: string;
  created_by_task_id?: string | null;
  created_by_run_id?: string | null;
  created_at: string;
  published_at?: string | null;
}

export interface ContextRefInput {
  requirement: "required" | "recommended" | "optional";
  ref_type: string;
  display_name: string;
  selection_reason: string;
  locator: Record<string, unknown>;
  delivery_mode?: "metadata_only" | "excerpt" | "on_demand";
  summary?: string;
  mime_type?: string | null;
  content_hash?: string | null;
  byte_size?: number | null;
  token_estimate?: number | null;
  provenance?: Record<string, unknown>;
  trust_level?: string;
}

export interface ContextRef extends ContextRefInput {
  id: string;
  task_id: string;
  brief_id: string;
  created_at: string;
  read_count?: number;
  last_read_at?: string | null;
  last_read_by_run_id?: string | null;
}

export interface TaskRelation {
  id: string;
  from_task_id: string;
  to_task_id: string;
  relation_type: "parent" | "blocks" | "reviews" | "related" | "supersedes" | string;
  metadata: Record<string, unknown>;
  created_at: string;
  removed_at?: string | null;
}

export interface TaskComment {
  id: string;
  task_id: string;
  sequence: number;
  author_type: string;
  author_id: string;
  created_by_run_id?: string | null;
  body_markdown: string;
  metadata: Record<string, unknown>;
  reply_to_comment_id?: string | null;
  created_at: string;
}

export interface TaskCommentDelta {
  task_id: string;
  after_sequence: number;
  latest_sequence: number;
  new_count: number;
  comments: TaskComment[];
  fallback_fetch_needed: boolean;
}

export interface WorkProduct {
  id: string;
  task_id: string;
  run_id?: string | null;
  kind: string;
  title: string;
  summary: string;
  evidence_id?: string | null;
  artifact_id?: string | null;
  uri?: string | null;
  content_hash?: string | null;
  metadata: Record<string, unknown>;
  verification_status: string;
  created_by: string;
  created_at: string;
}

export interface ResultQuestion {
  id: string;
  task_id: string;
  source_task_id: string;
  question: string;
  status: WorkStatus | "draft";
  terminal_outcome?: WorkStatus | "draft";
  stage: OrchestrationStage | string;
  progress?: number;
  answer?: string | null;
  answer_work_product_id?: string | null;
  answer_artifact_id?: string | null;
  source_work_product_ids: string[];
  created_at: string;
  updated_at: string;
}

export interface WakeRequest {
  id: string;
  target_task_id: string;
  target_run_id?: string | null;
  reason: string;
  source_task_id?: string | null;
  source_run_id?: string | null;
  source_event_id?: string | null;
  payload: Record<string, unknown>;
  status: "pending" | "deferred" | "claimed" | "delivered" | "completed" | "failed" | "canceled" | string;
  attempts: number;
  coalesced_count: number;
  last_error?: string | null;
  not_before?: string | null;
  claimed_by?: string | null;
  claimed_until?: string | null;
  delivered_at?: string | null;
  completed_at?: string | null;
  created_at: string;
  updated_at: string;
}

export interface HandoffSummary {
  protocol?: "structured" | "legacy" | string;
  loaded?: boolean;
  context?: { ref_count?: number; required_count?: number; estimated_tokens?: number };
  relations?: Record<string, number>;
  comments?: { count: number; latest_sequence: number; content_included: boolean };
  work_products?: { count: number };
  wakes?: { count: number; pending: number; failed: number };
}

export interface AttentionAction {
  id: string;
  label: string;
  tone?: "primary" | "neutral" | "danger";
  requires_response?: boolean;
}

export type AcceptanceCheckStatus = "pass" | "fail" | "unknown" | string;

export interface GateVerificationReport {
  node_id: string;
  node_key?: string;
  run_id?: string | null;
  role?: string;
  status: AcceptanceCheckStatus;
  criteria: Record<string, AcceptanceCheckStatus>;
  summary?: string;
  findings: string[];
  source?: string;
}

export interface AttentionGate {
  id: string;
  kind: "approval" | "question" | "permission" | "budget" | "review" | "conflict" | string;
  title: string;
  description?: string;
  status: "pending" | "resolved";
  actions?: AttentionAction[];
  response_placeholder?: string;
  created_at?: string;
  resolved_at?: string | null;
  resolution?: string | null;
  version?: number;
  /** Final-acceptance audit material retained from the durable gate prompt. */
  criteria?: Record<string, AcceptanceCheckStatus>;
  verification?: GateVerificationReport[];
  policy_reasons?: string[];
  /** Failed execution attempts retained by reconciliation gates. */
  failed_runs?: AgentRun[];
  /** Runs whose isolated workspace could not be published. */
  workspace_commit_failures?: AgentRun[];
}

export interface TaskNode {
  id: string;
  key?: string;
  title: string;
  description?: string;
  kind?: string;
  status: WorkStatus;
  depends_on: string[];
  profile_name?: string;
  profile_version?: number;
  run_ids?: string[];
}

export interface TaskEdge {
  from: string;
  to: string;
}

export interface AgentRun {
  id: string;
  session_id?: string | null;
  node_id?: string;
  parent_run_id?: string | null;
  title: string;
  agent_name?: string;
  status: WorkStatus;
  model_id?: string;
  routing_reason?: string;
  attempt?: number;
  started_at?: string | null;
  completed_at?: string | null;
  summary?: string;
  error_kind?: string;
  error_message?: string;
  budget?: {
    model_calls: number;
    tool_calls: number;
    tokens: number;
    wall_seconds: number;
  } | null;
}

export interface TaskEvidence {
  id: string;
  title: string;
  kind: "artifact" | "file" | "url" | "claim" | "test" | string;
  summary?: string;
  uri?: string;
  run_id?: string;
  created_at?: string;
  content_hash?: string;
  payload?: Record<string, unknown>;
  subject?: Record<string, unknown>;
  subject_matches?: boolean | null;
  missing_criteria?: string[];
  actor?: string;
}

export interface TaskActivity {
  id: string;
  type: string;
  summary: string;
  detail?: string;
  error_kind?: string;
  error_message?: string;
  actor?: string;
  created_at: string;
  stage?: OrchestrationStage | string;
  sequence?: number;
  event_hash?: string;
}

export interface ActivityPage {
  has_more: boolean;
  next_sequence?: number | null;
  next_parameter?: "before_sequence" | "after_sequence" | string;
  page_size?: number;
}

export interface AuditPage {
  has_more: boolean;
  page_size?: number;
  offset?: number;
  limit?: number;
  next_offset?: number | null;
  cursor?: string | null;
  next_cursor?: string | null;
  pagination?: "offset" | "cursor";
  order?: "oldest_to_newest" | string;
}

export interface DetailLimits {
  child_depth?: number;
  attention?: number;
  runs?: number;
  evidence?: number;
  activity?: number;
}

export interface ChildTaskPage {
  truncated: boolean;
  tree_truncated: boolean;
  depth_limit_reached: boolean;
  returned: number;
  total: number;
  tree_row_limit: number;
}

export interface RuntimePage {
  truncated: boolean;
  returned: number;
  limit: number;
}

export interface TaskRunsPage extends AuditPage {
  task_id: string;
  runs: AgentRun[];
}

export interface TaskGatesPage extends AuditPage {
  task_id: string;
  gates: AttentionGate[];
}

export interface TaskEvidencePage extends AuditPage {
  task_id: string;
  evidence: TaskEvidence[];
}

export interface OrchestrationHealth {
  ready: boolean;
  state: string;
  loop_alive: boolean;
  leader?: {
    held: boolean;
    epoch?: number | null;
    heartbeat_alive: boolean;
    last_heartbeat_at?: string | null;
  };
  outbox?: {
    loop_alive: boolean;
    last_success_at?: string | null;
    last_error?: string | null;
    pending: number;
    dead_letters: number;
    oldest_pending_at?: string | null;
    stale: boolean;
    stale_after_seconds?: number;
  };
  closing?: boolean;
  started_at?: string | null;
  last_tick_started_at?: string | null;
  last_success_at?: string | null;
  last_error_at?: string | null;
  last_error?: string | null;
  consecutive_failures?: number;
  handoff?: {
    settings: HandoffRuntimeSettings;
    metrics: {
      counters: Record<string, number>;
      gauges?: Record<string, number>;
      histograms?: Record<string, unknown>;
    };
  };
  task_quality?: {
    schema_version: number;
    metrics: Record<string, unknown>;
    series: Record<string, unknown>;
    alerts: Array<{
      code: string;
      severity: string;
      observed: number;
      message: string;
    }>;
    privacy: string;
  };
}

export interface HandoffRuntimeSettings {
  structured_handoff_enabled: boolean;
  structured_handoff_required_for_new_tasks: boolean;
  legacy_spawn_agent_enabled: boolean;
  default_context_token_budget: number;
  max_context_refs: number;
  max_inline_bytes_per_ref: number;
  max_inline_bytes_total: number;
  max_comment_batch: number;
  wake_coalesce_window_ms: number;
  wake_max_attempts: number;
  wake_backoff_seconds: number;
  context_read_audit_enabled: boolean;
  transcript_sharing_default: boolean;
}

export interface OutboxDeadLetter {
  id: string;
  event_id: string;
  topic: string;
  attempts: number;
  last_error?: string | null;
  dead_lettered_at?: string | null;
  payload: Record<string, unknown>;
}

export interface OutboxDeadLetterPage extends AuditPage {
  items: OutboxDeadLetter[];
}

export interface TranscriptMessage {
  role?: string;
  content?: unknown;
  [key: string]: unknown;
}

export interface RunTranscript {
  task_id: string;
  run_id: string;
  session_id?: string | null;
  available: boolean;
  title?: string;
  messages: TranscriptMessage[];
  message_count: number;
  offset: number;
  limit: number;
  has_more: boolean;
  next_offset?: number | null;
  updated_at?: string | null;
}

export type RunActivityKind = "lifecycle" | "reasoning_summary" | "tool" | "message" | "usage" | "error";
export type RunActivityStatus = "pending" | "running" | "completed" | "failed" | "canceled" | "info";

export interface RunActivity {
  sequence: number;
  id: string;
  event_key: string;
  source_id: string;
  kind: RunActivityKind;
  status: RunActivityStatus;
  title: string;
  summary: string;
  detail: Record<string, unknown>;
  created_at: string;
}

export interface RunActivityPage {
  task_id: string;
  run_id: string;
  activity: RunActivity[];
  has_more: boolean;
  next_sequence?: number | null;
  next_parameter?: "before_sequence" | "after_sequence" | string;
  order: "oldest_to_newest" | string;
  privacy: {
    reasoning: "provider_summary_only" | string;
    tool_output: "metadata_only" | string;
  };
}

export interface SnapshotRef {
  id: string;
  version: number;
  content_hash?: string;
  name?: string;
}

export interface OrchestrationTaskDetail extends OrchestrationTaskSummary {
  result?: Record<string, unknown> | null;
  brief?: TaskBrief;
  handoff_summary?: HandoffSummary;
  stages?: TaskStageState[];
  attention?: AttentionGate[];
  attention_page?: AuditPage;
  nodes?: TaskNode[];
  edges?: TaskEdge[];
  runs?: AgentRun[];
  runs_page?: AuditPage;
  evidence?: TaskEvidence[];
  evidence_page?: AuditPage;
  activity?: TaskActivity[];
  activity_page?: ActivityPage;
  detail_limits?: DetailLimits;
  profile_snapshot?: SnapshotRef;
  model_policy_snapshot?: SnapshotRef;
  children?: OrchestrationTaskSummary[];
  children_details?: OrchestrationTaskDetail[];
  children_page?: ChildTaskPage;
  runtime_page?: RuntimePage;
  quality_refs?: {
    contract_id?: string | null;
    snapshot_id?: string | null;
    strategy_id?: string | null;
    budget_ledger_id?: string | null;
  };
  legacy_quality_projection?: boolean;
  quality_projection_warning?: string;
}

export interface ValidationIssue {
  code: string;
  path: string;
  message: string;
  meta?: Record<string, unknown>;
}

export interface ValidationReport {
  valid: boolean;
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
  resolved?: Record<string, unknown>;
}

export type AgentRole =
  | "orchestrator"
  | "planner"
  | "worker"
  | "reviewer"
  | "tester"
  | "evaluator"
  | "scorer"
  | "explorer"
  | "integrator";

export interface ProfileRef {
  profile_id: string;
  version: number;
}

/** Core profile fields match coworker.orchestration.profiles.AgentProfileDraft exactly.
 * Product extensions stay explicit in metadata until promoted into core fields. */
export interface AgentProfileMetadata extends Record<string, unknown> {
  token_budget?: number | null;
  tool_call_budget?: number | null;
  timeout_seconds?: number | null;
  evidence_required?: boolean;
  tests_required?: boolean;
  review_required?: boolean;
}

export interface AgentProfileSpec {
  schema_version: 1 | 2;
  profile_id: string;
  display_name: string;
  role: AgentRole;
  instructions: string;
  allowed_tools: string[];
  allowed_child_roles: AgentRole[];
  permission_mode: "discuss" | "plan" | "interactive" | "custom" | "auto";
  model_policy: string;
  max_iterations: number;
  max_children: number;
  base?: ProfileRef | null;
  metadata: AgentProfileMetadata;
  communication_policy?: AgentCommunicationPolicy;
}

export interface AgentCommunicationPolicy {
  can_delegate: boolean;
  allowed_child_roles: AgentRole[];
  required_brief_fields: string[];
  max_initial_context_tokens: number;
  max_context_refs: number;
  max_inline_bytes_per_ref: number;
  max_inline_bytes_total: number;
  allowed_context_ref_types: string[];
  allow_full_transcript_reference: boolean;
  allowed_relation_types: string[];
  can_comment: boolean;
  can_mention: boolean;
  can_mention_receive: boolean;
  result_contract_id: string;
}

export interface VersionProvenance {
  profile_id?: string;
  policy_id?: string;
  version: number;
  content_hash: string;
}

export interface AgentProfileSummary {
  id: string;
  name: string;
  role?: AgentRole;
  description?: string;
  builtin: boolean;
  archived: boolean;
  current_version: number | null;
  has_draft: boolean;
  updated_at?: string;
  derived_from?: VersionProvenance;
}

export interface AgentProfileVersion {
  profile_id: string;
  version: number;
  spec: AgentProfileSpec;
  content_hash: string;
  builtin?: boolean;
  cloned_from?: ProfileRef | null;
  published_at?: string;
}

export interface AgentProfileDraft {
  profile_id: string;
  base_version: number | null;
  etag: string;
  spec: AgentProfileSpec;
  validation?: ValidationReport;
  updated_at?: string;
}

export interface AgentProfileDetail extends AgentProfileSummary {
  versions: AgentProfileVersion[];
  current?: AgentProfileVersion | null;
  draft?: AgentProfileDraft | null;
}

export interface ModelRoutingPolicySpec {
  schema_version: 1;
  policy_id: string;
  require_verified: boolean;
  allow_unknown_cost: boolean;
  allowed_providers: string[];
  /** Preserves the user's pool order; quality remains the primary router rank. */
  allowed_models: string[];
  blocked_models: string[];
  fallback_limit: number;
  fallback_for_explicit: boolean;
}

export interface ModelPolicySummary {
  id: string;
  name: string;
  description?: string;
  builtin: boolean;
  archived: boolean;
  current_version: number | null;
  has_draft: boolean;
  updated_at?: string;
  derived_from?: VersionProvenance;
}

export interface ModelPolicyVersion {
  policy_id: string;
  version: number;
  spec: ModelRoutingPolicySpec;
  content_hash: string;
  published_at?: string;
}

export interface ModelPolicyDraft {
  policy_id: string;
  base_version: number | null;
  etag: string;
  spec: ModelRoutingPolicySpec;
  validation?: ValidationReport;
  updated_at?: string;
}

export interface ModelPolicyDetail extends ModelPolicySummary {
  versions: ModelPolicyVersion[];
  current?: ModelPolicyVersion | null;
  draft?: ModelPolicyDraft | null;
}

export interface RoutingModelDescriptor {
  id: string;
  label: string;
  provider: string;
  source?: "curated" | "custom" | "subscription-runtime";
  quality: number;
  configured: boolean;
  in_composer_picker?: boolean;
  availability: "configured" | "unconfigured" | "offline" | "unknown" | "blocked_by_policy" | "unavailable";
  availability_reason?: string;
  verified: boolean;
  capabilities: string[];
  context_window: number | null;
  input_microusd_per_million?: number | null;
  output_microusd_per_million?: number | null;
  latency_rank: number;
  catalog_revision?: string;
  runtime?: RoutingSubscriptionRuntimeMetadata | null;
}

export interface RoutingSubscriptionRuntimeMetadata {
  protocol: string;
  model: string;
  reasoning_effort: string;
  local_owner_only: boolean;
  interactive_only: boolean;
}

export type SubscriptionRuntimeAvailability = "available" | "blocked_by_policy" | "unavailable" | "unknown";

export interface SubscriptionRuntimeHealth {
  runtime_id: string;
  provider: string;
  installed: boolean;
  authenticated: boolean;
  available: boolean;
  policy_eligible: boolean;
  version: string;
  auth_kind: string;
  executable: string;
  reason: string;
  checked_at: number | null;
}

/** A local subscription-backed Agent runtime and its last non-consuming health probe. */
export interface SubscriptionRuntimeDescriptor {
  runtime_id: string;
  provider: string;
  display_name: string;
  command: string;
  model: string;
  reasoning_effort: string;
  quality: number;
  context_window: number | null;
  minimum_cli_version: string;
  protocol: string;
  interactive_only: boolean;
  local_owner_only: boolean;
  capabilities: string[];
  health: SubscriptionRuntimeHealth;
  availability: SubscriptionRuntimeAvailability;
  availability_reason: string;
}

/** One isolated orchestration role assignment in a mixed-runtime preset. */
export interface RuntimePresetRoleAssignment {
  role: AgentRole | string;
  runtime_id: string;
  access?: string;
  fresh_session?: boolean;
  required?: boolean;
}

/** A backend-owned, versionable runtime assignment preset. */
export interface RuntimePresetDescriptor {
  id: string;
  name: string;
  description?: string;
  builtin: boolean;
  is_default: boolean;
  default_for_domains: string[];
  roles: RuntimePresetRoleAssignment[];
  required_runtime_ids: string[];
  available?: boolean;
  unavailable_runtime_ids: string[];
  availability_reason?: string;
}

export interface RoutingSimulationFacts {
  purpose: string;
  required_capabilities: string[];
  input_tokens: number;
  reserved_output_tokens: number;
  minimum_context: number;
  max_cost_microusd: number | null;
  requested_model: string | null;
  preferred_models: string[];
  allowed_providers: string[];
  excluded_models: string[];
  correlation?: Record<string, string>;
}

export interface RoutingSimulationCandidate {
  model_id: string;
  provider?: string;
  eligible: boolean;
  reasons: string[];
  quality: number;
  estimated_cost_microusd?: number | null;
  latency_rank?: number;
  rank?: number | null;
}

export interface RoutingSimulationResult {
  decision_id: string;
  selected_model: string | null;
  fallback_models: string[];
  reason: string;
  evaluations: RoutingSimulationCandidate[];
  catalog_hash?: string;
}
