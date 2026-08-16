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

export interface SnapshotRef {
  id: string;
  version: number;
  content_hash?: string;
  name?: string;
}

export interface OrchestrationTaskDetail extends OrchestrationTaskSummary {
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
  schema_version: 1;
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
