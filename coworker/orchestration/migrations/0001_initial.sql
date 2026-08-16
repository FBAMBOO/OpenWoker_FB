CREATE TABLE orch_tasks (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    creation_hash TEXT NOT NULL,
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    domain TEXT NOT NULL CHECK (domain IN ('code', 'knowledge')),
    workspace TEXT,
    constraints_json TEXT NOT NULL DEFAULT '[]',
    acceptance_criteria_json TEXT NOT NULL DEFAULT '[]',
    complexity_score REAL,
    complexity_level TEXT CHECK (
        complexity_level IS NULL OR complexity_level IN ('trivial', 'standard', 'complex', 'critical')
    ),
    risk_tier TEXT NOT NULL CHECK (risk_tier IN ('low', 'medium', 'high', 'critical')),
    budget_json TEXT NOT NULL DEFAULT '{}',
    policy_json TEXT NOT NULL DEFAULT '{}',
    input_json TEXT NOT NULL DEFAULT '{}',
    output_json TEXT,
    status TEXT NOT NULL CHECK (status IN (
        'draft', 'queued', 'running', 'waiting_human', 'waiting_child',
        'paused', 'blocked', 'needs_reconciliation', 'canceling', 'failed',
        'canceled', 'completed', 'archived'
    )),
    current_stage TEXT NOT NULL CHECK (current_stage IN (
        'intake', 'complexity_assessment', 'clarification', 'planning',
        'execution_review_test', 'inter_step_evaluation', 'final_acceptance', 'archive'
    )),
    active_plan_id TEXT,
    parent_task_id TEXT REFERENCES orch_tasks(id),
    parent_node_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    max_parallel_runs INTEGER NOT NULL DEFAULT 8 CHECK (max_parallel_runs > 0),
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE orch_stage_history (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
    stage TEXT NOT NULL CHECK (stage IN (
        'intake', 'complexity_assessment', 'clarification', 'planning',
        'execution_review_test', 'inter_step_evaluation', 'final_acceptance', 'archive'
    )),
    disposition TEXT NOT NULL CHECK (disposition IN (
        'active', 'completed', 'skipped', 'request_changes', 'canceled', 'failed'
    )),
    entered_at TEXT NOT NULL,
    exited_at TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    command_id TEXT,
    UNIQUE(task_id, sequence_no)
);

CREATE UNIQUE INDEX orch_one_active_stage_per_task
ON orch_stage_history(task_id) WHERE disposition = 'active';

CREATE TABLE orch_plans (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    parent_plan_id TEXT REFERENCES orch_plans(id),
    content_hash TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(task_id, revision)
);

CREATE TABLE orch_nodes (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES orch_plans(id),
    node_key TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    instructions TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL CHECK (kind IN (
        'execute', 'review', 'test', 'integrate', 'evaluate',
        'agent', 'human_gate', 'child_task', 'noop'
    )),
    agent TEXT NOT NULL,
    model TEXT,
    input_json TEXT NOT NULL DEFAULT '{}',
    join_policy TEXT NOT NULL CHECK (join_policy IN ('all', 'any')),
    failure_policy TEXT NOT NULL CHECK (
        failure_policy IN ('fail_fast', 'continue', 'skip_dependents', 'manual')
    ),
    effect_safety TEXT NOT NULL CHECK (
        effect_safety IN ('read_only', 'idempotent', 'non_idempotent')
    ),
    retry_policy_json TEXT NOT NULL,
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds > 0),
    priority INTEGER NOT NULL DEFAULT 0,
    concurrency_key TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(plan_id, node_key)
);

CREATE TABLE orch_edges (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES orch_plans(id),
    from_node_id TEXT NOT NULL REFERENCES orch_nodes(id),
    to_node_id TEXT NOT NULL REFERENCES orch_nodes(id),
    condition TEXT NOT NULL CHECK (condition IN ('success', 'failure', 'terminal', 'always')),
    required INTEGER NOT NULL CHECK (required IN (0, 1)),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    CHECK (from_node_id <> to_node_id),
    UNIQUE(plan_id, from_node_id, to_node_id)
);

CREATE TABLE orch_runs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    plan_id TEXT NOT NULL REFERENCES orch_plans(id),
    node_id TEXT NOT NULL REFERENCES orch_nodes(id),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    status TEXT NOT NULL CHECK (status IN (
        'queued', 'claimed', 'running', 'waiting_gate', 'succeeded',
        'failed', 'timed_out', 'canceled', 'lost', 'skipped'
    )),
    session_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    ready_at TEXT NOT NULL,
    fencing_token INTEGER NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    output_json TEXT,
    error_kind TEXT,
    error_message TEXT,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE(node_id, attempt)
);

CREATE TABLE orch_gates (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    run_id TEXT REFERENCES orch_runs(id),
    node_id TEXT REFERENCES orch_nodes(id),
    kind TEXT NOT NULL CHECK (kind IN (
        'clarification', 'plan_approval', 'permission', 'budget', 'workspace_conflict',
        'reconciliation', 'final_acceptance', 'approval', 'question', 'plan', 'review',
        'recovery', 'child_wait'
    )),
    status TEXT NOT NULL CHECK (status IN ('open', 'approved', 'rejected', 'expired', 'canceled')),
    source_key TEXT NOT NULL UNIQUE,
    prompt_json TEXT NOT NULL,
    resolution_json TEXT,
    resolved_by TEXT,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    opened_at TEXT NOT NULL,
    resolved_at TEXT,
    expires_at TEXT
);

CREATE TABLE orch_evidence (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    plan_id TEXT REFERENCES orch_plans(id),
    node_id TEXT REFERENCES orch_nodes(id),
    run_id TEXT REFERENCES orch_runs(id),
    kind TEXT NOT NULL CHECK (kind IN (
        'audit_event', 'metric', 'external_link', 'note', 'artifact', 'test_result',
        'review', 'decision', 'log', 'checkpoint', 'other'
    )),
    mime_type TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    blob_uri TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE orch_events (
    sequence_no INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    task_id TEXT REFERENCES orch_tasks(id),
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    command_id TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE orch_outbox (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE REFERENCES orch_events(id),
    topic TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    locked_by TEXT,
    locked_until TEXT,
    published_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE orch_leases (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE REFERENCES orch_runs(id),
    owner TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    fencing_token INTEGER NOT NULL CHECK (fencing_token > 0),
    expires_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE orch_commands (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    scope TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('in_progress', 'completed', 'failed')),
    result_json TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX orch_tasks_status_priority ON orch_tasks(status, priority DESC, created_at);
CREATE INDEX orch_stages_task_sequence ON orch_stage_history(task_id, sequence_no);
CREATE INDEX orch_plans_task_revision ON orch_plans(task_id, revision DESC);
CREATE INDEX orch_nodes_plan_key ON orch_nodes(plan_id, node_key);
CREATE INDEX orch_edges_plan_from ON orch_edges(plan_id, from_node_id);
CREATE INDEX orch_edges_plan_to ON orch_edges(plan_id, to_node_id);
CREATE INDEX orch_runs_ready ON orch_runs(status, ready_at, priority DESC, created_at);
CREATE INDEX orch_runs_task_status ON orch_runs(task_id, status);
CREATE INDEX orch_gates_task_status ON orch_gates(task_id, status);
CREATE INDEX orch_evidence_task_created ON orch_evidence(task_id, created_at);
CREATE INDEX orch_events_task_sequence ON orch_events(task_id, sequence_no);
CREATE INDEX orch_outbox_ready ON orch_outbox(published_at, available_at, locked_until);
CREATE INDEX orch_leases_expiry ON orch_leases(expires_at);

CREATE TRIGGER orch_plans_no_update BEFORE UPDATE ON orch_plans
BEGIN SELECT RAISE(ABORT, 'plans are immutable'); END;
CREATE TRIGGER orch_plans_no_delete BEFORE DELETE ON orch_plans
BEGIN SELECT RAISE(ABORT, 'plans are immutable'); END;
CREATE TRIGGER orch_nodes_no_update BEFORE UPDATE ON orch_nodes
BEGIN SELECT RAISE(ABORT, 'plan nodes are immutable'); END;
CREATE TRIGGER orch_nodes_no_delete BEFORE DELETE ON orch_nodes
BEGIN SELECT RAISE(ABORT, 'plan nodes are immutable'); END;
CREATE TRIGGER orch_edges_no_update BEFORE UPDATE ON orch_edges
BEGIN SELECT RAISE(ABORT, 'plan edges are immutable'); END;
CREATE TRIGGER orch_edges_no_delete BEFORE DELETE ON orch_edges
BEGIN SELECT RAISE(ABORT, 'plan edges are immutable'); END;
CREATE TRIGGER orch_evidence_no_update BEFORE UPDATE ON orch_evidence
BEGIN SELECT RAISE(ABORT, 'evidence is immutable'); END;
CREATE TRIGGER orch_evidence_no_delete BEFORE DELETE ON orch_evidence
BEGIN SELECT RAISE(ABORT, 'evidence is immutable'); END;
CREATE TRIGGER orch_events_no_update BEFORE UPDATE ON orch_events
BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;
CREATE TRIGGER orch_events_no_delete BEFORE DELETE ON orch_events
BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;
