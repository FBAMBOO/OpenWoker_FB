-- Frozen adaptive execution strategy and direct input bindings.
-- Rollback: additive only. Running tasks retain their frozen strategy/version;
-- downgrade affects new routing only and does not delete published strategy data.

CREATE TABLE orch_execution_strategies (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    version INTEGER NOT NULL CHECK (version > 0),
    status TEXT NOT NULL CHECK (status IN ('draft', 'published', 'superseded')),
    archetype TEXT NOT NULL CHECK (archetype IN (
        'repo_analysis', 'code_change', 'focused_question',
        'document_generation', 'incident_triage', 'custom'
    )),
    template_id TEXT NOT NULL,
    contract_id TEXT NOT NULL REFERENCES orch_quality_contracts(id),
    snapshot_id TEXT NOT NULL REFERENCES orch_repository_snapshots(id),
    rubric_id TEXT NOT NULL,
    rubric_version INTEGER NOT NULL CHECK (rubric_version > 0),
    assessment_json TEXT NOT NULL,
    effective_policy_json TEXT NOT NULL,
    policy_provenance_json TEXT NOT NULL,
    feature_flags_json TEXT NOT NULL,
    nodes_json TEXT NOT NULL,
    edges_json TEXT NOT NULL,
    semantic_scorer_node_key TEXT NOT NULL,
    budget_profile_json TEXT NOT NULL,
    max_repair_attempts INTEGER NOT NULL DEFAULT 2 CHECK (max_repair_attempts >= 0 AND max_repair_attempts <= 2),
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT,
    UNIQUE(task_id, version),
    UNIQUE(task_id, content_hash),
    FOREIGN KEY(rubric_id, rubric_version) REFERENCES orch_quality_rubrics(id, version)
);

CREATE TABLE orch_node_input_bindings (
    id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES orch_execution_strategies(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    consumer_node_key TEXT NOT NULL,
    source_type TEXT NOT NULL CHECK (source_type IN (
        'contract', 'snapshot', 'inventory', 'evidence_bundle', 'artifact', 'finding_set'
    )),
    source_selector_json TEXT NOT NULL,
    requirement TEXT NOT NULL CHECK (requirement IN ('required', 'recommended', 'optional')),
    delivery_mode TEXT NOT NULL CHECK (delivery_mode IN (
        'inline_metadata', 'on_demand', 'mounted_readonly'
    )),
    max_bytes INTEGER NOT NULL CHECK (max_bytes > 0),
    must_verify_hash INTEGER NOT NULL CHECK (must_verify_hash IN (0, 1)),
    UNIQUE(strategy_id, position)
);

ALTER TABLE orch_tasks ADD COLUMN active_strategy_id TEXT REFERENCES orch_execution_strategies(id);

CREATE INDEX orch_strategies_task_status
ON orch_execution_strategies(task_id, status, version DESC);
CREATE INDEX orch_bindings_consumer
ON orch_node_input_bindings(strategy_id, consumer_node_key, source_type);

CREATE TRIGGER orch_published_strategy_no_update
BEFORE UPDATE ON orch_execution_strategies
WHEN OLD.status IN ('published', 'superseded')
BEGIN SELECT RAISE(ABORT, 'published execution strategies are immutable'); END;
CREATE TRIGGER orch_published_strategy_no_delete
BEFORE DELETE ON orch_execution_strategies
WHEN OLD.status IN ('published', 'superseded')
BEGIN SELECT RAISE(ABORT, 'published execution strategies are immutable'); END;
CREATE TRIGGER orch_published_binding_no_update
BEFORE UPDATE ON orch_node_input_bindings
WHEN EXISTS (
    SELECT 1 FROM orch_execution_strategies s
    WHERE s.id = OLD.strategy_id AND s.status IN ('published', 'superseded')
)
BEGIN SELECT RAISE(ABORT, 'published strategy bindings are immutable'); END;
CREATE TRIGGER orch_published_binding_no_delete
BEFORE DELETE ON orch_node_input_bindings
WHEN EXISTS (
    SELECT 1 FROM orch_execution_strategies s
    WHERE s.id = OLD.strategy_id AND s.status IN ('published', 'superseded')
)
BEGIN SELECT RAISE(ABORT, 'published strategy bindings are immutable'); END;
