-- Task Quality Engine V2: canonical contracts coexist with legacy briefs.
-- Rollback: additive only. Downgrade the application and preserve these tables,
-- rows, and triggers; legacy binaries ignore them. Never drop V2 audit data.

CREATE TABLE orch_quality_contracts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    version INTEGER NOT NULL CHECK (version > 0),
    schema_id TEXT NOT NULL CHECK (schema_id = 'task_contract_v2'),
    schema_version INTEGER NOT NULL CHECK (schema_version = 2),
    status TEXT NOT NULL CHECK (status IN ('draft', 'published', 'superseded')),
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    background TEXT NOT NULL DEFAULT '',
    scope_json TEXT NOT NULL DEFAULT '{}',
    instructions_json TEXT NOT NULL DEFAULT '[]',
    original_prompt_hash TEXT NOT NULL,
    archetype TEXT NOT NULL CHECK (archetype IN (
        'repo_analysis', 'code_change', 'focused_question',
        'document_generation', 'incident_triage', 'custom'
    )),
    language TEXT NOT NULL,
    constraints_json TEXT NOT NULL DEFAULT '[]',
    non_goals_json TEXT NOT NULL DEFAULT '[]',
    quality_profile_id TEXT NOT NULL,
    compiler_json TEXT NOT NULL,
    content_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    etag TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT,
    superseded_at TEXT,
    UNIQUE(task_id, version),
    UNIQUE(task_id, content_hash),
    CHECK ((status = 'published' AND published_at IS NOT NULL) OR status <> 'published')
);

CREATE TABLE orch_contract_requirements (
    id TEXT NOT NULL,
    contract_id TEXT NOT NULL REFERENCES orch_quality_contracts(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    category TEXT NOT NULL CHECK (category IN (
        'scope', 'coverage', 'relationship', 'evidence', 'currentness',
        'format', 'safety', 'limitation', 'performance'
    )),
    text TEXT NOT NULL,
    required INTEGER NOT NULL CHECK (required IN (0, 1)),
    hard_gate INTEGER NOT NULL CHECK (hard_gate IN (0, 1)),
    source TEXT NOT NULL CHECK (source IN (
        'explicit_ui', 'explicit_prompt', 'user_custom', 'archetype',
        'policy', 'inferred'
    )),
    source_span_json TEXT,
    confidence REAL CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    verification_method TEXT NOT NULL CHECK (verification_method IN (
        'artifact_exists', 'coverage', 'citation', 'claim_support',
        'inventory_reconcile', 'workspace_unchanged', 'semantic_rubric', 'manual'
    )),
    verification_spec_json TEXT NOT NULL DEFAULT '{}',
    waivable INTEGER NOT NULL CHECK (waivable IN (0, 1)),
    PRIMARY KEY(contract_id, id),
    UNIQUE(contract_id, position)
);

CREATE TABLE orch_contract_deliverables (
    id TEXT NOT NULL,
    contract_id TEXT NOT NULL REFERENCES orch_quality_contracts(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    kind TEXT NOT NULL,
    filename TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel = 'task_artifact_store'),
    required INTEGER NOT NULL CHECK (required IN (0, 1)),
    is_primary INTEGER NOT NULL CHECK (is_primary IN (0, 1)),
    required_sections_json TEXT NOT NULL DEFAULT '[]',
    result_schema_id TEXT NOT NULL,
    PRIMARY KEY(contract_id, id),
    UNIQUE(contract_id, position)
);

CREATE TABLE orch_task_draft_analyses (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    contract_id TEXT NOT NULL REFERENCES orch_quality_contracts(id),
    target_resolution_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('resolved', 'needs_target_selection', 'failed')),
    created_at TEXT NOT NULL,
    UNIQUE(task_id, request_hash),
    UNIQUE(task_id, idempotency_key)
);

ALTER TABLE orch_tasks ADD COLUMN active_contract_id TEXT REFERENCES orch_quality_contracts(id);
ALTER TABLE orch_tasks ADD COLUMN workflow_status TEXT NOT NULL DEFAULT 'draft' CHECK (
    workflow_status IN (
        'draft', 'analyzing', 'needs_target_selection', 'ready', 'running',
        'validating', 'reviewing', 'repairing', 'recovering',
        'needs_reconciliation', 'needs_attention', 'completed', 'failed',
        'canceled', 'archived'
    )
);
-- Server-owned checkpoint used only by recovering/reconciliation and guarded
-- resume paths.  Agent/model result payloads never write this column.
ALTER TABLE orch_tasks ADD COLUMN workflow_resume_status TEXT CHECK (
    workflow_resume_status IS NULL OR workflow_resume_status IN (
        'ready', 'running', 'validating', 'reviewing', 'repairing'
    )
);

CREATE INDEX orch_contracts_task_status
ON orch_quality_contracts(task_id, status, version DESC);
CREATE INDEX orch_requirements_contract_required_gate
ON orch_contract_requirements(contract_id, required, hard_gate);
CREATE UNIQUE INDEX orch_contract_one_primary_deliverable
ON orch_contract_deliverables(contract_id) WHERE is_primary = 1;
CREATE INDEX orch_task_draft_analysis_task_created
ON orch_task_draft_analyses(task_id, created_at DESC);

CREATE TRIGGER orch_published_contract_no_update
BEFORE UPDATE ON orch_quality_contracts
WHEN OLD.status IN ('published', 'superseded')
BEGIN SELECT RAISE(ABORT, 'published quality contracts are immutable'); END;

CREATE TRIGGER orch_published_contract_no_delete
BEFORE DELETE ON orch_quality_contracts
WHEN OLD.status IN ('published', 'superseded')
BEGIN SELECT RAISE(ABORT, 'published quality contracts are immutable'); END;

CREATE TRIGGER orch_published_requirement_no_update
BEFORE UPDATE ON orch_contract_requirements
WHEN EXISTS (
    SELECT 1 FROM orch_quality_contracts c
    WHERE c.id = OLD.contract_id AND c.status IN ('published', 'superseded')
)
BEGIN SELECT RAISE(ABORT, 'published contract requirements are immutable'); END;

CREATE TRIGGER orch_published_requirement_no_delete
BEFORE DELETE ON orch_contract_requirements
WHEN EXISTS (
    SELECT 1 FROM orch_quality_contracts c
    WHERE c.id = OLD.contract_id AND c.status IN ('published', 'superseded')
)
BEGIN SELECT RAISE(ABORT, 'published contract requirements are immutable'); END;

CREATE TRIGGER orch_published_deliverable_no_update
BEFORE UPDATE ON orch_contract_deliverables
WHEN EXISTS (
    SELECT 1 FROM orch_quality_contracts c
    WHERE c.id = OLD.contract_id AND c.status IN ('published', 'superseded')
)
BEGIN SELECT RAISE(ABORT, 'published contract deliverables are immutable'); END;

CREATE TRIGGER orch_published_deliverable_no_delete
BEFORE DELETE ON orch_contract_deliverables
WHEN EXISTS (
    SELECT 1 FROM orch_quality_contracts c
    WHERE c.id = OLD.contract_id AND c.status IN ('published', 'superseded')
)
BEGIN SELECT RAISE(ABORT, 'published contract deliverables are immutable'); END;
