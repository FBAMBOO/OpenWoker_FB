-- Task-Centric Handoff Protocol: immutable, versioned task contracts and
-- manifest-only context references. Backfill is performed by the store startup
-- hook so content hashes use the same canonical JSON implementation as new data.

CREATE TABLE orch_task_briefs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    status TEXT NOT NULL CHECK (status IN ('draft', 'published', 'superseded')),
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    background TEXT NOT NULL DEFAULT '',
    scope_json TEXT NOT NULL DEFAULT '{}',
    instructions_json TEXT NOT NULL DEFAULT '[]',
    constraints_json TEXT NOT NULL DEFAULT '[]',
    non_goals_json TEXT NOT NULL DEFAULT '[]',
    acceptance_criteria_json TEXT NOT NULL DEFAULT '[]',
    deliverables_json TEXT NOT NULL DEFAULT '[]',
    result_contract_json TEXT NOT NULL DEFAULT '{}',
    created_by_task_id TEXT REFERENCES orch_tasks(id),
    created_by_run_id TEXT REFERENCES orch_runs(id),
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT,
    UNIQUE(task_id, revision)
);

CREATE INDEX orch_briefs_task_revision
ON orch_task_briefs(task_id, revision DESC);

CREATE TABLE orch_context_refs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    brief_id TEXT NOT NULL REFERENCES orch_task_briefs(id),
    requirement TEXT NOT NULL CHECK (requirement IN ('required', 'recommended', 'optional')),
    ref_type TEXT NOT NULL CHECK (ref_type IN (
        'file', 'file_range', 'artifact', 'task_output', 'work_product',
        'task_comment', 'event_range', 'url', 'workspace_query', 'git_diff'
    )),
    display_name TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    selection_reason TEXT NOT NULL,
    locator_json TEXT NOT NULL,
    delivery_mode TEXT NOT NULL CHECK (delivery_mode IN ('metadata_only', 'excerpt', 'on_demand')),
    mime_type TEXT,
    content_hash TEXT,
    byte_size INTEGER CHECK (byte_size IS NULL OR byte_size >= 0),
    token_estimate INTEGER CHECK (token_estimate IS NULL OR token_estimate >= 0),
    provenance_json TEXT NOT NULL DEFAULT '{}',
    trust_level TEXT NOT NULL DEFAULT 'untrusted',
    created_by_task_id TEXT REFERENCES orch_tasks(id),
    created_by_run_id TEXT REFERENCES orch_runs(id),
    created_at TEXT NOT NULL
);

CREATE INDEX orch_context_refs_manifest
ON orch_context_refs(task_id, brief_id, requirement, created_at, id);

ALTER TABLE orch_tasks ADD COLUMN active_brief_id TEXT REFERENCES orch_task_briefs(id);
ALTER TABLE orch_runs ADD COLUMN brief_id TEXT REFERENCES orch_task_briefs(id);

CREATE UNIQUE INDEX orch_one_active_brief
ON orch_tasks(id, active_brief_id) WHERE active_brief_id IS NOT NULL;

CREATE TRIGGER orch_briefs_published_content_immutable
BEFORE UPDATE OF task_id, revision, title, objective, background, scope_json,
    instructions_json, constraints_json, non_goals_json,
    acceptance_criteria_json, deliverables_json, result_contract_json,
    created_by_task_id, created_by_run_id, content_hash, created_at, published_at
ON orch_task_briefs
WHEN OLD.status <> 'draft'
BEGIN SELECT RAISE(ABORT, 'published task briefs are immutable'); END;

CREATE TRIGGER orch_briefs_status_transition
BEFORE UPDATE OF status ON orch_task_briefs
WHEN NOT (
    NEW.status = OLD.status OR
    (OLD.status = 'draft' AND NEW.status = 'published') OR
    (OLD.status = 'published' AND NEW.status = 'superseded')
)
BEGIN SELECT RAISE(ABORT, 'invalid task brief status transition'); END;

CREATE TRIGGER orch_briefs_no_published_delete
BEFORE DELETE ON orch_task_briefs
WHEN OLD.status <> 'draft'
BEGIN SELECT RAISE(ABORT, 'published task briefs are immutable'); END;

CREATE TRIGGER orch_context_refs_no_update
BEFORE UPDATE ON orch_context_refs
BEGIN SELECT RAISE(ABORT, 'context references are immutable'); END;

CREATE TRIGGER orch_context_refs_draft_delete_only
BEFORE DELETE ON orch_context_refs
WHEN (SELECT status FROM orch_task_briefs WHERE id = OLD.brief_id) <> 'draft'
BEGIN SELECT RAISE(ABORT, 'published context references are immutable'); END;
