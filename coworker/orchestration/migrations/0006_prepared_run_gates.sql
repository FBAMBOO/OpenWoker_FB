-- A run-owned interaction gate is prepared while its execution lease is still
-- active.  It becomes externally resolvable only after checkpoint persistence and
-- process-tree cleanup have both succeeded.

ALTER TABLE orch_gates RENAME TO orch_gates_before_preparing;

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
    status TEXT NOT NULL CHECK (status IN (
        'preparing', 'open', 'approved', 'rejected', 'expired', 'canceled'
    )),
    source_key TEXT NOT NULL UNIQUE,
    prompt_json TEXT NOT NULL,
    resolution_json TEXT,
    resolved_by TEXT,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    opened_at TEXT NOT NULL,
    published_at TEXT,
    resolved_at TEXT,
    expires_at TEXT
);

INSERT INTO orch_gates(
    id, task_id, run_id, node_id, kind, status, source_key, prompt_json,
    resolution_json, resolved_by, version, opened_at, published_at, resolved_at,
    expires_at
)
SELECT
    id, task_id, run_id, node_id, kind, status, source_key, prompt_json,
    resolution_json, resolved_by, version, opened_at, opened_at, resolved_at,
    expires_at
FROM orch_gates_before_preparing;

DROP TABLE orch_gates_before_preparing;

CREATE INDEX orch_gates_task_status ON orch_gates(task_id, status);
CREATE INDEX orch_gates_task_publication
ON orch_gates(task_id, published_at, id);
