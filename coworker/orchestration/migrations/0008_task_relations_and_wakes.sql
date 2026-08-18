-- First-class task graph relations and durable at-least-once scheduling intents.

CREATE TABLE orch_task_relations (
    id TEXT PRIMARY KEY,
    from_task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    to_task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    relation_type TEXT NOT NULL CHECK (relation_type IN (
        'parent', 'blocks', 'reviews', 'related', 'supersedes'
    )),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_by_task_id TEXT REFERENCES orch_tasks(id),
    created_by_run_id TEXT REFERENCES orch_runs(id),
    created_at TEXT NOT NULL,
    removed_at TEXT,
    CHECK (from_task_id <> to_task_id)
);

CREATE UNIQUE INDEX orch_live_relation_unique
ON orch_task_relations(from_task_id, to_task_id, relation_type)
WHERE removed_at IS NULL;
CREATE INDEX orch_relations_from
ON orch_task_relations(from_task_id, relation_type, removed_at);
CREATE INDEX orch_relations_to
ON orch_task_relations(to_task_id, relation_type, removed_at);

CREATE TABLE orch_wake_requests (
    id TEXT PRIMARY KEY,
    target_task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    target_run_id TEXT REFERENCES orch_runs(id),
    reason TEXT NOT NULL CHECK (reason IN (
        'assignment', 'task_assigned', 'task_commented',
        'task_comment_mentioned', 'task_children_completed',
        'task_blockers_resolved', 'gate_resolved', 'retry_requested',
        'replan_requested', 'manual_resume', 'lease_recovered',
        'brief_revision_available', 'review_assigned'
    )),
    source_task_id TEXT REFERENCES orch_tasks(id),
    source_run_id TEXT REFERENCES orch_runs(id),
    source_event_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    dedupe_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'deferred', 'claimed', 'delivered', 'completed', 'failed', 'canceled'
    )),
    coalesced_count INTEGER NOT NULL DEFAULT 0 CHECK (coalesced_count >= 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    not_before TEXT NOT NULL,
    claimed_by TEXT,
    claimed_until TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    delivered_at TEXT,
    completed_at TEXT
);

CREATE UNIQUE INDEX orch_live_wake_dedupe
ON orch_wake_requests(dedupe_key)
WHERE status IN ('pending', 'deferred', 'claimed', 'delivered');
CREATE INDEX orch_wakes_ready
ON orch_wake_requests(status, not_before, created_at, id);
CREATE INDEX orch_wakes_target
ON orch_wake_requests(target_task_id, status, created_at, id);
CREATE INDEX orch_wakes_claim_expiry
ON orch_wake_requests(status, claimed_until);
