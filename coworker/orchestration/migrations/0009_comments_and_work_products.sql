-- Incremental communication and immutable, inspectable task deliverables.

CREATE TABLE orch_task_comments (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
    author_type TEXT NOT NULL CHECK (author_type IN ('operator', 'agent', 'system')),
    author_id TEXT NOT NULL,
    created_by_run_id TEXT REFERENCES orch_runs(id),
    body_markdown TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    reply_to_comment_id TEXT REFERENCES orch_task_comments(id),
    created_at TEXT NOT NULL,
    UNIQUE(task_id, sequence_no)
);

CREATE INDEX orch_comments_delta
ON orch_task_comments(task_id, sequence_no);

CREATE TABLE orch_work_products (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    run_id TEXT REFERENCES orch_runs(id),
    kind TEXT NOT NULL CHECK (kind IN (
        'plan', 'progress_report', 'implementation_patch', 'pull_request',
        'commit', 'branch', 'workspace_file', 'artifact', 'test_result',
        'review_report', 'evaluation', 'preview_url', 'runtime_service', 'other'
    )),
    title TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    evidence_id TEXT REFERENCES orch_evidence(id),
    artifact_id TEXT,
    uri TEXT,
    content_hash TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    verification_status TEXT NOT NULL DEFAULT 'unverified' CHECK (
        verification_status IN ('unverified', 'verified', 'stale', 'missing', 'failed')
    ),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX orch_work_products_task
ON orch_work_products(task_id, kind, created_at, id);
CREATE INDEX orch_work_products_producer
ON orch_work_products(run_id, verification_status, created_at, id);

CREATE TRIGGER orch_comments_no_update BEFORE UPDATE ON orch_task_comments
BEGIN SELECT RAISE(ABORT, 'task comments are immutable'); END;
CREATE TRIGGER orch_comments_no_delete BEFORE DELETE ON orch_task_comments
BEGIN SELECT RAISE(ABORT, 'task comments are immutable'); END;
CREATE TRIGGER orch_work_products_no_update BEFORE UPDATE ON orch_work_products
BEGIN SELECT RAISE(ABORT, 'work products are immutable'); END;
CREATE TRIGGER orch_work_products_no_delete BEFORE DELETE ON orch_work_products
BEGIN SELECT RAISE(ABORT, 'work products are immutable'); END;
