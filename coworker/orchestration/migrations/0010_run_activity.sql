-- Safe, append-only, incrementally pageable activity for one durable Agent run.

CREATE TABLE orch_run_activity (
    sequence_no INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    run_id TEXT NOT NULL REFERENCES orch_runs(id),
    event_key TEXT NOT NULL,
    source_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
        'lifecycle', 'reasoning_summary', 'tool', 'message', 'usage', 'error'
    )),
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'running', 'completed', 'failed', 'canceled', 'info'
    )),
    title TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(run_id, event_key)
);

CREATE INDEX orch_run_activity_delta
ON orch_run_activity(run_id, sequence_no);

CREATE INDEX orch_task_run_activity_delta
ON orch_run_activity(task_id, sequence_no);

CREATE TRIGGER orch_run_activity_no_update BEFORE UPDATE ON orch_run_activity
BEGIN SELECT RAISE(ABORT, 'run activity is immutable'); END;

CREATE TRIGGER orch_run_activity_no_delete BEFORE DELETE ON orch_run_activity
BEGIN SELECT RAISE(ABORT, 'run activity is immutable'); END;
