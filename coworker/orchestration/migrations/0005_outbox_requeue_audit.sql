CREATE TABLE orch_outbox_requeue_history (
    id TEXT PRIMARY KEY,
    outbox_id TEXT NOT NULL REFERENCES orch_outbox(id),
    command_id TEXT NOT NULL UNIQUE REFERENCES orch_commands(id),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 200),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 2000),
    snapshot_attempts INTEGER NOT NULL CHECK (snapshot_attempts >= 0),
    snapshot_last_error TEXT,
    snapshot_dead_lettered_at TEXT NOT NULL,
    requeued_at TEXT NOT NULL
);

CREATE INDEX orch_outbox_requeue_history_item
    ON orch_outbox_requeue_history(outbox_id, requeued_at DESC, id DESC);

CREATE TRIGGER orch_outbox_requeue_history_no_update
BEFORE UPDATE ON orch_outbox_requeue_history
BEGIN SELECT RAISE(ABORT, 'outbox requeue history is append-only'); END;

CREATE TRIGGER orch_outbox_requeue_history_no_delete
BEFORE DELETE ON orch_outbox_requeue_history
BEGIN SELECT RAISE(ABORT, 'outbox requeue history is append-only'); END;
