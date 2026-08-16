ALTER TABLE orch_outbox ADD COLUMN dead_lettered_at TEXT;

CREATE INDEX orch_outbox_dead_letters
    ON orch_outbox(dead_lettered_at, attempts, created_at);

CREATE TABLE orch_scheduler_leader (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    expires_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL
);
