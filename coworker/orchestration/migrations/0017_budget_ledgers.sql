-- Transactional root budget ledgers, reservations and append-only events.
-- Rollback: additive only. Preserve ledgers/events and exhausted state; changing a
-- rollout from hard to observe applies only to new tasks and never completes old tasks.

CREATE TABLE orch_budget_ledgers (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    strategy_id TEXT NOT NULL REFERENCES orch_execution_strategies(id),
    mode TEXT NOT NULL CHECK (mode IN ('hard', 'soft', 'unlimited')),
    source_profile_id TEXT NOT NULL,
    effective_limits_json TEXT NOT NULL,
    reserved_json TEXT NOT NULL,
    consumed_json TEXT NOT NULL,
    provider_usage_semantics_json TEXT NOT NULL DEFAULT '{}',
    over_budget INTEGER NOT NULL DEFAULT 0 CHECK (over_budget IN (0, 1)),
    version INTEGER NOT NULL CHECK (version > 0),
    fencing_token INTEGER NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'exhausted', 'closed', 'superseded')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(task_id, version)
);

CREATE TABLE orch_budget_reservations (
    id TEXT PRIMARY KEY,
    ledger_id TEXT NOT NULL REFERENCES orch_budget_ledgers(id),
    run_id TEXT REFERENCES orch_runs(id),
    purpose TEXT NOT NULL,
    amounts_json TEXT NOT NULL,
    consumed_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL CHECK (status IN ('active', 'released', 'consumed', 'canceled')),
    fencing_token INTEGER NOT NULL CHECK (fencing_token >= 0),
    created_at TEXT NOT NULL,
    released_at TEXT
);

CREATE TABLE orch_budget_events (
    id TEXT PRIMARY KEY,
    ledger_id TEXT NOT NULL REFERENCES orch_budget_ledgers(id),
    sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'created', 'reserved', 'consumed', 'released', 'threshold',
        'exhausted', 'extended', 'closed', 'recovered'
    )),
    reservation_id TEXT REFERENCES orch_budget_reservations(id),
    run_id TEXT REFERENCES orch_runs(id),
    dimension TEXT,
    amount INTEGER,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(ledger_id, sequence_no)
);

ALTER TABLE orch_tasks ADD COLUMN budget_status TEXT NOT NULL DEFAULT 'unconfigured' CHECK (
    budget_status IN (
        'unconfigured', 'within_budget', 'warning', 'exhausted',
        'over_budget', 'unlimited'
    )
);
ALTER TABLE orch_tasks ADD COLUMN active_budget_ledger_id TEXT REFERENCES orch_budget_ledgers(id);
ALTER TABLE orch_tasks ADD COLUMN quality_reason_code TEXT;

CREATE INDEX orch_budget_ledgers_task_status
ON orch_budget_ledgers(task_id, status, version DESC);
CREATE INDEX orch_budget_reservations_active
ON orch_budget_reservations(ledger_id, status, run_id);
CREATE INDEX orch_budget_events_ledger_sequence
ON orch_budget_events(ledger_id, sequence_no);

CREATE TRIGGER orch_budget_event_no_update BEFORE UPDATE ON orch_budget_events
BEGIN SELECT RAISE(ABORT, 'budget events are immutable'); END;
CREATE TRIGGER orch_budget_event_no_delete BEFORE DELETE ON orch_budget_events
BEGIN SELECT RAISE(ABORT, 'budget events are immutable'); END;

CREATE TRIGGER orch_superseded_budget_no_update
BEFORE UPDATE ON orch_budget_ledgers WHEN OLD.status = 'superseded'
BEGIN SELECT RAISE(ABORT, 'superseded budget revisions are immutable'); END;
CREATE TRIGGER orch_superseded_budget_no_delete
BEFORE DELETE ON orch_budget_ledgers WHEN OLD.status = 'superseded'
BEGIN SELECT RAISE(ABORT, 'superseded budget revisions are immutable'); END;
