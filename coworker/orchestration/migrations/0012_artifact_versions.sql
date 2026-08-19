-- Immutable task-owned artifact versions and server-derived read receipts.
-- Rollback: additive only. Downgrade the application while retaining immutable
-- artifact metadata and blobs; legacy views may expose metadata/download only.

CREATE TABLE orch_artifact_versions (
    id TEXT PRIMARY KEY,
    logical_deliverable_id TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    run_id TEXT REFERENCES orch_runs(id),
    attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt > 0),
    version INTEGER NOT NULL CHECK (version > 0),
    filename TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    blob_uri TEXT,
    sha256 TEXT,
    byte_size INTEGER CHECK (byte_size IS NULL OR byte_size >= 0),
    section_index_artifact_id TEXT REFERENCES orch_artifact_versions(id),
    chunk_manifest_artifact_id TEXT REFERENCES orch_artifact_versions(id),
    status TEXT NOT NULL CHECK (status IN (
        'uploading', 'draft', 'validating', 'verified', 'rejected', 'superseded'
    )),
    producer_profile_id TEXT,
    parent_artifact_id TEXT REFERENCES orch_artifact_versions(id),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    finalized_at TEXT,
    UNIQUE(logical_deliverable_id, version),
    UNIQUE(task_id, sha256, logical_deliverable_id),
    CHECK (
        (status = 'uploading' AND sha256 IS NULL AND byte_size IS NULL AND blob_uri IS NULL)
        OR
        (status <> 'uploading' AND sha256 IS NOT NULL AND byte_size IS NOT NULL AND blob_uri IS NOT NULL)
    )
);

CREATE TABLE orch_artifact_uploads (
    id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL UNIQUE REFERENCES orch_artifact_versions(id),
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    run_id TEXT REFERENCES orch_runs(id),
    expected_sequence INTEGER NOT NULL DEFAULT 0 CHECK (expected_sequence >= 0),
    received_bytes INTEGER NOT NULL DEFAULT 0 CHECK (received_bytes >= 0),
    max_bytes INTEGER NOT NULL CHECK (max_bytes > 0),
    status TEXT NOT NULL CHECK (status IN ('open', 'completed', 'failed', 'aborted')),
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE orch_artifact_upload_chunks (
    upload_id TEXT NOT NULL REFERENCES orch_artifact_uploads(id),
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    chunk_hash TEXT NOT NULL,
    byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
    blob_uri TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(upload_id, sequence_no)
);

CREATE TABLE orch_artifact_read_receipts (
    id TEXT PRIMARY KEY,
    verifier_profile_id TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES orch_runs(id),
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    artifact_hash TEXT NOT NULL,
    ranges_json TEXT NOT NULL DEFAULT '[]',
    covered_bytes INTEGER NOT NULL DEFAULT 0 CHECK (covered_bytes >= 0),
    coverage_ratio REAL NOT NULL DEFAULT 0 CHECK (coverage_ratio >= 0 AND coverage_ratio <= 1),
    candidate_bound_at TEXT NOT NULL,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, artifact_id, artifact_hash, candidate_bound_at)
);

CREATE TABLE orch_artifact_read_events (
    id TEXT PRIMARY KEY,
    receipt_id TEXT NOT NULL REFERENCES orch_artifact_read_receipts(id),
    start_byte INTEGER NOT NULL CHECK (start_byte >= 0),
    end_byte INTEGER NOT NULL CHECK (end_byte > start_byte),
    delivered_hash TEXT NOT NULL,
    delivered_at TEXT NOT NULL,
    UNIQUE(receipt_id, start_byte, end_byte)
);

ALTER TABLE orch_work_products ADD COLUMN artifact_version_id TEXT REFERENCES orch_artifact_versions(id);

CREATE INDEX orch_artifacts_task_status
ON orch_artifact_versions(task_id, status, logical_deliverable_id, version DESC);
CREATE INDEX orch_artifacts_parent
ON orch_artifact_versions(parent_artifact_id);
CREATE INDEX orch_artifact_receipts_artifact_run
ON orch_artifact_read_receipts(artifact_id, run_id);
CREATE INDEX orch_artifact_read_events_receipt
ON orch_artifact_read_events(receipt_id, start_byte, end_byte);

CREATE TRIGGER orch_artifact_final_content_no_update
BEFORE UPDATE OF logical_deliverable_id, task_id, run_id, attempt, version,
    filename, mime_type, blob_uri, sha256, byte_size, parent_artifact_id
ON orch_artifact_versions
WHEN OLD.status <> 'uploading'
BEGIN SELECT RAISE(ABORT, 'finalized artifact content is immutable'); END;

CREATE TRIGGER orch_artifact_terminal_status_guard
BEFORE UPDATE OF status ON orch_artifact_versions
WHEN OLD.status = 'superseded'
    OR (OLD.status = 'rejected' AND NEW.status <> 'superseded')
    OR (OLD.status = 'verified' AND NEW.status <> 'superseded')
BEGIN SELECT RAISE(ABORT, 'terminal artifact status is immutable'); END;

CREATE TRIGGER orch_artifact_final_no_delete
BEFORE DELETE ON orch_artifact_versions
WHEN OLD.status <> 'uploading'
BEGIN SELECT RAISE(ABORT, 'finalized artifact versions are immutable'); END;

CREATE TRIGGER orch_completed_receipt_no_update
BEFORE UPDATE ON orch_artifact_read_receipts
WHEN OLD.completed_at IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'completed artifact read receipts are immutable'); END;

CREATE TRIGGER orch_completed_receipt_no_delete
BEFORE DELETE ON orch_artifact_read_receipts
WHEN OLD.completed_at IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'completed artifact read receipts are immutable'); END;
