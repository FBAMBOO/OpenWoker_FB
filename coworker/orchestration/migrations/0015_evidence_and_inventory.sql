-- Shared repository inventory, query cache, claim/evidence and coverage ledger.
-- Rollback: additive only. Preserve typed evidence and cache provenance for offline
-- audit; older application versions ignore these tables without deleting data.

CREATE TABLE orch_repository_inventories (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES orch_repository_snapshots(id),
    tool_version TEXT NOT NULL,
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    content_hash TEXT NOT NULL,
    file_count INTEGER NOT NULL CHECK (file_count >= 0),
    total_bytes INTEGER NOT NULL CHECK (total_bytes >= 0),
    project_markers_json TEXT NOT NULL DEFAULT '[]',
    generated_at TEXT NOT NULL,
    UNIQUE(snapshot_id, tool_version, content_hash)
);

CREATE TABLE orch_repo_query_cache (
    query_key TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES orch_repository_snapshots(id),
    tool_name TEXT NOT NULL,
    tool_version TEXT NOT NULL,
    normalized_args_hash TEXT NOT NULL,
    result_artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    result_hash TEXT NOT NULL,
    result_bytes INTEGER NOT NULL CHECK (result_bytes >= 0),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    continuation TEXT,
    sensitivity TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_accessed_at TEXT NOT NULL,
    hit_count INTEGER NOT NULL DEFAULT 0 CHECK (hit_count >= 0),
    UNIQUE(snapshot_id, tool_name, tool_version, normalized_args_hash)
);

CREATE TABLE orch_claims (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    artifact_version INTEGER NOT NULL CHECK (artifact_version > 0),
    section_id TEXT NOT NULL,
    text TEXT NOT NULL,
    claim_type TEXT NOT NULL CHECK (claim_type IN (
        'fact', 'inference', 'absence', 'risk', 'recommendation', 'limitation'
    )),
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low', 'info')),
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    requirement_ids_json TEXT NOT NULL DEFAULT '[]',
    source_key TEXT,
    status TEXT NOT NULL CHECK (status IN ('draft', 'verified', 'disputed', 'superseded')),
    created_at TEXT NOT NULL,
    UNIQUE(artifact_id, source_key)
);

CREATE TABLE orch_evidence_refs (
    id TEXT PRIMARY KEY,
    claim_id TEXT NOT NULL REFERENCES orch_claims(id),
    snapshot_id TEXT NOT NULL REFERENCES orch_repository_snapshots(id),
    path TEXT NOT NULL,
    line_start INTEGER NOT NULL CHECK (line_start > 0),
    line_end INTEGER NOT NULL CHECK (line_end >= line_start),
    blob_hash TEXT NOT NULL,
    git_blob_oid TEXT,
    excerpt_hash TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    support TEXT NOT NULL CHECK (support IN ('supports', 'contradicts', 'context')),
    content_withheld INTEGER NOT NULL DEFAULT 0 CHECK (content_withheld IN (0, 1)),
    created_by_run_id TEXT NOT NULL REFERENCES orch_runs(id),
    created_at TEXT NOT NULL,
    UNIQUE(claim_id, snapshot_id, path, line_start, line_end, support)
);

CREATE TABLE orch_negative_evidence (
    id TEXT PRIMARY KEY,
    claim_id TEXT NOT NULL REFERENCES orch_claims(id),
    query TEXT NOT NULL,
    tool_version TEXT NOT NULL,
    scope_paths_json TEXT NOT NULL,
    excluded_paths_json TEXT NOT NULL DEFAULT '[]',
    result_count INTEGER NOT NULL CHECK (result_count >= 0),
    query_result_hash TEXT NOT NULL,
    limitations_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(claim_id, query, query_result_hash)
);

CREATE TABLE orch_coverage_results (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    requirement_id TEXT NOT NULL,
    area TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pass', 'fail', 'unknown')),
    claim_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_count INTEGER NOT NULL CHECK (evidence_count >= 0),
    notes TEXT NOT NULL DEFAULT '',
    validator_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(artifact_id, requirement_id, area, validator_id)
);

CREATE TABLE orch_inventory_metrics (
    id TEXT PRIMARY KEY,
    inventory_id TEXT NOT NULL REFERENCES orch_repository_inventories(id),
    name TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    query_key TEXT NOT NULL,
    subtotals_json TEXT NOT NULL DEFAULT '{}',
    reconciles_to REAL,
    tolerance REAL NOT NULL DEFAULT 0 CHECK (tolerance >= 0),
    created_at TEXT NOT NULL,
    UNIQUE(inventory_id, name, query_key)
);

CREATE INDEX orch_claims_artifact_section_status
ON orch_claims(artifact_id, section_id, status);
CREATE INDEX orch_evidence_refs_claim
ON orch_evidence_refs(claim_id);
CREATE INDEX orch_evidence_refs_snapshot_path
ON orch_evidence_refs(snapshot_id, path, line_start);
CREATE INDEX orch_coverage_task_requirement
ON orch_coverage_results(task_id, requirement_id, status);

CREATE TRIGGER orch_evidence_ref_no_update BEFORE UPDATE ON orch_evidence_refs
BEGIN SELECT RAISE(ABORT, 'evidence references are immutable'); END;
CREATE TRIGGER orch_evidence_ref_no_delete BEFORE DELETE ON orch_evidence_refs
BEGIN SELECT RAISE(ABORT, 'evidence references are immutable'); END;
CREATE TRIGGER orch_coverage_no_update BEFORE UPDATE ON orch_coverage_results
BEGIN SELECT RAISE(ABORT, 'coverage results are immutable'); END;
CREATE TRIGGER orch_coverage_no_delete BEFORE DELETE ON orch_coverage_results
BEGIN SELECT RAISE(ABORT, 'coverage results are immutable'); END;
