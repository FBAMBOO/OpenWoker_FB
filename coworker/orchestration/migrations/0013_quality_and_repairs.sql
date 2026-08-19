-- Authoritative gates, findings, semantic scores, repairs and exact waivers.
-- Rollback: additive only. Preserve verdicts, findings, repairs, and waiver audit
-- records; an older application ignores these tables and must not rewrite them.

CREATE TABLE orch_quality_rubrics (
    id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    name TEXT NOT NULL,
    applicable_archetypes_json TEXT NOT NULL,
    dimensions_json TEXT NOT NULL,
    pass_threshold INTEGER NOT NULL CHECK (pass_threshold >= 0 AND pass_threshold <= 100),
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(id, version),
    UNIQUE(content_hash)
);

CREATE TABLE orch_rubric_scores (
    id TEXT PRIMARY KEY,
    rubric_id TEXT NOT NULL,
    rubric_version INTEGER NOT NULL,
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    artifact_hash TEXT NOT NULL,
    scorer_run_id TEXT NOT NULL REFERENCES orch_runs(id),
    dimension_scores_json TEXT NOT NULL,
    total INTEGER NOT NULL CHECK (total >= 0 AND total <= 100),
    created_at TEXT NOT NULL,
    FOREIGN KEY(rubric_id, rubric_version) REFERENCES orch_quality_rubrics(id, version),
    UNIQUE(artifact_id, rubric_id, rubric_version, scorer_run_id)
);

CREATE TABLE orch_gate_results (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    artifact_hash TEXT NOT NULL,
    subject_type TEXT NOT NULL CHECK (subject_type IN (
        'hard_gate', 'criterion', 'finding', 'semantic_score', 'soft_budget'
    )),
    subject_id TEXT NOT NULL,
    subject_version INTEGER NOT NULL CHECK (subject_version > 0),
    status TEXT NOT NULL CHECK (status IN ('pass', 'fail', 'unknown')),
    waivable INTEGER NOT NULL CHECK (waivable IN (0, 1)),
    reason_code TEXT NOT NULL,
    evidence_ids_json TEXT NOT NULL DEFAULT '[]',
    validator_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(task_id, artifact_id, subject_type, subject_id, subject_version, validator_id)
);

CREATE TABLE orch_quality_findings (
    id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    artifact_hash TEXT NOT NULL,
    category TEXT NOT NULL CHECK (category IN (
        'baseline', 'coverage', 'citation', 'support', 'consistency',
        'schema', 'security', 'budget', 'style', 'limitation'
    )),
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low', 'info')),
    blocking INTEGER NOT NULL CHECK (blocking IN (0, 1)),
    repairable INTEGER NOT NULL CHECK (repairable IN (0, 1)),
    requirement_id TEXT,
    claim_id TEXT,
    section_id TEXT,
    message TEXT NOT NULL,
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    suggested_fix TEXT,
    status TEXT NOT NULL CHECK (status IN ('open', 'repairing', 'resolved', 'dismissed')),
    supersedes_finding_id TEXT REFERENCES orch_quality_findings(id),
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    CHECK (requirement_id IS NOT NULL OR claim_id IS NOT NULL OR section_id IS NOT NULL),
    UNIQUE(task_id, artifact_id, fingerprint)
);

CREATE TABLE orch_quality_evaluations (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    artifact_hash TEXT NOT NULL,
    evaluation_type TEXT NOT NULL CHECK (evaluation_type IN (
        'deterministic', 'semantic', 'review', 'final'
    )),
    validator_id TEXT NOT NULL,
    validator_version TEXT NOT NULL,
    rubric_id TEXT,
    rubric_version INTEGER,
    criterion_results_json TEXT NOT NULL DEFAULT '[]',
    coverage_results_json TEXT NOT NULL DEFAULT '[]',
    rubric_score_id TEXT REFERENCES orch_rubric_scores(id),
    total_score INTEGER CHECK (total_score IS NULL OR (total_score >= 0 AND total_score <= 100)),
    verdict TEXT NOT NULL CHECK (verdict IN ('pass', 'fail', 'unknown')),
    decision TEXT CHECK (decision IS NULL OR decision IN (
        'publish', 'repair', 'needs_attention', 'reject'
    )),
    read_receipt_id TEXT REFERENCES orch_artifact_read_receipts(id),
    finding_ids_json TEXT NOT NULL DEFAULT '[]',
    created_by_run_id TEXT REFERENCES orch_runs(id),
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(artifact_id, evaluation_type, validator_id, validator_version, content_hash)
);

CREATE TABLE orch_repair_requests (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    source_artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    target_version INTEGER NOT NULL CHECK (target_version > 1),
    finding_ids_json TEXT NOT NULL,
    finding_set_hash TEXT NOT NULL,
    allowed_sections_json TEXT NOT NULL DEFAULT '[]',
    required_validators_json TEXT NOT NULL DEFAULT '[]',
    budget_allocation_json TEXT NOT NULL DEFAULT '{}',
    attempt INTEGER NOT NULL CHECK (attempt > 0 AND attempt <= 2),
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'running', 'completed', 'failed', 'exhausted'
    )),
    result_artifact_id TEXT REFERENCES orch_artifact_versions(id),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE(task_id, source_artifact_id, finding_set_hash, attempt)
);

CREATE TABLE orch_quality_waivers (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    artifact_hash TEXT NOT NULL,
    contract_id TEXT NOT NULL REFERENCES orch_quality_contracts(id),
    contract_version INTEGER NOT NULL CHECK (contract_version > 0),
    subject_type TEXT NOT NULL CHECK (subject_type IN (
        'gate_result', 'criterion', 'finding', 'semantic_score', 'soft_budget'
    )),
    subject_id TEXT NOT NULL,
    subject_version INTEGER NOT NULL CHECK (subject_version > 0),
    rubric_id TEXT,
    rubric_version INTEGER,
    actor_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    reference TEXT,
    expires_at TEXT,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    signature_hash TEXT NOT NULL,
    UNIQUE(task_id, artifact_id, subject_type, subject_id, subject_version, signature_hash)
);

ALTER TABLE orch_tasks ADD COLUMN quality_status TEXT NOT NULL DEFAULT 'pending' CHECK (
    quality_status IN ('pending', 'checking', 'pass', 'fail', 'unknown', 'waived')
);
ALTER TABLE orch_tasks ADD COLUMN artifact_status TEXT NOT NULL DEFAULT 'none' CHECK (
    artifact_status IN ('none', 'uploading', 'draft', 'validating', 'verified', 'rejected', 'superseded')
);
ALTER TABLE orch_tasks ADD COLUMN primary_artifact_id TEXT REFERENCES orch_artifact_versions(id);

CREATE INDEX orch_gate_results_subject
ON orch_gate_results(task_id, artifact_id, subject_type, subject_id, status);
CREATE INDEX orch_findings_blocking
ON orch_quality_findings(task_id, artifact_id, blocking, status);
CREATE INDEX orch_evaluations_artifact_type
ON orch_quality_evaluations(artifact_id, evaluation_type, created_at);
CREATE INDEX orch_rubric_scores_subject
ON orch_rubric_scores(artifact_id, rubric_id, scorer_run_id);
CREATE INDEX orch_waivers_subject
ON orch_quality_waivers(task_id, artifact_id, subject_type, subject_id, revoked_at, expires_at);

CREATE TRIGGER orch_rubric_no_update BEFORE UPDATE ON orch_quality_rubrics
BEGIN SELECT RAISE(ABORT, 'quality rubrics are immutable'); END;
CREATE TRIGGER orch_rubric_no_delete BEFORE DELETE ON orch_quality_rubrics
BEGIN SELECT RAISE(ABORT, 'quality rubrics are immutable'); END;
CREATE TRIGGER orch_rubric_score_no_update BEFORE UPDATE ON orch_rubric_scores
BEGIN SELECT RAISE(ABORT, 'rubric scores are immutable'); END;
CREATE TRIGGER orch_rubric_score_no_delete BEFORE DELETE ON orch_rubric_scores
BEGIN SELECT RAISE(ABORT, 'rubric scores are immutable'); END;
CREATE TRIGGER orch_gate_result_no_update BEFORE UPDATE ON orch_gate_results
BEGIN SELECT RAISE(ABORT, 'gate results are immutable'); END;
CREATE TRIGGER orch_gate_result_no_delete BEFORE DELETE ON orch_gate_results
BEGIN SELECT RAISE(ABORT, 'gate results are immutable'); END;
CREATE TRIGGER orch_evaluation_no_update BEFORE UPDATE ON orch_quality_evaluations
BEGIN SELECT RAISE(ABORT, 'quality evaluations are immutable'); END;
CREATE TRIGGER orch_evaluation_no_delete BEFORE DELETE ON orch_quality_evaluations
BEGIN SELECT RAISE(ABORT, 'quality evaluations are immutable'); END;
