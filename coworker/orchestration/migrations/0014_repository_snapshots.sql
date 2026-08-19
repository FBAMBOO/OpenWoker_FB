-- Frozen Git, working-tree and content-addressed directory snapshots.
-- Rollback: additive only. Preserve published snapshots and referenced artifacts;
-- downgrade code may ignore them but must never mutate published snapshot rows.

CREATE TABLE orch_repository_snapshots (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES orch_tasks(id),
    version INTEGER NOT NULL CHECK (version > 0),
    status TEXT NOT NULL CHECK (status IN ('draft', 'published', 'superseded')),
    workspace_root TEXT NOT NULL,
    repo_root TEXT NOT NULL,
    project_root TEXT NOT NULL,
    vcs_type TEXT NOT NULL CHECK (vcs_type IN ('git', 'none')),
    snapshot_kind TEXT NOT NULL CHECK (snapshot_kind IN ('commit', 'working_tree', 'directory')),
    selected_ref TEXT,
    vcs_object_format TEXT CHECK (vcs_object_format IS NULL OR vcs_object_format IN ('sha1', 'sha256')),
    commit_oid TEXT,
    base_tree_oid TEXT,
    head_oid TEXT,
    current_branch TEXT,
    default_ref TEXT,
    upstream_ref TEXT,
    ahead INTEGER CHECK (ahead IS NULL OR ahead >= 0),
    behind INTEGER CHECK (behind IS NULL OR behind >= 0),
    dirty INTEGER NOT NULL CHECK (dirty IN (0, 1)),
    worktree_count INTEGER NOT NULL CHECK (worktree_count >= 0),
    duplicate_roots_json TEXT NOT NULL DEFAULT '[]',
    ignore_rules_hash TEXT NOT NULL,
    manifest_artifact_id TEXT NOT NULL REFERENCES orch_artifact_versions(id),
    manifest_hash TEXT NOT NULL,
    overlay_artifact_id TEXT REFERENCES orch_artifact_versions(id),
    overlay_hash TEXT,
    directory_pack_artifact_id TEXT REFERENCES orch_artifact_versions(id),
    directory_pack_hash TEXT,
    resolution_confidence REAL NOT NULL CHECK (resolution_confidence >= 0 AND resolution_confidence <= 1),
    resolution_reason TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT,
    UNIQUE(task_id, version),
    UNIQUE(task_id, manifest_hash),
    CHECK (vcs_type <> 'git' OR (vcs_object_format IS NOT NULL AND commit_oid IS NOT NULL AND base_tree_oid IS NOT NULL)),
    CHECK (snapshot_kind <> 'working_tree' OR (overlay_artifact_id IS NOT NULL AND overlay_hash IS NOT NULL)),
    CHECK (snapshot_kind <> 'directory' OR (directory_pack_artifact_id IS NOT NULL AND directory_pack_hash IS NOT NULL))
);

ALTER TABLE orch_tasks ADD COLUMN active_snapshot_id TEXT REFERENCES orch_repository_snapshots(id);

CREATE INDEX orch_snapshots_task_created
ON orch_repository_snapshots(task_id, created_at DESC);

CREATE TRIGGER orch_published_snapshot_no_update
BEFORE UPDATE ON orch_repository_snapshots
WHEN OLD.status IN ('published', 'superseded')
BEGIN SELECT RAISE(ABORT, 'published repository snapshots are immutable'); END;

CREATE TRIGGER orch_published_snapshot_no_delete
BEFORE DELETE ON orch_repository_snapshots
WHEN OLD.status IN ('published', 'superseded')
BEGIN SELECT RAISE(ABORT, 'published repository snapshots are immutable'); END;
