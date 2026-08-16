from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

import coworker.orchestration.workspace as workspace_module
from coworker.orchestration.workspace import WorkspaceError, WorkspaceManager


class SimulatedHardCrash(BaseException):
    """Bypass prepare's ordinary exception cleanup like an exited process would."""


def _source(tmp_path: Path) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    (source / "value.txt").write_text("baseline\n", encoding="utf-8")
    return source


def _quarantined(manager: WorkspaceManager, snapshot_id: str) -> list[Path]:
    root = manager.base_dir / "snapshot-quarantine"
    return sorted(root.glob(f"{snapshot_id}-*")) if root.exists() else []


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _clean_git_source(tmp_path: Path) -> Path:
    if shutil.which("git") is None:
        pytest.skip("git is not installed")
    source = tmp_path / "repository"
    source.mkdir()
    _git(source, "init")
    _git(source, "config", "user.email", "tests@example.invalid")
    _git(source, "config", "user.name", "Tests")
    (source / "tracked.txt").write_text("baseline\n", encoding="utf-8")
    _git(source, "add", "tracked.txt")
    _git(source, "commit", "-m", "baseline")
    return source


def test_snapshot_metadata_is_atomic_fsynced_and_has_durable_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(tmp_path)
    manager = WorkspaceManager(tmp_path / "workspaces")
    fsync_calls: list[int] = []
    original_fsync = workspace_module.os.fsync

    def recording_fsync(descriptor: int) -> None:
        fsync_calls.append(descriptor)
        original_fsync(descriptor)

    monkeypatch.setattr(workspace_module.os, "fsync", recording_fsync)
    snapshot = manager.prepare(source, snapshot_id="atomic-metadata")

    metadata_path = snapshot.workspace_root / "snapshot.json"
    owner_path = snapshot.workspace_root / ".snapshot-owner.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    owner = json.loads(owner_path.read_text(encoding="utf-8"))
    assert metadata["snapshot_id"] == snapshot.snapshot_id
    assert owner == {
        "schema_version": 1,
        "snapshot_id": snapshot.snapshot_id,
        "source_root": str(source.resolve()),
        "created_at": owner["created_at"],
    }
    assert fsync_calls, "owner and snapshot metadata must be flushed before publication"
    assert not list(snapshot.workspace_root.glob("snapshot.json.tmp-*"))
    assert not list(snapshot.workspace_root.glob(".snapshot-owner.json.tmp-*"))
    assert manager.load(snapshot.snapshot_id) == snapshot


def test_prepare_quarantines_and_rebuilds_after_crash_before_metadata_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(tmp_path)
    manager = WorkspaceManager(tmp_path / "workspaces")
    original_replace = workspace_module.os.replace

    def crash_on_snapshot_metadata(source_path, destination_path) -> None:
        if Path(destination_path).name == "snapshot.json":
            raise SimulatedHardCrash("power loss before metadata rename")
        original_replace(source_path, destination_path)

    with monkeypatch.context() as patcher:
        patcher.setattr(workspace_module.os, "replace", crash_on_snapshot_metadata)
        with pytest.raises(SimulatedHardCrash):
            manager.prepare(source, snapshot_id="crashed-metadata")

    orphan = manager.base_dir / "snapshots" / "crashed-metadata"
    assert orphan.is_dir()
    assert (orphan / ".snapshot-owner.json").is_file()
    assert not (orphan / "snapshot.json").exists()

    rebuilt = WorkspaceManager(manager.base_dir).prepare(
        source, snapshot_id="crashed-metadata"
    )
    assert rebuilt.candidate.joinpath("value.txt").read_text(encoding="utf-8") == "baseline\n"
    quarantined = _quarantined(manager, "crashed-metadata")
    assert len(quarantined) == 1
    assert (quarantined[0] / ".snapshot-owner.json").is_file()
    assert (quarantined[0] / "baseline" / "value.txt").is_file()
    assert WorkspaceManager(manager.base_dir).load("crashed-metadata") == rebuilt


def test_prepare_rebuild_removes_crashed_git_worktree_registration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _clean_git_source(tmp_path)
    manager = WorkspaceManager(tmp_path / "workspaces")
    original_replace = workspace_module.os.replace

    def crash_on_snapshot_metadata(source_path, destination_path) -> None:
        if Path(destination_path).name == "snapshot.json":
            raise SimulatedHardCrash("power loss after git worktree creation")
        original_replace(source_path, destination_path)

    with monkeypatch.context() as patcher:
        patcher.setattr(workspace_module.os, "replace", crash_on_snapshot_metadata)
        with pytest.raises(SimulatedHardCrash):
            manager.prepare(source, snapshot_id="crashed-git")

    orphan_candidate = manager.base_dir / "snapshots" / "crashed-git" / "candidate"
    assert (orphan_candidate / ".git").is_file()
    worktrees = _git(source, "worktree", "list", "--porcelain").replace("\\", "/").lower()
    assert orphan_candidate.as_posix().lower() in worktrees

    rebuilt = WorkspaceManager(manager.base_dir).prepare(
        source, snapshot_id="crashed-git"
    )
    assert (rebuilt.candidate / ".git").is_file()
    worktrees = _git(source, "worktree", "list", "--porcelain").replace("\\", "/").lower()
    assert rebuilt.candidate.as_posix().lower() in worktrees
    assert len(_quarantined(manager, "crashed-git")) == 1


def test_prepare_recovers_legacy_empty_or_partial_metadata_directory(tmp_path: Path) -> None:
    source = _source(tmp_path)
    manager = WorkspaceManager(tmp_path / "workspaces")
    orphan = manager.base_dir / "snapshots" / "legacy-partial"
    (orphan / "baseline").mkdir(parents=True)
    (orphan / "candidate").mkdir()
    (orphan / "baseline" / "partial.txt").write_text("old", encoding="utf-8")
    (orphan / "snapshot.json").write_text('{"snapshot_id":', encoding="utf-8")

    rebuilt = manager.prepare(source, snapshot_id="legacy-partial")

    assert manager.load("legacy-partial") == rebuilt
    quarantined = _quarantined(manager, "legacy-partial")
    assert len(quarantined) == 1
    assert (quarantined[0] / "snapshot.json").read_text(encoding="utf-8") == '{"snapshot_id":'
    assert (quarantined[0] / "baseline" / "partial.txt").read_text(encoding="utf-8") == "old"


def test_prepare_never_replaces_a_complete_snapshot(tmp_path: Path) -> None:
    source = _source(tmp_path)
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="complete")
    marker = snapshot.candidate / "caller-state.txt"
    marker.write_text("preserve me", encoding="utf-8")

    with pytest.raises(WorkspaceError, match="already exists"):
        WorkspaceManager(manager.base_dir).prepare(source, snapshot_id="complete")

    assert marker.read_text(encoding="utf-8") == "preserve me"
    assert _quarantined(manager, "complete") == []


def test_prepare_refuses_unknown_or_mismatched_snapshot_directory(tmp_path: Path) -> None:
    source = _source(tmp_path)
    manager = WorkspaceManager(tmp_path / "workspaces")
    foreign = manager.base_dir / "snapshots" / "foreign"
    foreign.mkdir(parents=True)
    protected = foreign / "do-not-delete.txt"
    protected.write_text("operator data", encoding="utf-8")

    with pytest.raises(WorkspaceError, match="not a recoverable incomplete snapshot"):
        manager.prepare(source, snapshot_id="foreign")

    assert protected.read_text(encoding="utf-8") == "operator data"
    assert _quarantined(manager, "foreign") == []

    mismatched = manager.base_dir / "snapshots" / "mismatched"
    mismatched.mkdir()
    (mismatched / ".snapshot-owner.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_id": "mismatched",
                "source_root": str((tmp_path / "other-source").resolve()),
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(WorkspaceError, match="not a recoverable incomplete snapshot"):
        manager.prepare(source, snapshot_id="mismatched")
    assert mismatched.is_dir()
    assert _quarantined(manager, "mismatched") == []
