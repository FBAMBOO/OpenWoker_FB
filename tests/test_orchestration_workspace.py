import json
import shutil
import subprocess

import pytest

from coworker.orchestration.workspace import (
    WorkspaceConflictError,
    WorkspaceError,
    WorkspaceKind,
    WorkspaceManager,
    build_manifest,
)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def git(root, *args):
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def init_git_repo(root):
    if shutil.which("git") is None:
        pytest.skip("git is not installed")
    root.mkdir()
    git(root, "init")
    git(root, "config", "user.email", "tests@example.invalid")
    git(root, "config", "user.name", "Tests")
    write(root / "tracked.txt", "baseline\n")
    git(root, "add", "tracked.txt")
    git(root, "commit", "-m", "baseline")


def test_non_git_snapshot_evidence_delivery_and_journal(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "modify.txt", "before\n")
    write(source / "delete.txt", "remove me\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="plain")

    assert snapshot.kind is WorkspaceKind.FILESYSTEM_SNAPSHOT
    write(snapshot.candidate / "modify.txt", "after\n")
    write(snapshot.candidate / "new" / "added.txt", "new\n")
    (snapshot.candidate / "delete.txt").unlink()

    evidence = manager.collect_candidate(snapshot)
    assert evidence.changed_files == ("modify.txt", "new/added.txt")
    assert evidence.new_files == ("new/added.txt",)
    assert evidence.deleted_files == ("delete.txt",)
    assert "-before" in evidence.patch and "+after" in evidence.patch
    assert (snapshot.workspace_root / "candidate.patch").read_text(encoding="utf-8") == evidence.patch
    assert json.loads((snapshot.workspace_root / "evidence.json").read_text(encoding="utf-8"))["patch_sha256"] == evidence.patch_sha256

    preflight = manager.preflight(snapshot)
    assert preflight.can_deliver
    receipt = manager.deliver(snapshot)
    assert receipt.status == "delivered"
    assert receipt.candidate_manifest_sha256 == evidence.candidate_manifest.digest
    assert (source / "modify.txt").read_text(encoding="utf-8") == "after\n"
    assert (source / "new" / "added.txt").read_text(encoding="utf-8") == "new\n"
    assert not (source / "delete.txt").exists()
    assert [entry.status for entry in manager.journal()] == ["started", "delivered"]
    assert manager.journal()[-1] == receipt
    assert manager.incomplete_deliveries() == ()
    assert manager.load("plain").baseline_manifest == snapshot.baseline_manifest


def test_sealed_candidate_digest_is_checked_before_any_source_mutation(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "artifact.bin", "baseline")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="sealed")
    (snapshot.candidate / "artifact.bin").write_bytes(b"accepted-A\x00")
    sealed = manager.collect_candidate(snapshot)

    # A stale/orphaned writer changes the isolated candidate after the run result
    # sealed its manifest. Binary patch text alone cannot distinguish these bytes.
    (snapshot.candidate / "artifact.bin").write_bytes(b"tampered-B\x00")
    with pytest.raises(WorkspaceError, match="manifest changed after it was sealed"):
        manager.deliver(
            snapshot,
            expected_candidate_manifest_sha256=sealed.candidate_manifest.digest,
            expected_patch_sha256=sealed.patch_sha256,
        )

    assert (source / "artifact.bin").read_text(encoding="utf-8") == "baseline"
    assert manager.journal() == ()


def test_delivery_rechecks_scheduler_fence_before_source_mutation(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "artifact.txt", "baseline\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="fenced-delivery")
    write(snapshot.candidate / "artifact.txt", "candidate\n")
    checks = 0

    def fence_check() -> None:
        nonlocal checks
        checks += 1
        if checks >= 2:
            raise RuntimeError("stale scheduler epoch")

    with pytest.raises(WorkspaceError, match="stale scheduler epoch"):
        manager.deliver(snapshot, fence_check=fence_check)

    assert checks >= 2
    assert (source / "artifact.txt").read_text(encoding="utf-8") == "baseline\n"
    assert [entry.status for entry in manager.journal()] == ["started", "failed"]


def test_conflicting_source_edit_blocks_delivery_and_is_journaled(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "shared.txt", "base\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="conflict")
    write(snapshot.candidate / "shared.txt", "candidate\n")
    write(source / "shared.txt", "user\n")

    preflight = manager.preflight(snapshot)
    assert not preflight.can_deliver
    assert preflight.conflicts == ("shared.txt",)
    with pytest.raises(WorkspaceConflictError):
        manager.deliver(snapshot)
    assert (source / "shared.txt").read_text(encoding="utf-8") == "user\n"
    assert manager.journal()[-1].status == "blocked"


def test_separate_managers_cannot_deliver_same_source_concurrently(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "value.txt", "before\n")
    base = tmp_path / "workspaces"
    manager = WorkspaceManager(base)
    other_manager = WorkspaceManager(base)
    snapshot = manager.prepare(source, snapshot_id="locked-source")
    write(snapshot.candidate / "value.txt", "after\n")

    source_lock = manager._acquire_source_lock(source)
    try:
        with pytest.raises(WorkspaceError, match="another delivery"):
            other_manager.deliver(snapshot)
    finally:
        manager._release_source_lock(source_lock)
    assert (source / "value.txt").read_text(encoding="utf-8") == "before\n"


def test_same_source_and_candidate_edit_is_not_a_conflict(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "shared.txt", "base\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="same-change")
    write(snapshot.candidate / "shared.txt", "same\n")
    write(source / "shared.txt", "same\n")

    preflight = manager.preflight(snapshot)
    assert preflight.source_changed == ("shared.txt",)
    assert preflight.conflicts == ()
    manager.deliver(snapshot)


def test_interrupted_delivery_is_detected_recovered_and_blocks_cleanup(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "value.txt", "before\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="recoverable")
    write(snapshot.candidate / "value.txt", "after\n")
    receipt = manager.deliver(snapshot)

    # Simulate a crash after source mutation but before the terminal receipt became
    # durable by retaining only the fsynced intent record.
    started_line = manager.journal_path.read_text(encoding="utf-8").splitlines()[0]
    manager.journal_path.write_text(
        started_line + "\n" + '{"transaction_id":', encoding="utf-8"
    )
    assert manager.incomplete_deliveries()[0].transaction_id == receipt.transaction_id
    assert manager.journal_has_truncated_tail is True
    other_manager = WorkspaceManager(manager.base_dir)
    with pytest.raises(WorkspaceError, match="incomplete deliveries"):
        other_manager.deliver(snapshot)
    with pytest.raises(WorkspaceError, match="incomplete deliveries"):
        manager.cleanup(snapshot)

    recovered = manager.recover(receipt.transaction_id)
    assert recovered.status == "recovered"
    assert (source / "value.txt").read_text(encoding="utf-8") == "before\n"
    assert manager.incomplete_deliveries() == ()
    with pytest.raises(WorkspaceError, match="terminal"):
        other_manager.recover(receipt.transaction_id)
    manager.cleanup(snapshot)


def test_recovery_refuses_a_corrupt_backup_before_touching_source(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "value.txt", "before\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="corrupt-backup")
    write(snapshot.candidate / "value.txt", "after\n")
    receipt = manager.deliver(snapshot)
    started = manager.journal()[0]
    manager.journal_path.write_text(
        json.dumps(started.to_dict()) + "\n", encoding="utf-8"
    )
    write(snapshot.workspace_root / "delivery-backups" / receipt.transaction_id / "value.txt", "corrupt\n")

    with pytest.raises(WorkspaceError, match="integrity"):
        manager.recover(receipt.transaction_id)
    assert (source / "value.txt").read_text(encoding="utf-8") == "after\n"


def test_dirty_git_repository_uses_snapshot(tmp_path):
    source = tmp_path / "repo"
    init_git_repo(source)
    write(source / "tracked.txt", "dirty state\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="dirty-git")

    assert snapshot.kind is WorkspaceKind.GIT_SNAPSHOT
    assert snapshot.git_dirty is True
    assert (snapshot.candidate / "tracked.txt").read_text(encoding="utf-8") == "dirty state\n"
    assert not (snapshot.candidate / ".git").exists()


def test_unborn_git_repository_uses_dirty_snapshot(tmp_path):
    if shutil.which("git") is None:
        pytest.skip("git is not installed")
    source = tmp_path / "unborn"
    source.mkdir()
    git(source, "init")
    write(source / "untracked.txt", "initial\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="unborn-git")

    assert snapshot.kind is WorkspaceKind.GIT_SNAPSHOT
    assert snapshot.git_head is None
    assert snapshot.git_dirty is True
    assert (snapshot.candidate / "untracked.txt").is_file()


def test_clean_git_repository_uses_detached_worktree(tmp_path):
    source = tmp_path / "repo"
    init_git_repo(source)
    manager = WorkspaceManager(tmp_path / "workspaces")
    snapshot = manager.prepare(source, snapshot_id="clean-git")

    assert snapshot.kind is WorkspaceKind.GIT_WORKTREE
    assert snapshot.git_dirty is False
    assert (snapshot.candidate / ".git").is_file()
    assert not (snapshot.baseline / ".git").exists()
    write(snapshot.candidate / "tracked.txt", "candidate\n")
    assert (source / "tracked.txt").read_text(encoding="utf-8") == "baseline\n"
    manager.cleanup(snapshot)
    assert not snapshot.workspace_root.exists()
    assert "clean-git" not in git(source, "worktree", "list", "--porcelain")


def test_manifest_is_deterministic_and_manager_cannot_live_inside_source(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "b.txt", "b")
    write(source / "a.txt", "a")
    first = build_manifest(source)
    second = build_manifest(source)
    assert first == second
    assert [entry.path for entry in first.entries] == ["a.txt", "b.txt"]

    manager = WorkspaceManager(source / ".workspaces")
    with pytest.raises(WorkspaceError, match="outside"):
        manager.prepare(source)


def test_snapshot_paths_and_baseline_integrity_are_bound_to_manager(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write(source / "value.txt", "base\n")
    manager = WorkspaceManager(tmp_path / "workspaces")
    first = manager.prepare(source, snapshot_id="first")
    second = manager.prepare(source, snapshot_id="second")
    metadata_path = first.workspace_root / "snapshot.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["baseline_root"] = str(second.baseline_root)
    metadata["candidate_root"] = str(second.candidate_root)
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(WorkspaceError, match="root|path"):
        manager.load("first")

    write(second.baseline_root / "value.txt", "tampered\n")
    with pytest.raises(WorkspaceError, match="integrity"):
        manager.collect_candidate(second)
