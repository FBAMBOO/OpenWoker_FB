from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.models import TaskSpec
from coworker.orchestration.quality.artifacts import ArtifactService
from coworker.orchestration.quality.contract_compiler import ContractCompiler
from coworker.orchestration.quality.contract_linter import lint_contract
from coworker.orchestration.quality.contracts import ContractRepository
from coworker.orchestration.quality.models import RequirementCategory, SnapshotKind
from coworker.orchestration.quality import repository_resolver as resolver_module
from coworker.orchestration.quality.repository_resolver import (
    RepositoryResolver,
    git_command,
)
from coworker.orchestration.quality.repository_snapshot import (
    RepositorySnapshotService,
    SnapshotError,
)
from coworker.orchestration.store import OrchestrationStore


TEST12_PROMPT = (
    "只读分析当前 Fabric/dbt 项目的整体架构，识别 dbt 项目入口、models、macros、tests、"
    "seeds、snapshots 和部署配置之间的关系，并给出带文件证据的架构报告。不要修改任何文件。"
)


def test_50k_file_preflight_reads_metadata_only(monkeypatch, tmp_path) -> None:
    filenames = [f"file-{index:05d}.sql" for index in range(50_000)]
    filenames.append("dbt_project.yml")
    monkeypatch.setattr(
        resolver_module.os,
        "walk",
        lambda _root, followlinks=False: iter([(str(tmp_path), [], filenames)]),
    )
    monkeypatch.setattr(
        Path,
        "lstat",
        lambda _path: SimpleNamespace(st_size=128),
    )

    def body_read_forbidden(_path):
        raise AssertionError("preflight must not read file bodies")

    monkeypatch.setattr(Path, "read_bytes", body_read_forbidden)
    started = time.perf_counter()
    count, total, markers = resolver_module._walk_metadata(tmp_path)
    elapsed = time.perf_counter() - started
    assert count == 50_001
    assert total == 50_001 * 128
    assert [item.name for item in markers] == ["dbt_project.yml"]
    # A broad ceiling catches accidental content IO/algorithmic regressions while
    # the benchmark acceptance record carries the platform-specific p95 baseline.
    assert elapsed < 5


def test_git_probe_disables_hooks_fsmonitor_textconv_pager_and_env_injection(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.fsmonitor")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "malicious-command")
    captured = {}

    def run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(resolver_module.subprocess, "run", run)
    git_command(tmp_path, "status", "--porcelain=v1")
    command = captured["command"]
    settings = {
        command[index + 1]
        for index, value in enumerate(command[:-1])
        if value == "-c"
    }
    assert {
        f"core.hooksPath={resolver_module.os.devnull}",
        "diff.external=",
        f"core.attributesFile={resolver_module.os.devnull}",
        "core.pager=cat",
        "core.fsmonitor=false",
        "core.untrackedCache=false",
        "credential.helper=",
    } <= settings
    environment = captured["kwargs"]["env"]
    assert "GIT_CONFIG_COUNT" not in environment
    assert "GIT_CONFIG_KEY_0" not in environment
    assert environment["GIT_EXTERNAL_DIFF"] == ""
    assert environment["GIT_TERMINAL_PROMPT"] == "0"
    assert captured["kwargs"]["shell"] is False
    assert captured["kwargs"]["stdin"] is subprocess.DEVNULL
    with pytest.raises(ValueError, match="read-only allowlist"):
        git_command(tmp_path, "config", "credential.helper", "malicious")


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip()


def _repo(root: Path, *, content: str = "version: 1\n") -> None:
    root.mkdir(parents=True)
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.email", "quality@example.test")
    _git(root, "config", "user.name", "Quality Test")
    (root / "dbt_project.yml").write_text("name: fixture\n", encoding="utf-8")
    (root / "models").mkdir()
    (root / "models" / "model.sql").write_text(content, encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "initial")
    _git(root, "update-ref", "refs/remotes/origin/main", "HEAD")


@pytest.fixture
def snapshot_runtime(tmp_path):
    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(
        store, ContentAddressedBlobStore(tmp_path / "state" / "blobs")
    )
    service = RepositorySnapshotService(store, artifacts)
    try:
        yield store, service
    finally:
        store.close()


def _task(store: OrchestrationStore, key: str):
    return store.create_task(TaskSpec(idempotency_key=key, objective=TEST12_PROMPT))


def test_test12_goal_compiles_complete_traceable_seven_domain_contract() -> None:
    compiler = ContractCompiler()
    result = compiler.compile(task_id="task_test12", objective=TEST12_PROMPT)
    assert result.start_allowed is True
    assert result.contract.archetype.value == "repo_analysis"
    coverage = next(
        item
        for item in result.contract.requirements
        if item.id == "req-required-domains"
    )
    assert coverage.verification_spec["areas"] == [
        "entry",
        "models",
        "macros",
        "tests",
        "seeds",
        "snapshots",
        "deployment",
    ]
    categories = {
        item.category for item in result.contract.requirements if item.hard_gate
    }
    assert {
        RequirementCategory.CURRENTNESS,
        RequirementCategory.COVERAGE,
        RequirementCategory.RELATIONSHIP,
        RequirementCategory.EVIDENCE,
        RequirementCategory.LIMITATION,
        RequirementCategory.SAFETY,
        RequirementCategory.FORMAT,
    }.issubset(categories)
    safety = next(item for item in result.contract.requirements if item.id == "req-source-unchanged")
    assert safety.source_span is not None
    raw = TEST12_PROMPT.encode("utf-8")
    selected = raw[safety.source_span.start_byte : safety.source_span.end_byte].decode("utf-8")
    assert selected in {"只读", "不要修改", "不得修改"}
    assert result.contract.deliverables[0].filename == "fabric_dbt_architecture_report.md"
    assert result.contract.deliverables[0].primary is True
    result.contract.verify_content_hash()

    replay = compiler.compile(task_id="task_test12", objective=TEST12_PROMPT)
    assert replay.cache_hit is True
    assert replay.contract.id == result.contract.id


def test_read_only_as_only_requirement_fails_semantic_lint() -> None:
    contract = ContractCompiler().compile(
        task_id="task_test12", objective=TEST12_PROMPT
    ).contract
    safety = next(item for item in contract.requirements if item.category is RequirementCategory.SAFETY)
    broken = contract.model_copy(update={"requirements": (safety,)})
    codes = {item.code for item in lint_contract(broken)}
    assert "PERMISSION_ONLY_CONTRACT" in codes
    assert "REPOSITORY_AREAS_INCOMPLETE" in codes
    assert "MISSING_CURRENTNESS_GATE" in codes


def test_permission_conflict_is_explicit_and_blocks_start() -> None:
    result = ContractCompiler().compile(
        task_id="task_conflict",
        objective=TEST12_PROMPT,
        explicit_permissions={"source_workspace_write": True},
    )
    assert result.start_allowed is False
    assert {item.code for item in result.conflicts}.issuperset(
        {"PERMISSION_CEILING_CONFLICT", "READ_ONLY_CONFLICT"}
    )
    constraint = next(
        item for item in result.contract.constraints if item.type == "source_workspace_write"
    )
    assert constraint.value is False


def test_published_contract_and_snapshot_update_delete_triggers_fail_closed(
    snapshot_runtime, tmp_path
) -> None:
    store, snapshots = snapshot_runtime
    task = _task(store, "immutable-contract-snapshot")
    contracts = ContractRepository(store)
    draft = contracts.save_draft(
        ContractCompiler().compile(task_id=task.id, objective=TEST12_PROMPT).contract
    )
    with store._write() as connection:
        connection.execute(
            """
            CREATE TRIGGER inject_contract_publish_crash
            BEFORE UPDATE OF active_contract_id ON orch_tasks
            BEGIN SELECT RAISE(ABORT, 'injected contract publish crash'); END
            """
        )
    with pytest.raises(Exception, match="injected contract publish crash"):
        contracts.publish(draft.id, if_match=draft.content_hash)
    assert contracts.get(draft.id).status.value == "draft"
    with store._read() as connection:
        assert connection.execute(
            "SELECT active_contract_id FROM orch_tasks WHERE id=?", (task.id,)
        ).fetchone()[0] is None
    with store._write() as connection:
        connection.execute("DROP TRIGGER inject_contract_publish_crash")
    contract = contracts.publish(draft.id, if_match=draft.content_hash)
    repo = tmp_path / "immutable-repo"
    _repo(repo)
    snapshot = snapshots.freeze(
        task_id=task.id,
        resolution=RepositoryResolver().resolve(repo, objective=TEST12_PROMPT),
    )
    statements = (
        ("UPDATE orch_quality_contracts SET title='tampered' WHERE id=?", contract.id),
        ("DELETE FROM orch_quality_contracts WHERE id=?", contract.id),
        (
            "UPDATE orch_repository_snapshots SET resolution_reason='tampered' WHERE id=?",
            snapshot.id,
        ),
        ("DELETE FROM orch_repository_snapshots WHERE id=?", snapshot.id),
    )
    for sql, identifier in statements:
        with store._write() as connection, pytest.raises(Exception, match="immutable"):
            connection.execute(sql, (identifier,))


def test_snapshot_manifest_crash_leaves_no_active_snapshot_and_retry_recovers(
    snapshot_runtime, tmp_path, monkeypatch
) -> None:
    store, snapshots = snapshot_runtime
    repo = tmp_path / "manifest-crash"
    _repo(repo)
    task = _task(store, "manifest-crash")
    resolution = RepositoryResolver().resolve(repo, objective=TEST12_PROMPT)
    original_complete = snapshots.artifacts.complete
    injected = False

    def crash_after_manifest_write(upload_id, **kwargs):
        nonlocal injected
        artifact = original_complete(upload_id, **kwargs)
        if artifact.filename.startswith("snapshot_manifest_") and not injected:
            injected = True
            raise RuntimeError("injected snapshot manifest crash")
        return artifact

    monkeypatch.setattr(snapshots.artifacts, "complete", crash_after_manifest_write)
    with pytest.raises(RuntimeError, match="injected snapshot manifest crash"):
        snapshots.freeze(task_id=task.id, resolution=resolution)
    with store._read() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM orch_repository_snapshots WHERE task_id=?",
            (task.id,),
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT active_snapshot_id FROM orch_tasks WHERE id=?", (task.id,)
        ).fetchone()[0] is None

    monkeypatch.setattr(snapshots.artifacts, "complete", original_complete)
    recovered = snapshots.freeze(task_id=task.id, resolution=resolution)
    assert recovered.version == 1
    assert snapshots.get(recovered.id).content_hash == recovered.content_hash
    assert snapshots.artifacts.integrity_scan()["status"] == "pass"


def test_stale_checkout_recommends_local_default_ref_without_fetch(tmp_path) -> None:
    repo = tmp_path / "repo"
    _repo(repo, content="select 1 as old\n")
    _git(repo, "switch", "-c", "feature-old")
    _git(repo, "switch", "main")
    (repo / "models" / "model.sql").write_text("select 2 as current\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "advance main")
    current_main = _git(repo, "rev-parse", "HEAD")
    _git(repo, "update-ref", "refs/remotes/origin/main", current_main)
    _git(repo, "switch", "feature-old")

    resolution = RepositoryResolver().resolve(repo, objective=TEST12_PROMPT)
    candidate = resolution.recommended()
    assert resolution.status == "resolved"
    assert candidate is not None
    assert candidate.recommended_ref == "origin/main"
    assert candidate.behind == 1
    assert candidate.ahead == 0
    assert candidate.project_root == "."


def test_explicit_current_checkout_freezes_dirty_overlay(snapshot_runtime, tmp_path) -> None:
    store, snapshots = snapshot_runtime
    repo = tmp_path / "working"
    _repo(repo, content="select 1 as committed\n")
    dirty = "select 2 as frozen_dirty\n"
    (repo / "models" / "model.sql").write_text(dirty, encoding="utf-8")
    frozen_bytes = (repo / "models" / "model.sql").read_bytes()
    objective = "Read-only analyze the current checkout and do not modify files."
    resolution = RepositoryResolver().resolve(repo, objective=objective)
    candidate = resolution.recommended()
    assert candidate is not None
    assert candidate.recommended_snapshot_kind is SnapshotKind.WORKING_TREE
    task = _task(store, "dirty-overlay")
    snapshot = snapshots.freeze(task_id=task.id, resolution=resolution)
    assert snapshot.snapshot_kind is SnapshotKind.WORKING_TREE
    assert snapshot.overlay_artifact_id is not None

    (repo / "models" / "model.sql").write_text(
        "select 999 as live_changed_after_freeze\n", encoding="utf-8"
    )
    assert snapshots.read_file(snapshot.id, "models/model.sql") == frozen_bytes


def test_ref_move_after_commit_freeze_does_not_change_reads(snapshot_runtime, tmp_path) -> None:
    store, snapshots = snapshot_runtime
    repo = tmp_path / "ref-move"
    _repo(repo, content="select 1 as frozen\n")
    resolution = RepositoryResolver().resolve(repo, objective=TEST12_PROMPT)
    task = _task(store, "ref-move")
    snapshot = snapshots.freeze(task_id=task.id, resolution=resolution)
    frozen_oid = snapshot.commit_oid

    (repo / "models" / "model.sql").write_text("select 2 as moved\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "move ref")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    assert _git(repo, "rev-parse", "HEAD") != frozen_oid
    assert snapshots.read_file(snapshot.id, "models/model.sql") == b"select 1 as frozen\n"


def test_non_git_directory_pack_never_falls_back_to_live_files(
    snapshot_runtime, tmp_path
) -> None:
    store, snapshots = snapshot_runtime
    directory = tmp_path / "plain"
    directory.mkdir()
    (directory / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
    (directory / "source.py").write_text("VALUE = 1\n", encoding="utf-8")
    frozen_bytes = (directory / "source.py").read_bytes()
    resolution = RepositoryResolver().resolve(
        directory, objective="Read-only architecture analysis of this current project"
    )
    assert resolution.recommended().recommended_snapshot_kind is SnapshotKind.DIRECTORY
    task = _task(store, "directory-pack")
    snapshot = snapshots.freeze(task_id=task.id, resolution=resolution)
    (directory / "source.py").write_text("VALUE = 999\n", encoding="utf-8")
    assert snapshots.read_file(snapshot.id, "source.py") == frozen_bytes


def test_multiple_equal_repositories_require_target_selection(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _repo(workspace / "a")
    _repo(workspace / "b")
    resolution = RepositoryResolver().resolve(workspace, objective=TEST12_PROMPT)
    assert resolution.status == "needs_target_selection"
    assert resolution.recommended_candidate_id is None
    assert len(resolution.candidates) == 2


def test_snapshot_reads_reject_windows_and_traversal_paths(
    snapshot_runtime, tmp_path
) -> None:
    store, snapshots = snapshot_runtime
    repo = tmp_path / "safe-path"
    _repo(repo)
    task = _task(store, "safe-path")
    snapshot = snapshots.freeze(
        task_id=task.id,
        resolution=RepositoryResolver().resolve(repo, objective=TEST12_PROMPT),
    )
    for path in (
        "../secret",
        "C:secret",
        "C:/secret",
        "\\\\server\\share",
        "/etc/passwd",
        "safe\x00secret",
    ):
        with pytest.raises(ValueError):
            snapshots.read_file(snapshot.id, path)


def test_directory_snapshot_rejects_symlink_that_escapes_root(
    snapshot_runtime, tmp_path
) -> None:
    store, snapshots = snapshot_runtime
    root = tmp_path / "symlink-root"
    root.mkdir()
    outside = tmp_path / "outside-secret.txt"
    outside.write_text("must not enter snapshot\n", encoding="utf-8")
    try:
        os.symlink(outside, root / "escape.txt")
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable on this Windows host: {exc}")
    task = _task(store, "escaping-symlink")
    resolution = RepositoryResolver().resolve(
        root, objective="Analyze this directory without following symlinks"
    )
    with pytest.raises(SnapshotError, match="symlink escapes"):
        snapshots.freeze(task_id=task.id, resolution=resolution)
