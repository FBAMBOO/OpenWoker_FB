from __future__ import annotations

import hashlib
import sqlite3

import pytest

from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.errors import ConflictError
from coworker.orchestration.models import NodeSpec, PlanSpec, RetryPolicy, TaskSpec
from coworker.orchestration.quality.artifact_security import (
    ArtifactSecurityError,
    preview_policy,
)
from coworker.orchestration.quality.artifacts import ArtifactService
from coworker.orchestration.store import OrchestrationStore


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


@pytest.mark.parametrize(
    ("filename", "mime_type"),
    [
        ("report.html", "text/html"),
        ("diagram.svg", "image/svg+xml"),
        ("script.ps1", "text/plain"),
        ("program.exe", "application/octet-stream"),
    ],
)
def test_executable_artifacts_are_download_only(filename: str, mime_type: str) -> None:
    policy = preview_policy(filename, mime_type)
    assert policy == {
        "download_allowed": True,
        "inline_preview_allowed": False,
        "execute_allowed": False,
    }
    assert preview_policy("report.md", "text/markdown")["inline_preview_allowed"] is True


@pytest.fixture
def artifact_context(tmp_path):
    store = OrchestrationStore(tmp_path / "orchestration.db")
    task = store.create_task(
        TaskSpec(idempotency_key="artifact-test", objective="Create and review report")
    )
    plan = store.create_plan_revision(
        task.id,
        PlanSpec(
            nodes=(
                NodeSpec(
                    key="producer",
                    agent="worker",
                    retry_policy=RetryPolicy(max_attempts=2),
                ),
                NodeSpec(key="reviewer", agent="reviewer", kind="review"),
            ),
            edges=(),
        ),
        expected_task_version=task.version,
        created_by="test",
    )
    with store._write() as connection:
        connection.execute(
            "UPDATE orch_tasks SET status='queued' WHERE id=?", (task.id,)
        )
    producer = store.enqueue_run(task.id, "producer", plan_id=plan.plan.id)
    reviewer = store.enqueue_run(task.id, "reviewer", plan_id=plan.plan.id)
    service = ArtifactService(
        store,
        ContentAddressedBlobStore(tmp_path / "blobs"),
        max_artifact_bytes=10 * 1024 * 1024,
    )
    try:
        yield service, store, task, producer, reviewer
    finally:
        store.close()


def _upload(
    service: ArtifactService,
    *,
    task_id: str,
    run_id: str,
    content: bytes,
    deliverable: str = "report",
    filename: str = "report.md",
):
    created = service.create(
        task_id,
        logical_deliverable_id=deliverable,
        filename=filename,
        mime_type="text/markdown",
        run_id=run_id,
        attempt=1,
        producer_profile_id="producer",
    )
    chunk_size = 8192
    for sequence, start in enumerate(range(0, len(content), chunk_size)):
        chunk = content[start : start + chunk_size]
        service.append(
            created["upload_id"],
            sequence=sequence,
            content=chunk,
            chunk_hash=_digest(chunk),
            caller_task_id=task_id,
            caller_run_id=run_id,
        )
    return service.complete(
        created["upload_id"],
        expected_sha256=_digest(content),
        caller_task_id=task_id,
        caller_run_id=run_id,
    )


@pytest.mark.parametrize("size", [1, 8192, 10 * 1024, 10_757, 8 * 1024 * 1024])
def test_artifact_sizes_finalize_and_range_read(artifact_context, size: int) -> None:
    service, _, task, producer, _ = artifact_context
    content = (b"x" * size) if size == 1 else (b"# Report\n" + b"x" * (size - 9))
    artifact = _upload(
        service,
        task_id=task.id,
        run_id=producer.id,
        content=content,
        deliverable=f"report-{size}",
        filename=f"report-{size}.md",
    )
    assert artifact.byte_size == size
    assert artifact.sha256 == _digest(content)
    selected = service.read(
        artifact.id,
        expected_sha256=artifact.sha256,
        start_byte=0,
        end_byte=min(size, 17),
        caller_task_id=task.id,
        caller_run_id=producer.id,
    )
    assert selected["content"] == content[: min(size, 17)]
    assert selected["etag"] == artifact.sha256


def test_exact_candidate_requires_server_derived_full_receipt(artifact_context) -> None:
    service, _, task, producer, reviewer = artifact_context
    content = b"# Report\n" + b"evidence\n" * 2_000
    artifact = _upload(
        service, task_id=task.id, run_id=producer.id, content=content
    )
    receipt = service.bind_candidate(
        run_id=reviewer.id,
        artifact_id=artifact.id,
        expected_sha256=artifact.sha256,
        verifier_profile_id="reviewer",
        caller_task_id=task.id,
    )
    service.read(
        artifact.id,
        expected_sha256=artifact.sha256,
        start_byte=0,
        end_byte=len(content) // 2,
        caller_task_id=task.id,
        caller_run_id=reviewer.id,
        receipt_id=receipt.id,
    )
    assert service.fresh_complete_receipt(
        run_id=reviewer.id,
        artifact_id=artifact.id,
        expected_sha256=artifact.sha256,
    ) is None
    completed = service.read(
        artifact.id,
        expected_sha256=artifact.sha256,
        start_byte=len(content) // 2,
        end_byte=len(content),
        caller_task_id=task.id,
        caller_run_id=reviewer.id,
        receipt_id=receipt.id,
    )["receipt"]
    assert completed["covered_bytes"] == len(content)
    assert completed["coverage_ratio"] == 1.0
    assert service.fresh_complete_receipt(
        run_id=reviewer.id,
        artifact_id=artifact.id,
        expected_sha256=artifact.sha256,
    ) is not None


def test_old_producer_attempt_artifact_cannot_be_bound_to_downstream_review(
    artifact_context,
) -> None:
    service, store, task, producer, reviewer = artifact_context
    artifact = _upload(
        service,
        task_id=task.id,
        run_id=producer.id,
        content=b"# stale attempt\n",
    )
    with store._write() as connection:
        connection.execute(
            "UPDATE orch_runs SET status='failed' WHERE id=?",
            (producer.id,),
        )
    retry = store.enqueue_run(task.id, "producer", plan_id=producer.plan_id)
    assert retry.attempt == producer.attempt + 1

    with pytest.raises(ConflictError, match="old-attempt artifact replay"):
        service.bind_candidate(
            run_id=reviewer.id,
            artifact_id=artifact.id,
            expected_sha256=artifact.sha256,
            verifier_profile_id="reviewer",
            caller_task_id=task.id,
        )


def test_finalize_hash_mismatch_fails_closed(artifact_context) -> None:
    service, store, task, producer, _ = artifact_context
    created = service.create(
        task.id,
        logical_deliverable_id="bad-hash",
        filename="bad.md",
        mime_type="text/markdown",
        run_id=producer.id,
    )
    content = b"safe content"
    service.append(
        created["upload_id"],
        sequence=0,
        content=content,
        chunk_hash=_digest(content),
    )
    with pytest.raises(ArtifactSecurityError, match="hash mismatch"):
        service.complete(created["upload_id"], expected_sha256="sha256:" + "0" * 64)
    with store._read() as connection:
        assert connection.execute(
            "SELECT status FROM orch_artifact_uploads WHERE id=?",
            (created["upload_id"],),
        ).fetchone()["status"] == "failed"
        event = connection.execute(
            """
            SELECT payload_json FROM orch_events
            WHERE aggregate_id=? AND event_type='artifact_hash_failed'
            """,
            (created["artifact_id"],),
        ).fetchone()
    assert event is not None
    assert "safe content" not in event["payload_json"]


def test_final_blob_hash_swap_fails_read_and_appends_security_event(
    artifact_context,
) -> None:
    service, store, task, producer, _ = artifact_context
    artifact = _upload(
        service,
        task_id=task.id,
        run_id=producer.id,
        content=b"# immutable report\n",
    )
    service.blobs._path(str(artifact.sha256).removeprefix("sha256:")).write_bytes(
        b"tampered bytes"
    )
    with pytest.raises(ArtifactSecurityError, match="integrity"):
        service.read(
            artifact.id,
            expected_sha256=str(artifact.sha256),
            caller_task_id=task.id,
        )
    with store._read() as connection:
        event = connection.execute(
            """
            SELECT payload_json FROM orch_events
            WHERE aggregate_id=? AND event_type='artifact_hash_failed'
            ORDER BY sequence_no DESC LIMIT 1
            """,
            (artifact.id,),
        ).fetchone()
    assert event is not None
    assert "tampered bytes" not in event["payload_json"]


def test_db_backup_restore_preserves_blob_references_and_integrity_scan(
    artifact_context, tmp_path
) -> None:
    service, store, task, producer, _ = artifact_context
    artifact = _upload(
        service,
        task_id=task.id,
        run_id=producer.id,
        content=b"# restore-safe artifact\n",
    )
    before = service.integrity_scan()
    assert before["status"] == "pass"
    assert before["failure_count"] == 0

    backup_path = tmp_path / "restored-orchestration.db"
    with store.connect() as source, sqlite3.connect(backup_path) as destination:
        source.backup(destination)
    restored_store = OrchestrationStore(backup_path)
    restored_service = ArtifactService(restored_store, service.blobs)
    try:
        restored = restored_service.integrity_scan()
        assert restored["status"] == "pass"
        assert restored["verified_references"] == before["verified_references"]

        # Additive orphan blobs are reported for operator cleanup but never deleted.
        orphan = service.blobs.put(b"unreferenced crash residue")
        assert restored_service.integrity_scan()["orphan_blob_count"] >= 1
        assert service.blobs.get(orphan) == b"unreferenced crash residue"

        service.blobs._path(str(artifact.sha256).removeprefix("sha256:")).write_bytes(
            b"corrupted after restore"
        )
        failed = restored_service.integrity_scan()
        assert failed["status"] == "fail"
        assert failed["failure_count"] >= 1
        assert all("corrupted after restore" not in str(item) for item in failed["failures"])
        assert failed["privacy"] == "content_free_metadata_only"
    finally:
        restored_store.close()


def test_finalize_crash_after_blob_write_is_retryable_and_never_half_commits(
    artifact_context, monkeypatch
) -> None:
    service, store, task, producer, _ = artifact_context
    created = service.create(
        task.id,
        logical_deliverable_id="crash-safe",
        filename="crash-safe.md",
        mime_type="text/markdown",
        run_id=producer.id,
    )
    chunks = (b"# crash-safe\nfirst half\n", b"second half\n")
    for sequence, chunk in enumerate(chunks):
        service.append(
            created["upload_id"],
            sequence=sequence,
            content=chunk,
            chunk_hash=_digest(chunk),
        )
    content = b"".join(chunks)
    original_put = service.blobs.put
    injected = False

    def crash_after_put(value, **kwargs):
        nonlocal injected
        blob = original_put(value, **kwargs)
        if not injected:
            injected = True
            raise RuntimeError("injected artifact finalize crash")
        return blob

    monkeypatch.setattr(service.blobs, "put", crash_after_put)
    with pytest.raises(RuntimeError, match="injected artifact finalize crash"):
        service.complete(created["upload_id"], expected_sha256=_digest(content))
    with store._read() as connection:
        upload = connection.execute(
            "SELECT status FROM orch_artifact_uploads WHERE id=?",
            (created["upload_id"],),
        ).fetchone()
        artifact = connection.execute(
            "SELECT status, blob_uri FROM orch_artifact_versions WHERE id=?",
            (created["artifact_id"],),
        ).fetchone()
    assert upload["status"] == "open"
    assert dict(artifact) == {"status": "uploading", "blob_uri": None}
    assert service.integrity_scan()["orphan_blob_count"] >= 1

    monkeypatch.setattr(service.blobs, "put", original_put)
    recovered = service.complete(
        created["upload_id"], expected_sha256=_digest(content)
    )
    assert recovered.sha256 == _digest(content)
    assert recovered.status.value == "draft"
    assert service.integrity_scan()["status"] == "pass"


def test_final_artifact_is_immutable_and_repair_creates_child_version(
    artifact_context,
) -> None:
    service, store, task, producer, _ = artifact_context
    first = _upload(
        service, task_id=task.id, run_id=producer.id, content=b"# V1\n"
    )
    with store._write() as connection, pytest.raises(Exception, match="immutable"):
        connection.execute(
            "UPDATE orch_artifact_versions SET byte_size=999 WHERE id=?", (first.id,)
        )
    created = service.create(
        task.id,
        logical_deliverable_id="report",
        filename="report.md",
        mime_type="text/markdown",
        run_id=producer.id,
        parent_artifact_id=first.id,
    )
    service.append(
        created["upload_id"], sequence=0, content=b"# V2\n", chunk_hash=_digest(b"# V2\n")
    )
    second = service.complete(
        created["upload_id"], expected_sha256=_digest(b"# V2\n")
    )
    assert second.version == first.version + 1
    assert second.parent_artifact_id == first.id
    assert service.get(first.id).sha256 == _digest(b"# V1\n")


def test_cross_task_artifact_and_unsafe_filename_are_rejected(artifact_context) -> None:
    service, store, task, producer, _ = artifact_context
    artifact = _upload(
        service, task_id=task.id, run_id=producer.id, content=b"# report\n"
    )
    other = store.create_task(
        TaskSpec(idempotency_key="other-task", objective="Attempt cross-task read")
    )
    with pytest.raises(PermissionError):
        service.read(
            artifact.id,
            expected_sha256=artifact.sha256,
            caller_task_id=other.id,
        )
    for filename in ("../report.md", "C:\\report.md", "CON", "folder/report.md"):
        with pytest.raises(ArtifactSecurityError):
            service.create(
                task.id,
                logical_deliverable_id="unsafe",
                filename=filename,
                mime_type="text/markdown",
            )


def test_text_artifact_with_high_confidence_secret_is_rejected(artifact_context) -> None:
    service, _, task, producer, _ = artifact_context
    created = service.create(
        task.id,
        logical_deliverable_id="secret",
        filename="secret.md",
        mime_type="text/markdown",
        run_id=producer.id,
    )
    content = b"api_key = 'abcdefghijklmnopqrstuvwxyz123456'"
    service.append(
        created["upload_id"], sequence=0, content=content, chunk_hash=_digest(content)
    )
    with pytest.raises(ArtifactSecurityError, match="secret"):
        service.complete(created["upload_id"], expected_sha256=_digest(content))
