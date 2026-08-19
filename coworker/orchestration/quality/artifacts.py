"""Task-owned immutable artifact versions and server-derived read receipts."""

from __future__ import annotations

import difflib
import json
import sqlite3
import uuid
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from ..blobs import BlobIntegrityError, BlobRef, ContentAddressedBlobStore
from ..errors import ConflictError, NotFoundError
from ..store import OrchestrationStore
from .artifact_security import (
    MAX_ARTIFACT_BYTES,
    MAX_CHUNK_BYTES,
    ArtifactSecurityError,
    authorize_artifact,
    digest_bytes,
    normalize_sha256,
    preview_policy,
    safe_filename,
    safe_mime_type,
    validate_size,
    validate_text_secret_boundary,
)
from .models import (
    ArtifactReadReceipt,
    ArtifactVersion,
    ArtifactVersionStatus,
    ByteRange,
)


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _ranges_union(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    ordered = sorted((int(start), int(end)) for start, end in ranges if end > start)
    merged: list[tuple[int, int]] = []
    for start, end in ordered:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


class ArtifactService:
    def __init__(
        self,
        store: OrchestrationStore,
        blobs: ContentAddressedBlobStore,
        *,
        max_artifact_bytes: int = MAX_ARTIFACT_BYTES,
        max_chunk_bytes: int = MAX_CHUNK_BYTES,
    ) -> None:
        self.store = store
        self.blobs = blobs
        self.max_artifact_bytes = max(1, int(max_artifact_bytes))
        self.max_chunk_bytes = max(1, min(int(max_chunk_bytes), self.max_artifact_bytes))

    def record_integrity_failure(
        self,
        *,
        task_id: str,
        artifact_id: str,
        code: str,
        upload_id: str | None = None,
    ) -> None:
        """Persist content-free security evidence without mutating final artifacts."""

        with self.store._write() as connection:
            if upload_id is not None:
                now = _stamp(_now())
                connection.execute(
                    """
                    UPDATE orch_artifact_uploads
                    SET status='failed', completed_at=?
                    WHERE id=? AND status='open'
                    """,
                    (now, upload_id),
                )
                connection.execute(
                    """
                    UPDATE orch_tasks
                    SET artifact_status='rejected', quality_status='fail',
                        quality_reason_code=?
                    WHERE id=?
                    """,
                    (code, task_id),
                )
            self.store._append_event(
                connection,
                task_id=task_id,
                aggregate_type="artifact_version",
                aggregate_id=artifact_id,
                event_type=(
                    "artifact_hash_failed"
                    if code != "secret_detected"
                    else "artifact_secret_rejected"
                ),
                payload={
                    "artifact_id": artifact_id,
                    "upload_id": upload_id,
                    "reason_code": code,
                },
                command_id=None,
            )

    def integrity_scan(self) -> dict[str, Any]:
        """Verify every V2 DB/blob reference and report, never delete, orphans."""

        with self.store._read() as connection:
            artifacts = connection.execute(
                """
                SELECT id, task_id, blob_uri, sha256, byte_size, mime_type
                FROM orch_artifact_versions WHERE blob_uri IS NOT NULL
                ORDER BY id
                """
            ).fetchall()
            chunks = connection.execute(
                """
                SELECT upload_id, sequence_no, blob_uri, chunk_hash, byte_size
                FROM orch_artifact_upload_chunks ORDER BY upload_id, sequence_no
                """
            ).fetchall()

        referenced: set[str] = set()
        failures: list[dict[str, Any]] = []
        verified = 0

        def verify(
            *, owner_type: str, owner_id: str, uri: str, expected: str, size: int
        ) -> bytes | None:
            nonlocal verified
            digest = str(uri).removeprefix("sha256:")
            referenced.add(digest)
            try:
                data = self.blobs.get(uri)
                normalized_expected = normalize_sha256(expected)
            except (OSError, BlobIntegrityError, TypeError, ValueError):
                failures.append(
                    {
                        "owner_type": owner_type,
                        "owner_id": owner_id,
                        "code": "missing_or_corrupt_blob",
                    }
                )
                return None
            if len(data) != int(size) or digest_bytes(data) != normalized_expected:
                failures.append(
                    {
                        "owner_type": owner_type,
                        "owner_id": owner_id,
                        "code": "reference_metadata_mismatch",
                    }
                )
                return None
            verified += 1
            return data

        for row in artifacts:
            data = verify(
                owner_type="artifact_version",
                owner_id=str(row["id"]),
                uri=str(row["blob_uri"]),
                expected=str(row["sha256"]),
                size=int(row["byte_size"]),
            )
            if data is None or row["mime_type"] != "application/json":
                continue
            try:
                value = json.loads(data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(value, Mapping):
                continue
            if value.get("schema_id") not in {
                "working_tree_overlay_v2",
                "directory_pack_v2",
            }:
                continue
            for path, nested in sorted(dict(value.get("entries") or {}).items()):
                if not isinstance(nested, Mapping) or not nested.get("blob_uri"):
                    continue
                verify(
                    owner_type="snapshot_entry",
                    owner_id=f"{row['id']}:{path}",
                    uri=str(nested["blob_uri"]),
                    expected=str(nested.get("sha256") or nested["blob_uri"]),
                    size=int(nested.get("byte_size") or 0),
                )
        for row in chunks:
            verify(
                owner_type="upload_chunk",
                owner_id=f"{row['upload_id']}:{row['sequence_no']}",
                uri=str(row["blob_uri"]),
                expected=str(row["chunk_hash"]),
                size=int(row["byte_size"]),
            )

        on_disk = {
            path.name
            for path in self.blobs.root.glob("*/*/*")
            if path.is_file()
            and len(path.name) == 64
            and all(character in "0123456789abcdef" for character in path.name)
        }
        return {
            "schema_version": 1,
            "status": "pass" if not failures else "fail",
            "artifact_references": len(artifacts),
            "chunk_references": len(chunks),
            "verified_references": verified,
            "failure_count": len(failures),
            "failures": failures,
            "orphan_blob_count": len(on_disk - referenced),
            "privacy": "content_free_metadata_only",
        }

    @staticmethod
    def _artifact(row: sqlite3.Row) -> ArtifactVersion:
        return ArtifactVersion(
            id=row["id"],
            logical_deliverable_id=row["logical_deliverable_id"],
            task_id=row["task_id"],
            run_id=row["run_id"],
            attempt=row["attempt"],
            version=row["version"],
            filename=row["filename"],
            mime_type=row["mime_type"],
            blob_uri=row["blob_uri"],
            sha256=row["sha256"],
            byte_size=row["byte_size"],
            section_index_artifact_id=row["section_index_artifact_id"],
            chunk_manifest_artifact_id=row["chunk_manifest_artifact_id"],
            status=row["status"],
            producer_profile_id=row["producer_profile_id"],
            parent_artifact_id=row["parent_artifact_id"],
            created_at=_time(row["created_at"]),
            finalized_at=_time(row["finalized_at"]),
        )

    @staticmethod
    def _receipt(row: sqlite3.Row) -> ArtifactReadReceipt:
        return ArtifactReadReceipt(
            id=row["id"],
            verifier_profile_id=row["verifier_profile_id"],
            run_id=row["run_id"],
            artifact_id=row["artifact_id"],
            artifact_hash=row["artifact_hash"],
            ranges=tuple(ByteRange(**item) for item in json.loads(row["ranges_json"])),
            covered_bytes=row["covered_bytes"],
            coverage_ratio=row["coverage_ratio"],
            candidate_bound_at=_time(row["candidate_bound_at"]),
            completed_at=_time(row["completed_at"]),
        )

    def create(
        self,
        task_id: str,
        *,
        logical_deliverable_id: str,
        filename: str,
        mime_type: str,
        run_id: str | None = None,
        attempt: int = 1,
        producer_profile_id: str | None = None,
        parent_artifact_id: str | None = None,
        internal: bool = False,
    ) -> dict[str, Any]:
        chosen_filename = safe_filename(filename)
        chosen_mime = safe_mime_type(mime_type)
        if attempt < 1:
            raise ValueError("artifact attempt must be positive")
        artifact_id = _id("artifact")
        upload_id = _id("upload")
        created_at = _stamp(_now())
        with self.store._write() as connection:
            task = connection.execute(
                "SELECT id, active_contract_id FROM orch_tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise NotFoundError(f"task {task_id} not found")
            if run_id is not None:
                run = connection.execute(
                    "SELECT task_id, attempt FROM orch_runs WHERE id = ?", (run_id,)
                ).fetchone()
                if run is None or run["task_id"] != task_id:
                    raise PermissionError("artifact run is outside the task namespace")
                if int(run["attempt"]) != int(attempt):
                    raise ConflictError("artifact attempt does not match the producer run")
            active_contract = task["active_contract_id"]
            if active_contract is not None and not internal:
                deliverable = connection.execute(
                    """
                    SELECT filename, mime_type FROM orch_contract_deliverables
                    WHERE id = ? AND contract_id = ?
                    """,
                    (logical_deliverable_id, active_contract),
                ).fetchone()
                if deliverable is None:
                    raise ConflictError("artifact is not declared by the active contract")
                if deliverable["filename"] != chosen_filename or deliverable["mime_type"] != chosen_mime:
                    raise ConflictError("artifact filename/MIME does not match its deliverable contract")
            parent_version = 0
            if parent_artifact_id is not None:
                parent = connection.execute(
                    """
                    SELECT task_id, logical_deliverable_id, version, status
                    FROM orch_artifact_versions WHERE id = ?
                    """,
                    (parent_artifact_id,),
                ).fetchone()
                if parent is None:
                    raise NotFoundError(f"artifact {parent_artifact_id} not found")
                if parent["task_id"] != task_id or parent["logical_deliverable_id"] != logical_deliverable_id:
                    raise PermissionError("repair parent belongs to another deliverable/task")
                if parent["status"] == "uploading":
                    raise ConflictError("an unfinished artifact cannot be a repair parent")
                parent_version = int(parent["version"])
            row = connection.execute(
                """
                SELECT COALESCE(MAX(version), 0) AS version
                FROM orch_artifact_versions WHERE logical_deliverable_id = ?
                """,
                (logical_deliverable_id,),
            ).fetchone()
            version = max(int(row["version"]), parent_version) + 1
            connection.execute(
                """
                INSERT INTO orch_artifact_versions(
                    id, logical_deliverable_id, task_id, run_id, attempt, version,
                    filename, mime_type, status, producer_profile_id,
                    parent_artifact_id, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'uploading', ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    logical_deliverable_id,
                    task_id,
                    run_id,
                    attempt,
                    version,
                    chosen_filename,
                    chosen_mime,
                    producer_profile_id,
                    parent_artifact_id,
                    _json({"internal": bool(internal)}),
                    created_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO orch_artifact_uploads(
                    id, artifact_id, task_id, run_id, max_bytes, status, created_at
                ) VALUES (?, ?, ?, ?, ?, 'open', ?)
                """,
                (upload_id, artifact_id, task_id, run_id, self.max_artifact_bytes, created_at),
            )
            if not internal:
                connection.execute(
                    "UPDATE orch_tasks SET artifact_status = 'uploading' WHERE id = ?",
                    (task_id,),
                )
        return {
            "upload_id": upload_id,
            "artifact_id": artifact_id,
            "version": version,
            "filename": chosen_filename,
            "mime_type": chosen_mime,
            "max_bytes": self.max_artifact_bytes,
            "max_chunk_bytes": self.max_chunk_bytes,
            "preview_policy": preview_policy(chosen_filename, chosen_mime),
        }

    def store_internal_json(
        self,
        *,
        task_id: str,
        logical_deliverable_id: str,
        filename: str,
        value: Any,
    ) -> ArtifactVersion:
        """Persist a deterministic service artifact and mark it verified."""

        content = _json(value).encode("utf-8")
        content_hash = digest_bytes(content)
        # Internal artifacts are content-addressed service products.  Replaying the
        # same inventory/query/compiler operation must reuse the immutable version
        # instead of manufacturing an indistinguishable successor.
        with self.store._read() as connection:
            existing = connection.execute(
                """
                SELECT * FROM orch_artifact_versions
                WHERE task_id = ? AND logical_deliverable_id = ? AND sha256 = ?
                  AND status <> 'uploading'
                ORDER BY version DESC LIMIT 1
                """,
                (task_id, logical_deliverable_id, content_hash),
            ).fetchone()
        if existing is not None:
            return self._artifact(existing)
        created = self.create(
            task_id,
            logical_deliverable_id=logical_deliverable_id,
            filename=filename,
            mime_type="application/json",
            internal=True,
        )
        self.append(
            created["upload_id"],
            sequence=0,
            content=content,
            chunk_hash=content_hash,
            caller_task_id=task_id,
        )
        try:
            artifact = self.complete(
                created["upload_id"],
                expected_sha256=content_hash,
                caller_task_id=task_id,
            )
        except sqlite3.IntegrityError:
            # A concurrent writer may have finalized the exact same service
            # product after our first lookup.  Remove only our still-uploading
            # staging rows and return the authoritative immutable winner.
            with self.store._write() as connection:
                connection.execute(
                    "DELETE FROM orch_artifact_upload_chunks WHERE upload_id = ?",
                    (created["upload_id"],),
                )
                connection.execute(
                    "DELETE FROM orch_artifact_uploads WHERE id = ?",
                    (created["upload_id"],),
                )
                connection.execute(
                    """
                    DELETE FROM orch_artifact_versions
                    WHERE id = ? AND task_id = ? AND status = 'uploading'
                    """,
                    (created["artifact_id"], task_id),
                )
                existing = connection.execute(
                    """
                    SELECT * FROM orch_artifact_versions
                    WHERE task_id = ? AND logical_deliverable_id = ? AND sha256 = ?
                      AND status <> 'uploading'
                    ORDER BY version DESC LIMIT 1
                    """,
                    (task_id, logical_deliverable_id, content_hash),
                ).fetchone()
            if existing is None:
                raise
            return self._artifact(existing)
        self.set_status(artifact.id, ArtifactVersionStatus.VALIDATING, update_task_projection=False)
        return self.set_status(
            artifact.id, ArtifactVersionStatus.VERIFIED, update_task_projection=False
        )

    def append(
        self,
        upload_id: str,
        *,
        sequence: int,
        content: bytes | str,
        chunk_hash: str,
        caller_task_id: str | None = None,
        caller_run_id: str | None = None,
    ) -> dict[str, Any]:
        if sequence < 0:
            raise ValueError("artifact chunk sequence cannot be negative")
        data = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        validate_size(len(data), maximum=self.max_chunk_bytes)
        expected_hash = normalize_sha256(chunk_hash)
        observed_hash = digest_bytes(data)
        if expected_hash != observed_hash:
            raise ArtifactSecurityError("artifact chunk hash mismatch")
        blob = self.blobs.put(data)
        with self.store._write() as connection:
            upload = connection.execute(
                "SELECT * FROM orch_artifact_uploads WHERE id = ?", (upload_id,)
            ).fetchone()
            if upload is None:
                raise NotFoundError(f"artifact upload {upload_id} not found")
            if caller_task_id is not None and upload["task_id"] != caller_task_id:
                raise PermissionError("artifact upload is outside the caller task namespace")
            if caller_run_id is not None and upload["run_id"] != caller_run_id:
                raise PermissionError("artifact upload is not owned by the caller run")
            existing = connection.execute(
                """
                SELECT chunk_hash, byte_size FROM orch_artifact_upload_chunks
                WHERE upload_id = ? AND sequence_no = ?
                """,
                (upload_id, sequence),
            ).fetchone()
            if existing is not None:
                if existing["chunk_hash"] != observed_hash or existing["byte_size"] != len(data):
                    raise ConflictError("artifact chunk sequence was replayed with different bytes")
                return {
                    "upload_id": upload_id,
                    "sequence": sequence,
                    "chunk_hash": observed_hash,
                    "byte_size": len(data),
                    "replayed": True,
                }
            if upload["status"] != "open":
                raise ConflictError("artifact upload is not open")
            if int(upload["expected_sequence"]) != sequence:
                raise ConflictError(
                    f"expected artifact chunk {upload['expected_sequence']}, received {sequence}"
                )
            total = int(upload["received_bytes"]) + len(data)
            validate_size(total, maximum=int(upload["max_bytes"]))
            connection.execute(
                """
                INSERT INTO orch_artifact_upload_chunks(
                    upload_id, sequence_no, chunk_hash, byte_size, blob_uri, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (upload_id, sequence, observed_hash, len(data), blob.uri, _stamp(_now())),
            )
            connection.execute(
                """
                UPDATE orch_artifact_uploads
                SET expected_sequence = expected_sequence + 1, received_bytes = ?
                WHERE id = ?
                """,
                (total, upload_id),
            )
        return {
            "upload_id": upload_id,
            "sequence": sequence,
            "chunk_hash": observed_hash,
            "byte_size": len(data),
            "received_bytes": total,
            "replayed": False,
        }

    def complete(
        self,
        upload_id: str,
        *,
        expected_sha256: str,
        caller_task_id: str | None = None,
        caller_run_id: str | None = None,
    ) -> ArtifactVersion:
        expected = normalize_sha256(expected_sha256)
        with self.store._read() as connection:
            upload = connection.execute(
                """
                SELECT u.*, a.mime_type, a.status AS artifact_status
                FROM orch_artifact_uploads u
                JOIN orch_artifact_versions a ON a.id = u.artifact_id
                WHERE u.id = ?
                """,
                (upload_id,),
            ).fetchone()
            if upload is None:
                raise NotFoundError(f"artifact upload {upload_id} not found")
            if caller_task_id is not None and upload["task_id"] != caller_task_id:
                raise PermissionError("artifact upload is outside the caller task namespace")
            if caller_run_id is not None and upload["run_id"] != caller_run_id:
                raise PermissionError("artifact upload is not owned by the caller run")
            if upload["status"] == "completed":
                artifact = self.get(upload["artifact_id"])
                if artifact.sha256 != expected:
                    raise ConflictError("completed upload was replayed with a different hash")
                return artifact
            if upload["status"] != "open" or upload["artifact_status"] != "uploading":
                raise ConflictError("artifact upload cannot be completed")
            chunks = connection.execute(
                """
                SELECT sequence_no, chunk_hash, byte_size, blob_uri
                FROM orch_artifact_upload_chunks
                WHERE upload_id = ? ORDER BY sequence_no
                """,
                (upload_id,),
            ).fetchall()
        if [row["sequence_no"] for row in chunks] != list(range(len(chunks))):
            raise ConflictError("artifact upload has a non-contiguous chunk sequence")
        parts: list[bytes] = []
        for row in chunks:
            try:
                part = self.blobs.get(row["blob_uri"])
            except (OSError, BlobIntegrityError, ValueError) as exc:
                self.record_integrity_failure(
                    task_id=upload["task_id"],
                    artifact_id=upload["artifact_id"],
                    upload_id=upload_id,
                    code="chunk_blob_integrity",
                )
                raise ArtifactSecurityError(
                    "stored artifact chunk failed integrity verification"
                ) from exc
            if len(part) != row["byte_size"] or digest_bytes(part) != row["chunk_hash"]:
                self.record_integrity_failure(
                    task_id=upload["task_id"],
                    artifact_id=upload["artifact_id"],
                    upload_id=upload_id,
                    code="chunk_hash_mismatch",
                )
                raise ArtifactSecurityError("stored artifact chunk failed integrity verification")
            parts.append(part)
        content = b"".join(parts)
        validate_size(len(content), maximum=self.max_artifact_bytes)
        observed = digest_bytes(content)
        if observed != expected:
            self.record_integrity_failure(
                task_id=upload["task_id"],
                artifact_id=upload["artifact_id"],
                upload_id=upload_id,
                code="final_hash_mismatch",
            )
            raise ArtifactSecurityError(
                f"artifact hash mismatch: expected {expected}, observed {observed}"
            )
        try:
            validate_text_secret_boundary(content, upload["mime_type"])
        except ArtifactSecurityError:
            self.record_integrity_failure(
                task_id=upload["task_id"],
                artifact_id=upload["artifact_id"],
                upload_id=upload_id,
                code="secret_detected",
            )
            raise
        final_blob = self.blobs.put(content, mime_type=upload["mime_type"])
        finalized_at = _stamp(_now())
        with self.store._write() as connection:
            current = connection.execute(
                """
                SELECT u.*, a.metadata_json
                FROM orch_artifact_uploads u
                JOIN orch_artifact_versions a ON a.id = u.artifact_id
                WHERE u.id = ?
                """,
                (upload_id,),
            ).fetchone()
            if current is None:
                raise NotFoundError(f"artifact upload {upload_id} not found")
            if current["status"] == "completed":
                artifact = connection.execute(
                    "SELECT * FROM orch_artifact_versions WHERE id = ?",
                    (current["artifact_id"],),
                ).fetchone()
                if artifact["sha256"] != expected:
                    raise ConflictError("completed upload hash changed")
                return self._artifact(artifact)
            if current["status"] != "open" or int(current["received_bytes"]) != len(content):
                raise ConflictError("artifact upload changed during finalization")
            connection.execute(
                """
                UPDATE orch_artifact_versions
                SET blob_uri = ?, sha256 = ?, byte_size = ?, status = 'draft', finalized_at = ?
                WHERE id = ? AND status = 'uploading'
                """,
                (final_blob.uri, observed, len(content), finalized_at, current["artifact_id"]),
            )
            connection.execute(
                """
                UPDATE orch_artifact_uploads SET status = 'completed', completed_at = ?
                WHERE id = ? AND status = 'open'
                """,
                (finalized_at, upload_id),
            )
            if not bool(json.loads(current["metadata_json"] or "{}").get("internal")):
                connection.execute(
                    "UPDATE orch_tasks SET artifact_status = 'draft' WHERE id = ?",
                    (current["task_id"],),
                )
            artifact = connection.execute(
                "SELECT * FROM orch_artifact_versions WHERE id = ?",
                (current["artifact_id"],),
            ).fetchone()
        return self._artifact(artifact)

    def get(self, artifact_id: str) -> ArtifactVersion:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_artifact_versions WHERE id = ?", (artifact_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"artifact {artifact_id} not found")
        return self._artifact(row)

    def list(
        self,
        task_id: str,
        *,
        logical_deliverable_id: str | None = None,
        statuses: Iterable[ArtifactVersionStatus | str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[ArtifactVersion, ...]:
        if limit < 1 or limit > 1_000 or offset < 0:
            raise ValueError("artifact pagination is outside its bounded range")
        clauses = ["task_id = ?"]
        params: list[Any] = [task_id]
        if logical_deliverable_id is not None:
            clauses.append("logical_deliverable_id = ?")
            params.append(logical_deliverable_id)
        selected_statuses = [ArtifactVersionStatus(item).value for item in statuses or ()]
        if selected_statuses:
            clauses.append("status IN (" + ",".join("?" for _ in selected_statuses) + ")")
            params.extend(selected_statuses)
        params.extend([limit, offset])
        with self.store._read() as connection:
            rows = connection.execute(
                "SELECT * FROM orch_artifact_versions WHERE "
                + " AND ".join(clauses)
                + " ORDER BY logical_deliverable_id, version DESC, id LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        return tuple(self._artifact(row) for row in rows)

    def bind_candidate(
        self,
        *,
        run_id: str,
        artifact_id: str,
        expected_sha256: str,
        verifier_profile_id: str,
        caller_task_id: str,
    ) -> ArtifactReadReceipt:
        expected = normalize_sha256(expected_sha256)
        receipt_id = _id("receipt")
        bound_at = _stamp(_now())
        with self.store._write() as connection:
            artifact = connection.execute(
                "SELECT * FROM orch_artifact_versions WHERE id = ?", (artifact_id,)
            ).fetchone()
            if artifact is None:
                raise NotFoundError(f"artifact {artifact_id} not found")
            authorize_artifact(
                owner_task_id=artifact["task_id"],
                caller_task_id=caller_task_id,
                artifact_id=artifact_id,
            )
            run = connection.execute(
                "SELECT task_id, plan_id FROM orch_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if run is None or run["task_id"] != caller_task_id:
                raise PermissionError("verifier run is outside the caller task namespace")
            self._assert_current_attempt_artifact(
                connection,
                artifact=artifact,
                reader_run_id=run_id,
                reader_plan_id=run["plan_id"],
            )
            if artifact["sha256"] != expected or artifact["status"] == "uploading":
                raise ArtifactSecurityError("candidate artifact/hash is invalid")
            existing = connection.execute(
                """
                SELECT * FROM orch_artifact_read_receipts
                WHERE run_id = ? AND artifact_id = ? AND artifact_hash = ?
                ORDER BY candidate_bound_at DESC LIMIT 1
                """,
                (run_id, artifact_id, expected),
            ).fetchone()
            if existing is not None and existing["completed_at"] is None:
                return self._receipt(existing)
            connection.execute(
                """
                INSERT INTO orch_artifact_read_receipts(
                    id, verifier_profile_id, run_id, artifact_id, artifact_hash,
                    ranges_json, covered_bytes, coverage_ratio, candidate_bound_at, created_at
                ) VALUES (?, ?, ?, ?, ?, '[]', 0, 0, ?, ?)
                """,
                (
                    receipt_id,
                    verifier_profile_id,
                    run_id,
                    artifact_id,
                    expected,
                    bound_at,
                    bound_at,
                ),
            )
            row = connection.execute(
                "SELECT * FROM orch_artifact_read_receipts WHERE id = ?", (receipt_id,)
            ).fetchone()
        return self._receipt(row)

    def read(
        self,
        artifact_id: str,
        *,
        expected_sha256: str,
        start_byte: int = 0,
        end_byte: int | None = None,
        caller_task_id: str,
        caller_run_id: str | None = None,
        receipt_id: str | None = None,
        allowed_artifact_ids: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        expected = normalize_sha256(expected_sha256)
        artifact = self.get(artifact_id)
        authorize_artifact(
            owner_task_id=artifact.task_id,
            caller_task_id=caller_task_id,
            artifact_id=artifact_id,
            allowed_artifact_ids=allowed_artifact_ids,
        )
        if artifact.status in {ArtifactVersionStatus.UPLOADING, ArtifactVersionStatus.REJECTED}:
            raise ConflictError("artifact content is unavailable in its current state")
        if artifact.sha256 != expected or artifact.blob_uri is None or artifact.byte_size is None:
            raise ArtifactSecurityError("artifact hash/metadata mismatch")
        if caller_run_id is not None and artifact.run_id is not None:
            with self.store._read() as connection:
                run = connection.execute(
                    "SELECT task_id, plan_id FROM orch_runs WHERE id = ?", (caller_run_id,)
                ).fetchone()
                artifact_row = connection.execute(
                    "SELECT * FROM orch_artifact_versions WHERE id = ?", (artifact_id,)
                ).fetchone()
                if run is None or run["task_id"] != caller_task_id:
                    raise PermissionError("artifact reader run is outside the task namespace")
                self._assert_current_attempt_artifact(
                    connection,
                    artifact=artifact_row,
                    reader_run_id=caller_run_id,
                    reader_plan_id=run["plan_id"],
                )
        size = artifact.byte_size
        start = int(start_byte)
        end = size if end_byte is None else int(end_byte)
        if start < 0 or end <= start or end > size:
            raise ArtifactSecurityError(
                f"invalid artifact range [{start},{end}) for {size} bytes"
            )
        try:
            data = self.blobs.get(artifact.blob_uri)
        except (OSError, BlobIntegrityError, ValueError) as exc:
            self.record_integrity_failure(
                task_id=artifact.task_id,
                artifact_id=artifact.id,
                code="final_blob_integrity",
            )
            raise ArtifactSecurityError(
                "artifact blob failed immutable integrity verification"
            ) from exc
        if len(data) != size or digest_bytes(data) != expected:
            self.record_integrity_failure(
                task_id=artifact.task_id,
                artifact_id=artifact.id,
                code="final_blob_hash_mismatch",
            )
            raise ArtifactSecurityError("artifact blob failed immutable integrity verification")
        delivered = data[start:end]
        receipt: ArtifactReadReceipt | None = None
        if receipt_id is not None:
            if caller_run_id is None:
                raise PermissionError("a read receipt requires a run-bound reader")
            receipt = self._record_read(
                receipt_id=receipt_id,
                run_id=caller_run_id,
                artifact=artifact,
                start=start,
                end=end,
                delivered=delivered,
            )
        return {
            "artifact_id": artifact_id,
            "sha256": expected,
            "etag": expected,
            "start_byte": start,
            "end_byte": end,
            "total_bytes": size,
            "complete": start == 0 and end == size,
            "content": delivered,
            "receipt": receipt.model_dump(mode="json") if receipt is not None else None,
        }

    @staticmethod
    def _assert_current_attempt_artifact(
        connection: sqlite3.Connection,
        *,
        artifact: sqlite3.Row,
        reader_run_id: str,
        reader_plan_id: str,
    ) -> None:
        """Reject stale producer attempts without comparing unrelated node attempts."""

        producer_run_id = artifact["run_id"]
        if producer_run_id is None or producer_run_id == reader_run_id:
            return
        producer = connection.execute(
            """
            SELECT task_id, plan_id, node_id, status
            FROM orch_runs WHERE id=?
            """,
            (producer_run_id,),
        ).fetchone()
        if (
            producer is None
            or producer["task_id"] != artifact["task_id"]
            or producer["plan_id"] != reader_plan_id
        ):
            raise ConflictError("artifact producer is not a run in this plan")
        latest = connection.execute(
            """
            SELECT id FROM orch_runs
            WHERE task_id=? AND plan_id=? AND node_id=?
            ORDER BY attempt DESC, created_at DESC, id DESC LIMIT 1
            """,
            (producer["task_id"], producer["plan_id"], producer["node_id"]),
        ).fetchone()
        if latest is None or latest["id"] != producer_run_id:
            raise ConflictError("old-attempt artifact replay is forbidden")

    def _record_read(
        self,
        *,
        receipt_id: str,
        run_id: str,
        artifact: ArtifactVersion,
        start: int,
        end: int,
        delivered: bytes,
    ) -> ArtifactReadReceipt:
        delivered_hash = digest_bytes(delivered)
        now = _stamp(_now())
        with self.store._write() as connection:
            receipt = connection.execute(
                "SELECT * FROM orch_artifact_read_receipts WHERE id = ?", (receipt_id,)
            ).fetchone()
            if receipt is None:
                raise NotFoundError(f"artifact read receipt {receipt_id} not found")
            if (
                receipt["run_id"] != run_id
                or receipt["artifact_id"] != artifact.id
                or receipt["artifact_hash"] != artifact.sha256
            ):
                raise PermissionError("read receipt is not bound to this run/candidate")
            if receipt["completed_at"] is not None:
                return self._receipt(receipt)
            event = connection.execute(
                """
                SELECT delivered_hash FROM orch_artifact_read_events
                WHERE receipt_id = ? AND start_byte = ? AND end_byte = ?
                """,
                (receipt_id, start, end),
            ).fetchone()
            if event is not None and event["delivered_hash"] != delivered_hash:
                raise ArtifactSecurityError("read range was replayed with different bytes")
            if event is None:
                connection.execute(
                    """
                    INSERT INTO orch_artifact_read_events(
                        id, receipt_id, start_byte, end_byte, delivered_hash, delivered_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (_id("read"), receipt_id, start, end, delivered_hash, now),
                )
            events = connection.execute(
                """
                SELECT start_byte, end_byte FROM orch_artifact_read_events
                WHERE receipt_id = ? ORDER BY start_byte, end_byte
                """,
                (receipt_id,),
            ).fetchall()
            merged = _ranges_union((row["start_byte"], row["end_byte"]) for row in events)
            covered = sum(range_end - range_start for range_start, range_end in merged)
            size = int(artifact.byte_size or 0)
            ratio = 1.0 if size == 0 else covered / size
            complete = (size == 0) or merged == [(0, size)]
            completed_at = None
            if complete:
                completed_at = now
                # Freshness uses a strict ordering guard.  Extremely fast local
                # reads can otherwise share the same microsecond as candidate
                # binding and become nondeterministically non-fresh.
                if completed_at <= receipt["candidate_bound_at"]:
                    completed_at = _stamp(
                        _time(receipt["candidate_bound_at"])
                        + timedelta(microseconds=1)
                    )
            ranges_json = _json(
                [
                    {"start_byte": range_start, "end_byte": range_end}
                    for range_start, range_end in merged
                ]
            )
            connection.execute(
                """
                UPDATE orch_artifact_read_receipts
                SET ranges_json = ?, covered_bytes = ?, coverage_ratio = ?, completed_at = ?
                WHERE id = ? AND completed_at IS NULL
                """,
                (ranges_json, covered, ratio, completed_at, receipt_id),
            )
            updated = connection.execute(
                "SELECT * FROM orch_artifact_read_receipts WHERE id = ?", (receipt_id,)
            ).fetchone()
        return self._receipt(updated)

    def fresh_complete_receipt(
        self,
        *,
        run_id: str,
        artifact_id: str,
        expected_sha256: str,
    ) -> ArtifactReadReceipt | None:
        expected = normalize_sha256(expected_sha256)
        with self.store._read() as connection:
            row = connection.execute(
                """
                SELECT r.* FROM orch_artifact_read_receipts r
                JOIN orch_artifact_versions a ON a.id = r.artifact_id
                WHERE r.run_id = ? AND r.artifact_id = ? AND r.artifact_hash = ?
                  AND r.completed_at IS NOT NULL
                  AND r.coverage_ratio = 1.0
                  AND r.covered_bytes = a.byte_size
                  AND r.completed_at > r.candidate_bound_at
                ORDER BY r.completed_at DESC LIMIT 1
                """,
                (run_id, artifact_id, expected),
            ).fetchone()
        return self._receipt(row) if row is not None else None

    def get_receipt(self, receipt_id: str) -> ArtifactReadReceipt:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_artifact_read_receipts WHERE id = ?",
                (receipt_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"artifact read receipt {receipt_id} not found")
        return self._receipt(row)

    def set_status(
        self,
        artifact_id: str,
        status: ArtifactVersionStatus | str,
        *,
        update_task_projection: bool = True,
    ) -> ArtifactVersion:
        target = ArtifactVersionStatus(status)
        allowed = {
            ArtifactVersionStatus.DRAFT: {
                ArtifactVersionStatus.VALIDATING,
                ArtifactVersionStatus.REJECTED,
                ArtifactVersionStatus.SUPERSEDED,
            },
            ArtifactVersionStatus.VALIDATING: {
                ArtifactVersionStatus.VERIFIED,
                ArtifactVersionStatus.REJECTED,
                ArtifactVersionStatus.SUPERSEDED,
            },
            ArtifactVersionStatus.VERIFIED: {ArtifactVersionStatus.SUPERSEDED},
            ArtifactVersionStatus.REJECTED: {ArtifactVersionStatus.SUPERSEDED},
        }
        with self.store._write() as connection:
            row = connection.execute(
                "SELECT * FROM orch_artifact_versions WHERE id = ?", (artifact_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"artifact {artifact_id} not found")
            current = ArtifactVersionStatus(row["status"])
            if target is current:
                return self._artifact(row)
            if target not in allowed.get(current, set()):
                raise ConflictError(f"artifact status cannot transition {current} -> {target}")
            connection.execute(
                "UPDATE orch_artifact_versions SET status = ? WHERE id = ?",
                (target.value, artifact_id),
            )
            if update_task_projection and not bool(
                json.loads(row["metadata_json"] or "{}").get("internal")
            ):
                connection.execute(
                    "UPDATE orch_tasks SET artifact_status = ? WHERE id = ?",
                    (target.value, row["task_id"]),
                )
            updated = connection.execute(
                "SELECT * FROM orch_artifact_versions WHERE id = ?", (artifact_id,)
            ).fetchone()
        return self._artifact(updated)

    def publish_primary(
        self,
        artifact_id: str,
        *,
        _connection: sqlite3.Connection | None = None,
    ) -> ArtifactVersion:
        """Set a verified primary, optionally inside the caller's atomic publish."""

        with (
            self.store._write()
            if _connection is None
            else nullcontext(_connection)
        ) as connection:
            row = connection.execute(
                "SELECT * FROM orch_artifact_versions WHERE id = ?", (artifact_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"artifact {artifact_id} not found")
            if row["status"] != ArtifactVersionStatus.VERIFIED.value:
                raise ConflictError("only a verified artifact can be primary")
            connection.execute(
                """
                UPDATE orch_tasks
                SET primary_artifact_id = ?, artifact_status = 'verified'
                WHERE id = ?
                """,
                (artifact_id, row["task_id"]),
            )
        return self._artifact(row)

    def diff(self, artifact_id: str, *, base_artifact_id: str) -> str:
        current = self.get(artifact_id)
        base = self.get(base_artifact_id)
        if current.task_id != base.task_id or current.logical_deliverable_id != base.logical_deliverable_id:
            raise PermissionError("artifact diff requires versions of one task deliverable")
        if current.blob_uri is None or base.blob_uri is None:
            raise ConflictError("unfinished artifacts cannot be diffed")
        try:
            current_text = self.blobs.get(current.blob_uri).decode("utf-8").splitlines(True)
            base_text = self.blobs.get(base.blob_uri).decode("utf-8").splitlines(True)
        except UnicodeDecodeError as exc:
            raise ValueError("artifact diff currently supports UTF-8 text") from exc
        return "".join(
            difflib.unified_diff(
                base_text,
                current_text,
                fromfile=f"{base.filename}@v{base.version}",
                tofile=f"{current.filename}@v{current.version}",
            )
        )
