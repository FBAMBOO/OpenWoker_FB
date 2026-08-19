"""Immutable commit, working-tree overlay and non-Git directory snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import time
import unicodedata
import uuid
from pathlib import Path
from typing import Any, Iterable

from ..errors import ConflictError, NotFoundError
from ..store import OrchestrationStore
from .artifact_security import digest_bytes
from .artifacts import ArtifactService
from .models import (
    RepositorySnapshot,
    SnapshotKind,
    VcsObjectFormat,
    VcsType,
    model_content_sha256,
    validate_repo_relative_path,
)
from .repository_resolver import (
    RepositoryCandidate,
    TargetResolution,
    git_command,
)


_DIRECTORY_IGNORES = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        ".venv",
        "venv",
        "node_modules",
        "target",
        "dist",
        "build",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
    }
)


class SnapshotError(ValueError):
    pass


def _stamp(value) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _git_object_hash(data: bytes, object_format: VcsObjectFormat) -> str:
    envelope = f"blob {len(data)}\0".encode("ascii") + data
    return (
        hashlib.sha256(envelope).hexdigest()
        if object_format is VcsObjectFormat.SHA256
        else hashlib.sha1(envelope).hexdigest()
    )


def _stable_read(path: Path, *, attempts: int = 3) -> bytes:
    for attempt in range(attempts):
        before = path.lstat()
        if stat.S_ISLNK(before.st_mode):
            target = os.readlink(path)
            data = target.encode("utf-8", "surrogateescape")
        elif stat.S_ISREG(before.st_mode):
            data = path.read_bytes()
        else:
            raise SnapshotError(f"unsupported snapshot file type: {path}")
        after = path.lstat()
        if (
            before.st_size == after.st_size
            and before.st_mtime_ns == after.st_mtime_ns
            and before.st_mode == after.st_mode
        ):
            return data
        if attempt + 1 < attempts:
            time.sleep(0.01 * (attempt + 1))
    raise SnapshotError(f"file changed while snapshot was freezing: {path}")


def _safe_live_path(root: Path, relative: str) -> Path:
    normalized = validate_repo_relative_path(relative)
    candidate = root.joinpath(*normalized.split("/"))
    try:
        resolved_parent = candidate.parent.resolve(strict=True)
    except OSError as exc:
        raise SnapshotError(f"snapshot path parent is unreadable: {relative}") from exc
    canonical_root = root.resolve(strict=True)
    try:
        resolved_parent.relative_to(canonical_root)
    except ValueError as exc:
        raise SnapshotError(f"snapshot path escapes its root: {relative}") from exc
    if candidate.is_symlink():
        try:
            target = candidate.resolve(strict=True)
            target.relative_to(canonical_root)
        except (OSError, ValueError) as exc:
            raise SnapshotError(f"snapshot symlink escapes its root: {relative}") from exc
    return candidate


def _z_paths(raw: bytes) -> tuple[str, ...]:
    output: list[str] = []
    for item in raw.split(b"\x00"):
        if not item:
            continue
        output.append(
            validate_repo_relative_path(
                unicodedata.normalize("NFC", item.decode("utf-8", "surrogateescape"))
            )
        )
    return tuple(output)


class RepositorySnapshotService:
    def __init__(
        self,
        store: OrchestrationStore,
        artifacts: ArtifactService,
        *,
        max_directory_files: int = 100_000,
        max_directory_bytes: int = 2 * 1024 * 1024 * 1024,
    ) -> None:
        self.store = store
        self.artifacts = artifacts
        self.max_directory_files = int(max_directory_files)
        self.max_directory_bytes = int(max_directory_bytes)

    def _store_json_artifact(
        self,
        *,
        task_id: str,
        logical_id: str,
        filename: str,
        value: Any,
    ) -> tuple[str, str]:
        content = _json(value)
        created = self.artifacts.create(
            task_id,
            logical_deliverable_id=logical_id,
            filename=filename,
            mime_type="application/json",
            internal=True,
        )
        self.artifacts.append(
            created["upload_id"],
            sequence=0,
            content=content,
            chunk_hash=digest_bytes(content),
            caller_task_id=task_id,
        )
        artifact = self.artifacts.complete(
            created["upload_id"],
            expected_sha256=digest_bytes(content),
            caller_task_id=task_id,
        )
        artifact = self.artifacts.set_status(
            artifact.id, "validating", update_task_projection=False
        )
        artifact = self.artifacts.set_status(
            artifact.id, "verified", update_task_projection=False
        )
        return artifact.id, str(artifact.sha256)

    @staticmethod
    def _git_manifest(root: Path, oid: str) -> list[dict[str, Any]]:
        result = git_command(
            root,
            "ls-tree",
            "-r",
            "-z",
            "-l",
            "--full-tree",
            oid,
            text=False,
        )
        entries: list[dict[str, Any]] = []
        for raw in result.stdout.split(b"\x00"):
            if not raw:
                continue
            try:
                metadata, raw_path = raw.split(b"\t", 1)
                mode, kind, object_id, size = metadata.split(maxsplit=3)
            except ValueError as exc:
                raise SnapshotError("git ls-tree returned an invalid manifest entry") from exc
            path = validate_repo_relative_path(
                unicodedata.normalize("NFC", raw_path.decode("utf-8", "surrogateescape"))
            )
            entries.append(
                {
                    "path": path,
                    "mode": mode.decode("ascii"),
                    "type": kind.decode("ascii"),
                    "git_oid": object_id.decode("ascii"),
                    "size": None if size == b"-" else int(size),
                }
            )
        return entries

    def _working_overlay(
        self, root: Path, *, task_id: str, snapshot_id: str
    ) -> tuple[dict[str, Any], str, str]:
        tracked = git_command(root, "diff", "--name-only", "-z", "HEAD", text=False)
        untracked = git_command(
            root, "ls-files", "--others", "--exclude-standard", "-z", text=False
        )
        paths = tuple(dict.fromkeys((*_z_paths(tracked.stdout), *_z_paths(untracked.stdout))))
        entries: dict[str, dict[str, Any]] = {}
        deleted: list[str] = []
        for relative in paths:
            path = root.joinpath(*relative.split("/"))
            if not path.exists() and not path.is_symlink():
                deleted.append(relative)
                continue
            safe = _safe_live_path(root, relative)
            data = _stable_read(safe)
            blob = self.artifacts.blobs.put(data)
            entries[relative] = {
                "blob_uri": blob.uri,
                "sha256": f"sha256:{blob.sha256}",
                "byte_size": len(data),
                "mode": safe.lstat().st_mode,
                "symlink": safe.is_symlink(),
            }
        pack = {
            "schema_id": "working_tree_overlay_v2",
            "schema_version": 2,
            "entries": entries,
            "deleted": sorted(deleted),
        }
        artifact_id, artifact_hash = self._store_json_artifact(
            task_id=task_id,
            logical_id=f"system-overlay-{snapshot_id}",
            filename=f"snapshot_overlay_{snapshot_id}.json",
            value=pack,
        )
        return pack, artifact_id, artifact_hash

    def _directory_pack(
        self, root: Path, *, task_id: str, snapshot_id: str
    ) -> tuple[dict[str, Any], str, str, list[dict[str, Any]]]:
        entries: dict[str, dict[str, Any]] = {}
        manifest: list[dict[str, Any]] = []
        total = 0
        for current, directories, filenames in os.walk(root, followlinks=False):
            base = Path(current)
            directories[:] = [
                name
                for name in directories
                if name not in _DIRECTORY_IGNORES and not (base / name).is_symlink()
            ]
            for filename in filenames:
                path = base / filename
                relative = validate_repo_relative_path(path.relative_to(root).as_posix())
                safe = _safe_live_path(root, relative)
                data = _stable_read(safe)
                total += len(data)
                if len(entries) + 1 > self.max_directory_files or total > self.max_directory_bytes:
                    raise SnapshotError("directory snapshot exceeds admission limits")
                blob = self.artifacts.blobs.put(data)
                record = {
                    "blob_uri": blob.uri,
                    "sha256": f"sha256:{blob.sha256}",
                    "byte_size": len(data),
                    "mode": safe.lstat().st_mode,
                    "symlink": safe.is_symlink(),
                }
                entries[relative] = record
                manifest.append(
                    {
                        "path": relative,
                        "mode": record["mode"],
                        "type": "symlink" if record["symlink"] else "blob",
                        "sha256": record["sha256"],
                        "size": len(data),
                    }
                )
        pack = {
            "schema_id": "directory_pack_v2",
            "schema_version": 2,
            "entries": entries,
            "ignored_directories": sorted(_DIRECTORY_IGNORES),
        }
        artifact_id, artifact_hash = self._store_json_artifact(
            task_id=task_id,
            logical_id=f"system-directory-pack-{snapshot_id}",
            filename=f"directory_pack_{snapshot_id}.json",
            value=pack,
        )
        return pack, artifact_id, artifact_hash, manifest

    def freeze(
        self,
        *,
        task_id: str,
        resolution: TargetResolution,
        candidate_id: str | None = None,
        selected_ref: str | None = None,
        snapshot_kind: SnapshotKind | str | None = None,
    ) -> RepositorySnapshot:
        chosen_id = candidate_id or resolution.recommended_candidate_id
        candidate = next(
            (item for item in resolution.candidates if item.id == chosen_id), None
        )
        if candidate is None:
            raise ConflictError("target resolution requires an explicit valid candidate")
        if resolution.status != "resolved" and candidate_id is None:
            raise ConflictError("ambiguous target cannot be frozen without user selection")
        with self.store._read() as connection:
            if connection.execute("SELECT 1 FROM orch_tasks WHERE id = ?", (task_id,)).fetchone() is None:
                raise NotFoundError(f"task {task_id} not found")
            version = int(
                connection.execute(
                    "SELECT COALESCE(MAX(version), 0) FROM orch_repository_snapshots WHERE task_id = ?",
                    (task_id,),
                ).fetchone()[0]
            ) + 1
        snapshot_id = f"snapshot_{uuid.uuid4().hex}"
        kind = SnapshotKind(snapshot_kind or candidate.recommended_snapshot_kind)
        root = Path(candidate.repo_root).resolve()
        manifest: list[dict[str, Any]]
        commit_oid = base_tree_oid = None
        overlay_artifact_id = overlay_hash = None
        directory_artifact_id = directory_hash = None
        selected = selected_ref or candidate.recommended_ref
        if candidate.vcs_type is VcsType.GIT:
            if kind is SnapshotKind.DIRECTORY:
                raise SnapshotError("Git targets cannot be frozen as a non-Git directory snapshot")
            if not selected:
                raise ConflictError("Git target has no selected ref")
            resolved = git_command(root, "rev-parse", "--verify", f"{selected}^{{commit}}")
            commit_oid = resolved.stdout.strip()
            base_tree_oid = git_command(root, "rev-parse", f"{commit_oid}^{{tree}}").stdout.strip()
            manifest = self._git_manifest(root, commit_oid)
            if kind is SnapshotKind.WORKING_TREE:
                if selected != "HEAD":
                    raise ConflictError("working-tree overlay must use HEAD as its base")
                pack, overlay_artifact_id, overlay_hash = self._working_overlay(
                    root, task_id=task_id, snapshot_id=snapshot_id
                )
                by_path = {entry["path"]: entry for entry in manifest}
                for path in pack["deleted"]:
                    by_path.pop(path, None)
                for path, record in pack["entries"].items():
                    by_path[path] = {
                        "path": path,
                        "mode": record["mode"],
                        "type": "overlay",
                        "sha256": record["sha256"],
                        "size": record["byte_size"],
                    }
                manifest = sorted(by_path.values(), key=lambda item: item["path"])
        else:
            if kind is not SnapshotKind.DIRECTORY:
                raise SnapshotError("non-Git targets require a directory snapshot")
            _, directory_artifact_id, directory_hash, manifest = self._directory_pack(
                root, task_id=task_id, snapshot_id=snapshot_id
            )

        manifest_value = {
            "schema_id": "repository_manifest_v2",
            "schema_version": 2,
            "snapshot_id": snapshot_id,
            "snapshot_kind": kind.value,
            "entries": manifest,
        }
        manifest_artifact_id, manifest_hash = self._store_json_artifact(
            task_id=task_id,
            logical_id=f"system-manifest-{snapshot_id}",
            filename=f"snapshot_manifest_{snapshot_id}.json",
            value=manifest_value,
        )
        from datetime import datetime, timezone

        created_at = datetime.now(timezone.utc)
        ignore_hash = digest_bytes(_json(sorted(_DIRECTORY_IGNORES)))
        draft = RepositorySnapshot(
            id=snapshot_id,
            task_id=task_id,
            version=version,
            workspace_root=candidate.workspace_root,
            repo_root=str(root),
            project_root=candidate.project_root,
            vcs_type=candidate.vcs_type,
            snapshot_kind=kind,
            selected_ref=selected,
            vcs_object_format=candidate.vcs_object_format,
            commit_oid=commit_oid,
            base_tree_oid=base_tree_oid,
            head_oid=candidate.head_oid,
            current_branch=candidate.current_branch,
            default_ref=candidate.default_ref,
            upstream_ref=candidate.upstream_ref,
            ahead=candidate.ahead,
            behind=candidate.behind,
            dirty=candidate.dirty,
            worktree_count=candidate.worktree_count,
            duplicate_roots=candidate.duplicate_roots,
            ignore_rules_hash=ignore_hash,
            manifest_artifact_id=manifest_artifact_id,
            manifest_hash=manifest_hash,
            overlay_artifact_id=overlay_artifact_id,
            overlay_hash=overlay_hash,
            directory_pack_artifact_id=directory_artifact_id,
            directory_pack_hash=directory_hash,
            resolution_confidence=resolution.resolution_confidence,
            resolution_reason=resolution.resolution_reason,
            content_hash="sha256:" + "0" * 64,
            created_at=created_at,
        )
        snapshot = draft.model_copy(update={"content_hash": model_content_sha256(draft)})
        values = snapshot.model_dump(mode="json")
        with self.store._write() as connection:
            connection.execute(
                """
                INSERT INTO orch_repository_snapshots(
                    id, task_id, version, status, workspace_root, repo_root, project_root,
                    vcs_type, snapshot_kind, selected_ref, vcs_object_format, commit_oid,
                    base_tree_oid, head_oid, current_branch, default_ref, upstream_ref,
                    ahead, behind, dirty, worktree_count, duplicate_roots_json,
                    ignore_rules_hash, manifest_artifact_id, manifest_hash,
                    overlay_artifact_id, overlay_hash, directory_pack_artifact_id,
                    directory_pack_hash, resolution_confidence, resolution_reason,
                    content_hash, created_at, published_at
                ) VALUES (
                    ?, ?, ?, 'published', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    snapshot.id,
                    snapshot.task_id,
                    snapshot.version,
                    snapshot.workspace_root,
                    snapshot.repo_root,
                    snapshot.project_root,
                    snapshot.vcs_type.value,
                    snapshot.snapshot_kind.value,
                    snapshot.selected_ref,
                    snapshot.vcs_object_format.value if snapshot.vcs_object_format else None,
                    snapshot.commit_oid,
                    snapshot.base_tree_oid,
                    snapshot.head_oid,
                    snapshot.current_branch,
                    snapshot.default_ref,
                    snapshot.upstream_ref,
                    snapshot.ahead,
                    snapshot.behind,
                    int(snapshot.dirty),
                    snapshot.worktree_count,
                    json.dumps(list(snapshot.duplicate_roots)),
                    snapshot.ignore_rules_hash,
                    snapshot.manifest_artifact_id,
                    snapshot.manifest_hash,
                    snapshot.overlay_artifact_id,
                    snapshot.overlay_hash,
                    snapshot.directory_pack_artifact_id,
                    snapshot.directory_pack_hash,
                    snapshot.resolution_confidence,
                    snapshot.resolution_reason,
                    snapshot.content_hash,
                    _stamp(snapshot.created_at),
                    _stamp(snapshot.created_at),
                ),
            )
            connection.execute(
                "UPDATE orch_tasks SET active_snapshot_id = ? WHERE id = ?",
                (snapshot.id, task_id),
            )
        return snapshot

    def get(self, snapshot_id: str) -> RepositorySnapshot:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_repository_snapshots WHERE id = ?", (snapshot_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"repository snapshot {snapshot_id} not found")
        return RepositorySnapshot(
            id=row["id"],
            task_id=row["task_id"],
            version=row["version"],
            workspace_root=row["workspace_root"],
            repo_root=row["repo_root"],
            project_root=row["project_root"],
            vcs_type=row["vcs_type"],
            snapshot_kind=row["snapshot_kind"],
            selected_ref=row["selected_ref"],
            vcs_object_format=row["vcs_object_format"],
            commit_oid=row["commit_oid"],
            base_tree_oid=row["base_tree_oid"],
            head_oid=row["head_oid"],
            current_branch=row["current_branch"],
            default_ref=row["default_ref"],
            upstream_ref=row["upstream_ref"],
            ahead=row["ahead"],
            behind=row["behind"],
            dirty=bool(row["dirty"]),
            worktree_count=row["worktree_count"],
            duplicate_roots=tuple(json.loads(row["duplicate_roots_json"])),
            ignore_rules_hash=row["ignore_rules_hash"],
            manifest_artifact_id=row["manifest_artifact_id"],
            manifest_hash=row["manifest_hash"],
            overlay_artifact_id=row["overlay_artifact_id"],
            overlay_hash=row["overlay_hash"],
            directory_pack_artifact_id=row["directory_pack_artifact_id"],
            directory_pack_hash=row["directory_pack_hash"],
            resolution_confidence=row["resolution_confidence"],
            resolution_reason=row["resolution_reason"],
            content_hash=row["content_hash"],
            created_at=row["created_at"],
        )

    def _artifact_json(self, artifact_id: str, expected_hash: str) -> dict[str, Any]:
        artifact = self.artifacts.get(artifact_id)
        if artifact.sha256 != expected_hash or artifact.blob_uri is None:
            raise SnapshotError("snapshot artifact hash mismatch")
        data = self.artifacts.blobs.get(artifact.blob_uri)
        if digest_bytes(data) != expected_hash:
            raise SnapshotError("snapshot artifact content failed integrity verification")
        return json.loads(data.decode("utf-8"))

    def manifest(self, snapshot_id: str) -> dict[str, Any]:
        snapshot = self.get(snapshot_id)
        return self._artifact_json(snapshot.manifest_artifact_id, snapshot.manifest_hash)

    def read_file(self, snapshot_id: str, path: str) -> bytes:
        snapshot = self.get(snapshot_id)
        relative = validate_repo_relative_path(path)
        manifest = self.manifest(snapshot_id)
        entry = next(
            (item for item in manifest.get("entries", ()) if item.get("path") == relative),
            None,
        )
        if entry is None:
            raise NotFoundError(f"snapshot path not found: {relative}")
        if snapshot.snapshot_kind is SnapshotKind.DIRECTORY:
            pack = self._artifact_json(
                str(snapshot.directory_pack_artifact_id), str(snapshot.directory_pack_hash)
            )
            record = pack["entries"].get(relative)
            if record is None:
                raise SnapshotError("directory pack and manifest disagree")
            data = self.artifacts.blobs.get(record["blob_uri"])
            if digest_bytes(data) != record["sha256"]:
                raise SnapshotError("directory snapshot blob hash mismatch")
            return data
        if snapshot.snapshot_kind is SnapshotKind.WORKING_TREE and snapshot.overlay_artifact_id:
            pack = self._artifact_json(snapshot.overlay_artifact_id, str(snapshot.overlay_hash))
            record = pack["entries"].get(relative)
            if record is not None:
                data = self.artifacts.blobs.get(record["blob_uri"])
                if digest_bytes(data) != record["sha256"]:
                    raise SnapshotError("working-tree overlay blob hash mismatch")
                return data
            if relative in pack.get("deleted", ()):
                raise NotFoundError(f"snapshot path was deleted: {relative}")
        if not snapshot.commit_oid or snapshot.vcs_object_format is None:
            raise SnapshotError("Git snapshot has no immutable commit object")
        result = git_command(
            snapshot.repo_root,
            "cat-file",
            "blob",
            f"{snapshot.commit_oid}:{relative}",
            text=False,
        )
        data = bytes(result.stdout)
        expected_oid = entry.get("git_oid")
        if expected_oid and _git_object_hash(data, snapshot.vcs_object_format) != expected_oid:
            raise SnapshotError("frozen Git blob does not match the manifest object id")
        return data

    def read_file_lines(
        self,
        snapshot_id: str,
        path: str,
        *,
        start_line: int = 1,
        end_line: int | None = None,
    ) -> dict[str, Any]:
        if start_line < 1 or (end_line is not None and end_line < start_line):
            raise ValueError("snapshot line range is invalid")
        data = self.read_file(snapshot_id, path)
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SnapshotError("snapshot line reads require UTF-8 text") from exc
        lines = text.splitlines(keepends=True)
        chosen_end = len(lines) if end_line is None else min(end_line, len(lines))
        selected = "".join(lines[start_line - 1 : chosen_end])
        return {
            "snapshot_id": snapshot_id,
            "path": validate_repo_relative_path(path),
            "line_start": start_line,
            "line_end": chosen_end,
            "content": selected,
            "blob_hash": digest_bytes(data),
            "excerpt_hash": digest_bytes(selected.encode("utf-8")),
            "complete": end_line is None or chosen_end >= end_line,
        }

    def search(
        self,
        snapshot_id: str,
        query: str,
        *,
        paths: Iterable[str] = (),
        regex: bool = False,
        limit: int = 1_000,
    ) -> dict[str, Any]:
        if limit < 1 or limit > 10_000:
            raise ValueError("snapshot search limit is outside the bounded range")
        manifest = self.manifest(snapshot_id)
        prefixes = tuple(validate_repo_relative_path(path) for path in paths)
        pattern = re.compile(query) if regex else None
        results: list[dict[str, Any]] = []
        scanned = 0
        for entry in manifest.get("entries", ()):
            path = str(entry.get("path") or "")
            if prefixes and not any(path == prefix or path.startswith(prefix + "/") for prefix in prefixes):
                continue
            try:
                text = self.read_file(snapshot_id, path).decode("utf-8")
            except (UnicodeDecodeError, SnapshotError):
                continue
            scanned += 1
            for line_no, line in enumerate(text.splitlines(), start=1):
                matched = bool(pattern.search(line)) if pattern else query in line
                if matched:
                    results.append({"path": path, "line": line_no, "text": line[:2_000]})
                    if len(results) >= limit:
                        return {
                            "results": results,
                            "complete": False,
                            "continuation": {"path": path, "line": line_no + 1},
                            "scanned_files": scanned,
                        }
        return {"results": results, "complete": True, "continuation": None, "scanned_files": scanned}
