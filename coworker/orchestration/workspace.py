"""Isolated candidate workspaces with conflict-safe, journaled delivery.

Clean Git repositories use detached worktrees. Dirty repositories and ordinary
directories use byte-for-byte filesystem snapshots so the candidate always starts from
the state the user saw. Delivery is a separate, preflighted operation and never commits.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Optional


_SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")
_SNAPSHOT_METADATA = "snapshot.json"
_SNAPSHOT_OWNER = ".snapshot-owner.json"
_SNAPSHOT_QUARANTINE = "snapshot-quarantine"
_DEFAULT_MAX_SNAPSHOT_FILES = 50_000
_DEFAULT_MAX_SNAPSHOT_BYTES = 2 * 1024**3


class WorkspaceError(RuntimeError):
    pass


class WorkspaceConflictError(WorkspaceError):
    def __init__(self, preflight: "DeliveryPreflight") -> None:
        self.preflight = preflight
        super().__init__(
            "delivery conflicts with source changes: " + ", ".join(preflight.conflicts)
        )


class WorkspaceKind(str, Enum):
    GIT_WORKTREE = "git_worktree"
    GIT_SNAPSHOT = "git_snapshot"
    FILESYSTEM_SNAPSHOT = "filesystem_snapshot"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return _sha256_bytes(encoded)


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    path: str
    kind: str
    sha256: str
    size: int
    mode: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "sha256": self.sha256,
            "size": self.size,
            "mode": self.mode,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ManifestEntry":
        return cls(
            path=str(value["path"]),
            kind=str(value["kind"]),
            sha256=str(value["sha256"]),
            size=int(value["size"]),
            mode=int(value["mode"]),
        )


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    entries: tuple[ManifestEntry, ...]
    digest: str

    @property
    def by_path(self) -> Mapping[str, ManifestEntry]:
        return {entry.path: entry for entry in self.entries}

    def to_dict(self) -> dict[str, Any]:
        return {
            "digest": self.digest,
            "entries": [entry.to_dict() for entry in self.entries],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SnapshotManifest":
        entries = tuple(
            ManifestEntry.from_dict(item) for item in value.get("entries", ())
        )
        digest = str(value.get("digest") or _canonical_hash([e.to_dict() for e in entries]))
        return cls(entries=entries, digest=digest)


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshot:
    snapshot_id: str
    source_root: Path
    candidate_root: Path
    baseline_root: Path
    kind: WorkspaceKind
    baseline_manifest: SnapshotManifest
    created_at: str
    git_root: Optional[Path] = None
    git_head: Optional[str] = None
    git_dirty: bool = False

    @property
    def source(self) -> Path:
        return self.source_root

    @property
    def candidate(self) -> Path:
        return self.candidate_root

    @property
    def baseline(self) -> Path:
        return self.baseline_root

    @property
    def workspace_root(self) -> Path:
        return self.baseline_root.parent

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "snapshot_id": self.snapshot_id,
            "source_root": str(self.source_root),
            "candidate_root": str(self.candidate_root),
            "baseline_root": str(self.baseline_root),
            "kind": self.kind.value,
            "baseline_manifest": self.baseline_manifest.to_dict(),
            "created_at": self.created_at,
            "git_root": str(self.git_root) if self.git_root else None,
            "git_head": self.git_head,
            "git_dirty": self.git_dirty,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkspaceSnapshot":
        git_root = value.get("git_root")
        return cls(
            snapshot_id=str(value["snapshot_id"]),
            source_root=Path(str(value["source_root"])).resolve(),
            candidate_root=Path(str(value["candidate_root"])).resolve(),
            baseline_root=Path(str(value["baseline_root"])).resolve(),
            kind=WorkspaceKind(str(value["kind"])),
            baseline_manifest=SnapshotManifest.from_dict(value["baseline_manifest"]),
            created_at=str(value["created_at"]),
            git_root=Path(str(git_root)).resolve() if git_root else None,
            git_head=str(value["git_head"]) if value.get("git_head") else None,
            git_dirty=bool(value.get("git_dirty", False)),
        )


@dataclass(frozen=True, slots=True)
class CandidateEvidence:
    changed_files: tuple[str, ...]
    new_files: tuple[str, ...]
    deleted_files: tuple[str, ...]
    patch: str
    patch_sha256: str
    candidate_manifest: SnapshotManifest

    def to_dict(self, *, include_patch: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "changed_files": list(self.changed_files),
            "new_files": list(self.new_files),
            "deleted_files": list(self.deleted_files),
            "patch_sha256": self.patch_sha256,
            "candidate_manifest": self.candidate_manifest.to_dict(),
        }
        if include_patch:
            result["patch"] = self.patch
        return result


@dataclass(frozen=True, slots=True)
class DeliveryPreflight:
    can_deliver: bool
    changed_files: tuple[str, ...]
    deleted_files: tuple[str, ...]
    source_changed: tuple[str, ...]
    conflicts: tuple[str, ...]
    evidence: CandidateEvidence
    source_manifest_digest: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "can_deliver": self.can_deliver,
            "changed_files": list(self.changed_files),
            "deleted_files": list(self.deleted_files),
            "source_changed": list(self.source_changed),
            "conflicts": list(self.conflicts),
            "source_manifest_digest": self.source_manifest_digest,
            "evidence": self.evidence.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class DeliveryJournalEntry:
    transaction_id: str
    snapshot_id: str
    timestamp: str
    status: str
    source_root: str
    changed_files: tuple[str, ...]
    deleted_files: tuple[str, ...]
    conflicts: tuple[str, ...]
    patch_sha256: str
    candidate_manifest_sha256: str = ""
    backup_root: Optional[str] = None
    original_paths: tuple[str, ...] = ()
    backup_manifest: Optional[SnapshotManifest] = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp,
            "status": self.status,
            "source_root": self.source_root,
            "changed_files": list(self.changed_files),
            "deleted_files": list(self.deleted_files),
            "conflicts": list(self.conflicts),
            "patch_sha256": self.patch_sha256,
            "candidate_manifest_sha256": self.candidate_manifest_sha256,
            "backup_root": self.backup_root,
            "original_paths": list(self.original_paths),
            "backup_manifest": (
                self.backup_manifest.to_dict() if self.backup_manifest else None
            ),
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DeliveryJournalEntry":
        return cls(
            transaction_id=str(value["transaction_id"]),
            snapshot_id=str(value["snapshot_id"]),
            timestamp=str(value["timestamp"]),
            status=str(value["status"]),
            source_root=str(value["source_root"]),
            changed_files=tuple(value.get("changed_files", ())),
            deleted_files=tuple(value.get("deleted_files", ())),
            conflicts=tuple(value.get("conflicts", ())),
            patch_sha256=str(value.get("patch_sha256", "")),
            candidate_manifest_sha256=str(
                value.get("candidate_manifest_sha256", "")
            ),
            backup_root=(
                str(value["backup_root"]) if value.get("backup_root") else None
            ),
            original_paths=tuple(value.get("original_paths", ())),
            backup_manifest=(
                SnapshotManifest.from_dict(value["backup_manifest"])
                if isinstance(value.get("backup_manifest"), Mapping)
                else None
            ),
            error=str(value["error"]) if value.get("error") else None,
        )


def build_manifest(root: Path | str) -> SnapshotManifest:
    """Hash every file and symlink under *root* in stable relative-path order."""
    root_path = Path(root).expanduser().resolve()
    if not root_path.is_dir():
        raise WorkspaceError(f"manifest root is not a directory: {root_path}")
    entries: list[ManifestEntry] = []
    for current, directories, files in os.walk(root_path, followlinks=False):
        current_path = Path(current)
        directories[:] = sorted(name for name in directories if name != ".git")
        files = sorted(name for name in files if name != ".git")
        for name in tuple(directories):
            path = current_path / name
            if path.is_symlink():
                directories.remove(name)
                target = os.readlink(path).encode("utf-8", errors="surrogateescape")
                info = path.lstat()
                entries.append(
                    ManifestEntry(
                        path=path.relative_to(root_path).as_posix(),
                        kind="symlink",
                        sha256=_sha256_bytes(target),
                        size=len(target),
                        mode=stat.S_IMODE(info.st_mode),
                    )
                )
        for name in files:
            path = current_path / name
            relative = path.relative_to(root_path).as_posix()
            try:
                info = path.lstat()
                if path.is_symlink():
                    target = os.readlink(path).encode(
                        "utf-8", errors="surrogateescape"
                    )
                    entry = ManifestEntry(
                        relative,
                        "symlink",
                        _sha256_bytes(target),
                        len(target),
                        stat.S_IMODE(info.st_mode),
                    )
                else:
                    entry = ManifestEntry(
                        relative,
                        "file",
                        _sha256_file(path),
                        info.st_size,
                        stat.S_IMODE(info.st_mode),
                    )
            except FileNotFoundError as exc:
                raise WorkspaceError(f"file changed while manifest was built: {path}") from exc
            entries.append(entry)
    entries.sort(key=lambda item: item.path)
    frozen = tuple(entries)
    return SnapshotManifest(
        entries=frozen,
        digest=_canonical_hash([entry.to_dict() for entry in frozen]),
    )


class WorkspaceManager:
    """Create candidate workspaces and safely deliver their exact filesystem delta."""

    def __init__(
        self,
        base_dir: Path | str,
        *,
        journal_path: Optional[Path | str] = None,
        max_snapshot_files: int = _DEFAULT_MAX_SNAPSHOT_FILES,
        max_snapshot_bytes: int = _DEFAULT_MAX_SNAPSHOT_BYTES,
    ) -> None:
        self.base_dir = Path(base_dir).expanduser().resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.journal_path = (
            Path(journal_path).expanduser().resolve()
            if journal_path
            else self.base_dir / "delivery-journal.jsonl"
        )
        self.max_snapshot_files = max(1, int(max_snapshot_files))
        self.max_snapshot_bytes = max(1, int(max_snapshot_bytes))
        self._lock = threading.RLock()
        self._journal_tail_truncated = False

    def prepare(
        self,
        source: Path | str,
        *,
        snapshot_id: Optional[str] = None,
    ) -> WorkspaceSnapshot:
        source_root = Path(source).expanduser().resolve()
        if not source_root.is_dir():
            raise WorkspaceError(f"workspace source is not a directory: {source_root}")
        identifier = snapshot_id or f"ws-{uuid.uuid4().hex}"
        if not _SNAPSHOT_ID.fullmatch(identifier):
            raise WorkspaceError(f"invalid snapshot_id: {identifier!r}")

        git = self._git_info(source_root)
        if git:
            source_root, head, dirty = git
        else:
            head, dirty = None, False
        self._assert_base_outside(source_root)

        workspace_root = self.base_dir / "snapshots" / identifier
        baseline_root = workspace_root / "baseline"
        candidate_root = workspace_root / "candidate"
        snapshot_lock = self._acquire_snapshot_lock(workspace_root)
        source_lock = None
        kind: WorkspaceKind
        worktree_created = False
        owns_workspace_root = False
        try:
            source_lock = self._acquire_source_lock(source_root)
            # Re-read Git state after locking so a just-completed delivery cannot
            # leave the clean/dirty decision stale.
            git = self._git_info(source_root)
            if git:
                source_root, head, dirty = git
            self._recover_or_reject_existing_snapshot(
                workspace_root,
                snapshot_id=identifier,
                source_root=source_root,
            )
            if not git or dirty:
                self._assert_snapshot_within_limits(source_root)
            workspace_root.mkdir(parents=True)
            owns_workspace_root = True
            self._atomic_write_json(
                workspace_root / _SNAPSHOT_OWNER,
                {
                    "schema_version": 1,
                    "snapshot_id": identifier,
                    "source_root": str(source_root),
                    "created_at": _utc_now(),
                },
            )
            if git and not dirty:
                self._run_git(
                    source_root,
                    "worktree",
                    "add",
                    "--detach",
                    str(candidate_root),
                    head or "HEAD",
                )
                worktree_created = True
                self._copy_tree(candidate_root, baseline_root)
                kind = WorkspaceKind.GIT_WORKTREE
            else:
                self._copy_tree(source_root, baseline_root)
                self._copy_tree(baseline_root, candidate_root)
                kind = (
                    WorkspaceKind.GIT_SNAPSHOT
                    if git
                    else WorkspaceKind.FILESYSTEM_SNAPSHOT
                )
            manifest = build_manifest(baseline_root)
            snapshot = WorkspaceSnapshot(
                snapshot_id=identifier,
                source_root=source_root,
                candidate_root=candidate_root,
                baseline_root=baseline_root,
                kind=kind,
                baseline_manifest=manifest,
                created_at=_utc_now(),
                git_root=source_root if git else None,
                git_head=head,
                git_dirty=dirty,
            )
            self._atomic_write_json(
                workspace_root / _SNAPSHOT_METADATA,
                snapshot.to_dict(),
            )
            return snapshot
        except Exception:
            if owns_workspace_root:
                if worktree_created:
                    self._remove_git_worktree(source_root, candidate_root)
                self._safe_rmtree(workspace_root)
            raise
        finally:
            if source_lock is not None:
                self._release_source_lock(source_lock)
            self._release_source_lock(snapshot_lock)

    create = prepare
    prepare_workspace = prepare

    def load(self, snapshot_id: str) -> WorkspaceSnapshot:
        if not _SNAPSHOT_ID.fullmatch(snapshot_id):
            raise WorkspaceError(f"invalid snapshot_id: {snapshot_id!r}")
        path = self.base_dir / "snapshots" / snapshot_id / _SNAPSHOT_METADATA
        if not path.is_file():
            raise KeyError(snapshot_id)
        snapshot = WorkspaceSnapshot.from_dict(
            json.loads(path.read_text(encoding="utf-8"))
        )
        if snapshot.snapshot_id != snapshot_id:
            raise WorkspaceError("snapshot metadata id does not match requested id")
        self._validate_snapshot(snapshot)
        return snapshot

    load_snapshot = load

    def _recover_or_reject_existing_snapshot(
        self,
        workspace_root: Path,
        *,
        snapshot_id: str,
        source_root: Path,
    ) -> None:
        """Quarantine a provably incomplete prepare, never a usable snapshot.

        A hard process exit can leave the manager-owned directory after ``mkdir`` or
        while the metadata file is being committed.  The per-snapshot advisory lock
        excludes another current preparer.  We only recover directories whose top-level
        shape is limited to prepare-time files and whose ownership marker, when present,
        names this exact snapshot and source.  Anything else is preserved in place and
        requires operator inspection.
        """

        if not os.path.lexists(workspace_root):
            return
        if workspace_root.is_symlink() or not workspace_root.is_dir():
            raise WorkspaceError(
                f"snapshot path is not a manager-owned directory: {workspace_root}"
            )

        metadata_error: Optional[BaseException] = None
        try:
            self.load(snapshot_id)
        except (
            KeyError,
            OSError,
            UnicodeError,
            ValueError,
            TypeError,
            WorkspaceError,
        ) as exc:
            metadata_error = exc
        else:
            raise WorkspaceError(f"snapshot already exists: {snapshot_id}")

        if not self._is_recoverable_incomplete_snapshot(
            workspace_root,
            snapshot_id=snapshot_id,
            source_root=source_root,
        ):
            raise WorkspaceError(
                f"snapshot path exists but is not a recoverable incomplete snapshot: "
                f"{snapshot_id}"
            ) from metadata_error
        self._quarantine_incomplete_snapshot(
            workspace_root,
            snapshot_id=snapshot_id,
            source_root=source_root,
        )

    def _is_recoverable_incomplete_snapshot(
        self,
        workspace_root: Path,
        *,
        snapshot_id: str,
        source_root: Path,
    ) -> bool:
        allowed_names = {
            "baseline",
            "candidate",
            _SNAPSHOT_METADATA,
            _SNAPSHOT_OWNER,
        }
        try:
            children = tuple(workspace_root.iterdir())
        except OSError:
            return False
        for child in children:
            name = child.name
            if name not in allowed_names and not self._is_snapshot_metadata_temp(name):
                return False
            if child.is_symlink():
                return False
            if name in {"baseline", "candidate"} and not child.is_dir():
                return False
            if (
                name in {_SNAPSHOT_METADATA, _SNAPSHOT_OWNER}
                or self._is_snapshot_metadata_temp(name)
            ) and not child.is_file():
                return False

        owner_path = workspace_root / _SNAPSHOT_OWNER
        if not owner_path.exists():
            # Compatibility recovery for directories left by the pre-marker writer.
            # Their exact manager-owned path and prepare-only shape are the only safe
            # signals available; quarantine preserves every remaining byte.
            return True
        try:
            owner = json.loads(owner_path.read_text(encoding="utf-8"))
            if not isinstance(owner, Mapping):
                return False
            owner_source = Path(str(owner["source_root"])).expanduser().resolve()
        except (KeyError, OSError, UnicodeError, ValueError, TypeError):
            return False
        return (
            owner.get("schema_version") == 1
            and str(owner.get("snapshot_id") or "") == snapshot_id
            and os.path.normcase(str(owner_source))
            == os.path.normcase(str(source_root.resolve()))
        )

    @staticmethod
    def _is_snapshot_metadata_temp(name: str) -> bool:
        return any(
            name.startswith(f"{metadata_name}.tmp-")
            for metadata_name in (_SNAPSHOT_METADATA, _SNAPSHOT_OWNER)
        )

    def _quarantine_incomplete_snapshot(
        self,
        workspace_root: Path,
        *,
        snapshot_id: str,
        source_root: Path,
    ) -> Path:
        candidate_root = workspace_root / "candidate"
        if (candidate_root / ".git").is_file() and self._git_info(source_root):
            # A crash after `git worktree add` leaves repository administration tied
            # to the old candidate path. It is safe to remove because the ownership
            # and prepare-only shape checks above prove the snapshot was never published.
            self._remove_git_worktree(source_root, candidate_root)

        quarantine_root = self.base_dir / _SNAPSHOT_QUARANTINE
        quarantine_root.mkdir(parents=True, exist_ok=True)
        destination = quarantine_root / (
            f"{snapshot_id}-{time.time_ns()}-{uuid.uuid4().hex[:12]}"
        )
        self._assert_under_base(workspace_root)
        self._assert_under_base(destination)
        os.replace(workspace_root, destination)
        self._fsync_directory_chain(workspace_root.parent, self.base_dir)
        self._fsync_directory_chain(quarantine_root, self.base_dir)
        return destination

    def _atomic_write_json(self, path: Path, value: Mapping[str, Any]) -> None:
        """Commit JSON metadata with file and directory durability.

        The temporary file lives beside the destination so ``os.replace`` is atomic.
        A process crash therefore exposes either the previous complete metadata file or
        no metadata file; a leftover temporary is recognized as prepare-only state.
        """

        path = path.resolve()
        self._assert_under_base(path.parent)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.tmp-{uuid.uuid4().hex}")
        payload = (
            json.dumps(
                dict(value),
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
        try:
            with temporary.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            self._fsync_directory_chain(path.parent, self.base_dir)
        finally:
            if os.path.lexists(temporary):
                self._remove_path(temporary)

    def collect_candidate(self, snapshot: WorkspaceSnapshot) -> CandidateEvidence:
        self._validate_snapshot(snapshot)
        candidate_manifest = build_manifest(snapshot.candidate_root)
        baseline = snapshot.baseline_manifest.by_path
        candidate = candidate_manifest.by_path
        new_files = tuple(sorted(set(candidate) - set(baseline)))
        modified = tuple(
            sorted(
                path
                for path in set(candidate) & set(baseline)
                if candidate[path] != baseline[path]
            )
        )
        changed = tuple(sorted((*new_files, *modified)))
        deleted = tuple(sorted(set(baseline) - set(candidate)))
        patch = self._make_patch(snapshot, changed, deleted, new_files)
        evidence = CandidateEvidence(
            changed_files=changed,
            new_files=new_files,
            deleted_files=deleted,
            patch=patch,
            patch_sha256=_sha256_bytes(patch.encode("utf-8")),
            candidate_manifest=candidate_manifest,
        )
        (snapshot.workspace_root / "candidate.patch").write_text(patch, encoding="utf-8")
        (snapshot.workspace_root / "evidence.json").write_text(
            json.dumps(evidence.to_dict(include_patch=False), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return evidence

    candidate_evidence = collect_candidate

    def preflight(self, snapshot: WorkspaceSnapshot) -> DeliveryPreflight:
        evidence = self.collect_candidate(snapshot)
        current_manifest = build_manifest(snapshot.source_root)
        baseline = snapshot.baseline_manifest.by_path
        current = current_manifest.by_path
        candidate = evidence.candidate_manifest.by_path
        all_source_paths = set(baseline) | set(current)
        source_changed = tuple(
            sorted(path for path in all_source_paths if baseline.get(path) != current.get(path))
        )
        candidate_touched = set(evidence.changed_files) | set(evidence.deleted_files)
        conflicts = tuple(
            sorted(
                path
                for path in candidate_touched & set(source_changed)
                if current.get(path) != candidate.get(path)
            )
        )
        return DeliveryPreflight(
            can_deliver=not conflicts,
            changed_files=evidence.changed_files,
            deleted_files=evidence.deleted_files,
            source_changed=source_changed,
            conflicts=conflicts,
            evidence=evidence,
            source_manifest_digest=current_manifest.digest,
        )

    preflight_delivery = preflight

    def deliver(
        self,
        snapshot: WorkspaceSnapshot,
        *,
        expected_candidate_manifest_sha256: Optional[str] = None,
        expected_patch_sha256: Optional[str] = None,
        fence_check: Optional[Callable[[], None]] = None,
        _fence_held: bool = False,
    ) -> DeliveryJournalEntry:
        """Apply a preflighted candidate delta and append an immutable JSONL receipt."""
        if not _fence_held:
            # Generic deliveries preserve the historical fail-fast contention
            # contract. Formal publication acquires the same fence explicitly with
            # a bounded wait so cancel/publish can be linearly ordered.
            with self.delivery_fence(snapshot, wait_seconds=0.0):
                return self.deliver(
                    snapshot,
                    expected_candidate_manifest_sha256=(
                        expected_candidate_manifest_sha256
                    ),
                    expected_patch_sha256=expected_patch_sha256,
                    fence_check=fence_check,
                    _fence_held=True,
                )
        with self._lock:
            if fence_check is not None:
                fence_check()
            preflight = self.preflight(snapshot)
            actual_manifest = preflight.evidence.candidate_manifest.digest
            if (
                expected_candidate_manifest_sha256 is not None
                and actual_manifest != expected_candidate_manifest_sha256
            ):
                raise WorkspaceError(
                    "candidate manifest changed after it was sealed: "
                    f"expected {expected_candidate_manifest_sha256}, found "
                    f"{actual_manifest}"
                )
            if (
                expected_patch_sha256 is not None
                and preflight.evidence.patch_sha256 != expected_patch_sha256
            ):
                raise WorkspaceError(
                    "candidate patch changed after it was sealed: "
                    f"expected {expected_patch_sha256}, found "
                    f"{preflight.evidence.patch_sha256}"
                )
            transaction_id = f"delivery-{uuid.uuid4().hex}"
            if preflight.conflicts:
                entry = self._journal_entry(
                    snapshot,
                    preflight,
                    transaction_id,
                    status="blocked",
                )
                self._append_journal(entry)
                raise WorkspaceConflictError(preflight)

            backup_root = snapshot.workspace_root / "delivery-backups" / transaction_id
            stage_root = snapshot.workspace_root / "delivery-stage" / transaction_id
            touched = tuple(
                sorted(
                    set(preflight.changed_files) | set(preflight.deleted_files),
                    key=lambda item: (item.count("/"), item),
                )
            )
            existed: dict[str, bool] = {}
            source_mutated = False
            backup_manifest: Optional[SnapshotManifest] = None
            try:
                pending = tuple(
                    entry.transaction_id
                    for entry in self.incomplete_deliveries()
                    if Path(entry.source_root).resolve() == snapshot.source_root
                )
                if pending:
                    raise WorkspaceError(
                        "source has incomplete deliveries requiring recovery: "
                        + ", ".join(pending)
                    )
                backup_root.mkdir(parents=True)
                stage_root.mkdir(parents=True)
                for relative in preflight.changed_files:
                    candidate_path = self._member(snapshot.candidate_root, relative)
                    self._copy_path(candidate_path, self._member(stage_root, relative))
                staged = build_manifest(stage_root).by_path
                expected = preflight.evidence.candidate_manifest.by_path
                unstable = tuple(
                    relative
                    for relative in preflight.changed_files
                    if staged.get(relative) != expected.get(relative)
                )
                if unstable:
                    raise WorkspaceError(
                        "candidate changed while delivery was staged: "
                        + ", ".join(unstable)
                    )
                if build_manifest(snapshot.source_root).digest != preflight.source_manifest_digest:
                    raise WorkspaceError("source changed after delivery preflight; retry")

                for relative in touched:
                    source_path = self._member(snapshot.source_root, relative)
                    existed[relative] = source_path.exists() or source_path.is_symlink()
                    if existed[relative]:
                        self._copy_path(source_path, self._member(backup_root, relative))
                if build_manifest(snapshot.source_root).digest != preflight.source_manifest_digest:
                    raise WorkspaceError("source changed while delivery backup was created; retry")
                backup_manifest = build_manifest(backup_root)
                original_paths = tuple(
                    relative for relative in touched if existed.get(relative, False)
                )
                self._fsync_tree(backup_root)
                self._fsync_directory_chain(
                    backup_root.parent, snapshot.workspace_root
                )

                # A durable intent record precedes the first source mutation. If the
                # process exits mid-delivery, ``incomplete_deliveries`` and ``recover``
                # can find the backup and restore the prior state.
                self._append_journal(
                    self._journal_entry(
                        snapshot,
                        preflight,
                        transaction_id,
                        status="started",
                        backup_root=backup_root,
                        original_paths=original_paths,
                        backup_manifest=backup_manifest,
                    )
                )
                if build_manifest(snapshot.source_root).digest != preflight.source_manifest_digest:
                    raise WorkspaceError(
                        "source changed while delivery intent was persisted; retry"
                    )
                if fence_check is not None:
                    # Renew/check after all potentially long staging and backup I/O,
                    # immediately before the first user-workspace mutation.
                    fence_check()
                # Delete deepest paths first, then install shallow paths. This handles a
                # candidate changing a file into a directory tree (and the inverse).
                source_mutated = True
                for relative in sorted(
                    preflight.deleted_files,
                    key=lambda item: (-item.count("/"), item),
                ):
                    if fence_check is not None:
                        fence_check()
                    self._remove_path(self._member(snapshot.source_root, relative))
                for relative in sorted(
                    preflight.changed_files,
                    key=lambda item: (item.count("/"), item),
                ):
                    if fence_check is not None:
                        fence_check()
                    self._install_path(
                        self._member(stage_root, relative),
                        self._member(snapshot.source_root, relative),
                    )
                self._fsync_delivery(
                    snapshot.source_root,
                    preflight.changed_files,
                    preflight.deleted_files,
                )
                if fence_check is not None:
                    fence_check()
                entry = self._journal_entry(
                    snapshot,
                    preflight,
                    transaction_id,
                    status="delivered",
                    backup_root=backup_root,
                    original_paths=original_paths,
                    backup_manifest=backup_manifest,
                )
                self._append_journal(entry)
                return entry
            except Exception as exc:
                rollback_error: Optional[Exception] = None
                if source_mutated:
                    try:
                        self._rollback(snapshot.source_root, backup_root, touched, existed)
                        self._fsync_delivery(snapshot.source_root, touched, ())
                    except Exception as rollback_exc:  # pragma: no cover - exceptional I/O
                        rollback_error = rollback_exc
                error = str(exc)
                if rollback_error:
                    error += f"; rollback failed: {rollback_error}"
                status = "recovery_required" if rollback_error else "failed"
                entry = self._journal_entry(
                    snapshot,
                    preflight,
                    transaction_id,
                    status=status,
                    backup_root=backup_root,
                    original_paths=tuple(
                        relative for relative in touched if existed.get(relative, False)
                    ),
                    backup_manifest=backup_manifest,
                    error=error,
                )
                self._append_journal(entry)
                raise WorkspaceError(error) from exc
            finally:
                self._safe_rmtree(stage_root)

    deliver_candidate = deliver

    def journal(self) -> tuple[DeliveryJournalEntry, ...]:
        with self._lock:
            journal_lock = self._acquire_journal_lock()
            try:
                self._journal_tail_truncated = False
                if not self.journal_path.exists():
                    return ()
                records: list[DeliveryJournalEntry] = []
                lines = self.journal_path.read_bytes().splitlines(keepends=True)
                for index, raw_line in enumerate(lines, start=1):
                    if not raw_line.endswith(b"\n"):
                        if index == len(lines):
                            self._journal_tail_truncated = True
                            break
                        raise WorkspaceError(
                            f"delivery journal has an incomplete record at line {index}"
                        )
                    payload = raw_line.rstrip(b"\r\n")
                    if not payload:
                        continue
                    try:
                        value = json.loads(payload.decode("utf-8"))
                        records.append(DeliveryJournalEntry.from_dict(value))
                    except (UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
                        raise WorkspaceError(
                            f"delivery journal is corrupt at line {index}: {exc}"
                        ) from exc
                return tuple(records)
            finally:
                self._release_source_lock(journal_lock)

    @property
    def journal_has_truncated_tail(self) -> bool:
        return self._journal_tail_truncated

    delivery_journal = journal

    def incomplete_deliveries(self) -> tuple[DeliveryJournalEntry, ...]:
        """Return transactions whose last durable state is ``started``."""
        latest: dict[str, DeliveryJournalEntry] = {}
        for entry in self.journal():
            latest[entry.transaction_id] = entry
        return tuple(
            entry
            for _, entry in sorted(latest.items())
            if entry.status in {"started", "recovery_required"}
        )

    def recover(
        self,
        transaction_id: str,
        *,
        fence_check: Optional[Callable[[], None]] = None,
    ) -> DeliveryJournalEntry:
        """Roll an interrupted delivery back from its manager-owned backup."""
        with self._lock:
            preliminary = next(
                (
                    entry
                    for entry in self.incomplete_deliveries()
                    if entry.transaction_id == transaction_id
                ),
                None,
            )
            if preliminary is None:
                raise WorkspaceError(
                    f"delivery is not recoverable or is already terminal: {transaction_id}"
                )
            snapshot = self.load(preliminary.snapshot_id)
            if str(snapshot.source_root) != preliminary.source_root:
                raise WorkspaceError("delivery journal source does not match snapshot")
            source_lock = self._acquire_source_lock(snapshot.source_root)
            try:
                # Re-read under the source lock. Another process may have recovered the
                # transaction after our preliminary lookup but before we acquired it.
                started = next(
                    (
                        entry
                        for entry in self.incomplete_deliveries()
                        if entry.transaction_id == transaction_id
                    ),
                    None,
                )
                if started is None:
                    raise WorkspaceError(
                        f"delivery is already terminal: {transaction_id}"
                    )
                snapshot = self.load(started.snapshot_id)
                if str(snapshot.source_root) != started.source_root:
                    raise WorkspaceError(
                        "delivery journal source does not match snapshot"
                    )
                expected_backup = (
                    snapshot.workspace_root / "delivery-backups" / transaction_id
                ).resolve()
                if (
                    not started.backup_root
                    or Path(started.backup_root).resolve() != expected_backup
                ):
                    raise WorkspaceError("delivery journal backup path is invalid")
                self._assert_under_base(expected_backup)
                if started.backup_manifest is None:
                    raise WorkspaceError("delivery journal has no backup manifest")
                if build_manifest(expected_backup) != started.backup_manifest:
                    raise WorkspaceError("delivery backup failed integrity validation")
                touched = tuple(
                    sorted(set(started.changed_files) | set(started.deleted_files))
                )
                original = set(started.original_paths)
                for relative in original:
                    backup_path = self._member(expected_backup, relative)
                    if not backup_path.exists() and not backup_path.is_symlink():
                        raise WorkspaceError(
                            f"delivery backup is missing original path: {relative}"
                        )
                existed = {
                    relative: relative in original for relative in touched
                }
                if fence_check is not None:
                    fence_check()
                self._rollback(snapshot.source_root, expected_backup, touched, existed)
                self._fsync_delivery(snapshot.source_root, touched, ())
                if fence_check is not None:
                    fence_check()
                recovered = DeliveryJournalEntry(
                    transaction_id=started.transaction_id,
                    snapshot_id=started.snapshot_id,
                    timestamp=_utc_now(),
                    status="recovered",
                    source_root=started.source_root,
                    changed_files=started.changed_files,
                    deleted_files=started.deleted_files,
                    conflicts=started.conflicts,
                    patch_sha256=started.patch_sha256,
                    candidate_manifest_sha256=(
                        started.candidate_manifest_sha256
                    ),
                    backup_root=started.backup_root,
                    original_paths=started.original_paths,
                    backup_manifest=started.backup_manifest,
                )
                self._append_journal(recovered)
                return recovered
            finally:
                self._release_source_lock(source_lock)

    def cleanup(self, snapshot: WorkspaceSnapshot) -> None:
        with self._lock:
            source_lock = self._acquire_source_lock(snapshot.source_root)
            try:
                pending = tuple(
                    entry.transaction_id
                    for entry in self.incomplete_deliveries()
                    if entry.snapshot_id == snapshot.snapshot_id
                )
                if pending:
                    raise WorkspaceError(
                        "cannot clean a workspace with incomplete deliveries: "
                        + ", ".join(pending)
                    )
                self._validate_snapshot(snapshot)
                if snapshot.kind is WorkspaceKind.GIT_WORKTREE and snapshot.git_root:
                    self._remove_git_worktree(snapshot.git_root, snapshot.candidate_root)
                self._safe_rmtree(snapshot.workspace_root)
            finally:
                self._release_source_lock(source_lock)

    def _git_info(
        self, source: Path
    ) -> Optional[tuple[Path, Optional[str], bool]]:
        try:
            root_result = subprocess.run(
                ["git", "-C", str(source), "rev-parse", "--show-toplevel"],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            return None
        if root_result.returncode:
            return None
        root = Path(root_result.stdout.strip()).resolve()
        head_result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        head = head_result.stdout.strip() if head_result.returncode == 0 else None
        dirty = bool(
            self._run_git(
                root, "status", "--porcelain", "--untracked-files=all"
            ).strip()
        ) or head is None
        return root, head, dirty

    @contextmanager
    def source_fence(
        self, source_root: Path | str, *, wait_seconds: float = 30.0
    ):
        """Linearize a formal publication with pause/cancel across processes."""

        normalized_source = Path(source_root).expanduser().resolve()
        git = self._git_info(normalized_source)
        if git:
            normalized_source = git[0]
        source_lock = self._acquire_source_lock(
            normalized_source, wait_seconds=wait_seconds
        )
        try:
            yield normalized_source
        finally:
            self._release_source_lock(source_lock)

    @contextmanager
    def delivery_fence(
        self, snapshot: WorkspaceSnapshot, *, wait_seconds: float = 30.0
    ):
        """Hold the snapshot and source fences for sealed validation + delivery."""

        snapshot_lock = self._acquire_snapshot_lock(snapshot.workspace_root)
        source_lock = None
        try:
            source_lock = self._acquire_source_lock(
                snapshot.source_root, wait_seconds=wait_seconds
            )
            self._validate_snapshot(snapshot)
            yield
        finally:
            if source_lock is not None:
                self._release_source_lock(source_lock)
            self._release_source_lock(snapshot_lock)

    @staticmethod
    def _acquire_source_lock(
        source_root: Path, *, wait_seconds: float = 0.0
    ):
        normalized = os.path.normcase(str(source_root.resolve()))
        return WorkspaceManager._acquire_named_lock(
            "source:" + normalized,
            f"another delivery is active for source: {source_root}",
            wait_seconds=wait_seconds,
        )

    @staticmethod
    def _acquire_snapshot_lock(workspace_root: Path):
        normalized = os.path.normcase(str(workspace_root.resolve()))
        return WorkspaceManager._acquire_named_lock(
            "snapshot:" + normalized,
            f"snapshot preparation is busy: {workspace_root.name}",
            wait_seconds=30.0,
        )

    def _acquire_journal_lock(self):
        normalized = os.path.normcase(str(self.journal_path.resolve()))
        return self._acquire_named_lock(
            "journal:" + normalized,
            f"delivery journal is busy: {self.journal_path}",
            wait_seconds=30.0,
        )

    @staticmethod
    def _acquire_named_lock(
        key_value: str,
        error_message: str,
        *,
        wait_seconds: float = 0.0,
    ):
        lock_directory = Path(tempfile.gettempdir()) / "openworker-workspace-locks"
        lock_directory.mkdir(parents=True, exist_ok=True)
        key = _sha256_bytes(key_value.encode("utf-8"))
        stream = (lock_directory / f"{key}.lock").open("a+b")
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        deadline = time.monotonic() + max(0.0, wait_seconds)
        while True:
            try:
                stream.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return stream
            except OSError as exc:
                if time.monotonic() >= deadline:
                    stream.close()
                    raise WorkspaceError(error_message) from exc
                time.sleep(0.01)

    @staticmethod
    def _release_source_lock(stream) -> None:
        try:
            try:
                stream.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
            except OSError:
                # Closing the descriptor also releases an advisory lock; never mask a
                # completed delivery merely because an explicit unlock raced shutdown.
                pass
        finally:
            stream.close()

    @staticmethod
    def _run_git(root: Path, *arguments: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(root), *arguments],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise WorkspaceError(f"git {' '.join(arguments)} failed: {detail}")
        return result.stdout

    def _remove_git_worktree(self, git_root: Path, candidate_root: Path) -> None:
        self._assert_under_base(candidate_root)
        result = subprocess.run(
            [
                "git",
                "-C",
                str(git_root),
                "worktree",
                "remove",
                "--force",
                str(candidate_root),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode and candidate_root.exists():
            raise WorkspaceError(
                "git worktree remove failed: "
                + (result.stderr.strip() or result.stdout.strip())
            )

    @staticmethod
    def _copy_tree(source: Path, destination: Path) -> None:
        shutil.copytree(
            source,
            destination,
            symlinks=True,
            ignore=lambda _path, names: {".git"} if ".git" in names else set(),
        )

    def _assert_snapshot_within_limits(self, source: Path) -> None:
        """Reject oversized copies before creating any snapshot directories."""

        file_count = 0
        byte_count = 0
        pending = [source]
        try:
            while pending:
                current = pending.pop()
                with os.scandir(current) as entries:
                    for entry in entries:
                        if entry.name == ".git":
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                            continue
                        file_count += 1
                        byte_count += entry.stat(follow_symlinks=False).st_size
                        if (
                            file_count > self.max_snapshot_files
                            or byte_count > self.max_snapshot_bytes
                        ):
                            raise WorkspaceError(
                                "workspace exceeds the safe writable snapshot limit "
                                f"({self.max_snapshot_files:,} files or "
                                f"{self.max_snapshot_bytes / 1024**3:.1f} GiB); "
                                "use read_only=true, select the actual Git repository "
                                "root, or reduce the workspace scope"
                            )
        except OSError as exc:
            raise WorkspaceError(
                f"workspace changed or became unreadable during snapshot preflight: {exc}"
            ) from exc

    @staticmethod
    def _remove_path(path: Path) -> None:
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)

    def _copy_path(self, source: Path, destination: Path) -> None:
        if not source.exists() and not source.is_symlink():
            raise WorkspaceError(f"candidate path disappeared: {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._remove_path(destination)
        if source.is_symlink():
            destination.symlink_to(os.readlink(source), target_is_directory=source.is_dir())
        elif source.is_dir():
            shutil.copytree(source, destination, symlinks=True)
        else:
            shutil.copy2(source, destination)

    def _install_path(self, source: Path, destination: Path) -> None:
        """Install a staged file/symlink with an atomic leaf replacement when possible."""
        if source.is_dir() and not source.is_symlink():
            self._copy_path(source, destination)
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(
            f".{destination.name}.{uuid.uuid4().hex}.delivery"
        )
        try:
            if source.is_symlink():
                temporary.symlink_to(
                    os.readlink(source), target_is_directory=source.is_dir()
                )
            else:
                shutil.copy2(source, temporary)
            if destination.is_dir() and not destination.is_symlink():
                shutil.rmtree(destination)
            os.replace(temporary, destination)
        finally:
            self._remove_path(temporary)

    @staticmethod
    def _fsync_tree(root: Path) -> None:
        for current, directories, files in os.walk(root, followlinks=False):
            current_path = Path(current)
            directories[:] = [
                name for name in directories if not (current_path / name).is_symlink()
            ]
            for name in files:
                path = current_path / name
                if path.is_symlink():
                    continue
                WorkspaceManager._fsync_file(path)
        if os.name != "nt":
            for current, _directories, _files in os.walk(root, topdown=False):
                descriptor = os.open(current, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)

    @staticmethod
    def _fsync_delivery(
        root: Path,
        changed: Iterable[str],
        deleted: Iterable[str],
    ) -> None:
        directories: set[Path] = {root}
        for relative in tuple(changed) + tuple(deleted):
            path = WorkspaceManager._member(root, relative)
            if path.is_file() and not path.is_symlink():
                WorkspaceManager._fsync_file(path)
            cursor = path.parent
            while True:
                directories.add(cursor)
                if cursor == root:
                    break
                cursor = cursor.parent
        if os.name != "nt":
            for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
                if not directory.is_dir():
                    continue
                descriptor = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)

    @staticmethod
    def _fsync_file(path: Path) -> None:
        if os.name != "nt":
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
            return
        original_mode = stat.S_IMODE(path.stat().st_mode)
        try:
            path.chmod(original_mode | stat.S_IWRITE)
            with path.open("r+b") as stream:
                os.fsync(stream.fileno())
        finally:
            path.chmod(original_mode)

    @staticmethod
    def _fsync_directory_chain(start: Path, stop: Path) -> None:
        if os.name == "nt":
            return
        cursor = start.resolve()
        boundary = stop.resolve()
        while True:
            if cursor.is_dir():
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                descriptor = os.open(cursor, flags)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            if cursor == boundary or cursor.parent == cursor:
                break
            cursor = cursor.parent

    def _rollback(
        self,
        source_root: Path,
        backup_root: Path,
        touched: Iterable[str],
        existed: Mapping[str, bool],
    ) -> None:
        ordered = sorted(touched, key=lambda item: (-item.count("/"), item))
        for relative in ordered:
            self._remove_path(self._member(source_root, relative))
        for relative in sorted(touched, key=lambda item: (item.count("/"), item)):
            if existed.get(relative):
                self._copy_path(
                    self._member(backup_root, relative),
                    self._member(source_root, relative),
                )

    def _make_patch(
        self,
        snapshot: WorkspaceSnapshot,
        changed: Iterable[str],
        deleted: Iterable[str],
        new_files: Iterable[str],
    ) -> str:
        new_set = set(new_files)
        chunks: list[str] = []
        for relative in sorted((*changed, *deleted)):
            old_path = self._member(snapshot.baseline_root, relative)
            new_path = self._member(snapshot.candidate_root, relative)
            old_exists = relative not in new_set and (
                old_path.exists() or old_path.is_symlink()
            )
            new_exists = new_path.exists() or new_path.is_symlink()
            old_value = self._text_value(old_path) if old_exists else ""
            new_value = self._text_value(new_path) if new_exists else ""
            if old_value is None or new_value is None:
                chunks.append(
                    f"diff --git a/{relative} b/{relative}\n"
                    f"Binary files a/{relative} and b/{relative} differ\n"
                )
                continue
            chunks.append(f"diff --git a/{relative} b/{relative}\n")
            old_name = f"a/{relative}" if old_exists else "/dev/null"
            new_name = f"b/{relative}" if new_exists else "/dev/null"
            chunks.extend(
                difflib.unified_diff(
                    old_value.splitlines(keepends=True),
                    new_value.splitlines(keepends=True),
                    fromfile=old_name,
                    tofile=new_name,
                )
            )
        return "".join(chunks)

    @staticmethod
    def _text_value(path: Path) -> Optional[str]:
        if path.is_symlink():
            return f"symlink -> {os.readlink(path)}\n"
        if not path.exists():
            return ""
        value = path.read_bytes()
        if b"\x00" in value:
            return None
        try:
            # Patches and their hashes must replay identically on Windows and POSIX.
            return value.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
        except UnicodeDecodeError:
            return None

    def _journal_entry(
        self,
        snapshot: WorkspaceSnapshot,
        preflight: DeliveryPreflight,
        transaction_id: str,
        *,
        status: str,
        backup_root: Optional[Path] = None,
        original_paths: tuple[str, ...] = (),
        backup_manifest: Optional[SnapshotManifest] = None,
        error: Optional[str] = None,
    ) -> DeliveryJournalEntry:
        return DeliveryJournalEntry(
            transaction_id=transaction_id,
            snapshot_id=snapshot.snapshot_id,
            timestamp=_utc_now(),
            status=status,
            source_root=str(snapshot.source_root),
            changed_files=preflight.changed_files,
            deleted_files=preflight.deleted_files,
            conflicts=preflight.conflicts,
            patch_sha256=preflight.evidence.patch_sha256,
            candidate_manifest_sha256=(
                preflight.evidence.candidate_manifest.digest
            ),
            backup_root=str(backup_root) if backup_root else None,
            original_paths=original_paths,
            backup_manifest=backup_manifest,
            error=error,
        )

    def _append_journal(self, entry: DeliveryJournalEntry) -> None:
        with self._lock:
            journal_lock = self._acquire_journal_lock()
            try:
                journal_parent = self.journal_path.parent
                existing_ancestor = journal_parent
                while not existing_ancestor.exists():
                    parent = existing_ancestor.parent
                    if parent == existing_ancestor:
                        break
                    existing_ancestor = parent
                journal_parent.mkdir(parents=True, exist_ok=True)
                if self.journal_path.exists():
                    existing = self.journal_path.read_bytes()
                    if existing and not existing.endswith(b"\n"):
                        boundary = existing.rfind(b"\n")
                        with self.journal_path.open("r+b") as stream:
                            stream.truncate(boundary + 1 if boundary >= 0 else 0)
                            stream.flush()
                            os.fsync(stream.fileno())
                        self._journal_tail_truncated = False
                payload = (
                    json.dumps(entry.to_dict(), sort_keys=True, ensure_ascii=False)
                    + "\n"
                ).encode("utf-8")
                with self.journal_path.open("ab") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                self._fsync_directory_chain(journal_parent, existing_ancestor)
            finally:
                self._release_source_lock(journal_lock)

    def _validate_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        if not isinstance(snapshot, WorkspaceSnapshot):
            raise TypeError("snapshot must be a WorkspaceSnapshot")
        expected_root = (
            self.base_dir / "snapshots" / snapshot.snapshot_id
        ).resolve()
        if snapshot.workspace_root.resolve() != expected_root:
            raise WorkspaceError("snapshot workspace root does not match its id")
        if snapshot.baseline_root.resolve() != (expected_root / "baseline").resolve():
            raise WorkspaceError("snapshot baseline path is not manager-owned")
        if snapshot.candidate_root.resolve() != (expected_root / "candidate").resolve():
            raise WorkspaceError("snapshot candidate path is not manager-owned")
        self._assert_under_base(snapshot.workspace_root)
        self._assert_under_base(snapshot.baseline_root)
        self._assert_under_base(snapshot.candidate_root)
        if not snapshot.baseline_root.is_dir() or not snapshot.candidate_root.is_dir():
            raise WorkspaceError("snapshot workspace is missing")
        if build_manifest(snapshot.baseline_root) != snapshot.baseline_manifest:
            raise WorkspaceError("snapshot baseline manifest failed integrity validation")

    def _assert_under_base(self, path: Path) -> None:
        resolved = path.expanduser().resolve()
        try:
            resolved.relative_to(self.base_dir)
        except ValueError as exc:
            raise WorkspaceError(f"path is outside workspace manager: {resolved}") from exc
        if resolved == self.base_dir:
            raise WorkspaceError("refusing to operate on the workspace-manager root")

    def _assert_base_outside(self, source: Path) -> None:
        try:
            self.base_dir.relative_to(source)
        except ValueError:
            return
        raise WorkspaceError("base_dir must be outside the source workspace")

    def _safe_rmtree(self, path: Path) -> None:
        if not path.exists():
            return
        self._assert_under_base(path)
        shutil.rmtree(path)

    @staticmethod
    def _member(root: Path, relative: str) -> Path:
        pure = PurePosixPath(relative)
        if pure.is_absolute() or not pure.parts or any(
            part in {"", ".", ".."} for part in pure.parts
        ):
            raise WorkspaceError(f"unsafe relative path: {relative!r}")
        root = root.resolve()
        destination = root.joinpath(*pure.parts)
        # Never traverse a pre-existing symlinked parent: otherwise a candidate path
        # could redirect delivery outside the source root.
        cursor = root
        for part in pure.parts[:-1]:
            cursor /= part
            if cursor.is_symlink():
                raise WorkspaceError(f"refusing to traverse symlink parent: {cursor}")
        return destination


Workspace = WorkspaceSnapshot
Manifest = SnapshotManifest
