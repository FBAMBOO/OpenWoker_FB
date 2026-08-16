"""Durable, versioned configuration catalogs for orchestration.

Profiles and routing policies are part of a run's executable input.  The scheduler
therefore resolves an immutable published version and records its hash instead of
reading mutable UI preferences while a run is in flight.  Drafts use ETags so two
settings windows cannot silently overwrite each other.
"""

from __future__ import annotations

import copy
import errno
import hashlib
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from .profiles import (
    BUILTIN_PROFILES,
    AgentProfile,
    AgentProfileDraft,
    ProfileRef,
    ProfileValidationError,
    clone_profile,
)
from .routing import ModelPolicy, QUALITY_FIRST_POLICY


class CatalogError(RuntimeError):
    """Base class for durable catalog failures."""


class CatalogNotFound(CatalogError):
    pass


class CatalogConflict(CatalogError):
    pass


class _CatalogLock:
    """Re-entrant in-process lock backed by an inter-process file lock.

    A catalog instance caches parsed JSON, so atomic ``os.replace`` alone does not
    prevent two server processes from deriving candidates from stale snapshots and
    losing one writer. Every outer catalog operation takes the same byte lock and
    reloads the committed file before it reads or mutates state. Process termination
    releases the OS lock automatically.
    """

    def __init__(self, catalog: "ConfigurationCatalog") -> None:
        self._catalog = catalog
        self._thread_lock = threading.RLock()
        self._local = threading.local()
        self._pid = os.getpid()
        lock_path = catalog.path.with_name(f".{catalog.path.name}.lock")
        self._lock_path = lock_path
        self._stream = self._open_stream()

    def _open_stream(self):
        stream = self._lock_path.open("a+b", buffering=0)
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
        return stream

    def _refresh_after_fork(self) -> None:
        """Do not reuse an inherited flock open-file-description after fork."""

        pid = os.getpid()
        if pid == self._pid:
            return
        try:
            self._stream.close()
        except OSError:
            pass
        # Locks and thread-local depth can be inherited while held by a vanished
        # thread. A child process needs independent synchronization primitives.
        self._thread_lock = threading.RLock()
        self._local = threading.local()
        self._stream = self._open_stream()
        self._pid = pid

    def _acquire_file(self) -> None:
        self._stream.seek(0)
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(self._stream.fileno(), msvcrt.LK_NBLCK, 1)
                    return
                except OSError as exc:
                    # LK_NBLCK reports ordinary contention as EACCES/EAGAIN. Other
                    # failures (bad handle, read-only filesystem, etc.) must fail
                    # fast instead of wedging every catalog operation forever.
                    if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                        raise
                    time.sleep(0.01)
        else:
            import fcntl

            fcntl.flock(self._stream.fileno(), fcntl.LOCK_EX)

    def _release_file(self) -> None:
        self._stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(self._stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)

    def close(self) -> None:
        """Release the lifetime lock-file handle during graceful shutdown."""

        with self._thread_lock:
            if int(getattr(self._local, "depth", 0)):
                raise CatalogError("cannot close catalog while an operation is active")
            if not self._stream.closed:
                self._stream.close()

    def __enter__(self) -> "_CatalogLock":
        self._refresh_after_fork()
        self._thread_lock.acquire()
        depth = int(getattr(self._local, "depth", 0))
        file_acquired = False
        try:
            if depth == 0:
                self._acquire_file()
                file_acquired = True
                self._catalog._state = self._catalog._load()
            self._local.depth = depth + 1
            return self
        except BaseException:
            if file_acquired:
                try:
                    self._release_file()
                except OSError:
                    pass
            self._thread_lock.release()
            raise

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        depth = int(getattr(self._local, "depth", 1)) - 1
        self._local.depth = depth
        try:
            if depth == 0:
                self._release_file()
        finally:
            self._thread_lock.release()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _etag(spec: Mapping[str, Any], base_version: Optional[int]) -> str:
    return '"' + _hash({"spec": spec, "base_version": base_version}) + '"'


def _policy_spec(policy: ModelPolicy) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "policy_id": policy.policy_id,
        "require_verified": policy.require_verified,
        "allow_unknown_cost": policy.allow_unknown_cost,
        "allowed_providers": list(policy.allowed_providers),
        "allowed_models": list(policy.allowed_models),
        "blocked_models": list(policy.blocked_models),
        "fallback_limit": policy.fallback_limit,
        "fallback_for_explicit": policy.fallback_for_explicit,
    }


def _policy_from_spec(spec: Mapping[str, Any], *, version: int) -> ModelPolicy:
    return ModelPolicy(
        policy_id=str(spec["policy_id"]),
        version=version,
        require_verified=bool(spec.get("require_verified", True)),
        allow_unknown_cost=bool(spec.get("allow_unknown_cost", True)),
        allowed_providers=tuple(spec.get("allowed_providers", ())),
        allowed_models=tuple(spec.get("allowed_models", ())),
        blocked_models=tuple(spec.get("blocked_models", ())),
        fallback_limit=int(spec.get("fallback_limit", 2)),
        fallback_for_explicit=bool(spec.get("fallback_for_explicit", False)),
    )


class ConfigurationCatalog:
    """Atomic JSON catalog for immutable versions and optimistic drafts.

    Built-ins are code-owned and overlaid at read time, which keeps upstream defaults
    upgradeable without rewriting user state.  User-created versions and drafts live in
    one small atomic file; task/run state remains in the transactional SQLite store.
    """

    SCHEMA_VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load()
        self._lock = _CatalogLock(self)

    def close(self) -> None:
        self._lock.close()

    def _empty(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "profiles": {},
            "model_policies": {},
        }

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._empty()
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise CatalogError(f"cannot read orchestration catalog: {exc}") from exc
        if value.get("schema_version") != self.SCHEMA_VERSION:
            raise CatalogError("unsupported orchestration catalog schema")
        value.setdefault("profiles", {})
        value.setdefault("model_policies", {})
        return value

    def _save(self, state: Mapping[str, Any]) -> None:
        """Durably replace the catalog, then publish the candidate in memory.

        Callers always mutate a private deep copy and pass it here.  The in-memory
        pointer changes only after ``os.replace`` succeeds, so a short write, fsync
        failure, or failed replacement cannot leave readers observing state that was
        never committed to disk.  The directory fsync happens after that commit point;
        if it fails, both the visible file and memory still contain the candidate and a
        retry is safely resolved by the existing ETag checks.
        """

        temp = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temp.open("wb") as stream:
                stream.write(_canonical(state))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.path)
            # os.replace is the commit point.  Publish only after it succeeds; do so
            # before the directory fsync because that fsync can report uncertain
            # durability even though the replacement is already visible.
            self._state = dict(state)
            if os.name != "nt":
                directory_fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                pass

    # -- profiles ---------------------------------------------------------
    def _profile_entry(
        self,
        profile_id: str,
        *,
        create: bool = False,
        state: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        entries = (self._state if state is None else state)["profiles"]
        if create:
            return entries.setdefault(profile_id, {"versions": [], "draft": None})
        entry = entries.get(profile_id)
        if entry is None:
            raise CatalogNotFound(f"agent profile not found: {profile_id}")
        return entry

    def profile_versions(self, profile_id: str) -> tuple[AgentProfile, ...]:
        builtin = BUILTIN_PROFILES.get(profile_id)
        if builtin is not None:
            return (builtin,)
        with self._lock:
            entry = self._profile_entry(profile_id)
            return tuple(AgentProfile.from_dict(item) for item in entry["versions"])

    def resolve_profile(
        self, profile_id: str, version: Optional[int] = None
    ) -> AgentProfile:
        versions = self.profile_versions(profile_id)
        if not versions:
            raise CatalogNotFound(f"agent profile has no published version: {profile_id}")
        if version is None:
            return versions[-1]
        for profile in versions:
            if profile.version == version:
                return profile
        raise CatalogNotFound(f"agent profile version not found: {profile_id}@{version}")

    @staticmethod
    def validate_profile(spec: Mapping[str, Any]) -> dict[str, Any]:
        try:
            AgentProfileDraft.from_dict(spec)
        except (KeyError, TypeError, ValueError, ProfileValidationError) as exc:
            return {
                "valid": False,
                "errors": [{"code": "invalid_profile", "path": "spec", "message": str(exc)}],
                "warnings": [],
            }
        warnings: list[dict[str, Any]] = []
        if not spec.get("allowed_tools"):
            warnings.append(
                {
                    "code": "no_tools",
                    "path": "spec.allowed_tools",
                    "message": "This profile cannot call tools.",
                }
            )
        return {"valid": True, "errors": [], "warnings": warnings, "resolved": dict(spec)}

    def create_profile(self, spec: Mapping[str, Any]) -> dict[str, Any]:
        draft = AgentProfileDraft.from_dict(spec)
        with self._lock:
            if draft.profile_id in BUILTIN_PROFILES or draft.profile_id in self._state["profiles"]:
                raise CatalogConflict(f"agent profile already exists: {draft.profile_id}")
            candidate = copy.deepcopy(self._state)
            self._put_profile_draft(draft, base_version=None, state=candidate)
            self._save(candidate)
            return self.get_profile(draft.profile_id)

    def create_profile_draft(
        self, profile_id: str, *, base_version: Optional[int] = None
    ) -> dict[str, Any]:
        if profile_id in BUILTIN_PROFILES:
            raise CatalogConflict("builtin profiles are immutable; clone one to customize it")
        with self._lock:
            entry = self._profile_entry(profile_id)
            if entry.get("draft") is not None:
                return self.get_profile(profile_id)
            versions = tuple(AgentProfile.from_dict(v) for v in entry["versions"])
            if not versions:
                raise CatalogConflict("a profile without a version must already have a draft")
            source = versions[-1] if base_version is None else next(
                (item for item in versions if item.version == base_version), None
            )
            if source is None:
                raise CatalogNotFound(f"agent profile version not found: {profile_id}@{base_version}")
            draft = source.to_draft()
            candidate = copy.deepcopy(self._state)
            self._put_profile_draft(
                draft, base_version=source.version, state=candidate
            )
            self._save(candidate)
            return self.get_profile(profile_id)

    def save_profile_draft(
        self, profile_id: str, spec: Mapping[str, Any], *, expected_etag: str
    ) -> dict[str, Any]:
        draft = AgentProfileDraft.from_dict(spec)
        if draft.profile_id != profile_id:
            raise CatalogConflict("profile id in the path and draft must match")
        with self._lock:
            entry = self._profile_entry(profile_id)
            current = entry.get("draft")
            if current is None:
                raise CatalogConflict("profile has no draft")
            if current["etag"] != expected_etag:
                raise CatalogConflict("stale profile draft ETag")
            candidate = copy.deepcopy(self._state)
            self._put_profile_draft(
                draft,
                base_version=current.get("base_version"),
                state=candidate,
            )
            self._save(candidate)
            return self.get_profile(profile_id)

    def publish_profile(self, profile_id: str, *, expected_etag: str) -> dict[str, Any]:
        with self._lock:
            entry = self._profile_entry(profile_id)
            current = entry.get("draft")
            if current is None:
                raise CatalogConflict("profile has no draft")
            if current["etag"] != expected_etag:
                raise CatalogConflict("stale profile draft ETag")
            draft = AgentProfileDraft.from_dict(current["spec"])
            candidate = copy.deepcopy(self._state)
            candidate_entry = self._profile_entry(profile_id, state=candidate)
            next_version = len(candidate_entry["versions"]) + 1
            published = draft.publish(next_version)
            candidate_entry["versions"].append(published.to_dict())
            candidate_entry["draft"] = None
            candidate_entry["updated_at"] = _now()
            self._save(candidate)
            return self.get_profile(profile_id)

    def clone_profile(
        self, source_id: str, new_profile_id: str, *, overrides: Optional[Mapping[str, Any]] = None
    ) -> dict[str, Any]:
        source = self.resolve_profile(source_id)
        draft = clone_profile(source, new_profile_id, **dict(overrides or {}))
        return self.create_profile(draft.to_dict())

    def _put_profile_draft(
        self,
        draft: AgentProfileDraft,
        *,
        base_version: Optional[int],
        state: Optional[dict[str, Any]] = None,
    ) -> None:
        entry = self._profile_entry(draft.profile_id, create=True, state=state)
        spec = draft.to_dict()
        stamp = _now()
        entry["draft"] = {
            "spec": spec,
            "base_version": base_version,
            "etag": _etag(spec, base_version),
            "updated_at": stamp,
        }
        entry["updated_at"] = stamp

    def list_profiles(self) -> list[dict[str, Any]]:
        with self._lock:
            ids = set(BUILTIN_PROFILES) | set(self._state["profiles"])
            return [self._profile_summary(profile_id) for profile_id in sorted(ids)]

    def _profile_summary(self, profile_id: str) -> dict[str, Any]:
        versions = self.profile_versions(profile_id)
        current = versions[-1] if versions else None
        entry = self._state["profiles"].get(profile_id, {})
        metadata = dict(current.metadata) if current else dict((entry.get("draft") or {}).get("spec", {}).get("metadata", {}))
        return {
            "id": profile_id,
            "name": current.display_name if current else (entry.get("draft") or {}).get("spec", {}).get("display_name", profile_id),
            "description": str(metadata.get("description", "")),
            "role": current.role.value if current else None,
            "builtin": bool(current and current.builtin),
            "archived": False,
            "current_version": current.version if current else None,
            "has_draft": bool(entry.get("draft")),
            "updated_at": entry.get("updated_at"),
            "derived_from": (
                {**current.cloned_from.to_dict(), "content_hash": self.resolve_profile(current.cloned_from.profile_id, current.cloned_from.version).content_hash}
                if current and current.cloned_from else None
            ),
        }

    def get_profile(self, profile_id: str) -> dict[str, Any]:
        with self._lock:
            if profile_id not in BUILTIN_PROFILES and profile_id not in self._state["profiles"]:
                raise CatalogNotFound(f"agent profile not found: {profile_id}")
            versions = self.profile_versions(profile_id)
            entry = self._state["profiles"].get(profile_id, {})

            def version_item(profile: AgentProfile) -> dict[str, Any]:
                return {
                    "profile_id": profile.profile_id,
                    "version": profile.version,
                    "spec": profile.to_draft().to_dict(),
                    "content_hash": profile.content_hash,
                    "builtin": profile.builtin,
                    "cloned_from": profile.cloned_from.to_dict() if profile.cloned_from else None,
                }

            current = version_item(versions[-1]) if versions else None
            draft = entry.get("draft")
            return {
                **self._profile_summary(profile_id),
                "versions": [version_item(item) for item in versions],
                "current": current,
                "draft": (
                    {
                        "profile_id": profile_id,
                        "base_version": draft.get("base_version"),
                        "etag": draft["etag"],
                        "spec": draft["spec"],
                        "validation": self.validate_profile(draft["spec"]),
                        "updated_at": draft.get("updated_at"),
                    }
                    if draft else None
                ),
            }

    # -- model policies ---------------------------------------------------
    def _policy_entry(
        self,
        policy_id: str,
        *,
        create: bool = False,
        state: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        entries = (self._state if state is None else state)["model_policies"]
        if create:
            return entries.setdefault(policy_id, {"versions": [], "draft": None})
        entry = entries.get(policy_id)
        if entry is None:
            raise CatalogNotFound(f"model policy not found: {policy_id}")
        return entry

    def policy_versions(self, policy_id: str) -> tuple[ModelPolicy, ...]:
        if policy_id == QUALITY_FIRST_POLICY.policy_id:
            return (QUALITY_FIRST_POLICY,)
        with self._lock:
            entry = self._policy_entry(policy_id)
            return tuple(
                _policy_from_spec(item["spec"], version=int(item["version"]))
                for item in entry["versions"]
            )

    def resolve_policy(self, policy_id: str, version: Optional[int] = None) -> ModelPolicy:
        versions = self.policy_versions(policy_id)
        if not versions:
            raise CatalogNotFound(f"model policy has no published version: {policy_id}")
        if version is None:
            return versions[-1]
        for policy in versions:
            if policy.version == version:
                return policy
        raise CatalogNotFound(f"model policy version not found: {policy_id}@{version}")

    @staticmethod
    def validate_policy(spec: Mapping[str, Any]) -> dict[str, Any]:
        try:
            _policy_from_spec(spec, version=1)
        except (KeyError, TypeError, ValueError) as exc:
            return {
                "valid": False,
                "errors": [{"code": "invalid_policy", "path": "spec", "message": str(exc)}],
                "warnings": [],
            }
        return {"valid": True, "errors": [], "warnings": [], "resolved": dict(spec)}

    def create_policy(self, spec: Mapping[str, Any]) -> dict[str, Any]:
        policy = _policy_from_spec(spec, version=1)
        with self._lock:
            if policy.policy_id == QUALITY_FIRST_POLICY.policy_id or policy.policy_id in self._state["model_policies"]:
                raise CatalogConflict(f"model policy already exists: {policy.policy_id}")
            candidate = copy.deepcopy(self._state)
            self._put_policy_draft(
                _policy_spec(policy), base_version=None, state=candidate
            )
            self._save(candidate)
            return self.get_policy(policy.policy_id)

    def create_policy_draft(
        self, policy_id: str, *, base_version: Optional[int] = None
    ) -> dict[str, Any]:
        if policy_id == QUALITY_FIRST_POLICY.policy_id:
            raise CatalogConflict("builtin policies are immutable; clone one to customize it")
        with self._lock:
            entry = self._policy_entry(policy_id)
            if entry.get("draft") is not None:
                return self.get_policy(policy_id)
            versions = self.policy_versions(policy_id)
            source = versions[-1] if base_version is None else next(
                (item for item in versions if item.version == base_version), None
            )
            if source is None:
                raise CatalogNotFound(f"model policy version not found: {policy_id}@{base_version}")
            candidate = copy.deepcopy(self._state)
            self._put_policy_draft(
                _policy_spec(source), base_version=source.version, state=candidate
            )
            self._save(candidate)
            return self.get_policy(policy_id)

    def save_policy_draft(
        self, policy_id: str, spec: Mapping[str, Any], *, expected_etag: str
    ) -> dict[str, Any]:
        parsed = _policy_from_spec(spec, version=1)
        if parsed.policy_id != policy_id:
            raise CatalogConflict("policy id in the path and draft must match")
        with self._lock:
            entry = self._policy_entry(policy_id)
            current = entry.get("draft")
            if current is None or current["etag"] != expected_etag:
                raise CatalogConflict("stale or missing model-policy draft ETag")
            candidate = copy.deepcopy(self._state)
            self._put_policy_draft(
                _policy_spec(parsed),
                base_version=current.get("base_version"),
                state=candidate,
            )
            self._save(candidate)
            return self.get_policy(policy_id)

    def publish_policy(self, policy_id: str, *, expected_etag: str) -> dict[str, Any]:
        with self._lock:
            entry = self._policy_entry(policy_id)
            current = entry.get("draft")
            if current is None or current["etag"] != expected_etag:
                raise CatalogConflict("stale or missing model-policy draft ETag")
            candidate = copy.deepcopy(self._state)
            candidate_entry = self._policy_entry(policy_id, state=candidate)
            next_version = len(candidate_entry["versions"]) + 1
            policy = _policy_from_spec(current["spec"], version=next_version)
            candidate_entry["versions"].append(
                {
                    "version": next_version,
                    "spec": _policy_spec(policy),
                    "content_hash": _hash(_policy_spec(policy)),
                    "published_at": _now(),
                }
            )
            candidate_entry["draft"] = None
            candidate_entry["updated_at"] = _now()
            self._save(candidate)
            return self.get_policy(policy_id)

    def clone_policy(self, source_id: str, new_policy_id: str) -> dict[str, Any]:
        source = self.resolve_policy(source_id)
        spec = {**_policy_spec(source), "policy_id": new_policy_id}
        policy = _policy_from_spec(spec, version=1)
        with self._lock:
            if (
                policy.policy_id == QUALITY_FIRST_POLICY.policy_id
                or policy.policy_id in self._state["model_policies"]
            ):
                raise CatalogConflict(f"model policy already exists: {policy.policy_id}")
            candidate = copy.deepcopy(self._state)
            self._put_policy_draft(
                _policy_spec(policy), base_version=None, state=candidate
            )
            entry = self._policy_entry(new_policy_id, state=candidate)
            entry["derived_from"] = {
                "policy_id": source.policy_id,
                "version": source.version,
                "content_hash": _hash(_policy_spec(source)),
            }
            self._save(candidate)
        return self.get_policy(new_policy_id)

    def _put_policy_draft(
        self,
        spec: Mapping[str, Any],
        *,
        base_version: Optional[int],
        state: Optional[dict[str, Any]] = None,
    ) -> None:
        policy_id = str(spec["policy_id"])
        entry = self._policy_entry(policy_id, create=True, state=state)
        clean = dict(spec)
        stamp = _now()
        entry["draft"] = {
            "spec": clean,
            "base_version": base_version,
            "etag": _etag(clean, base_version),
            "updated_at": stamp,
        }
        entry["updated_at"] = stamp

    def list_policies(self) -> list[dict[str, Any]]:
        with self._lock:
            ids = {QUALITY_FIRST_POLICY.policy_id} | set(self._state["model_policies"])
            return [self._policy_summary(policy_id) for policy_id in sorted(ids)]

    def _policy_summary(self, policy_id: str) -> dict[str, Any]:
        versions = self.policy_versions(policy_id)
        current = versions[-1] if versions else None
        entry = self._state["model_policies"].get(policy_id, {})
        return {
            "id": policy_id,
            "name": "Quality first" if policy_id == QUALITY_FIRST_POLICY.policy_id else policy_id,
            "description": "Deterministic capability-safe quality-first routing." if policy_id == QUALITY_FIRST_POLICY.policy_id else "",
            "builtin": policy_id == QUALITY_FIRST_POLICY.policy_id,
            "archived": False,
            "current_version": current.version if current else None,
            "has_draft": bool(entry.get("draft")),
            "updated_at": entry.get("updated_at"),
            "derived_from": entry.get("derived_from"),
        }

    def get_policy(self, policy_id: str) -> dict[str, Any]:
        with self._lock:
            if policy_id != QUALITY_FIRST_POLICY.policy_id and policy_id not in self._state["model_policies"]:
                raise CatalogNotFound(f"model policy not found: {policy_id}")
            versions = self.policy_versions(policy_id)
            entry = self._state["model_policies"].get(policy_id, {})

            def version_item(policy: ModelPolicy) -> dict[str, Any]:
                spec = _policy_spec(policy)
                persisted = next(
                    (item for item in entry.get("versions", ()) if int(item["version"]) == policy.version),
                    {},
                )
                return {
                    "policy_id": policy.policy_id,
                    "version": policy.version,
                    "spec": spec,
                    "content_hash": persisted.get("content_hash") or _hash(spec),
                    "published_at": persisted.get("published_at"),
                }

            draft = entry.get("draft")
            items = [version_item(item) for item in versions]
            return {
                **self._policy_summary(policy_id),
                "versions": items,
                "current": items[-1] if items else None,
                "draft": (
                    {
                        "policy_id": policy_id,
                        "base_version": draft.get("base_version"),
                        "etag": draft["etag"],
                        "spec": draft["spec"],
                        "validation": self.validate_policy(draft["spec"]),
                        "updated_at": draft.get("updated_at"),
                    }
                    if draft else None
                ),
            }
