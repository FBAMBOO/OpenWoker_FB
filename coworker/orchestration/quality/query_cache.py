"""Snapshot-scoped normalized repository query cache with bounded result metadata."""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from ..errors import NotFoundError
from ..store import OrchestrationStore
from .artifacts import ArtifactService
from .models import canonical_json


def normalized_query_key(
    *,
    snapshot_id: str,
    tool_name: str,
    tool_version: str,
    args: Mapping[str, Any],
) -> tuple[str, str]:
    normalized_args_hash = "sha256:" + hashlib.sha256(canonical_json(dict(args))).hexdigest()
    body = {
        "snapshot_id": str(snapshot_id),
        "tool_name": str(tool_name),
        "tool_version": str(tool_version),
        "normalized_args_hash": normalized_args_hash,
    }
    return (
        "sha256:" + hashlib.sha256(canonical_json(body)).hexdigest(),
        normalized_args_hash,
    )


@dataclass(frozen=True, slots=True)
class CachedQueryResult:
    query_key: str
    value: Mapping[str, Any]
    artifact_id: str
    result_hash: str
    result_bytes: int
    complete: bool
    continuation: Any
    cache_hit: bool
    hit_count: int


class RepositoryQueryCache:
    def __init__(self, store: OrchestrationStore, artifacts: ArtifactService) -> None:
        self.store = store
        self.artifacts = artifacts
        self._lock_guard = threading.Lock()
        self._query_locks: dict[str, threading.Lock] = {}

    def get(self, query_key: str, *, touch: bool = True) -> CachedQueryResult | None:
        with self.store._write() if touch else self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_repo_query_cache WHERE query_key = ?", (query_key,)
            ).fetchone()
            if row is None:
                return None
            if touch:
                connection.execute(
                    """
                    UPDATE orch_repo_query_cache
                    SET hit_count = hit_count + 1, last_accessed_at = ?
                    WHERE query_key = ?
                    """,
                    (
                        datetime.now(timezone.utc)
                        .isoformat(timespec="microseconds")
                        .replace("+00:00", "Z"),
                        query_key,
                    ),
                )
                hit_count = int(row["hit_count"]) + 1
            else:
                hit_count = int(row["hit_count"])
        artifact = self.artifacts.get(row["result_artifact_id"])
        if artifact.sha256 != row["result_hash"] or artifact.blob_uri is None:
            raise ValueError("cached repository query artifact failed integrity validation")
        raw = self.artifacts.blobs.get(artifact.blob_uri)
        if len(raw) != row["result_bytes"]:
            raise ValueError("cached repository query byte count mismatch")
        value = json.loads(raw.decode("utf-8"))
        return CachedQueryResult(
            query_key=query_key,
            value=value,
            artifact_id=artifact.id,
            result_hash=str(artifact.sha256),
            result_bytes=len(raw),
            complete=bool(row["complete"]),
            continuation=json.loads(row["continuation"]) if row["continuation"] else None,
            cache_hit=touch,
            hit_count=hit_count,
        )

    def execute(
        self,
        *,
        task_id: str,
        snapshot_id: str,
        tool_name: str,
        tool_version: str,
        args: Mapping[str, Any],
        operation: Callable[[], Mapping[str, Any]],
        sensitivity: str = "repository",
        bypass: bool = False,
    ) -> CachedQueryResult:
        query_key, args_hash = normalized_query_key(
            snapshot_id=snapshot_id,
            tool_name=tool_name,
            tool_version=tool_version,
            args=args,
        )
        if not bypass:
            cached = self.get(query_key)
            if cached is not None:
                return cached
        query_lock: threading.Lock | None = None
        if not bypass:
            with self._lock_guard:
                query_lock = self._query_locks.setdefault(query_key, threading.Lock())
            query_lock.acquire()
            cached = self.get(query_key)
            if cached is not None:
                query_lock.release()
                return cached
        try:
            value = dict(operation())
            complete = bool(value.get("complete", True))
            continuation = value.get("continuation")
            artifact = self.artifacts.store_internal_json(
                task_id=task_id,
                logical_deliverable_id=f"system-query-{query_key.removeprefix('sha256:')}",
                filename=f"query_{query_key.removeprefix('sha256:')}.json",
                value=value,
            )
            now = (
                datetime.now(timezone.utc)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z")
            )
            with self.store._write() as connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO orch_repo_query_cache(
                        query_key, snapshot_id, tool_name, tool_version,
                        normalized_args_hash, result_artifact_id, result_hash,
                        result_bytes, complete, continuation, sensitivity,
                        created_at, last_accessed_at, hit_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        query_key,
                        snapshot_id,
                        tool_name,
                        tool_version,
                        args_hash,
                        artifact.id,
                        artifact.sha256,
                        artifact.byte_size,
                        int(complete),
                        json.dumps(continuation, sort_keys=True)
                        if continuation is not None
                        else None,
                        sensitivity,
                        now,
                        now,
                    ),
                )
                winner = connection.execute(
                    "SELECT * FROM orch_repo_query_cache WHERE query_key = ?",
                    (query_key,),
                ).fetchone()
            if winner is None:
                raise NotFoundError("repository query cache write disappeared")
            if winner["result_artifact_id"] != artifact.id:
                # A concurrent process won. Return exactly its authoritative row.
                selected = self.get(query_key, touch=False)
                if selected is None:
                    raise NotFoundError("repository query cache winner disappeared")
                return selected
            return CachedQueryResult(
                query_key=query_key,
                value=value,
                artifact_id=artifact.id,
                result_hash=str(artifact.sha256),
                result_bytes=int(artifact.byte_size or 0),
                complete=complete,
                continuation=continuation,
                cache_hit=False,
                hit_count=0,
            )
        finally:
            if query_lock is not None:
                query_lock.release()
