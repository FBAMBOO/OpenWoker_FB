"""Snapshot-aware bounded read tools with shared query-cache accounting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from .query_cache import RepositoryQueryCache
from .repo_inventory import RepositoryInventoryService
from .repository_snapshot import RepositorySnapshotService


REPO_TOOL_VERSION = "snapshot-repo-tools@1"


@dataclass(slots=True)
class RepoToolMetrics:
    calls: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    bypasses: int = 0
    result_bytes: int = 0
    query_counts: dict[str, int] = field(default_factory=dict)

    @property
    def duplicate_non_cached_ratio(self) -> float:
        duplicates = sum(max(0, count - 1) for count in self.query_counts.values())
        return duplicates / self.calls if self.calls else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "calls": self.calls,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "bypasses": self.bypasses,
            "result_bytes": self.result_bytes,
            "duplicate_non_cached_ratio": self.duplicate_non_cached_ratio,
        }


class SnapshotRepoTools:
    def __init__(
        self,
        snapshots: RepositorySnapshotService,
        inventories: RepositoryInventoryService,
        cache: RepositoryQueryCache,
    ) -> None:
        self.snapshots = snapshots
        self.inventories = inventories
        self.cache = cache
        self.metrics = RepoToolMetrics()

    def git_snapshot_info(self, snapshot_id: str) -> dict[str, Any]:
        snapshot = self.snapshots.get(snapshot_id)
        return snapshot.model_dump(
            mode="json",
            exclude={"workspace_root", "repo_root"},
        )

    def read_snapshot_file(
        self,
        snapshot_id: str,
        path: str,
        *,
        start_line: int = 1,
        end_line: int | None = None,
    ) -> dict[str, Any]:
        return self.snapshots.read_file_lines(
            snapshot_id, path, start_line=start_line, end_line=end_line
        )

    def get_inventory(self, snapshot_id: str) -> dict[str, Any]:
        record = self.inventories.build(snapshot_id)
        _, value = self.inventories.get(record.id)
        return {"metadata": record.model_dump(mode="json"), "inventory": value}

    def search_snapshot(
        self,
        snapshot_id: str,
        query: str,
        *,
        paths: Iterable[str] = (),
        mode: str = "literal",
        limit: int = 1_000,
        bypass_cache: bool = False,
        bypass_reason: str | None = None,
    ) -> dict[str, Any]:
        if mode not in {"literal", "regex"}:
            raise ValueError("snapshot search mode must be literal or regex")
        if bypass_cache and not str(bypass_reason or "").strip():
            raise ValueError("cache bypass requires an auditable reason")
        snapshot = self.snapshots.get(snapshot_id)
        args: Mapping[str, Any] = {
            "query": query,
            "paths": sorted(set(str(item) for item in paths)),
            "mode": mode,
            "limit": int(limit),
        }
        result = self.cache.execute(
            task_id=snapshot.task_id,
            snapshot_id=snapshot_id,
            tool_name="search_snapshot",
            tool_version=REPO_TOOL_VERSION,
            args=args,
            operation=lambda: self.snapshots.search(
                snapshot_id,
                query,
                paths=args["paths"],
                regex=mode == "regex",
                limit=limit,
            ),
            bypass=bypass_cache,
        )
        self.metrics.calls += 1
        self.metrics.result_bytes += result.result_bytes
        if result.cache_hit:
            self.metrics.cache_hits += 1
        else:
            self.metrics.cache_misses += 1
            self.metrics.query_counts[result.query_key] = (
                self.metrics.query_counts.get(result.query_key, 0) + 1
            )
        if bypass_cache:
            self.metrics.bypasses += 1
        return {
            **dict(result.value),
            "query_key": result.query_key,
            "cache_hit": result.cache_hit,
            "result_artifact_id": result.artifact_id,
            "result_hash": result.result_hash,
        }
