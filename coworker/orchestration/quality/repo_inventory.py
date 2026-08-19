"""One immutable repository inventory shared by every evidence collector."""

from __future__ import annotations

import json
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from ..errors import NotFoundError
from ..store import OrchestrationStore
from .adapters.dbt_static import analyze_dbt_static
from .artifacts import ArtifactService
from .models import RepositoryInventory
from .repository_snapshot import RepositorySnapshotService


INVENTORY_TOOL_VERSION = "repository-inventory@1"


class RepositoryInventoryService:
    def __init__(
        self,
        store: OrchestrationStore,
        artifacts: ArtifactService,
        snapshots: RepositorySnapshotService,
    ) -> None:
        self.store = store
        self.artifacts = artifacts
        self.snapshots = snapshots

    def build(self, snapshot_id: str) -> RepositoryInventory:
        snapshot = self.snapshots.get(snapshot_id)
        with self.store._read() as connection:
            row = connection.execute(
                """
                SELECT * FROM orch_repository_inventories
                WHERE snapshot_id = ? AND tool_version = ?
                ORDER BY generated_at DESC LIMIT 1
                """,
                (snapshot_id, INVENTORY_TOOL_VERSION),
            ).fetchone()
        if row is not None:
            return self._record(row)
        manifest = self.snapshots.manifest(snapshot_id)
        entries = tuple(manifest.get("entries", ()))
        paths = tuple(str(item["path"]) for item in entries)
        extensions = Counter(PurePosixPath(path).suffix.casefold() or "<none>" for path in paths)
        top_directories = Counter(
            PurePosixPath(path).parts[0] if len(PurePosixPath(path).parts) > 1 else "."
            for path in paths
        )
        total_bytes = sum(int(item.get("size") or 0) for item in entries)
        dbt = analyze_dbt_static(
            paths,
            project_root=snapshot.project_root,
            read_text=lambda path: self.snapshots.read_file(snapshot_id, path).decode("utf-8"),
        )
        value: dict[str, Any] = {
            "schema_id": "repository_inventory_v2",
            "schema_version": 2,
            "snapshot_id": snapshot_id,
            "tool_version": INVENTORY_TOOL_VERSION,
            "file_count": len(paths),
            "total_bytes": total_bytes,
            "extensions": dict(sorted(extensions.items())),
            "top_directories": dict(sorted(top_directories.items())),
            "project_markers": [
                path
                for path in paths
                if PurePosixPath(path).name
                in {"dbt_project.yml", "pyproject.toml", "package.json", "Cargo.toml", "go.mod"}
            ],
            "dbt_static": dbt,
            "generated_vendor_policy": {
                "manifest_is_authoritative": True,
                "ignored_content_was_frozen_by_snapshot_policy": True,
            },
        }
        inventory_id = f"inventory_{uuid.uuid4().hex}"
        artifact = self.artifacts.store_internal_json(
            task_id=snapshot.task_id,
            logical_deliverable_id=f"system-inventory-{snapshot_id}",
            filename=f"repository_inventory_{snapshot_id}.json",
            value=value,
        )
        generated_at = datetime.now(timezone.utc)
        with self.store._write() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_repository_inventories(
                    id, snapshot_id, tool_version, artifact_id, content_hash,
                    file_count, total_bytes, project_markers_json, generated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    inventory_id,
                    snapshot_id,
                    INVENTORY_TOOL_VERSION,
                    artifact.id,
                    artifact.sha256,
                    len(paths),
                    total_bytes,
                    json.dumps(value["project_markers"]),
                    generated_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            row = connection.execute(
                """
                SELECT * FROM orch_repository_inventories
                WHERE snapshot_id = ? AND tool_version = ?
                ORDER BY generated_at LIMIT 1
                """,
                (snapshot_id, INVENTORY_TOOL_VERSION),
            ).fetchone()
        return self._record(row)

    @staticmethod
    def _record(row) -> RepositoryInventory:
        return RepositoryInventory(
            id=row["id"],
            snapshot_id=row["snapshot_id"],
            tool_version=row["tool_version"],
            artifact_id=row["artifact_id"],
            content_hash=row["content_hash"],
            file_count=row["file_count"],
            total_bytes=row["total_bytes"],
            project_markers=tuple(json.loads(row["project_markers_json"])),
            generated_at=row["generated_at"],
        )

    def get(self, inventory_id: str) -> tuple[RepositoryInventory, dict[str, Any]]:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_repository_inventories WHERE id = ?", (inventory_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"repository inventory {inventory_id} not found")
        record = self._record(row)
        artifact = self.artifacts.get(record.artifact_id)
        if artifact.sha256 != record.content_hash or artifact.blob_uri is None:
            raise ValueError("repository inventory artifact hash mismatch")
        value = json.loads(self.artifacts.blobs.get(artifact.blob_uri).decode("utf-8"))
        return record, value
