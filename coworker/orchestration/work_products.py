"""Immutable work-product lifecycle and acceptance-criterion linkage."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Optional

from .context import ContextRefResolver
from .handoff_models import WorkProductKind, WorkProductRecord
from .store import OrchestrationStore


class WorkProductService:
    def __init__(self, store: OrchestrationStore) -> None:
        self.store = store

    def create(
        self,
        task_id: str,
        *,
        kind: WorkProductKind | str,
        title: str,
        workspace: Optional[str | Path] = None,
        uri: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> WorkProductRecord:
        chosen_kind = WorkProductKind(kind)
        if chosen_kind is WorkProductKind.WORKSPACE_FILE:
            if workspace is None or not uri:
                raise ValueError("workspace_file products require a workspace URI")
            relative = str(uri).removeprefix("workspace:").lstrip("/")
            ContextRefResolver.canonical_workspace_path(workspace, relative)
        return self.store.create_work_product(
            task_id,
            kind=chosen_kind,
            title=title,
            uri=uri,
            metadata=metadata,
            **kwargs,
        )

    def list(self, task_id: str, **kwargs: Any) -> tuple[WorkProductRecord, ...]:
        return self.store.list_work_products(task_id, **kwargs)

    def verify(
        self,
        product_id: str,
        *,
        available: bool,
        actual_hash: Optional[str],
        actor: str,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        return self.store.verify_work_product(
            product_id,
            available=available,
            actual_hash=actual_hash,
            actor=actor,
            command_id=command_id,
        )
