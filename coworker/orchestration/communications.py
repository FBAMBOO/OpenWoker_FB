"""Incremental task comments; comments never confer assignment or checkout."""

from __future__ import annotations

import hashlib
from typing import Any, Mapping, Optional

from .blobs import ContentAddressedBlobStore
from .handoff_models import (
    TaskCommentRecord,
    WorkProductKind,
    contains_secret_like,
)
from .store import OrchestrationStore


MAX_INLINE_COMMENT_BYTES = 65_536


def post_comment_with_externalization(
    store: OrchestrationStore,
    blob_store: Optional[ContentAddressedBlobStore],
    task_id: str,
    body_markdown: str,
    *,
    author_type: str,
    author_id: str,
    metadata: Optional[Mapping[str, Any]] = None,
    command_id: Optional[str] = None,
    **kwargs: Any,
) -> TaskCommentRecord:
    """Post a comment, externalizing oversized bodies as immutable artifacts."""

    body = str(body_markdown)
    encoded = body.encode("utf-8")
    if len(encoded) <= MAX_INLINE_COMMENT_BYTES:
        return store.post_task_comment(
            task_id,
            body,
            author_type=author_type,
            author_id=author_id,
            metadata=metadata,
            command_id=command_id,
            **kwargs,
        )
    if blob_store is None:
        raise ValueError(
            "comment exceeds 65536 bytes and no artifact store is configured"
        )
    if contains_secret_like({"body": body, "metadata": dict(metadata or {})}):
        raise ValueError(
            "comment cannot contain secret-like values; use the runtime secret mechanism"
        )

    digest = hashlib.sha256(encoded).hexdigest()
    operation = command_id or (
        f"large-comment:{task_id}:{author_type}:{author_id}:"
        f"{kwargs.get('created_by_run_id') or 'operator'}:{digest}"
    )
    blob = blob_store.put(encoded, mime_type="text/markdown")
    product = store.create_work_product(
        task_id,
        kind=WorkProductKind.ARTIFACT,
        title="Large task comment attachment",
        summary="Oversized Markdown comment body externalized for on-demand access.",
        run_id=kwargs.get("created_by_run_id"),
        artifact_id=blob.uri,
        uri=blob.uri,
        content_hash=f"sha256:{digest}",
        metadata={
            "source": "large_task_comment",
            "byte_size": len(encoded),
            "mime_type": "text/markdown",
        },
        created_by=author_id,
        lease_token=kwargs.get("lease_token"),
        fencing_token=kwargs.get("fencing_token"),
        command_id=f"{operation}:artifact",
    )
    chosen_metadata = {
        **dict(metadata or {}),
        "externalized_body": {
            "work_product_id": product.id,
            "artifact_id": blob.uri,
            "content_hash": f"sha256:{digest}",
            "byte_size": len(encoded),
            "mime_type": "text/markdown",
        },
    }
    return store.post_task_comment(
        task_id,
        (
            "Large comment body externalized as work product "
            f"{product.id} ({len(encoded)} bytes)."
        ),
        author_type=author_type,
        author_id=author_id,
        metadata=chosen_metadata,
        command_id=f"{operation}:comment",
        **kwargs,
    )


class TaskCommunicationService:
    def __init__(
        self,
        store: OrchestrationStore,
        *,
        blob_store: Optional[ContentAddressedBlobStore] = None,
        max_batch: int = 100,
        wake_coalesce_window_ms: int = 1_000,
    ) -> None:
        self.store = store
        self.blob_store = blob_store
        self.max_batch = max(1, min(int(max_batch), 1_000))
        self.wake_coalesce_window_ms = max(0, int(wake_coalesce_window_ms))

    def post_comment(
        self,
        task_id: str,
        body_markdown: str,
        *,
        author_type: str,
        author_id: str,
        metadata: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> TaskCommentRecord:
        kwargs.setdefault(
            "wake_coalesce_window_ms", self.wake_coalesce_window_ms
        )
        return post_comment_with_externalization(
            self.store,
            self.blob_store,
            task_id,
            body_markdown,
            author_type=author_type,
            author_id=author_id,
            metadata=metadata,
            **kwargs,
        )

    def delta(
        self, task_id: str, *, after_sequence: int = 0
    ) -> dict[str, Any]:
        rows = self.store.list_task_comments(
            task_id, after_sequence=after_sequence, limit=self.max_batch + 1
        )
        visible = rows[: self.max_batch]
        latest = visible[-1].sequence if visible else after_sequence
        return {
            "task_id": task_id,
            "after_sequence": int(after_sequence),
            "latest_sequence": latest,
            "new_count": len(visible),
            "comments": visible,
            "fallback_fetch_needed": len(rows) > self.max_batch,
        }
