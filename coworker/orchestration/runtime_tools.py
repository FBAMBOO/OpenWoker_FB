"""Run-bound tools for the Task-Centric Handoff Protocol.

The model never supplies task, run, lease, or fencing identities.  The factory
closes over the durable :class:`RunExecutionContext` and every mutation crosses a
store method which verifies that exact active lease inside its transaction.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Optional

from .communications import post_comment_with_externalization
from .context import ContextRefResolver
from .handoff_models import (
    ContextRefType,
    ContextRequirement,
    ExecutionEnvelope,
    TaskRelationType,
    WorkProductKind,
    jsonable,
)
from .store import OrchestrationStore


DelegateCallback = Callable[[dict[str, Any]], Mapping[str, Any]]
ProfileResolver = Callable[[str], Any]


class HandoffToolFactory:
    """Build the bounded communication tool set for one leased run."""

    def __init__(
        self,
        store: OrchestrationStore,
        resolver: ContextRefResolver,
        *,
        delegate: Optional[DelegateCallback] = None,
        metrics: Optional[Any] = None,
        profile_resolver: Optional[ProfileResolver] = None,
        wake_coalesce_window_ms: int = 1_000,
    ) -> None:
        self.store = store
        self.resolver = resolver
        self.delegate = delegate
        self.metrics = metrics
        self.profile_resolver = profile_resolver
        self.wake_coalesce_window_ms = max(0, int(wake_coalesce_window_ms))

    def build(
        self,
        context: Any,
        report: dict[str, Any],
    ) -> list[Callable[..., Any]]:
        run = context.claim.run
        lease = context.claim.lease
        task = context.task
        policy = context.profile.communication_policy

        def identity() -> dict[str, Any]:
            return {
                "task_id": task.id,
                "run_id": run.id,
                "node_id": context.node.id,
                "parent_runtime_id": context.parent_runtime_id,
                "workspace": str(context.workspace) if context.workspace else None,
                "lease_token": lease.token,
                "fencing_token": lease.fencing_token,
            }

        def get_task_context() -> dict[str, Any]:
            """Return the published Brief and compact handoff state; never raw files."""

            envelope = context.execution_envelope
            if not isinstance(envelope, ExecutionEnvelope):
                raise RuntimeError("execution envelope is unavailable")
            relations = self.store.list_relations(task.id)
            products = self.store.list_work_products(task.id, limit=100)
            comments = self.store.list_task_comments(task.id, after_sequence=0, limit=1_000)
            return {
                **envelope.to_dict(),
                "brief": context.brief.to_dict(),
                "relations": [jsonable(item) for item in relations],
                "comments": {
                    "latest_sequence": comments[-1].sequence if comments else 0,
                    "count": len(comments),
                    "content_included": False,
                },
                "work_products": [jsonable(item) for item in products],
            }

        def list_context_refs(
            requirement: Optional[str] = None,
            ref_type: Optional[str] = None,
        ) -> list[dict[str, Any]]:
            """List context metadata without resolving referenced content."""

            required = ContextRequirement(requirement) if requirement else None
            chosen_type = ContextRefType(ref_type) if ref_type else None
            rows = self.store.list_context_refs(task.id, brief_id=context.brief.id)
            return [
                item.to_dict()
                for item in rows
                if (required is None or item.requirement is required)
                and (chosen_type is None or item.ref_type is chosen_type)
            ]

        def read_context_ref(
            ref_id: str,
            start_line: Optional[int] = None,
            end_line: Optional[int] = None,
        ) -> dict[str, Any]:
            """Resolve one authorized reference and append a context-read audit event."""

            selected = self.store.get_context_ref(str(ref_id))
            if selected.ref_type not in policy.allowed_context_ref_types:
                raise PermissionError(
                    f"context ref type is not permitted: {selected.ref_type.value}"
                )
            result = self.resolver.read(
                str(ref_id),
                task_id=task.id,
                run_id=run.id,
                workspace=context.workspace,
                start_line=start_line,
                end_line=end_line,
            )
            if self.metrics is not None:
                self.metrics.increment("orchestration_context_reads_total")
                self.metrics.increment(
                    "orchestration_context_bytes_read_total",
                    int(result.get("byte_size") or 0),
                )
            return result

        def delegate_task(
            operation_id: str,
            role: str,
            brief: dict[str, Any],
            context_refs: Optional[list[dict[str, Any]]] = None,
            blocked_by_task_ids: Optional[list[str]] = None,
            priority: int = 0,
            runtime_preset_id: Optional[str] = None,
        ) -> dict[str, Any]:
            """Atomically create a child, published Brief, refs, relations and wake."""

            if not policy.can_delegate:
                raise PermissionError("this profile cannot delegate tasks")
            chosen_role = str(role).strip().lower()
            allowed = {item.value for item in policy.allowed_child_roles}
            if chosen_role not in allowed:
                raise PermissionError(f"child role is not allowed: {chosen_role}")
            if self.delegate is None:
                raise RuntimeError("structured delegation is unavailable")
            return dict(
                self.delegate(
                    {
                        **identity(),
                        "operation_id": operation_id,
                        "role": chosen_role,
                        "brief": dict(brief),
                        "context_refs": list(context_refs or ()),
                        "blocked_by_task_ids": list(blocked_by_task_ids or ()),
                        "priority": int(priority),
                        "runtime_preset_id": runtime_preset_id,
                    }
                )
            )

        def post_task_comment(
            status_line: str,
            changed: Optional[list[str]] = None,
            remaining: Optional[list[str]] = None,
            blocker: Optional[dict[str, Any]] = None,
            mentions: Optional[list[str]] = None,
        ) -> dict[str, Any]:
            """Post a concise progress delta; a comment never changes ownership."""

            if not policy.can_comment:
                raise PermissionError("this profile cannot comment")
            chosen_mentions = list(
                dict.fromkeys(
                    str(item).strip()
                    for item in (mentions or ())
                    if str(item).strip()
                )
            )
            if chosen_mentions and not policy.can_mention:
                raise PermissionError("this profile cannot mention other roles")
            if self.profile_resolver is not None:
                for target in chosen_mentions:
                    if target.startswith("task:"):
                        continue
                    target_profile = self.profile_resolver(target)
                    if not target_profile.communication_policy.can_mention_receive:
                        raise PermissionError(
                            f"profile cannot receive mentions: {target_profile.profile_id}"
                        )
            changed_items = [str(item) for item in (changed or ()) if str(item).strip()]
            remaining_items = [str(item) for item in (remaining or ()) if str(item).strip()]
            sections = [str(status_line).strip()]
            if changed_items:
                sections.append("Completed:\n" + "\n".join(f"- {item}" for item in changed_items))
            if remaining_items:
                sections.append("Remaining:\n" + "\n".join(f"- {item}" for item in remaining_items))
            if blocker:
                sections.append("Blocker:\n" + str(dict(blocker)))
            comment = post_comment_with_externalization(
                self.store,
                self.resolver.blob_store,
                task.id,
                "\n\n".join(item for item in sections if item),
                author_type="agent",
                author_id=context.profile.profile_id,
                metadata={
                    "status_line": str(status_line).strip(),
                    "changed": changed_items,
                    "remaining": remaining_items,
                    "blocker": dict(blocker or {}),
                    "mentions": chosen_mentions,
                },
                created_by_run_id=run.id,
                lease_token=lease.token,
                fencing_token=lease.fencing_token,
                wake_owner=False,
                wake_coalesce_window_ms=self.wake_coalesce_window_ms,
            )
            if self.metrics is not None:
                self.metrics.increment("orchestration_comments_total")
            return jsonable(comment)

        def list_task_comments(after_sequence: Optional[int] = None) -> dict[str, Any]:
            """Read an ordered comment delta, bounded to one hundred entries."""

            rows = self.store.list_task_comments(
                task.id,
                after_sequence=max(0, int(after_sequence or 0)),
                limit=101,
            )
            visible = rows[:100]
            return {
                "comments": [jsonable(item) for item in visible],
                "latest_sequence": visible[-1].sequence if visible else int(after_sequence or 0),
                "fallback_fetch_needed": len(rows) > 100,
            }

        def add_task_blockers(
            task_ids: list[str],
            reason: str,
            owner: str,
            required_action: str,
        ) -> dict[str, Any]:
            """Replace this task's blocker set using run-fenced relation writes."""

            if TaskRelationType.BLOCKS not in policy.allowed_relation_types:
                raise PermissionError("this profile cannot manage blocker relations")
            rows = self.store.replace_blockers(
                task.id,
                task_ids,
                reason=reason,
                owner=owner,
                required_action=required_action,
                created_by_task_id=task.id,
                created_by_run_id=run.id,
                lease_token=lease.token,
                fencing_token=lease.fencing_token,
            )
            return {"ok": True, "relations": [jsonable(item) for item in rows]}

        def remove_task_blocker(task_id: str) -> dict[str, Any]:
            """Remove one blocker while retaining all other current blockers."""

            current = self.store.list_relations(
                task.id, relation_type=TaskRelationType.BLOCKS
            )
            blockers = [
                item.from_task_id
                for item in current
                if item.to_task_id == task.id and item.from_task_id != str(task_id)
            ]
            rows = self.store.replace_blockers(
                task.id,
                blockers,
                reason="blocker removed by assigned Agent",
                owner="",
                required_action="continue when all blockers are resolved",
                created_by_task_id=task.id,
                created_by_run_id=run.id,
                lease_token=lease.token,
                fencing_token=lease.fencing_token,
            )
            return {"ok": True, "relations": [jsonable(item) for item in rows]}

        def create_work_product(
            kind: str,
            title: str,
            summary: str = "",
            uri: Optional[str] = None,
            content_hash: Optional[str] = None,
            deliverable_id: Optional[str] = None,
            metadata: Optional[dict[str, Any]] = None,
        ) -> dict[str, Any]:
            """Persist one immutable, run-owned work-product reference."""

            product = self.store.create_work_product(
                task.id,
                kind=WorkProductKind(kind),
                title=title,
                summary=summary,
                run_id=run.id,
                uri=uri,
                content_hash=content_hash,
                metadata={**dict(metadata or {}), "deliverable_id": deliverable_id},
                created_by=context.profile.profile_id,
                lease_token=lease.token,
                fencing_token=lease.fencing_token,
            )
            if self.metrics is not None:
                self.metrics.increment("orchestration_work_products_total")
            return jsonable(product)

        def complete_task(
            summary: str,
            work_products: list[dict[str, Any]],
            criterion_results: dict[str, str],
            remaining_risks: list[str],
            follow_up_task_ids: Optional[list[str]] = None,
        ) -> dict[str, Any]:
            """Submit the structured result that must pass server-side settlement."""

            normalized_summary = str(summary).strip()
            if not normalized_summary:
                raise ValueError("completion summary is required")
            if report.get("failure"):
                raise ValueError("fail_task was already submitted for this run")
            report["completion"] = {
                "summary": normalized_summary,
                "work_products": [dict(item) for item in work_products],
                "criterion_results": {
                    str(key): str(value).strip().lower()
                    for key, value in dict(criterion_results).items()
                },
                "remaining_risks": [str(item) for item in remaining_risks if str(item).strip()],
                "follow_up_task_ids": [str(item) for item in (follow_up_task_ids or ()) if str(item).strip()],
            }
            return {"ok": True, "accepted_for_settlement": True}

        def fail_task(
            error_kind: str,
            message: str,
            retryable: bool = False,
        ) -> dict[str, Any]:
            """Submit a structured failed outcome for the current leased run."""

            if report.get("completion"):
                raise ValueError("complete_task was already submitted for this run")
            if not str(error_kind).strip() or not str(message).strip():
                raise ValueError("error_kind and message are required")
            report["failure"] = {
                "error_kind": str(error_kind).strip()[:200],
                "message": str(message).strip()[:8_000],
                "retryable": bool(retryable),
            }
            return {"ok": True, "accepted_for_settlement": True}

        return [
            get_task_context,
            list_context_refs,
            read_context_ref,
            delegate_task,
            post_task_comment,
            list_task_comments,
            add_task_blockers,
            remove_task_blocker,
            create_work_product,
            complete_task,
            fail_task,
        ]
