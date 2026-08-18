"""Compact execution-envelope construction and deterministic prompt rendering."""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional, Sequence

from .context import ContextBudgetCalculator
from .handoff_models import (
    ExecutionEnvelope,
    TaskBriefRecord,
    WakeRequestRecord,
)
from .models import NodeRecord, RunClaim, TaskRecord
from .profiles import AgentProfile
from .routing import RoutingDecision


DEFAULT_INITIAL_PROMPT_BYTES = 32 * 1024
HARD_INITIAL_PROMPT_BYTES = 64 * 1024
WORK_PRODUCT_PROMPT_BYTES = 10 * 1024


def _clip_utf8(value: Any, max_bytes: int) -> str:
    """Return a deterministic UTF-8-safe prefix with an explicit omission marker."""

    text = str(value or "")
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    marker = "…[truncated]"
    budget = max(0, max_bytes - len(marker.encode("utf-8")))
    prefix = raw[:budget].decode("utf-8", errors="ignore")
    return prefix + marker


def _bounded_lines(
    items: Sequence[Any],
    formatter: Any,
    *,
    max_bytes: int,
    empty: str,
    max_item_bytes: int = 1_024,
) -> str:
    if not items:
        return empty
    lines: list[str] = []
    used = 0
    for index, item in enumerate(items):
        line = _clip_utf8(formatter(item), min(max_item_bytes, max_bytes))
        cost = len((line + "\n").encode("utf-8"))
        if used + cost > max_bytes:
            remaining = len(items) - index
            marker = f"- … {remaining} more omitted from the initial prompt; use the handoff tools."
            marker_cost = len((marker + "\n").encode("utf-8"))
            while lines and used + marker_cost > max_bytes:
                removed = lines.pop()
                used -= len((removed + "\n").encode("utf-8"))
                remaining += 1
                marker = f"- … {remaining} more omitted from the initial prompt; use the handoff tools."
                marker_cost = len((marker + "\n").encode("utf-8"))
            if marker_cost <= max_bytes:
                lines.append(marker)
            break
        lines.append(line)
        used += cost
    return "\n".join(lines)


def build_execution_envelope(
    *,
    task: TaskRecord,
    brief: TaskBriefRecord,
    claim: RunClaim,
    node: NodeRecord,
    profile: AgentProfile,
    routing: RoutingDecision,
    context_refs: Sequence[Any],
    work_products: Sequence[Any] = (),
    wake: Optional[WakeRequestRecord] = None,
    workspace_id: Optional[str] = None,
    effective_tools: Sequence[str] = (),
) -> ExecutionEnvelope:
    manifest = ContextBudgetCalculator.manifest(context_refs)
    manifest.update(
        {
            "refs": [
                {
                    "id": item.id,
                    "requirement": item.requirement.value,
                    "ref_type": item.ref_type.value,
                    "display_name": item.display_name,
                    "summary": item.summary,
                    "selection_reason": item.selection_reason,
                    "delivery_mode": item.delivery_mode.value,
                    "token_estimate": item.token_estimate,
                }
                for item in context_refs
            ],
            # Work Products are the explicit cross-role result channel.  Include
            # bounded immutable summaries in the envelope so runtimes that cannot
            # host OpenWorker callback tools (notably Claude Code CLI) can still
            # inspect the selected candidate instead of falling back to a private
            # transcript or an unrelated workspace scan.
            "work_products": [
                {
                    "id": item.id,
                    "run_id": item.run_id,
                    "kind": item.kind.value,
                    "title": item.title,
                    "summary": item.summary,
                    "content_hash": item.content_hash,
                    "verification_status": item.verification_status,
                    "artifact_available": bool(item.artifact_id or item.uri),
                }
                for item in work_products
            ],
            "work_product_count": len(work_products),
        }
    )
    criteria = [
        {
            "id": str(item.get("id") or ""),
            "text": str(item.get("text") or ""),
            "required": bool(item.get("required", True)),
        }
        for item in brief.acceptance_criteria
    ]
    deliverables = [
        {
            "id": str(item.get("id") or ""),
            "kind": str(item.get("kind") or "other"),
            "title": str(item.get("title") or item.get("kind") or "deliverable"),
            "required": bool(item.get("required", True)),
        }
        for item in brief.deliverables
    ]
    wake_payload = dict(wake.payload) if wake else {}
    return ExecutionEnvelope(
        schema_version=1,
        dispatch_id=wake.id if wake else None,
        wake={
            "reason": wake.reason.value if wake else "assignment",
            "source_task_id": wake.source_task_id if wake else task.parent_task_id,
            "source_event_id": wake.source_event_id if wake else None,
            "comment_ids": list(wake_payload.get("comment_ids") or ()),
            "response_delta": dict(wake_payload.get("response_delta") or {}),
            "child_results": list(wake_payload.get("children") or ()),
            "fallback_fetch_needed": bool(wake_payload.get("fallback_fetch_needed")),
        },
        task={
            "id": task.id,
            "run_id": claim.run.id,
            "parent_task_id": task.parent_task_id,
            "title": task.title,
            "status": task.status.value,
            "stage": task.current_stage.value,
            "priority": task.priority,
            "node_key": node.key,
            "node_title": node.title,
            "node_kind": node.kind.value,
            "assignment": node.instructions or brief.objective,
        },
        brief={
            "id": brief.id,
            "revision": brief.revision,
            "content_hash": brief.content_hash,
            "objective": brief.objective,
            "acceptance_criteria": criteria,
            "required_deliverables": deliverables,
        },
        assignment={
            "profile_id": profile.profile_id,
            "profile_version": profile.version,
            "model": routing.selected_model,
            "workspace_id": workspace_id,
        },
        context_manifest=manifest,
        capability_contract={
            "tools": sorted(set(str(item) for item in effective_tools)),
            "skills": ["orchestration-handoff"],
            "write_scope": "none" if bool(task.policy.get("read_only")) else "task_workspace",
        },
        result_contract=dict(brief.result_contract),
        trace={
            "correlation_id": task.id,
            "causation_id": wake.source_event_id if wake else None,
        },
    )


def render_initial_user_prompt(envelope: ExecutionEnvelope) -> str:
    data = envelope.to_dict()
    wake = data["wake"]
    task = data["task"]
    brief = data["brief"]
    manifest = data["context_manifest"]
    criterion_items = list(brief.get("acceptance_criteria", ()))
    criteria = _bounded_lines(
        criterion_items,
        lambda item: (
            f"- [{item.get('id') or 'criterion'}] {item.get('text') or ''}"
        ),
        max_bytes=6 * 1024,
        empty="- Complete the scoped assignment correctly.",
    )
    deliverable_items = [
        item
        for item in brief.get("required_deliverables", ())
        if item.get("required", True)
    ]
    deliverables = _bounded_lines(
        deliverable_items,
        lambda item: (
            f"- [{item.get('id') or 'deliverable'}] "
            f"{item.get('kind')}: {item.get('title')}"
        ),
        max_bytes=3 * 1024,
        empty="- Structured completion summary",
    )
    ref_items = list(manifest.get("refs", ()))
    ref_lines = _bounded_lines(
        ref_items,
        lambda item: (
            f"- {item['id']} ({item['requirement']}/{item['ref_type']}, "
            f"{item['delivery_mode']}): {item['display_name']} — "
            f"{item.get('summary') or item.get('selection_reason') or ''}"
        ),
        max_bytes=8 * 1024,
        empty="- No context references selected.",
    )
    product_items = list(manifest.get("work_products", ()))
    # A greedy per-item allowance let an early, long Worker report consume the
    # complete section and hide the later Reviewer/Tester verdicts from an
    # Evaluator. Fair-share the bounded section whenever several products are
    # selected. This keeps every product header and a useful summary visible for
    # ordinary DAG fan-in sizes, while the existing omission marker still protects
    # pathological inputs.
    product_item_bytes = (
        8 * 1024
        if len(product_items) <= 1
        else min(
            4 * 1024,
            max(
                512,
                (WORK_PRODUCT_PROMPT_BYTES - len(product_items) - 1)
                // len(product_items),
            ),
        )
    )
    product_lines = _bounded_lines(
        product_items,
        lambda item: (
            f"- {item['id']} ({item['kind']}, run {item.get('run_id') or 'operator'}): "
            f"{item['title']}\n  {item.get('summary') or 'No summary supplied.'}"
        ),
        max_bytes=WORK_PRODUCT_PROMPT_BYTES,
        max_item_bytes=product_item_bytes,
        empty="- No upstream Work Products are available yet.",
    )
    comment_delta = list(wake.get("comment_ids") or ())
    child_results = list(wake.get("child_results") or ())
    response_delta = dict(wake.get("response_delta") or {})
    delta_lines = []
    if comment_delta:
        delta_lines.append(
            "New comment ids: "
            + _clip_utf8(", ".join(str(item) for item in comment_delta), 1_024)
        )
    if child_results:
        delta_lines.append(
            "Child result summaries: "
            + _clip_utf8(
                json.dumps(child_results, ensure_ascii=False, separators=(",", ":")),
                2_048,
            )
        )
    if response_delta:
        delta_lines.append(
            "Gate answer delta: "
            + _clip_utf8(
                json.dumps(response_delta, ensure_ascii=False, separators=(",", ":")),
                2_048,
            )
        )
    delta = "\n".join(delta_lines) or "No incremental wake payload."
    prompt = (
        f"Wake reason: {wake.get('reason') or 'assignment'}\n"
        f"Task: {_clip_utf8(task.get('id'), 256)} — {_clip_utf8(task.get('title'), 512)}\n"
        f"Objective: {_clip_utf8(brief.get('objective'), 2_048)}\n"
        f"Current node: {_clip_utf8(task.get('node_title') or task.get('node_key'), 512)} "
        f"({_clip_utf8(task.get('node_kind'), 128)})\n"
        f"Assignment: {_clip_utf8(task.get('assignment'), 2_048)}\n"
        f"Parent: {task.get('parent_task_id') or 'none'}\n"
        f"Published brief: revision {brief.get('revision')} ({brief.get('content_hash')})\n\n"
        f"Required deliverables:\n{deliverables}\n\n"
        f"Acceptance criteria:\n{criteria}\n\n"
        f"Upstream Work Products ({manifest.get('work_product_count', 0)}):\n"
        f"{product_lines}\n"
        "These immutable summaries are authorized cross-role evidence; they are not "
        "private Agent transcripts.\n\n"
        f"Context manifest: {manifest.get('ref_count', 0)} references; "
        f"{manifest.get('required_count', 0)} required; approximately "
        f"{manifest.get('estimated_tokens', 0)} tokens available on demand.\n"
        f"{ref_lines}\n"
        "When exposed by the runtime, use get_task_context/list_context_refs/"
        "read_context_ref for additional selected evidence. If those callback tools are "
        "not exposed, rely on the bounded Work Product summaries above and the ordinary "
        "read-only workspace tools; do not treat an unavailable callback as missing "
        "candidate evidence. File bodies are not included here.\n\n"
        f"Wake delta:\n{delta}\n\n"
        "Result contract: "
        f"{_clip_utf8(data['result_contract'].get('schema_id') or 'structured_result_v1', 256)}"
    )
    # Existing sections already fit the default envelope budget.  Work Product
    # summaries are the only newly variable inline section, so shrink that section
    # first rather than rejecting an otherwise valid run assignment.
    encoded = len(prompt.encode("utf-8"))
    if encoded > DEFAULT_INITIAL_PROMPT_BYTES and product_items:
        excess = encoded - DEFAULT_INITIAL_PROMPT_BYTES
        product_budget = max(
            256,
            len(product_lines.encode("utf-8")) - excess - 256,
        )
        shortened = _clip_utf8(product_lines, product_budget)
        prompt = prompt.replace(product_lines, shortened, 1)
    assert_envelope_limits(prompt)
    return prompt


def assert_envelope_limits(
    prompt_or_envelope: str | Mapping[str, Any] | ExecutionEnvelope,
    *,
    limit: int = DEFAULT_INITIAL_PROMPT_BYTES,
    hard_limit: int = HARD_INITIAL_PROMPT_BYTES,
) -> int:
    if isinstance(prompt_or_envelope, ExecutionEnvelope):
        raw = json.dumps(prompt_or_envelope.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    elif isinstance(prompt_or_envelope, Mapping):
        raw = json.dumps(prompt_or_envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    else:
        raw = str(prompt_or_envelope)
    size = len(raw.encode("utf-8"))
    ceiling = min(max(1, int(limit)), HARD_INITIAL_PROMPT_BYTES)
    if hard_limit > HARD_INITIAL_PROMPT_BYTES:
        hard_limit = HARD_INITIAL_PROMPT_BYTES
    if size > hard_limit or size > ceiling:
        raise ValueError(
            f"initial execution envelope is {size} bytes; maximum is {ceiling} bytes"
        )
    return size
