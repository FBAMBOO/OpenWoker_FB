from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import threading
import time

import pytest
from fastapi.testclient import TestClient

from coworker.orchestration.context import ContextManifestBuilder, ContextPolicy
from coworker.orchestration.envelope import (
    build_execution_envelope,
    render_initial_user_prompt,
)
from coworker.orchestration.handoff_models import ContextRefDraft, TaskBriefDraft
from coworker.orchestration.models import (
    NodeSpec,
    PlanSpec,
    RunStatus,
    TaskDomain,
    TaskSpec,
    TaskStatus,
)
from coworker.orchestration.profiles import builtin_profile
from coworker.orchestration.routing import ModelCandidate, ModelRouter, RoutingRequest
from coworker.orchestration.store import OrchestrationStore
from coworker.providers import ModelCapabilities, ProviderClient
from coworker.server.app import create_app
from coworker.server.manager import SessionManager


def _brief() -> TaskBriefDraft:
    return TaskBriefDraft(
        title="Bounded performance fixture",
        objective="Verify structured handoff remains bounded",
        scope={"include": ["coworker/orchestration"]},
        instructions=("Measure the control-plane path",),
        acceptance_criteria=(
            {"id": "AC-PERF", "text": "The operation stays bounded", "required": True},
        ),
        deliverables=(
            {
                "id": "DEL-PERF",
                "kind": "test_result",
                "title": "Performance result",
                "required": True,
            },
        ),
        result_contract={"schema_id": "test_result_v1"},
    )


def _create_task(
    store: OrchestrationStore,
    key: str,
    *,
    workspace: str | None = None,
):
    return store.create_task(
        TaskSpec(
            idempotency_key=key,
            title="Handoff performance",
            objective="Exercise a bounded handoff operation",
            domain=TaskDomain.KNOWLEDGE,
            workspace=workspace,
            acceptance_criteria=("The operation stays bounded",),
            policy={"profile_id": "worker", "structured_handoff": True},
        ),
        brief=_brief(),
        command_id=f"create-{key}",
    )


def _queued_run(store: OrchestrationStore, key: str, *, workspace: str | None = None):
    task = _create_task(store, key, workspace=workspace)
    graph = store.create_plan_revision(
        task.id,
        PlanSpec(nodes=(NodeSpec("work", agent="worker"),)),
        expected_task_version=task.version,
        created_by="performance-test",
        command_id=f"plan-{key}",
    )
    task = store.get_task(task.id)
    task = store.transition_task_status(
        task.id,
        TaskStatus.QUEUED,
        expected_version=task.version,
        command_id=f"queue-{key}",
    )
    run = store.enqueue_run(task.id, "work", command_id=f"enqueue-{key}")
    return store.get_task(task.id), graph, run


def test_perf_01_prompt_construction_never_traverses_workspace(monkeypatch, tmp_path):
    """A traversal trap is stronger and cheaper than materializing 50,000 empty files."""

    small_workspace = tmp_path / "repository-with-100-files"
    large_workspace = tmp_path / "repository-with-50000-files"
    small_workspace.mkdir()
    large_workspace.mkdir()
    store = OrchestrationStore(tmp_path / "prompt-size.db")
    try:
        task, graph, _run = _queued_run(
            store, "perf-prompt", workspace=str(small_workspace)
        )
        claim = store.claim_next_run("prompt-worker", command_id="claim-perf-prompt")
        assert claim is not None
        route = ModelRouter(
            (ModelCandidate("gpt-test", quality=100, context_window=100_000),)
        ).select(RoutingRequest(purpose="prompt-performance"))

        def reject_traversal(*_args, **_kwargs):
            raise AssertionError("initial envelope construction traversed the workspace")

        monkeypatch.setattr(Path, "iterdir", reject_traversal)
        monkeypatch.setattr(Path, "rglob", reject_traversal)
        first = build_execution_envelope(
            task=task,
            brief=store.get_active_brief(task.id),
            claim=claim,
            node=graph.nodes[0],
            profile=builtin_profile("worker"),
            routing=route,
            context_refs=(),
            workspace_id="candidate-snapshot",
        )
        second = build_execution_envelope(
            task=replace(task, workspace=str(large_workspace)),
            brief=store.get_active_brief(task.id),
            claim=claim,
            node=graph.nodes[0],
            profile=builtin_profile("worker"),
            routing=route,
            context_refs=(),
            workspace_id="candidate-snapshot",
        )
        first_prompt = render_initial_user_prompt(first)
        second_prompt = render_initial_user_prompt(second)
        assert first_prompt == second_prompt
        assert len(first_prompt.encode("utf-8")) < 32 * 1024
    finally:
        store.close()


class _NoopProvider(ProviderClient):
    def complete(self, **_kwargs):  # pragma: no cover - no run is submitted
        raise AssertionError("provider must not be called")

    def capabilities(self, _model):
        return ModelCapabilities()


def test_perf_02_context_manifest_rejects_51_and_pages_1000_historical_refs(
    tmp_path,
):
    refs = [
        ContextRefDraft(
            requirement="optional",
            ref_type="file",
            display_name=f"File {index}",
            selection_reason="Stress manifest pagination",
            locator={"relative_path": f"src/file-{index:04d}.py"},
            delivery_mode="on_demand",
        )
        for index in range(51)
    ]
    with pytest.raises(ValueError, match="maximum is 50"):
        ContextManifestBuilder(ContextPolicy(max_context_refs=50)).normalize(refs)

    manager = SessionManager(data_dir=tmp_path / "context-data", provider=_NoopProvider())
    store = manager.orchestration.store
    task = _create_task(store, "perf-context")
    brief = store.get_active_brief(task.id)
    timestamp = "2026-08-17T00:00:00.000000Z"
    connection = store.connect()
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany(
            """
            INSERT INTO orch_context_refs(
                id, task_id, brief_id, requirement, ref_type, display_name,
                summary, selection_reason, locator_json, delivery_mode,
                mime_type, content_hash, byte_size, token_estimate,
                provenance_json, trust_level, created_by_task_id,
                created_by_run_id, created_at
            ) VALUES (?, ?, ?, 'optional', 'file', ?, '', ?, ?, 'on_demand',
                      NULL, NULL, 0, 1, '{}', 'untrusted', NULL, NULL, ?)
            """,
            [
                (
                    f"ctx-perf-{index:04d}",
                    task.id,
                    brief.id,
                    f"File {index}",
                    "Imported historical stress fixture",
                    f'{{"relative_path":"src/file-{index:04d}.py"}}',
                    timestamp,
                )
                for index in range(1_000)
            ],
        )
        connection.commit()
    finally:
        connection.close()

    with TestClient(create_app(manager)) as client:
        response = client.get(
            f"/v1/orchestration/tasks/{task.id}/context-refs?limit=100&offset=900"
        )
        assert response.status_code == 200, response.text
        page = response.json()
        assert len(page) == 100
        assert page[0]["id"] == "ctx-perf-0900"
        assert page[-1]["id"] == "ctx-perf-0999"


def test_perf_03_comment_delta_uses_index_for_10000_rows(tmp_path):
    store = OrchestrationStore(tmp_path / "comment-delta.db")
    try:
        task = _create_task(store, "perf-comments")
        timestamp = "2026-08-17T00:00:00.000000Z"
        connection = store.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                """
                INSERT INTO orch_task_comments(
                    id, task_id, sequence_no, author_type, author_id,
                    body_markdown, metadata_json, created_at
                ) VALUES (?, ?, ?, 'system', 'performance-fixture', ?, '{}', ?)
                """,
                [
                    (
                        f"comment-perf-{index:05d}",
                        task.id,
                        index,
                        f"Delta {index}",
                        timestamp,
                    )
                    for index in range(1, 10_001)
                ],
            )
            connection.commit()
            plan = connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT * FROM orch_task_comments
                WHERE task_id = ? AND sequence_no > ?
                ORDER BY sequence_no LIMIT ?
                """,
                (task.id, 9_900, 100),
            ).fetchall()
            assert any("orch_comments_delta" in str(row[3]) for row in plan)
        finally:
            connection.close()

        started = time.perf_counter()
        delta = store.list_task_comments(task.id, after_sequence=9_900, limit=100)
        assert time.perf_counter() - started < 5
        assert len(delta) == 100
        assert delta[0].sequence == 9_901
        assert delta[-1].sequence == 10_000
    finally:
        store.close()


def test_perf_04_wake_claim_uses_ready_index_for_10000_rows(tmp_path):
    store = OrchestrationStore(tmp_path / "wake-ready.db")
    try:
        task = _create_task(store, "perf-wakes")
        due = "2020-01-01T00:00:00.000000Z"
        future = "2999-01-01T00:00:00.000000Z"
        created = "2026-08-17T00:00:00.000000Z"
        connection = store.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                """
                INSERT INTO orch_wake_requests(
                    id, target_task_id, reason, payload_json, dedupe_key,
                    status, not_before, created_at, updated_at
                ) VALUES (?, ?, 'manual_resume', '{}', ?, ?, ?, ?, ?)
                """,
                [
                    (
                        f"wake-perf-{index:05d}",
                        task.id,
                        f"wake-perf-key-{index:05d}",
                        "pending" if index < 5_000 else "deferred",
                        due if index < 5_000 else future,
                        created,
                        created,
                    )
                    for index in range(10_000)
                ],
            )
            connection.commit()
            plan = connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT * FROM orch_wake_requests
                WHERE status = 'pending' AND not_before <= ?
                ORDER BY not_before, created_at, id LIMIT 1
                """,
                (created,),
            ).fetchall()
            assert any("orch_wakes_ready" in str(row[3]) for row in plan)
        finally:
            connection.close()

        started = time.perf_counter()
        claimed = store.claim_ready_wake(
            "performance-scheduler", now=datetime.now(timezone.utc)
        )
        assert time.perf_counter() - started < 5
        assert claimed is not None
        assert claimed.id == "wake-perf-00000"
    finally:
        store.close()


def test_perf_05_eight_concurrent_agents_cannot_duplicate_run_claim(tmp_path):
    store = OrchestrationStore(tmp_path / "concurrent-claim.db", busy_timeout_ms=10_000)
    try:
        _task, _graph, run = _queued_run(store, "perf-concurrent")
        barrier = threading.Barrier(8)

        def claim(index: int):
            barrier.wait()
            return store.claim_next_run(
                f"worker-{index}", command_id=f"claim-perf-concurrent-{index}"
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(claim, range(8)))

        successful = [item for item in results if item is not None]
        assert len(successful) == 1
        assert successful[0].run.id == run.id
        assert store.get_run(run.id).status is RunStatus.CLAIMED
        connection = store.connect()
        try:
            assert connection.execute(
                "SELECT COUNT(*) FROM orch_leases WHERE run_id = ?", (run.id,)
            ).fetchone()[0] == 1
        finally:
            connection.close()
    finally:
        store.close()
