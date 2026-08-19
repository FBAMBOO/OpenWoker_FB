from __future__ import annotations

import hashlib
import io
import zipfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from coworker.orchestration.api import create_orchestration_router
from coworker.orchestration.quality.models import (
    ArtifactStatus,
    ArtifactVersionStatus,
    BudgetStatus,
    QualityStatus,
    WorkflowStatus,
)
from coworker.orchestration.quality.state_machine import (
    WorkflowEvent,
    task_quality_schema_snapshot,
)
from coworker.providers import AssistantTurn, ModelCapabilities, ProviderClient
from coworker.server.manager import SessionManager
from scripts.generate_task_quality_types import TARGET as GENERATED_TYPES
from scripts.generate_task_quality_types import render as render_generated_types


class _Provider(ProviderClient):
    def complete(self, **_kwargs):
        return AssistantTurn(text="done")

    def capabilities(self, _model):
        return ModelCapabilities()


def _control_plane(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "dbt_project.yml").write_text("name: fixture\n", encoding="utf-8")
    (workspace / "models").mkdir()
    (workspace / "models" / "orders.sql").write_text(
        "select 1 as order_id\n", encoding="utf-8"
    )
    manager = SessionManager(
        data_dir=tmp_path / "data", provider=_Provider(), workspace=str(workspace)
    )
    app = FastAPI()
    app.include_router(create_orchestration_router(manager))
    return manager, workspace, app


def _prepare_quality_task(client: TestClient, workspace) -> tuple[str, dict]:
    objective = (
        "Read-only analyze this dbt repository architecture, including models, macros, "
        "tests, seeds, snapshots, deployment relationships, file evidence, and limitations."
    )
    draft = client.post(
        "/v1/orchestration/task-drafts",
        headers={"Idempotency-Key": "draft-quality-api-1"},
        json={
            "title": "Architecture report",
            "objective": objective,
            "domain": "code",
            "workspace": str(workspace),
            "read_only": True,
            "acceptance_criteria": [],
        },
    )
    assert draft.status_code == 201, draft.text
    task_id = draft.json()["task_id"]
    analysis = client.post(
        f"/v1/orchestration/task-drafts/{task_id}:analyze",
        headers={"Idempotency-Key": "analyze-quality-api-1"},
        json={},
    )
    assert analysis.status_code == 200, analysis.text
    body = analysis.json()
    assert body["contract"]["task_id"] == task_id
    assert body["status"] == "resolved"
    frozen = client.post(
        f"/v1/orchestration/task-drafts/{task_id}/snapshots", json={}
    )
    assert frozen.status_code == 201, frozen.text
    published = client.post(
        f"/v1/orchestration/task-drafts/{task_id}/contract:publish",
        headers={"If-Match": body["contract_etag"]},
    )
    assert published.status_code == 200, published.text
    strategy = client.post(
        f"/v1/orchestration/task-drafts/{task_id}/strategy:generate", json={}
    )
    assert strategy.status_code == 200, strategy.text
    return task_id, published.json()


def _start_transaction_state(service, task_id: str) -> dict:
    with service.store._read() as connection:
        task = connection.execute(
            """
            SELECT status, workflow_status, budget_status, active_plan_id,
                   active_budget_ledger_id, active_brief_id
            FROM orch_tasks WHERE id=?
            """,
            (task_id,),
        ).fetchone()
        assert task is not None
        return {
            **dict(task),
            "plans": connection.execute(
                "SELECT COUNT(*) FROM orch_plans WHERE task_id=?", (task_id,)
            ).fetchone()[0],
            "ledgers": connection.execute(
                "SELECT COUNT(*) FROM orch_budget_ledgers WHERE task_id=?", (task_id,)
            ).fetchone()[0],
            "wakes": connection.execute(
                "SELECT COUNT(*) FROM orch_wake_requests WHERE target_task_id=?",
                (task_id,),
            ).fetchone()[0],
            "briefs": connection.execute(
                "SELECT COUNT(*) FROM orch_task_briefs WHERE task_id=?", (task_id,)
            ).fetchone()[0],
        }


def test_python_openapi_and_generated_types_share_one_quality_schema(tmp_path) -> None:
    manager, _workspace, app = _control_plane(tmp_path)
    try:
        canonical = task_quality_schema_snapshot()
        with TestClient(app) as client:
            response = client.get("/v1/orchestration/task-quality/schema")
            assert response.status_code == 200, response.text
            assert response.json() == canonical
            capabilities = client.get("/v1/orchestration/capabilities")
            assert capabilities.status_code == 200
            assert capabilities.json()["task_quality_v2"] == canonical

            rejected = client.post(
                "/v1/orchestration/tasks/not-needed:resume",
                json={"actor_id": "forged", "resume_status": "completed"},
            )
            assert rejected.status_code == 422
            assert rejected.json()["error"]["code"] == "SERVER_DERIVED_IDENTITY_REQUIRED"

        schemas = app.openapi()["components"]["schemas"]
        expected = {
            "WorkflowStatus": [item.value for item in WorkflowStatus],
            "QualityStatus": [item.value for item in QualityStatus],
            "ArtifactStatus": [item.value for item in ArtifactStatus],
            "BudgetStatus": [item.value for item in BudgetStatus],
            "WorkflowEvent": [item.value for item in WorkflowEvent],
        }
        for schema_name, values in expected.items():
            assert schemas[schema_name]["enum"] == values
        assert GENERATED_TYPES.read_text(encoding="utf-8") == render_generated_types()
    finally:
        manager.orchestration.store.close()


@pytest.mark.parametrize(
    "crash_point", ["after_brief", "after_plan", "after_transition"]
)
def test_start_is_atomic_across_crash_points_and_retry_is_exactly_once(
    tmp_path, monkeypatch, crash_point: str
) -> None:
    manager, workspace, app = _control_plane(tmp_path)
    try:
        with TestClient(app) as client:
            task_id, _contract = _prepare_quality_task(client, workspace)

        service = manager.orchestration
        before = _start_transaction_state(service, task_id)
        assert before == {
            "status": "draft",
            "workflow_status": "ready",
            "budget_status": "unconfigured",
            "active_plan_id": None,
            "active_budget_ledger_id": None,
            "active_brief_id": None,
            "plans": 0,
            "ledgers": 0,
            "wakes": 0,
            "briefs": 1,
        }

        method_name = {
            "after_brief": "publish_brief",
            "after_plan": "create_plan_revision",
            "after_transition": "transition_task_status",
        }[crash_point]
        original = getattr(service.store, method_name)

        def fail_after_durable_write(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError(f"injected crash {crash_point}")

        with monkeypatch.context() as patcher:
            patcher.setattr(service.store, method_name, fail_after_durable_write)
            with pytest.raises(RuntimeError, match=f"injected crash {crash_point}"):
                service.quality.start(task_id)

        # Every start-side effect, including the wake emitted by the transition,
        # must disappear with the failed transaction.
        assert _start_transaction_state(service, task_id) == before

        started = service.quality.start(task_id)
        assert started["status"] == "queued"
        committed = _start_transaction_state(service, task_id)
        assert committed["status"] == "queued"
        assert committed["workflow_status"] == "running"
        assert committed["budget_status"] in {"within_budget", "unlimited"}
        assert committed["active_plan_id"]
        assert committed["active_budget_ledger_id"]
        assert committed["active_brief_id"]
        assert committed["plans"] == 1
        assert committed["ledgers"] == 1
        assert committed["wakes"] == 1
        assert committed["briefs"] == 2

        # A retried client request observes the already committed intent and
        # cannot create duplicate plans, ledgers, or initial wakes.
        service.quality.start(task_id)
        assert _start_transaction_state(service, task_id) == committed
    finally:
        manager.orchestration.store.close()


def test_cursor_pagination_is_append_stable_and_scope_bound(tmp_path) -> None:
    manager, workspace, app = _control_plane(tmp_path)
    try:
        with TestClient(app) as client:
            task_id, _contract = _prepare_quality_task(client, workspace)
            artifact = manager.orchestration.quality_artifacts.store_internal_json(
                task_id=task_id,
                logical_deliverable_id="cursor-fixture",
                filename="cursor_fixture.json",
                value={"fixture": True},
            )

            def add_coverage(index: int) -> None:
                manager.orchestration.quality_evidence.record_coverage(
                    task_id=task_id,
                    artifact_id=artifact.id,
                    requirement_id=f"REQ-CURSOR-{index}",
                    area=f"area-{index}",
                    status="pass",
                    claim_ids=(),
                    evidence_count=index,
                    notes="cursor fixture",
                    validator_id="cursor-test@1",
                )

            for index in range(3):
                add_coverage(index)
            first = client.get(
                f"/v1/orchestration/tasks/{task_id}/coverage", params={"limit": 2}
            )
            assert first.status_code == 200, first.text
            page = first.json()["coverage"]
            first_ids = [item["id"] for item in page["items"]]
            assert len(first_ids) == 2
            assert page["has_more"] is True
            assert page["next_cursor"]

            # A concurrent append must not move or duplicate either historical page.
            add_coverage(3)
            repeated = client.get(
                f"/v1/orchestration/tasks/{task_id}/coverage", params={"limit": 2}
            )
            assert [
                item["id"] for item in repeated.json()["coverage"]["items"]
            ] == first_ids
            second = client.get(
                f"/v1/orchestration/tasks/{task_id}/coverage",
                params={"limit": 2, "cursor": page["next_cursor"]},
            )
            assert second.status_code == 200, second.text
            second_page = second.json()["coverage"]
            second_ids = [item["id"] for item in second_page["items"]]
            assert len(second_ids) == 2
            assert not set(first_ids) & set(second_ids)
            assert second_page["pagination"] == "cursor"
            assert second_page["has_more"] is False

            wrong_stream = client.get(
                f"/v1/orchestration/tasks/{task_id}/claims",
                params={"cursor": page["next_cursor"]},
            )
            assert wrong_stream.status_code == 422
            assert (
                wrong_stream.json()["error"]["code"]
                == "SEMANTIC_VALIDATION_FAILED"
            )
    finally:
        manager.orchestration.store.close()


def test_secret_bearing_draft_is_denied_without_echo_or_persistence(tmp_path) -> None:
    manager, workspace, app = _control_plane(tmp_path)
    secret = "abcdefghijklmnopqrstuvwxyz123456"
    try:
        with TestClient(app) as client:
            response = client.post(
                "/v1/orchestration/task-drafts",
                headers={"Idempotency-Key": "secret-draft"},
                json={
                    "objective": f"Analyze repo with api_key = '{secret}'",
                    "workspace": str(workspace),
                    "read_only": True,
                },
            )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "SEMANTIC_VALIDATION_FAILED"
        assert secret not in response.text
        with manager.orchestration.store._read() as connection:
            assert connection.execute(
                "SELECT COUNT(*) FROM orch_tasks WHERE idempotency_key='secret-draft'"
            ).fetchone()[0] == 0
    finally:
        manager.orchestration.store.close()


def test_draft_identity_analysis_idempotency_etag_and_four_axis_projection(
    tmp_path,
) -> None:
    manager, workspace, app = _control_plane(tmp_path)
    try:
        with TestClient(app) as client:
            missing = client.post(
                "/v1/orchestration/task-drafts",
                json={
                    "objective": "analyze repository",
                    "domain": "code",
                    "workspace": str(workspace),
                    "read_only": True,
                },
            )
            assert missing.status_code == 428
            assert set(missing.json()) == {"error"}
            assert missing.json()["error"]["code"] == "PRECONDITION_REQUIRED"
            assert missing.json()["error"]["retryable"] is False
            assert missing.headers["x-correlation-id"] == missing.json()["error"][
                "correlation_id"
            ]

            task_id, contract = _prepare_quality_task(client, workspace)
            analysis = client.get(
                f"/v1/orchestration/task-drafts/{task_id}/analysis"
            )
            assert analysis.status_code == 200
            assert analysis.headers["etag"].strip('"') == contract["content_hash"]

            replay = client.post(
                f"/v1/orchestration/task-drafts/{task_id}:analyze",
                headers={"Idempotency-Key": "analyze-quality-api-1"},
                json={},
            )
            assert replay.status_code == 200
            assert replay.json()["id"] == analysis.json()["id"]
            conflict = client.post(
                f"/v1/orchestration/task-drafts/{task_id}:analyze",
                headers={"Idempotency-Key": "analyze-quality-api-1"},
                json={"language": "en"},
            )
            assert conflict.status_code == 409

            detail = client.get(f"/v1/orchestration/tasks/{task_id}")
            assert detail.status_code == 200
            value = detail.json()
            assert value["id"] == task_id
            assert value["workflow_status"] == "ready"
            assert value["quality_status"] == "pending"
            assert value["artifact_status"] == "none"
            assert value["budget_status"] == "unconfigured"
            assert value["primary_deliverable"] is None
            assert value["quality_verdict"] is None
            assert value["effective_budget"]["mode"] == "unconfigured"

            dashboard = client.get(
                "/v1/orchestration/tasks",
                params={
                    "workflow_status": "ready",
                    "quality_status": "pending",
                    "artifact_status": "none",
                    "budget_status": "unconfigured",
                    "archetype": "repo_analysis",
                    "repo": workspace.name,
                    "has_waiver": "false",
                    "repair_count": 0,
                },
            )
            assert dashboard.status_code == 200, dashboard.text
            assert [item["id"] for item in dashboard.json()] == [task_id]
            row = dashboard.json()[0]
            assert row["workflow_status"] == "ready"
            assert row["quality_status"] == "pending"
            assert row["artifact_status"] == "none"
            assert row["budget_status"] == "unconfigured"
            assert row["archetype"] == "repo_analysis"
            assert row["target"]["repo"] == workspace.name
            assert row["primary_deliverable"] is None
            assert row["hard_gate_status"] == "pending"
            assert row["has_waiver"] is False

            no_match = client.get(
                "/v1/orchestration/tasks",
                params={"workflow_status": "completed"},
            )
            assert no_match.status_code == 200
            assert no_match.json() == []
    finally:
        manager.orchestration.store.close()


def test_artifact_range_etag_download_diff_and_exact_export(tmp_path) -> None:
    manager, workspace, app = _control_plane(tmp_path)
    try:
        with TestClient(app) as client:
            task_id, contract = _prepare_quality_task(client, workspace)
            deliverable = next(
                item for item in contract["deliverables"] if item["primary"]
            )
            content = (
                "# Architecture Report\n\n## Executive summary\nVerified fixture.\n\n"
                "## Scope and baseline\nFrozen directory snapshot.\n\n"
                "## Limitations\nStatic analysis only.\n"
            ).encode("utf-8")
            digest = "sha256:" + hashlib.sha256(content).hexdigest()
            upload = manager.orchestration.quality_artifacts.create(
                task_id,
                logical_deliverable_id=deliverable["id"],
                filename=deliverable["filename"],
                mime_type=deliverable["mime_type"],
            )
            manager.orchestration.quality_artifacts.append(
                upload["upload_id"],
                sequence=0,
                content=content,
                chunk_hash=digest,
                caller_task_id=task_id,
            )
            artifact = manager.orchestration.quality_artifacts.complete(
                upload["upload_id"],
                expected_sha256=digest,
                caller_task_id=task_id,
            )
            manager.orchestration.quality_artifacts.set_status(
                artifact.id, ArtifactVersionStatus.VALIDATING
            )
            manager.orchestration.quality_artifacts.set_status(
                artifact.id, ArtifactVersionStatus.VERIFIED
            )
            manager.orchestration.quality_artifacts.publish_primary(artifact.id)

            deliverables_page = client.get(
                f"/v1/orchestration/tasks/{task_id}/deliverables",
                params={"limit": 1},
            )
            assert deliverables_page.status_code == 200, deliverables_page.text
            deliverables_body = deliverables_page.json()
            assert deliverables_body["primary_artifact_id"] == artifact.id
            assert deliverables_body["deliverables"]["items"][0]["id"] == artifact.id
            assert deliverables_body["deliverables"]["items"][0]["is_primary"] == 1

            metadata = client.get(f"/v1/orchestration/artifacts/{artifact.id}")
            assert metadata.status_code == 200
            assert metadata.json()["sha256"] == digest
            assert metadata.headers["etag"] == f'"{digest}"'

            ranged = client.get(
                f"/v1/orchestration/artifacts/{artifact.id}/content",
                headers={"Range": "bytes=0-9"},
            )
            assert ranged.status_code == 206
            assert ranged.content == content[:10]
            assert ranged.headers["content-range"] == f"bytes 0-9/{len(content)}"
            unchanged = client.get(
                f"/v1/orchestration/artifacts/{artifact.id}/content",
                headers={"If-None-Match": f'"{digest}"'},
            )
            assert unchanged.status_code == 304
            invalid = client.get(
                f"/v1/orchestration/artifacts/{artifact.id}/content",
                headers={"Range": f"bytes={len(content)}-"},
            )
            assert invalid.status_code == 416
            assert invalid.headers["content-range"] == f"bytes */{len(content)}"
            download = client.get(
                f"/v1/orchestration/artifacts/{artifact.id}/download"
            )
            assert download.status_code == 200
            assert download.content == content
            assert "attachment" in download.headers["content-disposition"]

            exported = client.get(f"/v1/orchestration/tasks/{task_id}/export")
            assert exported.status_code == 200, exported.text
            with zipfile.ZipFile(io.BytesIO(exported.content)) as archive:
                names = set(archive.namelist())
                assert {
                    "PRIMARY_DELIVERABLE.md",
                    "CONTRACT.json",
                    "SNAPSHOT.json",
                    "STRATEGY.json",
                    "COVERAGE.json",
                    "CLAIMS.json",
                    "EVIDENCE_INDEX.json",
                    "QUALITY.json",
                    "BUDGET_LEDGER.json",
                    "EVENT_PROVENANCE.json",
                    "MANIFEST.json",
                }.issubset(names)
                assert archive.read("PRIMARY_DELIVERABLE.md") == content
                manifest = archive.read("MANIFEST.json").decode("utf-8")
                assert digest in manifest
    finally:
        manager.orchestration.store.close()


def test_registered_offline_benchmark_api_and_admin_baseline_promotion(tmp_path) -> None:
    manager, _workspace, app = _control_plane(tmp_path)
    try:
        with TestClient(app) as client:
            suites = client.get("/v1/orchestration/benchmarks/suites")
            assert suites.status_code == 200, suites.text
            assert {item["id"] for item in suites.json()} >= {
                "test12",
                "python_fastapi",
                "typescript_react",
                "go_service",
                "java_spring",
            }

            rejected = client.post(
                "/v1/orchestration/benchmarks/runs",
                json={
                    "suite_id": "test12",
                    "candidate_id": "v2",
                    "workspace_path": "C:/production/private",
                },
            )
            assert rejected.status_code == 422
            assert rejected.json()["error"]["code"] == "BENCHMARK_INPUT_REJECTED"

            created = client.post(
                "/v1/orchestration/benchmarks/runs",
                json={"suite_id": "test12", "candidate_id": "v2"},
            )
            assert created.status_code == 201, created.text
            run = created.json()
            assert run["status"] == "pass"
            assert run["metrics"]["quality_score"] >= 85
            assert run["metrics"]["citation_resolution_ratio"] == 1
            assert "prompt" not in run

            loaded = client.get(
                f"/v1/orchestration/benchmarks/runs/{run['id']}"
            )
            assert loaded.status_code == 200
            assert loaded.json()["content_hash"] == run["content_hash"]
            comparison = client.get(
                f"/v1/orchestration/benchmarks/runs/{run['id']}/comparison"
            )
            assert comparison.status_code == 200
            assert comparison.json()["deltas"]["quality_score"] > 5

            forged = client.post(
                "/v1/orchestration/benchmarks/suites/test12:promote-baseline",
                json={
                    "run_id": run["id"],
                    "actor_id": "forged-admin",
                    "actor_role": "admin",
                    "reason": "must not forge the audit signer",
                },
            )
            assert forged.status_code == 422
            assert (
                forged.json()["error"]["code"]
                == "SERVER_DERIVED_IDENTITY_REQUIRED"
            )
            promoted = client.post(
                "/v1/orchestration/benchmarks/suites/test12:promote-baseline",
                json={
                    "run_id": run["id"],
                    "reason": "approved release corpus",
                },
            )
            assert promoted.status_code == 200, promoted.text
            assert promoted.json()["run_id"] == run["id"]
            assert promoted.json()["actor_id"] == "local-user"
            assert promoted.json()["actor_role"] == "admin"
    finally:
        manager.orchestration.store.close()
