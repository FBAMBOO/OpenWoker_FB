from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from coworker.orchestration.api import create_orchestration_router
from coworker.orchestration.errors import IntegrityError
from coworker.orchestration.models import (
    EvidenceKind,
    GateKind,
    GateStatus,
    TaskSpec,
    TaskStatus,
)
from coworker.orchestration.service import OrchestrationService
from coworker.providers import AssistantTurn, ModelCapabilities, ProviderClient
from coworker.server.app import create_app
from coworker.server.manager import SessionManager
from coworker.sessions import SessionRecord


class ScriptedProvider(ProviderClient):
    def __init__(self, turns):
        self.turns = list(turns)

    def complete(self, **_kwargs):
        return self.turns.pop(0) if self.turns else AssistantTurn(text="done")

    def capabilities(self, _model):
        return ModelCapabilities()


def test_subscription_runtime_health_endpoint_is_sanitized_and_refreshable(
    tmp_path, monkeypatch
):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    refresh_values = []
    payload = [
        {
            "runtime_id": "codex-subscription:gpt-5.6-sol@max",
            "provider": "codex-subscription",
            "display_name": "Codex Subscription · GPT-5.6 Sol · Max",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "max",
            "health": {
                "runtime_id": "codex-subscription:gpt-5.6-sol@max",
                "provider": "codex-subscription",
                "installed": True,
                "authenticated": True,
                "available": True,
                "policy_eligible": True,
                "version": "0.146.0",
                "auth_kind": "chatgpt_subscription",
                "executable": "codex.exe",
                "reason": "",
                "checked_at": 1.0,
            },
        }
    ]

    def snapshot(*, refresh=False):
        refresh_values.append(refresh)
        return payload

    monkeypatch.setattr(
        manager.orchestration,
        "subscription_runtime_catalog",
        snapshot,
    )
    with TestClient(create_app(manager)) as client:
        response = client.get(
            "/v1/orchestration/subscription-runtimes", params={"refresh": "true"}
        )

    assert response.status_code == 200
    assert response.json() == payload
    assert refresh_values == [True]
    serialized = response.text.lower()
    assert "api_key" not in serialized
    assert "access_token" not in serialized


def _dead_letter_control_plane(tmp_path):
    service = OrchestrationService(
        SimpleNamespace(), tmp_path / "dead-letter-control-plane", executor=object()
    )
    app = FastAPI()
    app.include_router(
        create_orchestration_router(SimpleNamespace(orchestration=service))
    )
    return service, app


def test_dead_letter_requeue_api_is_idempotent_audited_and_queryable(tmp_path):
    service, app = _dead_letter_control_plane(tmp_path)
    try:
        service.store.create_task(
            TaskSpec(
                idempotency_key="api-outbox-audit-task",
                objective="Exercise formal dead-letter recovery controls",
            ),
            command_id="create-api-outbox-audit-task",
        )
        original = service.store.claim_outbox("publisher-one", limit=1)[0]
        dead = service.store.mark_outbox_dead_lettered(
            original.id, "publisher-one", "subscriber rejected event"
        )
        request = {
            "actor": "on-call@example.com",
            "reason": "Subscriber repair was deployed and verified.",
        }
        route = f"/v1/orchestration/outbox/dead-letters/{original.id}/requeue"

        with TestClient(app) as client:
            missing_key = client.post(route, json=request)
            assert missing_key.status_code == 428

            empty_actor = client.post(
                route,
                headers={"Idempotency-Key": "api-requeue-audit-1"},
                json={"actor": "  ", "reason": request["reason"]},
            )
            assert empty_actor.status_code == 422
            non_string_reason = client.post(
                route,
                headers={"Idempotency-Key": "api-requeue-audit-1"},
                json={"actor": request["actor"], "reason": ["invalid"]},
            )
            assert non_string_reason.status_code == 422

            first = client.post(
                route,
                headers={"Idempotency-Key": "api-requeue-audit-1"},
                json=request,
            )
            assert first.status_code == 200
            body = first.json()
            assert body["replayed"] is False
            assert body["audit"]["actor"] == request["actor"]
            assert body["audit"]["reason"] == request["reason"]
            assert body["audit"]["snapshot"] == {
                "attempts": dead.attempts,
                "last_error": "subscriber rejected event",
                "dead_lettered_at": dead.dead_lettered_at.isoformat().replace(
                    "+00:00", "Z"
                ),
            }
            assert body["current"]["status"] == "queued"
            assert body["requeue_history_total"] == 1

            replay = client.post(
                route,
                headers={"Idempotency-Key": "api-requeue-audit-1"},
                json=request,
            )
            assert replay.status_code == 200
            assert replay.json()["replayed"] is True
            assert replay.json()["audit"]["id"] == body["audit"]["id"]
            assert replay.json()["requeue_history_total"] == 1

            conflict = client.post(
                route,
                headers={"Idempotency-Key": "api-requeue-audit-1"},
                json={**request, "reason": "A different operator assertion."},
            )
            assert conflict.status_code == 409

            detail = client.get(
                f"/v1/orchestration/outbox/dead-letters/{original.id}"
            )
            assert detail.status_code == 200
            assert detail.json()["status"] == "queued"
            assert detail.json()["requeue_history_total"] == 1
            assert detail.json()["requeue_history"][0]["id"] == body["audit"]["id"]

            claimed = service.store.claim_outbox("publisher-two", limit=100)
            reclaimed = next(item for item in claimed if item.id == original.id)
            service.store.mark_outbox_dead_lettered(
                reclaimed.id, "publisher-two", "subscriber failed again"
            )
            dead_letters = client.get(
                "/v1/orchestration/outbox/dead-letters?limit=100"
            )
            assert dead_letters.status_code == 200
            listed = next(
                item
                for item in dead_letters.json()["items"]
                if item["id"] == original.id
            )
            assert listed["requeue_history_total"] == 1
            assert listed["requeue_history"][0]["id"] == body["audit"]["id"]
    finally:
        service.store.close()


def test_orchestration_api_exposes_tasks_and_versioned_profiles(tmp_path):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        capabilities = client.get("/v1/orchestration/capabilities")
        assert capabilities.status_code == 200
        assert len(capabilities.json()["stages"]) == 8
        assert capabilities.json()["features"]["durable_resume"] is True
        assert capabilities.json()["health"]["ready"] is True
        assert client.get("/v1/orchestration/health").status_code == 200

        missing_key = client.post(
            "/v1/orchestration/tasks",
            json={"objective": "This request must be retry-safe"},
        )
        assert missing_key.status_code == 428

        created = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "api-create-summary-1"},
            json={
                "objective": "Produce an auditable summary",
                "domain": "knowledge",
                "acceptance_criteria": ["The summary exists"],
                "auto_start": False,
            },
        )
        assert created.status_code == 201
        task = created.json()
        assert task["status"] == "draft"
        replay = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "api-create-summary-1"},
            json={
                "objective": "Produce an auditable summary",
                "domain": "knowledge",
                "acceptance_criteria": ["The summary exists"],
                "auto_start": False,
            },
        )
        assert replay.status_code == 201
        assert replay.json()["id"] == task["id"]
        conflict = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "api-create-summary-1"},
            json={
                "objective": "A different task",
                "domain": "knowledge",
                "acceptance_criteria": ["A different result exists"],
                "auto_start": False,
            },
        )
        assert conflict.status_code == 409
        assert client.get(f"/v1/orchestration/tasks/{task['id']}").status_code == 200
        by_key = client.get(
            "/v1/orchestration/tasks/by-idempotency-key/api-create-summary-1"
        )
        assert by_key.status_code == 200
        assert by_key.json()["id"] == task["id"]
        opaque_key = "tenant/acme:summary-request"
        opaque_created = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": opaque_key},
            json={
                "objective": "Recover a task by an opaque idempotency key",
                "domain": "knowledge",
                "auto_start": False,
            },
        )
        assert opaque_created.status_code == 201
        opaque_lookup = client.get(
            "/v1/orchestration/tasks/by-idempotency-key",
            params={"idempotency_key": opaque_key},
        )
        assert opaque_lookup.status_code == 200
        assert opaque_lookup.json()["id"] == opaque_created.json()["id"]
        assert any(item["id"] == task["id"] for item in client.get("/v1/orchestration/tasks").json())

        clone = client.post(
            "/v1/orchestration/agent-profiles/worker/clone",
            json={"new_profile_id": "api-worker", "overrides": {"display_name": "API worker"}},
        )
        assert clone.status_code == 201
        etag = clone.json()["draft"]["etag"]
        published = client.post(
            "/v1/orchestration/agent-profiles/api-worker/draft/publish",
            headers={"If-Match": etag},
        )
        assert published.status_code == 200
        assert published.json()["current"]["version"] == 1


def test_orchestration_api_requires_etag_and_maps_not_found(tmp_path):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        assert client.get("/v1/orchestration/tasks/missing").status_code == 404
        clone = client.post(
            "/v1/orchestration/model-policies/quality-first/clone",
            json={"new_policy_id": "api-policy"},
        )
        assert clone.status_code == 201
        missing_etag = client.post(
            "/v1/orchestration/model-policies/api-policy/draft/publish"
        )
        assert missing_etag.status_code == 428


def test_orchestration_health_is_not_ready_when_scheduler_is_unhealthy(
    tmp_path, monkeypatch
):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        unhealthy = {
            "ready": False,
            "state": "unhealthy",
            "loop_alive": False,
            "closing": False,
            "started_at": "2026-08-03T00:00:00Z",
            "last_success_at": None,
            "last_error_at": "2026-08-03T00:00:01Z",
            "last_error": "RuntimeError: scheduler crashed",
            "consecutive_failures": 3,
            "failure_limit": 3,
            "active_jobs": 0,
        }
        monkeypatch.setattr(
            manager.orchestration,
            "health_snapshot",
            lambda: dict(unhealthy),
        )
        response = client.get("/v1/orchestration/health")
        assert response.status_code == 503
        assert response.json() == unhealthy
        assert client.get("/v1/orchestration/capabilities").json()["health"] == unhealthy
        standard = client.get("/v1/health")
        assert standard.status_code == 503
        assert standard.json()["status"] == "not_ready"
        assert standard.json()["orchestration"] == unhealthy


def test_gate_resolve_http_retry_is_idempotent_and_payload_reuse_conflicts(tmp_path):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        created = client.post(
            "/v1/orchestration/tasks",
            json={
                "idempotency_key": "api-clarification-1",
                "objective": "Clarify the acceptance contract",
                "domain": "knowledge",
                "acceptance_criteria": [],
                "auto_start": False,
            },
        ).json()
        task_id = str(created["id"])
        manager.orchestration.submit_task(task_id)
        manager.orchestration._advance_task(task_id)
        gate = next(
            item
            for item in manager.orchestration.store.list_gates(task_id)
            if item.status.value == "open" and item.kind.value == "clarification"
        )
        payload = {
            "decision": "submit",
            "response": "The answer must include sources and a conclusion.",
            "resolved_by": "api-owner",
            "idempotency_key": "resolve-clarification-1",
        }
        first = client.post(
            f"/v1/orchestration/tasks/{task_id}/gates/{gate.id}/resolve",
            json=payload,
        )
        assert first.status_code == 200

        # Simulate losing the first HTTP response. No expected_version was sent;
        # command replay must occur before reading the now-incremented gate version.
        retry_payload = dict(payload)
        retry_payload.pop("idempotency_key")
        retry_payload["command_id"] = "resolve-clarification-1"
        replay = client.post(
            f"/v1/orchestration/tasks/{task_id}/attention/{gate.id}/resolve",
            json=retry_payload,
        )
        assert replay.status_code == 200
        assert replay.json() == first.json()

        changed = dict(payload)
        changed["response"] = "A different acceptance contract."
        conflict = client.post(
            f"/v1/orchestration/tasks/{task_id}/gates/{gate.id}/resolve",
            json=changed,
        )
        assert conflict.status_code == 409
        assert "reused with different input" in conflict.json()["detail"]

        decisions = [
            evidence
            for evidence in manager.orchestration.store.list_evidence(task_id)
            if evidence.payload.get("gate_id") == gate.id
            and evidence.payload.get("decision") == "submit"
        ]
        assert len(decisions) == 1


def test_task_list_supports_stable_pagination_and_status_filter(tmp_path):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        created = []
        for priority in (3, 2, 1):
            response = client.post(
                "/v1/orchestration/tasks",
                json={
                    "idempotency_key": f"page-{priority}",
                    "objective": f"Task {priority}",
                    "domain": "knowledge",
                    "acceptance_criteria": ["done"],
                    "priority": priority,
                    "auto_start": False,
                },
            )
            assert response.status_code == 201
            created.append(response.json())

        page = client.get("/v1/orchestration/tasks", params={"limit": 1, "offset": 1})
        assert page.status_code == 200
        assert [item["id"] for item in page.json()] == [created[1]["id"]]

        manager.orchestration.submit_task(created[0]["id"])
        drafts = client.get(
            "/v1/orchestration/tasks",
            params={"status": "draft", "limit": 10},
        )
        assert drafts.status_code == 200
        assert [item["id"] for item in drafts.json()] == [
            created[1]["id"],
            created[2]["id"],
        ]


def test_orchestration_api_rejects_invalid_state_commands_and_serves_verified_blobs(
    tmp_path,
):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        created = client.post(
            "/v1/orchestration/tasks",
            json={
                "idempotency_key": "api-evidence-1",
                "objective": "Keep evidence",
                "domain": "knowledge",
                "acceptance_criteria": ["evidence is retrievable"],
                "auto_start": False,
            },
        ).json()
        assert client.post(
            f"/v1/orchestration/tasks/{created['id']}/archive"
        ).status_code == 409

        ref = manager.orchestration.blobs.put(
            b"immutable evidence", mime_type="text/plain"
        )
        manager.orchestration.store.add_evidence(
            created["id"],
            kind=EvidenceKind.ARTIFACT,
            payload={"title": "downloadable"},
            created_by="test",
            content_hash=ref.sha256,
            blob_uri=ref.uri,
            mime_type=ref.mime_type,
        )
        response = client.get(f"/v1/orchestration/blobs/{ref.sha256}")
        assert response.status_code == 200
        assert response.content == b"immutable evidence"
        assert response.headers["content-type"].startswith("text/plain")
        assert client.get("/v1/orchestration/blobs/" + "0" * 64).status_code == 404


def test_task_events_default_to_latest_and_expose_bidirectional_page_cursors(
    tmp_path, monkeypatch
):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        task = client.post(
            "/v1/orchestration/tasks",
            json={
                "idempotency_key": "api-audit-events-1",
                "objective": "Audit every decision",
                "domain": "knowledge",
                "acceptance_criteria": ["audit retained"],
                "auto_start": False,
            },
        ).json()
        task_id = task["id"]
        for index in range(4):
            manager.orchestration.store.add_evidence(
                task_id,
                kind=EvidenceKind.NOTE,
                payload={"title": f"note-{index}"},
                created_by=f"actor-{index}",
                command_id=f"audit-page-{index}",
            )

        all_events = manager.orchestration.store.list_events(task_id=task_id)
        monkeypatch.setattr(
            manager.orchestration.store,
            "verify_event_chain",
            lambda: (_ for _ in ()).throw(
                AssertionError("interactive pagination must not rescan global history")
            ),
        )
        latest = client.get(
            f"/v1/orchestration/tasks/{task_id}/events", params={"limit": 2}
        )
        assert latest.status_code == 200
        latest_payload = latest.json()
        assert [item["sequence"] for item in latest_payload["events"]] == [
            item.sequence for item in all_events[-2:]
        ]
        assert latest_payload["has_more"] is True
        assert latest_payload["next_parameter"] == "before_sequence"
        assert latest_payload["events"][-1]["actor"] == "actor-3"
        assert latest_payload["chain_valid"] is True
        assert latest_payload["chain_verification"] == {
            "valid": True,
            "scope": "page_with_predecessors",
            "verified_events": 2,
            "through_sequence": latest_payload["events"][-1]["sequence"],
            "through_hash": latest_payload["events"][-1]["event_hash"],
        }

        evidence_page = client.get(
            f"/v1/orchestration/tasks/{task_id}/evidence",
            params={"limit": 2},
        ).json()
        assert [item["title"] for item in evidence_page["evidence"]] == [
            "note-2",
            "note-3",
        ]
        assert evidence_page["has_more"] is True
        older_evidence = client.get(
            f"/v1/orchestration/tasks/{task_id}/evidence",
            params={"limit": 2, "offset": evidence_page["next_offset"]},
        ).json()
        assert [item["title"] for item in older_evidence["evidence"]] == [
            "note-0",
            "note-1",
        ]

        older = client.get(
            f"/v1/orchestration/tasks/{task_id}/events",
            params={
                "before_sequence": latest_payload["next_sequence"],
                "limit": 2,
            },
        ).json()
        assert older["events"]
        assert max(item["sequence"] for item in older["events"]) < min(
            item["sequence"] for item in latest_payload["events"]
        )

        forward = client.get(
            f"/v1/orchestration/tasks/{task_id}/events",
            params={
                "latest": False,
                "after_sequence": all_events[0].sequence,
                "limit": 2,
            },
        ).json()
        assert [item["sequence"] for item in forward["events"]] == [
            item.sequence for item in all_events[1:3]
        ]
        assert forward["next_parameter"] == "after_sequence"

        detail = client.get(f"/v1/orchestration/tasks/{task_id}").json()
        assert detail["activity"][-1]["actor"] == "actor-3"
        assert detail["evidence"][-1]["actor"] == "actor-3"


def test_task_gates_endpoint_pages_only_published_attention_with_a_bounded_offset(
    tmp_path,
):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        store = manager.orchestration.store
        task = store.create_task(
            TaskSpec(
                idempotency_key="api-gate-page-1",
                objective="Page the published attention ledger",
            )
        )
        task = store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = store.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        for index in range(4):
            gate = store.open_task_gate(
                task.id,
                kind=GateKind.QUESTION,
                source_key=f"api-gate-page:{index}",
                prompt={"title": f"gate-{index}", "actions": ["submit"]},
                command_id=f"open-api-gate-page-{index}",
            )
            store.resolve_gate(
                gate.id,
                GateStatus.APPROVED,
                {"decision": "submit", "response": f"answer-{index}"},
                resolved_by="api-test",
                expected_version=gate.version,
                command_id=f"resolve-api-gate-page-{index}",
            )

        published = store.list_gates(task.id)
        latest = client.get(
            f"/v1/orchestration/tasks/{task.id}/gates", params={"limit": 2}
        )
        assert latest.status_code == 200
        latest_payload = latest.json()
        assert [item["id"] for item in latest_payload["gates"]] == [
            item.id for item in published[-2:]
        ]
        assert latest_payload["task_id"] == task.id
        assert latest_payload["offset"] == 0
        assert latest_payload["limit"] == 2
        assert latest_payload["has_more"] is True
        assert latest_payload["next_offset"] == 2
        assert latest_payload["order"] == "oldest_to_newest"

        older = client.get(
            f"/v1/orchestration/tasks/{task.id}/gates",
            params={"limit": 2, "offset": latest_payload["next_offset"]},
        )
        assert older.status_code == 200
        older_payload = older.json()
        assert [item["id"] for item in older_payload["gates"]] == [
            item.id for item in published[:2]
        ]
        assert older_payload["has_more"] is False
        assert older_payload["next_offset"] is None
        beyond_history = client.get(
            f"/v1/orchestration/tasks/{task.id}/gates", params={"offset": 10_001}
        )
        assert beyond_history.status_code == 200
        assert beyond_history.json()["gates"] == []
        assert beyond_history.json()["has_more"] is False

def test_run_transcript_is_read_only_task_scoped_and_does_not_invent_session_ids(
    tmp_path, monkeypatch
):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    with TestClient(create_app(manager)) as client:
        task = client.post(
            "/v1/orchestration/tasks",
            json={
                "idempotency_key": "api-transcript-1",
                "objective": "Retain a transcript",
                "domain": "knowledge",
                "acceptance_criteria": ["transcript retained"],
                "auto_start": False,
            },
        ).json()
        task_id = task["id"]
        session_id = "__orch__transcript-test"
        manager.session_store.save(
            SessionRecord(
                session_id=session_id,
                workspace=str(tmp_path),
                model="test-model",
                mode="auto",
                messages=[
                    {"role": "user", "content": "inspect"},
                    {"role": "assistant", "content": "verified"},
                ],
            )
        )
        runs = {
            "run-with-session": SimpleNamespace(
                id="run-with-session",
                task_id=task_id,
                session_id=session_id,
                node_key="verify",
            ),
            "run-without-session": SimpleNamespace(
                id="run-without-session",
                task_id=task_id,
                session_id=None,
                node_key="archive",
            ),
        }
        monkeypatch.setattr(
            manager.orchestration.store,
            "get_run",
            lambda run_id: runs[run_id],
        )

        response = client.get(
            f"/v1/orchestration/tasks/{task_id}/runs/run-with-session/transcript",
            params={"offset": 1, "limit": 1},
        )
        assert response.status_code == 200
        assert response.json() == {
            "task_id": task_id,
            "run_id": "run-with-session",
            "session_id": session_id,
            "available": True,
            "title": "inspect",
            "messages": [{"role": "assistant", "content": "verified"}],
            "message_count": 2,
            "offset": 1,
            "limit": 1,
            "has_more": False,
            "next_offset": None,
            "updated_at": response.json()["updated_at"],
        }

        missing = client.get(
            f"/v1/orchestration/tasks/{task_id}/runs/run-without-session/transcript"
        ).json()
        assert missing["available"] is False
        assert missing["session_id"] is None
        assert missing["messages"] == []


def test_corrupt_audit_chain_fails_application_startup_closed(tmp_path):
    manager = SessionManager(data_dir=tmp_path / "data", provider=ScriptedProvider([]))
    manager.orchestration.create_task(
        {
            "objective": "seed audit",
            "domain": "knowledge",
            "acceptance_criteria": ["seeded"],
            "auto_start": False,
        }
    )
    database = tmp_path / "data" / "orchestration" / "orchestration.db"
    with sqlite3.connect(database) as connection:
        # Simulate out-of-band disk/database tampering that bypasses the normal
        # append-only trigger (for example, an attacker with direct DB access).
        connection.execute("DROP TRIGGER orch_events_no_update")
        connection.execute(
            "UPDATE orch_events SET payload_json = '{\"tampered\":true}' "
            "WHERE sequence_no = 1"
        )
        connection.commit()
    try:
        with pytest.raises(IntegrityError):
            with TestClient(create_app(manager)):
                pass
    finally:
        manager.orchestration.store.close()
