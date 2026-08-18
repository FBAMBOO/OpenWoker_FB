from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.communications import TaskCommunicationService
from coworker.orchestration.context import (
    ContextManifestBuilder,
    ContextPolicy,
    ContextRefResolver,
)
from coworker.orchestration.envelope import (
    build_execution_envelope,
    render_initial_user_prompt,
)
from coworker.orchestration.errors import ConflictError, IntegrityError
from coworker.orchestration.handoff_models import (
    BriefStatus,
    ContextDeliveryMode,
    ContextRefDraft,
    ContextRefType,
    ContextRequirement,
    HandoffValidationError,
    ExecutionEnvelope,
    TaskBriefDraft,
    TaskRelationType,
    WakeReason,
    WakeStatus,
)
from coworker.orchestration.models import (
    NodeSpec,
    PlanSpec,
    RunStatus,
    TaskDomain,
    TaskSpec,
    TaskStatus,
)
from coworker.orchestration.observability import HandoffMetrics
from coworker.orchestration.profiles import builtin_profile
from coworker.orchestration.routing import (
    ModelCandidate,
    ModelRouter,
    RoutingRequest,
)
from coworker.orchestration.runtime_tools import HandoffToolFactory
from coworker.orchestration.service import OrchestrationService
from coworker.orchestration.store import OrchestrationStore
from coworker.providers import ModelCapabilities, ProviderClient
from coworker.server.app import create_app
from coworker.server.manager import SessionManager


def _brief(title: str = "Implement handoff") -> TaskBriefDraft:
    return TaskBriefDraft(
        title=title,
        objective="Implement the bounded handoff change",
        background="The caller needs a durable structured result.",
        scope={"include": ["coworker/orchestration"]},
        instructions=("Implement the requested bounded change",),
        constraints=("Do not publish",),
        acceptance_criteria=(
            {
                "id": "AC-01",
                "text": "The bounded behavior is verified",
                "required": True,
            },
        ),
        deliverables=(
            {
                "id": "DEL-01",
                "kind": "implementation_patch",
                "title": "Implementation patch",
                "required": True,
            },
        ),
        result_contract={"schema_id": "implementation_result_v1"},
    )


def _task_with_running_run(store: OrchestrationStore, key: str = "structured"):
    task = store.create_task(
        TaskSpec(
            idempotency_key=key,
            title="Structured task",
            objective="Complete structured work",
            domain=TaskDomain.KNOWLEDGE,
            acceptance_criteria=("The bounded behavior is verified",),
            policy={"profile_id": "worker", "structured_handoff": True},
        ),
        brief=_brief(),
        command_id=f"create-{key}",
    )
    graph = store.create_plan_revision(
        task.id,
        PlanSpec(nodes=(NodeSpec("work", agent="worker"),)),
        expected_task_version=task.version,
        created_by="test",
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
    claim = store.claim_next_run("handoff-test", command_id=f"claim-{key}")
    assert claim is not None and claim.run.id == run.id
    store.start_run(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        command_id=f"start-{key}",
    )
    return store.get_task(task.id), graph, claim


def test_handoff_observability_contract_tracks_durable_queue_state(tmp_path):
    store = OrchestrationStore(tmp_path / "handoff-observability.db")
    try:
        task = store.create_task(
            TaskSpec(
                idempotency_key="handoff-observability",
                objective="Verify the handoff metric contract",
            )
        )
        first = store.enqueue_wake(
            task.id,
            WakeReason.MANUAL_RESUME,
            payload={"comment_ids": ["comment-1"]},
            dedupe_key="handoff-observability-wake",
        )
        second = store.enqueue_wake(
            task.id,
            WakeReason.MANUAL_RESUME,
            payload={"comment_ids": ["comment-2"]},
            dedupe_key="handoff-observability-wake",
        )
        assert first.id == second.id
        assert second.coalesced_count == 1

        metrics = HandoffMetrics()
        service = SimpleNamespace(store=store, handoff_metrics=metrics)
        OrchestrationService._refresh_handoff_metrics(
            service, now=datetime.now(timezone.utc)
        )
        snapshot = metrics.snapshot()
        assert {
            "orchestration_context_reads_total",
            "orchestration_context_bytes_read_total",
            "orchestration_wake_coalesced_total",
            "orchestration_wake_failures_total",
            "orchestration_work_products_total",
            "orchestration_legacy_delegation_total",
            "orchestration_transcript_cross_role_reads_total",
        } <= snapshot["counters"].keys()
        assert snapshot["counters"]["orchestration_wake_coalesced_total"] == 1
        assert {
            "orchestration_handoff_initial_prompt_bytes",
            "orchestration_handoff_context_refs",
            "orchestration_handoff_context_tokens_estimated",
            "orchestration_wakes_pending",
            "orchestration_wake_delivery_latency_seconds",
            "orchestration_task_blocked_duration_seconds",
        } <= snapshot["last"].keys()
        assert snapshot["last"]["orchestration_wakes_pending"] == 1
    finally:
        store.close()


def test_brief_revisions_are_validated_immutable_and_run_snapshotted(tmp_path):
    store = OrchestrationStore(tmp_path / "briefs.db")
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="brief-task", objective="Version a Brief"),
            brief=_brief("Revision one"),
            command_id="create-brief-task",
        )
        first = store.get_active_brief(task.id)
        assert first.status is BriefStatus.PUBLISHED
        with pytest.raises(ConflictError, match="immutable"):
            store.update_brief_draft(
                task.id,
                1,
                _brief("Illegal update"),
                expected_hash=first.content_hash,
            )

        graph = store.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec("work"),)),
            expected_task_version=task.version,
            created_by="test",
        )
        task = store.get_task(task.id)
        task = store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        run = store.enqueue_run(task.id, graph.nodes[0].key)
        assert run.brief_id == first.id

        second = store.create_brief_draft(
            task.id, _brief("Revision two"), copy_context_from_brief_id=first.id
        )
        published = store.publish_brief(task.id, second.revision)
        assert published.revision == 2
        assert store.get_brief(task.id, 1).status is BriefStatus.SUPERSEDED
        assert store.get_run(run.id).brief_id == first.id
    finally:
        store.close()


def test_incomplete_delegation_is_rejected_without_partial_rows(tmp_path):
    store = OrchestrationStore(tmp_path / "delegation.db")
    try:
        parent, graph, claim = _task_with_running_run(store, "parent")
        before = len(store.list_all_tasks())
        incomplete = TaskBriefDraft(
            title="Missing deliverable",
            objective="Should fail",
            scope={"whole_task": True, "reason": "bounded test"},
            instructions=("Validate before writing",),
            acceptance_criteria=(
                {"id": "AC-01", "text": "No partial rows", "required": True},
            ),
            result_contract={"schema_id": "implementation_result_v1"},
        )
        with pytest.raises(HandoffValidationError):
            store.create_delegated_task(
                TaskSpec(
                    idempotency_key="child-incomplete",
                    objective="Should fail",
                    parent_task_id=parent.id,
                    parent_node_id=graph.nodes[0].id,
                ),
                parent_run_id=claim.run.id,
                lease_token=claim.lease.token,
                fencing_token=claim.lease.fencing_token,
                brief=incomplete,
                command_id="delegate-incomplete",
            )
        assert len(store.list_all_tasks()) == before
        assert not [
            item
            for item in store.list_wakes(limit=1_000)
            if item.dedupe_key.startswith("child-incomplete")
        ]

        delegated = store.create_delegated_task(
            TaskSpec(
                idempotency_key="child-complete",
                objective="Implement child",
                parent_task_id=parent.id,
                parent_node_id=graph.nodes[0].id,
                policy={"profile_id": "worker", "structured_handoff": True},
            ),
            parent_run_id=claim.run.id,
            lease_token=claim.lease.token,
            fencing_token=claim.lease.fencing_token,
            brief=_brief("Child Brief"),
            command_id="delegate-complete",
        )
        child = delegated["task"]
        assert child.status is TaskStatus.QUEUED
        assert delegated["brief"].status is BriefStatus.PUBLISHED
        assert delegated["wake"].reason is WakeReason.TASK_ASSIGNED
        relations = store.list_relations(child.id)
        assert any(
            item.relation_type is TaskRelationType.PARENT
            and item.from_task_id == parent.id
            and item.to_task_id == child.id
            for item in relations
        )
    finally:
        store.close()


def test_context_is_manifest_only_audited_and_path_safe(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "source.txt"
    source.write_text("one\ntwo\nthree\n", encoding="utf-8")
    store = OrchestrationStore(tmp_path / "context.db")
    try:
        resolver = ContextRefResolver(store)
        prepared = resolver.prepare_file_ref(
            workspace,
            ContextRefDraft(
                requirement="required",
                ref_type="file_range",
                display_name="Selected lines",
                selection_reason="Only these lines are relevant",
                locator={
                    "relative_path": "source.txt",
                    "start_line": 2,
                    "end_line": 2,
                },
            ),
        )
        normalized = ContextManifestBuilder(
            ContextPolicy(max_initial_context_tokens=100)
        ).normalize((prepared, prepared))
        assert len(normalized) == 1
        task = store.create_task(
            TaskSpec(
                idempotency_key="context-task",
                objective="Read selected evidence",
                workspace=str(workspace),
            ),
            brief=_brief("Context task"),
            context_refs=normalized,
        )
        ref = store.list_context_refs(task.id)[0]
        result = resolver.read(
            ref.id,
            task_id=task.id,
            run_id=None,
            workspace=workspace,
        )
        assert "two" in result["content"]
        assert "one" not in result["content"]
        assert "untrusted task data" in result["content"]
        assert any(
            item.event_type == "context_ref_read"
            for item in store.list_events(task_id=task.id)
        )
        with pytest.raises(ValueError, match="escape"):
            resolver.canonical_workspace_path(workspace, "../outside.txt")
    finally:
        store.close()


@pytest.mark.parametrize(
    "unsafe_path",
    (
        "",
        "nul\x00path.txt",
        "C:\\Windows\\system.ini",
        "C:Windows\\system.ini",
        "\\Windows\\system.ini",
        "/etc/passwd",
        "\\\\server\\share\\file.txt",
        "\\\\?\\C:\\Windows\\system.ini",
    ),
)
def test_context_workspace_path_rejects_absolute_unc_and_nul(tmp_path, unsafe_path):
    workspace = tmp_path / "path-workspace"
    workspace.mkdir()
    with pytest.raises(ValueError):
        ContextRefResolver.canonical_workspace_path(workspace, unsafe_path)


def test_stale_context_required_fails_and_recommended_records_provenance(tmp_path):
    workspace = tmp_path / "stale-workspace"
    workspace.mkdir()
    required_file = workspace / "required.txt"
    recommended_file = workspace / "recommended.txt"
    required_file.write_text("required-v1", encoding="utf-8")
    recommended_file.write_text("recommended-v1", encoding="utf-8")
    store = OrchestrationStore(tmp_path / "stale-context.db")
    try:
        resolver = ContextRefResolver(store)
        refs = tuple(
            resolver.prepare_file_ref(
                workspace,
                ContextRefDraft(
                    requirement=requirement,
                    ref_type="file",
                    display_name=f"{requirement} file",
                    selection_reason="Exercise stale-reference semantics",
                    locator={"relative_path": filename},
                    trust_level="operator_verified",
                ),
            )
            for requirement, filename in (
                ("required", required_file.name),
                ("recommended", recommended_file.name),
            )
        )
        task = store.create_task(
            TaskSpec(
                idempotency_key="stale-context",
                objective="Verify stale context policy",
                workspace=str(workspace),
            ),
            brief=_brief("Stale context"),
            context_refs=refs,
        )
        required_file.write_text("required-v2", encoding="utf-8")
        recommended_file.write_text("recommended-v2", encoding="utf-8")
        records = {
            ref.requirement: ref for ref in store.list_context_refs(task.id)
        }
        assert (
            records[ContextRequirement.RECOMMENDED].trust_level
            == "operator_verified"
        )
        with pytest.raises(ConflictError, match="stale"):
            resolver.read(
                records[ContextRequirement.REQUIRED].id,
                task_id=task.id,
                run_id=None,
                workspace=workspace,
            )
        recommended = resolver.read(
            records[ContextRequirement.RECOMMENDED].id,
            task_id=task.id,
            run_id=None,
            workspace=workspace,
        )
        assert recommended["stale"] is True
        assert "recommended-v2" in recommended["content"]
        stale_ids = {
            event.aggregate_id
            for event in store.list_events(task_id=task.id)
            if event.event_type == "context_ref_stale"
        }
        assert stale_ids == {
            records[ContextRequirement.REQUIRED].id,
            records[ContextRequirement.RECOMMENDED].id,
        }
    finally:
        store.close()


def test_context_and_comment_security_boundaries_fail_closed(tmp_path):
    workspace = tmp_path / "secure-workspace"
    workspace.mkdir()
    secret = workspace / "credentials.txt"
    secret.write_text("password=abcdefghijklmnop\n", encoding="utf-8")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    store = OrchestrationStore(tmp_path / "security-boundaries.db")
    try:
        resolver = ContextRefResolver(store)
        secret_ref = resolver.prepare_file_ref(
            workspace,
            ContextRefDraft(
                requirement="recommended",
                ref_type="file",
                display_name="Credential-shaped file",
                selection_reason="Verify inline secret blocking",
                locator={"relative_path": "credentials.txt"},
                delivery_mode="excerpt",
            ),
        )
        assert secret_ref.delivery_mode is ContextDeliveryMode.METADATA_ONLY
        url_ref = ContextRefDraft(
            requirement="optional",
            ref_type="url",
            display_name="Cloud metadata address",
            selection_reason="Verify a URL ref grants no ambient network access",
            locator={"url": "http://169.254.169.254/latest/meta-data/"},
            delivery_mode="on_demand",
        )
        task = store.create_task(
            TaskSpec(
                idempotency_key="security-boundaries",
                objective="Reject unsafe handoff content",
                workspace=str(workspace),
            ),
            brief=_brief("Security boundaries"),
            context_refs=(secret_ref, url_ref),
        )
        refs = {ref.ref_type: ref for ref in store.list_context_refs(task.id)}
        with pytest.raises(PermissionError, match="secret-like"):
            resolver.read(
                refs[ContextRefType.FILE].id,
                task_id=task.id,
                run_id=None,
                workspace=workspace,
            )
        with pytest.raises(PermissionError, match="network access is disabled"):
            resolver.read(
                refs[ContextRefType.URL].id,
                task_id=task.id,
                run_id=None,
                workspace=workspace,
            )

        link = workspace / "escaping-link.txt"
        try:
            link.symlink_to(outside)
        except OSError:
            pass  # Windows may not grant symlink creation to this test process.
        else:
            with pytest.raises(PermissionError, match="outside the workspace"):
                resolver.canonical_workspace_path(workspace, link.name)

        with pytest.raises(ValueError, match="65536 bytes"):
            store.post_task_comment(
                task.id,
                "x" * 65_537,
                author_type="operator",
                author_id="local-user",
            )
        with pytest.raises(ValueError, match="at most 10"):
            store.post_task_comment(
                task.id,
                "Mention storm",
                author_type="operator",
                author_id="local-user",
                metadata={"mentions": [f"reviewer-{index}" for index in range(11)]},
            )
        sanitized = store.post_task_comment(
            task.id,
            "<script>alert('x')</script>Safe update",
            author_type="operator",
            author_id="local-user",
            wake_owner=False,
        )
        assert sanitized.body_markdown == "Safe update"
    finally:
        store.close()


def test_handoff_visible_metadata_rejects_secrets_and_legacy_brief_redacts(tmp_path):
    secret = "client_secret=abcdefghijklmnop"
    with pytest.raises(HandoffValidationError) as brief_error:
        TaskBriefDraft(
            title="Unsafe",
            objective=secret,
            scope={"whole_task": True, "reason": "security test"},
            instructions=("Do the bounded work",),
            acceptance_criteria=(
                {"id": "AC-01", "text": "Safe", "required": True},
            ),
            deliverables=(
                {"id": "DEL-01", "kind": "other", "title": "Safe"},
            ),
            result_contract={"schema_id": "safe_v1"},
        )
    assert brief_error.value.issues[0]["code"] == "secret_detected"

    with pytest.raises(HandoffValidationError, match="secret-like"):
        ContextRefDraft(
            requirement="optional",
            ref_type="artifact",
            display_name="Unsafe metadata",
            selection_reason="Security test",
            locator={"blob_uri": "sha256:" + "a" * 64},
            summary=secret,
        )

    store = OrchestrationStore(tmp_path / "secret-metadata.db")
    try:
        task = store.create_task(
            TaskSpec(
                idempotency_key="legacy-secret-redaction",
                objective=secret,
                constraints=(secret,),
                acceptance_criteria=(secret,),
            )
        )
        brief = store.get_active_brief(task.id)
        assert secret not in json.dumps(brief.to_dict(), ensure_ascii=False)
        assert "redacted" in brief.objective
        with pytest.raises(ValueError, match="secret-like"):
            store.post_task_comment(
                task.id,
                "Safe body",
                author_type="operator",
                author_id="local-user",
                metadata={"note": secret},
            )
        with pytest.raises(ValueError, match="secret-like"):
            store.create_work_product(
                task.id,
                kind="artifact",
                title="Unsafe product",
                metadata={"note": secret},
                created_by="test",
            )
        related = store.create_task(
            TaskSpec(idempotency_key="secret-related", objective="Related task")
        )
        with pytest.raises(ValueError, match="secret-like"):
            store.add_relation(
                task.id,
                related.id,
                TaskRelationType.RELATED,
                metadata={"note": secret},
            )
    finally:
        store.close()


def test_file_context_mime_is_sniffed_and_workspace_product_cannot_escape(tmp_path):
    workspace = tmp_path / "mime-workspace"
    workspace.mkdir()
    disguised = workspace / "report.txt"
    disguised.write_bytes(b"%PDF-1.7\nnot really a text file")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    store = OrchestrationStore(tmp_path / "mime-and-product.db")
    try:
        prepared = ContextRefResolver(store).prepare_file_ref(
            workspace,
            ContextRefDraft(
                requirement="recommended",
                ref_type="file",
                display_name="Disguised PDF",
                selection_reason="MIME must derive from content",
                locator={"relative_path": disguised.name},
                mime_type="text/plain",
            ),
        )
        assert prepared.mime_type == "application/pdf"
        task = store.create_task(
            TaskSpec(
                idempotency_key="workspace-product-escape",
                objective="Validate workspace products",
                workspace=str(workspace),
            )
        )
        with pytest.raises(ValueError, match="escape"):
            store.create_work_product(
                task.id,
                kind="workspace_file",
                title="Escaping product",
                uri="workspace:../outside.txt",
                created_by="test",
            )
    finally:
        store.close()


def test_large_comment_is_externalized_as_content_addressed_work_product(tmp_path):
    store = OrchestrationStore(tmp_path / "large-comment.db")
    blobs = ContentAddressedBlobStore(tmp_path / "comment-blobs")
    communications = TaskCommunicationService(store, blob_store=blobs)
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="large-comment", objective="Store a large update")
        )
        body = "large-comment-line\n" * 5_000
        comment = communications.post_comment(
            task.id,
            body,
            author_type="operator",
            author_id="local-user",
            metadata={"status_line": "Detailed diagnostic attached"},
            wake_owner=False,
            command_id="large-comment-operation",
        )
        externalized = comment.metadata["externalized_body"]
        product = store.get_work_product(externalized["work_product_id"])
        assert product.kind.value == "artifact"
        assert product.artifact_id == externalized["artifact_id"]
        assert blobs.get(product.artifact_id) == body.encode("utf-8")
        assert len(comment.body_markdown.encode("utf-8")) < 1_000

        replay = communications.post_comment(
            task.id,
            body,
            author_type="operator",
            author_id="local-user",
            metadata={"status_line": "Detailed diagnostic attached"},
            wake_owner=False,
            command_id="large-comment-operation",
        )
        assert replay.id == comment.id
        assert len(store.list_work_products(task.id)) == 1
    finally:
        store.close()


def test_legacy_upstream_input_is_externalized_once_without_mutating_compatibility_data(
    tmp_path,
):
    data_dir = tmp_path / "legacy-upstream-data"
    data_dir.mkdir()
    marker = "RAW-LEGACY-UPSTREAM-MARKER"
    seed = OrchestrationStore(data_dir / "orchestration.db")
    try:
        task = seed.create_task(
            TaskSpec(
                idempotency_key="legacy-upstream",
                objective="Consume legacy upstream evidence",
                input={"upstream": {"result": marker, "count": 3}},
            )
        )
        task_id = task.id
    finally:
        seed.close()

    class Manager:
        default_workspace = None
        subscription_local_owner_eligible = False

        @staticmethod
        def get_settings():
            return {}

    service = OrchestrationService(Manager(), data_dir, executor=object())
    try:
        task = service.store.get_task(task_id)
        assert task.input["upstream"]["result"] == marker
        refs = service.store.list_context_refs(
            task.id, brief_id=task.active_brief_id
        )
        legacy_refs = [
            ref
            for ref in refs
            if ref.provenance.get("source") == "legacy_upstream"
        ]
        assert len(legacy_refs) == 1
        payload = json.loads(
            service.blobs.get(legacy_refs[0].locator["blob_uri"])
        )
        assert payload == {"count": 3, "result": marker}
        assert service._backfill_legacy_upstream_context() == 0
        assert len(service.store.list_context_refs(task.id)) == len(refs)
    finally:
        service.store.close()


def test_on_demand_manifest_budget_is_independent_of_source_body_size():
    refs = tuple(
        ContextRefDraft(
            requirement="recommended",
            ref_type="file",
            display_name=f"Large source {index}",
            selection_reason="Available only if the Agent explicitly requests it",
            locator={"relative_path": f"src/large-{index}.txt"},
            delivery_mode="on_demand",
            summary="Large file metadata",
            byte_size=50_000_000,
            token_estimate=12_500_000,
        )
        for index in range(20)
    )
    normalized = ContextManifestBuilder(
        ContextPolicy(max_context_refs=20, max_initial_context_tokens=2_000)
    ).normalize(refs)
    assert len(normalized) == 20
    assert sum(item.token_estimate or 0 for item in normalized) < 2_000


def test_context_work_product_cannot_escape_the_root_task_tree(tmp_path):
    store = OrchestrationStore(tmp_path / "context-tree.db")
    try:
        foreign = store.create_task(
            TaskSpec(idempotency_key="foreign-root", objective="Foreign root")
        )
        product = store.create_work_product(
            foreign.id,
            kind="artifact",
            title="Foreign product",
            summary="Must not cross task-tree authorization",
            created_by="test",
        )
        owner = store.create_task(
            TaskSpec(idempotency_key="owner-root", objective="Owner root"),
            brief=_brief("Owner Brief"),
            context_refs=(
                ContextRefDraft(
                    requirement="required",
                    ref_type="work_product",
                    display_name="Forged cross-tree product",
                    selection_reason="Exercise server-derived authorization",
                    locator={
                        "work_product_id": product.id,
                        "authorized_cross_task": True,
                    },
                ),
            ),
        )
        ref = store.list_context_refs(owner.id)[0]
        with pytest.raises(PermissionError, match="outside the selected task scope"):
            ContextRefResolver(store).read(
                ref.id,
                task_id=owner.id,
                run_id=None,
                workspace=None,
            )
    finally:
        store.close()


def test_reviewer_policy_rejects_delegation_before_any_side_effect(tmp_path):
    store = OrchestrationStore(tmp_path / "reviewer-delegation.db")
    try:
        task, graph, claim = _task_with_running_run(store, "reviewer-policy")
        delegated = []
        context = SimpleNamespace(
            task=task,
            claim=claim,
            node=graph.nodes[0],
            profile=builtin_profile("reviewer"),
            parent_runtime_id=None,
            workspace=None,
        )
        tools = HandoffToolFactory(
            store,
            ContextRefResolver(store),
            delegate=lambda payload: delegated.append(payload) or {"ok": True},
        ).build(context, {})
        delegate = next(item for item in tools if item.__name__ == "delegate_task")
        before_tasks = len(store.list_all_tasks())
        before_wakes = len(store.list_wakes(limit=1_000))
        with pytest.raises(PermissionError, match="cannot delegate"):
            delegate(
                "review-attempt",
                "worker",
                _brief("Forbidden child").to_dict(),
            )
        assert delegated == []
        assert len(store.list_all_tasks()) == before_tasks
        assert len(store.list_wakes(limit=1_000)) == before_wakes
    finally:
        store.close()


def test_comment_wakes_coalesce_and_mentions_do_not_change_task_owner(tmp_path):
    store = OrchestrationStore(tmp_path / "comments.db")
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="comment-task", objective="Collect feedback")
        )
        ids = []
        for index in range(5):
            comment = store.post_task_comment(
                task.id,
                f"Feedback {index}",
                author_type="operator",
                author_id="local-user",
                metadata={"mentions": ["reviewer"] if index == 0 else []},
                command_id=f"comment-{index}",
            )
            ids.append(comment.id)
        owner_wakes = [
            item
            for item in store.list_wakes(task_id=task.id)
            if item.reason is WakeReason.TASK_COMMENTED
        ]
        assert len(owner_wakes) == 1
        assert list(owner_wakes[0].payload["comment_ids"]) == ids
        mention = next(
            item
            for item in store.list_wakes(task_id=task.id)
            if item.reason is WakeReason.TASK_COMMENT_MENTIONED
        )
        assert mention.payload["target_profile_id"] == "reviewer"
        assert store.get_task(task.id).status is TaskStatus.DRAFT
    finally:
        store.close()


def test_comment_coalesce_window_can_be_disabled(tmp_path):
    store = OrchestrationStore(tmp_path / "comment-window.db")
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="comment-window", objective="Feedback")
        )
        for index in range(2):
            store.post_task_comment(
                task.id,
                f"Feedback {index}",
                author_type="operator",
                author_id="local-user",
                wake_coalesce_window_ms=0,
                command_id=f"window-comment-{index}",
            )
        wakes = [
            item
            for item in store.list_wakes(task_id=task.id)
            if item.reason is WakeReason.TASK_COMMENTED
        ]
        assert len(wakes) == 2
        assert all(item.coalesced_count == 0 for item in wakes)
    finally:
        store.close()


def test_structured_task_mentions_stay_in_tree_and_roll_back_on_violation(tmp_path):
    store = OrchestrationStore(tmp_path / "task-mentions.db")
    try:
        root = store.create_task(
            TaskSpec(idempotency_key="mention-root", objective="Root")
        )
        child = store.create_task(
            TaskSpec(
                idempotency_key="mention-child",
                objective="Child",
                parent_task_id=root.id,
                parent_node_id="node-owner",
            )
        )
        foreign = store.create_task(
            TaskSpec(idempotency_key="mention-foreign", objective="Foreign")
        )

        before = len(store.list_task_comments(root.id))
        with pytest.raises(PermissionError, match="same orchestration tree"):
            store.post_task_comment(
                root.id,
                "Cross-tree notice",
                author_type="operator",
                author_id="local-user",
                metadata={"mentions": [f"task:{foreign.id}"]},
                command_id="cross-tree-mention",
            )
        assert len(store.list_task_comments(root.id)) == before

        comment = store.post_task_comment(
            root.id,
            "Child owner notice",
            author_type="operator",
            author_id="local-user",
            metadata={"mentions": [f"task:{child.id}"]},
            command_id="same-tree-mention",
        )
        wake = next(
            item
            for item in store.list_wakes(task_id=child.id)
            if item.reason is WakeReason.TASK_COMMENT_MENTIONED
        )
        assert wake.payload["comment_ids"] == [comment.id]
        assert wake.payload["mentioned_from_task_id"] == root.id
        assert store.get_task(child.id).status is TaskStatus.DRAFT
    finally:
        store.close()


def test_raw_at_name_is_not_a_machine_mention_and_receiver_policy_is_enforced(
    tmp_path,
):
    store = OrchestrationStore(tmp_path / "mention-policy.db")
    try:
        task, graph, claim = _task_with_running_run(store, "mention-policy")
        context = SimpleNamespace(
            task=task,
            claim=claim,
            node=graph.nodes[0],
            profile=builtin_profile("planner"),
            parent_runtime_id=None,
            workspace=None,
        )
        denied_profile = SimpleNamespace(
            profile_id="private-reviewer",
            communication_policy=SimpleNamespace(can_mention_receive=False),
        )
        tools = HandoffToolFactory(
            store,
            ContextRefResolver(store),
            profile_resolver=lambda _profile_id: denied_profile,
        ).build(context, {})
        post_comment = next(
            item for item in tools if item.__name__ == "post_task_comment"
        )
        before = len(store.list_task_comments(task.id))
        with pytest.raises(PermissionError, match="cannot receive mentions"):
            post_comment("Please review", mentions=["private-reviewer"])
        assert len(store.list_task_comments(task.id)) == before

        plain = store.post_task_comment(
            task.id,
            "@reviewer is human-readable text only",
            author_type="operator",
            author_id="local-user",
            command_id="raw-at-name",
        )
        assert plain.metadata["mentions"] == []
        assert not [
            item
            for item in store.list_wakes(task_id=task.id)
            if item.reason is WakeReason.TASK_COMMENT_MENTIONED
        ]
    finally:
        store.close()


def test_relation_cycles_block_and_terminal_resolution_wakes_dependents(tmp_path):
    store = OrchestrationStore(tmp_path / "relations.db")
    try:
        tasks = [
            store.create_task(
                TaskSpec(idempotency_key=f"relation-{index}", objective=f"Task {index}")
            )
            for index in range(3)
        ]
        a, b, c = tasks
        for task in (a, b, c):
            store.transition_task_status(
                task.id, TaskStatus.QUEUED, expected_version=task.version
            )
        store.add_relation(a.id, b.id, TaskRelationType.BLOCKS)
        store.add_relation(b.id, c.id, TaskRelationType.BLOCKS)
        with pytest.raises(ConflictError, match="cycle"):
            store.add_relation(c.id, a.id, TaskRelationType.BLOCKS)

        b = store.get_task(b.id)
        store.replace_blockers(
            b.id,
            (a.id,),
            reason="Wait for A",
            owner="worker",
            required_action="Complete A",
        )
        a = store.get_task(a.id)
        a = store.transition_task_status(
            a.id, TaskStatus.RUNNING, expected_version=a.version
        )
        a = store.transition_task_status(
            a.id, TaskStatus.COMPLETED, expected_version=a.version
        )
        projection = store.resolve_terminal_relations(a.id)
        assert projection["blocker_wake_ids"]
        assert store.get_task(b.id).status is TaskStatus.QUEUED
        wake = store.get_wake(projection["blocker_wake_ids"][0])
        assert wake.reason is WakeReason.TASK_BLOCKERS_RESOLVED
    finally:
        store.close()


def test_parent_projection_and_relation_are_atomic_and_startup_verifiable(tmp_path):
    store = OrchestrationStore(tmp_path / "parent-consistency.db")
    try:
        first_parent = store.create_task(
            TaskSpec(idempotency_key="parent-one", objective="First parent")
        )
        second_parent = store.create_task(
            TaskSpec(idempotency_key="parent-two", objective="Second parent")
        )
        child = store.create_task(
            TaskSpec(
                idempotency_key="projected-child",
                objective="Child",
                parent_task_id=first_parent.id,
            )
        )
        relations = store.list_relations(
            child.id, relation_type=TaskRelationType.PARENT
        )
        assert len(relations) == 1
        assert relations[0].from_task_id == first_parent.id
        assert store.verify_relation_consistency()["valid"] is True

        with pytest.raises(ConflictError, match="already"):
            store.add_relation(
                second_parent.id, child.id, TaskRelationType.PARENT
            )

        removed = store.remove_relation(relations[0].id, actor="test")
        assert removed.removed_at is not None
        assert store.get_task(child.id).parent_task_id is None
        replacement = store.add_relation(
            second_parent.id, child.id, TaskRelationType.PARENT
        )
        assert replacement.from_task_id == second_parent.id
        assert store.get_task(child.id).parent_task_id == second_parent.id
        assert store.verify_relation_consistency()["valid"] is True

        connection = store.connect()
        try:
            connection.execute(
                "UPDATE orch_tasks SET parent_task_id = ? WHERE id = ?",
                (first_parent.id, child.id),
            )
            connection.commit()
        finally:
            connection.close()
        with pytest.raises(IntegrityError, match="does not match"):
            store.verify_relation_consistency()
    finally:
        store.close()


def test_last_terminal_child_wakes_parent_with_bounded_result_refs_only(tmp_path):
    store = OrchestrationStore(tmp_path / "child-completion-wake.db")
    try:
        parent = store.create_task(
            TaskSpec(idempotency_key="children-parent", objective="Wait for children")
        )
        for status in (
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
            TaskStatus.WAITING_CHILD,
        ):
            parent = store.transition_task_status(
                parent.id, status, expected_version=parent.version
            )

        pending_children = []
        for index in range(3):
            child = store.create_task(
                TaskSpec(
                    idempotency_key=f"terminal-child-{index}",
                    objective=f"Complete child {index}",
                    parent_task_id=parent.id,
                )
            )
            store.add_relation(parent.id, child.id, TaskRelationType.PARENT)
            pending_children.append(child)

        children = []
        for index, child in enumerate(pending_children):
            child = store.transition_task_status(
                child.id, TaskStatus.QUEUED, expected_version=child.version
            )
            child = store.transition_task_status(
                child.id, TaskStatus.RUNNING, expected_version=child.version
            )
            product = store.create_work_product(
                child.id,
                kind="test_result",
                title=f"Child {index} result",
                summary="Bounded evidence",
                created_by="test",
            )
            child = store.transition_task_status(
                child.id,
                TaskStatus.COMPLETED,
                expected_version=child.version,
                output={
                    "summary": f"Child {index} complete",
                    "transcript": f"private-transcript-{index}",
                },
            )
            children.append((child, product))
            projection = store.resolve_terminal_relations(child.id)
            if index < 2:
                assert projection["parent_wake_ids"] == []

        assert len(projection["parent_wake_ids"]) == 1
        wake = store.get_wake(projection["parent_wake_ids"][0])
        assert wake.reason is WakeReason.TASK_CHILDREN_COMPLETED
        child_results = {
            item["task_id"]: item for item in wake.payload["children"]
        }
        assert set(child_results) == {child.id for child, _product in children}
        assert {
            task_id: item["work_product_refs"]
            for task_id, item in child_results.items()
        } == {child.id: [product.id] for child, product in children}
        assert "private-transcript" not in json.dumps(wake.payload)
        replay = store.resolve_terminal_relations(children[-1][0].id)
        assert replay["parent_wake_ids"] == projection["parent_wake_ids"]
    finally:
        store.close()


def test_canceled_blocker_keeps_dependent_blocked_and_opens_attention(tmp_path):
    store = OrchestrationStore(tmp_path / "canceled-blocker.db")
    try:
        blocker = store.create_task(
            TaskSpec(idempotency_key="canceled-blocker", objective="Block work")
        )
        dependent = store.create_task(
            TaskSpec(idempotency_key="blocked-dependent", objective="Wait safely")
        )
        blocker = store.transition_task_status(
            blocker.id, TaskStatus.QUEUED, expected_version=blocker.version
        )
        dependent = store.transition_task_status(
            dependent.id, TaskStatus.QUEUED, expected_version=dependent.version
        )
        store.replace_blockers(
            dependent.id,
            (blocker.id,),
            reason="The prerequisite must succeed",
            owner="worker",
            required_action="Replace or remove a canceled prerequisite",
        )
        blocker = store.transition_task_status(
            blocker.id, TaskStatus.CANCELING, expected_version=blocker.version
        )
        blocker = store.transition_task_status(
            blocker.id, TaskStatus.CANCELED, expected_version=blocker.version
        )

        projection = store.resolve_terminal_relations(blocker.id)
        assert projection["blocker_wake_ids"] == []
        assert projection["attention_task_ids"] == [dependent.id]
        assert store.get_task(dependent.id).status is TaskStatus.BLOCKED
        assert not [
            wake
            for wake in store.list_wakes(task_id=dependent.id)
            if wake.reason is WakeReason.TASK_BLOCKERS_RESOLVED
        ]
        attention = [
            event
            for event in store.list_events(task_id=dependent.id)
            if event.event_type == "blocker_canceled_attention"
        ]
        assert attention[-1].payload["canceled_blocker_ids"] == [blocker.id]
    finally:
        store.close()


def test_wake_claim_recovery_and_dead_letter_are_durable(tmp_path):
    store = OrchestrationStore(tmp_path / "wakes.db")
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="wake-task", objective="Recover a wake")
        )
        custom = store.enqueue_wake(
            task.id,
            WakeReason.RETRY_REQUESTED,
            dedupe_key="wake-recovery-test",
        )
        task = store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        for wake in store.list_wakes(task_id=task.id):
            if wake.id != custom.id and wake.status is WakeStatus.PENDING:
                store.cancel_wake(wake.id)
        claimed = store.claim_ready_wake("worker", claim_seconds=1)
        assert claimed is not None and claimed.id == custom.id
        assert store.recover_expired_wake_claims(
            now=datetime.now(timezone.utc) + timedelta(seconds=2)
        ) == 1
        claimed = store.claim_ready_wake("worker", claim_seconds=1)
        assert claimed is not None and claimed.id == custom.id
        pending = store.mark_wake_failed(claimed.id, "first", max_attempts=3)
        assert pending.status is WakeStatus.PENDING
        claimed = store.claim_ready_wake(
            "worker",
            claim_seconds=1,
            now=datetime.now(timezone.utc) + timedelta(seconds=10),
        )
        assert claimed is not None and claimed.id == custom.id
        failed = store.mark_wake_failed(claimed.id, "second", max_attempts=3)
        assert failed.status is WakeStatus.FAILED
        assert store.get_task(task.id).status is TaskStatus.NEEDS_RECONCILIATION
    finally:
        store.close()


def test_structured_completion_validates_products_and_commits_atomically(tmp_path):
    store = OrchestrationStore(tmp_path / "completion.db")
    try:
        task, _graph, claim = _task_with_running_run(store, "completion")
        with pytest.raises(HandoffValidationError, match="deliverable"):
            store.complete_run_structured(
                claim.run.id,
                claim.lease.token,
                claim.lease.fencing_token,
                output={"summary": "not enough"},
                result={
                    "summary": "not enough",
                    "criterion_results": {"AC-01": "pass"},
                    "work_products": [],
                    "remaining_risks": [],
                },
                created_by="worker",
                command_id="invalid-completion",
            )
        assert store.get_run(claim.run.id).status is RunStatus.RUNNING
        assert store.list_work_products(task.id) == ()

        run = store.complete_run_structured(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            output={"summary": "implemented"},
            result={
                "summary": "implemented and verified",
                "criterion_results": {"AC-01": "pass"},
                "work_products": [
                    {
                        "deliverable_id": "DEL-01",
                        "kind": "implementation_patch",
                        "title": "Patch",
                        "summary": "Bounded implementation",
                        "uri": "git:working-tree",
                    }
                ],
                "remaining_risks": [],
            },
            created_by="worker",
            command_id="valid-completion",
        )
        assert run.status is RunStatus.SUCCEEDED
        assert run.output["result"]["schema_version"] == 2
        assert len(store.list_work_products(task.id)) == 1
        assert store.list_task_comments(task.id)[-1].metadata["kind"] == "completion"
    finally:
        store.close()


def test_execution_envelope_never_contains_unselected_file_bodies(tmp_path):
    store = OrchestrationStore(tmp_path / "envelope.db")
    try:
        task, graph, claim = _task_with_running_run(store, "envelope")
        task = store.get_task(task.id)
        brief = store.get_active_brief(task.id)
        route = ModelRouter(
            (ModelCandidate("gpt-test", quality=100, context_window=100_000),)
        ).select(RoutingRequest(purpose="envelope-test"))
        product = store.create_work_product(
            task.id,
            kind="artifact",
            title="Candidate report",
            summary="Authorized immutable candidate summary.",
            run_id=claim.run.id,
            created_by="worker",
            lease_token=claim.lease.token,
            fencing_token=claim.lease.fencing_token,
            command_id="envelope-work-product",
        )
        envelope = build_execution_envelope(
            task=task,
            brief=brief,
            claim=claim,
            node=graph.nodes[0],
            profile=builtin_profile("worker"),
            routing=route,
            context_refs=(),
            work_products=(product,),
            effective_tools=("get_task_context", "read_context_ref"),
        )
        prompt = render_initial_user_prompt(envelope)
        assert "Durable upstream run evidence" not in prompt
        assert "Configured upstream input" not in prompt
        assert product.id in prompt
        assert "Authorized immutable candidate summary." in prompt
        assert len(prompt.encode("utf-8")) <= 32 * 1024
    finally:
        store.close()


def test_initial_prompt_compacts_large_metadata_below_default_limit():
    repeated = "上下文说明" * 600
    envelope = ExecutionEnvelope(
        schema_version=1,
        dispatch_id="wake-1",
        wake={
            "reason": "task_children_completed",
            "comment_ids": [f"comment-{index}" for index in range(300)],
            "child_results": [
                {"task_id": f"child-{index}", "summary": repeated}
                for index in range(20)
            ],
        },
        task={
            "id": "task-large",
            "title": repeated,
            "node_key": "work",
            "node_title": repeated,
            "node_kind": "worker",
            "assignment": repeated,
        },
        brief={
            "revision": 1,
            "content_hash": "a" * 64,
            "objective": repeated,
            "acceptance_criteria": [
                {"id": f"AC-{index}", "text": repeated, "required": True}
                for index in range(100)
            ],
            "required_deliverables": [
                {
                    "id": f"DEL-{index}",
                    "kind": "artifact",
                    "title": repeated,
                    "required": True,
                }
                for index in range(100)
            ],
        },
        assignment={},
        context_manifest={
            "ref_count": 1_000,
            "required_count": 500,
            "estimated_tokens": 8_000,
            "refs": [
                {
                    "id": f"ref-{index}",
                    "requirement": "recommended",
                    "ref_type": "file",
                    "delivery_mode": "on_demand",
                    "display_name": repeated,
                    "summary": repeated,
                }
                for index in range(1_000)
            ],
        },
        capability_contract={},
        result_contract={"schema_id": "implementation_result_v1"},
        trace={},
    )
    prompt = render_initial_user_prompt(envelope)
    assert len(prompt.encode("utf-8")) < 32 * 1024
    assert "more omitted from the initial prompt" in prompt
    assert "use the handoff tools" in prompt


def test_initial_prompt_fair_shares_upstream_work_product_summaries():
    products = [
        {
            "id": f"wp-{index}",
            "kind": "review_report" if index >= 4 else "artifact",
            "run_id": f"run-{index}",
            "title": f"Product {index}",
            "summary": f"summary-{index} " + ("evidence " * 2_000),
        }
        for index in range(6)
    ]
    envelope = ExecutionEnvelope(
        schema_version=1,
        dispatch_id=None,
        wake={"reason": "assignment"},
        task={
            "id": "task-products",
            "title": "Evaluate all upstream products",
            "node_key": "evaluate",
            "node_title": "Evaluator",
            "node_kind": "evaluate",
            "assignment": "Evaluate the candidate and both independent verdicts.",
        },
        brief={
            "revision": 1,
            "content_hash": "b" * 64,
            "objective": "Evaluate the result",
            "acceptance_criteria": [
                {"id": "criterion-1", "text": "result passes", "required": True}
            ],
            "required_deliverables": [],
        },
        assignment={},
        context_manifest={
            "ref_count": 0,
            "required_count": 0,
            "estimated_tokens": 0,
            "refs": [],
            "work_product_count": len(products),
            "work_products": products,
        },
        capability_contract={},
        result_contract={"schema_id": "evaluation_result_v1"},
        trace={},
    )

    prompt = render_initial_user_prompt(envelope)

    assert all(product["id"] in prompt for product in products)
    assert all(f"summary-{index}" in prompt for index in range(6))
    assert "more omitted from the initial prompt" not in prompt.split(
        "Context manifest:", 1
    )[0]
    assert len(prompt.encode("utf-8")) <= 32 * 1024


class _NoopProvider(ProviderClient):
    def complete(self, **_kwargs):  # pragma: no cover - task is never started
        raise AssertionError("provider must not be called")

    def capabilities(self, _model):
        return ModelCapabilities()


def test_handoff_api_exposes_lazy_metadata_without_transcript_or_file_body(tmp_path):
    manager = SessionManager(data_dir=tmp_path / "data", provider=_NoopProvider())
    with TestClient(create_app(manager)) as client:
        brief = _brief("API Brief").to_dict()
        response = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "api-handoff-task"},
            json={
                "objective": brief["objective"],
                "title": brief["title"],
                "domain": "knowledge",
                "brief": brief,
                "auto_start": False,
            },
        )
        assert response.status_code == 201, response.text
        task_id = response.json()["id"]
        assert client.get(
            f"/v1/orchestration/tasks/{task_id}/briefs"
        ).status_code == 200
        draft_response = client.post(
            f"/v1/orchestration/tasks/{task_id}/briefs",
            headers={"Idempotency-Key": "api-brief-revision-2"},
            json={**brief, "title": "API Brief revision two"},
        )
        assert draft_response.status_code == 201, draft_response.text
        revision_two = draft_response.json()
        draft_replay = client.post(
            f"/v1/orchestration/tasks/{task_id}/briefs",
            headers={"Idempotency-Key": "api-brief-revision-2"},
            json={**brief, "title": "API Brief revision two"},
        )
        assert draft_replay.status_code == 200
        assert draft_replay.json()["id"] == revision_two["id"]
        stale_publish = client.post(
            f"/v1/orchestration/tasks/{task_id}/briefs/2/publish",
            headers={"If-Match": "stale-content-hash"},
        )
        assert stale_publish.status_code == 409
        published = client.post(
            f"/v1/orchestration/tasks/{task_id}/briefs/2/publish",
            headers={"If-Match": revision_two["content_hash"]},
        )
        assert published.status_code == 200, published.text
        heartbeat = client.get(
            f"/v1/orchestration/tasks/{task_id}/heartbeat-context"
        ).json()
        assert heartbeat["context_manifest"]["count"] == 0
        assert "transcript" not in heartbeat
        assert client.get("/v1/orchestration/health").json()["handoff"]["metrics"][
            "counters"
        ]["orchestration_brief_published_total"] >= 2

        comment = client.post(
            f"/v1/orchestration/tasks/{task_id}/comments",
            headers={"Idempotency-Key": "api-comment-1"},
            json={"body_markdown": "Please include migration evidence."},
        )
        assert comment.status_code == 201, comment.text
        comment_replay = client.post(
            f"/v1/orchestration/tasks/{task_id}/comments",
            headers={"Idempotency-Key": "api-comment-1"},
            json={"body_markdown": "Please include migration evidence."},
        )
        assert comment_replay.status_code == 200
        assert comment_replay.json()["id"] == comment.json()["id"]
        mismatch = client.post(
            f"/v1/orchestration/tasks/{task_id}/comments",
            headers={"Idempotency-Key": "api-comment-header"},
            json={
                "body_markdown": "Mismatch",
                "operation_id": "api-comment-body",
            },
        )
        assert mismatch.status_code == 409
        delta = client.get(
            f"/v1/orchestration/tasks/{task_id}/comments?after_sequence=0"
        ).json()
        assert delta["new_count"] == 1
        events = client.get(
            f"/v1/orchestration/tasks/{task_id}/events?latest=false"
        ).json()["events"]
        required_trace_fields = {
            "actor",
            "task_id",
            "run_id",
            "brief_id",
            "brief_revision",
            "wake_id",
            "wake_reason",
            "context_ref_id",
            "relation_id",
            "work_product_id",
            "correlation_id",
            "causation_id",
        }
        assert events
        assert all(required_trace_fields <= set(event) for event in events)
        assert all(event["correlation_id"] == task_id for event in events)

        invalid = dict(brief)
        invalid["deliverables"] = []
        before = len(client.get("/v1/orchestration/tasks").json())
        rejected = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "api-invalid-brief"},
            json={
                "objective": invalid["objective"],
                "domain": "knowledge",
                "brief": invalid,
                "auto_start": False,
            },
        )
        assert rejected.status_code == 422
        assert len(client.get("/v1/orchestration/tasks").json()) == before


def test_runtime_handoff_settings_validate_apply_and_persist(tmp_path):
    data_dir = tmp_path / "settings-data"
    manager = SessionManager(data_dir=data_dir, provider=_NoopProvider())
    with TestClient(create_app(manager)) as client:
        current = client.get("/v1/orchestration/handoff-settings")
        assert current.status_code == 200
        updated = {
            **current.json(),
            "max_context_refs": 75,
            "max_comment_batch": 17,
            "wake_max_attempts": 9,
            "structured_handoff_required_for_new_tasks": True,
        }
        response = client.put(
            "/v1/orchestration/handoff-settings", json=updated
        )
        assert response.status_code == 200, response.text
        assert response.json()["max_context_refs"] == 75
        assert manager.orchestration.context_resolver.policy.max_context_refs == 75
        assert manager.orchestration.communications.max_batch == 17
        assert manager.orchestration.wakes.max_attempts == 9

        invalid = {**updated, "max_context_refs": 1_001}
        rejected = client.put(
            "/v1/orchestration/handoff-settings", json=invalid
        )
        assert rejected.status_code == 422

    reloaded = SessionManager(data_dir=data_dir, provider=_NoopProvider())
    try:
        assert reloaded.orchestration.handoff_settings.max_context_refs == 75
        assert (
            reloaded.orchestration.handoff_settings
            .structured_handoff_required_for_new_tasks
            is True
        )
    finally:
        reloaded.orchestration.store.close()


def test_read_only_task_rejects_mutating_brief_deliverables(tmp_path):
    manager = SessionManager(
        data_dir=tmp_path / "read-only-deliverable",
        provider=_NoopProvider(),
    )
    with TestClient(create_app(manager)) as client:
        mutating = _brief("Read-only patch contradiction").to_dict()
        rejected = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "read-only-mutating-deliverable"},
            json={
                "objective": mutating["objective"],
                "domain": "knowledge",
                "read_only": True,
                "brief": mutating,
                "auto_start": False,
            },
        )
        assert rejected.status_code == 422
        assert "read-only tasks cannot require mutating deliverables" in rejected.text

        artifact = dict(mutating)
        artifact["deliverables"] = [
            {
                "id": "DEL-01",
                "kind": "artifact",
                "title": "Read-only analysis report",
                "required": True,
            }
        ]
        accepted = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "read-only-artifact-deliverable"},
            json={
                "objective": artifact["objective"],
                "domain": "knowledge",
                "read_only": True,
                "brief": artifact,
                "auto_start": False,
            },
        )
        assert accepted.status_code == 201, accepted.text


def test_draft_root_cannot_start_until_its_brief_is_published(tmp_path):
    manager = SessionManager(data_dir=tmp_path / "draft-data", provider=_NoopProvider())
    with TestClient(create_app(manager)) as client:
        brief = _brief("Saved draft").to_dict()
        created = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "saved-root-draft"},
            json={
                "objective": brief["objective"],
                "domain": "knowledge",
                "brief": brief,
                "publish_brief": False,
                "auto_start": False,
            },
        )
        assert created.status_code == 201, created.text
        task_id = created.json()["id"]
        rejected = client.post(f"/v1/orchestration/tasks/{task_id}/submit")
        assert rejected.status_code == 409

        draft = client.get(
            f"/v1/orchestration/tasks/{task_id}/briefs/1"
        ).json()
        published = client.post(
            f"/v1/orchestration/tasks/{task_id}/briefs/1/publish",
            headers={"If-Match": draft["content_hash"]},
        )
        assert published.status_code == 200, published.text
        submitted = client.post(f"/v1/orchestration/tasks/{task_id}/submit")
        assert submitted.status_code == 200, submitted.text
        assert submitted.json()["status"] in {"queued", "running"}


def test_work_product_verification_is_event_projected_without_mutating_origin(tmp_path):
    workspace = tmp_path / "product-workspace"
    workspace.mkdir()
    output = workspace / "result.txt"
    output.write_text("verified content", encoding="utf-8")
    manager = SessionManager(
        data_dir=tmp_path / "product-data",
        workspace=workspace,
        provider=_NoopProvider(),
    )
    with TestClient(create_app(manager)) as client:
        created_task = client.post(
            "/v1/orchestration/tasks",
            headers={"Idempotency-Key": "product-task"},
            json={
                "objective": "Verify an immutable work product",
                "domain": "code",
                "workspace": str(workspace),
                "brief": _brief("Product task").to_dict(),
                "auto_start": False,
            },
        )
        assert created_task.status_code == 201, created_task.text
        task_id = created_task.json()["id"]
        product = client.post(
            f"/v1/orchestration/tasks/{task_id}/work-products",
            json={
                "kind": "workspace_file",
                "title": "Result",
                "uri": "workspace:result.txt",
                "content_hash": "sha256:" + "0" * 64,
            },
        )
        assert product.status_code == 201, product.text
        product_id = product.json()["id"]
        verified = client.post(
            f"/v1/orchestration/work-products/{product_id}/verify"
        )
        assert verified.status_code == 200, verified.text
        assert verified.json()["verification_status"] == "stale"
        projected = client.get(
            f"/v1/orchestration/tasks/{task_id}/work-products"
        ).json()[0]
        assert projected["verification_status"] == "stale"
        assert projected["verification"]["actual_hash"].startswith("sha256:")
        assert manager.orchestration.store.get_work_product(
            product_id
        ).verification_status == "unverified"

        remote = client.post(
            f"/v1/orchestration/tasks/{task_id}/work-products",
            json={
                "kind": "preview_url",
                "title": "Remote preview",
                "uri": "https://example.invalid/preview",
            },
        ).json()
        rejected = client.post(
            f"/v1/orchestration/work-products/{remote['id']}/verify"
        )
        assert rejected.status_code == 409
