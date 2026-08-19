import json

from coworker.orchestration.quality.observability import TaskQualityObservability
from coworker.orchestration.store import OrchestrationStore


REQUIRED_METRICS = {
    "orchestration_task_quality_score",
    "orchestration_quality_gate_failures_total",
    "orchestration_citation_resolution_ratio",
    "orchestration_artifact_read_coverage_ratio",
    "orchestration_duplicate_query_ratio",
    "orchestration_contract_inferred_requirements_total",
    "orchestration_target_resolution_confidence",
    "orchestration_repair_attempts_total",
    "orchestration_repair_outcomes_total",
    "orchestration_budget_utilization_ratio",
    "orchestration_primary_deliverable_missing_total",
}


def test_quality_metrics_contract_is_complete_and_content_free(tmp_path) -> None:
    store = OrchestrationStore(tmp_path / "quality-observability.db")
    try:
        snapshot = TaskQualityObservability(store).snapshot()
        assert REQUIRED_METRICS <= set(snapshot["metrics"])
        assert snapshot["alerts"] == []
        assert snapshot["privacy"] == "content_free_metadata_only"
        serialized = json.dumps(snapshot, sort_keys=True)
        assert "objective" not in serialized
        assert "prompt" not in serialized
        assert str(tmp_path) not in serialized
    finally:
        store.close()


def test_quality_no_go_alert_rules_fail_closed() -> None:
    alerts = TaskQualityObservability.alerts(
        {
            "primary_deliverable_missing": 1,
            "passed_incomplete_reads": 2,
            "pass_with_open_blocking": 1,
            "waived_with_uncovered_blocking": 1,
            "hard_budget_over": 1,
            "low_confidence_started": 1,
            "artifact_hash_failures": 3,
            "repair_exhausted_ratio": 0.25,
            "duplicate_query_ratio": 0.21,
            "schema_adapter_warning_rate_delta": 0.1,
            "quality_score_regression_points": 5.1,
        }
    )
    assert {item["code"] for item in alerts} == {
        "PRIMARY_DELIVERABLE_MISSING",
        "PASSED_ARTIFACT_NOT_FULLY_READ",
        "PASS_WITH_OPEN_BLOCKING_FINDING",
        "WAIVED_WITH_UNCOVERED_BLOCKING_FINDING",
        "HARD_BUDGET_EXCEEDED",
        "LOW_CONFIDENCE_TARGET_STARTED",
        "ARTIFACT_HASH_FAILURE",
        "REPAIR_EXHAUSTED_RATIO",
        "DUPLICATE_SCAN_RATIO",
        "SCHEMA_ADAPTER_WARNING_RATE_SPIKE",
        "QUALITY_SCORE_REGRESSION",
    }
    assert all("prompt" not in item["message"].lower() for item in alerts)


def test_release_alert_facts_are_consumed_without_content(tmp_path) -> None:
    class ReleaseFacts:
        @staticmethod
        def release_observability_facts():
            return {
                "schema_adapter_warning_rate_delta": 0.01,
                "quality_score_regression_points": 6,
            }

    store = OrchestrationStore(tmp_path / "release-alerts.db")
    try:
        snapshot = TaskQualityObservability(
            store, benchmarks=ReleaseFacts()
        ).snapshot()
        assert {item["code"] for item in snapshot["alerts"]} == {
            "SCHEMA_ADAPTER_WARNING_RATE_SPIKE",
            "QUALITY_SCORE_REGRESSION",
        }
        assert snapshot["privacy"] == "content_free_metadata_only"
    finally:
        store.close()
