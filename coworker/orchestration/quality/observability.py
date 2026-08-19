"""Content-free Task Quality metrics and release-safety alerts."""

from __future__ import annotations

import json
from typing import Any, Mapping


_METRIC_NAMES = (
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
)


def _aggregate(values: list[float]) -> dict[str, int | float]:
    return {
        "count": len(values),
        "average": round(sum(values) / len(values), 6) if values else 0,
        "minimum": round(min(values), 6) if values else 0,
        "maximum": round(max(values), 6) if values else 0,
    }


def _mapping(value: str | None) -> dict[str, Any]:
    try:
        decoded = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


class TaskQualityObservability:
    """Derive restart-safe metrics from durable metadata without reading content."""

    def __init__(
        self, store: Any, *, repo_tools: Any = None, benchmarks: Any = None
    ) -> None:
        self.store = store
        self.repo_tools = repo_tools
        self.benchmarks = benchmarks

    def snapshot(self) -> dict[str, Any]:
        with self.store._read() as connection:
            scores = [
                float(row["total"])
                for row in connection.execute(
                    "SELECT total FROM orch_rubric_scores"
                ).fetchall()
            ]
            gate_rows = connection.execute(
                """
                SELECT validator_id, COUNT(*) AS value FROM orch_gate_results
                WHERE status='fail' GROUP BY validator_id ORDER BY validator_id
                """
            ).fetchall()
            citation = connection.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN path<>'' AND blob_hash<>'' AND line_start>0
                                AND line_end>=line_start THEN 1 ELSE 0 END) AS resolved
                FROM orch_evidence_refs
                """
            ).fetchone()
            read_rows = connection.execute(
                """
                SELECT MAX(r.coverage_ratio) AS ratio
                FROM orch_artifact_read_receipts r
                JOIN orch_tasks t ON t.primary_artifact_id=r.artifact_id
                WHERE t.quality_status IN ('pass', 'waived')
                GROUP BY t.id
                """
            ).fetchall()
            inferred_rows = connection.execute(
                """
                SELECT category, COUNT(*) AS value FROM orch_contract_requirements
                WHERE source='inferred' GROUP BY category ORDER BY category
                """
            ).fetchall()
            confidence_values = [
                float(row["resolution_confidence"])
                for row in connection.execute(
                    "SELECT resolution_confidence FROM orch_repository_snapshots"
                ).fetchall()
            ]
            repair_rows = connection.execute(
                """
                SELECT status, COUNT(*) AS value, MAX(attempt) AS max_attempt
                FROM orch_repair_requests GROUP BY status ORDER BY status
                """
            ).fetchall()
            budget_rows = connection.execute(
                """
                SELECT id, task_id, mode, effective_limits_json, consumed_json,
                       reserved_json, over_budget, status
                FROM orch_budget_ledgers
                """
            ).fetchall()
            primary_missing = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS value FROM orch_tasks
                    WHERE active_contract_id IS NOT NULL
                      AND workflow_status='completed' AND primary_artifact_id IS NULL
                    """
                ).fetchone()["value"]
            )
            pass_with_open_blocking = int(
                connection.execute(
                    """
                    SELECT COUNT(DISTINCT t.id) AS value
                    FROM orch_tasks t
                    JOIN orch_quality_findings f ON f.task_id=t.id
                    WHERE t.quality_status='pass' AND f.blocking=1 AND f.status='open'
                    """
                ).fetchone()["value"]
            )
            waived_with_uncovered_blocking = int(
                connection.execute(
                    """
                    SELECT COUNT(DISTINCT t.id) AS value
                    FROM orch_tasks t
                    JOIN orch_quality_contracts c ON c.id=t.active_contract_id
                    JOIN orch_quality_findings f
                      ON f.task_id=t.id AND f.artifact_id=t.primary_artifact_id
                    WHERE t.quality_status='waived'
                      AND f.blocking=1 AND f.status='open'
                      AND (
                        f.category IN ('security', 'schema')
                        OR NOT EXISTS (
                          SELECT 1 FROM orch_quality_waivers w
                          WHERE w.task_id=t.id
                            AND w.artifact_id=f.artifact_id
                            AND w.artifact_hash=f.artifact_hash
                            AND w.contract_id=c.id
                            AND w.contract_version=c.version
                            AND w.subject_type='finding'
                            AND w.subject_id=f.id
                            AND w.subject_version=1
                            AND w.revoked_at IS NULL
                            AND (
                              w.expires_at IS NULL
                              OR w.expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now')
                            )
                        )
                      )
                    """
                ).fetchone()["value"]
            )
            passed_incomplete_reads = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS value FROM orch_tasks t
                    WHERE t.quality_status='pass' AND t.primary_artifact_id IS NOT NULL
                      AND COALESCE((
                        SELECT MAX(r.coverage_ratio)
                        FROM orch_artifact_read_receipts r
                        WHERE r.artifact_id=t.primary_artifact_id
                      ), 0) < 1
                    """
                ).fetchone()["value"]
            )
            low_confidence_started = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS value FROM orch_tasks t
                    JOIN orch_repository_snapshots s ON s.id=t.active_snapshot_id
                    WHERE t.workflow_status NOT IN (
                        'draft', 'analyzing', 'needs_target_selection', 'ready'
                    ) AND s.resolution_confidence < 0.8
                    """
                ).fetchone()["value"]
            )
            artifact_hash_failures = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS value FROM orch_events
                    WHERE event_type='artifact_hash_failed'
                    """
                ).fetchone()["value"]
            )

        citation_total = int(citation["total"] or 0)
        citation_resolved = int(citation["resolved"] or 0)
        citation_ratio = citation_resolved / citation_total if citation_total else 0.0
        read_values = [float(row["ratio"] or 0) for row in read_rows]
        duplicate_ratio = float(
            getattr(
                getattr(self.repo_tools, "metrics", None),
                "duplicate_non_cached_ratio",
                0.0,
            )
        )
        budget_series: dict[str, list[float]] = {}
        hard_budget_over = 0
        for row in budget_rows:
            limits = _mapping(row["effective_limits_json"])
            consumed = _mapping(row["consumed_json"])
            reserved = _mapping(row["reserved_json"])
            for dimension, raw_limit in limits.items():
                if not isinstance(raw_limit, (int, float)) or float(raw_limit) <= 0:
                    continue
                ratio = (
                    float(consumed.get(dimension, 0) or 0)
                    + float(reserved.get(dimension, 0) or 0)
                ) / float(raw_limit)
                budget_series.setdefault(str(dimension), []).append(ratio)
                if row["mode"] == "hard" and ratio > 1:
                    hard_budget_over += 1
            if row["mode"] == "hard" and bool(row["over_budget"]):
                hard_budget_over += 1

        repair_outcomes = {
            str(row["status"]): int(row["value"] or 0) for row in repair_rows
        }
        repair_total = sum(repair_outcomes.values())
        repair_exhausted = repair_outcomes.get("exhausted", 0)
        repair_exhausted_ratio = (
            repair_exhausted / repair_total if repair_total else 0.0
        )
        release_facts = (
            dict(self.benchmarks.release_observability_facts())
            if self.benchmarks is not None
            else {}
        )
        values = {
            "orchestration_task_quality_score": _aggregate(scores),
            "orchestration_quality_gate_failures_total": sum(
                int(row["value"] or 0) for row in gate_rows
            ),
            "orchestration_citation_resolution_ratio": round(citation_ratio, 6),
            "orchestration_artifact_read_coverage_ratio": _aggregate(read_values),
            "orchestration_duplicate_query_ratio": round(duplicate_ratio, 6),
            "orchestration_contract_inferred_requirements_total": sum(
                int(row["value"] or 0) for row in inferred_rows
            ),
            "orchestration_target_resolution_confidence": _aggregate(
                confidence_values
            ),
            "orchestration_repair_attempts_total": repair_total,
            "orchestration_repair_outcomes_total": repair_total,
            "orchestration_budget_utilization_ratio": {
                key: _aggregate(items) for key, items in sorted(budget_series.items())
            },
            "orchestration_primary_deliverable_missing_total": primary_missing,
        }
        assert set(_METRIC_NAMES) <= set(values)
        series = {
            "quality_gate_failures_by_gate": {
                str(row["validator_id"]): int(row["value"] or 0)
                for row in gate_rows
            },
            "contract_inferred_requirements_by_category": {
                str(row["category"]): int(row["value"] or 0)
                for row in inferred_rows
            },
            "repair_outcomes": repair_outcomes,
        }
        alert_facts = {
            "primary_deliverable_missing": primary_missing,
            "passed_incomplete_reads": passed_incomplete_reads,
            "pass_with_open_blocking": pass_with_open_blocking,
            "waived_with_uncovered_blocking": waived_with_uncovered_blocking,
            "hard_budget_over": hard_budget_over,
            "low_confidence_started": low_confidence_started,
            "artifact_hash_failures": artifact_hash_failures,
            "repair_exhausted_ratio": repair_exhausted_ratio,
            "duplicate_query_ratio": duplicate_ratio,
            **release_facts,
        }
        return {
            "schema_version": 1,
            "metrics": values,
            "series": series,
            "alerts": self.alerts(alert_facts),
            "privacy": "content_free_metadata_only",
        }

    @staticmethod
    def alerts(facts: Mapping[str, int | float]) -> list[dict[str, Any]]:
        rules = (
            ("PRIMARY_DELIVERABLE_MISSING", "critical", "primary_deliverable_missing", lambda value: value > 0, "A completed V2 task is missing its primary artifact."),
            ("PASSED_ARTIFACT_NOT_FULLY_READ", "critical", "passed_incomplete_reads", lambda value: value > 0, "A passed task lacks a fresh 100% artifact read receipt."),
            ("PASS_WITH_OPEN_BLOCKING_FINDING", "critical", "pass_with_open_blocking", lambda value: value > 0, "A passed task retains an open blocking finding."),
            ("WAIVED_WITH_UNCOVERED_BLOCKING_FINDING", "critical", "waived_with_uncovered_blocking", lambda value: value > 0, "A waived task retains an uncovered open blocking finding."),
            ("HARD_BUDGET_EXCEEDED", "critical", "hard_budget_over", lambda value: value > 0, "A hard budget ledger exceeded an effective limit."),
            ("LOW_CONFIDENCE_TARGET_STARTED", "high", "low_confidence_started", lambda value: value > 0, "A task started with target confidence below 0.8."),
            ("ARTIFACT_HASH_FAILURE", "critical", "artifact_hash_failures", lambda value: value > 0, "An artifact upload failed integrity validation."),
            ("REPAIR_EXHAUSTED_RATIO", "high", "repair_exhausted_ratio", lambda value: value > 0, "At least one bounded repair loop exhausted."),
            ("DUPLICATE_SCAN_RATIO", "warning", "duplicate_query_ratio", lambda value: value > 0.2, "Non-cached duplicate repository scans exceed 20%."),
            ("SCHEMA_ADAPTER_WARNING_RATE_SPIKE", "high", "schema_adapter_warning_rate_delta", lambda value: value > 0, "Schema adapter warning rate increased versus the promoted release baseline."),
            ("QUALITY_SCORE_REGRESSION", "high", "quality_score_regression_points", lambda value: value > 5, "Quality score regressed by more than five points versus the promoted release baseline."),
        )
        alerts = []
        for code, severity, key, predicate, message in rules:
            observed = float(facts.get(key, 0) or 0)
            if predicate(observed):
                alerts.append(
                    {
                        "code": code,
                        "severity": severity,
                        "observed": observed,
                        "message": message,
                    }
                )
        return alerts
