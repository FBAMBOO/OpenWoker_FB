"""Offline, content-safe Task Quality benchmark suites and durable run records."""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_REGISTERED_NAME = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,79}$")
_MAX_SUITE_BYTES = 1_048_576
_MAX_RUNS = 10_000


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return 0.0 if float(denominator) <= 0 else float(numerator) / float(denominator)


def _is_absolute_path(value: str) -> bool:
    return bool(
        value.startswith(("/", "\\\\"))
        or re.match(r"^[A-Za-z]:", value)
        or "..\\" in value
        or "../" in value
        or "\x00" in value
    )


class BenchmarkValidationError(ValueError):
    """A suite or run request crossed the offline fixture boundary."""


@dataclass(frozen=True)
class BenchmarkSuite:
    id: str
    name: str
    stack: str
    version: int
    snapshot_artifact_id: str
    prompt_hash: str
    oracle: Mapping[str, Any]
    thresholds: Mapping[str, Any]
    candidates: Mapping[str, Mapping[str, Any]]
    baseline_candidate: str
    content_hash: str

    def summary(self, promoted: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "stack": self.stack,
            "version": self.version,
            "snapshot_artifact_id": self.snapshot_artifact_id,
            "prompt_hash": self.prompt_hash,
            "candidate_ids": sorted(self.candidates),
            "baseline_candidate": self.baseline_candidate,
            "thresholds": dict(self.thresholds),
            "content_hash": self.content_hash,
            "promoted_baseline": dict(promoted or {}),
        }


class TaskQualityBenchmarkService:
    """Run only registered, sanitized fixtures and persist content-free metrics.

    A caller selects a suite and candidate by identifier. It cannot provide a host
    path, prompt body, repository content, or provider transcript. The persisted
    state therefore contains hashes, counts and scores only.
    """

    def __init__(self, suite_root: str | Path, state_path: str | Path) -> None:
        self.suite_root = Path(suite_root).resolve()
        self.state_path = Path(state_path).resolve()
        self._lock = threading.RLock()
        self._suites = self._load_suites()
        self._state = self._load_state()

    def _load_suites(self) -> dict[str, BenchmarkSuite]:
        if not self.suite_root.is_dir():
            return {}
        suites: dict[str, BenchmarkSuite] = {}
        for path in sorted(self.suite_root.glob("*/suite.json")):
            resolved = path.resolve()
            try:
                resolved.relative_to(self.suite_root)
            except ValueError as exc:
                raise BenchmarkValidationError("benchmark suite escaped its root") from exc
            if resolved.stat().st_size > _MAX_SUITE_BYTES:
                raise BenchmarkValidationError(f"benchmark suite is too large: {path.name}")
            raw = json.loads(resolved.read_text(encoding="utf-8"))
            suite = self._validate_suite(raw)
            if suite.id in suites:
                raise BenchmarkValidationError(f"duplicate benchmark suite id: {suite.id}")
            suites[suite.id] = suite
        return suites

    @staticmethod
    def _validate_suite(raw: Mapping[str, Any]) -> BenchmarkSuite:
        value = dict(raw)
        identifier = str(value.get("id") or "")
        if not _REGISTERED_NAME.fullmatch(identifier):
            raise BenchmarkValidationError("benchmark suite id is invalid")
        snapshot_artifact_id = str(value.get("snapshot_artifact_id") or "")
        prompt_hash = str(value.get("prompt_hash") or "")
        if not _HASH.fullmatch(snapshot_artifact_id):
            raise BenchmarkValidationError(
                f"suite {identifier} must reference a sanitized snapshot artifact hash"
            )
        if not _HASH.fullmatch(prompt_hash):
            raise BenchmarkValidationError(f"suite {identifier} prompt_hash is invalid")
        candidates = value.get("candidates")
        if not isinstance(candidates, Mapping) or not candidates:
            raise BenchmarkValidationError(f"suite {identifier} has no candidates")
        candidate_defaults = value.get("candidate_defaults") or {}
        if not isinstance(candidate_defaults, Mapping):
            raise BenchmarkValidationError("candidate_defaults must be an object")
        TaskQualityBenchmarkService._reject_paths(candidate_defaults)
        normalized_candidates: dict[str, Mapping[str, Any]] = {}
        for name, raw_candidate in candidates.items():
            candidate_name = str(name)
            if not _REGISTERED_NAME.fullmatch(candidate_name):
                raise BenchmarkValidationError("benchmark candidate id is invalid")
            if not isinstance(raw_candidate, Mapping):
                raise BenchmarkValidationError("benchmark candidate must be an object")
            merged_candidate = {**dict(candidate_defaults), **dict(raw_candidate)}
            TaskQualityBenchmarkService._reject_paths(merged_candidate)
            normalized_candidates[candidate_name] = merged_candidate
        baseline = str(value.get("baseline_candidate") or "legacy")
        if baseline not in normalized_candidates:
            raise BenchmarkValidationError("baseline candidate is not registered")
        oracle = value.get("oracle")
        thresholds = value.get("thresholds")
        if not isinstance(oracle, Mapping) or not isinstance(thresholds, Mapping):
            raise BenchmarkValidationError("suite oracle and thresholds are required")
        canonical = {
            key: value[key]
            for key in sorted(value)
            if key not in {"content_hash", "notes"}
        }
        return BenchmarkSuite(
            id=identifier,
            name=str(value.get("name") or identifier),
            stack=str(value.get("stack") or "unknown"),
            version=max(1, int(value.get("version") or 1)),
            snapshot_artifact_id=snapshot_artifact_id,
            prompt_hash=prompt_hash,
            oracle=dict(oracle),
            thresholds=dict(thresholds),
            candidates=normalized_candidates,
            baseline_candidate=baseline,
            content_hash=_sha(canonical),
        )

    @staticmethod
    def _reject_paths(value: Any, *, key: str = "") -> None:
        if isinstance(value, Mapping):
            for nested_key, nested in value.items():
                TaskQualityBenchmarkService._reject_paths(
                    nested, key=str(nested_key).lower()
                )
            return
        if isinstance(value, (list, tuple)):
            for nested in value:
                TaskQualityBenchmarkService._reject_paths(nested, key=key)
            return
        if isinstance(value, str) and (
            key.endswith(("path", "root", "workspace")) or "path" in key
        ) and _is_absolute_path(value):
            raise BenchmarkValidationError(
                "benchmark fixtures cannot contain production absolute paths"
            )

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"schema_version": 1, "runs": {}, "promoted_baselines": {}}
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BenchmarkValidationError("benchmark state is unreadable") from exc
        if not isinstance(value, Mapping) or int(value.get("schema_version") or 0) != 1:
            raise BenchmarkValidationError("benchmark state schema is unsupported")
        return {
            "schema_version": 1,
            "runs": dict(value.get("runs") or {}),
            "promoted_baselines": dict(value.get("promoted_baselines") or {}),
        }

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_name(
            f".{self.state_path.name}.{uuid.uuid4().hex}.tmp"
        )
        payload = _canonical(self._state)
        try:
            with temporary.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.state_path)
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def list_suites(self) -> list[dict[str, Any]]:
        promoted = dict(self._state.get("promoted_baselines") or {})
        return [
            suite.summary(promoted.get(suite.id))
            for suite in sorted(self._suites.values(), key=lambda item: item.id)
        ]

    def run(self, suite_id: str, *, candidate_id: str = "v2") -> dict[str, Any]:
        if any(token in str(suite_id) for token in ("/", "\\", "..", "\x00")):
            raise BenchmarkValidationError("suite_id must be a registered identifier")
        suite = self._suites.get(str(suite_id))
        if suite is None:
            raise KeyError(f"benchmark suite {suite_id} was not found")
        if any(token in str(candidate_id) for token in ("/", "\\", "..", "\x00")):
            raise BenchmarkValidationError("candidate_id must be a registered identifier")
        candidate = suite.candidates.get(str(candidate_id))
        if candidate is None:
            raise KeyError(
                f"candidate {candidate_id} is not registered for suite {suite_id}"
            )
        metrics, failures = self._evaluate(suite, candidate)
        created_at = _now()
        run_id = f"benchmark_{uuid.uuid4().hex}"
        record = {
            "id": run_id,
            "suite_id": suite.id,
            "suite_version": suite.version,
            "suite_hash": suite.content_hash,
            "snapshot_artifact_id": suite.snapshot_artifact_id,
            "prompt_hash": suite.prompt_hash,
            "candidate_id": str(candidate_id),
            "status": "pass" if not failures else "fail",
            "metrics": metrics,
            "failures": failures,
            "created_at": created_at,
            "completed_at": created_at,
        }
        record["content_hash"] = _sha(record)
        with self._lock:
            runs = dict(self._state.get("runs") or {})
            runs[run_id] = record
            if len(runs) > _MAX_RUNS:
                ordered = sorted(
                    runs.values(), key=lambda item: (str(item["created_at"]), str(item["id"]))
                )
                for old in ordered[: len(runs) - _MAX_RUNS]:
                    runs.pop(str(old["id"]), None)
            self._state["runs"] = runs
            self._save_state()
        return dict(record)

    @staticmethod
    def _evaluate(
        suite: BenchmarkSuite, candidate: Mapping[str, Any]
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        rubric = {
            str(key): float(value)
            for key, value in dict(candidate.get("rubric_scores") or {}).items()
        }
        score = round(sum(rubric.values()), 2)
        required_areas = [str(item) for item in suite.oracle.get("required_areas") or ()]
        covered = {str(item) for item in candidate.get("covered_areas") or ()}
        coverage_count = len(set(required_areas) & covered)
        citations_total = int(candidate.get("citations_total") or 0)
        citations_resolved = int(candidate.get("citations_resolved") or 0)
        priority_claims = int(candidate.get("priority_claims") or 0)
        priority_direct = int(candidate.get("priority_claims_with_direct_evidence") or 0)
        read_bytes = int(candidate.get("reviewer_read_bytes") or 0)
        artifact_bytes = int(candidate.get("artifact_bytes") or 0)
        duplicate_queries = int(candidate.get("duplicate_non_cached_queries") or 0)
        expensive_queries = int(candidate.get("expensive_queries") or 0)
        inventory_expected = dict(suite.oracle.get("inventory") or {})
        inventory_observed = {
            str(key): int(value)
            for key, value in dict(candidate.get("inventory") or {}).items()
        }
        inventory_mismatches = {
            key: {"expected": int(expected), "observed": inventory_observed.get(key)}
            for key, expected in inventory_expected.items()
            if inventory_observed.get(key) != int(expected)
        }
        hard_gate_failures = sorted(
            str(key)
            for key, status in dict(candidate.get("hard_gates") or {}).items()
            if str(status) != "pass"
        )
        metrics = {
            "quality_score": score,
            "rubric_scores": rubric,
            "hard_gate_failures": hard_gate_failures,
            "required_area_coverage": coverage_count,
            "required_area_total": len(required_areas),
            "citation_resolution_ratio": round(
                _ratio(citations_resolved, citations_total), 6
            ),
            "priority_direct_evidence_ratio": round(
                _ratio(priority_direct, priority_claims), 6
            ),
            "artifact_read_coverage_ratio": round(
                _ratio(read_bytes, artifact_bytes), 6
            ),
            "snapshot_correct": bool(candidate.get("snapshot_correct")),
            "baseline_error_count": int(candidate.get("baseline_error_count") or 0),
            "reported_tokens": int(candidate.get("reported_tokens") or 0),
            "tool_calls": int(candidate.get("tool_calls") or 0),
            "elapsed_seconds": float(candidate.get("elapsed_seconds") or 0),
            "duplicate_scan_ratio": round(
                _ratio(duplicate_queries, expensive_queries), 6
            ),
            "repair_attempts": int(candidate.get("repair_attempts") or 0),
            "repair_success_ratio": float(candidate.get("repair_success_ratio") or 0),
            "primary_deliverable_present": bool(
                candidate.get("primary_deliverable_present")
            ),
            "primary_deliverable_mime": str(
                candidate.get("primary_deliverable_mime") or ""
            ),
            "workspace_unchanged": bool(candidate.get("workspace_unchanged")),
            "aggregation_separated": bool(candidate.get("aggregation_separated")),
            "schema_field_loss": int(candidate.get("schema_field_loss") or 0),
            "schema_adapter_warnings": int(
                candidate.get("schema_adapter_warnings") or 0
            ),
            "schema_adapter_inputs": max(
                1, int(candidate.get("schema_adapter_inputs") or 1)
            ),
            "inventory_mismatches": inventory_mismatches,
            "model": str(candidate.get("model") or "offline-fixture"),
            "provider": str(candidate.get("provider") or "offline"),
        }
        thresholds = {
            "quality_score": float(suite.thresholds.get("quality_score") or 85),
            "citation_resolution_ratio": float(
                suite.thresholds.get("citation_resolution_ratio") or 1
            ),
            "artifact_read_coverage_ratio": float(
                suite.thresholds.get("artifact_read_coverage_ratio") or 1
            ),
            "duplicate_scan_ratio": float(
                suite.thresholds.get("duplicate_scan_ratio") or 0.2
            ),
            "reported_tokens": int(
                suite.thresholds.get("reported_tokens") or 3_000_000
            ),
            "tool_calls": int(suite.thresholds.get("tool_calls") or 120),
            "elapsed_seconds": float(
                suite.thresholds.get("elapsed_seconds") or 1_200
            ),
            "repair_success_ratio": float(
                suite.thresholds.get("repair_success_ratio") or 0.9
            ),
        }
        failures: list[dict[str, Any]] = []

        def require(code: str, passed: bool, observed: Any, expected: Any) -> None:
            if not passed:
                failures.append(
                    {
                        "code": code,
                        "observed": observed,
                        "expected": expected,
                    }
                )

        require("QUALITY_SCORE", score >= thresholds["quality_score"], score, f">={thresholds['quality_score']}")
        require("HARD_GATES", not hard_gate_failures, hard_gate_failures, [])
        require("REQUIRED_COVERAGE", coverage_count == len(required_areas), coverage_count, len(required_areas))
        require("CITATION_RESOLUTION", metrics["citation_resolution_ratio"] >= thresholds["citation_resolution_ratio"], metrics["citation_resolution_ratio"], thresholds["citation_resolution_ratio"])
        require("PRIORITY_EVIDENCE", metrics["priority_direct_evidence_ratio"] == 1, metrics["priority_direct_evidence_ratio"], 1)
        require("REVIEW_READ", metrics["artifact_read_coverage_ratio"] >= thresholds["artifact_read_coverage_ratio"], metrics["artifact_read_coverage_ratio"], thresholds["artifact_read_coverage_ratio"])
        require("SNAPSHOT", metrics["snapshot_correct"] and metrics["baseline_error_count"] == 0, {"correct": metrics["snapshot_correct"], "errors": metrics["baseline_error_count"]}, {"correct": True, "errors": 0})
        require("INVENTORY", not inventory_mismatches, inventory_mismatches, {})
        require("DUPLICATE_SCAN", metrics["duplicate_scan_ratio"] <= thresholds["duplicate_scan_ratio"], metrics["duplicate_scan_ratio"], f"<={thresholds['duplicate_scan_ratio']}")
        require("TOKEN_BUDGET", metrics["reported_tokens"] <= thresholds["reported_tokens"], metrics["reported_tokens"], f"<={thresholds['reported_tokens']}")
        require("TOOL_BUDGET", metrics["tool_calls"] <= thresholds["tool_calls"], metrics["tool_calls"], f"<={thresholds['tool_calls']}")
        require("ELAPSED_BUDGET", metrics["elapsed_seconds"] <= thresholds["elapsed_seconds"], metrics["elapsed_seconds"], f"<={thresholds['elapsed_seconds']}")
        require("PRIMARY_DELIVERABLE", metrics["primary_deliverable_present"] and metrics["primary_deliverable_mime"] == "text/markdown", {"present": metrics["primary_deliverable_present"], "mime": metrics["primary_deliverable_mime"]}, {"present": True, "mime": "text/markdown"})
        require("WORKSPACE_UNCHANGED", metrics["workspace_unchanged"], False, True)
        require("AGGREGATION", metrics["aggregation_separated"], False, True)
        require("SCHEMA_FIELD_LOSS", metrics["schema_field_loss"] == 0, metrics["schema_field_loss"], 0)
        if int(candidate.get("injected_repair_cases") or 0) > 0:
            require("REPAIR_EFFECTIVENESS", metrics["repair_success_ratio"] >= thresholds["repair_success_ratio"] and metrics["repair_attempts"] <= 2, {"ratio": metrics["repair_success_ratio"], "attempts": metrics["repair_attempts"]}, {"ratio": f">={thresholds['repair_success_ratio']}", "attempts": "<=2"})
        return metrics, failures

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            value = dict(self._state.get("runs") or {}).get(str(run_id))
        if value is None:
            raise KeyError(f"benchmark run {run_id} was not found")
        return dict(value)

    def comparison(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        suite = self._suites[str(run["suite_id"])]
        promoted = dict(self._state.get("promoted_baselines") or {}).get(suite.id)
        if isinstance(promoted, Mapping) and promoted.get("metrics"):
            baseline_metrics = dict(promoted["metrics"])
            baseline_source = {"kind": "promoted_run", "run_id": promoted.get("run_id")}
        else:
            baseline_metrics, _ = self._evaluate(
                suite, suite.candidates[suite.baseline_candidate]
            )
            baseline_source = {
                "kind": "fixture_candidate",
                "candidate_id": suite.baseline_candidate,
            }
        current = dict(run["metrics"])
        numeric_deltas = {
            key: round(float(current[key]) - float(baseline_metrics[key]), 6)
            for key in sorted(set(current) & set(baseline_metrics))
            if isinstance(current[key], (int, float))
            and not isinstance(current[key], bool)
            and isinstance(baseline_metrics[key], (int, float))
            and not isinstance(baseline_metrics[key], bool)
        }
        return {
            "run_id": run_id,
            "suite_id": suite.id,
            "candidate_id": run["candidate_id"],
            "baseline": baseline_source,
            "current_metrics": current,
            "baseline_metrics": baseline_metrics,
            "deltas": numeric_deltas,
            "quality_score_regression": numeric_deltas.get("quality_score", 0) < -5,
        }

    def release_observability_facts(self) -> dict[str, float]:
        """Return content-free deltas for release alerts.

        Only the newest V2 run per suite participates. Quality regression is
        compared with an explicitly promoted previous-release baseline; absent a
        promoted baseline it remains unknown/zero instead of comparing against the
        intentionally weak legacy characterization candidate.
        """

        with self._lock:
            runs = [
                dict(item)
                for item in dict(self._state.get("runs") or {}).values()
                if str(item.get("candidate_id")) == "v2"
            ]
            promoted = dict(self._state.get("promoted_baselines") or {})
        latest: dict[str, dict[str, Any]] = {}
        for run in runs:
            suite_id = str(run.get("suite_id") or "")
            previous = latest.get(suite_id)
            if previous is None or (
                str(run.get("created_at") or ""), str(run.get("id") or "")
            ) > (
                str(previous.get("created_at") or ""),
                str(previous.get("id") or ""),
            ):
                latest[suite_id] = run

        warning_count = 0
        adapter_inputs = 0
        baseline_warning_count = 0
        baseline_adapter_inputs = 0
        quality_regression_points = 0.0
        for suite_id, run in latest.items():
            metrics = dict(run.get("metrics") or {})
            warning_count += int(metrics.get("schema_adapter_warnings") or 0)
            adapter_inputs += max(1, int(metrics.get("schema_adapter_inputs") or 1))
            baseline = promoted.get(suite_id)
            if not isinstance(baseline, Mapping):
                continue
            baseline_metrics = dict(baseline.get("metrics") or {})
            baseline_warning_count += int(
                baseline_metrics.get("schema_adapter_warnings") or 0
            )
            baseline_adapter_inputs += max(
                1, int(baseline_metrics.get("schema_adapter_inputs") or 1)
            )
            quality_regression_points = max(
                quality_regression_points,
                float(baseline_metrics.get("quality_score") or 0)
                - float(metrics.get("quality_score") or 0),
            )
        current_rate = _ratio(warning_count, adapter_inputs)
        baseline_rate = _ratio(baseline_warning_count, baseline_adapter_inputs)
        return {
            "schema_adapter_warning_rate": round(current_rate, 6),
            "schema_adapter_warning_rate_delta": round(
                max(0.0, current_rate - baseline_rate), 6
            ),
            "quality_score_regression_points": round(
                quality_regression_points, 6
            ),
        }

    def promote_baseline(
        self,
        suite_id: str,
        *,
        run_id: str,
        actor_id: str,
        actor_role: str,
        reason: str,
    ) -> dict[str, Any]:
        if str(actor_role) != "admin":
            raise PermissionError("only an admin may promote a benchmark baseline")
        if not str(actor_id).strip() or not str(reason).strip():
            raise BenchmarkValidationError("baseline promotion requires actor_id and reason")
        suite = self._suites.get(str(suite_id))
        if suite is None:
            raise KeyError(f"benchmark suite {suite_id} was not found")
        run = self.get_run(run_id)
        if run["suite_id"] != suite.id:
            raise BenchmarkValidationError("benchmark run belongs to another suite")
        if run["status"] != "pass":
            raise BenchmarkValidationError("only a passing benchmark run may be promoted")
        record = {
            "suite_id": suite.id,
            "suite_version": suite.version,
            "suite_hash": suite.content_hash,
            "run_id": run_id,
            "candidate_id": run["candidate_id"],
            "metrics": dict(run["metrics"]),
            "actor_id": str(actor_id).strip(),
            "actor_role": "admin",
            "reason": str(reason).strip(),
            "promoted_at": _now(),
        }
        record["content_hash"] = _sha(record)
        with self._lock:
            baselines = dict(self._state.get("promoted_baselines") or {})
            baselines[suite.id] = record
            self._state["promoted_baselines"] = baselines
            self._save_state()
        return dict(record)
