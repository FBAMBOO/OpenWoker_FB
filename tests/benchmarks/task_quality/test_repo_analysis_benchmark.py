from __future__ import annotations

import json
from pathlib import Path

import pytest

from coworker.orchestration.quality import benchmark as benchmark_module
from coworker.orchestration.quality.benchmark import (
    BenchmarkValidationError,
    TaskQualityBenchmarkService,
)


SUITES = Path(benchmark_module.__file__).resolve().parent / "benchmark_suites"
TEST12 = Path(__file__).resolve().parent / "fixtures" / "test12"


def _service(tmp_path: Path) -> TaskQualityBenchmarkService:
    return TaskQualityBenchmarkService(SUITES, tmp_path / "benchmark-state.json")


def test_test12_fixture_is_offline_sanitized_and_captures_locked_oracle() -> None:
    oracle = json.loads((TEST12 / "oracle.json").read_text(encoding="utf-8"))
    manifest = json.loads(
        (TEST12 / "snapshot_manifest.json").read_text(encoding="utf-8")
    )
    characterization = json.loads(
        (TEST12 / "characterization.json").read_text(encoding="utf-8")
    )
    assert oracle["commit_oid"] == "2b2360f32117cc5b234e63230e1ae6741a64be70"
    assert oracle["inventory"] == {
        "models": 228,
        "macro_sql": 52,
        "sql_tests": 42,
        "seeds": 5,
        "snapshots": 2,
        "pipeline_yaml": 15,
    }
    assert len(oracle["required_areas"]) == 7
    assert manifest["network_used"] is False
    assert all(not Path(item["path"]).is_absolute() for item in manifest["files"])
    assert characterization["provider_transcript_included"] is False
    assert characterization["absolute_paths_included"] is False
    assert "Limitations" in (TEST12 / "GOLD_REPORT.md").read_text(encoding="utf-8")


def test_v2_repo_analysis_corpus_meets_go_thresholds_and_legacy_is_characterized(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    suites = service.list_suites()
    assert {item["stack"] for item in suites} == {
        "fabric-dbt",
        "python-fastapi",
        "typescript-react",
        "go-service",
        "java-spring",
    }
    for suite in suites:
        current = service.run(suite["id"], candidate_id="v2")
        assert current["status"] == "pass", current
        assert current["metrics"]["quality_score"] >= 85
        assert current["metrics"]["hard_gate_failures"] == []
        assert current["metrics"]["citation_resolution_ratio"] == 1
        assert current["metrics"]["artifact_read_coverage_ratio"] == 1
        assert current["metrics"]["duplicate_scan_ratio"] <= 0.2
        assert current["metrics"]["primary_deliverable_present"] is True
        legacy = service.run(suite["id"], candidate_id="legacy")
        assert legacy["status"] == "fail"
        assert legacy["failures"]
        serialized = json.dumps(current, sort_keys=True)
        assert str(tmp_path) not in serialized
        assert "provider_transcript" not in serialized


def test_test12_metrics_and_defect_injections_fail_closed(tmp_path: Path) -> None:
    service = _service(tmp_path)
    run = service.run("test12", candidate_id="v2")
    assert run["metrics"] == {
        **run["metrics"],
        "required_area_coverage": 7,
        "required_area_total": 7,
        "reported_tokens": 1_800_000,
        "tool_calls": 88,
        "elapsed_seconds": 720.0,
        "repair_attempts": 1,
        "repair_success_ratio": 0.9,
        "schema_field_loss": 0,
        "inventory_mismatches": {},
    }

    suite = service._suites["test12"]
    broken = dict(suite.candidates["v2"])
    broken.update(
        {
            "citations_resolved": 64,
            "reviewer_read_bytes": 13_107,
            "snapshot_correct": False,
            "inventory": {**dict(broken["inventory"]), "models": 205},
            "hard_gates": {
                **dict(broken["hard_gates"]),
                "QG002": "fail",
                "QG010": "fail",
                "QG011": "fail",
                "QG013": "fail",
            },
        }
    )
    _, failures = service._evaluate(suite, broken)
    codes = {item["code"] for item in failures}
    assert {"HARD_GATES", "CITATION_RESOLUTION", "REVIEW_READ", "SNAPSHOT", "INVENTORY"} <= codes


def test_comparison_and_admin_baseline_promotion_are_durable(tmp_path: Path) -> None:
    service = _service(tmp_path)
    run = service.run("test12", candidate_id="v2")
    comparison = service.comparison(run["id"])
    assert comparison["deltas"]["quality_score"] > 5
    assert comparison["quality_score_regression"] is False
    with pytest.raises(PermissionError):
        service.promote_baseline(
            "test12",
            run_id=run["id"],
            actor_id="operator",
            actor_role="quality_owner",
            reason="not authorized",
        )
    promoted = service.promote_baseline(
        "test12",
        run_id=run["id"],
        actor_id="release-admin",
        actor_role="admin",
        reason="Release candidate passed the approved corpus",
    )
    assert promoted["content_hash"].startswith("sha256:")
    reopened = _service(tmp_path)
    assert reopened.get_run(run["id"])["content_hash"] == run["content_hash"]
    suites = {item["id"]: item for item in reopened.list_suites()}
    assert suites["test12"]["promoted_baseline"]["run_id"] == run["id"]


def test_suite_loader_rejects_absolute_production_paths(tmp_path: Path) -> None:
    suite_dir = tmp_path / "suites" / "unsafe"
    suite_dir.mkdir(parents=True)
    value = json.loads((SUITES / "python_fastapi" / "suite.json").read_text(encoding="utf-8"))
    value["id"] = "unsafe"
    value["candidates"]["v2"] = {"workspace_path": "C:/Users/private/production"}
    (suite_dir / "suite.json").write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(BenchmarkValidationError, match="absolute paths"):
        TaskQualityBenchmarkService(tmp_path / "suites", tmp_path / "state.json")
