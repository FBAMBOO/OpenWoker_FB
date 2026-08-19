import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { ApiRequest } from "./api";
import { TaskQualitySettings } from "./TaskQualitySettings";

describe("TaskQualitySettings", () => {
  it("runs only registered offline suites and displays release metrics against baseline", async () => {
    const request: ApiRequest = async <T,>(path: string) => {
      if (path.endsWith("/benchmarks/suites")) return [{
        id: "test12", name: "Test12 Fabric/dbt repository analysis", stack: "fabric-dbt", version: 1,
        snapshot_artifact_id: `sha256:${"a".repeat(64)}`, prompt_hash: `sha256:${"b".repeat(64)}`,
        candidate_ids: ["legacy", "v2"], baseline_candidate: "legacy", thresholds: { quality_score: 85 },
        content_hash: `sha256:${"c".repeat(64)}`, promoted_baseline: {},
      }] as T;
      if (path.endsWith("/benchmarks/runs")) return {
        id: "benchmark-1", suite_id: "test12", suite_version: 1, suite_hash: "sha256:suite",
        snapshot_artifact_id: "sha256:snapshot", prompt_hash: "sha256:prompt", candidate_id: "v2", status: "pass",
        metrics: { quality_score: 91, hard_gate_failures: [], required_area_coverage: 7, required_area_total: 7, citation_resolution_ratio: 1, artifact_read_coverage_ratio: 1, snapshot_correct: true, reported_tokens: 1800000, tool_calls: 88, elapsed_seconds: 720, duplicate_scan_ratio: 0.11, repair_attempts: 1, provider: "offline", model: "fixture-v2" },
        failures: [], created_at: "2026-08-19T00:00:00Z", completed_at: "2026-08-19T00:00:00Z", content_hash: "sha256:run",
      } as T;
      if (path.endsWith("/benchmarks/runs/benchmark-1/comparison")) return {
        run_id: "benchmark-1", suite_id: "test12", candidate_id: "v2", baseline: { kind: "fixture_candidate" },
        current_metrics: { quality_score: 91 }, baseline_metrics: { quality_score: 50 }, deltas: { quality_score: 41 }, quality_score_regression: false,
      } as T;
      throw new Error(`Unexpected request: ${path}`);
    };
    render(<TaskQualitySettings apiRequest={request} />);

    await screen.findByText(/Test12 Fabric\/dbt repository analysis/);
    fireEvent.click(screen.getByRole("button", { name: "Run benchmark" }));
    await screen.findByText("Benchmark pass");
    expect(screen.getByText("7/7")).toBeTruthy();
    expect(screen.getByText("50 -> 91 (+41)")).toBeTruthy();
    expect(screen.getByText(/Only registered, sanitized snapshot artifacts/)).toBeTruthy();
  });
});
