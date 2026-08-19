import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { OrchestrationApi } from "./api";
import { ContractPreviewStep } from "./ContractPreviewStep";
import { TaskQualityWizard } from "./TaskQualityWizard";
import type { ExecutionStrategyV2, RepositorySnapshotV2, TaskQualityContract } from "./types";

const contract: TaskQualityContract = {
  id: "contract-1", task_id: "task-quality-1", version: 1, status: "draft",
  title: "Architecture report", objective: "Analyze the repository", archetype: "repo_analysis",
  quality_profile_id: "quality-first", original_prompt_hash: "sha256:prompt",
  requirements: [{ id: "req-1", category: "architecture", text: "Cover the architecture", required: true, hard_gate: true, source: "explicit_prompt", confidence: 1, verification_method: "coverage" }],
  deliverables: [{ id: "report", kind: "report", filename: "ARCHITECTURE.md", mime_type: "text/markdown", required: true, primary: true, required_sections: ["Summary", "Limitations"], result_schema_id: "analysis_report_result_v2" }],
  constraints: [{ kind: "permission", source_workspace_write: false }], non_goals: [],
  content_hash: "sha256:contract", etag: "sha256:contract",
};

const snapshot: RepositorySnapshotV2 = {
  id: "snapshot-1", task_id: "task-quality-1", version: 1, status: "frozen",
  repo_root: "C:/repo", project_root: "C:/repo", snapshot_kind: "git_commit",
  selected_ref: "refs/heads/main", commit_oid: "a".repeat(40), dirty: false,
  manifest_hash: `sha256:${"b".repeat(64)}`, resolution_confidence: 0.98,
  resolution_reason: "Explicit current repository",
};

const strategy: ExecutionStrategyV2 = {
  id: "strategy-1", task_id: "task-quality-1", version: 1, archetype: "repo_analysis",
  template_id: "repo-analysis-v2", nodes: [{ key: "explore", title: "Explore", role: "explorer", result_schema_id: "evidence_bundle_v2" }],
  assessment: { cognitive_complexity: 82, operational_risk: 12, evidence_workload: 74, rationale: ["Large evidence surface"] },
  edges: [], effective_policy: { independent_review: { value: true, source: "quality_profile" } }, policy_provenance: { independent_review: "quality_profile" },
  budget_profile: { mode: "hard", limits: { reported_tokens: 3_000_000 } },
  max_repair_attempts: 2, content_hash: "sha256:strategy",
};

describe("TaskQualityWizard", () => {
  it("binds draft, contract, target and strategy before starting the same task identity", async () => {
    const api = {
      createTaskQualityDraft: vi.fn().mockResolvedValue({ task_id: "task-quality-1", id: "task-quality-1", workflow_status: "draft", prompt_hash: "sha256:prompt", created_at: "2026-08-19T00:00:00Z" }),
      analyzeTaskQualityDraft: vi.fn().mockResolvedValue({ id: "analysis-1", task_id: "task-quality-1", status: "resolved", contract, contract_etag: contract.content_hash, request_hash: "sha256:request", contract_conflicts: [], target_resolution: { status: "resolved", recommended_candidate_id: "candidate-1", resolution_confidence: 0.98, resolution_reason: "Explicit root", candidates: [{ id: "candidate-1", repo_root: "C:/repo", project_root: "C:/repo", vcs_type: "git", current_branch: "main", recommended_ref: "refs/heads/main", recommended_snapshot_kind: "git_commit", recommendation_reason: "Explicit current checkout", head_oid: "a".repeat(40), default_ref: "refs/remotes/origin/main", default_oid: "b".repeat(40), ahead: 1, behind: 2, dirty: false, worktree_count: 1, file_count: 120, total_bytes: 4096, score: 98 }] } }),
      updateTaskQualityContract: vi.fn(),
      publishTaskQualityContract: vi.fn().mockResolvedValue({ ...contract, status: "published" }),
      freezeTaskQualitySnapshot: vi.fn().mockResolvedValue(snapshot),
      generateTaskQualityStrategy: vi.fn().mockResolvedValue(strategy),
      startTaskQualityDraft: vi.fn().mockResolvedValue({ id: "task-quality-1" }),
    } as unknown as OrchestrationApi;
    const onStarted = vi.fn();
    render(<TaskQualityWizard api={api} initialWorkspace="C:/repo" onStarted={onStarted} onCancel={vi.fn()} />);

    fireEvent.change(screen.getByLabelText("Objective"), { target: { value: "Analyze the repository" } });
    fireEvent.click(screen.getByRole("button", { name: "Analyze goal" }));
    await screen.findByText("Contract complete");
    expect(api.createTaskQualityDraft).toHaveBeenCalledWith(expect.objectContaining({ read_only: true, source_workspace_write: false, task_artifact_write: true, network: false }), expect.any(String));

    fireEvent.click(screen.getByRole("button", { name: "Publish contract & continue" }));
    await screen.findByText("Recommended");
    expect(screen.getByText(/HEAD main @ aaaaaaaaaaaa/)).toBeTruthy();
    expect(screen.getByText(/Default refs\/remotes\/origin\/main @ bbbbbbbbbbbb/)).toBeTruthy();
    expect(screen.getByText(/Ahead 1 · behind 2/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Freeze target & continue" }));
    await screen.findByText("Adaptive DAG");
    expect(screen.getByText("Cognitive complexity")).toBeTruthy();
    expect(screen.getByText("Operational risk")).toBeTruthy();
    expect(screen.getByText("Evidence workload")).toBeTruthy();
    expect(screen.getByText("source: quality_profile")).toBeTruthy();
    expect(screen.getByText("Mode hard")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Review publication" }));
    await screen.findByText("Immutable execution envelope");
    fireEvent.click(screen.getByRole("button", { name: "Publish & Start" }));

    await waitFor(() => expect(onStarted).toHaveBeenCalledWith("task-quality-1"));
    expect(api.publishTaskQualityContract).toHaveBeenCalledWith("task-quality-1", "sha256:contract");
    expect(api.freezeTaskQualitySnapshot).toHaveBeenCalledWith("task-quality-1", { candidate_id: "candidate-1" });
    expect(api.generateTaskQualityStrategy).toHaveBeenCalledWith("task-quality-1");
    expect(api.startTaskQualityDraft).toHaveBeenCalledWith("task-quality-1");
  });

  it("blocks an incomplete contract and identifies the conflicting requirement", () => {
    render(
      <ContractPreviewStep
        contract={{ ...contract, requirements: [{ ...contract.requirements[0], text: "" }] }}
        conflicts={[{ code: "MISSING_COVERAGE", requirement_id: "req-1", message: "Coverage is incomplete" }]}
        busy={false}
        dirty={false}
        onRequirementChange={vi.fn()}
        onSave={vi.fn()}
        onContinue={vi.fn()}
      />,
    );

    expect(screen.getByText("Contract incomplete")).toBeTruthy();
    expect(screen.getByRole("alert").textContent).toContain("req-1");
    expect(screen.getByRole("alert").textContent).toContain("Coverage is incomplete");
    expect((screen.getByRole("button", { name: "Publish contract & continue" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
