import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { OrchestrationApi } from "./api";
import { BudgetPanel } from "./BudgetPanel";
import { DeliverableViewer } from "./DeliverableViewer";
import { EvidenceExplorer } from "./EvidenceExplorer";
import { TaskQualityPanel } from "./TaskQualityPanel";
import type { OrchestrationTaskDetail } from "./types";

afterEach(cleanup);

const primary = {
  artifact_id: "artifact-v2", deliverable_id: "report", filename: "ARCHITECTURE.md",
  mime_type: "text/markdown", sha256: `sha256:${"a".repeat(64)}`, byte_size: 28,
  version: 2, status: "verified" as const,
};

const task: OrchestrationTaskDetail = {
  id: "task-quality", title: "Repository architecture", objective: "Analyze the repository",
  status: "completed", stage: "archive", updated_at: "2026-08-19T00:00:00Z",
  task_quality_v2: true, workflow_status: "completed", quality_status: "fail",
  artifact_status: "verified", budget_status: "warning", primary_deliverable: primary,
  effective_budget: {
    ledger_id: "ledger-1", mode: "hard", source: "quality-first",
    used: { reported_tokens: 800 }, reserved: { reported_tokens: 100 },
    remaining: { reported_tokens: 100 }, limit: { reported_tokens: 1000 },
  },
};

describe("Task Quality V2 panels", () => {
  it("reads immutable deliverables in bounded ranges and exposes download and diff", async () => {
    const api = {
      getTaskDeliverables: vi.fn().mockResolvedValue({ deliverables: { items: [
        { id: "artifact-v2", logical_deliverable_id: "report", filename: "ARCHITECTURE.md", version: 2, is_primary: 1 },
        { id: "artifact-v1", logical_deliverable_id: "report", filename: "ARCHITECTURE.md", version: 1, is_primary: 0 },
      ] } }),
      getArtifactMetadata: vi.fn().mockResolvedValue({ ...primary, id: "artifact-v2", preview_policy: { inline: true, executable: false }, max_read_coverage_ratio: 1 }),
      getArtifactContentRange: vi.fn().mockResolvedValue("# Architecture\n\nVerified."),
      getArtifactDiff: vi.fn().mockResolvedValue("@@ Summary @@\n-old\n+new"),
    } as unknown as OrchestrationApi;
    const download = vi.fn().mockResolvedValue(undefined);
    render(<DeliverableViewer api={api} apiDownload={download} taskId="task-quality" />);

    await screen.findByText("Architecture", { selector: "h1" });
    expect(api.getArtifactContentRange).toHaveBeenCalledWith("artifact-v2", 0, 27);
    expect(screen.getByText("100%")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Download exact artifact" }));
    expect(download).toHaveBeenCalledWith("/v1/orchestration/artifacts/artifact-v2/download", "ARCHITECTURE.md");
    fireEvent.change(screen.getByLabelText(/Compare with/), { target: { value: "artifact-v1" } });
    fireEvent.click(screen.getByRole("button", { name: "Load section diff" }));
    await screen.findByText(/@@ Summary @@/);
  });

  it("shows authoritative hard gates, typed findings, and repair controls", async () => {
    const api = {
      getTaskQuality: vi.fn().mockResolvedValue({
        task_id: task.id, quality_status: "fail", quality_reason_code: "BLOCKING_FINDING",
        quality_verdict: { evaluation_id: "eval-1", decision: "repair", total_score: 72, finding_ids: ["finding-1"], content_hash: "sha256:eval" },
        gates: { items: [{ id: "gate-1", gate_id: "QG004", status: "fail", message: "Citation unresolved", artifact_hash: primary.sha256 }], offset: 0, limit: 200, has_more: false },
        findings: { items: [{ id: "finding-1", severity: "high", category: "citation", message: "Resolve the cited file", blocking: true, repairable: true, status: "open", section_id: "lineage", suggested_fix: "Bind the snapshot path" }], offset: 0, limit: 200, has_more: false },
        evaluations: { items: [], offset: 0, limit: 200, has_more: false },
        waivers: { items: [{ id: "waiver-1", subject_type: "criterion", subject_id: "req-risk", reason: "Approved exact scope", signature_hash: "sha256:signed-waiver" }], offset: 0, limit: 200, has_more: false },
      }),
      getArtifactMetadata: vi.fn().mockResolvedValue({ max_read_coverage_ratio: 0.75 }),
      requestTaskRepair: vi.fn().mockResolvedValue({ id: "repair-1" }),
    } as unknown as OrchestrationApi;
    render(<TaskQualityPanel api={api} task={task} onTaskRefresh={vi.fn()} />);

    await screen.findByText("Score 72");
    expect(screen.getAllByText(/QG004/).length).toBeGreaterThan(0);
    expect(screen.getByText("Resolve the cited file")).toBeTruthy();
    expect(screen.getByText("75%")).toBeTruthy();
    expect(screen.getByText("Signed waivers")).toBeTruthy();
    expect(screen.getByText("sha256:signed-waiver")).toBeTruthy();
    const repairButton = screen.getByRole("button", { name: "Request repair" });
    expect((repairButton as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(repairButton);
    await waitFor(() => expect(api.requestTaskRepair).toHaveBeenCalledWith(task.id, expect.objectContaining({ source_artifact_id: "artifact-v2", finding_ids: ["finding-1"] })));
  });

  it("requires a complete audited limit revision before resuming exhaustion", async () => {
    const exhausted = {
      ...task,
      status: "paused" as const,
      workflow_status: "needs_attention" as const,
      budget_status: "exhausted" as const,
      effective_budget: {
        ...task.effective_budget!,
        limit: { model_calls: 10, tool_calls: 20, reported_tokens: 1000, active_seconds: 300, tool_payload_bytes: 4096 },
      },
    };
    const api = {
      getTaskQuality: vi.fn().mockResolvedValue({
        task_id: task.id, quality_status: "fail", quality_reason_code: "budget_exhausted",
        gates: { items: [], offset: 0, limit: 200, has_more: false },
        findings: { items: [], offset: 0, limit: 200, has_more: false },
        evaluations: { items: [], offset: 0, limit: 200, has_more: false },
        waivers: { items: [], offset: 0, limit: 200, has_more: false },
      }),
      getArtifactMetadata: vi.fn().mockResolvedValue({ max_read_coverage_ratio: 1 }),
      resumeTaskQuality: vi.fn().mockResolvedValue(exhausted),
    } as unknown as OrchestrationApi;
    render(<TaskQualityPanel api={api} task={exhausted} onTaskRefresh={vi.fn()} />);
    await screen.findByLabelText("Budget extension");
    fireEvent.change(screen.getByLabelText("Budget reported_tokens"), { target: { value: "2000" } });
    fireEvent.change(screen.getByLabelText("Budget extension reason"), { target: { value: "Approved additional review" } });
    fireEvent.click(screen.getByRole("button", { name: "Resume quality workflow" }));
    await waitFor(() => expect(api.resumeTaskQuality).toHaveBeenCalledWith(task.id, {
      effective_limits: { model_calls: 10, tool_calls: 20, reported_tokens: 2000, active_seconds: 300, tool_payload_bytes: 4096 },
      reason: "Approved additional review",
    }));
  });

  it("separates coverage, claims, and frozen file citations", async () => {
    const api = {
      getTaskQualityCoverage: vi.fn().mockResolvedValue({ coverage: { items: [{ id: "coverage-1", requirement_id: "req-1", area: "models", status: "covered", evidence_count: 1, claim_ids: ["claim-1"] }] } }),
      getTaskQualityClaims: vi.fn().mockResolvedValue({ claims: { items: [{ id: "claim-1", claim_type: "fact", status: "pass", confidence: 0.99, text: "Orders depend on staging orders", evidence_count: 1 }] } }),
      getTaskQualityEvidence: vi.fn().mockResolvedValue({ evidence: [{ id: "evidence-1", path: "models/orders.sql", start_line: 1, end_line: 7, claim_id: "claim-1", claim_text: "Orders depend on staging orders", blob_hash: `sha256:${"b".repeat(64)}` }] }),
    } as unknown as OrchestrationApi;
    render(<EvidenceExplorer api={api} taskId={task.id} />);
    await screen.findByText("models");
    fireEvent.click(screen.getByRole("tab", { name: "Claims" }));
    expect(screen.getByText("Orders depend on staging orders")).toBeTruthy();
    fireEvent.click(screen.getByRole("tab", { name: "Files & citations" }));
    expect(screen.getByText("models/orders.sql")).toBeTruthy();
    expect(screen.getByText("1-7")).toBeTruthy();
  });

  it("renders explicit unlimited mode and hard-budget utilization independently", () => {
    const { rerender } = render(<BudgetPanel task={task} />);
    expect(screen.getByText("90%")).toBeTruthy();
    rerender(<BudgetPanel task={{ ...task, budget_status: "unlimited", effective_budget: { mode: "unlimited", used: { reported_tokens: 100 }, reserved: {}, remaining: {}, limit: { reported_tokens: null } } }} />);
    expect(screen.getByText("Unlimited budget - no hard stop")).toBeTruthy();
    expect(screen.getByText(/Unlimited is explicit and auditable/)).toBeTruthy();
  });
});
