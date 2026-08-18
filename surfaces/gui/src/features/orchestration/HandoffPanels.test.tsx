import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ApiRequest, OrchestrationApi } from "./api";
import { createBlankAgentProfile } from "./AgentProfilesSettings";
import { TaskHandoffPanel } from "./HandoffPanels";
import { OrchestrationSurface } from "./OrchestrationSurface";

afterEach(cleanup);

const brief = {
  id: "brief-1",
  task_id: "task-1",
  revision: 1,
  status: "published",
  title: "Bounded task",
  objective: "Demonstrate lazy handoff reads",
  background: "",
  scope: { included: ["src"] },
  instructions: ["Inspect metadata first"],
  constraints: [],
  non_goals: [],
  acceptance_criteria: [{ id: "criterion-1", text: "No eager body reads", required: true }],
  deliverables: [{ id: "deliverable-1", kind: "test_result", title: "Proof", required: true }],
  result_contract: { id: "test_result_v1" },
  content_hash: "sha256:brief",
  created_at: "2026-08-17T00:00:00Z",
  published_at: "2026-08-17T00:01:00Z",
};

describe("structured handoff GUI", () => {
  it("promotes the declared deliverable and starts a result-grounded follow-up", async () => {
    const askResultQuestion = vi.fn(async (_taskId: string, question: string) => ({
      id: "question-task-1",
      task_id: "question-task-1",
      source_task_id: "task-1",
      question,
      status: "queued",
      stage: "intake",
      progress: 0,
      answer: null,
      source_work_product_ids: ["wp-final"],
      created_at: "2026-08-18T00:03:00Z",
      updated_at: "2026-08-18T00:03:00Z",
    }));
    const api = {
      listTaskWorkProducts: vi.fn(async () => [
        {
          id: "wp-final", task_id: "task-1", run_id: "run-execute", kind: "artifact",
          title: "Architecture report", summary: "# Report\nEvidence: `src/system.py:42`",
          artifact_id: `sha256:${"a".repeat(64)}`, uri: `sha256:${"a".repeat(64)}`,
          metadata: { deliverable_id: "deliverable-1" }, verification_status: "verified",
          created_by: "worker", created_at: "2026-08-18T00:01:00Z",
        },
        {
          id: "wp-review", task_id: "task-1", run_id: "run-review", kind: "review_report",
          title: "Review notes", summary: "Supporting review detail", metadata: {},
          verification_status: "verified", created_by: "reviewer", created_at: "2026-08-18T00:02:00Z",
        },
      ]),
      listResultQuestions: vi.fn(async () => []),
      askResultQuestion,
      verifyWorkProduct: vi.fn(),
    } as unknown as OrchestrationApi;
    const task = {
      id: "task-1",
      status: "completed",
      brief: {
        ...brief,
        deliverables: [{ id: "deliverable-1", kind: "artifact", title: "Architecture report", required: true }],
      },
    } as never;

    render(<TaskHandoffPanel api={api} task={task} kind="products" onTaskRefresh={() => undefined} />);

    expect(await screen.findByText("Architecture report")).toBeTruthy();
    expect(screen.getByText("Final deliverable")).toBeTruthy();
    const supporting = screen.getByText(/Supporting work and audit products/).closest("details");
    expect(supporting?.open).toBe(false);

    fireEvent.change(screen.getByPlaceholderText(/Ask for an explanation/), {
      target: { value: "Which file proves this conclusion?" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Ask" }));
    await waitFor(() => expect(askResultQuestion).toHaveBeenCalledWith(
      "task-1",
      "Which file proves this conclusion?",
    ));
    expect(await screen.findByText("Which file proves this conclusion?")).toBeTruthy();
    expect(screen.getByText(/Answer in progress/)).toBeTruthy();
  });

  it("opens preserved work products while a verification decision is pending", async () => {
    const task = {
      task: {
        id: "task-conflict",
        title: "Preserved result",
        objective: "Show completed output before resolving verifier dissent.",
        status: "waiting_human",
        stage: "inter_step_evaluation",
        updated_at: "2026-08-18T00:02:00Z",
      },
      handoff_summary: {
        work_products: { count: 2 },
        context: { ref_count: 0 },
        relations: {},
        comments: { count: 0, latest_sequence: 0, content_included: false },
        wakes: { count: 0, pending: 0, failed: 0 },
      },
      nodes: [], edges: [], runs: [], evidence: [], activity: [],
      attention: [{
        id: "gate-conflict",
        kind: "reconciliation",
        status: "open",
        prompt: {
          title: "Verification needs reconciliation",
          description: "Completed work products are preserved while this is decided.",
          actions: [
            { id: "accept_current", label: "Accept current results", tone: "primary", requires_response: true },
            { id: "retry", label: "Re-run disputed checks", tone: "neutral" },
            { id: "request_changes", label: "Revise deliverable", tone: "neutral", requires_response: true },
          ],
        },
      }],
    };
    const apiRequest: ApiRequest = async <T,>(path: string) => {
      if (path.startsWith("/v1/orchestration/tasks?")) return { tasks: [task.task] } as T;
      if (path === "/v1/orchestration/health") return { ready: true, state: "ready", loop_alive: true } as T;
      if (path === "/v1/orchestration/tasks/task-conflict") return task as T;
      if (path === "/v1/orchestration/tasks/task-conflict/work-products") return [
        {
          id: "wp-result", task_id: "task-conflict", run_id: "run-execute", kind: "report",
          title: "Completed analysis", summary: "Line one\nLine two", metadata: {},
          verification_status: "verified", created_by: "researcher", created_at: "2026-08-18T00:01:00Z",
        },
        {
          id: "wp-review", task_id: "task-conflict", run_id: "run-review", kind: "review_report",
          title: "Review report", summary: "One non-blocking objection remains.", metadata: {},
          verification_status: "verified", created_by: "reviewer", created_at: "2026-08-18T00:01:30Z",
        },
      ] as T;
      if (path === "/v1/orchestration/tasks/task-conflict/result-questions") return [] as T;
      throw new Error(`Unexpected request: ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={async () => undefined} initialTaskId="task-conflict" />);

    expect(await screen.findByTestId("completed-results-banner")).toBeTruthy();
    expect(screen.getByText("Final result is ready")).toBeTruthy();
    expect(await screen.findByText("Completed analysis")).toBeTruthy();
    expect(screen.getByText(/Line one/).textContent).toContain("Line two");
    expect(screen.getByRole("button", { name: "Accept current results" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Re-run disputed checks" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Revise deliverable" })).toBeTruthy();
  });

  it("loads handoff metadata by tab and context content only after an explicit read", async () => {
    const calls: string[] = [];
    const task = {
      task: { id: "task-1", title: "Bounded task", objective: brief.objective, status: "running", stage: "execution_review_test", updated_at: "2026-08-17T00:02:00Z" },
      brief,
      handoff_summary: {
        context: { ref_count: 1, required_count: 1, estimated_tokens: 25 },
        relations: { blocks: 1 }, comments: { count: 1, latest_sequence: 1, content_included: false },
        work_products: { count: 1 }, wakes: { count: 1, pending: 1, failed: 0 },
      },
      nodes: [], edges: [], runs: [], evidence: [], activity: [],
    };
    const apiRequest: ApiRequest = async <T,>(path: string) => {
      calls.push(path);
      if (path.startsWith("/v1/orchestration/tasks?")) return { tasks: [task.task] } as T;
      if (path === "/v1/orchestration/health") return { ready: true, state: "ready", loop_alive: true } as T;
      if (path === "/v1/orchestration/tasks/task-1") return task as T;
      if (path === "/v1/orchestration/tasks/task-1/briefs") return [brief] as T;
      if (path === "/v1/orchestration/tasks/task-1/context-refs") return [{
        id: "ref-1", task_id: "task-1", brief_id: "brief-1", requirement: "required", ref_type: "file",
        display_name: "src/main.ts", selection_reason: "Implementation entrypoint", locator: { relative_path: "src/main.ts" },
        delivery_mode: "on_demand", summary: "Entrypoint metadata", token_estimate: 25, trust_level: "operator_provided", created_at: "2026-08-17T00:00:00Z",
      }] as T;
      if (path === "/v1/orchestration/context-refs/ref-1/content") return { id: "ref-1", content: "bounded body" } as T;
      if (path === "/v1/orchestration/tasks/task-1/relations") return [] as T;
      if (path === "/v1/orchestration/tasks/task-1/comments?after_sequence=0") return { task_id: "task-1", latest_sequence: 0, after_sequence: 0, new_count: 0, comments: [] } as T;
      if (path === "/v1/orchestration/tasks/task-1/work-products") return [] as T;
      if (path === "/v1/orchestration/tasks/task-1/result-questions") return [] as T;
      if (path === "/v1/orchestration/tasks/task-1/wakes") return [] as T;
      throw new Error(`Unexpected request: ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={async () => undefined} initialTaskId="task-1" />);
    expect(await screen.findByRole("heading", { name: "Bounded task" })).toBeTruthy();
    expect(calls.some((path) => path.includes("/context-refs/ref-1/content"))).toBe(false);
    expect(calls.some((path) => path.endsWith("/briefs"))).toBe(false);

    fireEvent.click(screen.getByRole("button", { name: /Brief/ }));
    expect(await screen.findByTestId("handoff-brief-panel")).toBeTruthy();
    await waitFor(() => expect(calls).toContain("/v1/orchestration/tasks/task-1/briefs"));

    fireEvent.click(screen.getByRole("button", { name: /Context/ }));
    expect(await screen.findByText("src/main.ts")).toBeTruthy();
    expect(calls).not.toContain("/v1/orchestration/context-refs/ref-1/content");
    fireEvent.click(screen.getByRole("button", { name: "Read content" }));
    expect((await screen.findByTestId("context-content-ref-1")).textContent).toContain("bounded body");
    expect(calls).toContain("/v1/orchestration/context-refs/ref-1/content");

    for (const label of [/Dependencies/, /Communication/, /Results/, /Wakes/]) {
      fireEvent.click(screen.getByRole("button", { name: label }));
    }
    await waitFor(() => {
      expect(calls).toContain("/v1/orchestration/tasks/task-1/relations");
      expect(calls).toContain("/v1/orchestration/tasks/task-1/comments?after_sequence=0");
      expect(calls).toContain("/v1/orchestration/tasks/task-1/work-products");
      expect(calls).toContain("/v1/orchestration/tasks/task-1/wakes");
    });
  });

  it("creates schema-v2 profiles with a safe communication policy", () => {
    const profile = createBlankAgentProfile();
    expect(profile.schema_version).toBe(2);
    expect(profile.communication_policy).toMatchObject({
      can_delegate: false,
      allowed_child_roles: [],
      allow_full_transcript_reference: false,
      result_contract_id: "implementation_result_v1",
    });
  });

  it("exposes a failed wake diagnostic and retries it explicitly", async () => {
    const retryWake = vi.fn(async () => ({ status: "pending" }));
    let loads = 0;
    const failedWake = {
      id: "wake-failed",
      target_task_id: "task-1",
      target_run_id: null,
      reason: "task_commented",
      source_task_id: "task-1",
      source_run_id: null,
      source_event_id: "event-1",
      payload: { comment_ids: ["comment-1"] },
      dedupe_key: "task-1:comment",
      status: "failed",
      coalesced_count: 0,
      attempts: 5,
      not_before: "2026-08-17T00:00:00Z",
      claimed_by: null,
      claimed_until: null,
      last_error: "delivery exhausted",
      created_at: "2026-08-17T00:00:00Z",
      updated_at: "2026-08-17T00:01:00Z",
      delivered_at: null,
      completed_at: null,
    };
    const api = {
      listTaskWakes: vi.fn(async () => {
        loads += 1;
        return [{ ...failedWake, status: loads === 1 ? "failed" : "pending" }];
      }),
      retryWake,
      cancelWake: vi.fn(),
    } as unknown as OrchestrationApi;
    const task = {
      id: "task-1",
      task: { id: "task-1" },
    } as never;

    render(
      <TaskHandoffPanel
        api={api}
        task={task}
        kind="wakes"
        onTaskRefresh={() => undefined}
      />,
    );
    expect(await screen.findByText("delivery exhausted")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() => expect(retryWake).toHaveBeenCalledWith("wake-failed"));
    await waitFor(() => expect(api.listTaskWakes).toHaveBeenCalledTimes(2));
    expect(screen.queryByRole("button", { name: "Retry" })).toBeNull();
  });
});
