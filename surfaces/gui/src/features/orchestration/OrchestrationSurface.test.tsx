import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { OrchestrationSurface } from "./OrchestrationSurface";
import type { ApiRequest } from "./api";

type RequestCall = { path: string; method: string; body?: unknown };
const noDownload = async () => undefined;

afterEach(cleanup);

describe("OrchestrationSurface", () => {
  it("renders the eight-stage task record and exposes gates, graph, runs, evidence, and activity", async () => {
    const calls: RequestCall[] = [];
    const detail = {
      task: {
        task_id: "task-1",
        title: "Ship the release",
        objective: "Release only after review and tests pass.",
        status: "waiting_human",
        current_stage: "inter_step_evaluation",
        updated_at: "2026-08-03T01:00:00Z",
      },
      stage_history: [
        { stage: "intake", disposition: "completed", sequence: 1 },
        { stage: "complexity_assessment", disposition: "completed", sequence: 2 },
        { stage: "clarification", disposition: "skipped", sequence: 3 },
        { stage: "planning", disposition: "completed", sequence: 4 },
        { stage: "execution_review_test", disposition: "completed", sequence: 5 },
        { stage: "inter_step_evaluation", disposition: "completed", attempt: 1, sequence: 6 },
        { stage: "inter_step_evaluation", disposition: "active", attempt: 2, sequence: 7 },
      ],
      plan: {
        nodes: [
          { node_id: "prepare", title: "Prepare assets", status: "completed", agent: "worker" },
          { node_id: "verify", title: "Verify release", status: "running", agent: "tester" },
        ],
        edges: [{ from_node_id: "prepare", to_node_id: "verify" }],
      },
      runs: [
        { run_id: "run-root", session_id: "session-root", node_id: "prepare", title: "Prepare run", status: "succeeded", model: "openai:gpt-high" },
        {
          run_id: "run-child",
          session_id: "session-child",
          node_id: "verify",
          parent_run_id: "run-root",
          title: "Test run",
          status: "running",
          model: "anthropic:balanced",
          budget: { model_calls: 2, tool_calls: 14, tokens: 2_000, wall_seconds: 900 },
        },
      ],
      gates: [{
        gate_id: "gate-1",
        kind: "final_acceptance",
        status: "open",
        version: 4,
        prompt: {
          question: "Approve production?",
          description: "The final smoke test passed.",
          criteria: {
            "Release artifacts are complete": "pass",
            "Operator sign-off is recorded": "unknown",
          },
          verification: [{
            node_id: "verify",
            node_key: "verify",
            run_id: "run-child",
            role: "tester",
            status: "pass",
            criteria: { "Release artifacts are complete": "pass" },
            summary: "All isolated checks passed.",
            findings: ["No blocking findings"],
            source: "run_output",
          }],
          policy_reasons: ["High-risk work requires explicit operator acceptance"],
          actions: ["approve", "request_changes", "cancel"],
        },
        actions: [
          { id: "approve", label: "Approve", tone: "primary", requires_response: false },
          { id: "request_changes", label: "Request Changes", tone: "neutral", requires_response: true },
          { id: "cancel", label: "Cancel", tone: "danger", requires_response: false },
        ],
      }],
      evidence: [{
        evidence_id: "evidence-1",
        kind: "test",
        run_id: "run-child",
        blob_uri: `sha256:${"a".repeat(64)}`,
        content_hash: "a".repeat(64),
        actor: "isolated-tester",
        subject: { candidate_hash: "candidate-123" },
        subject_matches: false,
        missing_criteria: ["Rollback succeeds"],
        payload: { title: "Smoke test", summary: "All checks passed", raw_verdict: "fail" },
      }],
      events: [{ id: "event-1", event_type: "stage_advanced", created_at: "2026-08-03T01:05:00Z", payload: { message: "Evaluation requested", actor: "orchestrator", stage: "inter_step_evaluation" } }],
      agent_profile_snapshot: { profile_id: "release-worker", display_name: "Release worker", version: 3 },
      routing_policy_snapshot: { policy_id: "quality-first", version: 2 },
      children_details: [{
        id: "task-child",
        title: "Delegated verification",
        status: "running",
        stage: "execution_review_test",
        updated_at: "2026-08-03T01:04:00Z",
        nodes: [],
        edges: [],
        runs: [{ run_id: "run-grandchild", parent_run_id: "run-child", title: "Delegated probe", status: "running" }],
        evidence: [],
        activity: [],
      }],
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      calls.push({ path, method, body: init?.body ? JSON.parse(String(init.body)) : undefined });
      if (path.startsWith("/v1/orchestration/tasks?") && method === "GET") {
        return { tasks: [{ ...detail.task, attention_count: 1 }] } as T;
      }
      if (path === "/v1/orchestration/tasks/task-1") return detail as T;
      if (path === "/v1/orchestration/tasks/task-1/runs/run-child/transcript") {
        return {
          task_id: "task-1", run_id: "run-child", session_id: "session-child", available: true,
          title: "Test run", messages: [{ role: "assistant", content: "All checks passed." }],
          message_count: 1, offset: 0, limit: 500, has_more: false,
        } as T;
      }
      if (path.startsWith("/v1/orchestration/tasks/task-1/runs/run-child/activity?")) {
        return {
          task_id: "task-1",
          run_id: "run-child",
          activity: [
            {
              sequence: 1, id: "activity-1", event_key: "reasoning-1", source_id: "reasoning-source",
              kind: "reasoning_summary", status: "running", title: "Reasoning summary",
              summary: "Inspecting ", detail: { provider_summary: true }, created_at: "2026-08-03T01:01:00Z",
            },
            {
              sequence: 2, id: "activity-2", event_key: "reasoning-2", source_id: "reasoning-source",
              kind: "reasoning_summary", status: "completed", title: "Reasoning summary",
              summary: "test results.", detail: { provider_summary: true }, created_at: "2026-08-03T01:01:01Z",
            },
            {
              sequence: 3, id: "activity-3", event_key: "tool-1-start", source_id: "tool-source",
              kind: "tool", status: "running", title: "Command", summary: "npm test",
              detail: { command: "npm test", cwd: "C:/workspace" }, created_at: "2026-08-03T01:01:02Z",
            },
            {
              sequence: 4, id: "activity-4", event_key: "tool-1-finish", source_id: "tool-source",
              kind: "tool", status: "completed", title: "Command", summary: "npm test",
              detail: { exit_code: 0, duration_ms: 450 }, created_at: "2026-08-03T01:01:03Z",
            },
            {
              sequence: 5, id: "activity-5", event_key: "usage", source_id: "usage-source",
              kind: "usage", status: "info", title: "Token usage updated", summary: "1,250 total tokens",
              detail: { total_tokens: 1250, input_tokens: 1000, cached_input_tokens: 800, output_tokens: 250 },
              created_at: "2026-08-03T01:01:04Z",
            },
          ],
          has_more: false,
          order: "oldest_to_newest",
          privacy: { reasoning: "provider_summary_only", tool_output: "metadata_only" },
        } as T;
      }
      if (path === "/v1/orchestration/tasks/task-1/pause" && method === "POST") return detail as T;
      if (path.endsWith("/resolve")) return { ok: true } as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };
    const onOpenProfile = vi.fn();
    const onOpenPolicy = vi.fn();
    const apiDownload = vi.fn(async () => undefined);

    render(
      <OrchestrationSurface
        apiRequest={apiRequest}
        apiDownload={apiDownload}
        initialTaskId="task-1"
        onOpenProfile={onOpenProfile}
        onOpenPolicy={onOpenPolicy}
      />,
    );

    expect(await screen.findByRole("heading", { name: "Ship the release" })).toBeTruthy();
    const timeline = screen.getByLabelText("Task stages");
    for (const label of ["Intake", "Complexity", "Clarification", "Planning", "Evaluation", "Final acceptance", "Finalize"]) {
      expect(within(timeline).getByText(label)).toBeTruthy();
    }
    expect(timeline.textContent).toContain("Execute");
    expect(timeline.textContent).toContain("review");
    expect(timeline.textContent).toContain("test");
    expect(timeline.textContent).toContain("Skipped");
    expect(timeline.textContent).toContain("×2");

    expect(screen.getByText("Approve production?")).toBeTruthy();
    expect(within(screen.getByLabelText("Acceptance criteria")).getByText("Release artifacts are complete")).toBeTruthy();
    expect(within(screen.getByLabelText("Independent verification")).getByText("All isolated checks passed.")).toBeTruthy();
    expect(within(screen.getByLabelText("Acceptance policy reasons")).getByText("High-risk work requires explicit operator acceptance")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    await waitFor(() => {
      const resolution = calls.find((call) => call.path.endsWith("/resolve"));
      expect(resolution).toMatchObject({
        path: "/v1/orchestration/tasks/task-1/attention/gate-1/resolve",
        method: "POST",
        body: {
          decision: "approve",
          expected_version: 4,
          idempotency_key: expect.stringMatching(/^gui:gate-gate-1:/),
        },
      });
    });
    expect((screen.getByRole("button", { name: "Request Changes" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByLabelText("Response for Approve production?")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Pause" }));
    await waitFor(() => {
      expect(calls).toContainEqual({
        path: "/v1/orchestration/tasks/task-1/pause",
        method: "POST",
        body: undefined,
      });
    });

    expect(screen.getByTestId("dag-view")).toBeTruthy();
    fireEvent.click(screen.getByRole("tab", { name: "List" }));
    expect(screen.queryByTestId("dag-view")).toBeNull();
    expect(screen.getByText("Depends on prepare")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /Agent runs/ }));
    expect(screen.getByText("Run tree")).toBeTruthy();
    expect(screen.getByText("Delegated probe")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Test run/ }));
    const runDialog = await screen.findByRole("dialog", { name: "Agent run details" });
    expect(within(runDialog).getByText("Test run")).toBeTruthy();
    expect(within(runDialog).getAllByText("run-child")).toHaveLength(2);
    expect(within(runDialog).getByText("session-child")).toBeTruthy();
    expect(within(runDialog).getByText("anthropic:balanced")).toBeTruthy();
    expect(within(runDialog).getByText("tester")).toBeTruthy();
    expect(within(runDialog).getByRole("region", { name: "Retained transcript" })).toBeTruthy();
    expect(within(runDialog).getByRole("region", { name: "Live Agent activity" })).toBeTruthy();
    expect((await within(runDialog).findAllByText("Inspecting test results.")).length).toBeGreaterThan(0);
    expect(within(runDialog).getByText("1,250 / 2,000")).toBeTruthy();
    fireEvent.click(within(runDialog).getByLabelText("Expand Command"));
    expect(within(runDialog).getByText("C:/workspace")).toBeTruthy();
    expect(await within(runDialog).findByText("All checks passed.")).toBeTruthy();
    fireEvent.click(within(runDialog).getByRole("button", { name: "Close" }));
    Reflect.set(detail.runs[1], "budget", null);
    const detailReadsBeforeUnlimitedRefresh = calls.filter(
      (call) => call.path === "/v1/orchestration/tasks/task-1" && call.method === "GET",
    ).length;
    fireEvent.click(screen.getByLabelText("Refresh tasks"));
    await waitFor(() => {
      expect(calls.filter(
        (call) => call.path === "/v1/orchestration/tasks/task-1" && call.method === "GET",
      ).length).toBeGreaterThan(detailReadsBeforeUnlimitedRefresh);
    });
    fireEvent.click(screen.getByRole("button", { name: /Test run/ }));
    const unlimitedRunDialog = await screen.findByRole("dialog", { name: "Agent run details" });
    expect(within(unlimitedRunDialog).getByText("Reported tokens · no run cap")).toBeTruthy();
    expect(within(unlimitedRunDialog).getByText("1,250")).toBeTruthy();
    expect(within(unlimitedRunDialog).queryByText("1,250 / 2,000")).toBeNull();
    fireEvent.click(within(unlimitedRunDialog).getByRole("button", { name: "Close" }));
    const delegatedRunButton = screen.getByRole("button", { name: /Delegated probe/ }) as HTMLButtonElement;
    expect(delegatedRunButton.disabled).toBe(false);
    fireEvent.click(screen.getAllByRole("button", { name: "View Agent progress" })[1]);
    expect(await screen.findByText("All checks passed.")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Close" }));

    fireEvent.click(screen.getByRole("button", { name: /Evidence/ }));
    expect(screen.getByText("Smoke test")).toBeTruthy();
    expect(screen.getByText("Recorded by isolated-tester")).toBeTruthy();
    expect(screen.getByText("Subject does not match accepted candidate")).toBeTruthy();
    expect(screen.getByText("Missing criteria: Rollback succeeds")).toBeTruthy();
    expect(screen.getByText("Evidence subject")).toBeTruthy();
    expect(screen.getByText("Audit payload")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Download Smoke test" }));
    await waitFor(() => expect(apiDownload).toHaveBeenCalledWith(
      `/v1/orchestration/blobs/${"a".repeat(64)}`,
      "Smoke test",
    ));
    fireEvent.click(screen.getByRole("button", { name: "View run" }));
    const evidenceRunDialog = await screen.findByRole("dialog", { name: "Agent run details" });
    expect(within(evidenceRunDialog).getAllByText("run-child")).toHaveLength(2);
    expect(await within(evidenceRunDialog).findByText("All checks passed.")).toBeTruthy();
    fireEvent.click(within(evidenceRunDialog).getByRole("button", { name: "Close" }));

    fireEvent.click(screen.getByRole("button", { name: /Activity/ }));
    expect(screen.getByText("Evaluation requested")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /Profile: Release worker v3/ }));
    expect(onOpenProfile).toHaveBeenCalledWith("release-worker");
    fireEvent.click(screen.getByRole("button", { name: /Routing: quality-first v2/ }));
    expect(onOpenPolicy).toHaveBeenCalledWith("quality-first");
  });

  it("shows safe, bounded failure details in reconciliation, run, and activity views", async () => {
    const diagnostic = `HTTP 400 invalid_json_schema <script>alert("unsafe")</script>\u0001 ${"x".repeat(2_000)}`;
    const detail = {
      id: "task-runtime-failure",
      title: "Runtime failure",
      objective: "Explain a failed execution.",
      status: "waiting_human",
      stage: "inter_step_evaluation",
      updated_at: "2026-08-04T01:00:00Z",
      stages: [],
      nodes: [{ id: "node-understand", key: "understand", title: "Understand", status: "failed", depends_on: [] }],
      edges: [],
      runs: [{
        id: "run-failed",
        node_id: "node-understand",
        title: "Understand",
        status: "failed",
        attempt: 2,
        error_kind: "codex_turn_failed",
        summary: diagnostic,
      }],
      attention: [{
        id: "gate-reconcile",
        kind: "reconciliation",
        status: "open",
        prompt: {
          title: "Execution needs reconciliation",
          description: "One or more required runs did not succeed.",
          failed_runs: [{
            id: "run-failed",
            node_key: "understand",
            status: "failed",
            attempt: 2,
            error_kind: "codex_turn_failed",
            summary: diagnostic,
          }],
          actions: ["retry", "cancel"],
        },
      }],
      evidence: [],
      activity: [{
        id: "event-failed",
        type: "run.failed",
        summary: "run failed",
        detail: JSON.stringify({ error_kind: "codex_turn_failed", error_message: diagnostic }),
        created_at: "2026-08-04T01:01:00Z",
      }],
    };
    const apiRequest: ApiRequest = async <T,>(path: string) => {
      if (path.startsWith("/v1/orchestration/tasks?")) return [detail] as T;
      if (path === "/v1/orchestration/tasks/task-runtime-failure") return detail as T;
      throw new Error(`Unexpected request: GET ${path}`);
    };

    const { container } = render(
      <OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} initialTaskId="task-runtime-failure" />,
    );

    expect(await screen.findByRole("heading", { name: "Runtime failure" })).toBeTruthy();
    const gateFailures = screen.getByLabelText("Failed runs");
    const gateDiagnostic = within(gateFailures).getByLabelText("Failure details for understand");
    expect(gateDiagnostic.textContent).toContain("codex_turn_failed");
    expect(gateDiagnostic.textContent).toContain("HTTP 400 invalid_json_schema");
    expect(gateDiagnostic.textContent).toContain("…");
    expect(gateDiagnostic.textContent?.length).toBeLessThan(1_900);
    expect(container.querySelector("script")).toBeNull();
    expect(container.textContent).not.toContain("\u0001");

    fireEvent.click(screen.getByRole("button", { name: /Agent runs/ }));
    const runSection = screen.getByText("Run tree").closest("section");
    expect(runSection).toBeTruthy();
    const runDiagnostic = within(runSection as HTMLElement).getByLabelText("Failure details for Understand");
    expect(runDiagnostic.textContent).toContain("codex_turn_failed");
    expect(runDiagnostic.textContent?.length).toBeLessThan(1_900);

    fireEvent.click(screen.getByRole("button", { name: /Activity/ }));
    expect(screen.getByText("run failed")).toBeTruthy();
    expect(screen.getAllByText(/codex_turn_failed/).length).toBeGreaterThan(0);
    expect(container.querySelector("script")).toBeNull();
  });

  it("creates and starts a task from the Tasks surface", async () => {
    const calls: RequestCall[] = [];
    const created = {
      id: "task-new",
      title: "Research the decision",
      objective: "Research the decision and cite the conclusion.",
      status: "queued",
      stage: "intake",
      updated_at: "2026-08-03T02:00:00Z",
      stages: [],
      attention: [],
      nodes: [],
      edges: [],
      runs: [],
      evidence: [],
      activity: [],
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = init?.body ? JSON.parse(String(init.body)) : undefined;
      calls.push({ path, method, body });
      if (path === "/v1/orchestration/tasks" && method === "POST") return created as T;
      if (path.startsWith("/v1/orchestration/tasks?") && method === "GET") return [] as T;
      if (path === "/v1/orchestration/agent-profiles") return [
        { id: "worker", name: "Worker", role: "worker", builtin: true, archived: false, current_version: 1, has_draft: false },
        { id: "researcher", name: "Researcher", role: "worker", description: "Implements evidence-backed research changes.", builtin: false, archived: false, current_version: 2, has_draft: false },
      ] as T;
      if (path === "/v1/orchestration/model-policies") return [
        { id: "quality-first", name: "Quality first", builtin: true, archived: false, current_version: 1, has_draft: false },
        { id: "budget-aware", name: "Budget aware", builtin: false, archived: false, current_version: 3, has_draft: false },
      ] as T;
      if (path === "/v1/orchestration/model-catalog") return [
        {
          id: "codex-subscription:gpt-5.6-sol@max",
          label: "Codex Subscription · GPT-5.6 Sol · Max",
          provider: "codex-subscription",
          source: "subscription-runtime",
          quality: 100,
          configured: true,
          availability: "configured",
          verified: true,
          capabilities: ["tools", "streaming"],
          context_window: 200000,
          latency_rank: 1000,
          runtime: {
            protocol: "codex-app-server-v2",
            model: "gpt-5.6-sol",
            reasoning_effort: "max",
            local_owner_only: true,
            interactive_only: false,
          },
        },
      ] as T;
      if (path === "/v1/orchestration/tasks/task-new") return created as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} currentWorkspace="C:/work/openworker" />);
    await screen.findByText("No active orchestration tasks.");
    fireEvent.click(screen.getByRole("button", { name: /New/ }));
    expect((screen.getByLabelText(/Workspace/) as HTMLInputElement).value).toBe("C:/work/openworker");
    fireEvent.change(screen.getByLabelText("Objective"), {
      target: { value: "Research the decision and cite the conclusion." },
    });
    fireEvent.change(screen.getByLabelText("Title (optional)"), {
      target: { value: "Research the decision" },
    });
    await waitFor(() => expect((screen.getByLabelText("Primary agent profile") as HTMLSelectElement).disabled).toBe(false));
    fireEvent.change(screen.getByLabelText("Primary agent profile"), { target: { value: "researcher" } });
    fireEvent.change(screen.getByLabelText("Model routing policy"), { target: { value: "budget-aware" } });
    fireEvent.change(screen.getByLabelText(/Requested model/), {
      target: { value: "codex-subscription:gpt-5.6-sol@max" },
    });
    fireEvent.change(screen.getByLabelText(/Acceptance criteria/), {
      target: { value: "Conclusion is cited\nSources are traceable" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create and start" }));

    await waitFor(() => {
      expect(calls.find((call) => call.path === "/v1/orchestration/tasks" && call.method === "POST")).toMatchObject({
        path: "/v1/orchestration/tasks",
        method: "POST",
        body: {
          idempotency_key: expect.stringMatching(/^gui:task-create:/),
          title: "Research the decision",
          objective: "Research the decision and cite the conclusion.",
          domain: "code",
          read_only: false,
          workspace: "C:/work/openworker",
          acceptance_criteria: ["Conclusion is cited", "Sources are traceable"],
          constraints: [],
          profile_id: "researcher",
          model_policy_id: "budget-aware",
          requested_model: "codex-subscription:gpt-5.6-sol@max",
          require_review: false,
          require_tests: false,
          auto_start: true,
          publish_brief: true,
          context_refs: [],
          brief: {
            title: "Research the decision",
            objective: "Research the decision and cite the conclusion.",
            acceptance_criteria: [
              { id: "criterion-1", text: "Conclusion is cited", required: true },
              { id: "criterion-2", text: "Sources are traceable", required: true },
            ],
            deliverables: [
              { id: "deliverable-1", kind: "implementation_patch", required: true },
            ],
            result_contract: { schema_id: "implementation_result_v1", schema_version: 1 },
          },
        },
      });
    });
    expect(await screen.findByRole("heading", { name: "Research the decision" })).toBeTruthy();
  });

  it("defaults code tasks to the backend Codex-led mixed preset and submits it exclusively", async () => {
    const calls: RequestCall[] = [];
    const presetId = "production-codex-led-mixed-v1";
    const codexId = "codex-subscription:gpt-5.6-sol@max";
    const claudeHighId = "claude-code-subscription:claude-opus-5@high";
    const claudeMaxId = "claude-code-subscription:claude-opus-5@max";
    const created = {
      id: "task-mixed",
      title: "Implement safely",
      objective: "Implement safely",
      status: "queued",
      stage: "intake",
      updated_at: "2026-08-03T02:00:00Z",
      stages: [], attention: [], nodes: [], edges: [], runs: [], evidence: [], activity: [],
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = init?.body ? JSON.parse(String(init.body)) : undefined;
      calls.push({ path, method, body });
      if (path.startsWith("/v1/orchestration/tasks?") && method === "GET") return [] as T;
      if (path === "/v1/orchestration/agent-profiles") return [
        { id: "worker", name: "Worker", role: "worker", builtin: true, archived: false, current_version: 1, has_draft: false },
      ] as T;
      if (path === "/v1/orchestration/model-policies") return [
        { id: "quality-first", name: "Quality first", builtin: true, archived: false, current_version: 1, has_draft: false },
      ] as T;
      if (path === "/v1/orchestration/model-catalog") return [
        { id: codexId, label: "Codex · GPT-5.6 Sol · Max", provider: "codex-subscription", quality: 100, configured: true, availability: "configured", verified: true, capabilities: [], context_window: 200000, latency_rank: 1 },
        { id: claudeHighId, label: "Claude Code · Opus 5 · High", provider: "claude-code-subscription", quality: 99, configured: true, availability: "configured", verified: true, capabilities: [], context_window: 200000, latency_rank: 2 },
        { id: claudeMaxId, label: "Claude Code · Opus 5 · Max", provider: "claude-code-subscription", quality: 100, configured: true, availability: "configured", verified: true, capabilities: [], context_window: 200000, latency_rank: 3 },
      ] as T;
      if (path === "/v1/orchestration/runtime-presets") return {
        default_preset_id: presetId,
        items: [{
          id: presetId,
          display_name: "Production · Codex-led mixed",
          description: "Codex performs semantic understanding, repository exploration, planning, implementation and integration; Claude independently verifies.",
          roles: {
            planner: codexId,
            worker: codexId,
            reviewer: claudeHighId,
            tester: claudeMaxId,
            evaluator: claudeMaxId,
          },
          required_runtime_ids: [codexId, claudeHighId, claudeMaxId],
          available: true,
        }],
      } as T;
      if (path === "/v1/orchestration/tasks" && method === "POST") return created as T;
      if (path === "/v1/orchestration/tasks/task-mixed") return created as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} currentWorkspace="C:/work/mixed" />);
    await screen.findByText("No active orchestration tasks.");
    fireEvent.click(screen.getByRole("button", { name: /New/ }));

    await waitFor(() => expect((screen.getByLabelText(/Runtime orchestration preset/) as HTMLSelectElement).value).toBe(presetId));
    expect(screen.getByLabelText("Task root profile")).toBeTruthy();
    expect(screen.getByLabelText("Mixed runtime role mapping")).toBeTruthy();
    expect(screen.getByText("Semantic understanding, repository exploration, planning, implementation & integration")).toBeTruthy();
    expect(screen.getByText("Independent reviewer")).toBeTruthy();
    expect(screen.getByText("Isolated tester & evaluator")).toBeTruthy();
    expect(screen.getByText(/labels the task runtime-tree root and task summary/)).toBeTruthy();
    expect((screen.getByLabelText(/Requested model/) as HTMLSelectElement).disabled).toBe(true);
    expect((screen.getByLabelText("Require reviewer") as HTMLInputElement).checked).toBe(true);
    expect((screen.getByLabelText("Require tester") as HTMLInputElement).checked).toBe(true);
    const readOnlyToggle = screen.getByLabelText("Read-only task") as HTMLInputElement;
    expect(readOnlyToggle.checked).toBe(false);
    fireEvent.click(readOnlyToggle);
    expect(screen.getByText(/Read-only remains a global hard boundary/)).toBeTruthy();
    fireEvent.click(readOnlyToggle);

    fireEvent.change(screen.getByLabelText("Objective"), { target: { value: "Implement safely" } });
    fireEvent.click(screen.getByRole("button", { name: "Create and start" }));

    await waitFor(() => {
      const submitted = calls.find((call) => call.path === "/v1/orchestration/tasks" && call.method === "POST");
      expect(submitted?.body).toMatchObject({
        objective: "Implement safely",
        domain: "code",
        read_only: false,
        workspace: "C:/work/mixed",
        runtime_preset_id: presetId,
        require_review: true,
        require_tests: true,
      });
      expect(submitted?.body).not.toHaveProperty("requested_model");
    });
  });

  it("blocks a non-Worker writable code primary and allows it only behind explicit read-only", async () => {
    const calls: RequestCall[] = [];
    const created = {
      id: "task-read-only-plan",
      title: "Inspect architecture",
      objective: "Inspect architecture without changing files.",
      status: "queued",
      stage: "intake",
      updated_at: "2026-08-04T02:00:00Z",
      stages: [], attention: [], nodes: [], edges: [], runs: [], evidence: [], activity: [],
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = init?.body ? JSON.parse(String(init.body)) : undefined;
      calls.push({ path, method, body });
      if (path.startsWith("/v1/orchestration/tasks?") && method === "GET") return [] as T;
      if (path === "/v1/orchestration/agent-profiles") return [
        { id: "worker", name: "Worker", role: "worker", builtin: true, archived: false, current_version: 1, has_draft: false },
        { id: "architecture-planner", name: "Architecture planner", role: "planner", description: "Produces implementation-ready architecture plans.", builtin: false, archived: false, current_version: 4, has_draft: false },
      ] as T;
      if (path === "/v1/orchestration/model-policies") return [
        { id: "quality-first", name: "Quality first", builtin: true, archived: false, current_version: 1, has_draft: false },
      ] as T;
      if (path === "/v1/orchestration/model-catalog" || path === "/v1/orchestration/runtime-presets") return [] as T;
      if (path === "/v1/orchestration/tasks" && method === "POST") return created as T;
      if (path === "/v1/orchestration/tasks/task-read-only-plan") return created as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} currentWorkspace="C:/work/read-only" />);
    await screen.findByText("No active orchestration tasks.");
    fireEvent.click(screen.getByRole("button", { name: /New/ }));
    await waitFor(() => expect((screen.getByLabelText("Primary agent profile") as HTMLSelectElement).disabled).toBe(false));
    fireEvent.change(screen.getByLabelText("Primary agent profile"), { target: { value: "architecture-planner" } });
    fireEvent.change(screen.getByLabelText("Objective"), { target: { value: "Inspect architecture without changing files." } });

    const guidance = screen.getByLabelText("Selected agent profile guidance");
    expect(within(guidance).getByText("Primary role · Planner")).toBeTruthy();
    expect(guidance.textContent).toContain("Produces implementation-ready architecture plans.");
    expect(guidance.textContent).toContain("under Automatic routing, select Worker for a writable code task");
    const roleConflict = screen.getByText(/Primary profile cannot start writable code work/).closest("[role=alert]");
    expect(roleConflict?.textContent).toContain("Turn on Read-only task or select a Worker profile");
    expect((screen.getByRole("button", { name: "Create and start" }) as HTMLButtonElement).disabled).toBe(true);

    fireEvent.submit(screen.getByTestId("create-orchestration-task"));
    expect(calls.some((call) => call.path === "/v1/orchestration/tasks" && call.method === "POST")).toBe(false);

    fireEvent.click(screen.getByLabelText("Read-only task"));
    expect(screen.queryByText(/Primary profile cannot start writable code work/)).toBeNull();
    expect((screen.getByRole("button", { name: "Create and start" }) as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Create and start" }));

    await waitFor(() => {
      const submitted = calls.find((call) => call.path === "/v1/orchestration/tasks" && call.method === "POST");
      expect(submitted?.body).toMatchObject({
        domain: "code",
        read_only: true,
        profile_id: "architecture-planner",
        workspace: "C:/work/read-only",
        brief: {
          deliverables: [
            {
              id: "deliverable-1",
              kind: "artifact",
              title: "Read-only analysis report",
              required: true,
            },
          ],
          result_contract: {
            schema_id: "analysis_result_v1",
          },
        },
      });
    });
  });

  it("allows an Orchestrator task root for writable code when a preset assigns the DAG roles", async () => {
    const calls: RequestCall[] = [];
    const presetId = "production-codex-led-mixed-v1";
    const created = {
      id: "task-preset-root",
      title: "Coordinate implementation",
      objective: "Coordinate a writable implementation.",
      status: "queued",
      stage: "intake",
      updated_at: "2026-08-04T02:10:00Z",
      stages: [], attention: [], nodes: [], edges: [], runs: [], evidence: [], activity: [],
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = init?.body ? JSON.parse(String(init.body)) : undefined;
      calls.push({ path, method, body });
      if (path.startsWith("/v1/orchestration/tasks?") && method === "GET") return [] as T;
      if (path === "/v1/orchestration/agent-profiles") return [
        { id: "orchestrator", name: "Orchestrator", role: "orchestrator", builtin: true, archived: false, current_version: 1, has_draft: false },
      ] as T;
      if (path === "/v1/orchestration/model-policies") return [
        { id: "quality-first", name: "Quality first", builtin: true, archived: false, current_version: 1, has_draft: false },
      ] as T;
      if (path === "/v1/orchestration/model-catalog") return [] as T;
      if (path === "/v1/orchestration/runtime-presets") return {
        default_preset_id: presetId,
        items: [{
          id: presetId,
          display_name: "Production role-aware preset",
          roles: { orchestrator: "codex-runtime", worker: "codex-runtime", reviewer: "claude-runtime", tester: "claude-runtime" },
          available: true,
        }],
      } as T;
      if (path === "/v1/orchestration/tasks" && method === "POST") return created as T;
      if (path === "/v1/orchestration/tasks/task-preset-root") return created as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} currentWorkspace="C:/work/preset-root" />);
    await screen.findByText("No active orchestration tasks.");
    fireEvent.click(screen.getByRole("button", { name: /New/ }));
    await waitFor(() => expect((screen.getByLabelText("Task root profile") as HTMLSelectElement).value).toBe("orchestrator"));
    fireEvent.change(screen.getByLabelText("Objective"), { target: { value: "Coordinate a writable implementation." } });

    expect((screen.getByLabelText("Read-only task") as HTMLInputElement).checked).toBe(false);
    expect(screen.queryByText(/Primary profile cannot start writable code work/)).toBeNull();
    expect(screen.getByText(/not assigned to every DAG node/)).toBeTruthy();
    expect((screen.getByRole("button", { name: "Create and start" }) as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Create and start" }));

    await waitFor(() => {
      const submitted = calls.find((call) => call.path === "/v1/orchestration/tasks" && call.method === "POST");
      expect(submitted?.body).toMatchObject({
        domain: "code",
        read_only: false,
        profile_id: "orchestrator",
        runtime_preset_id: presetId,
      });
    });
  });

  it("blocks a preset with an unavailable required runtime and explains why", async () => {
    const presetId = "production-codex-led-mixed-v1";
    const codexId = "codex-subscription:gpt-5.6-sol@max";
    const claudeHighId = "claude-code-subscription:claude-opus-5@high";
    const claudeMaxId = "claude-code-subscription:claude-opus-5@max";
    const apiRequest: ApiRequest = async <T,>(path: string) => {
      if (path.startsWith("/v1/orchestration/tasks?")) return [] as T;
      if (path === "/v1/orchestration/agent-profiles" || path === "/v1/orchestration/model-policies") return [] as T;
      if (path === "/v1/orchestration/model-catalog") return [
        { id: codexId, label: "Codex Max", provider: "codex-subscription", availability: "configured", configured: true, verified: true },
        { id: claudeHighId, label: "Claude High", provider: "claude-code-subscription", availability: "configured", configured: true, verified: true },
        { id: claudeMaxId, label: "Claude Max", provider: "claude-code-subscription", availability: "unavailable", availability_reason: "Claude CLI is not authenticated.", configured: false, verified: false },
      ] as T;
      if (path === "/v1/orchestration/runtime-presets") return {
        default_preset_id: presetId,
        items: [{
          id: presetId,
          display_name: "Production · Codex-led mixed",
          roles: { worker: codexId, reviewer: claudeHighId, tester: claudeMaxId },
          required_runtime_ids: [codexId, claudeHighId, claudeMaxId],
        }],
      } as T;
      throw new Error(`Unexpected request: ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} />);
    await screen.findByText("No active orchestration tasks.");
    fireEvent.click(screen.getByRole("button", { name: /New/ }));
    fireEvent.change(screen.getByLabelText("Objective"), { target: { value: "Implement safely" } });

    const runtimeReason = await screen.findByText(/Claude CLI is not authenticated\./);
    expect(runtimeReason.closest("[role=alert]")?.textContent).toContain("This preset cannot start");
    expect((screen.getByRole("button", { name: "Create and start" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("discards stale transcript responses after a newer request or modal close", async () => {
    const detail = {
      id: "task-transcripts",
      title: "Inspect transcripts",
      objective: "Keep each transcript bound to its run.",
      status: "running",
      stage: "execution_review_test",
      updated_at: "2026-08-03T02:00:00Z",
      stages: [], attention: [], nodes: [], edges: [], evidence: [], activity: [],
      runs: [
        { id: "run-a", session_id: "session-a", title: "Run A", status: "completed" },
        { id: "run-b", session_id: "session-b", title: "Run B", status: "completed" },
      ],
    };
    const resolvers = new Map<string, (value: unknown) => void>();
    const apiRequest: ApiRequest = async <T,>(path: string) => {
      if (path.startsWith("/v1/orchestration/tasks?")) return [detail] as T;
      if (path === "/v1/orchestration/tasks/task-transcripts") return detail as T;
      const match = /\/runs\/(run-[ab])\/transcript$/.exec(path);
      if (match) {
        return await new Promise<T>((resolve) => {
          resolvers.set(match[1], resolve as (value: unknown) => void);
        });
      }
      if (/\/runs\/(run-[ab])\/activity\?/.test(path)) {
        const runId = /\/runs\/(run-[ab])\//.exec(path)?.[1] || "";
        return {
          task_id: "task-transcripts", run_id: runId, activity: [], has_more: false,
          order: "oldest_to_newest",
          privacy: { reasoning: "provider_summary_only", tool_output: "metadata_only" },
        } as T;
      }
      throw new Error(`Unexpected request: GET ${path}`);
    };
    const transcript = (runId: string, content: string) => ({
      task_id: "task-transcripts",
      run_id: runId,
      session_id: `session-${runId.slice(-1)}`,
      available: true,
      title: runId,
      messages: [{ role: "assistant", content }],
      message_count: 1,
      offset: 0,
      limit: 500,
      has_more: false,
    });

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} />);
    await screen.findByRole("heading", { name: "Inspect transcripts" });
    fireEvent.click(screen.getByRole("button", { name: /Agent runs/ }));
    const transcriptButtons = screen.getAllByRole("button", { name: "View Agent progress" });
    fireEvent.click(transcriptButtons[0]);
    fireEvent.click(transcriptButtons[1]);

    await act(async () => {
      resolvers.get("run-b")?.(transcript("run-b", "newer transcript B"));
    });
    expect(await screen.findByText("newer transcript B")).toBeTruthy();
    await act(async () => {
      resolvers.get("run-a")?.(transcript("run-a", "stale transcript A"));
    });
    expect(screen.queryByText("stale transcript A")).toBeNull();
    expect(screen.getByText("newer transcript B")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    fireEvent.click(transcriptButtons[0]);
    expect(screen.getByRole("dialog", { name: "Agent run details" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    await act(async () => {
      resolvers.get("run-a")?.(transcript("run-a", "closed transcript A"));
    });
    expect(screen.queryByRole("dialog", { name: "Agent run details" })).toBeNull();
    expect(screen.queryByText("closed transcript A")).toBeNull();
  });

  it("derives open run details from refreshed task state and refreshes its transcript", async () => {
    const task = (status: "running" | "completed", model: string, summary: string) => ({
      id: "task-live-run",
      title: "Observe live run",
      objective: "Keep the inspector current.",
      status: status === "completed" ? "completed" : "running",
      stage: "execution_review_test",
      updated_at: status === "completed" ? "2026-08-04T03:02:00Z" : "2026-08-04T03:00:00Z",
      stages: [], attention: [], nodes: [], edges: [], evidence: [], activity: [],
      runs: [{
        id: "run-live",
        session_id: "session-live",
        node_id: "execute",
        title: "Live worker",
        agent_name: "worker",
        status,
        model_id: model,
        summary,
      }],
    });
    let current = task("running", "runtime-old", "Still working");
    let transcriptCalls = 0;
    let emit: ((event: { type: string; data?: Record<string, unknown> }) => void) | undefined;
    const apiRequest: ApiRequest = async <T,>(path: string) => {
      if (path.startsWith("/v1/orchestration/tasks?")) return [current] as T;
      if (path === "/v1/orchestration/tasks/task-live-run") return current as T;
      if (path === "/v1/orchestration/tasks/task-live-run/runs/run-live/transcript") {
        transcriptCalls += 1;
        const finished = current.runs[0].status === "completed";
        return {
          task_id: current.id,
          run_id: "run-live",
          session_id: "session-live",
          available: true,
          title: "Live worker",
          messages: [{ role: "assistant", content: finished ? "final retained transcript" : "working transcript" }],
          message_count: 1,
          offset: 0,
          limit: 500,
          has_more: false,
        } as T;
      }
      if (path.startsWith("/v1/orchestration/tasks/task-live-run/runs/run-live/activity?")) {
        return {
          task_id: current.id, run_id: "run-live", activity: [], has_more: false,
          order: "oldest_to_newest",
          privacy: { reasoning: "provider_summary_only", tool_output: "metadata_only" },
        } as T;
      }
      throw new Error(`Unexpected request: GET ${path}`);
    };

    render(
      <OrchestrationSurface
        apiRequest={apiRequest}
        apiDownload={noDownload}
        initialTaskId="task-live-run"
        subscribeEvents={(listener) => {
          emit = listener;
          return () => undefined;
        }}
      />,
    );
    await screen.findByRole("heading", { name: "Observe live run" });
    fireEvent.click(screen.getByRole("button", { name: /Agent runs/ }));
    fireEvent.click(screen.getByRole("button", { name: /Live worker/ }));
    const dialog = await screen.findByRole("dialog", { name: "Agent run details" });
    expect(await within(dialog).findByText("working transcript")).toBeTruthy();
    expect(within(dialog).getByText("runtime-old")).toBeTruthy();
    expect(within(dialog).getByText("Running")).toBeTruthy();

    current = task("completed", "runtime-final", "Finished cleanly");
    act(() => emit?.({ type: "orchestration_event", data: { task_id: current.id } }));

    await waitFor(() => {
      expect(within(dialog).getByText("Completed")).toBeTruthy();
      expect(within(dialog).getByText("runtime-final")).toBeTruthy();
      expect(within(dialog).getByText("Finished cleanly")).toBeTruthy();
      expect(within(dialog).getByText("final retained transcript")).toBeTruthy();
      expect(transcriptCalls).toBeGreaterThanOrEqual(2);
    });
  });

  it("keeps the current task list mounted during event-driven refreshes", async () => {
    const current = {
      id: "task-stable-list",
      title: "Stable task",
      objective: "Refresh without flashing.",
      status: "running",
      stage: "execution_review_test",
      updated_at: "2026-08-17T08:31:22Z",
      stages: [], attention: [], nodes: [], edges: [], runs: [], evidence: [], activity: [],
    };
    let listCalls = 0;
    let finishRefresh: ((value: typeof current[]) => void) | undefined;
    let emit: ((event: { type: string; data?: Record<string, unknown> }) => void) | undefined;
    const apiRequest: ApiRequest = async <T,>(path: string) => {
      if (path.startsWith("/v1/orchestration/tasks?")) {
        listCalls += 1;
        if (listCalls === 1) return [current] as T;
        return await new Promise<T>((resolve) => {
          finishRefresh = resolve as (value: typeof current[]) => void;
        });
      }
      if (path === "/v1/orchestration/tasks/task-stable-list") return current as T;
      throw new Error(`Unexpected request: GET ${path}`);
    };

    render(
      <OrchestrationSurface
        apiRequest={apiRequest}
        apiDownload={noDownload}
        subscribeEvents={(listener) => {
          emit = listener;
          return () => undefined;
        }}
      />,
    );
    await screen.findByRole("heading", { name: "Stable task" });
    expect(screen.getAllByText("Stable task").length).toBeGreaterThanOrEqual(2);

    act(() => emit?.({ type: "orchestration_event", data: { task_id: current.id } }));
    await waitFor(() => expect(listCalls).toBe(2));

    expect(screen.queryByText("Loading tasks…")).toBeNull();
    expect(screen.getAllByText("Stable task").length).toBeGreaterThanOrEqual(2);

    await act(async () => finishRefresh?.([current]));
  });

  it("does not retain the code workspace when creation switches to knowledge", async () => {
    const calls: RequestCall[] = [];
    const created = {
      id: "task-knowledge",
      title: "Answer independently",
      objective: "Answer independently",
      status: "queued",
      stage: "intake",
      updated_at: "2026-08-03T02:00:00Z",
      stages: [], attention: [], nodes: [], edges: [], runs: [], evidence: [], activity: [],
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = init?.body ? JSON.parse(String(init.body)) : undefined;
      calls.push({ path, method, body });
      if (path.startsWith("/v1/orchestration/tasks?") && method === "GET") return [] as T;
      if (path === "/v1/orchestration/agent-profiles" || path === "/v1/orchestration/model-policies") return [] as T;
      if (path === "/v1/orchestration/tasks" && method === "POST") return created as T;
      if (path === "/v1/orchestration/tasks/task-knowledge") return created as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} currentWorkspace="C:/sensitive/project" />);
    await screen.findByText("No active orchestration tasks.");
    fireEvent.click(screen.getByRole("button", { name: /New/ }));
    expect((screen.getByLabelText(/Workspace/) as HTMLInputElement).value).toBe("C:/sensitive/project");
    fireEvent.change(screen.getByLabelText("Domain"), { target: { value: "knowledge" } });
    expect(screen.queryByLabelText(/Workspace/)).toBeNull();
    fireEvent.change(screen.getByLabelText("Objective"), { target: { value: "Answer independently" } });
    fireEvent.click(screen.getByRole("button", { name: "Create and start" }));

    await waitFor(() => {
      const submitted = calls.find((call) => call.path === "/v1/orchestration/tasks" && call.method === "POST");
      expect(submitted?.body).toMatchObject({ domain: "knowledge", objective: "Answer independently", read_only: false });
      expect(submitted?.body).not.toHaveProperty("workspace");
    });
  });

  it("keeps one create idempotency key across a failed network retry", async () => {
    const calls: RequestCall[] = [];
    let createAttempts = 0;
    const created = {
      id: "task-retry",
      title: "Retry safely",
      objective: "Retry safely",
      status: "queued",
      stage: "intake",
      updated_at: "2026-08-03T02:00:00Z",
      stages: [], attention: [], nodes: [], edges: [], runs: [], evidence: [], activity: [],
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = init?.body ? JSON.parse(String(init.body)) : undefined;
      calls.push({ path, method, body });
      if (path.startsWith("/v1/orchestration/tasks?") && method === "GET") return [] as T;
      if (path === "/v1/orchestration/agent-profiles" || path === "/v1/orchestration/model-policies") return [] as T;
      if (path === "/v1/orchestration/tasks" && method === "POST") {
        createAttempts += 1;
        if (createAttempts === 1) throw new Error("Connection interrupted after send");
        return created as T;
      }
      if (path === "/v1/orchestration/tasks/task-retry") return created as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} />);
    await screen.findByText("No active orchestration tasks.");
    fireEvent.click(screen.getByRole("button", { name: /New/ }));
    fireEvent.change(screen.getByLabelText("Objective"), { target: { value: "Retry safely" } });
    fireEvent.click(screen.getByRole("button", { name: "Create and start" }));
    expect(await screen.findByText("Connection interrupted after send")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Create and start" }));
    expect(await screen.findByRole("heading", { name: "Retry safely" })).toBeTruthy();

    const attempts = calls.filter((call) => call.path === "/v1/orchestration/tasks" && call.method === "POST");
    expect(attempts).toHaveLength(2);
    const firstKey = (attempts[0].body as Record<string, unknown>).idempotency_key;
    expect(firstKey).toEqual(expect.stringMatching(/^gui:task-create:/));
    expect((attempts[1].body as Record<string, unknown>).idempotency_key).toBe(firstKey);
  });

  it("defaults to active tasks and pages each explicit status filter", async () => {
    const listPaths: string[] = [];
    const task = (index: number, status = "running") => ({
      id: `task-${index}`,
      title: `Task ${index}`,
      status,
      stage: "execution_review_test",
      updated_at: `2026-08-03T02:${String(index).padStart(2, "0")}:00Z`,
    });
    const apiRequest: ApiRequest = async <T,>(path: string) => {
      if (path.startsWith("/v1/orchestration/tasks?")) {
        listPaths.push(path);
        const query = new URL(`http://local${path}`).searchParams;
        if (query.getAll("status").includes("archived")) return [task(99, "archived")] as T;
        if (query.get("offset") === "20") return [task(20)] as T;
        return Array.from({ length: 21 }, (_, index) => task(index)) as T;
      }
      if (/\/v1\/orchestration\/tasks\/task-\d+$/.test(path)) {
        const parts = path.split("-");
        const summary = task(Number(parts[parts.length - 1]));
        return { ...summary, stages: [], attention: [], nodes: [], edges: [], runs: [], evidence: [], activity: [] } as T;
      }
      throw new Error(`Unexpected request: GET ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} />);
    await screen.findByText("Task 0");
    expect(listPaths[0]).toContain("status=draft");
    expect(listPaths[0]).not.toContain("status=archived");
    expect(listPaths[0]).toContain("limit=21&offset=0");

    await waitFor(() => expect((screen.getByRole("button", { name: "Next" }) as HTMLButtonElement).disabled).toBe(false));
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    await waitFor(() => expect(listPaths.some((path) => path.includes("offset=20"))).toBe(true));
    expect(screen.getByLabelText("Task page").textContent).toContain("2");

    fireEvent.change(screen.getByLabelText("Task filter"), { target: { value: "archived" } });
    await screen.findByText("Task 99");
    expect(listPaths[listPaths.length - 1]).toContain("status=archived&limit=21&offset=0");
  });

  it("marks bounded ledgers, loads older audit pages, and exposes dead-letter recovery", async () => {
    const calls: RequestCall[] = [];
    let recovered = false;
    const detail = {
      id: "task-bounded",
      title: "Bounded audit task",
      status: "running",
      stage: "execution_review_test",
      updated_at: "2026-08-03T03:00:00Z",
      stages: [], attention: [], attention_page: { has_more: true, page_size: 1 }, nodes: [], edges: [], activity: [],
      runs: [{ id: "run-new", title: "Newest run", status: "running" }],
      runs_page: { has_more: true, page_size: 1 },
      evidence: [{ id: "evidence-new", title: "Newest evidence", kind: "test" }],
      evidence_page: { has_more: true, page_size: 1 },
      detail_limits: { child_depth: 3, runs: 1, evidence: 1 },
      children: [],
      children_page: { truncated: true, tree_truncated: true, depth_limit_reached: false, returned: 0, total: 0, tree_row_limit: 256 },
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      calls.push({ path, method });
      if (path === "/v1/orchestration/health") return (recovered ? {
        ready: true, state: "healthy", loop_alive: true,
        leader: { held: true, epoch: 8, heartbeat_alive: true },
        outbox: { loop_alive: true, pending: 0, dead_letters: 0, stale: false },
      } : {
        ready: false, state: "unhealthy", loop_alive: true,
        leader: { held: false, epoch: 7, heartbeat_alive: false },
        outbox: { loop_alive: true, pending: 1, dead_letters: 1, stale: true, last_error: "relay unavailable" },
      }) as T;
      if (path === "/v1/orchestration/outbox/dead-letters?offset=0&limit=100") return {
        items: [{ id: "outbox-1", event_id: "event-1", topic: "orchestration.run.failed", attempts: 10, last_error: "relay unavailable", dead_lettered_at: "2026-08-03T03:01:00Z", payload: {} }],
        offset: 0, limit: 100, has_more: false,
      } as T;
      if (path === "/v1/orchestration/outbox/dead-letters/outbox-1/requeue" && method === "POST") {
        recovered = true;
        return { id: "outbox-1", event_id: "event-1", status: "queued", attempts: 0 } as T;
      }
      if (path.startsWith("/v1/orchestration/tasks?") && method === "GET") return [detail] as T;
      if (path === "/v1/orchestration/tasks/task-bounded") return detail as T;
      if (path === "/v1/orchestration/tasks/task-bounded/gates?offset=1&limit=500") return {
        task_id: "task-bounded",
        gates: [{ id: "gate-old", kind: "approval", title: "Older approval", status: "open", actions: ["approve"] }],
        offset: 1, limit: 500, has_more: false, next_offset: null, order: "oldest_to_newest",
      } as T;
      if (path === "/v1/orchestration/tasks/task-bounded/runs?offset=1&limit=500") return {
        task_id: "task-bounded",
        runs: [{ id: "run-old", title: "Older run", status: "succeeded" }],
        offset: 1, limit: 500, has_more: false, next_offset: null, order: "oldest_to_newest",
      } as T;
      if (path === "/v1/orchestration/tasks/task-bounded/evidence?offset=1&limit=500") return {
        task_id: "task-bounded",
        evidence: [{ id: "evidence-old", title: "Older evidence", kind: "claim" }],
        offset: 1, limit: 500, has_more: false, next_offset: null, order: "oldest_to_newest",
      } as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<OrchestrationSurface apiRequest={apiRequest} apiDownload={noDownload} initialTaskId="task-bounded" />);

    expect(await screen.findByRole("heading", { name: "Bounded audit task" })).toBeTruthy();
    expect(screen.getByText(/Nested hierarchy is bounded to 256 tasks and depth 3/)).toBeTruthy();
    const healthPanel = await screen.findByLabelText("Orchestration health");
    expect(within(healthPanel).getByText("Lease not held")).toBeTruthy();
    expect(within(healthPanel).getByText("1 pending · 1 dead letter")).toBeTruthy();
    fireEvent.change(within(healthPanel).getByLabelText("Dead-letter operator identity"), {
      target: { value: "on-call@example.com" },
    });
    fireEvent.change(within(healthPanel).getByLabelText("Dead-letter recovery reason"), {
      target: { value: "Subscriber was repaired and verified" },
    });
    fireEvent.click(within(healthPanel).getByRole("button", { name: "Requeue" }));
    await waitFor(() => expect(calls).toContainEqual({
      path: "/v1/orchestration/outbox/dead-letters/outbox-1/requeue",
      method: "POST",
    }));
    await waitFor(() => expect(screen.queryByLabelText("Orchestration health")).toBeNull());

    expect(screen.getByText(/Showing 0 loaded attention gates; older records are not shown yet\./)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Load older" }));
    expect(await screen.findByText("Older approval")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /Agent runs/ }));
    expect(screen.getByText(/Showing 1 loaded runs; older records are not shown yet\./)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Load older" }));
    expect(await screen.findByText("Older run")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /Evidence/ }));
    expect(screen.getByText(/Showing 1 loaded evidence records; older records are not shown yet\./)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Load older" }));
    expect(await screen.findByText("Older evidence")).toBeTruthy();
  });
});
