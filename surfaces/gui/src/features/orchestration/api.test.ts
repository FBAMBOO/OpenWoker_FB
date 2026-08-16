import { describe, expect, it } from "vitest";
import { createOrchestrationApi, type ApiRequest } from "./api";
import {
  ORCHESTRATION_STAGES,
  type AgentProfileSpec,
  type CreateOrchestrationTask,
  type ModelRoutingPolicySpec,
} from "./types";

type RequestCall = { path: string; init?: RequestInit };

const bodyOf = (call: RequestCall) => call.init?.body ? JSON.parse(String(call.init.body)) : undefined;

describe("orchestration API contracts", () => {
  it("uses the versioned task control endpoints", async () => {
    const calls: RequestCall[] = [];
    const request: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      calls.push({ path, init });
      return {
        id: "task-control",
        title: "Control task",
        status: "running",
        stage: "execution_review_test",
        updated_at: "2026-08-03T01:00:00Z",
      } as T;
    };
    const api = createOrchestrationApi(request);

    await api.submitTask("task-control");
    await api.pauseTask("task-control");
    await api.resumeTask("task-control");
    await api.cancelTask("task-control");
    await api.archiveTask("task-control");

    expect(calls.map((call) => [call.path, call.init?.method])).toEqual([
      ["/v1/orchestration/tasks/task-control/submit", "POST"],
      ["/v1/orchestration/tasks/task-control/pause", "POST"],
      ["/v1/orchestration/tasks/task-control/resume", "POST"],
      ["/v1/orchestration/tasks/task-control/cancel", "POST"],
      ["/v1/orchestration/tasks/task-control/archive", "POST"],
    ]);
  });

  it("preserves normalized gate actions and canonicalizes key-based DAG dependencies", async () => {
    const request: ApiRequest = async <T,>() => ({
      id: "task-contract",
      title: "Contract probe",
      status: "waiting_human",
      stage: "planning",
      updated_at: "2026-08-03T01:00:00Z",
      nodes: [
        { id: "node-uuid-1", key: "prepare", title: "Prepare", status: "completed" },
        { id: "node-uuid-2", key: "verify", title: "Verify", status: "pending", depends_on: ["prepare"] },
      ],
      edges: [{ from: "prepare", to: "verify" }],
      attention: [{
        id: "gate-contract",
        kind: "clarification",
        status: "pending",
        version: 3,
        prompt: { title: "Clarify", actions: ["submit", "cancel"] },
        actions: [
          { id: "submit", label: "Submit", tone: "primary", requires_response: true },
          { id: "cancel", label: "Cancel", tone: "danger", requires_response: false },
        ],
      }],
      runs: [{ id: "run-parent", node_id: "node-uuid-1", title: "Parent", status: "running" }],
      children_details: [{
        id: "task-child",
        title: "Child",
        status: "running",
        stage: "execution_review_test",
        updated_at: "2026-08-03T01:01:00Z",
        parent_task_id: "task-contract",
        parent_run_id: "run-parent",
        nodes: [],
        edges: [],
        attention: [],
        runs: [{ id: "run-child", parent_run_id: "run-parent", title: "Child run", status: "running" }],
      }],
    }) as T;

    const detail = await createOrchestrationApi(request).getTask("task-contract");

    expect(detail.nodes?.[1]).toMatchObject({
      id: "node-uuid-2",
      key: "verify",
      depends_on: ["node-uuid-1"],
    });
    expect(detail.edges).toEqual([{ from: "node-uuid-1", to: "node-uuid-2" }]);
    expect(detail.attention?.[0]).toMatchObject({
      version: 3,
      actions: [
        expect.objectContaining({ id: "submit", tone: "primary", requires_response: true }),
        expect.objectContaining({ id: "cancel", tone: "danger", requires_response: false }),
      ],
    });
    expect(detail.children_details?.[0]).toMatchObject({
      id: "task-child",
      parent_task_id: "task-contract",
      parent_run_id: "run-parent",
    });
    expect(detail.children_details?.[0].runs?.[0]).toMatchObject({ id: "run-child", parent_run_id: "run-parent" });
  });

  it("keeps the fixed protocol and normalizes core task records", async () => {
    const request: ApiRequest = async <T,>(path: string) => {
      if (path === "/v1/orchestration/tasks") {
        return {
          tasks: [{
            task_id: "task-1",
            title: "Ship the release",
            status: "waiting_human",
            current_stage: "inter_step_evaluation",
            updated_at: "2026-08-03T01:00:00Z",
            attention_gates: [{ status: "open" }],
          }],
        } as T;
      }
      return {
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
          { stage: "clarification", disposition: "skipped", sequence: 2 },
          { stage: "inter_step_evaluation", disposition: "completed", attempt: 1, sequence: 6 },
          { stage: "inter_step_evaluation", disposition: "active", attempt: 2, sequence: 7 },
        ],
        plan: {
          nodes: [
            { node_id: "prepare", title: "Prepare", status: "completed", agent: "worker" },
            { node_id: "verify", title: "Verify", status: "pending", agent: "tester" },
          ],
          edges: [{ from_node_id: "prepare", to_node_id: "verify" }],
        },
        runs: [{
          run_id: "run-1",
          session_id: "session-1",
          node_id: "prepare",
          status: "succeeded",
          model: "openai:gpt-high",
          routing_decision: { reason: "Highest eligible quality" },
          output: { summary: "Prepared release assets" },
        }],
        gates: [{
          gate_id: "gate-1",
          kind: "approval",
          status: "open",
          prompt: {
            question: "Approve production?",
            criteria: { "Release tests pass": "pass" },
            verification: [{
              node_id: "verify",
              node_key: "verify",
              role: "tester",
              status: "pass",
              criteria: { "Release tests pass": "pass" },
              findings: ["No regressions"],
              source: "run_output",
            }],
            policy_reasons: ["Explicit acceptance is required"],
            options: [{ value: "approve", label: "Approve", tone: "primary" }],
          },
        }],
        evidence: [{
          evidence_id: "evidence-1",
          kind: "test",
          run_id: "run-1",
          created_by: "tester-runtime",
          payload: {
            title: "Release tests",
            summary: "42 tests passed",
            subject: { candidate_hash: "candidate-hash" },
            subject_matches: true,
            missing_criteria: ["operator sign-off"],
          },
        }],
        events: [{
          id: "event-1",
          event_type: "stage_advanced",
          created_at: "2026-08-03T01:05:00Z",
          payload: { message: "Evaluation requested", actor: "orchestrator", stage: "inter_step_evaluation" },
        }],
        agent_profile_snapshot: { profile_id: "release-worker", display_name: "Release worker", version: 3, content_hash: "profile-hash" },
        routing_policy_snapshot: { policy_id: "quality-first", version: 2, content_hash: "policy-hash" },
      } as T;
    };

    expect(ORCHESTRATION_STAGES).toEqual([
      "intake",
      "complexity_assessment",
      "clarification",
      "planning",
      "execution_review_test",
      "inter_step_evaluation",
      "final_acceptance",
      "archive",
    ]);

    const api = createOrchestrationApi(request);
    const tasks = await api.listTasks();
    const detail = await api.getTask("task-1");

    expect(tasks[0]).toMatchObject({
      id: "task-1",
      status: "waiting_human",
      stage: "inter_step_evaluation",
      attention_count: 1,
    });
    expect(detail.stages).toEqual(expect.arrayContaining([
      expect.objectContaining({ stage: "clarification", status: "skipped" }),
      expect.objectContaining({ stage: "inter_step_evaluation", status: "running", attempt: 2 }),
    ]));
    expect(detail.nodes).toEqual([
      expect.objectContaining({ id: "prepare", run_ids: ["run-1"] }),
      expect.objectContaining({ id: "verify", depends_on: ["prepare"] }),
    ]);
    expect(detail.edges).toEqual([{ from: "prepare", to: "verify" }]);
    expect(detail.runs?.[0]).toMatchObject({
      id: "run-1",
      title: "Prepare",
      status: "succeeded",
      model_id: "openai:gpt-high",
      routing_reason: "Highest eligible quality",
      session_id: "session-1",
    });
    expect(detail.attention?.[0]).toMatchObject({
      id: "gate-1",
      status: "pending",
      title: "Approve production?",
      actions: [{ id: "approve", label: "Approve", tone: "primary", requires_response: false }],
      criteria: { "Release tests pass": "pass" },
      verification: [expect.objectContaining({ node_id: "verify", status: "pass", findings: ["No regressions"] })],
      policy_reasons: ["Explicit acceptance is required"],
    });
    expect(detail.evidence?.[0]).toMatchObject({
      title: "Release tests",
      summary: "42 tests passed",
      actor: "tester-runtime",
      subject: { candidate_hash: "candidate-hash" },
      subject_matches: true,
      missing_criteria: ["operator sign-off"],
      payload: expect.objectContaining({ subject_matches: true }),
    });
    expect(detail.activity?.[0]).toMatchObject({ summary: "Evaluation requested", actor: "orchestrator" });
    expect(detail.profile_snapshot).toEqual({ id: "release-worker", name: "Release worker", version: 3, content_hash: "profile-hash" });
    expect(detail.model_policy_snapshot).toEqual({ id: "quality-first", version: 2, content_hash: "policy-hash", name: undefined });
  });

  it("preserves bounded failure diagnostics for runs, activity, and reconciliation gates", async () => {
    const request: ApiRequest = async <T,>() => ({
      id: "task-failed",
      title: "Diagnose execution",
      status: "waiting_human",
      stage: "inter_step_evaluation",
      updated_at: "2026-08-04T01:00:00Z",
      runs: [{
        id: "run-failed",
        node_key: "understand",
        status: "failed",
        attempt: 2,
        summary: "The runtime rejected its output schema.",
        error_kind: "codex_turn_failed",
        error_message: "HTTP 400 invalid_json_schema",
      }],
      attention: [{
        id: "gate-reconcile",
        kind: "reconciliation",
        status: "open",
        prompt: {
          title: "Execution needs reconciliation",
          failed_runs: [{
            id: "run-failed",
            node_key: "understand",
            status: "failed",
            attempt: 2,
            summary: "HTTP 400 invalid_json_schema",
            error_kind: "codex_turn_failed",
          }],
          workspace_commit_failures: [{
            id: "run-publish",
            node_key: "publish",
            status: "failed",
            summary: "The isolated workspace could not be published.",
            error_kind: "workspace_commit_failed",
          }],
          actions: ["retry", "cancel"],
        },
      }],
      activity: [{
        id: "event-failed",
        type: "run.failed",
        summary: "run failed",
        detail: JSON.stringify({
          error_kind: "codex_turn_failed",
          error_message: "HTTP 400 invalid_json_schema",
        }),
        created_at: "2026-08-04T01:01:00Z",
      }],
    }) as T;

    const detail = await createOrchestrationApi(request).getTask("task-failed");

    expect(detail.runs?.[0]).toMatchObject({
      id: "run-failed",
      error_kind: "codex_turn_failed",
      error_message: "HTTP 400 invalid_json_schema",
      summary: "The runtime rejected its output schema.",
    });
    expect(detail.attention?.[0].failed_runs?.[0]).toMatchObject({
      id: "run-failed",
      title: "understand",
      attempt: 2,
      error_kind: "codex_turn_failed",
      summary: "HTTP 400 invalid_json_schema",
    });
    expect(detail.attention?.[0].workspace_commit_failures?.[0]).toMatchObject({
      id: "run-publish",
      error_kind: "workspace_commit_failed",
    });
    expect(detail.activity?.[0]).toMatchObject({
      error_kind: "codex_turn_failed",
      error_message: "HTTP 400 invalid_json_schema",
    });
  });

  it("sends pagination filters and reuses command keys for exact create and gate retries", async () => {
    const calls: RequestCall[] = [];
    const request: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      calls.push({ path, init });
      if (path.includes("/resolve")) return { ok: true } as T;
      if (path.includes("/tasks?")) return [] as T;
      return {
        id: "task-keyed",
        title: "Keyed task",
        status: "draft",
        stage: "intake",
        updated_at: "2026-08-03T01:00:00Z",
      } as T;
    };
    const spec: CreateOrchestrationTask = {
      objective: "Create exactly once",
      domain: "knowledge",
      read_only: false,
      acceptance_criteria: ["Only one task exists"],
    };
    const api = createOrchestrationApi(request);

    await api.createTask(spec);
    await api.createTask(spec);
    await api.resolveAttention("task-keyed", "gate-1", "accept", undefined, 2);
    await api.resolveAttention("task-keyed", "gate-1", "accept", undefined, 2);
    await api.listTasks({ statuses: ["running", "waiting_human"], limit: 21, offset: 20 });

    const createBodies = calls.slice(0, 2).map(bodyOf);
    expect(createBodies[0].idempotency_key).toEqual(expect.stringMatching(/^gui:task-create:/));
    expect(createBodies[0].read_only).toBe(false);
    expect(createBodies[1].idempotency_key).toBe(createBodies[0].idempotency_key);
    const resolveBodies = calls.slice(2, 4).map(bodyOf);
    expect(resolveBodies[0].idempotency_key).toEqual(expect.stringMatching(/^gui:gate-gate-1:/));
    expect(resolveBodies[1].idempotency_key).toBe(resolveBodies[0].idempotency_key);
    expect(calls[4].path).toBe(
      "/v1/orchestration/tasks?status=running&status=waiting_human&limit=21&offset=20",
    );
  });

  it("preserves bounded detail metadata and exposes audit, health, and dead-letter recovery contracts", async () => {
    const calls: RequestCall[] = [];
    const request: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      calls.push({ path, init });
      if (path === "/v1/orchestration/tasks/task-audit") return {
        id: "task-audit",
        title: "Audit task",
        status: "running",
        stage: "execution_review_test",
        updated_at: "2026-08-03T01:00:00Z",
        attention: [{ id: "gate-new", kind: "question", title: "New gate", status: "resolved" }],
        attention_page: { has_more: true, page_size: 500 },
        runs: [{ id: "run-new", title: "New run", status: "running" }],
        runs_page: { has_more: true, page_size: 500 },
        evidence: [{ id: "evidence-new", kind: "test", title: "New evidence" }],
        evidence_page: { has_more: true, page_size: 500 },
        detail_limits: { child_depth: 3, runs: 500, evidence: 500, activity: 500 },
        children_page: { truncated: true, tree_truncated: true, depth_limit_reached: false, returned: 8, total: 8, tree_row_limit: 256 },
        runtime_page: { truncated: true, returned: 256, limit: 256 },
      } as T;
      if (path === "/v1/orchestration/tasks/task-audit/gates?offset=500&limit=500") return {
        task_id: "task-audit",
        gates: [{ id: "gate-old", kind: "approval", title: "Old gate", status: "open" }],
        offset: 500, limit: 500, has_more: false, next_offset: null, order: "oldest_to_newest",
      } as T;
      if (path === "/v1/orchestration/tasks/task-audit/runs?offset=500&limit=500") return {
        task_id: "task-audit",
        runs: [{ id: "run-old", title: "Old run", status: "succeeded" }],
        offset: 500, limit: 500, has_more: false, next_offset: null, order: "oldest_to_newest",
      } as T;
      if (path === "/v1/orchestration/tasks/task-audit/evidence?offset=500&limit=500") return {
        task_id: "task-audit",
        evidence: [{ id: "evidence-old", kind: "claim", title: "Old evidence", created_by: "reviewer" }],
        offset: 500, limit: 500, has_more: false, next_offset: null, order: "oldest_to_newest",
      } as T;
      if (path === "/v1/orchestration/health") {
        const readiness = new Error("Request failed (503)") as Error & { payload: unknown };
        readiness.payload = {
          ready: false,
          state: "unhealthy",
          loop_alive: true,
          leader: { held: false, epoch: 4, heartbeat_alive: false },
          outbox: { loop_alive: true, pending: 1, dead_letters: 1, stale: true },
        };
        throw readiness;
      }
      if (path === "/v1/orchestration/outbox/dead-letters?offset=0&limit=100") return {
        items: [{ id: "outbox-1", event_id: "event-1", topic: "orchestration.run.failed", attempts: 10, last_error: "relay down", payload: {} }],
        offset: 0, limit: 100, has_more: false, next_offset: null,
      } as T;
      if (path === "/v1/orchestration/outbox/dead-letters/outbox-1/requeue") return {
        id: "outbox-1", event_id: "event-1", status: "queued", attempts: 0,
      } as T;
      throw new Error(`Unexpected request: ${path}`);
    };
    const api = createOrchestrationApi(request);

    const detail = await api.getTask("task-audit");
    const gates = await api.listTaskGates("task-audit", 500, 500);
    const runs = await api.listTaskRuns("task-audit", 500, 500);
    const evidence = await api.listTaskEvidence("task-audit", 500, 500);
    const health = await api.getHealth();
    const deadLetters = await api.listDeadLetters();
    await api.requeueDeadLetter("outbox-1", {
      actor: "on-call@example.com",
      reason: "Subscriber was repaired and verified",
      idempotencyKey: "requeue-outbox-1",
    });

    expect(detail.attention_page).toEqual(expect.objectContaining({ has_more: true, page_size: 500 }));
    expect(detail.runs_page).toEqual(expect.objectContaining({ has_more: true, page_size: 500 }));
    expect(detail.evidence_page).toEqual(expect.objectContaining({ has_more: true, page_size: 500 }));
    expect(detail.detail_limits).toEqual(expect.objectContaining({ child_depth: 3, runs: 500, evidence: 500 }));
    expect(detail.children_page).toEqual(expect.objectContaining({ truncated: true, tree_row_limit: 256 }));
    expect(detail.runtime_page).toEqual({ truncated: true, returned: 256, limit: 256 });
    expect(gates).toEqual(expect.objectContaining({ has_more: false, next_offset: null, gates: [expect.objectContaining({ id: "gate-old", status: "pending" })] }));
    expect(runs).toEqual(expect.objectContaining({ has_more: false, next_offset: null, runs: [expect.objectContaining({ id: "run-old" })] }));
    expect(evidence.evidence[0]).toMatchObject({ id: "evidence-old", actor: "reviewer" });
    expect(health).toMatchObject({ ready: false, leader: { held: false }, outbox: { dead_letters: 1, stale: true } });
    expect(deadLetters.items[0]).toMatchObject({ id: "outbox-1", attempts: 10, last_error: "relay down" });
    const requeueCall = calls[calls.length - 1];
    expect(requeueCall).toMatchObject({
      path: "/v1/orchestration/outbox/dead-letters/outbox-1/requeue",
      init: { method: "POST" },
    });
    expect(new Headers(requeueCall.init?.headers).get("Idempotency-Key")).toBe("requeue-outbox-1");
    expect(bodyOf(requeueCall)).toEqual({
      actor: "on-call@example.com",
      reason: "Subscriber was repaired and verified",
    });
  });

  it("sends stable clone, ETag draft, publish, and RoutingRequest shapes", async () => {
    const calls: RequestCall[] = [];
    const request: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      calls.push({ path, init });
      if (path.endsWith("/draft/simulate")) {
        return {
          decision: {
            decision_id: "decision-1",
            selected_model: "openai:gpt-high",
            fallback_models: ["anthropic:balanced"],
            reason: "Highest eligible quality",
            catalog_hash: "catalog-hash",
            evaluations: [
              { model_id: "openai:gpt-high", provider: "openai", eligible: true, reasons: [], quality: 95, rank: 1 },
              { model_id: "anthropic:balanced", provider: "anthropic", eligible: false, reason_codes: ["provider_not_allowed"], quality: 80 },
            ],
          },
        } as T;
      }
      return {} as T;
    };
    const profile: AgentProfileSpec = {
      schema_version: 1,
      profile_id: "release/worker",
      display_name: "Release worker",
      role: "worker",
      instructions: "Prepare the release.",
      allowed_tools: ["read_file"],
      allowed_child_roles: [],
      permission_mode: "interactive",
      model_policy: "quality-first",
      max_iterations: 12,
      max_children: 0,
      base: null,
      metadata: { token_budget: 12000, evidence_required: true },
    };
    const policy: ModelRoutingPolicySpec = {
      schema_version: 1,
      policy_id: "quality-first",
      require_verified: true,
      allow_unknown_cost: false,
      allowed_providers: ["openai"],
      allowed_models: ["openai:gpt-high", "anthropic:balanced"],
      blocked_models: [],
      fallback_limit: 1,
      fallback_for_explicit: false,
    };
    const routingRequest = {
      purpose: "Review a release",
      required_capabilities: ["tools"],
      input_tokens: 5000,
      reserved_output_tokens: 1000,
      minimum_context: 16000,
      max_cost_microusd: 250000,
      requested_model: null,
      preferred_models: ["openai:gpt-high"],
      allowed_providers: ["openai"],
      excluded_models: [],
      correlation: { task_id: "task-1" },
    };

    const api = createOrchestrationApi(request);
    await api.saveAgentProfileDraft("release/worker", profile, "profile-etag");
    await api.cloneAgentProfile("release/worker", "release-worker-copy", "Release worker copy");
    await api.publishAgentProfile("release/worker", "profile-etag-2");
    await api.saveModelPolicyDraft("quality/first", policy, "policy-etag");
    const decision = await api.simulateModelPolicy("quality/first", policy, routingRequest);

    expect(calls[0].path).toBe("/v1/orchestration/agent-profiles/release%2Fworker/draft");
    expect(calls[0].init?.method).toBe("PUT");
    expect(calls[0].init?.headers).toMatchObject({ "If-Match": "profile-etag" });
    expect(bodyOf(calls[0])).toEqual({ spec: profile });

    expect(calls[1].path).toBe("/v1/orchestration/agent-profiles/release%2Fworker/clone");
    expect(bodyOf(calls[1])).toEqual({
      new_profile_id: "release-worker-copy",
      overrides: { display_name: "Release worker copy" },
    });
    expect(calls[2].path).toBe("/v1/orchestration/agent-profiles/release%2Fworker/draft/publish");
    expect(calls[2].init?.headers).toMatchObject({ "If-Match": "profile-etag-2" });

    expect(calls[3].path).toBe("/v1/orchestration/model-policies/quality%2Ffirst/draft");
    expect(calls[3].init?.method).toBe("PUT");
    expect(calls[3].init?.headers).toMatchObject({ "If-Match": "policy-etag" });
    expect(bodyOf(calls[3])).toEqual({ spec: policy });
    expect(bodyOf(calls[4])).toEqual({ policy, request: routingRequest });
    expect(decision).toEqual(expect.objectContaining({
      decision_id: "decision-1",
      selected_model: "openai:gpt-high",
      fallback_models: ["anthropic:balanced"],
      catalog_hash: "catalog-hash",
    }));
    expect(decision.evaluations[1]).toMatchObject({
      model_id: "anthropic:balanced",
      eligible: false,
      reasons: ["provider_not_allowed"],
    });
  });

  it("normalizes subscription runtime catalog metadata and refresh health", async () => {
    const calls: RequestCall[] = [];
    const runtimeId = "codex-subscription:gpt-5.6-sol@max";
    const request: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      calls.push({ path, init });
      if (path === "/v1/orchestration/model-catalog") return [{
        id: runtimeId,
        label: "Codex Subscription · GPT-5.6 Sol · Max",
        provider: "codex-subscription",
        source: "subscription-runtime",
        quality: 100,
        configured: true,
        availability: "blocked_by_policy",
        availability_reason: "Local-owner policy requires an interactive task.",
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
      }] as T;
      if (path === "/v1/orchestration/subscription-runtimes?refresh=true") return [{
        runtime_id: runtimeId,
        provider: "codex-subscription",
        display_name: "Codex Subscription · GPT-5.6 Sol · Max",
        command: "codex",
        model: "gpt-5.6-sol",
        reasoning_effort: "max",
        quality: 100,
        context_window: 200000,
        minimum_cli_version: "0.146.0",
        protocol: "codex-app-server-v2",
        interactive_only: false,
        local_owner_only: true,
        capabilities: ["tools", "streaming"],
        health: {
          runtime_id: runtimeId,
          provider: "codex-subscription",
          installed: true,
          authenticated: true,
          available: true,
          policy_eligible: true,
          version: "0.150.0",
          auth_kind: "chatgpt_subscription",
          executable: "codex.exe",
          reason: "Ready",
          checked_at: 1785722400,
        },
      }] as T;
      throw new Error(`Unexpected request: ${path}`);
    };

    const api = createOrchestrationApi(request);
    const catalog = await api.getModelCatalog();
    const runtimes = await api.getSubscriptionRuntimes(true);

    expect(catalog[0]).toMatchObject({
      id: runtimeId,
      source: "subscription-runtime",
      availability: "blocked_by_policy",
      availability_reason: "Local-owner policy requires an interactive task.",
      runtime: { model: "gpt-5.6-sol", reasoning_effort: "max", local_owner_only: true },
    });
    expect(runtimes[0]).toMatchObject({
      runtime_id: runtimeId,
      model: "gpt-5.6-sol",
      reasoning_effort: "max",
      availability: "available",
      availability_reason: "Ready",
      health: { installed: true, authenticated: true, version: "0.150.0", auth_kind: "chatgpt_subscription" },
    });
    expect(calls.map((call) => call.path)).toEqual([
      "/v1/orchestration/model-catalog",
      "/v1/orchestration/subscription-runtimes?refresh=true",
    ]);
  });

  it("normalizes the additive mixed-runtime preset catalog", async () => {
    const request: ApiRequest = async <T,>(path: string) => {
      expect(path).toBe("/v1/orchestration/runtime-presets");
      return {
        default_preset_id: "production-codex-led-mixed-v1",
        items: [{
          id: "production-codex-led-mixed-v1",
          display_name: "Production · Codex-led mixed",
          description: "Codex plans and implements; Claude verifies.",
          roles: {
            planner: "codex-subscription:gpt-5.6-sol@max",
            reviewer: {
              runtime_id: "claude-code-subscription:claude-opus-5@high",
              access: "read_only",
              fresh_session: true,
            },
          },
          required_runtime_ids: [
            "codex-subscription:gpt-5.6-sol@max",
            "claude-code-subscription:claude-opus-5@high",
          ],
          available: true,
        }],
      } as T;
    };

    const presets = await createOrchestrationApi(request).getRuntimePresets();

    expect(presets).toEqual([expect.objectContaining({
      id: "production-codex-led-mixed-v1",
      name: "Production · Codex-led mixed",
      is_default: true,
      available: true,
      roles: [
        expect.objectContaining({ role: "planner", runtime_id: "codex-subscription:gpt-5.6-sol@max" }),
        expect.objectContaining({
          role: "reviewer",
          runtime_id: "claude-code-subscription:claude-opus-5@high",
          access: "read_only",
          fresh_session: true,
        }),
      ],
    })]);
  });
});
