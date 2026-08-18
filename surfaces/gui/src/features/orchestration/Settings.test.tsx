import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { AgentProfilesSettings } from "./AgentProfilesSettings";
import { ModelRoutingSettings } from "./ModelRoutingSettings";
import { RuntimeCommunicationSettings } from "./RuntimeCommunicationSettings";
import type { ApiRequest } from "./api";
import type { AgentProfileSpec, ModelRoutingPolicySpec } from "./types";

type RequestCall = { path: string; method: string; body?: unknown; headers?: HeadersInit };

const parseBody = (init?: RequestInit) => init?.body ? JSON.parse(String(init.body)) : undefined;

const profileSpec = (overrides: Partial<AgentProfileSpec> = {}): AgentProfileSpec => ({
  schema_version: 1,
  profile_id: "release-worker",
  display_name: "Release worker",
  role: "worker",
  instructions: "Prepare the scoped release and attach evidence.",
  allowed_tools: ["read_file", "run_shell"],
  allowed_child_roles: [],
  permission_mode: "interactive",
  model_policy: "quality-first",
  max_iterations: 12,
  max_children: 0,
  base: null,
  metadata: {
    token_budget: 24000,
    tool_call_budget: 20,
    timeout_seconds: 1800,
    evidence_required: true,
    tests_required: true,
    review_required: true,
  },
  ...overrides,
});

const policySpec = (overrides: Partial<ModelRoutingPolicySpec> = {}): ModelRoutingPolicySpec => ({
  schema_version: 1,
  policy_id: "release-routing",
  require_verified: true,
  allow_unknown_cost: false,
  allowed_providers: ["openai", "anthropic"],
  allowed_models: ["openai:gpt-high", "anthropic:balanced"],
  blocked_models: ["openai:offline"],
  fallback_limit: 2,
  fallback_for_explicit: false,
  ...overrides,
});

afterEach(cleanup);

describe("AgentProfilesSettings", () => {
  it("keeps built-ins immutable and clones the selected published version", async () => {
    const calls: RequestCall[] = [];
    let cloned = false;
    const builtin = {
      ...profileSpec({ profile_id: "worker", display_name: "Worker" }),
      version: 4,
      content_hash: "builtin-profile-hash",
      builtin: true,
      published_at: "2026-08-01T00:00:00Z",
    };
    const clone = {
      ...profileSpec({
        profile_id: "my-worker",
        display_name: "My worker",
        base: { profile_id: "worker", version: 4 },
      }),
      version: 1,
      content_hash: "clone-profile-hash",
      builtin: false,
      cloned_from: { profile_id: "worker", version: 4 },
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = parseBody(init);
      calls.push({ path, method, body, headers: init?.headers });
      if (path === "/v1/orchestration/agent-profiles" && method === "GET") {
        return { profiles: [builtin, ...(cloned ? [clone] : [])] } as T;
      }
      if (path === "/v1/orchestration/model-policies") {
        return { policies: [{ policy_id: "quality-first", display_name: "Quality first", version: 2, builtin: true }] } as T;
      }
      if (path === "/v1/orchestration/agent-profiles/worker/clone" && method === "POST") {
        cloned = true;
        return { profile: clone } as T;
      }
      if (path === "/v1/orchestration/agent-profiles/worker") return { profile: builtin } as T;
      if (path === "/v1/orchestration/agent-profiles/my-worker") return { profile: clone } as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<AgentProfilesSettings apiRequest={apiRequest} initialProfileId="worker" />);

    const displayName = await screen.findByLabelText("Display name") as HTMLInputElement;
    expect(displayName.value).toBe("Worker");
    expect(displayName.disabled).toBe(true);
    expect(screen.getAllByText(/Built-in/).length).toBeGreaterThan(0);
    expect(screen.getByText(/cannot be overwritten/)).toBeTruthy();

    fireEvent.change(screen.getByLabelText("Clone profile ID"), { target: { value: "My Worker" } });
    fireEvent.change(screen.getByLabelText("Clone profile name"), { target: { value: "My worker" } });
    fireEvent.click(screen.getByRole("button", { name: "Clone" }));

    await waitFor(() => {
      expect(calls).toContainEqual(expect.objectContaining({
        path: "/v1/orchestration/agent-profiles/worker/clone",
        method: "POST",
        body: { new_profile_id: "my-worker", overrides: { display_name: "My worker" } },
      }));
    });
    expect(await screen.findByText("Cloned from worker v4")).toBeTruthy();
  });

  it("edits metadata in a versioned draft, validates it, and publishes with its ETag", async () => {
    const calls: RequestCall[] = [];
    let serverSpec = profileSpec();
    const detail = () => ({
      id: "release-worker",
      name: serverSpec.display_name,
      builtin: false,
      archived: false,
      current_version: 1,
      has_draft: true,
      versions: [{ profile_id: "release-worker", version: 1, spec: profileSpec(), content_hash: "profile-v1" }],
      draft: {
        profile_id: "release-worker",
        base_version: 1,
        etag: "profile-draft-etag",
        spec: serverSpec,
      },
    });
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = parseBody(init);
      calls.push({ path, method, body, headers: init?.headers });
      if (path === "/v1/orchestration/agent-profiles" && method === "GET") {
        return { profiles: [{ id: "release-worker", name: serverSpec.display_name, builtin: false, current_version: 1, has_draft: true }] } as T;
      }
      if (path === "/v1/orchestration/model-policies") {
        return { policies: [{ policy_id: "quality-first", display_name: "Quality first", version: 2, builtin: true }] } as T;
      }
      if (path === "/v1/orchestration/agent-profiles/release-worker/draft" && method === "PUT") {
        serverSpec = (body as { spec: AgentProfileSpec }).spec;
        return detail() as T;
      }
      if (path.endsWith("/draft/validate")) return { valid: true, errors: [], warnings: [] } as T;
      if (path.endsWith("/draft/publish")) return detail() as T;
      if (path === "/v1/orchestration/agent-profiles/release-worker") return detail() as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<AgentProfilesSettings apiRequest={apiRequest} initialProfileId="release-worker" />);

    const displayName = await screen.findByLabelText("Display name") as HTMLInputElement;
    expect(displayName.disabled).toBe(false);
    const versionSelect = screen.getByLabelText("Profile version") as HTMLSelectElement;
    expect([...versionSelect.options].map((option) => option.value)).toEqual(["draft", "v1"]);
    expect(screen.queryByLabelText(/Tool-call budget/)).toBeNull();
    expect(screen.queryByLabelText(/^Token budget/)).toBeNull();
    expect(screen.queryByLabelText(/^Timeout/)).toBeNull();
    expect(screen.queryByLabelText("Max iterations")).toBeNull();

    fireEvent.change(displayName, { target: { value: "Release specialist" } });
    fireEvent.change(screen.getByLabelText("Allowed tool IDs"), { target: { value: "read_file, grep" } });
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));

    await waitFor(() => {
      const save = calls.find((call) => call.path.endsWith("/release-worker/draft") && call.method === "PUT");
      expect(save?.headers).toMatchObject({ "If-Match": "profile-draft-etag" });
      expect(save?.body).toMatchObject({
        spec: {
          display_name: "Release specialist",
          allowed_tools: ["read_file", "grep"],
        },
      });
    });

    const validate = screen.getByRole("button", { name: "Validate" }) as HTMLButtonElement;
    await waitFor(() => expect(validate.disabled).toBe(false));
    fireEvent.click(validate);
    expect(await screen.findByText("Validation passed")).toBeTruthy();

    const publish = screen.getByRole("button", { name: "Publish version" }) as HTMLButtonElement;
    expect(publish.disabled).toBe(false);
    fireEvent.click(publish);
    await waitFor(() => {
      const publishCall = calls.find((call) => call.path.endsWith("/draft/publish"));
      expect(publishCall?.method).toBe("POST");
      expect(publishCall?.headers).toMatchObject({ "If-Match": "profile-draft-etag" });
    });
  });
});

describe("ModelRoutingSettings", () => {
  it("preserves pool order in drafts and explains a quality-first RoutingDecision", async () => {
    const calls: RequestCall[] = [];
    let serverSpec = policySpec();
    const detail = () => ({
      id: "release-routing",
      name: "Release routing",
      builtin: false,
      archived: false,
      current_version: 1,
      has_draft: true,
      versions: [{ policy_id: "release-routing", version: 1, spec: policySpec(), content_hash: "routing-v1" }],
      draft: {
        policy_id: "release-routing",
        base_version: 1,
        etag: "routing-draft-etag",
        spec: serverSpec,
      },
    });
    const catalog = [
      { model_id: "openai:gpt-high", label: "GPT High", provider: "openai", quality: 95, configured: true, available: true, verified: true, capabilities: ["tools", "vision"], context_window_tokens: 200000, input_microusd_per_million: 5000000, latency_rank: 2 },
      { model_id: "anthropic:balanced", label: "Balanced", provider: "anthropic", quality: 80, configured: true, available: true, verified: true, capabilities: { tools: true, vision: false }, context_window: 100000, input_microusd_per_million: 3000000, latency_rank: 1 },
      { model_id: "openai:offline", label: "Offline", provider: "openai", quality: 99, configured: true, available: false, verified: false, capabilities: [], context_window: 100000, latency_rank: 3 },
      { id: "codex-subscription:gpt-5.6-sol@max", label: "Codex Subscription · GPT-5.6 Sol · Max", provider: "codex-subscription", source: "subscription-runtime", quality: 100, configured: true, availability: "configured", verified: true, capabilities: ["tools", "streaming"], context_window: 200000, latency_rank: 1000, runtime: { protocol: "codex-app-server-v2", model: "gpt-5.6-sol", reasoning_effort: "max", local_owner_only: true, interactive_only: false } },
    ];
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = parseBody(init);
      calls.push({ path, method, body, headers: init?.headers });
      if (path === "/v1/orchestration/model-policies" && method === "GET") {
        return { policies: [{ id: "release-routing", name: "Release routing", builtin: false, current_version: 1, has_draft: true }] } as T;
      }
      if (path === "/v1/orchestration/model-catalog") return { models: catalog } as T;
      if (path === "/v1/orchestration/subscription-runtimes" || path === "/v1/orchestration/subscription-runtimes?refresh=true") return [{
        runtime_id: "codex-subscription:gpt-5.6-sol@max",
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
        health: { installed: true, authenticated: true, available: true, policy_eligible: true, version: "0.150.0", auth_kind: "chatgpt_subscription", executable: "codex.exe", reason: "Ready", checked_at: 1785722400 },
      }] as T;
      if (path === "/v1/orchestration/model-policies/release-routing/draft" && method === "PUT") {
        serverSpec = (body as { spec: ModelRoutingPolicySpec }).spec;
        return detail() as T;
      }
      if (path.endsWith("/draft/simulate")) {
        return {
          decision_id: "routing-decision-1",
          selected_model: "openai:gpt-high",
          fallback_models: ["anthropic:balanced"],
          reason: "Highest eligible quality; latency broke no ties.",
          catalog_hash: "catalog-hash",
          evaluations: [
            { model_id: "openai:gpt-high", provider: "openai", eligible: true, reasons: [], quality: 95, rank: 1, latency_rank: 2 },
            { model_id: "anthropic:balanced", provider: "anthropic", eligible: true, reasons: [], quality: 80, rank: 2, latency_rank: 1 },
            { model_id: "openai:offline", provider: "openai", eligible: false, reasons: ["blocked_by_policy", "unverified"], quality: 99 },
          ],
        } as T;
      }
      if (path === "/v1/orchestration/model-policies/release-routing") return detail() as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<ModelRoutingSettings apiRequest={apiRequest} initialPolicyId="release-routing" />);

    expect(await screen.findByText(/Quality is the primary rank/)).toBeTruthy();
    expect(screen.getAllByText(/High.*95/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Balanced.*80/).length).toBeGreaterThan(0);
    expect(await screen.findByTestId("subscription-runtime-health")).toBeTruthy();
    expect(screen.getAllByText("Codex Subscription · GPT-5.6 Sol · Max").length).toBeGreaterThan(0);
    expect(screen.getByText("gpt-5.6-sol")).toBeTruthy();
    expect(screen.getByText("codex 0.150.0")).toBeTruthy();
    expect(screen.getByText("Chatgpt Subscription")).toBeTruthy();
    expect(screen.getAllByText("Subscription runtime").length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: "Refresh subscription runtimes" }));
    await waitFor(() => expect(calls.some((call) => call.path === "/v1/orchestration/subscription-runtimes?refresh=true")).toBe(true));
    const versionSelect = screen.getByLabelText("Policy version") as HTMLSelectElement;
    expect([...versionSelect.options].map((option) => option.value)).toEqual(["draft", "v1"]);

    fireEvent.click(screen.getByLabelText("Move openai:gpt-high down"));
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
    await waitFor(() => {
      const save = calls.find((call) => call.path.endsWith("/release-routing/draft") && call.method === "PUT");
      expect(save?.headers).toMatchObject({ "If-Match": "routing-draft-etag" });
      expect(save?.body).toMatchObject({
        spec: { allowed_models: ["anthropic:balanced", "openai:gpt-high"] },
      });
    });

    const simulate = screen.getByRole("button", { name: "Run simulation" }) as HTMLButtonElement;
    await waitFor(() => expect(simulate.disabled).toBe(false));
    fireEvent.click(simulate);

    expect(await screen.findByText("Selected openai:gpt-high")).toBeTruthy();
    expect(screen.getByText("Highest eligible quality; latency broke no ties.")).toBeTruthy();
    expect(screen.getByText(/Fallbacks:/).textContent).toContain("anthropic:balanced");
    expect(screen.getByText(/Blocked By Policy, Unverified/)).toBeTruthy();

    const simulationCall = calls.find((call) => call.path.endsWith("/draft/simulate"));
    expect(simulationCall?.body).toMatchObject({
      policy: { policy_id: "release-routing", allowed_models: ["anthropic:balanced", "openai:gpt-high"] },
      request: {
        purpose: "Execute a bounded analysis task",
        required_capabilities: ["tools"],
        input_tokens: 32000,
        reserved_output_tokens: 4096,
        requested_model: null,
      },
    });
  });
});

describe("RuntimeCommunicationSettings", () => {
  it("loads, validates through numeric controls, and applies the full live policy", async () => {
    const calls: RequestCall[] = [];
    const current = {
      structured_handoff_enabled: true,
      structured_handoff_required_for_new_tasks: false,
      legacy_spawn_agent_enabled: true,
      default_context_token_budget: 8000,
      max_context_refs: 50,
      max_inline_bytes_per_ref: 8192,
      max_inline_bytes_total: 32768,
      max_comment_batch: 100,
      wake_coalesce_window_ms: 1000,
      wake_max_attempts: 5,
      wake_backoff_seconds: 1,
      context_read_audit_enabled: true,
      transcript_sharing_default: false,
    };
    const apiRequest: ApiRequest = async <T,>(path: string, init?: RequestInit) => {
      const method = (init?.method || "GET").toUpperCase();
      const body = parseBody(init);
      calls.push({ path, method, body, headers: init?.headers });
      if (path === "/v1/orchestration/handoff-settings" && method === "GET") return current as T;
      if (path === "/v1/orchestration/handoff-settings" && method === "PUT") return body as T;
      throw new Error(`Unexpected request: ${method} ${path}`);
    };

    render(<RuntimeCommunicationSettings apiRequest={apiRequest} />);
    const refs = await screen.findByLabelText("Maximum ContextRefs") as HTMLInputElement;
    expect(refs.value).toBe("50");
    fireEvent.change(refs, { target: { value: "75" } });
    fireEvent.click(screen.getByLabelText("Require structured handoff for new Agent tasks"));
    fireEvent.click(screen.getByRole("button", { name: "Save settings" }));

    await waitFor(() => expect(calls).toContainEqual(expect.objectContaining({
      path: "/v1/orchestration/handoff-settings",
      method: "PUT",
      body: expect.objectContaining({
        max_context_refs: 75,
        structured_handoff_required_for_new_tasks: true,
        transcript_sharing_default: false,
      }),
    })));
    expect((await screen.findByRole("status")).textContent).toContain("saved and applied");
  });
});
