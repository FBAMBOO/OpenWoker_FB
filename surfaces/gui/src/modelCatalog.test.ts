import { describe, expect, it } from "vitest";
import type { InteractiveSubscriptionRuntime, ModelSettings } from "./api";
import { isSubscriptionRuntimeId, selectableModelCatalog } from "./modelCatalog";

const runtime = (
  runtime_id: string,
  interactive_eligible: boolean,
  context_window = 200_000,
): InteractiveSubscriptionRuntime => ({
  runtime_id,
  provider: "codex-subscription",
  label: `Subscription Agent · ${runtime_id}`,
  model: "model",
  reasoning_effort: "max",
  context_window,
  interactive_only: true,
  health: {
    runtime_id,
    provider: "codex-subscription",
    installed: true,
    authenticated: true,
    available: true,
    policy_eligible: true,
    version: "1",
    auth_kind: "subscription",
    executable: "agent",
    reason: "",
    checked_at: 1,
  },
  interactive_eligible,
  interactive_reason: interactive_eligible ? "ready" : "not authenticated",
  background_eligible: false,
  background_reason: "interactive only",
});

const settings = (overrides: Partial<ModelSettings> = {}): ModelSettings => ({
  provider: "openai",
  model: "api-model",
  models: ["api-model"],
  has_key: true,
  model_ready: true,
  source: "store",
  onboarded: true,
  surfaces: { cowork: true, chat: true, code: true },
  scratch_base: "C:/scratch",
  secrets_path: "C:/secrets.json",
  model_labels: { "api-model": "API model" },
  model_context_windows: { "api-model": 128_000 },
  context_bar: false,
  ...overrides,
});

describe("selectableModelCatalog", () => {
  it("identifies Subscription Agent runtime ids without classifying API models", () => {
    expect(isSubscriptionRuntimeId("codex-subscription:gpt-5.6-sol@max")).toBe(true);
    expect(isSubscriptionRuntimeId("claude-code-subscription:claude-opus-5@high")).toBe(true);
    expect(isSubscriptionRuntimeId("kimi-code-subscription:kimi-code/k3@max")).toBe(true);
    expect(isSubscriptionRuntimeId("openai:gpt-5.6-sol")).toBe(false);
    expect(isSubscriptionRuntimeId("anthropic:claude-opus-5")).toBe(false);
  });

  it("adds only interactive-eligible subscription runtimes and merges their metadata", () => {
    const eligible = runtime("codex-subscription:gpt-5.6-sol@max", true, 300_000);
    const backgroundOnly = runtime("blocked-runtime", false);
    const result = selectableModelCatalog(settings({
      subscription_runtimes: [eligible, backgroundOnly],
    }));

    expect(result.models).toEqual(["api-model", eligible.runtime_id]);
    expect(result.labels[eligible.runtime_id]).toBe(eligible.label);
    expect(result.contextWindows[eligible.runtime_id]).toBe(300_000);
    expect(result.labels[backgroundOnly.runtime_id]).toBeUndefined();
    expect(result.modelReady).toBe(true);
    expect(result.subscriptionOnlyDefault).toBeNull();
  });

  it("makes an eligible runtime the fresh-session default on subscription-only installs", () => {
    const eligible = runtime("claude-code-subscription:claude-opus-5@high", true);
    const result = selectableModelCatalog(settings({
      models: [],
      model_ready: false,
      subscription_runtimes: [eligible],
    }));

    expect(result.modelReady).toBe(true);
    expect(result.subscriptionOnlyDefault).toBe(eligible.runtime_id);
  });

  it("does not claim readiness when neither an API model nor an interactive runtime is usable", () => {
    const result = selectableModelCatalog(settings({
      models: [],
      model_ready: false,
      subscription_runtimes: [runtime("blocked-runtime", false)],
    }));

    expect(result.models).toEqual([]);
    expect(result.modelReady).toBe(false);
    expect(result.subscriptionOnlyDefault).toBeNull();
  });
});
