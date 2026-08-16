import type { ModelSettings } from "./api";

export interface SelectableModelCatalog {
  models: string[];
  labels: Record<string, string>;
  contextWindows: Record<string, number>;
  modelReady: boolean;
  subscriptionOnlyDefault: string | null;
}

/**
 * Subscription Agent runtimes are complete local agent loops, not API model ids.
 * Keep this check in the catalog boundary so every conversation surface applies the
 * same capability rules when one of those runtime ids is selected.
 */
export function isSubscriptionRuntimeId(modelId: string | null | undefined): boolean {
  return /^[a-z0-9][a-z0-9-]*-subscription:/i.test(String(modelId || "").trim());
}

/**
 * Build the conversation model menu without mixing background-only Agent runtimes
 * into it.  The API model catalog remains authoritative; eligible subscription
 * runtimes are an additive, local execution option.
 */
export function selectableModelCatalog(settings: ModelSettings): SelectableModelCatalog {
  const runtimes = (settings.subscription_runtimes || []).filter(
    (runtime) => runtime.interactive_eligible,
  );
  const runtimeIds = runtimes.map((runtime) => runtime.runtime_id);
  const contextWindows = { ...(settings.model_context_windows || {}) };

  for (const runtime of runtimes) {
    if (Number.isFinite(runtime.context_window) && runtime.context_window > 0) {
      contextWindows[runtime.runtime_id] = runtime.context_window;
    }
  }

  return {
    models: Array.from(new Set([...(settings.models || []), ...runtimeIds])),
    labels: {
      ...(settings.model_labels || {}),
      ...Object.fromEntries(runtimes.map((runtime) => [runtime.runtime_id, runtime.label])),
    },
    contextWindows,
    modelReady: settings.model_ready || runtimeIds.length > 0,
    // Only replace the server's API default when no API model can actually run.
    subscriptionOnlyDefault: !settings.model_ready && runtimeIds.length ? runtimeIds[0] : null,
  };
}
