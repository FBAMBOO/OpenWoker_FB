import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { Icon } from "../../components/Icon";
import { createOrchestrationApi, type ApiRequest } from "./api";
import type {
  ModelPolicyDetail,
  ModelPolicySummary,
  ModelRoutingPolicySpec,
  RoutingModelDescriptor,
  RoutingSimulationFacts,
  RoutingSimulationResult,
  SubscriptionRuntimeDescriptor,
  ValidationReport,
} from "./types";
import { ValidationSummary } from "./AgentProfilesSettings";
import {
  BUTTON,
  CARD,
  EmptyState,
  ErrorNotice,
  humanize,
  INPUT,
  LoadingBlock,
  PRIMARY_BUTTON,
  SectionHead,
  StatusBadge,
} from "./ui";

export interface ModelRoutingSettingsProps {
  apiRequest: ApiRequest;
  initialPolicyId?: string;
  onChanged?: () => void;
}

export function createBlankModelPolicy(policyId = "untitled-policy"): ModelRoutingPolicySpec {
  return {
    schema_version: 1,
    policy_id: policyId,
    require_verified: true,
    allow_unknown_cost: true,
    allowed_providers: [],
    allowed_models: [],
    blocked_models: [],
    fallback_limit: 2,
    fallback_for_explicit: false,
  };
}

const blankRequest = (): RoutingSimulationFacts => ({
  purpose: "Execute a bounded analysis task",
  required_capabilities: ["tools"],
  input_tokens: 32000,
  reserved_output_tokens: 4096,
  minimum_context: 0,
  max_cost_microusd: null,
  requested_model: null,
  preferred_models: [],
  allowed_providers: [],
  excluded_models: [],
});

const messageOf = (error: unknown) =>
  error instanceof Error ? error.message : typeof error === "string" ? error : "The routing policy request failed.";

const slugify = (value: string) => value.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "-").replace(/^[-_]+|[-_]+$/g, "").slice(0, 64);

export function ModelRoutingSettings({ apiRequest, initialPolicyId, onChanged }: ModelRoutingSettingsProps) {
  const api = useMemo(() => createOrchestrationApi(apiRequest), [apiRequest]);
  const [policies, setPolicies] = useState<ModelPolicySummary[]>([]);
  const [catalog, setCatalog] = useState<RoutingModelDescriptor[]>([]);
  const [subscriptionRuntimes, setSubscriptionRuntimes] = useState<SubscriptionRuntimeDescriptor[]>([]);
  const [runtimeLoading, setRuntimeLoading] = useState(true);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState(initialPolicyId || "");
  const [detail, setDetail] = useState<ModelPolicyDetail | null>(null);
  const [draft, setDraft] = useState<ModelRoutingPolicySpec | null>(null);
  const [view, setView] = useState<"draft" | `v${number}`>("draft");
  const [dirty, setDirty] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [validation, setValidation] = useState<ValidationReport | null>(null);
  const [cloneId, setCloneId] = useState("");
  const [request, setRequest] = useState<RoutingSimulationFacts>(blankRequest);
  const [simulation, setSimulation] = useState<RoutingSimulationResult | null>(null);

  const loadPolicies = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextPolicies, nextCatalog] = await Promise.all([api.listModelPolicies(), api.getModelCatalog()]);
      setPolicies(nextPolicies);
      setCatalog(nextCatalog);
      setSelectedId((current) => current || initialPolicyId || nextPolicies[0]?.id || "");
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setLoading(false);
    }
  }, [api, initialPolicyId]);

  const loadSubscriptionRuntimes = useCallback(async (refresh = false) => {
    setRuntimeLoading(true);
    setRuntimeError(null);
    try {
      setSubscriptionRuntimes(await api.getSubscriptionRuntimes(refresh));
    } catch (caught) {
      setRuntimeError(messageOf(caught));
    } finally {
      setRuntimeLoading(false);
    }
  }, [api]);

  const adoptDetail = useCallback((next: ModelPolicyDetail) => {
    setDetail(next);
    setDraft(next.draft?.spec || null);
    setView(next.draft ? "draft" : (`v${next.current?.version || next.current_version || 1}` as const));
    setDirty(false);
    setValidation(next.draft?.validation || null);
    setCloneId(slugify(`${next.id}-copy`));
    setSimulation(null);
  }, []);

  const loadDetail = useCallback(async (id: string) => {
    if (!id || id === "__new__") return;
    setBusy("load");
    setError(null);
    try {
      adoptDetail(await api.getModelPolicy(id));
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  }, [adoptDetail, api]);

  useEffect(() => { void loadPolicies(); }, [loadPolicies]);
  useEffect(() => { void loadSubscriptionRuntimes(); }, [loadSubscriptionRuntimes]);
  useEffect(() => { if (selectedId && selectedId !== "__new__") void loadDetail(selectedId); }, [loadDetail, selectedId]);

  const selectPolicy = (id: string) => {
    if (dirty && !window.confirm("Discard unsaved routing changes?")) return;
    setSelectedId(id);
    setValidation(null);
    setSimulation(null);
    if (id === "__new__") {
      setDetail(null);
      setDraft(createBlankModelPolicy());
      setView("draft");
      setDirty(false);
      setCloneId("");
    }
  };

  const refresh = async (nextId?: string) => {
    const nextPolicies = await api.listModelPolicies();
    setPolicies(nextPolicies);
    const id = nextId || selectedId;
    if (id && id !== "__new__") {
      setSelectedId(id);
      adoptDetail(await api.getModelPolicy(id));
    }
    onChanged?.();
  };

  const updateDraft = (next: ModelRoutingPolicySpec) => {
    setDraft(next);
    setDirty(true);
    setValidation(null);
    setSimulation(null);
  };

  const save = async () => {
    if (!draft) return;
    setBusy("save");
    setError(null);
    try {
      if (selectedId === "__new__") {
        const created = await api.createModelPolicy(draft);
        await refresh(created.id || draft.policy_id);
      } else if (detail?.draft) {
        adoptDetail(await api.saveModelPolicyDraft(selectedId, draft, detail.draft.etag));
        await refresh(selectedId);
      }
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const validate = async () => {
    if (!draft || selectedId === "__new__") return;
    setBusy("validate");
    setError(null);
    try {
      setValidation(await api.validateModelPolicy(selectedId, draft));
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const publish = async () => {
    if (!detail?.draft || dirty || validation?.valid !== true) return;
    setBusy("publish");
    setError(null);
    try {
      adoptDetail(await api.publishModelPolicy(detail.id, detail.draft.etag));
      await refresh(detail.id);
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const createDraft = async () => {
    if (!detail?.current_version) return;
    setBusy("draft");
    setError(null);
    try {
      adoptDetail(await api.createModelPolicyDraft(detail.id, detail.current_version));
      await refresh(detail.id);
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const clone = async () => {
    if (!detail || !cloneId.trim()) return;
    setBusy("clone");
    setError(null);
    try {
      const created = await api.cloneModelPolicy(detail.id, cloneId.trim());
      await refresh(created.id || cloneId.trim());
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const simulate = async () => {
    const spec = shownPolicy(detail, draft, view);
    if (!spec || selectedId === "__new__") return;
    setBusy("simulate");
    setError(null);
    try {
      setSimulation(await api.simulateModelPolicy(selectedId, spec, request));
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const shownSpec = shownPolicy(detail, draft, view);
  const editable = view === "draft" && (!!detail?.draft || selectedId === "__new__");

  return (
    <section data-testid="model-routing-settings">
      <div className="mb-5 flex items-start gap-4">
        <div className="min-w-0 flex-1">
          <h2 className="text-[18px] font-semibold tracking-[-0.01em]">Model routing</h2>
          <p className="mt-1 text-[12.5px] leading-relaxed text-muted">
            Quality-first constraints over a verified model catalog. Every simulation and runtime choice remains replayable and explainable.
          </p>
        </div>
        <button className={PRIMARY_BUTTON} onClick={() => selectPolicy("__new__")}>+ New policy</button>
      </div>
      <SubscriptionRuntimePanel
        runtimes={subscriptionRuntimes}
        loading={runtimeLoading}
        error={runtimeError}
        onRefresh={() => void loadSubscriptionRuntimes(true)}
      />
      {error && <div className="mb-3"><ErrorNotice message={error} onRetry={() => void loadPolicies()} /></div>}
      {loading ? <LoadingBlock label="Loading routing policies…" /> : (
        <div className="grid min-h-[620px] grid-cols-[220px_minmax(0,1fr)] overflow-hidden rounded-xl2 border border-line bg-panel">
          <nav className="border-r border-line bg-paper/50 p-2.5" aria-label="Model routing policies">
            {selectedId === "__new__" && <PolicyRow name={draft?.policy_id || "Untitled policy"} badge="Draft" selected onClick={() => selectPolicy("__new__")} />}
            {policies.map((policy) => <PolicyRow key={policy.id} name={policy.name || policy.id} selected={policy.id === selectedId} badge={policy.builtin ? "Built-in" : policy.has_draft ? "Draft" : policy.archived ? "Archived" : `v${policy.current_version || 0}`} onClick={() => selectPolicy(policy.id)} />)}
          </nav>
          <div className="min-w-0 p-5">
            {!selectedId ? <EmptyState title="Choose a routing policy" /> : busy === "load" && !shownSpec ? <LoadingBlock label="Loading policy…" /> : shownSpec ? (
              <>
                <VersionBar detail={detail} view={view} dirty={dirty} onView={(next) => { setView(next); setDirty(false); setValidation(null); setSimulation(null); }} />
                <PolicyEditor spec={shownSpec} editable={editable} isNew={selectedId === "__new__"} catalog={catalog} onChange={updateDraft} />
                {validation && <ValidationSummary report={validation} />}
                <LifecycleActions detail={detail} isNew={selectedId === "__new__"} editable={editable} dirty={dirty} busy={busy} validation={validation} onSave={() => void save()} onValidate={() => void validate()} onPublish={() => void publish()} onCreateDraft={() => void createDraft()} />
                {detail && <div className="mt-5 rounded-xl border border-line bg-paper/60 p-3.5"><SectionHead title={detail.builtin ? "Clone to customize" : "Duplicate policy"} /><div className="flex gap-2"><input className={`${INPUT} font-mono`} aria-label="Clone policy ID" value={cloneId} onChange={(event) => setCloneId(slugify(event.target.value))} placeholder="policy-id" /><button className={BUTTON} disabled={!cloneId.trim() || !!busy} onClick={() => void clone()}><Icon name="copy" size={13} /> {busy === "clone" ? "Cloning…" : "Clone"}</button></div></div>}
                <SimulationPanel request={request} result={simulation} catalog={catalog} busy={busy === "simulate"} disabled={selectedId === "__new__"} onRequest={setRequest} onRun={() => void simulate()} />
              </>
            ) : null}
          </div>
        </div>
      )}
    </section>
  );
}

function SubscriptionRuntimePanel({ runtimes, loading, error, onRefresh }: { runtimes: SubscriptionRuntimeDescriptor[]; loading: boolean; error: string | null; onRefresh: () => void }) {
  return (
    <section className={`${CARD} mb-5 p-4`} data-testid="subscription-runtime-health">
      <SectionHead
        title={`Subscription Agent Runtimes · ${runtimes.length}`}
        aside={<button className={BUTTON} aria-label="Refresh subscription runtimes" disabled={loading} onClick={onRefresh}><Icon name="refresh" size={13} />{loading ? "Refreshing…" : "Refresh"}</button>}
      />
      <p className="-mt-1 mb-3 text-[11.5px] leading-relaxed text-muted">Local Agent CLIs use their signed-in subscription sessions. Health checks inspect installation, authentication, and policy eligibility without consuming model quota.</p>
      {error && <div role="alert" className="mb-3 rounded-lg border border-warnInk/20 bg-warnSoft px-3 py-2 text-[11px] text-warnInk">Runtime health is unavailable: {error}</div>}
      {loading && !runtimes.length ? <div className="rounded-lg border border-line bg-paper px-3 py-5 text-center text-[11.5px] text-muted">Checking local Agent CLIs…</div> : runtimes.length ? (
        <div className="grid gap-3 lg:grid-cols-2">
          {runtimes.map((runtime) => <SubscriptionRuntimeCard key={runtime.runtime_id} runtime={runtime} />)}
        </div>
      ) : !error ? <div className="rounded-lg border border-line bg-paper px-3 py-5 text-center text-[11.5px] text-muted">No subscription Agent runtimes are registered.</div> : null}
    </section>
  );
}

function SubscriptionRuntimeCard({ runtime }: { runtime: SubscriptionRuntimeDescriptor }) {
  const status = runtime.availability === "available" ? "succeeded" : runtime.availability === "blocked_by_policy" ? "blocked" : "failed";
  const auth = runtime.health.authenticated
    ? runtime.health.auth_kind && runtime.health.auth_kind !== "unknown" ? humanize(runtime.health.auth_kind) : "Authenticated"
    : "Not authenticated";
  const cli = runtime.health.version
    ? `${runtime.command || runtime.health.executable || "CLI"} ${runtime.health.version}`
    : runtime.health.installed ? runtime.command || runtime.health.executable || "Installed" : "Not detected";
  return (
    <article className="rounded-xl border border-line bg-paper p-3.5" aria-label={`${runtime.display_name} runtime`}>
      <div className="flex items-start gap-3">
        <div className="min-w-0 flex-1">
          <div className="truncate text-[12.5px] font-semibold text-ink">{runtime.display_name}</div>
          <div className="mt-0.5 truncate font-mono text-[9.5px] text-faint">{runtime.runtime_id}</div>
        </div>
        <StatusBadge status={status} label={humanize(runtime.availability)} />
      </div>
      <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-2 text-[10.5px]">
        <RuntimeDatum label="Vendor model" value={runtime.model || "Unknown"} mono />
        <RuntimeDatum label="Reasoning effort" value={humanize(runtime.reasoning_effort || "Unknown")} />
        <RuntimeDatum label="CLI" value={cli} />
        <RuntimeDatum label="Authentication" value={auth} tone={runtime.health.authenticated ? "ok" : "warn"} />
      </dl>
      <div className="mt-2 flex flex-wrap gap-1.5">
        {runtime.interactive_only && <span className="rounded-full border border-warnInk/20 bg-warnSoft px-2 py-0.5 text-[9.5px] text-warnInk">Interactive only</span>}
        {runtime.local_owner_only && <span className="rounded-full border border-line bg-panel px-2 py-0.5 text-[9.5px] text-muted">Local owner only</span>}
        {runtime.protocol && <span className="rounded-full border border-line bg-panel px-2 py-0.5 font-mono text-[9px] text-faint">{runtime.protocol}</span>}
      </div>
      {runtime.availability_reason && <p className={`mt-2 border-t border-line pt-2 text-[10.5px] leading-relaxed ${runtime.availability === "unavailable" ? "text-danger" : "text-warnInk"}`}>{runtime.availability_reason}</p>}
    </article>
  );
}

function RuntimeDatum({ label, value, mono, tone }: { label: string; value: string; mono?: boolean; tone?: "ok" | "warn" }) {
  return <div><dt className="text-faint">{label}</dt><dd className={`mt-0.5 truncate ${mono ? "font-mono" : ""} ${tone === "ok" ? "text-ok" : tone === "warn" ? "text-warnInk" : "text-ink"}`} title={value}>{value}</dd></div>;
}

const shownPolicy = (detail: ModelPolicyDetail | null, draft: ModelRoutingPolicySpec | null, view: "draft" | `v${number}`) => view === "draft" ? draft : detail?.versions.find((version) => `v${version.version}` === view)?.spec || detail?.current?.spec || null;

function PolicyRow({ name, badge, selected, onClick }: { name: string; badge: string; selected: boolean; onClick: () => void }) {
  return <button className={`mb-1 w-full rounded-lg px-2.5 py-2 text-left ${selected ? "bg-accentSoft text-ink" : "text-muted hover:bg-panel hover:text-ink"}`} onClick={onClick}><span className="block truncate text-[12.5px] font-medium">{name}</span><span className="mt-0.5 block text-[10px] text-faint">{badge}</span></button>;
}

function VersionBar({ detail, view, dirty, onView }: { detail: ModelPolicyDetail | null; view: "draft" | `v${number}`; dirty: boolean; onView: (view: "draft" | `v${number}`) => void }) {
  if (!detail) return null;
  return <div className="mb-4 flex flex-wrap items-center gap-2 border-b border-line pb-3">{detail.builtin && <StatusBadge status="completed" label="Built-in · read only" />}{detail.derived_from && <span className="text-[10.5px] text-faint">Cloned from {detail.derived_from.policy_id} v{detail.derived_from.version}</span>}<select className={`${INPUT} ml-auto w-auto py-1.5`} aria-label="Policy version" value={view} onChange={(event) => { const next = event.target.value as "draft" | `v${number}`; if (dirty && !window.confirm("Discard unsaved routing changes?")) return; onView(next); }}>{detail.draft && <option value="draft">Draft{detail.draft.base_version ? ` · based on v${detail.draft.base_version}` : ""}</option>}{[...detail.versions].sort((a, b) => b.version - a.version).map((version) => <option key={version.version} value={`v${version.version}`}>Version {version.version} · published</option>)}</select></div>;
}

function PolicyEditor({ spec, editable, isNew, catalog, onChange }: { spec: ModelRoutingPolicySpec; editable: boolean; isNew: boolean; catalog: RoutingModelDescriptor[]; onChange: (spec: ModelRoutingPolicySpec) => void }) {
  const patch = (value: Partial<ModelRoutingPolicySpec>) => onChange({ ...spec, ...value });
  const selected = spec.allowed_models.map((id) => catalog.find((model) => model.id === id) || missingModel(id));
  const availableToAdd = catalog.filter((model) => !spec.allowed_models.includes(model.id) && !spec.blocked_models.includes(model.id));
  const addModel = (id: string) => { if (id) patch({ allowed_models: [...spec.allowed_models, id] }); };
  const moveModel = (index: number, delta: number) => { const target = index + delta; if (target < 0 || target >= spec.allowed_models.length) return; const allowed_models = [...spec.allowed_models]; [allowed_models[index], allowed_models[target]] = [allowed_models[target], allowed_models[index]]; patch({ allowed_models }); };
  return (
    <div className="space-y-4">
      <section className={`${CARD} grid grid-cols-1 gap-3 p-4 md:grid-cols-2`}>
        <Field label="Policy ID"><input className={`${INPUT} font-mono`} disabled={!editable || !isNew} value={spec.policy_id} onChange={(event) => patch({ policy_id: slugify(event.target.value) })} /></Field>
        <NumberField label="Fallback limit" value={spec.fallback_limit} min={0} max={8} disabled={!editable} onChange={(fallback_limit) => patch({ fallback_limit })} />
        <CheckField label="Require verified models" detail="Reject catalog entries that have not passed verification." checked={spec.require_verified} disabled={!editable} onChange={(require_verified) => patch({ require_verified })} />
        <CheckField label="Allow unknown cost" detail="Unknown prices remain eligible unless a request has a hard cost ceiling." checked={spec.allow_unknown_cost} disabled={!editable} onChange={(allow_unknown_cost) => patch({ allow_unknown_cost })} />
        <CheckField label="Fallback after explicit model" detail="An operator's requested model may fall back when it fails hard constraints." checked={spec.fallback_for_explicit} disabled={!editable} onChange={(fallback_for_explicit) => patch({ fallback_for_explicit })} />
        <Field label="Allowed providers · empty = all"><input className={INPUT} disabled={!editable} placeholder="openai, anthropic" value={spec.allowed_providers.join(", ")} onChange={(event) => patch({ allowed_providers: csv(event.target.value) })} /></Field>
      </section>

      <section className={`${CARD} p-4`}>
        <SectionHead title={`Allowed model pool · ${selected.length || "all"}`} />
        <p className="-mt-1 mb-3 text-[11.5px] leading-relaxed text-muted">Quality is the primary rank. Pool order is saved for consistent review; request preferences, cost, latency, and canonical ID break equal-quality ties.</p>
        {!selected.length ? <div className="mb-3 rounded-lg border border-line bg-paper px-3 py-2 text-[11.5px] text-muted">No allow-list: every catalog model may compete, subject to the constraints above.</div> : (
          <div className="mb-3 space-y-2">{selected.map((model, index) => <ModelPoolRow key={model.id} model={model} index={index} editable={editable} onMove={(delta) => moveModel(index, delta)} onRemove={() => patch({ allowed_models: spec.allowed_models.filter((_, i) => i !== index) })} />)}</div>
        )}
        {editable && <select className={`${INPUT} w-auto min-w-64 py-1.5`} aria-label="Add model to pool" value="" onChange={(event) => addModel(event.target.value)}><option value="">+ Add model to pool…</option>{availableToAdd.map((model) => <option key={model.id} value={model.id}>{model.label} · quality {model.quality}</option>)}</select>}
      </section>

      <section className={`${CARD} p-4`}>
        <SectionHead title="Blocked models" />
        <p className="-mt-1 mb-3 text-[11.5px] text-muted">Blocked models are never eligible, even if a request explicitly asks for one.</p>
        <div className="flex flex-wrap gap-2">{spec.blocked_models.map((id) => <span key={id} className="inline-flex items-center gap-1.5 rounded-full border border-danger/20 bg-dangerSoft px-2.5 py-1 text-[10.5px] text-danger"><span className="font-mono">{id}</span>{editable && <button aria-label={`Unblock ${id}`} onClick={() => patch({ blocked_models: spec.blocked_models.filter((item) => item !== id) })}>×</button>}</span>)}</div>
        {editable && <select className={`${INPUT} mt-3 w-auto min-w-64 py-1.5`} aria-label="Block model" value="" onChange={(event) => { if (event.target.value) patch({ blocked_models: [...spec.blocked_models, event.target.value], allowed_models: spec.allowed_models.filter((id) => id !== event.target.value) }); }}><option value="">+ Block a model…</option>{catalog.filter((model) => !spec.blocked_models.includes(model.id)).map((model) => <option key={model.id} value={model.id}>{model.label}</option>)}</select>}
      </section>

      <CatalogTable catalog={catalog} />
    </div>
  );
}

function ModelPoolRow({ model, index, editable, onMove, onRemove }: { model: RoutingModelDescriptor; index: number; editable: boolean; onMove: (delta: number) => void; onRemove: () => void }) {
  return <div className="flex items-center gap-2 rounded-lg border border-line bg-paper px-2.5 py-2"><span className="grid h-5 w-5 place-items-center rounded-full bg-panel text-[9.5px] text-muted">{index + 1}</span><span className="min-w-0 flex-1"><span className="block truncate text-[11.5px] font-medium text-ink">{model.label || model.id}</span><span className="block truncate font-mono text-[9.5px] text-faint">{model.id}</span></span><QualityBadge quality={model.quality} /><ModelAvailability model={model} />{editable && <><button className="text-faint hover:text-ink" aria-label={`Move ${model.id} up`} disabled={index === 0} onClick={() => onMove(-1)}>↑</button><button className="text-faint hover:text-ink" aria-label={`Move ${model.id} down`} onClick={() => onMove(1)}>↓</button><button className="px-1 text-faint hover:text-danger" aria-label={`Remove ${model.id}`} onClick={onRemove}>×</button></>}</div>;
}

function CatalogTable({ catalog }: { catalog: RoutingModelDescriptor[] }) {
  const sorted = [...catalog].sort((a, b) => b.quality - a.quality || a.latency_rank - b.latency_rank || a.id.localeCompare(b.id));
  return <section className={`${CARD} overflow-hidden`}><div className="px-4 pt-4"><SectionHead title={`Model catalog · ${catalog.length}`} /></div><div className="overflow-x-auto"><table className="w-full min-w-[720px] text-left text-[10.5px]"><thead className="border-y border-line bg-paper text-faint"><tr><th className="px-3 py-2 font-medium">Model</th><th className="px-3 py-2 font-medium">Quality</th><th className="px-3 py-2 font-medium">Capabilities</th><th className="px-3 py-2 font-medium">Context</th><th className="px-3 py-2 font-medium">Cost / 1M input</th><th className="px-3 py-2 font-medium">State</th></tr></thead><tbody className="divide-y divide-line">{sorted.map((model) => <tr key={model.id}><td className="px-3 py-2"><span className="flex items-center gap-1.5"><span className="text-[11.5px] font-medium text-ink">{model.label || model.id}</span>{model.source === "subscription-runtime" && <span className="rounded-full border border-accent/20 bg-accentSoft px-1.5 py-0.5 text-[8.5px] text-accent">Subscription runtime</span>}</span><span className="font-mono text-[9.5px] text-faint">{model.id}</span>{model.runtime && <span className="mt-0.5 block text-[9px] text-faint">{model.runtime.model} · {humanize(model.runtime.reasoning_effort)}</span>}</td><td className="px-3 py-2"><QualityBadge quality={model.quality} /></td><td className="px-3 py-2 text-muted">{model.capabilities.join(", ") || "—"}</td><td className="px-3 py-2 text-muted">{model.context_window ? model.context_window.toLocaleString() : "Unknown"}</td><td className="px-3 py-2 text-muted">{model.input_microusd_per_million == null ? "Unknown" : `$${(model.input_microusd_per_million / 1_000_000).toFixed(2)}`}</td><td className="px-3 py-2"><ModelAvailability model={model} showReason /></td></tr>)}</tbody></table></div></section>;
}

function ModelAvailability({ model, showReason = false }: { model: RoutingModelDescriptor; showReason?: boolean }) {
  const label = model.verified ? humanize(model.availability) : "Unverified";
  const tone = model.availability === "configured" && model.verified ? "text-ok" : ["unavailable", "offline"].includes(model.availability) ? "text-danger" : "text-warnInk";
  return <span className="block"><span className={`text-[9.5px] ${tone}`}>{label}</span>{showReason && model.availability_reason && <span className="mt-0.5 block max-w-56 text-[9px] leading-snug text-faint" title={model.availability_reason}>{model.availability_reason}</span>}</span>;
}

function QualityBadge({ quality }: { quality: number }) {
  const tier = quality >= 85 ? "High" : quality >= 65 ? "Balanced" : "Economy";
  return <span className="inline-flex rounded-full border border-line bg-panel px-2 py-0.5 text-[9.5px] text-muted">{tier} · {quality}</span>;
}

function SimulationPanel({ request, result, catalog, busy, disabled, onRequest, onRun }: { request: RoutingSimulationFacts; result: RoutingSimulationResult | null; catalog: RoutingModelDescriptor[]; busy: boolean; disabled: boolean; onRequest: (request: RoutingSimulationFacts) => void; onRun: () => void }) {
  return <section className={`${CARD} mt-5 p-4`} data-testid="routing-simulation"><SectionHead title="Simulate RoutingRequest" aside={<button className={PRIMARY_BUTTON} disabled={busy || disabled} onClick={onRun}>{busy ? "Simulating…" : "Run simulation"}</button>} /><p className="-mt-1 mb-3 text-[11.5px] text-muted">Runs the deterministic router without calling a model. The result includes every rejected candidate and reason.</p><div className="grid grid-cols-2 gap-2.5 lg:grid-cols-4"><Field label="Purpose" wide><input className={`${INPUT} py-1.5`} value={request.purpose} onChange={(event) => onRequest({ ...request, purpose: event.target.value })} /></Field><Field label="Required capabilities"><input className={`${INPUT} py-1.5`} value={request.required_capabilities.join(", ")} onChange={(event) => onRequest({ ...request, required_capabilities: csv(event.target.value) })} /></Field><NumberField label="Input tokens" value={request.input_tokens} min={0} max={10000000} disabled={false} onChange={(input_tokens) => onRequest({ ...request, input_tokens })} /><NumberField label="Reserved output" value={request.reserved_output_tokens} min={0} max={1000000} disabled={false} onChange={(reserved_output_tokens) => onRequest({ ...request, reserved_output_tokens })} /><NumberField label="Minimum context" value={request.minimum_context} min={0} max={10000000} disabled={false} onChange={(minimum_context) => onRequest({ ...request, minimum_context })} /><NumberField label="Max cost · μUSD · 0 = unset" value={request.max_cost_microusd || 0} min={0} max={1000000000} disabled={false} onChange={(value) => onRequest({ ...request, max_cost_microusd: value || null })} /><Field label="Requested model"><select className={`${INPUT} py-1.5`} value={request.requested_model || ""} onChange={(event) => onRequest({ ...request, requested_model: event.target.value || null })}><option value="">Automatic</option>{catalog.map((model) => <option key={model.id} value={model.id}>{model.label}</option>)}</select></Field><Field label="Preferred models"><input className={`${INPUT} py-1.5`} value={request.preferred_models.join(", ")} onChange={(event) => onRequest({ ...request, preferred_models: csv(event.target.value) })} /></Field><Field label="Allowed providers"><input className={`${INPUT} py-1.5`} value={request.allowed_providers.join(", ")} onChange={(event) => onRequest({ ...request, allowed_providers: csv(event.target.value) })} /></Field><Field label="Excluded models"><input className={`${INPUT} py-1.5`} value={request.excluded_models.join(", ")} onChange={(event) => onRequest({ ...request, excluded_models: csv(event.target.value) })} /></Field></div>{result && <RoutingDecisionView result={result} />}</section>;
}

function RoutingDecisionView({ result }: { result: RoutingSimulationResult }) {
  return <div className="mt-4 rounded-xl border border-accent/20 bg-accentSoft/50 p-3.5" aria-live="polite"><div className="flex items-center gap-2"><span className="text-[12.5px] font-medium text-ink">{result.selected_model ? `Selected ${result.selected_model}` : "No eligible model"}</span><span className="ml-auto font-mono text-[9.5px] text-faint">{result.decision_id}</span></div><div className="mt-1 text-[11px] text-muted">{result.reason}</div>{result.fallback_models.length > 0 && <div className="mt-1 text-[10.5px] text-muted">Fallbacks: {result.fallback_models.join(" → ")}</div>}<div className="mt-2 space-y-1">{result.evaluations.map((candidate) => <div key={candidate.model_id} className="grid grid-cols-[14px_minmax(140px,1fr)_auto_minmax(180px,1.5fr)] gap-2 text-[10.5px]"><span className={candidate.eligible ? "text-ok" : "text-faint"}>{candidate.eligible ? "✓" : "×"}</span><span className="truncate font-mono text-ink">{candidate.model_id}</span><span className="text-muted">quality {candidate.quality}{candidate.rank ? ` · #${candidate.rank}` : ""}</span><span className="text-muted">{candidate.reasons.map(humanize).join(", ") || "Eligible"}</span></div>)}</div></div>;
}

function LifecycleActions({ detail, isNew, editable, dirty, busy, validation, onSave, onValidate, onPublish, onCreateDraft }: { detail: ModelPolicyDetail | null; isNew: boolean; editable: boolean; dirty: boolean; busy: string; validation: ValidationReport | null; onSave: () => void; onValidate: () => void; onPublish: () => void; onCreateDraft: () => void }) {
  return <div className="mt-5 flex flex-wrap items-center gap-2 border-t border-line pt-4">{editable && <><button className={PRIMARY_BUTTON} disabled={!dirty || !!busy} onClick={onSave}>{busy === "save" ? "Saving…" : isNew ? "Create draft" : "Save draft"}</button>{!isNew && <button className={BUTTON} disabled={dirty || !!busy} onClick={onValidate}>{busy === "validate" ? "Validating…" : "Validate"}</button>}{detail?.draft && <button className={BUTTON} disabled={dirty || validation?.valid !== true || !!busy} onClick={onPublish}>{busy === "publish" ? "Publishing…" : "Publish version"}</button>}{dirty && <span className="text-[10.5px] text-warnInk">Unsaved changes</span>}</>}{!editable && detail && !detail.builtin && !detail.archived && !detail.draft && <button className={PRIMARY_BUTTON} disabled={!!busy} onClick={onCreateDraft}>Edit as new draft</button>}</div>;
}

function Field({ label, wide, children }: { label: string; wide?: boolean; children: ReactNode }) { return <label className={wide ? "md:col-span-2" : ""}><span className="mb-1 block text-[11px] font-medium text-muted">{label}</span>{children}</label>; }
function NumberField({ label, value, disabled, min, max, onChange }: { label: string; value: number; disabled: boolean; min: number; max: number; onChange: (value: number) => void }) { return <Field label={label}><input type="number" className={INPUT} disabled={disabled} min={min} max={max} value={value} onChange={(event) => onChange(Number(event.target.value))} /></Field>; }
function CheckField({ label, detail, checked, disabled, onChange }: { label: string; detail: string; checked: boolean; disabled: boolean; onChange: (checked: boolean) => void }) { return <label className="flex items-start gap-2 rounded-lg border border-line bg-paper px-3 py-2"><input className="mt-0.5" type="checkbox" checked={checked} disabled={disabled} onChange={(event) => onChange(event.target.checked)} /><span><span className="block text-[11.5px] font-medium text-ink">{label}</span><span className="mt-0.5 block text-[10px] leading-relaxed text-muted">{detail}</span></span></label>; }
const csv = (value: string) => value.split(",").map((item) => item.trim()).filter(Boolean);
const missingModel = (id: string): RoutingModelDescriptor => ({ id, label: id, provider: id.includes(":") ? id.split(":", 1)[0] : "openai", quality: 0, configured: false, availability: "unknown", verified: false, capabilities: [], context_window: null, latency_rank: 0 });
