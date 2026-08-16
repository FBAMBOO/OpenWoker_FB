import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { Icon } from "../../components/Icon";
import { createOrchestrationApi, type ApiRequest } from "./api";
import type {
  AgentProfileDetail,
  AgentProfileSpec,
  AgentProfileSummary,
  AgentRole,
  ModelPolicySummary,
  ValidationReport,
} from "./types";
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

export interface AgentProfilesSettingsProps {
  apiRequest: ApiRequest;
  initialProfileId?: string;
  onChanged?: () => void;
}

export const AGENT_ROLES: AgentRole[] = [
  "orchestrator",
  "planner",
  "worker",
  "reviewer",
  "tester",
  "evaluator",
  "scorer",
  "explorer",
  "integrator",
];

export function createBlankAgentProfile(
  profileId = "untitled-profile",
  policy?: ModelPolicySummary,
): AgentProfileSpec {
  return {
    schema_version: 1,
    profile_id: profileId,
    display_name: "Untitled profile",
    role: "worker",
    instructions: "Complete the scoped assignment and return structured evidence.",
    allowed_tools: [],
    allowed_child_roles: [],
    permission_mode: "interactive",
    model_policy: policy?.id || "quality-first",
    max_iterations: 12,
    max_children: 0,
    base: null,
    metadata: {
      token_budget: null,
      tool_call_budget: null,
      timeout_seconds: 1800,
      evidence_required: true,
      tests_required: false,
      review_required: true,
    },
  };
}

const messageOf = (error: unknown) =>
  error instanceof Error ? error.message : typeof error === "string" ? error : "The profile request failed.";

const slugify = (value: string) => value
  .trim()
  .toLowerCase()
  .replace(/[^a-z0-9_-]+/g, "-")
  .replace(/^[-_]+|[-_]+$/g, "")
  .slice(0, 64);

export function AgentProfilesSettings({ apiRequest, initialProfileId, onChanged }: AgentProfilesSettingsProps) {
  const api = useMemo(() => createOrchestrationApi(apiRequest), [apiRequest]);
  const [profiles, setProfiles] = useState<AgentProfileSummary[]>([]);
  const [policies, setPolicies] = useState<ModelPolicySummary[]>([]);
  const [selectedId, setSelectedId] = useState(initialProfileId || "");
  const [detail, setDetail] = useState<AgentProfileDetail | null>(null);
  const [draft, setDraft] = useState<AgentProfileSpec | null>(null);
  const [view, setView] = useState<"draft" | `v${number}`>("draft");
  const [dirty, setDirty] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [validation, setValidation] = useState<ValidationReport | null>(null);
  const [cloneId, setCloneId] = useState("");
  const [cloneName, setCloneName] = useState("");

  const loadProfiles = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextProfiles, nextPolicies] = await Promise.all([api.listAgentProfiles(), api.listModelPolicies()]);
      setProfiles(nextProfiles);
      setPolicies(nextPolicies);
      setSelectedId((current) => current || initialProfileId || nextProfiles[0]?.id || "");
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setLoading(false);
    }
  }, [api, initialProfileId]);

  const adoptDetail = useCallback((next: AgentProfileDetail) => {
    setDetail(next);
    setDraft(next.draft?.spec || null);
    setView(next.draft ? "draft" : (`v${next.current?.version || next.current_version || 1}` as const));
    setDirty(false);
    setValidation(next.draft?.validation || null);
    setCloneName(`${next.name} copy`);
    setCloneId(slugify(`${next.id}-copy`));
  }, []);

  const loadDetail = useCallback(async (id: string) => {
    if (!id || id === "__new__") return;
    setBusy("load");
    setError(null);
    try {
      adoptDetail(await api.getAgentProfile(id));
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  }, [adoptDetail, api]);

  useEffect(() => { void loadProfiles(); }, [loadProfiles]);
  useEffect(() => { if (selectedId && selectedId !== "__new__") void loadDetail(selectedId); }, [loadDetail, selectedId]);

  const selectProfile = (id: string) => {
    if (dirty && !window.confirm("Discard unsaved profile changes?")) return;
    setSelectedId(id);
    setValidation(null);
    if (id === "__new__") {
      setDetail(null);
      setDraft(createBlankAgentProfile("untitled-profile", policies[0]));
      setView("draft");
      setDirty(false);
      setCloneId("");
      setCloneName("");
    }
  };

  const refresh = async (nextId?: string) => {
    const nextProfiles = await api.listAgentProfiles();
    setProfiles(nextProfiles);
    const id = nextId || selectedId;
    if (id && id !== "__new__") {
      setSelectedId(id);
      adoptDetail(await api.getAgentProfile(id));
    }
    onChanged?.();
  };

  const updateDraft = (next: AgentProfileSpec) => {
    setDraft(next);
    setDirty(true);
    setValidation(null);
  };

  const save = async () => {
    if (!draft) return;
    setBusy("save");
    setError(null);
    try {
      if (selectedId === "__new__") {
        const created = await api.createAgentProfile(draft);
        await refresh(created.id || draft.profile_id);
      } else if (detail?.draft) {
        adoptDetail(await api.saveAgentProfileDraft(selectedId, draft, detail.draft.etag));
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
      setValidation(await api.validateAgentProfile(selectedId, draft));
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
      adoptDetail(await api.publishAgentProfile(detail.id, detail.draft.etag));
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
      adoptDetail(await api.createAgentProfileDraft(detail.id, detail.current_version));
      await refresh(detail.id);
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const clone = async () => {
    if (!detail || !cloneId.trim() || !cloneName.trim()) return;
    setBusy("clone");
    setError(null);
    try {
      const created = await api.cloneAgentProfile(detail.id, cloneId.trim(), cloneName.trim());
      await refresh(created.id || cloneId.trim());
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const shownSpec = view === "draft"
    ? draft
    : detail?.versions.find((version) => `v${version.version}` === view)?.spec || detail?.current?.spec || null;
  const editable = view === "draft" && (!!detail?.draft || selectedId === "__new__");

  return (
    <section data-testid="agent-profiles-settings">
      <div className="mb-5 flex items-start gap-4">
        <div className="min-w-0 flex-1">
          <h2 className="text-[18px] font-semibold tracking-[-0.01em]">Agent profiles</h2>
          <p className="mt-1 text-[12.5px] leading-relaxed text-muted">
            Versioned roles, instructions, tool ceilings, delegation limits, and routing policy. Published versions are immutable.
          </p>
        </div>
        <button className={PRIMARY_BUTTON} onClick={() => selectProfile("__new__")}>+ New profile</button>
      </div>

      {error && <div className="mb-3"><ErrorNotice message={error} onRetry={() => void loadProfiles()} /></div>}
      {loading ? <LoadingBlock label="Loading profiles…" /> : (
        <div className="grid min-h-[560px] grid-cols-[220px_minmax(0,1fr)] overflow-hidden rounded-xl2 border border-line bg-panel">
          <nav className="border-r border-line bg-paper/50 p-2.5" aria-label="Agent profiles">
            {selectedId === "__new__" && <ResourceRow name={draft?.display_name || "Untitled profile"} selected badge="Draft" onClick={() => selectProfile("__new__")} />}
            {profiles.map((profile) => (
              <ResourceRow
                key={profile.id}
                name={profile.name}
                selected={profile.id === selectedId}
                badge={profile.builtin ? "Built-in" : profile.has_draft ? "Draft" : profile.archived ? "Archived" : `v${profile.current_version || 0}`}
                onClick={() => selectProfile(profile.id)}
              />
            ))}
          </nav>

          <div className="min-w-0 p-5">
            {!selectedId ? <EmptyState title="Choose an agent profile" /> : busy === "load" && !shownSpec ? <LoadingBlock label="Loading profile…" /> : shownSpec ? (
              <>
                <VersionBar
                  detail={detail}
                  view={view}
                  dirty={dirty}
                  onView={(next) => { setView(next); setDirty(false); setValidation(null); }}
                />
                <ProfileEditor spec={shownSpec} editable={editable} isNew={selectedId === "__new__"} policies={policies} onChange={updateDraft} />
                {validation && <ValidationSummary report={validation} />}
                <LifecycleActions
                  detail={detail}
                  isNew={selectedId === "__new__"}
                  editable={editable}
                  dirty={dirty}
                  busy={busy}
                  validation={validation}
                  onSave={() => void save()}
                  onValidate={() => void validate()}
                  onPublish={() => void publish()}
                  onCreateDraft={() => void createDraft()}
                />
                {detail && (
                  <div className="mt-5 rounded-xl border border-line bg-paper/60 p-3.5">
                    <SectionHead title={detail.builtin ? "Clone to customize" : "Duplicate profile"} />
                    <div className="grid gap-2 md:grid-cols-[minmax(130px,.7fr)_minmax(180px,1fr)_auto]">
                      <input className={INPUT} aria-label="Clone profile ID" value={cloneId} onChange={(event) => setCloneId(slugify(event.target.value))} placeholder="profile-id" />
                      <input className={INPUT} aria-label="Clone profile name" value={cloneName} onChange={(event) => setCloneName(event.target.value)} />
                      <button className={BUTTON} disabled={!cloneId.trim() || !cloneName.trim() || !!busy} onClick={() => void clone()}>
                        <Icon name="copy" size={13} /> {busy === "clone" ? "Cloning…" : "Clone"}
                      </button>
                    </div>
                    {detail.builtin && <p className="mt-2 text-[11px] text-muted">Built-in profiles cannot be overwritten. The clone records this exact source version as its base.</p>}
                  </div>
                )}
              </>
            ) : null}
          </div>
        </div>
      )}
    </section>
  );
}

function ResourceRow({ name, badge, selected, onClick }: { name: string; badge: string; selected: boolean; onClick: () => void }) {
  return <button className={`mb-1 w-full rounded-lg px-2.5 py-2 text-left ${selected ? "bg-accentSoft text-ink" : "text-muted hover:bg-panel hover:text-ink"}`} onClick={onClick}><span className="block truncate text-[12.5px] font-medium">{name}</span><span className="mt-0.5 block text-[10px] text-faint">{badge}</span></button>;
}

function VersionBar({ detail, view, dirty, onView }: { detail: AgentProfileDetail | null; view: "draft" | `v${number}`; dirty: boolean; onView: (view: "draft" | `v${number}`) => void }) {
  if (!detail) return null;
  return (
    <div className="mb-4 flex flex-wrap items-center gap-2 border-b border-line pb-3">
      {detail.builtin && <StatusBadge status="completed" label="Built-in · read only" />}
      {detail.derived_from && <span className="text-[10.5px] text-faint">Cloned from {detail.derived_from.profile_id} v{detail.derived_from.version}</span>}
      <select className={`${INPUT} ml-auto w-auto py-1.5`} aria-label="Profile version" value={view} onChange={(event) => {
        const next = event.target.value as "draft" | `v${number}`;
        if (dirty && !window.confirm("Discard unsaved profile changes?")) return;
        onView(next);
      }}>
        {detail.draft && <option value="draft">Draft{detail.draft.base_version ? ` · based on v${detail.draft.base_version}` : ""}</option>}
        {[...detail.versions].sort((a, b) => b.version - a.version).map((version) => <option key={version.version} value={`v${version.version}`}>Version {version.version} · published</option>)}
      </select>
    </div>
  );
}

function ProfileEditor({ spec, editable, isNew, policies, onChange }: { spec: AgentProfileSpec; editable: boolean; isNew: boolean; policies: ModelPolicySummary[]; onChange: (spec: AgentProfileSpec) => void }) {
  const patch = (value: Partial<AgentProfileSpec>) => onChange({ ...spec, ...value });
  const metadata = spec.metadata || {};
  const patchMetadata = (value: Partial<AgentProfileSpec["metadata"]>) => patch({ metadata: { ...metadata, ...value } });
  const roleAllowed = (role: AgentRole) => spec.allowed_child_roles.includes(role);
  return (
    <div className="space-y-4">
      <EditorSection title="Identity and behavior" detail="Profile IDs are stable; instructions and every published field are copied into the task snapshot.">
        <Field label="Profile ID"><input className={`${INPUT} font-mono`} disabled={!editable || !isNew} value={spec.profile_id} onChange={(event) => patch({ profile_id: slugify(event.target.value) })} /></Field>
        <Field label="Display name"><input className={INPUT} disabled={!editable} value={spec.display_name} onChange={(event) => patch({ display_name: event.target.value })} /></Field>
        <Field label="Role"><select className={INPUT} disabled={!editable} value={spec.role} onChange={(event) => patch({ role: event.target.value as AgentRole })}>{AGENT_ROLES.map((role) => <option key={role} value={role}>{humanize(role)}</option>)}</select></Field>
        <Field label="Permission mode"><select className={INPUT} disabled={!editable} value={spec.permission_mode} onChange={(event) => patch({ permission_mode: event.target.value as AgentProfileSpec["permission_mode"] })}>{["discuss", "plan", "interactive", "custom", "auto"].map((mode) => <option key={mode} value={mode}>{humanize(mode)}</option>)}</select></Field>
        <Field label="Instructions" wide><textarea className={`${INPUT} min-h-32 resize-y font-mono text-[12px]`} disabled={!editable} value={spec.instructions} onChange={(event) => patch({ instructions: event.target.value })} /></Field>
      </EditorSection>

      <EditorSection title="Tool ceiling" detail="The runtime intersects this allow-list with parent and session permissions; a profile can never grant a new tool.">
        <Field label="Allowed tool IDs" wide><textarea className={`${INPUT} min-h-20 resize-y font-mono text-[12px]`} disabled={!editable} placeholder="read_file, grep, run_shell" value={spec.allowed_tools.join(", ")} onChange={(event) => patch({ allowed_tools: csv(event.target.value) })} /></Field>
        <NumberField label="Max iterations" value={spec.max_iterations} min={1} max={200} disabled={!editable} onChange={(max_iterations) => patch({ max_iterations })} />
        <NumberField label="Tool-call budget · 0 = unset" value={numberMeta(metadata.tool_call_budget)} min={0} max={10000} disabled={!editable} onChange={(value) => patchMetadata({ tool_call_budget: value || null })} />
      </EditorSection>

      <EditorSection title="Delegation ceiling" detail="Choose the child roles this agent may create. Zero children requires an empty role list.">
        <NumberField label="Max children" value={spec.max_children} min={0} max={8} disabled={!editable} onChange={(max_children) => patch({ max_children, allowed_child_roles: max_children ? spec.allowed_child_roles : [] })} />
        <div className="md:col-span-2 grid grid-cols-2 gap-2 lg:grid-cols-3">
          {AGENT_ROLES.map((role) => <CheckField key={role} label={humanize(role)} checked={roleAllowed(role)} disabled={!editable || spec.max_children === 0} onChange={(checked) => patch({ allowed_child_roles: checked ? [...spec.allowed_child_roles, role] : spec.allowed_child_roles.filter((item) => item !== role) })} />)}
        </div>
      </EditorSection>

      <EditorSection title="Routing and run limits" detail="The profile stores a policy ID. Task creation snapshots the exact published profile and routing decision inputs.">
        <Field label="Model policy"><select className={INPUT} disabled={!editable} value={spec.model_policy} onChange={(event) => patch({ model_policy: event.target.value })}>{policies.map((policy) => <option key={policy.id} value={policy.id}>{policy.name || policy.id}{policy.current_version ? ` · v${policy.current_version}` : ""}</option>)}{!policies.some((policy) => policy.id === spec.model_policy) && <option value={spec.model_policy}>{spec.model_policy}</option>}</select></Field>
        <NumberField label="Timeout (seconds) · 0 = unset" value={numberMeta(metadata.timeout_seconds)} min={0} max={86400} disabled={!editable} onChange={(value) => patchMetadata({ timeout_seconds: value || null })} />
        <NumberField label="Token budget · 0 = unset" value={numberMeta(metadata.token_budget)} min={0} max={10000000} disabled={!editable} onChange={(value) => patchMetadata({ token_budget: value || null })} />
        <CheckField label="Require evidence" checked={boolMeta(metadata.evidence_required, true)} disabled={!editable} onChange={(value) => patchMetadata({ evidence_required: value })} />
        <CheckField label="Require tests" checked={boolMeta(metadata.tests_required)} disabled={!editable} onChange={(value) => patchMetadata({ tests_required: value })} />
        <CheckField label="Require review" checked={boolMeta(metadata.review_required, true)} disabled={!editable} onChange={(value) => patchMetadata({ review_required: value })} />
      </EditorSection>
    </div>
  );
}

function LifecycleActions({ detail, isNew, editable, dirty, busy, validation, onSave, onValidate, onPublish, onCreateDraft }: { detail: AgentProfileDetail | null; isNew: boolean; editable: boolean; dirty: boolean; busy: string; validation: ValidationReport | null; onSave: () => void; onValidate: () => void; onPublish: () => void; onCreateDraft: () => void }) {
  return (
    <div className="mt-5 flex flex-wrap items-center gap-2 border-t border-line pt-4">
      {editable && <><button className={PRIMARY_BUTTON} disabled={!dirty || !!busy} onClick={onSave}>{busy === "save" ? "Saving…" : isNew ? "Create draft" : "Save draft"}</button>{!isNew && <button className={BUTTON} disabled={dirty || !!busy} onClick={onValidate}>{busy === "validate" ? "Validating…" : "Validate"}</button>}{detail?.draft && <button className={BUTTON} disabled={dirty || validation?.valid !== true || !!busy} title={dirty ? "Save the draft first" : validation?.valid !== true ? "Validate the saved draft first" : undefined} onClick={onPublish}>{busy === "publish" ? "Publishing…" : "Publish version"}</button>}{dirty && <span className="text-[10.5px] text-warnInk">Unsaved changes</span>}</>}
      {!editable && detail && !detail.builtin && !detail.archived && !detail.draft && <button className={PRIMARY_BUTTON} disabled={!!busy} onClick={onCreateDraft}>Edit as new draft</button>}
    </div>
  );
}

function EditorSection({ title, detail, children }: { title: string; detail?: string; children: ReactNode }) {
  return <section className={`${CARD} p-4`}><SectionHead title={title} />{detail && <p className="-mt-1 mb-3 text-[11.5px] leading-relaxed text-muted">{detail}</p>}<div className="grid grid-cols-1 gap-3 md:grid-cols-2">{children}</div></section>;
}

function Field({ label, wide, children }: { label: string; wide?: boolean; children: ReactNode }) {
  return <label className={wide ? "md:col-span-2" : ""}><span className="mb-1 block text-[11.5px] font-medium text-muted">{label}</span>{children}</label>;
}

function NumberField({ label, value, disabled, min, max, onChange }: { label: string; value: number; disabled: boolean; min: number; max: number; onChange: (value: number) => void }) {
  return <Field label={label}><input type="number" className={INPUT} disabled={disabled} min={min} max={max} value={value} onChange={(event) => onChange(Number(event.target.value))} /></Field>;
}

function CheckField({ label, checked, disabled, onChange }: { label: string; checked: boolean; disabled: boolean; onChange: (checked: boolean) => void }) {
  return <label className="flex items-center gap-2 rounded-lg border border-line bg-paper px-3 py-2 text-[12px] text-ink"><input type="checkbox" checked={checked} disabled={disabled} onChange={(event) => onChange(event.target.checked)} />{label}</label>;
}

const csv = (value: string) => value.split(",").map((item) => item.trim()).filter(Boolean);
const numberMeta = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? value : 0;
const boolMeta = (value: unknown, fallback = false) => typeof value === "boolean" ? value : fallback;

export function ValidationSummary({ report }: { report: ValidationReport }) {
  return <div className={`mt-4 rounded-xl border p-3.5 ${report.valid ? "border-okLine bg-okSoft" : "border-danger/20 bg-dangerSoft"}`} data-testid="validation-summary"><div className={`text-[12.5px] font-medium ${report.valid ? "text-ok" : "text-danger"}`}>{report.valid ? "Validation passed" : `${report.errors.length} validation error${report.errors.length === 1 ? "" : "s"}`}</div>{[...report.errors, ...report.warnings].map((issue) => <div key={`${issue.code}:${issue.path}`} className="mt-1.5 text-[11.5px] text-muted"><span className="font-mono text-[10px] text-faint">{issue.path}</span> · {issue.message}</div>)}</div>;
}

