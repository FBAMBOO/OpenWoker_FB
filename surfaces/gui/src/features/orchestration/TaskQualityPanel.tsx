import { useCallback, useEffect, useState } from "react";
import type { OrchestrationApi } from "./api";
import type { OrchestrationTaskDetail, QualityBundleV2, QualityFinding } from "./types";
import { RepairPanel } from "./RepairPanel";
import { BUTTON, CARD, EmptyState, ErrorNotice, INPUT, LoadingBlock, StatusBadge } from "./ui";

type Row = Record<string, unknown>;
const BUDGET_DIMENSIONS = ["model_calls", "tool_calls", "reported_tokens", "active_seconds", "tool_payload_bytes"] as const;

export function TaskQualityPanel({ api, task, onTaskRefresh }: { api: OrchestrationApi; task: OrchestrationTaskDetail; onTaskRefresh: () => void }) {
  const [quality, setQuality] = useState<QualityBundleV2 | null>(null);
  const [artifact, setArtifact] = useState<Row | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [resumeBusy, setResumeBusy] = useState(false);
  const [resumeReason, setResumeReason] = useState("");
  const [budgetLimits, setBudgetLimits] = useState<Record<string, string>>({});
  const [paging, setPaging] = useState<"gates" | "findings" | "waivers" | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [qualityValue, artifactValue] = await Promise.all([
        api.getTaskQuality(task.id),
        task.primary_deliverable ? api.getArtifactMetadata(task.primary_deliverable.artifact_id).catch(() => null) : Promise.resolve(null),
      ]);
      setQuality(qualityValue);
      setArtifact(artifactValue);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Quality results could not be loaded.");
    } finally {
      setLoading(false);
    }
  }, [api, task.id, task.primary_deliverable]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    const limits = task.effective_budget?.limit || {};
    setBudgetLimits(Object.fromEntries(BUDGET_DIMENSIONS.map((name) => [name, limits[name] == null ? "" : String(limits[name])])))
  }, [task.id, task.effective_budget]);
  const changed = async () => { await load(); onTaskRefresh(); };
  const resume = async () => {
    setResumeBusy(true);
    setError(null);
    try {
      const payload: Record<string, unknown> = {};
      if (task.budget_status === "exhausted") {
        const reason = resumeReason.trim();
        const parsed = Object.fromEntries(BUDGET_DIMENSIONS.map((name) => [name, Number(budgetLimits[name])]));
        if (!reason) throw new Error("An audit reason is required to increase an exhausted budget.");
        if (Object.values(parsed).some((value) => !Number.isSafeInteger(value) || value < 0)) throw new Error("Every budget limit must be a non-negative integer.");
        payload.effective_limits = parsed;
        payload.reason = reason;
      }
      await api.resumeTaskQuality(task.id, payload);
      await changed();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Quality workflow could not resume.");
    } finally {
      setResumeBusy(false);
    }
  };
  const loadMore = async (kind: "gates" | "findings" | "waivers") => {
    if (!quality?.[kind].next_cursor) return;
    const cursorKey = kind === "gates" ? "gate_cursor" : kind === "findings" ? "finding_cursor" : "waiver_cursor";
    setPaging(kind);
    setError(null);
    try {
      const next = await api.getTaskQuality(task.id, 0, 200, { [cursorKey]: quality[kind].next_cursor });
      setQuality((current) => current ? {
        ...current,
        [kind]: {
          ...next[kind],
          items: [...current[kind].items, ...next[kind].items],
        },
      } : current);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : `The next ${kind} page could not be loaded.`);
    } finally {
      setPaging(null);
    }
  };
  if (loading) return <LoadingBlock label="Loading authoritative quality results..." />;
  if (error && !quality) return <ErrorNotice message={error} onRetry={() => void load()} />;
  if (!quality) return <EmptyState title="No quality result" detail="Hard gates and independent semantic evaluations appear after artifact validation." />;

  const gates = quality.gates.items || [];
  const findings: QualityFinding[] = quality.findings.items || [];
  const failedGates = gates.filter((item) => String(item.status) === "fail");
  const openFindings = findings.filter((item) => item.status === "open");
  const resumeAllowed = ["needs_attention", "needs_reconciliation", "recovering"].includes(String(task.workflow_status));

  return (
    <section aria-label="Task quality" data-testid="task-quality-panel" className="space-y-4">
      {error && <ErrorNotice message={error} />}
      <div className={`${CARD} p-4`}>
        <div className="flex flex-wrap items-center gap-2">
          <StatusBadge status={quality.quality_status} label={`Quality ${quality.quality_status}`} />
          <StatusBadge status={failedGates.length ? "fail" : gates.length ? "pass" : "pending"} label={`${failedGates.length} hard-gate failures`} />
          {quality.quality_verdict && <StatusBadge status={quality.quality_verdict.decision === "publish" ? "pass" : "fail"} label={`Verdict: ${quality.quality_verdict.decision}`} />}
          {quality.quality_verdict?.total_score != null && <span className="text-[12px] font-semibold text-ink">Score {quality.quality_verdict.total_score}</span>}
          {resumeAllowed && <button type="button" className={`${BUTTON} ml-auto`} disabled={resumeBusy} onClick={() => void resume()}>{resumeBusy ? "Resuming..." : "Resume quality workflow"}</button>}
        </div>
        {resumeAllowed && task.budget_status === "exhausted" && <div className="mt-3 rounded border border-line p-3" aria-label="Budget extension"><p className="mb-2 text-[10px] text-muted">Increase at least one hard limit. The server creates a new ledger revision and preserves historical consumption.</p><div className="grid gap-2 sm:grid-cols-5">{BUDGET_DIMENSIONS.map((name) => <label key={name} className="text-[9px] text-muted">{name}<input aria-label={`Budget ${name}`} className={`${INPUT} mt-1`} inputMode="numeric" value={budgetLimits[name] || ""} onChange={(event) => setBudgetLimits((current) => ({ ...current, [name]: event.target.value }))} /></label>)}</div><label className="mt-2 block text-[9px] text-muted">Audit reason<input aria-label="Budget extension reason" className={`${INPUT} mt-1`} value={resumeReason} onChange={(event) => setResumeReason(event.target.value)} /></label></div>}
        <div className="mt-2 grid gap-2 text-[10px] sm:grid-cols-3"><div><span className="block text-faint">Reason code</span><span className="text-ink">{quality.quality_reason_code || "none"}</span></div><div><span className="block text-faint">Artifact subject</span><span className="break-all font-mono text-[9px] text-ink">{task.primary_deliverable?.sha256 || "pending"}</span></div><div><span className="block text-faint">Independent read coverage</span><span className="text-ink">{Math.round(Number(artifact?.max_read_coverage_ratio || 0) * 100)}%</span></div></div>
      </div>

      <section aria-label="Quality gates">
        <h3 className="mb-2 text-[11px] font-semibold text-ink">Deterministic and semantic gates</h3>
        {gates.length ? <div className="grid gap-2 sm:grid-cols-2">{gates.map((item, index) => <article key={String(item.id || index)} className={`${CARD} p-3`}><div className="flex items-center gap-2"><span className="min-w-0 flex-1 truncate text-[10.5px] font-medium text-ink">{String(item.gate_id || item.validator_id || item.id)}</span><StatusBadge status={String(item.status || "unknown")} /></div><div className="mt-1 text-[9.5px] text-muted">{String(item.message || item.reason || item.details || "")}</div><code className="mt-1 block break-all text-[8.5px] text-faint">{String(item.artifact_hash || item.content_hash || "")}</code></article>)}</div> : <EmptyState title="No gate executions" detail="Gate results are artifact-version bound and appear after validation starts." />}
        {quality.gates.next_cursor && <button type="button" className={`${BUTTON} mt-2`} disabled={paging === "gates"} onClick={() => void loadMore("gates")}>{paging === "gates" ? "Loading gates..." : "Load more gates"}</button>}
      </section>

      <section aria-label="Quality findings">
        <div className="mb-2 flex items-center gap-2"><h3 className="text-[11px] font-semibold text-ink">Findings</h3><span className="text-[10px] text-muted">{openFindings.length} open / {findings.length} returned</span></div>
        {findings.length ? <div className="space-y-2">{findings.map((item) => <article key={item.id} className={`${CARD} p-3`}><div className="flex flex-wrap items-center gap-2"><StatusBadge status={item.status} /><span className="text-[9.5px] font-semibold uppercase tracking-wide text-ink">{item.severity} / {item.category}</span>{item.blocking && <StatusBadge status="fail" label="Blocking" />}{item.repairable && <StatusBadge status="pending" label="Repairable" />}<code className="ml-auto text-[8.5px] text-faint">{item.id}</code></div><p className="mt-1 text-[10.5px] leading-relaxed text-ink">{item.message}</p>{item.suggested_fix && <p className="mt-1 text-[10px] text-muted">Suggested fix: {item.suggested_fix}</p>}<div className="mt-1 text-[9px] text-faint">{item.section_id ? `Section ${item.section_id}` : "Global"} / source {String(item.source_role || item.validator_id || "quality service")}</div></article>)}</div> : <EmptyState title="No findings" detail="No typed quality findings have been persisted for this artifact version." />}
        {quality.findings.next_cursor && <button type="button" className={`${BUTTON} mt-2`} disabled={paging === "findings"} onClick={() => void loadMore("findings")}>{paging === "findings" ? "Loading findings..." : "Load more findings"}</button>}
      </section>

      {quality.waivers.items.length > 0 && <section className={`${CARD} p-3`} aria-label="Quality waivers"><h3 className="text-[11px] font-semibold text-ink">Signed waivers</h3>{quality.waivers.items.map((item, index) => <div key={String(item.id || index)} className="mt-2 border-t border-line pt-2 text-[10px] text-muted"><span className="font-medium text-ink">{String(item.subject_type)} / {String(item.subject_id)}</span><span className="ml-2">{String(item.reason || "")}</span><code className="mt-1 block break-all text-[8.5px] text-faint">{String(item.signature_hash || item.content_hash || "")}</code></div>)}{quality.waivers.next_cursor && <button type="button" className={`${BUTTON} mt-2`} disabled={paging === "waivers"} onClick={() => void loadMore("waivers")}>{paging === "waivers" ? "Loading waivers..." : "Load more waivers"}</button>}</section>}
      <RepairPanel api={api} task={task} findings={findings} gates={gates} onChanged={changed} />
    </section>
  );
}
