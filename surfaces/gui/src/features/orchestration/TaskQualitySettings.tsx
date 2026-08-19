import { useCallback, useEffect, useMemo, useState } from "react";
import { createOrchestrationApi, type ApiRequest, type OrchestrationApi } from "./api";
import type { TaskQualityBenchmarkComparison, TaskQualityBenchmarkRun, TaskQualityBenchmarkSuite } from "./types";
import { BUTTON, CARD, ErrorNotice, INPUT, LoadingBlock, PRIMARY_BUTTON, StatusBadge } from "./ui";

export function TaskQualitySettings({ apiRequest }: { apiRequest: ApiRequest }) {
  const api = useMemo(() => createOrchestrationApi(apiRequest), [apiRequest]);
  return (
    <section aria-label="Task Quality settings" data-testid="task-quality-settings" className="space-y-6">
      <div><h1 className="text-[20px] font-semibold text-ink">Task Quality V2</h1><p className="mt-1 text-[12px] text-muted">Immutable contracts, repository snapshots, artifact-bound gates, effective budgets, and the offline release corpus.</p></div>
      <div className="grid gap-3 lg:grid-cols-3">
        <section className={`${CARD} p-4`}><h2 className="text-[11px] font-semibold text-ink">Rollout policy</h2><dl className="mt-2 space-y-1.5 text-[10px]"><div className="flex justify-between"><dt className="text-muted">Task Quality V2</dt><dd><StatusBadge status="pending" label="Opt-in" /></dd></div><div className="flex justify-between"><dt className="text-muted">Repository snapshot</dt><dd className="text-ink">Required for V2 code tasks</dd></div><div className="flex justify-between"><dt className="text-muted">Auto repair</dt><dd className="text-ink">Off</dd></div><div className="flex justify-between"><dt className="text-muted">Runtime budget</dt><dd className="text-ink">Hard by default</dd></div></dl></section>
        <section className={`${CARD} p-4`}><h2 className="text-[11px] font-semibold text-ink">Quality-first rubric</h2><div className="mt-2 grid grid-cols-2 gap-1 text-[9.5px] text-muted"><span>Architecture 30</span><span>Relationships 20</span><span>Evidence 20</span><span>Quantitative 10</span><span>Risk 10</span><span>Limits + structure 10</span></div><div className="mt-2 text-[10px] font-medium text-ink">Publish threshold 85 / hard gates authoritative</div></section>
        <section className={`${CARD} p-4`}><h2 className="text-[11px] font-semibold text-ink">Benchmark budget</h2><dl className="mt-2 grid grid-cols-2 gap-1 text-[9.5px]"><dt className="text-muted">Reported tokens</dt><dd className="text-right text-ink">3,000,000</dd><dt className="text-muted">Tool calls</dt><dd className="text-right text-ink">120</dd><dt className="text-muted">Elapsed</dt><dd className="text-right text-ink">20 minutes</dd><dt className="text-muted">Duplicate scan</dt><dd className="text-right text-ink">20% max</dd></dl></section>
      </div>
      <BenchmarkRuns api={api} />
    </section>
  );
}

export function BenchmarkRuns({ api }: { api: OrchestrationApi }) {
  const [suites, setSuites] = useState<TaskQualityBenchmarkSuite[]>([]);
  const [suiteId, setSuiteId] = useState("");
  const [candidateId, setCandidateId] = useState("v2");
  const [run, setRun] = useState<TaskQualityBenchmarkRun | null>(null);
  const [comparison, setComparison] = useState<TaskQualityBenchmarkComparison | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [promoted, setPromoted] = useState(false);

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const values = await api.listTaskQualityBenchmarkSuites();
      setSuites(values);
      setSuiteId((current) => current || values[0]?.id || "");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Benchmark suites could not be loaded.");
    } finally { setLoading(false); }
  }, [api]);
  useEffect(() => { void load(); }, [load]);
  const selected = suites.find((item) => item.id === suiteId);

  const execute = async () => {
    if (!suiteId) return;
    setBusy(true); setError(null); setPromoted(false);
    try {
      const next = await api.runTaskQualityBenchmark(suiteId, candidateId);
      setRun(next);
      setComparison(await api.getTaskQualityBenchmarkComparison(next.id));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Benchmark run failed.");
    } finally { setBusy(false); }
  };

  if (loading) return <LoadingBlock label="Loading offline benchmark corpus..." />;
  return (
    <section aria-label="Benchmark runs" data-testid="benchmark-runs" className="space-y-3">
      <div><h2 className="text-[14px] font-semibold text-ink">Offline benchmark runs</h2><p className="text-[10.5px] text-muted">Only registered, sanitized snapshot artifacts can run. Host paths, prompt bodies and provider transcripts are rejected.</p></div>
      {error && <ErrorNotice message={error} onRetry={() => void load()} />}
      <div className={`${CARD} flex flex-wrap items-end gap-3 p-3`}>
        <label className="min-w-52 flex-1 text-[10px] text-muted">Suite<select className={`${INPUT} mt-1 w-full`} value={suiteId} onChange={(event) => { setSuiteId(event.target.value); setRun(null); setComparison(null); const suite = suites.find((item) => item.id === event.target.value); setCandidateId(suite?.candidate_ids.includes("v2") ? "v2" : suite?.candidate_ids[0] || ""); }}><option value="">Select suite</option>{suites.map((item) => <option key={item.id} value={item.id}>{item.name} ({item.stack})</option>)}</select></label>
        <label className="min-w-40 text-[10px] text-muted">Registered candidate<select className={`${INPUT} mt-1 w-full`} value={candidateId} onChange={(event) => setCandidateId(event.target.value)}>{(selected?.candidate_ids || []).map((item) => <option key={item} value={item}>{item}</option>)}</select></label>
        <button type="button" className={PRIMARY_BUTTON} disabled={!suiteId || !candidateId || busy} onClick={() => void execute()}>{busy ? "Running offline corpus..." : "Run benchmark"}</button>
      </div>
      {selected && <div className="grid gap-2 text-[9.5px] sm:grid-cols-3"><div className={`${CARD} p-2.5`}><span className="block text-faint">Snapshot artifact</span><code className="break-all text-ink">{selected.snapshot_artifact_id}</code></div><div className={`${CARD} p-2.5`}><span className="block text-faint">Suite version / hash</span><code className="break-all text-ink">v{selected.version} / {selected.content_hash}</code></div><div className={`${CARD} p-2.5`}><span className="block text-faint">Promoted baseline</span><span className="text-ink">{String(selected.promoted_baseline?.candidate_id || selected.baseline_candidate)}</span></div></div>}
      {run && <BenchmarkRunResult run={run} comparison={comparison} />}
      {run?.status === "pass" && (
        <details className={`${CARD} p-3`}><summary className="cursor-pointer text-[10.5px] font-medium text-ink">Admin: promote this run as baseline</summary><p className="mt-2 text-[10px] text-muted">The server signs this action as the authenticated local operator; request fields cannot override the actor.</p><div className="mt-3"><label className="text-[10px] text-muted">Audit reason<input className={`${INPUT} mt-1 w-full`} value={reason} onChange={(event) => setReason(event.target.value)} /></label></div><div className="mt-3 flex items-center justify-end gap-2">{promoted && <StatusBadge status="pass" label="Baseline promoted" />}<button type="button" className={BUTTON} disabled={!reason.trim() || busy} onClick={() => { setBusy(true); setError(null); void api.promoteTaskQualityBenchmarkBaseline(run.suite_id, { run_id: run.id, reason: reason.trim() }).then(() => { setPromoted(true); return load(); }).catch((caught) => setError(caught instanceof Error ? caught.message : "Baseline promotion failed.")).finally(() => setBusy(false)); }}>Promote signed baseline</button></div></details>
      )}
    </section>
  );
}

function BenchmarkRunResult({ run, comparison }: { run: TaskQualityBenchmarkRun; comparison: TaskQualityBenchmarkComparison | null }) {
  const metrics = run.metrics;
  const rows = [
    ["Quality score", metrics.quality_score], ["Hard-gate failures", Array.isArray(metrics.hard_gate_failures) ? metrics.hard_gate_failures.length : 0],
    ["Coverage", `${metrics.required_area_coverage}/${metrics.required_area_total}`], ["Citation resolution", `${Math.round(Number(metrics.citation_resolution_ratio || 0) * 100)}%`],
    ["Reviewer read", `${Math.round(Number(metrics.artifact_read_coverage_ratio || 0) * 100)}%`], ["Target correct", metrics.snapshot_correct ? "yes" : "no"],
    ["Reported tokens", Number(metrics.reported_tokens || 0).toLocaleString()], ["Tool calls", metrics.tool_calls],
    ["Elapsed", `${metrics.elapsed_seconds}s`], ["Duplicate scan", `${Math.round(Number(metrics.duplicate_scan_ratio || 0) * 100)}%`],
    ["Repairs", metrics.repair_attempts], ["Provider / model", `${metrics.provider} / ${metrics.model}`],
  ];
  return <section className={`${CARD} p-4`} aria-label="Benchmark result"><div className="flex flex-wrap items-center gap-2"><StatusBadge status={run.status} label={`Benchmark ${run.status}`} /><span className="text-[10.5px] text-muted">{run.suite_id} / {run.candidate_id}</span><code className="ml-auto text-[9px] text-faint">{run.id}</code></div><div className="mt-3 grid gap-x-5 gap-y-2 sm:grid-cols-2 lg:grid-cols-3">{rows.map(([label, value]) => <div key={String(label)} className="border-b border-line pb-1.5 text-[10px]"><span className="text-faint">{String(label)}</span><span className="float-right font-medium text-ink">{String(value)}</span></div>)}</div>{comparison && <ScoreTrend comparison={comparison} />}{run.failures.length > 0 && <div className="mt-3 rounded-lg border border-danger/20 bg-dangerSoft p-3"><div className="text-[10.5px] font-medium text-danger">Release blockers</div>{run.failures.map((item, index) => <div key={String(item.code || index)} className="mt-1 text-[9.5px] text-danger">{String(item.code)}: observed {JSON.stringify(item.observed)}, expected {JSON.stringify(item.expected)}</div>)}</div>}<code className="mt-3 block break-all text-[8.5px] text-faint">Run hash {run.content_hash}</code></section>;
}

export function ScoreTrend({ comparison }: { comparison: TaskQualityBenchmarkComparison }) {
  const current = Number(comparison.current_metrics.quality_score || 0);
  const baseline = Number(comparison.baseline_metrics.quality_score || 0);
  return <section className="mt-4" aria-label="Score trend"><div className="mb-1 flex justify-between text-[9.5px] text-muted"><span>Quality score vs baseline</span><span>{baseline} -&gt; {current} ({comparison.deltas.quality_score >= 0 ? "+" : ""}{comparison.deltas.quality_score || 0})</span></div><div className="relative h-3 overflow-hidden rounded-full bg-line"><div className="absolute h-full bg-faint/40" style={{ width: `${Math.min(100, baseline)}%` }} /><div className={`absolute h-full ${comparison.quality_score_regression ? "bg-danger" : "bg-accent"}`} style={{ width: `${Math.min(100, current)}%`, opacity: 0.75 }} /></div>{comparison.quality_score_regression && <div className="mt-1 text-[9.5px] text-danger">No-go: quality regressed more than five points.</div>}</section>;
}
