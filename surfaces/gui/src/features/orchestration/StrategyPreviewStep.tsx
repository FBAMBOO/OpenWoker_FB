import type { ExecutionStrategyV2, RepositorySnapshotV2, TaskQualityContract } from "./types";
import { CARD, PRIMARY_BUTTON, StatusBadge } from "./ui";

const asRecord = (value: unknown): Record<string, unknown> => value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};

export function StrategyPreviewStep({ strategy, snapshot, contract, busy, onContinue }: { strategy: ExecutionStrategyV2; snapshot: RepositorySnapshotV2; contract: TaskQualityContract; busy: boolean; onContinue: () => void }) {
  const assessment = strategy.assessment;
  const axes = [
    ["Cognitive complexity", assessment.cognitive_complexity],
    ["Operational risk", assessment.operational_risk],
    ["Evidence workload", assessment.evidence_workload],
  ] as const;
  const policyRows = Object.entries(strategy.effective_policy).map(([name, raw]) => {
    const value = asRecord(raw);
    return {
      name,
      value: "value" in value ? value.value : raw,
      source: value.source || strategy.policy_provenance?.[name] || "frozen strategy",
    };
  });
  const budget = asRecord(strategy.budget_profile);
  return (
    <section aria-label="Strategy preview step" data-testid="quality-strategy-step" className="space-y-3">
      <div className="flex flex-wrap items-center gap-2"><StatusBadge status="pass" label={strategy.template_id} /><span className="text-[10.5px] text-muted">{strategy.archetype.replace(/_/g, " ")} · max {strategy.max_repair_attempts} repair rounds</span></div>
      <section aria-label="Strategy assessment axes" className="grid gap-2 sm:grid-cols-3">
        {axes.map(([label, value]) => <div key={label} className={`${CARD} p-3`}><div className="text-[9.5px] uppercase tracking-wide text-faint">{label}</div><div className="mt-1 text-[18px] font-semibold text-ink">{value}<span className="text-[10px] font-normal text-muted"> / 100</span></div></div>)}
      </section>
      {assessment.rationale.length > 0 && <ul aria-label="Strategy assessment rationale" className={`${CARD} list-disc space-y-1 px-7 py-3 text-[10px] text-muted`}>{assessment.rationale.map((item) => <li key={item}>{item}</li>)}</ul>}
      <div className="grid gap-3 lg:grid-cols-3">
        <section className={`${CARD} p-3 lg:col-span-2`}><h3 className="mb-2 text-[11px] font-semibold text-ink">Adaptive DAG</h3><ol className="space-y-1.5">{strategy.nodes.map((raw, index) => { const node = raw as Record<string, unknown>; return <li key={String(node.key || index)} className="rounded-lg border border-line bg-paper px-2.5 py-2 text-[10.5px]"><span className="font-medium text-ink">{index + 1}. {String(node.title || node.key || "Node")}</span><span className="ml-2 text-muted">{String(node.role || node.agent || node.kind || "service")}</span><div className="mt-0.5 text-faint">Schema {String(node.result_schema_id || (node.quality_node_config as Record<string, unknown> | undefined)?.result_schema_id || "service-owned")}</div></li>; })}</ol></section>
        <div className="space-y-3">
          <section className={`${CARD} p-3`}><h3 className="mb-1 text-[11px] font-semibold text-ink">Frozen target</h3><div className="break-all font-mono text-[9.5px] text-muted">{snapshot.selected_ref || snapshot.snapshot_kind}@{snapshot.commit_oid || snapshot.manifest_hash}</div></section>
          <section className={`${CARD} p-3`} aria-label="Effective budget policy"><h3 className="mb-1 text-[11px] font-semibold text-ink">Budget profile</h3><div className="text-[10px] text-ink">Mode {String(budget.mode || "unconfigured")}</div><pre className="mt-1 max-h-48 overflow-auto whitespace-pre-wrap text-[9.5px] text-muted">{JSON.stringify(budget.limits || {}, null, 2)}</pre></section>
          <section className={`${CARD} p-3`}><h3 className="mb-1 text-[11px] font-semibold text-ink">Hard gates</h3><div className="text-[10px] text-muted">{contract.requirements.filter((item) => item.hard_gate).length} contract hard gates plus deterministic validators and independent semantic review.</div></section>
        </div>
      </div>
      <section className={`${CARD} p-3`} aria-label="Effective policy provenance"><h3 className="mb-2 text-[11px] font-semibold text-ink">Effective policy and provenance</h3><dl className="grid gap-x-3 gap-y-1 text-[9.5px] sm:grid-cols-[12rem_1fr_12rem]">{policyRows.map((item) => <div key={item.name} className="contents"><dt className="font-mono text-faint">{item.name}</dt><dd className="break-all text-ink">{JSON.stringify(item.value)}</dd><dd className="text-muted">source: {String(item.source)}</dd></div>)}</dl></section>
      <div className="flex justify-end"><button type="button" className={PRIMARY_BUTTON} disabled={busy} onClick={onContinue}>Review publication</button></div>
    </section>
  );
}
