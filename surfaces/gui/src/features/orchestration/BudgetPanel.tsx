import type { EffectiveBudget, OrchestrationTaskDetail } from "./types";
import { CARD, EmptyState, StatusBadge } from "./ui";

const labelFor = (value: string) => value.replace(/_/g, " ").replace(/\b\w/g, (letter: string) => letter.toUpperCase());

export function BudgetPanel({ task }: { task: OrchestrationTaskDetail }) {
  const budget: EffectiveBudget | undefined = task.effective_budget;
  if (!budget || budget.mode === "unconfigured") {
    return <EmptyState title="Budget not configured" detail="The immutable budget ledger is created when the admitted strategy starts." />;
  }
  const dimensions = [...new Set([...Object.keys(budget.limit || {}), ...Object.keys(budget.used || {}), ...Object.keys(budget.reserved || {})])];
  return (
    <section aria-label="Effective budget" data-testid="budget-panel" className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge status={task.budget_status || (budget.over_budget ? "over_budget" : "within_budget")} />
        <span className="text-[11px] font-medium text-ink">{budget.mode === "unlimited" ? "Unlimited budget - no hard stop" : `${labelFor(budget.mode)} enforcement`}</span>
        {budget.source && <span className="text-[10px] text-muted">Source: {budget.source}</span>}
        {budget.ledger_id && <code className="ml-auto text-[9.5px] text-faint">{budget.ledger_id}</code>}
      </div>
      {budget.mode === "unlimited" && <div className="rounded-lg border border-warnInk/20 bg-warnSoft px-3 py-2 text-[10.5px] text-warnInk">Unlimited is explicit and auditable; it does not imply a hidden zero-cost run.</div>}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {dimensions.map((key) => {
          const used = Number(budget.used?.[key] || 0);
          const reserved = Number(budget.reserved?.[key] || 0);
          const rawLimit = budget.limit?.[key];
          const limit = typeof rawLimit === "number" ? rawLimit : null;
          const ratio = limit && limit > 0 ? Math.min(1, (used + reserved) / limit) : 0;
          const tone = ratio >= 1 ? "bg-danger" : ratio >= 0.8 ? "bg-warnInk" : "bg-accent";
          return (
            <article key={key} className={`${CARD} p-3`}>
              <div className="flex items-center justify-between text-[10.5px]"><span className="font-medium text-ink">{labelFor(key)}</span><span className="text-muted">{limit == null ? "unlimited" : `${Math.round(ratio * 100)}%`}</span></div>
              <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-line"><div className={`h-full ${tone}`} style={{ width: `${Math.round(ratio * 100)}%` }} /></div>
              <dl className="mt-2 grid grid-cols-3 gap-1 text-[9.5px]"><div><dt className="text-faint">Used</dt><dd className="text-ink">{used.toLocaleString()}</dd></div><div><dt className="text-faint">Reserved</dt><dd className="text-ink">{reserved.toLocaleString()}</dd></div><div><dt className="text-faint">Limit</dt><dd className="text-ink">{limit == null ? "infinite" : limit.toLocaleString()}</dd></div></dl>
            </article>
          );
        })}
      </div>
      {budget.provider_usage && <details className={`${CARD} p-3`}><summary className="cursor-pointer text-[10.5px] font-medium text-ink">Provider-reported usage semantics</summary><pre className="mt-2 max-h-64 overflow-auto whitespace-pre-wrap text-[9.5px] text-muted">{JSON.stringify(budget.provider_usage, null, 2)}</pre></details>}
    </section>
  );
}
