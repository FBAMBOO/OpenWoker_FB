import type { ApiDownload } from "./api";
import type { OrchestrationTaskDetail } from "./types";
import { BUTTON, CARD, EmptyState, PRIMARY_BUTTON, StatusBadge } from "./ui";

export function TaskQualityOverview({ task, apiDownload, onViewDeliverable, onViewQuality }: { task: OrchestrationTaskDetail; apiDownload: ApiDownload; onViewDeliverable: () => void; onViewQuality: () => void }) {
  const primary = task.primary_deliverable;
  return (
    <section aria-label="Task Quality V2 overview" data-testid="task-quality-overview" className="space-y-4">
      <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        {[
          ["Workflow", task.workflow_status || "unknown"],
          ["Quality", task.quality_status || "unknown"],
          ["Artifact", task.artifact_status || "none"],
          ["Budget", task.budget_status || "unconfigured"],
        ].map(([label, status]) => <div key={label} className={`${CARD} p-3`}><div className="mb-1 text-[9.5px] uppercase tracking-wide text-faint">{label}</div><StatusBadge status={status} /></div>)}
      </div>

      {primary ? (
        <article className="rounded-xl2 border border-okLine bg-okSoft p-4" aria-label="Primary deliverable">
          <div className="flex flex-wrap items-start gap-3">
            <div className="min-w-0 flex-1"><div className="text-[10px] font-semibold uppercase tracking-wide text-ok">Primary deliverable</div><h2 className="mt-1 break-all text-[15px] font-semibold text-ink">{primary.filename}</h2><div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-muted"><span>{primary.mime_type}</span><span>{primary.byte_size.toLocaleString()} bytes</span><span>version {primary.version}</span><StatusBadge status={primary.status} /></div><code className="mt-2 block break-all text-[9px] text-faint">{primary.sha256}</code></div>
            <div className="flex gap-2"><button type="button" className={PRIMARY_BUTTON} onClick={onViewDeliverable}>View complete artifact</button><button type="button" className={BUTTON} onClick={() => void apiDownload(`/v1/orchestration/artifacts/${encodeURIComponent(primary.artifact_id)}/download`, primary.filename)}>Download</button></div>
          </div>
        </article>
      ) : <EmptyState title="Primary deliverable pending" detail="The producer must upload and the quality service must verify the declared artifact before it becomes the task result." />}

      <div className="grid gap-3 lg:grid-cols-3">
        <section className={`${CARD} p-3`}><h3 className="text-[10px] font-semibold uppercase tracking-wide text-faint">Frozen target</h3>{task.target ? <><div className="mt-1 break-all text-[11px] font-medium text-ink">{task.target.repo}</div><div className="mt-1 font-mono text-[9.5px] text-muted">{task.target.snapshot_ref || "working tree"}@{task.target.short_sha || task.target.snapshot_id}</div><div className="mt-1 text-[9.5px] text-faint">{task.target.dirty ? "Dirty snapshot" : "Clean immutable snapshot"}</div></> : <div className="mt-2 text-[10px] text-muted">Target is not frozen yet.</div>}</section>
        <section className={`${CARD} p-3`}><h3 className="text-[10px] font-semibold uppercase tracking-wide text-faint">Quality decision</h3><div className="mt-1 text-[20px] font-semibold text-ink">{task.quality_score == null ? "Pending" : task.quality_score}</div><div className="mt-1 flex items-center gap-2"><StatusBadge status={task.hard_gate_status || "pending"} label={`Hard gates ${task.hard_gate_status || "pending"}`} />{task.has_waiver && <StatusBadge status="waived" label="Waiver active" />}</div><button type="button" className="mt-2 text-[10px] font-medium text-accent" onClick={onViewQuality}>Open quality evidence</button></section>
        <section className={`${CARD} p-3`}><h3 className="text-[10px] font-semibold uppercase tracking-wide text-faint">Effective budget</h3><div className="mt-1 text-[12px] font-medium text-ink">{task.effective_budget?.mode || "unconfigured"}</div><div className="mt-1 text-[10px] text-muted">{task.budget_utilization_percent == null ? "Utilization begins when the run starts." : `${task.budget_utilization_percent}% maximum dimension utilization`}</div><div className="mt-2 text-[9.5px] text-faint">Repairs {task.run_summary?.repairs || 0} / execution nodes {task.run_summary?.nodes || 0}</div></section>
      </div>
      {task.attention_reason && <div className="rounded-lg border border-warnInk/20 bg-warnSoft px-3 py-2 text-[10.5px] text-warnInk">Attention: {task.attention_reason}</div>}
    </section>
  );
}
