import { useCallback, useEffect, useState } from "react";
import type { OrchestrationApi } from "./api";
import type { ExecutionStrategyV2, RepositorySnapshotV2, TaskQualityContract } from "./types";
import { CARD, EmptyState, ErrorNotice, LoadingBlock, StatusBadge } from "./ui";

export function TaskQualityDefinitionPanel({ api, taskId, kind }: { api: OrchestrationApi; taskId: string; kind: "contract" | "target" | "plan" }) {
  const [value, setValue] = useState<TaskQualityContract | RepositorySnapshotV2 | ExecutionStrategyV2 | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setValue(await (kind === "contract" ? api.getTaskQualityContract(taskId) : kind === "target" ? api.getTaskQualitySnapshot(taskId) : api.getTaskQualityStrategy(taskId)));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : `${kind} could not be loaded.`);
    } finally {
      setLoading(false);
    }
  }, [api, kind, taskId]);
  useEffect(() => { void load(); }, [load]);
  if (loading) return <LoadingBlock label={`Loading immutable ${kind}...`} />;
  if (error) return <ErrorNotice message={error} onRetry={() => void load()} />;
  if (!value) return <EmptyState title={`No active ${kind}`} detail="This version has not been bound to the task." />;

  if (kind === "contract") {
    const contract = value as TaskQualityContract;
    return <section aria-label="Immutable task contract" className="space-y-3"><div className="flex flex-wrap items-center gap-2"><StatusBadge status={contract.status} /><span className="text-[10.5px] text-muted">{contract.archetype.replace(/_/g, " ")} / {contract.quality_profile_id}</span><code className="ml-auto break-all text-[9px] text-faint">v{contract.version} {contract.content_hash}</code></div><div className="space-y-2">{contract.requirements.map((item) => <article key={item.id} className={`${CARD} p-3`}><div className="flex flex-wrap items-center gap-2 text-[9.5px]"><span className="uppercase tracking-wide text-faint">{item.category} / {item.source}</span>{item.hard_gate && <StatusBadge status="pending" label="Hard gate" />}<span className="ml-auto text-muted">{item.verification_method}</span></div><p className="mt-1 text-[10.5px] text-ink">{item.text}</p></article>)}</div><details className={`${CARD} p-3`}><summary className="cursor-pointer text-[10.5px] font-medium text-ink">Deliverables, constraints and non-goals</summary><pre className="mt-2 max-h-96 overflow-auto whitespace-pre-wrap text-[9.5px] text-muted">{JSON.stringify({ deliverables: contract.deliverables, constraints: contract.constraints, non_goals: contract.non_goals }, null, 2)}</pre></details></section>;
  }
  if (kind === "target") {
    const snapshot = value as RepositorySnapshotV2;
    return <section aria-label="Frozen repository target" className="space-y-3"><div className={`${CARD} p-4`}><div className="flex flex-wrap items-center gap-2"><StatusBadge status={snapshot.status} /><span className="text-[10.5px] text-muted">{snapshot.snapshot_kind} / confidence {Math.round(snapshot.resolution_confidence * 100)}%</span><code className="ml-auto text-[9px] text-faint">v{snapshot.version}</code></div><dl className="mt-3 grid gap-2 text-[10px] sm:grid-cols-[10rem_1fr]"><dt className="text-faint">Repository root</dt><dd className="break-all text-ink">{snapshot.repo_root}</dd><dt className="text-faint">Project root</dt><dd className="break-all text-ink">{snapshot.project_root}</dd><dt className="text-faint">Selected ref</dt><dd className="font-mono text-ink">{snapshot.selected_ref || "working tree"}</dd><dt className="text-faint">Commit / manifest</dt><dd className="break-all font-mono text-ink">{snapshot.commit_oid || snapshot.manifest_hash}</dd><dt className="text-faint">Working tree</dt><dd className="text-ink">{snapshot.dirty ? "Dirty, content-addressed manifest" : "Clean"}</dd><dt className="text-faint">Resolution reason</dt><dd className="text-ink">{snapshot.resolution_reason}</dd></dl></div><details className={`${CARD} p-3`}><summary className="cursor-pointer text-[10.5px] font-medium text-ink">Snapshot manifest metadata</summary><pre className="mt-2 max-h-96 overflow-auto whitespace-pre-wrap text-[9.5px] text-muted">{JSON.stringify(snapshot, null, 2)}</pre></details></section>;
  }
  const strategy = value as ExecutionStrategyV2;
  return <section aria-label="Immutable execution strategy" className="space-y-3"><div className="flex flex-wrap items-center gap-2"><StatusBadge status="pass" label={strategy.template_id} /><span className="text-[10.5px] text-muted">{strategy.archetype.replace(/_/g, " ")} / max repairs {strategy.max_repair_attempts}</span><code className="ml-auto break-all text-[9px] text-faint">v{strategy.version} {strategy.content_hash}</code></div><div className="space-y-2">{strategy.nodes.map((item, index) => <article key={String(item.key || index)} className={`${CARD} p-3`}><div className="text-[10.5px] font-medium text-ink">{index + 1}. {String(item.title || item.key || "Node")}</div><div className="mt-1 text-[9.5px] text-muted">{String(item.role || item.agent || item.kind || "service")} / {String(item.model || item.provider || "policy-selected")}</div><div className="mt-1 text-[9px] text-faint">Schema {String(item.result_schema_id || "service-owned")}</div></article>)}</div><div className="grid gap-3 lg:grid-cols-2"><details className={`${CARD} p-3`} open><summary className="cursor-pointer text-[10.5px] font-medium text-ink">Effective policy and provenance</summary><pre className="mt-2 max-h-80 overflow-auto whitespace-pre-wrap text-[9.5px] text-muted">{JSON.stringify({ effective_policy: strategy.effective_policy, provenance: strategy.policy_provenance }, null, 2)}</pre></details><details className={`${CARD} p-3`} open><summary className="cursor-pointer text-[10.5px] font-medium text-ink">Budget profile and admission</summary><pre className="mt-2 max-h-80 overflow-auto whitespace-pre-wrap text-[9.5px] text-muted">{JSON.stringify(strategy.budget_profile, null, 2)}</pre></details></div></section>;
}
