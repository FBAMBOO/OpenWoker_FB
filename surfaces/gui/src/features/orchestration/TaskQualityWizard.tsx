import { useMemo, useState } from "react";
import { createClientIdempotencyKey, type OrchestrationApi } from "./api";
import { ContractPreviewStep } from "./ContractPreviewStep";
import { GoalStep, type QualityGoalValue } from "./GoalStep";
import { StrategyPreviewStep } from "./StrategyPreviewStep";
import { TargetResolverStep } from "./TargetResolverStep";
import type {
  ExecutionStrategyV2,
  RepositorySnapshotV2,
  TaskDraftAnalysisV2,
  TaskQualityContract,
} from "./types";
import { BUTTON, CARD, ErrorNotice, PRIMARY_BUTTON, SectionHead, StatusBadge } from "./ui";

type WizardStep = "goal" | "contract" | "target" | "strategy" | "publish";

const stepOrder: WizardStep[] = ["goal", "contract", "target", "strategy", "publish"];
const stepLabels: Record<WizardStep, string> = {
  goal: "Goal",
  contract: "Contract",
  target: "Target",
  strategy: "Strategy",
  publish: "Publish & Start",
};

const messageFor = (error: unknown) => error instanceof Error
  ? error.message
  : typeof error === "string"
    ? error
    : "Task Quality V2 could not complete this operation.";

export function TaskQualityWizard({
  api,
  initialWorkspace,
  onStarted,
  onCancel,
}: {
  api: OrchestrationApi;
  initialWorkspace?: string;
  onStarted: (taskId: string) => Promise<void> | void;
  onCancel: () => void;
}) {
  const [step, setStep] = useState<WizardStep>("goal");
  const [goal, setGoal] = useState<QualityGoalValue>({
    title: "",
    objective: "",
    workspace: initialWorkspace || "",
    sourceWorkspaceWrite: false,
    network: false,
    qualityProfile: "quality-first",
  });
  const [taskId, setTaskId] = useState("");
  const [promptHash, setPromptHash] = useState("");
  const [analysis, setAnalysis] = useState<TaskDraftAnalysisV2 | null>(null);
  const [contract, setContract] = useState<TaskQualityContract | null>(null);
  const [contractEtag, setContractEtag] = useState("");
  const [contractDirty, setContractDirty] = useState(false);
  const [snapshot, setSnapshot] = useState<RepositorySnapshotV2 | null>(null);
  const [strategy, setStrategy] = useState<ExecutionStrategyV2 | null>(null);
  const [selectedCandidateId, setSelectedCandidateId] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [draftKey] = useState(() => createClientIdempotencyKey("quality-draft"));
  const [analysisKey] = useState(() => createClientIdempotencyKey("quality-analysis"));

  const activeIndex = stepOrder.indexOf(step);
  const conflicts = useMemo(() => {
    const value = analysis as (TaskDraftAnalysisV2 & { contract_conflicts?: Array<Record<string, unknown>> }) | null;
    return value?.contract_conflicts || [];
  }, [analysis]);

  const run = async (operation: () => Promise<void>) => {
    setBusy(true);
    setError(null);
    try {
      await operation();
    } catch (caught) {
      setError(messageFor(caught));
    } finally {
      setBusy(false);
    }
  };

  const analyzeGoal = () => run(async () => {
    const draft = await api.createTaskQualityDraft({
      title: goal.title.trim() || undefined,
      objective: goal.objective,
      domain: "code",
      workspace: goal.workspace,
      read_only: !goal.sourceWorkspaceWrite,
      source_workspace_write: goal.sourceWorkspaceWrite,
      task_artifact_write: true,
      network: goal.network,
      quality_profile_id: goal.qualityProfile,
      input: { rollout_stage: "opt-in", created_by: "local-user" },
    }, draftKey);
    const result = await api.analyzeTaskQualityDraft(draft.task_id, {
      title: goal.title.trim() || undefined,
      quality_profile_id: goal.qualityProfile,
    }, analysisKey);
    setTaskId(draft.task_id);
    setPromptHash(draft.prompt_hash);
    setAnalysis(result);
    setContract(result.contract);
    setContractEtag(result.contract_etag || result.contract.content_hash);
    setSelectedCandidateId(String(result.target_resolution.recommended_candidate_id || ""));
    setStep("contract");
  });

  const saveContract = async (): Promise<TaskQualityContract | null> => {
    if (!taskId || !contract) return null;
    if (!contractDirty) return contract;
    const updated = await api.updateTaskQualityContract(taskId, contract, contractEtag);
    setContract(updated);
    setContractEtag(updated.content_hash);
    setContractDirty(false);
    return updated;
  };

  const publishContract = () => run(async () => {
    const current = await saveContract();
    if (!current) throw new Error("The analyzed contract is unavailable.");
    const published = await api.publishTaskQualityContract(taskId, current.content_hash);
    setContract(published);
    setContractEtag(published.content_hash);
    setContractDirty(false);
    setStep("target");
  });

  const freezeTarget = () => run(async () => {
    if (!taskId) throw new Error("The draft task identity is unavailable.");
    const frozen = await api.freezeTaskQualitySnapshot(taskId, {
      ...(selectedCandidateId ? { candidate_id: selectedCandidateId } : {}),
    });
    setSnapshot(frozen);
    const generated = await api.generateTaskQualityStrategy(taskId);
    setStrategy(generated);
    setStep("strategy");
  });

  const start = () => run(async () => {
    if (!taskId) throw new Error("The draft task identity is unavailable.");
    const started = await api.startTaskQualityDraft(taskId);
    await onStarted(started.id);
  });

  return (
    <section aria-label="Task Quality V2 creation wizard" data-testid="task-quality-wizard">
      <div className="mb-4 flex flex-wrap items-start gap-3">
        <div className="min-w-0 flex-1">
          <SectionHead title="Create Task - Task Quality V2" />
          <p className="text-[11px] text-muted">Analyze the goal, bind immutable contract and target versions, then review the admitted strategy before execution.</p>
        </div>
        <StatusBadge status="pending" label="Opt-in" />
      </div>
      <ol className="mb-5 grid gap-1.5 sm:grid-cols-5" aria-label="Creation progress">
        {stepOrder.map((item, index) => (
          <li key={item} className={`rounded-lg border px-2.5 py-2 text-[10.5px] ${index === activeIndex ? "border-accent bg-accentSoft text-accent" : index < activeIndex ? "border-okLine bg-okSoft text-ok" : "border-line bg-paper text-faint"}`}>
            <span className="mr-1 font-semibold">{index + 1}.</span>{stepLabels[item]}
          </li>
        ))}
      </ol>

      {error && <div className="mb-4"><ErrorNotice message={error} /></div>}
      {step === "goal" && <GoalStep value={goal} busy={busy} onChange={setGoal} onAnalyze={analyzeGoal} />}
      {step === "contract" && contract && (
        <ContractPreviewStep
          contract={contract}
          conflicts={conflicts}
          busy={busy}
          dirty={contractDirty}
          onRequirementChange={(id, text) => {
            setContract({ ...contract, requirements: contract.requirements.map((item) => item.id === id ? { ...item, text } : item) });
            setContractDirty(true);
          }}
          onSave={() => void run(async () => { await saveContract(); })}
          onContinue={publishContract}
        />
      )}
      {step === "target" && analysis && (
        <TargetResolverStep
          resolution={analysis.target_resolution}
          selectedCandidateId={selectedCandidateId}
          busy={busy}
          onSelect={setSelectedCandidateId}
          onFreeze={freezeTarget}
        />
      )}
      {step === "strategy" && strategy && snapshot && contract && (
        <StrategyPreviewStep strategy={strategy} snapshot={snapshot} contract={contract} busy={busy} onContinue={() => setStep("publish")} />
      )}
      {step === "publish" && strategy && snapshot && contract && (
        <section aria-label="Publish and start" className="space-y-4">
          <div className={`${CARD} p-4`}>
            <h3 className="mb-3 text-[12px] font-semibold text-ink">Immutable execution envelope</h3>
            <dl className="grid gap-2 text-[10.5px] sm:grid-cols-[11rem_1fr]">
              <dt className="text-faint">Original objective hash</dt><dd className="break-all font-mono text-ink">{promptHash}</dd>
              <dt className="text-faint">Contract</dt><dd className="break-all font-mono text-ink">v{contract.version} / {contract.content_hash}</dd>
              <dt className="text-faint">Snapshot</dt><dd className="break-all font-mono text-ink">{snapshot.id} / {snapshot.commit_oid || snapshot.manifest_hash}</dd>
              <dt className="text-faint">Strategy</dt><dd className="break-all font-mono text-ink">v{strategy.version} / {strategy.content_hash}</dd>
              <dt className="text-faint">Quality profile</dt><dd className="text-ink">{contract.quality_profile_id}</dd>
              <dt className="text-faint">Effective budget</dt><dd className="text-ink">{String(strategy.budget_profile.mode || "hard")} / {JSON.stringify(strategy.budget_profile.limits || strategy.budget_profile)}</dd>
              <dt className="text-faint">Permission ceiling</dt><dd className="text-ink">Source {goal.sourceWorkspaceWrite ? "writable" : "read-only"}; artifact store writable; network {goal.network ? "on" : "off"}</dd>
              <dt className="text-faint">Primary deliverable</dt><dd className="text-ink">{contract.deliverables.find((item) => item.primary)?.filename || "Missing"}</dd>
            </dl>
          </div>
          <div className="rounded-lg border border-warnInk/20 bg-warnSoft px-3 py-2 text-[10.5px] text-warnInk">Starting binds these exact versions. Later changes create new versions and require replanning.</div>
          <div className="flex justify-end gap-2">
            <button type="button" className={BUTTON} disabled={busy} onClick={() => setStep("strategy")}>Back</button>
            <button type="button" className={PRIMARY_BUTTON} disabled={busy} onClick={start}>{busy ? "Starting..." : "Publish & Start"}</button>
          </div>
        </section>
      )}
      <div className="mt-5 border-t border-line pt-3">
        <button type="button" className={BUTTON} disabled={busy} onClick={onCancel}>Cancel</button>
        {taskId && <code className="ml-3 text-[9.5px] text-faint">Draft {taskId}</code>}
      </div>
    </section>
  );
}
