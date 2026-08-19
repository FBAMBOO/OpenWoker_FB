import { INPUT, PRIMARY_BUTTON } from "./ui";

export interface QualityGoalValue {
  title: string;
  objective: string;
  workspace: string;
  sourceWorkspaceWrite: boolean;
  network: boolean;
  qualityProfile: "quality-first" | "balanced" | "custom";
}

export function GoalStep({
  value,
  busy,
  onChange,
  onAnalyze,
}: {
  value: QualityGoalValue;
  busy: boolean;
  onChange: (value: QualityGoalValue) => void;
  onAnalyze: () => void;
}) {
  const update = <K extends keyof QualityGoalValue>(key: K, next: QualityGoalValue[K]) =>
    onChange({ ...value, [key]: next });
  const valid = Boolean(value.objective.trim() && value.workspace.trim());
  return (
    <section aria-label="Goal step" data-testid="quality-goal-step" className="space-y-3">
      <label className="block text-[11px] text-muted">
        Title
        <input className={`${INPUT} mt-1 w-full`} value={value.title} onChange={(event) => update("title", event.target.value)} placeholder="Generated from the objective when empty" />
      </label>
      <label className="block text-[11px] text-muted">
        Objective
        <textarea className={`${INPUT} mt-1 min-h-36 w-full resize-y`} required value={value.objective} onChange={(event) => update("objective", event.target.value)} placeholder="Describe the exact outcome, scope, evidence and limitations you expect." />
      </label>
      <label className="block text-[11px] text-muted">
        Workspace
        <input className={`${INPUT} mt-1 w-full font-mono`} required value={value.workspace} onChange={(event) => update("workspace", event.target.value)} placeholder="Canonical repository path" />
      </label>
      <div className="grid gap-3 sm:grid-cols-3">
        <label className="rounded-lg border border-line bg-panel px-3 py-2 text-[11px] text-ink">
          <span className="block font-medium">Source workspace</span>
          <select className={`${INPUT} mt-1 w-full`} value={value.sourceWorkspaceWrite ? "writable" : "read-only"} onChange={(event) => update("sourceWorkspaceWrite", event.target.value === "writable")}>
            <option value="read-only">Read-only</option>
            <option value="writable">Writable</option>
          </select>
        </label>
        <label className="rounded-lg border border-line bg-panel px-3 py-2 text-[11px] text-ink">
          <span className="block font-medium">External network</span>
          <select className={`${INPUT} mt-1 w-full`} value={value.network ? "on" : "off"} onChange={(event) => update("network", event.target.value === "on")}>
            <option value="off">Off</option>
            <option value="on">On</option>
          </select>
        </label>
        <label className="rounded-lg border border-line bg-panel px-3 py-2 text-[11px] text-ink">
          <span className="block font-medium">Quality profile</span>
          <select className={`${INPUT} mt-1 w-full`} value={value.qualityProfile} onChange={(event) => update("qualityProfile", event.target.value as QualityGoalValue["qualityProfile"])}>
            <option value="quality-first">Quality-first</option>
            <option value="balanced">Balanced</option>
            <option value="custom">Custom</option>
          </select>
        </label>
      </div>
      <div className="rounded-lg border border-line bg-paper px-3 py-2 text-[10.5px] text-muted">
        Read-only applies to the source workspace. The task artifact store remains writable so the requested report can be delivered as an immutable file.
      </div>
      <div className="flex justify-end">
        <button type="button" className={PRIMARY_BUTTON} disabled={!valid || busy} onClick={onAnalyze}>
          {busy ? "Analyzing goal…" : "Analyze goal"}
        </button>
      </div>
    </section>
  );
}
