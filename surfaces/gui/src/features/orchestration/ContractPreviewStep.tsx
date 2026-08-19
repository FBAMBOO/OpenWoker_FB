import type { TaskQualityContract } from "./types";
import { BUTTON, CARD, PRIMARY_BUTTON, StatusBadge } from "./ui";

export function ContractPreviewStep({
  contract,
  conflicts,
  busy,
  dirty,
  onRequirementChange,
  onSave,
  onContinue,
}: {
  contract: TaskQualityContract;
  conflicts: Array<Record<string, unknown>>;
  busy: boolean;
  dirty: boolean;
  onRequirementChange: (id: string, text: string) => void;
  onSave: () => void;
  onContinue: () => void;
}) {
  const required = contract.requirements.filter((item) => item.required);
  const complete = required.every((item) => item.text.trim()) && contract.deliverables.some((item) => item.required && item.primary) && conflicts.length === 0;
  return (
    <section aria-label="Contract preview step" data-testid="quality-contract-step" className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge status={complete ? "pass" : "fail"} label={complete ? "Contract complete" : "Contract incomplete"} />
        <span className="text-[10.5px] text-muted">{required.length} required requirements · {contract.archetype.replace(/_/g, " ")}</span>
        <code className="ml-auto break-all text-[9.5px] text-faint">{contract.content_hash}</code>
      </div>
      {conflicts.length > 0 && (
        <div role="alert" className="rounded-lg border border-danger/20 bg-dangerSoft px-3 py-2 text-[11px] text-danger">
          <div>{conflicts.length} semantic conflict{conflicts.length === 1 ? "" : "s"} must be resolved before publishing.</div>
          <ul className="mt-1 list-disc space-y-0.5 pl-4">
            {conflicts.map((item, index) => {
              const location = String(item.requirement_id || item.field || item.path || item.code || `conflict-${index + 1}`);
              const message = String(item.message || item.detail || item.reason || "Contract policy conflict");
              return <li key={`${location}-${index}`}><code className="font-mono">{location}</code>: {message}</li>;
            })}
          </ul>
        </div>
      )}
      <div className="grid gap-3 lg:grid-cols-2">
        <section className={`${CARD} p-3`}>
          <h3 className="mb-2 text-[11px] font-semibold text-ink">Requirements</h3>
          <div className="space-y-2">
            {contract.requirements.map((item) => (
              <article key={item.id} className="rounded-lg border border-line bg-paper p-2.5">
                <div className="mb-1 flex flex-wrap items-center gap-1.5 text-[9.5px] uppercase tracking-wide text-faint">
                  <span>{item.category}</span><span>·</span><span>{item.source}</span>
                  {item.hard_gate && <StatusBadge status="pending" label="Hard gate" />}
                  {item.confidence != null && <span className="ml-auto">{Math.round(item.confidence * 100)}% confidence</span>}
                </div>
                {item.source === "inferred" && !item.hard_gate ? (
                  <textarea aria-label={`Requirement ${item.id}`} className="min-h-14 w-full resize-y rounded-md border border-line bg-panel px-2 py-1.5 text-[11px] text-ink" value={item.text} onChange={(event) => onRequirementChange(item.id, event.target.value)} />
                ) : <p className="text-[11px] leading-relaxed text-ink">{item.text}</p>}
                <div className="mt-1 text-[9.5px] text-muted">Verify with {item.verification_method.replace(/_/g, " ")}</div>
              </article>
            ))}
          </div>
        </section>
        <div className="space-y-3">
          <section className={`${CARD} p-3`}>
            <h3 className="mb-2 text-[11px] font-semibold text-ink">Coverage matrix</h3>
            <div className="flex flex-wrap gap-1.5">
              {[...new Set(contract.requirements.map((item) => item.category))].map((category) => <span key={category} className="rounded-full bg-accentSoft px-2 py-1 text-[10px] text-accent">{category}</span>)}
            </div>
          </section>
          <section className={`${CARD} p-3`}>
            <h3 className="mb-2 text-[11px] font-semibold text-ink">Deliverables</h3>
            {contract.deliverables.map((item) => <div key={item.id} className="mb-2 rounded-lg border border-line bg-paper p-2 text-[10.5px]"><div className="font-medium text-ink">{item.filename}{item.primary ? " · primary" : ""}</div><div className="text-muted">{item.mime_type} · {item.result_schema_id}</div><div className="mt-1 text-faint">{item.required_sections.join(" · ")}</div></div>)}
          </section>
          <section className={`${CARD} p-3`}>
            <h3 className="mb-2 text-[11px] font-semibold text-ink">Constraints & non-goals</h3>
            <pre className="max-h-44 overflow-auto whitespace-pre-wrap text-[10px] text-muted">{JSON.stringify({ constraints: contract.constraints, non_goals: contract.non_goals }, null, 2)}</pre>
          </section>
        </div>
      </div>
      <div className="flex justify-end gap-2">
        <button type="button" className={BUTTON} disabled={!dirty || busy} onClick={onSave}>{busy ? "Saving…" : "Save draft"}</button>
        <button type="button" className={PRIMARY_BUTTON} disabled={!complete || busy} onClick={onContinue}>{busy ? "Publishing…" : "Publish contract & continue"}</button>
      </div>
    </section>
  );
}
