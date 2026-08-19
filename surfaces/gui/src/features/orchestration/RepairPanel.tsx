import { useEffect, useMemo, useState } from "react";
import type { OrchestrationApi } from "./api";
import type { OrchestrationTaskDetail, QualityFinding } from "./types";
import { BUTTON, CARD, ErrorNotice, INPUT, PRIMARY_BUTTON, StatusBadge } from "./ui";

type Row = Record<string, unknown>;

export function RepairPanel({
  api,
  task,
  findings,
  gates,
  onChanged,
}: {
  api: OrchestrationApi;
  task: OrchestrationTaskDetail;
  findings: QualityFinding[];
  gates: Row[];
  onChanged: () => Promise<void> | void;
}) {
  const repairable = useMemo(() => findings.filter((item) => item.status === "open" && item.repairable), [findings]);
  const [selected, setSelected] = useState<string[]>(() => repairable.map((item) => item.id));
  const [busy, setBusy] = useState<"repair" | "waiver" | "">("");
  const [error, setError] = useState<string | null>(null);
  const [waiverSubject, setWaiverSubject] = useState("");
  const [reason, setReason] = useState("");
  const [reference, setReference] = useState("");

  useEffect(() => { setSelected(repairable.map((item) => item.id)); }, [repairable]);
  const primary = task.primary_deliverable;
  const waiverCandidates = [
    ...gates.filter((item) => String(item.status) === "fail").map((item) => ({ type: "gate_result", id: String(item.id), label: `Gate: ${String(item.gate_id || item.validator_id || item.id)}`, version: Number(item.version || 1) })),
    ...findings.filter((item) => item.status === "open").map((item) => ({ type: "finding", id: item.id, label: `Finding: ${item.category} - ${item.message}`, version: Number(item.version || 1) })),
  ];

  const requestRepair = async () => {
    if (!primary || !selected.length) return;
    setBusy("repair");
    setError(null);
    try {
      await api.requestTaskRepair(task.id, {
        source_artifact_id: primary.artifact_id,
        finding_ids: selected,
        budget_available: task.budget_status !== "exhausted",
      });
      await onChanged();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Repair could not be requested.");
    } finally {
      setBusy("");
    }
  };

  const createWaiver = async () => {
    const subject = waiverCandidates.find((item) => `${item.type}:${item.id}` === waiverSubject);
    if (!primary || !subject) return;
    setBusy("waiver");
    setError(null);
    try {
      await api.createTaskQualityWaiver(task.id, {
        artifact_id: primary.artifact_id,
        subject_type: subject.type,
        subject_id: subject.id,
        subject_version: subject.version,
        reason: reason.trim(),
        reference: reference.trim() || undefined,
      });
      setReason("");
      setReference("");
      await onChanged();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Waiver could not be created.");
    } finally {
      setBusy("");
    }
  };

  return (
    <section aria-label="Repair and waiver controls" data-testid="repair-panel" className="space-y-3">
      {error && <ErrorNotice message={error} />}
      <div className={`${CARD} p-3`}>
        <div className="flex flex-wrap items-center gap-2"><h3 className="text-[11px] font-semibold text-ink">Bounded repair request</h3><StatusBadge status={repairable.length ? "pending" : "pass"} label={`${repairable.length} repairable open`} /><span className="ml-auto text-[9.5px] text-faint">Manual only; auto-repair remains off</span></div>
        {repairable.length ? <div className="mt-2 space-y-1.5">{repairable.map((item) => <label key={item.id} className="flex items-start gap-2 rounded-md border border-line bg-paper px-2.5 py-2 text-[10.5px]"><input type="checkbox" className="mt-0.5" checked={selected.includes(item.id)} onChange={(event) => setSelected((current) => event.target.checked ? [...new Set([...current, item.id])] : current.filter((id) => id !== item.id))} /><span><span className="font-medium text-ink">{item.category}</span>{item.section_id && <span className="text-muted"> / section {item.section_id}</span>}<span className="block text-muted">{item.suggested_fix || item.message}</span></span></label>)}</div> : <p className="mt-2 text-[10.5px] text-muted">There are no open repairable findings for the immutable source artifact.</p>}
        <div className="mt-3 flex justify-end"><button type="button" className={PRIMARY_BUTTON} disabled={!primary || !selected.length || Boolean(busy) || task.budget_status === "exhausted"} onClick={() => void requestRepair()}>{busy === "repair" ? "Requesting repair..." : "Request repair"}</button></div>
      </div>

      <details className={`${CARD} p-3`}>
        <summary className="cursor-pointer text-[11px] font-semibold text-ink">Authorized quality waiver</summary>
        <p className="mt-2 text-[10px] text-warnInk">Waivers are signed audit records, never silent gate deletion. The authenticated local operator identity is recorded by the server.</p>
        <div className="mt-3 grid gap-2 sm:grid-cols-2">
          <label className="text-[10px] text-muted">Failed subject<select className={`${INPUT} mt-1 w-full`} value={waiverSubject} onChange={(event) => setWaiverSubject(event.target.value)}><option value="">Select failed gate or finding</option>{waiverCandidates.map((item) => <option key={`${item.type}:${item.id}`} value={`${item.type}:${item.id}`}>{item.label}</option>)}</select></label>
          <label className="text-[10px] text-muted">Ticket / reference<input className={`${INPUT} mt-1 w-full`} value={reference} onChange={(event) => setReference(event.target.value)} /></label>
          <label className="text-[10px] text-muted sm:col-span-2">Reason<textarea className={`${INPUT} mt-1 min-h-20 w-full resize-y`} value={reason} onChange={(event) => setReason(event.target.value)} /></label>
        </div>
        <div className="mt-3 flex justify-end"><button type="button" className={BUTTON} disabled={!primary || !waiverSubject || !reason.trim() || Boolean(busy)} onClick={() => void createWaiver()}>{busy === "waiver" ? "Signing waiver..." : "Create signed waiver"}</button></div>
      </details>
    </section>
  );
}
