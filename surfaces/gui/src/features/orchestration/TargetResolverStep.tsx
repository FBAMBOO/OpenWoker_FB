import { CARD, PRIMARY_BUTTON, StatusBadge } from "./ui";

const record = (value: unknown): Record<string, unknown> => value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const rows = (value: unknown) => Array.isArray(value) ? value.map(record) : [];
const shortOid = (value: unknown) => String(value || "unavailable").slice(0, 12);

export function TargetResolverStep({
  resolution,
  selectedCandidateId,
  busy,
  onSelect,
  onFreeze,
}: {
  resolution: Record<string, unknown>;
  selectedCandidateId: string;
  busy: boolean;
  onSelect: (id: string) => void;
  onFreeze: () => void;
}) {
  const candidates = rows(resolution.candidates);
  const recommended = String(resolution.recommended_candidate_id || "");
  const selected = selectedCandidateId || recommended;
  return (
    <section aria-label="Target resolver step" data-testid="quality-target-step" className="space-y-3">
      <div className="flex items-center gap-2"><StatusBadge status={String(resolution.status || "pending")} /><span className="text-[10.5px] text-muted">Confidence {Math.round(Number(resolution.resolution_confidence || 0) * 100)}% · {String(resolution.resolution_reason || "")}</span></div>
      <div className="space-y-2">
        {candidates.map((candidate) => {
          const id = String(candidate.id || "");
          const active = selected === id;
          return <button type="button" key={id} className={`${CARD} w-full p-3 text-left ${active ? "border-accent ring-1 ring-accent" : ""}`} onClick={() => onSelect(id)}>
            <div className="flex items-center gap-2"><span className="min-w-0 flex-1 truncate text-[11.5px] font-medium text-ink">{String(candidate.project_root || candidate.repo_root || id)}</span>{id === recommended && <StatusBadge status="pass" label="Recommended" />}</div>
            <div className="mt-1 grid gap-1 text-[10px] text-muted sm:grid-cols-2 lg:grid-cols-4">
              <span>HEAD {String(candidate.current_branch || "detached")} @ {shortOid(candidate.head_oid)}</span>
              <span>Default {String(candidate.default_ref || "unavailable")} @ {shortOid(candidate.default_oid)}</span>
              <span>Ahead {candidate.ahead == null ? "unknown" : Number(candidate.ahead)} · behind {candidate.behind == null ? "unknown" : Number(candidate.behind)}</span>
              <span>{Number(candidate.file_count || 0).toLocaleString()} files · {Number(candidate.total_bytes || 0).toLocaleString()} bytes</span>
            </div>
            <div className="mt-1 text-[9.5px] text-faint">{candidate.dirty ? "Dirty working tree" : "Clean"} · {Number(candidate.worktree_count || 0)} worktrees · score {Number(candidate.score || 0)}</div>
            <div className="mt-1 text-[9.5px] text-muted">Recommended {String(candidate.recommended_ref || candidate.recommended_snapshot_kind || "working tree")} · {String(candidate.recommendation_reason || "No recommendation rationale")}</div>
          </button>;
        })}
      </div>
      <div className="rounded-lg border border-line bg-paper px-3 py-2 text-[10.5px] text-muted">Freezing is offline by default. The selected ref resolves to an immutable SHA and manifest; later ref movement does not change this run.</div>
      <div className="flex justify-end"><button type="button" className={PRIMARY_BUTTON} disabled={!selected || busy} onClick={onFreeze}>{busy ? "Freezing snapshot…" : "Freeze target & continue"}</button></div>
    </section>
  );
}
