import { useCallback, useEffect, useState } from "react";
import type { OrchestrationApi } from "./api";
import { BUTTON, CARD, EmptyState, ErrorNotice, LoadingBlock, StatusBadge } from "./ui";

type View = "coverage" | "claims" | "files";
type Row = Record<string, unknown>;

const asRecord = (value: unknown): Row => value && typeof value === "object" && !Array.isArray(value) ? value as Row : {};
const asRows = (value: unknown): Row[] => Array.isArray(value) ? value.map(asRecord) : [];
const asStrings = (value: unknown): string[] => Array.isArray(value) ? value.map(String) : [];
const pageData = (value: unknown, key: string): { items: Row[]; nextCursor: string | null } => {
  const root = asRecord(value);
  const nested = asRecord(root[key]);
  return {
    items: asRows(nested.items || root[key]),
    nextCursor: String(nested.next_cursor || root.next_cursor || "") || null,
  };
};

export function EvidenceExplorer({ api, taskId }: { api: OrchestrationApi; taskId: string }) {
  const [view, setView] = useState<View>("coverage");
  const [coverage, setCoverage] = useState<Row[]>([]);
  const [claims, setClaims] = useState<Row[]>([]);
  const [evidence, setEvidence] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [paging, setPaging] = useState(false);
  const [cursors, setCursors] = useState<Record<View, string | null>>({ coverage: null, claims: null, files: null });
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [coverageValue, claimsValue, evidenceValue] = await Promise.all([
        api.getTaskQualityCoverage(taskId),
        api.getTaskQualityClaims(taskId),
        api.getTaskQualityEvidence(taskId),
      ]);
      const coveragePage = pageData(coverageValue, "coverage");
      const claimsPage = pageData(claimsValue, "claims");
      const evidencePage = pageData(evidenceValue, "evidence");
      setCoverage(coveragePage.items);
      setClaims(claimsPage.items);
      setEvidence(evidencePage.items);
      setCursors({ coverage: coveragePage.nextCursor, claims: claimsPage.nextCursor, files: evidencePage.nextCursor });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Evidence could not be loaded.");
    } finally {
      setLoading(false);
    }
  }, [api, taskId]);

  const loadMore = useCallback(async () => {
    const cursor = cursors[view];
    if (!cursor) return;
    setPaging(true);
    setError(null);
    try {
      const value = view === "coverage"
        ? await api.getTaskQualityCoverage(taskId, 0, 200, cursor)
        : view === "claims"
          ? await api.getTaskQualityClaims(taskId, 0, 200, cursor)
          : await api.getTaskQualityEvidence(taskId, 0, 200, cursor);
      const key = view === "files" ? "evidence" : view;
      const page = pageData(value, key);
      if (view === "coverage") setCoverage((current) => [...current, ...page.items]);
      else if (view === "claims") setClaims((current) => [...current, ...page.items]);
      else setEvidence((current) => [...current, ...page.items]);
      setCursors((current) => ({ ...current, [view]: page.nextCursor }));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "The next evidence page could not be loaded.");
    } finally {
      setPaging(false);
    }
  }, [api, cursors, taskId, view]);

  useEffect(() => { void load(); }, [load]);
  if (loading) return <LoadingBlock label="Loading canonical evidence..." />;
  if (error) return <ErrorNotice message={error} onRetry={() => void load()} />;

  return (
    <section aria-label="Evidence explorer" data-testid="evidence-explorer">
      <div className="mb-3 flex flex-wrap gap-2" role="tablist" aria-label="Evidence views">
        {(["coverage", "claims", "files"] as View[]).map((item) => (
          <button key={item} type="button" role="tab" aria-selected={view === item} className={view === item ? `${BUTTON} border-accent bg-accentSoft text-accent` : BUTTON} onClick={() => setView(item)}>
            {item === "files" ? "Files & citations" : item[0].toUpperCase() + item.slice(1)}
          </button>
        ))}
        <button type="button" className={`${BUTTON} ml-auto`} onClick={() => void load()}>Refresh evidence</button>
      </div>

      {view === "coverage" && (coverage.length ? (
        <div className="space-y-2">
          {coverage.map((item, index) => (
            <article key={String(item.id || index)} className={`${CARD} p-3`}>
              <div className="flex flex-wrap items-center gap-2"><span className="font-mono text-[10px] text-faint">{String(item.requirement_id || item.area || "requirement")}</span><StatusBadge status={String(item.status || "unknown")} /><span className="ml-auto text-[10px] text-muted">{Number(item.evidence_count || 0)} evidence refs</span></div>
              <div className="mt-1 text-[11px] text-ink">{String(item.area || item.notes || "Coverage result")}</div>
              {!!asStrings(item.claim_ids).length && <div className="mt-1 text-[9.5px] text-faint">Claims: {asStrings(item.claim_ids).join(", ")}</div>}
            </article>
          ))}
        </div>
      ) : <EmptyState title="No coverage results" detail="Coverage appears after artifact validation begins." />)}

      {view === "claims" && (claims.length ? (
        <div className="space-y-2">
          {claims.map((item, index) => (
            <article key={String(item.id || index)} className={`${CARD} p-3`}>
              <div className="flex flex-wrap items-center gap-2"><StatusBadge status={String(item.validator_status || item.status || "pending")} /><span className="text-[9.5px] uppercase tracking-wide text-faint">{String(item.claim_type || item.type || "claim")}</span><span className="ml-auto text-[10px] text-muted">confidence {Math.round(Number(item.confidence || 0) * 100)}%</span></div>
              <p className="mt-1 text-[11px] leading-relaxed text-ink">{String(item.text || item.claim_text || "")}</p>
              <div className="mt-1 text-[9.5px] text-faint">{String(item.id || "")} / {Number(item.evidence_count || 0)} evidence refs</div>
              {Boolean(item.negative_search_query || item.search_query) && <pre className="mt-2 overflow-auto whitespace-pre-wrap rounded-md bg-paper p-2 text-[9.5px] text-muted">{JSON.stringify({ query: item.negative_search_query || item.search_query, scope: item.search_scope || item.scope, exclusions: item.exclusions, snapshot_hash: item.snapshot_hash }, null, 2)}</pre>}
            </article>
          ))}
        </div>
      ) : <EmptyState title="No claims" detail="Claims are persisted by the synthesis stage." />)}

      {view === "files" && (evidence.length ? (
        <div className="overflow-x-auto rounded-xl border border-line">
          <table className="w-full min-w-[720px] text-left text-[10.5px]">
            <thead className="bg-paper text-faint"><tr><th className="px-3 py-2">Snapshot path</th><th className="px-3 py-2">Lines</th><th className="px-3 py-2">Claim</th><th className="px-3 py-2">Blob / subject hash</th></tr></thead>
            <tbody>{evidence.map((item, index) => <tr key={String(item.id || index)} className="border-t border-line"><td className="max-w-xs break-all px-3 py-2 font-mono text-ink">{String(item.path || item.uri || "")}</td><td className="px-3 py-2 text-muted">{String(item.start_line || "?")}-{String(item.end_line || "?")}</td><td className="max-w-sm px-3 py-2 text-muted">{String(item.claim_text || item.claim_id || "")}</td><td className="max-w-xs break-all px-3 py-2 font-mono text-[9px] text-faint">{String(item.blob_hash || item.subject_hash || item.snapshot_hash || "")}</td></tr>)}</tbody>
          </table>
        </div>
      ) : <EmptyState title="No file citations" detail="Resolved citations appear here with their frozen snapshot coordinates." />)}
      {cursors[view] && <div className="mt-3 flex justify-center"><button type="button" className={BUTTON} disabled={paging} onClick={() => void loadMore()}>{paging ? "Loading page..." : "Load more"}</button></div>}
    </section>
  );
}
