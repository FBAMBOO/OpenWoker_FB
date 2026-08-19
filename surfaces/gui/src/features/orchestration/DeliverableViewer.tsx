import { useCallback, useEffect, useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type { ApiDownload, OrchestrationApi } from "./api";
import { BUTTON, CARD, EmptyState, ErrorNotice, LoadingBlock, StatusBadge } from "./ui";

type Artifact = Record<string, unknown>;
const CHUNK_BYTES = 64 * 1024;
const asRecord = (value: unknown): Artifact => value && typeof value === "object" && !Array.isArray(value) ? value as Artifact : {};
const asRows = (value: unknown): Artifact[] => Array.isArray(value) ? value.map(asRecord) : [];
const mergeArtifacts = (current: Artifact[], incoming: Artifact[]): Artifact[] => {
  const seen = new Set(current.map((item) => String(item.id)));
  return [...current, ...incoming.filter((item) => !seen.has(String(item.id)))];
};

export function DeliverableViewer({ api, apiDownload, taskId }: { api: OrchestrationApi; apiDownload: ApiDownload; taskId: string }) {
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [metadata, setMetadata] = useState<Artifact | null>(null);
  const [mode, setMode] = useState<"rendered" | "raw">("rendered");
  const [offset, setOffset] = useState(0);
  const [content, setContent] = useState("");
  const [contentLoading, setContentLoading] = useState(false);
  const [baseId, setBaseId] = useState("");
  const [diff, setDiff] = useState("");
  const [loading, setLoading] = useState(true);
  const [paging, setPaging] = useState(false);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const value = asRecord(await api.getTaskDeliverables(taskId));
      const page = asRecord(value.deliverables);
      let rows = asRows(page.items || value.deliverables);
      const primary = asRecord(value.primary_deliverable);
      const primaryId = String(value.primary_artifact_id || primary.artifact_id || "");
      if (primaryId && !rows.some((item) => String(item.id) === primaryId)) {
        rows = mergeArtifacts(rows, [{ ...primary, id: primaryId, logical_deliverable_id: primary.deliverable_id, is_primary: true }]);
      }
      setArtifacts(rows);
      setNextCursor(String(page.next_cursor || "") || null);
      setSelectedId((current) => current || primaryId || String(rows.find((item) => Boolean(item.is_primary))?.id || rows[0]?.id || ""));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Deliverables could not be loaded.");
    } finally {
      setLoading(false);
    }
  }, [api, taskId]);

  const loadMore = useCallback(async () => {
    if (!nextCursor) return;
    setPaging(true);
    setError(null);
    try {
      const value = asRecord(await api.getTaskDeliverables(taskId, 0, 200, nextCursor));
      const page = asRecord(value.deliverables);
      setArtifacts((current) => mergeArtifacts(current, asRows(page.items)));
      setNextCursor(String(page.next_cursor || "") || null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "The next deliverable page could not be loaded.");
    } finally {
      setPaging(false);
    }
  }, [api, nextCursor, taskId]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!selectedId) { setMetadata(null); return; }
    let active = true;
    setOffset(0);
    setContent("");
    setDiff("");
    setBaseId("");
    void api.getArtifactMetadata(selectedId).then((value) => { if (active) setMetadata(asRecord(value)); }).catch((caught) => { if (active) setError(caught instanceof Error ? caught.message : "Artifact metadata could not be loaded."); });
    return () => { active = false; };
  }, [api, selectedId]);

  const totalBytes = Number(metadata?.byte_size || 0);
  const mimeType = String(metadata?.mime_type || "application/octet-stream");
  const previewPolicy = asRecord(metadata?.preview_policy);
  const previewable = Boolean(previewPolicy.inline ?? previewPolicy.inline_preview_allowed);
  const end = Math.min(totalBytes, offset + CHUNK_BYTES) - 1;
  const selected = useMemo(() => artifacts.find((item) => String(item.id) === selectedId), [artifacts, selectedId]);
  const olderVersions = useMemo(() => artifacts.filter((item) => item.logical_deliverable_id === selected?.logical_deliverable_id && String(item.id) !== selectedId), [artifacts, selected, selectedId]);

  useEffect(() => {
    if (!selectedId || !metadata || !previewable || end < offset) return;
    let active = true;
    setContentLoading(true);
    void api.getArtifactContentRange(selectedId, offset, end)
      .then((value) => { if (active) setContent(value); })
      .catch((caught) => { if (active) setError(caught instanceof Error ? caught.message : "Artifact content could not be read."); })
      .finally(() => { if (active) setContentLoading(false); });
    return () => { active = false; };
  }, [api, end, metadata, offset, previewable, selectedId]);

  if (loading) return <LoadingBlock label="Loading immutable deliverables..." />;
  if (error && !artifacts.length) return <ErrorNotice message={error} onRetry={() => void load()} />;
  if (!artifacts.length) return <EmptyState title="No deliverable artifact" detail="The declared deliverable appears here after the producer uploads an immutable artifact version." />;

  return (
    <section aria-label="Deliverable viewer" data-testid="deliverable-viewer" className="space-y-3">
      {error && <ErrorNotice message={error} />}
      <div className="flex flex-wrap gap-2">
        <label className="text-[10.5px] text-muted">Artifact version
          <select className="ml-2 rounded-md border border-line bg-panel px-2 py-1 text-ink" value={selectedId} onChange={(event) => setSelectedId(event.target.value)}>
            {artifacts.map((item) => <option key={String(item.id)} value={String(item.id)}>v{String(item.version || "?")} - {String(item.filename || item.id)}{item.is_primary ? " (primary)" : ""}</option>)}
          </select>
        </label>
        {metadata && <StatusBadge status={String(metadata.status || "unknown")} />}
        {nextCursor && <button type="button" className={BUTTON} disabled={paging} onClick={() => void loadMore()}>{paging ? "Loading versions..." : "Load more versions"}</button>}
        <button type="button" className={`${BUTTON} ml-auto`} onClick={() => void apiDownload(`/v1/orchestration/artifacts/${encodeURIComponent(selectedId)}/download`, String(metadata?.filename || "artifact"))}>Download exact artifact</button>
      </div>
      {metadata && (
        <div className={`${CARD} grid gap-2 p-3 text-[10px] sm:grid-cols-2 lg:grid-cols-4`}>
          <div><span className="block text-faint">Filename / MIME</span><span className="break-all text-ink">{String(metadata.filename)} / {mimeType}</span></div>
          <div><span className="block text-faint">Bytes / version</span><span className="text-ink">{totalBytes.toLocaleString()} / v{String(metadata.version)}</span></div>
          <div><span className="block text-faint">SHA-256 subject</span><span className="break-all font-mono text-[9px] text-ink">{String(metadata.sha256 || "pending")}</span></div>
          <div><span className="block text-faint">Reviewer read coverage</span><span className="text-ink">{Math.round(Number(metadata.max_read_coverage_ratio || 0) * 100)}%</span></div>
        </div>
      )}
      {previewable ? (
        <div className={`${CARD} overflow-hidden`}>
          <div className="flex flex-wrap items-center gap-2 border-b border-line bg-paper px-3 py-2">
            <button type="button" className={mode === "rendered" ? `${BUTTON} border-accent text-accent` : BUTTON} onClick={() => setMode("rendered")}>Rendered</button>
            <button type="button" className={mode === "raw" ? `${BUTTON} border-accent text-accent` : BUTTON} onClick={() => setMode("raw")}>Raw</button>
            <span className="ml-auto text-[9.5px] text-faint">Range {offset.toLocaleString()}-{Math.max(offset, end).toLocaleString()} of {totalBytes.toLocaleString()} bytes</span>
          </div>
          <div className="max-h-[38rem] overflow-auto p-4">
            {contentLoading ? <LoadingBlock label="Reading artifact range..." /> : mode === "rendered" && (mimeType.includes("markdown") || String(metadata?.filename || "").endsWith(".md")) ? <div className="prose prose-sm max-w-none text-ink"><ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown></div> : <pre className="whitespace-pre-wrap break-words text-[10.5px] text-ink">{content}</pre>}
          </div>
          <div className="flex items-center justify-between border-t border-line bg-paper px-3 py-2"><button type="button" className={BUTTON} disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - CHUNK_BYTES))}>Previous range</button><span className="text-[9.5px] text-muted">Content is read through bounded HTTP Range requests.</span><button type="button" className={BUTTON} disabled={end >= totalBytes - 1} onClick={() => setOffset(offset + CHUNK_BYTES)}>Next range</button></div>
        </div>
      ) : <div className="rounded-lg border border-warnInk/20 bg-warnSoft px-3 py-3 text-[10.5px] text-warnInk">This MIME type is download-only. OpenWorker will not execute or automatically open it.</div>}
      {olderVersions.length > 0 && (
        <section className={`${CARD} p-3`} aria-label="Artifact diff">
          <div className="flex flex-wrap items-center gap-2"><label className="text-[10.5px] text-muted">Compare with <select className="ml-2 rounded-md border border-line bg-panel px-2 py-1 text-ink" value={baseId} onChange={(event) => setBaseId(event.target.value)}><option value="">Select version</option>{olderVersions.map((item) => <option key={String(item.id)} value={String(item.id)}>v{String(item.version)} - {String(item.filename)}</option>)}</select></label><button type="button" className={BUTTON} disabled={!baseId} onClick={() => void api.getArtifactDiff(selectedId, baseId).then(setDiff).catch((caught) => setError(caught instanceof Error ? caught.message : "Diff could not be loaded."))}>Load section diff</button></div>
          {diff && <pre className="mt-3 max-h-96 overflow-auto whitespace-pre-wrap rounded-md bg-paper p-3 text-[9.5px] text-ink">{diff}</pre>}
        </section>
      )}
    </section>
  );
}
