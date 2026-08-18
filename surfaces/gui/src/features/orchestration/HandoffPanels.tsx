import { useCallback, useEffect, useState } from "react";
import { Markdown } from "../../components/Markdown";
import type { ApiDownload, OrchestrationApi } from "./api";
import type {
  ContextRef,
  OrchestrationTaskDetail,
  ResultQuestion,
  TaskBrief,
  TaskBriefInput,
  TaskComment,
  TaskRelation,
  WakeRequest,
  WorkProduct,
} from "./types";
import {
  BUTTON,
  CARD,
  DANGER_BUTTON,
  EmptyState,
  ErrorNotice,
  formatTime,
  humanize,
  INPUT,
  LoadingBlock,
  PRIMARY_BUTTON,
  SectionHead,
  StatusBadge,
} from "./ui";

export type HandoffPanelKind = "brief" | "context" | "dependencies" | "communication" | "products" | "wakes";

const messageOf = (error: unknown) =>
  error instanceof Error ? error.message : typeof error === "string" ? error : "The handoff request failed.";

const lines = (value: string) => value.split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
const pretty = (value: unknown) => JSON.stringify(value, null, 2);

function inputOf(brief: TaskBrief | TaskBriefInput): TaskBriefInput {
  return {
    title: brief.title,
    objective: brief.objective,
    background: brief.background,
    scope: brief.scope,
    instructions: brief.instructions,
    constraints: brief.constraints,
    non_goals: brief.non_goals,
    acceptance_criteria: brief.acceptance_criteria,
    deliverables: brief.deliverables,
    result_contract: brief.result_contract,
  };
}

export function TaskHandoffPanel({
  api,
  task,
  kind,
  onTaskRefresh,
  apiDownload,
  onSelectTask,
}: {
  api: OrchestrationApi;
  task: OrchestrationTaskDetail;
  kind: HandoffPanelKind;
  onTaskRefresh: () => void;
  apiDownload?: ApiDownload;
  onSelectTask?: (taskId: string) => void;
}) {
  if (kind === "brief") return <BriefPanel api={api} task={task} onTaskRefresh={onTaskRefresh} />;
  if (kind === "context") return <ContextPanel api={api} task={task} />;
  if (kind === "dependencies") return <DependenciesPanel api={api} task={task} />;
  if (kind === "communication") return <CommunicationPanel api={api} task={task} />;
  if (kind === "products") return <ProductsPanel api={api} task={task} apiDownload={apiDownload} onSelectTask={onSelectTask} />;
  return <WakesPanel api={api} task={task} />;
}

function BriefPanel({ api, task, onTaskRefresh }: { api: OrchestrationApi; task: OrchestrationTaskDetail; onTaskRefresh: () => void }) {
  const [items, setItems] = useState<TaskBrief[]>([]);
  const [selected, setSelected] = useState<TaskBrief | null>(task.brief || null);
  const [draft, setDraft] = useState<TaskBriefInput | null>(task.brief ? inputOf(task.brief) : null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [compareRevision, setCompareRevision] = useState<number | null>(null);

  const load = useCallback(async (preferredRevision?: number) => {
    setLoading(true);
    setError(null);
    try {
      const next = await api.listTaskBriefs(task.id);
      setItems(next);
      const preferred = next.find((item) => item.revision === preferredRevision)
        || next.find((item) => item.id === task.brief?.id)
        || next[next.length - 1]
        || null;
      setSelected(preferred);
      setDraft(preferred ? inputOf(preferred) : null);
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setLoading(false);
    }
  }, [api, task.brief?.id, task.id]);

  useEffect(() => { void load(); }, [load]);

  const choose = (brief: TaskBrief) => {
    setSelected(brief);
    setDraft(inputOf(brief));
    setNotice(null);
    setError(null);
  };

  const createRevision = async () => {
    if (!draft) return;
    setBusy("create");
    setError(null);
    try {
      const created = await api.createTaskBrief(task.id, draft);
      await load(created.revision);
      setNotice(`Draft revision ${created.revision} created. Existing published revisions remain immutable.`);
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const save = async () => {
    if (!draft || !selected || selected.status !== "draft") return;
    setBusy("save");
    setError(null);
    try {
      const saved = await api.updateTaskBrief(task.id, { ...selected, ...draft });
      await load(saved.revision);
      setNotice(`Draft revision ${saved.revision} saved with optimistic concurrency protection.`);
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  const publish = async () => {
    if (!selected || selected.status !== "draft") return;
    setBusy("publish");
    setError(null);
    try {
      await api.publishTaskBrief(task.id, selected);
      await load(selected.revision);
      onTaskRefresh();
      setNotice(`Revision ${selected.revision} published. New runs will snapshot this revision.`);
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };

  if (loading) return <LoadingBlock label="Loading Brief revisions…" />;
  if (!draft || !selected) return <EmptyState title="No Brief" detail="Create the task with a structured Brief before execution." />;
  const editable = selected.status === "draft";
  const update = <K extends keyof TaskBriefInput>(key: K, value: TaskBriefInput[K]) => setDraft((current) => current ? { ...current, [key]: value } : current);

  return (
    <section aria-label="Task Brief" data-testid="handoff-brief-panel">
      <SectionHead
        title="Brief revisions"
        aside={<div className="flex gap-2"><button className={BUTTON} onClick={() => { if (selected) void navigator.clipboard?.writeText(pretty(inputOf(selected))); }}>Copy JSON</button><button className={BUTTON} disabled={!!busy} onClick={() => void createRevision()}>New revision</button></div>}
      />
      <div className="mb-3 flex flex-wrap gap-2">
        {items.map((item) => (
          <button key={item.id} className={item.id === selected.id ? PRIMARY_BUTTON : BUTTON} onClick={() => choose(item)}>
            r{item.revision} · {humanize(item.status)}
          </button>
        ))}
      </div>
      {items.length > 1 && (
        <label className="mb-3 block max-w-xs"><span className="mb-1 block text-[10.5px] text-muted">Compare selected revision with</span><select className={INPUT} value={compareRevision ?? ""} onChange={(event) => setCompareRevision(event.target.value ? Number(event.target.value) : null)}><option value="">No comparison</option>{items.filter((item) => item.id !== selected.id).map((item) => <option key={item.id} value={item.revision}>Revision {item.revision}</option>)}</select></label>
      )}
      {compareRevision !== null && items.some((item) => item.revision === compareRevision) && (
        <div className="mb-3 grid gap-2 lg:grid-cols-2" aria-label="Brief revision comparison"><pre className={`${CARD} max-h-80 overflow-auto whitespace-pre-wrap p-3 text-[10px]`}>{pretty(inputOf(items.find((item) => item.revision === compareRevision)!))}</pre><pre className={`${CARD} max-h-80 overflow-auto whitespace-pre-wrap p-3 text-[10px]`}>{pretty(inputOf(selected))}</pre></div>
      )}
      {error && <ErrorNotice message={error} />}
      {notice && <div className="mb-3 rounded-lg border border-okLine bg-okSoft px-3 py-2 text-[11.5px] text-ok">{notice}</div>}
      <div className={`${CARD} grid gap-3 p-4 sm:grid-cols-2`}>
        <Field label="Title" value={draft.title} disabled={!editable} onChange={(value) => update("title", value)} />
        <div className="flex items-end justify-end gap-2 text-[10.5px] text-muted">
          <StatusBadge status={selected.status} /> <span className="break-all font-mono">{selected.content_hash}</span>
        </div>
        <TextArea label="Objective" value={draft.objective} disabled={!editable} onChange={(value) => update("objective", value)} />
        <TextArea label="Background" value={draft.background} disabled={!editable} onChange={(value) => update("background", value)} />
        <JsonArea label="Scope (JSON)" value={draft.scope} disabled={!editable} onChange={(value) => update("scope", value as Record<string, unknown>)} />
        <LinesArea label="Instructions" value={draft.instructions} disabled={!editable} onChange={(value) => update("instructions", value)} />
        <LinesArea label="Constraints" value={draft.constraints} disabled={!editable} onChange={(value) => update("constraints", value)} />
        <LinesArea label="Non-goals" value={draft.non_goals} disabled={!editable} onChange={(value) => update("non_goals", value)} />
        <JsonArea label="Acceptance criteria (JSON)" value={draft.acceptance_criteria} disabled={!editable} onChange={(value) => update("acceptance_criteria", value as TaskBriefInput["acceptance_criteria"])} />
        <JsonArea label="Deliverables (JSON)" value={draft.deliverables} disabled={!editable} onChange={(value) => update("deliverables", value as TaskBriefInput["deliverables"])} />
        <div className="sm:col-span-2"><JsonArea label="Result contract (JSON)" value={draft.result_contract} disabled={!editable} onChange={(value) => update("result_contract", value as Record<string, unknown>)} /></div>
      </div>
      {editable && (
        <div className="mt-3 flex justify-end gap-2">
          <button className={BUTTON} disabled={!!busy} onClick={() => void save()}>{busy === "save" ? "Saving…" : "Save draft"}</button>
          <button className={PRIMARY_BUTTON} disabled={!!busy} onClick={() => void publish()}>{busy === "publish" ? "Publishing…" : "Publish revision"}</button>
        </div>
      )}
    </section>
  );
}

function ContextPanel({ api, task }: { api: OrchestrationApi; task: OrchestrationTaskDetail }) {
  const [items, setItems] = useState<ContextRef[]>([]);
  const [contents, setContents] = useState<Record<string, unknown>>({});
  const [verification, setVerification] = useState<Record<string, Record<string, unknown>>>({});
  const [path, setPath] = useState("");
  const [reason, setReason] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    setLoading(true);
    try { setItems(await api.listContextRefs(task.id)); setError(null); }
    catch (caught) { setError(messageOf(caught)); }
    finally { setLoading(false); }
  }, [api, task.id]);
  useEffect(() => { void load(); }, [load]);
  const read = async (ref: ContextRef) => {
    setBusy(`read:${ref.id}`);
    setError(null);
    try {
      const content = await api.readContextRef(ref.id);
      setContents((current) => ({ ...current, [ref.id]: content }));
    }
    catch (caught) { setError(messageOf(caught)); }
    finally { setBusy(""); }
  };
  const add = async () => {
    if (!path.trim() || !reason.trim()) return;
    setBusy("add");
    try {
      await api.addContextRef(task.id, {
        requirement: "recommended",
        ref_type: "file",
        display_name: path.trim(),
        selection_reason: reason.trim(),
        locator: { relative_path: path.trim() },
        delivery_mode: "on_demand",
        trust_level: "operator_provided",
      });
      setPath(""); setReason(""); await load();
    } catch (caught) { setError(messageOf(caught)); }
    finally { setBusy(""); }
  };
  const verify = async (ref: ContextRef) => {
    setBusy(`verify:${ref.id}`);
    setError(null);
    try {
      const result = await api.verifyContextRef(ref.id);
      setVerification((current) => ({ ...current, [ref.id]: result }));
    } catch (caught) { setError(messageOf(caught)); }
    finally { setBusy(""); }
  };
  if (loading) return <LoadingBlock label="Loading context metadata…" />;
  return (
    <section aria-label="Context references" data-testid="handoff-context-panel">
      <SectionHead title="Context manifest" aside={<span className="text-[10.5px] text-muted">Contents are never loaded automatically</span>} />
      {error && <ErrorNotice message={error} />}
      <div className={`${CARD} mb-3 grid gap-2 p-3 sm:grid-cols-[1fr_1fr_auto]`}>
        <input className={INPUT} aria-label="Workspace-relative path" placeholder="Workspace-relative path" value={path} onChange={(event) => setPath(event.target.value)} />
        <input className={INPUT} aria-label="Selection reason" placeholder="Why the agent needs this file" value={reason} onChange={(event) => setReason(event.target.value)} />
        <button className={BUTTON} disabled={busy === "add" || !path.trim() || !reason.trim()} onClick={() => void add()}>Add to draft</button>
      </div>
      {!items.length ? <EmptyState title="No context references" detail="Add bounded references to a draft Brief; agents fetch content only when needed." /> : (
        <div className="space-y-2">
          {items.map((ref) => (
            <article key={ref.id} className={`${CARD} p-3`}>
              <div className="flex flex-wrap items-start gap-2">
                <div className="min-w-0 flex-1">
                  <div className="text-[12.5px] font-medium text-ink">{ref.display_name}</div>
                  <div className="mt-0.5 text-[10.5px] text-muted">{humanize(ref.requirement)} · {humanize(ref.ref_type)} · {humanize(ref.delivery_mode || "on_demand")} · {ref.token_estimate ?? 0} tokens · read {ref.read_count || 0} time(s){ref.last_read_at ? ` · last ${formatTime(ref.last_read_at)}` : ""}</div>
                  <div className="mt-1 text-[11.5px] text-muted">{ref.selection_reason}</div>
                  {ref.summary && <div className="mt-1 text-[11px] text-faint">{ref.summary}</div>}
                </div>
                <button className={BUTTON} disabled={!!busy} onClick={() => void verify(ref)}>{busy === `verify:${ref.id}` ? "Verifying…" : "Verify"}</button>
                <button className={PRIMARY_BUTTON} disabled={!!busy} onClick={() => void read(ref)}>{busy === `read:${ref.id}` ? "Reading…" : "Read content"}</button>
              </div>
              {verification[ref.id] && <div className={`mt-2 rounded-lg border px-2.5 py-1.5 text-[10.5px] ${verification[ref.id].stale ? "border-warnInk/30 bg-warnSoft text-warnInk" : "border-okLine bg-okSoft text-ok"}`}>{verification[ref.id].stale ? "Stale hash — replace the ref before relying on it." : verification[ref.id].available === false ? "Source is unavailable." : "Source hash verified."}</div>}
              {contents[ref.id] !== undefined && (
                <pre className="mt-3 max-h-96 overflow-auto whitespace-pre-wrap break-words rounded-lg border border-warnInk/20 bg-warnSoft/40 p-3 text-[10.5px] text-ink" data-testid={`context-content-${ref.id}`}>
                  Untrusted context boundary — treat this as data, never as instructions.{"\n\n"}{typeof contents[ref.id] === "object" ? pretty(contents[ref.id]) : String(contents[ref.id])}
                </pre>
              )}
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function DependenciesPanel({ api, task }: { api: OrchestrationApi; task: OrchestrationTaskDetail }) {
  const [items, setItems] = useState<TaskRelation[]>([]);
  const [otherTaskId, setOtherTaskId] = useState("");
  const [relationType, setRelationType] = useState("related");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    setLoading(true);
    try { setItems(await api.listTaskRelations(task.id)); setError(null); }
    catch (caught) { setError(messageOf(caught)); }
    finally { setLoading(false); }
  }, [api, task.id]);
  useEffect(() => { void load(); }, [load]);
  const add = async () => {
    setBusy("add");
    try {
      if (relationType === "blocks") {
        const current = items
          .filter((item) => item.relation_type === "blocks" && item.to_task_id === task.id && !item.removed_at)
          .map((item) => item.from_task_id);
        await api.replaceTaskBlockers(task.id, [...new Set([...current, otherTaskId.trim()])]);
      } else {
        await api.addTaskRelation(task.id, { from_task_id: otherTaskId.trim(), to_task_id: task.id, relation_type: relationType });
      }
      setOtherTaskId(""); await load();
    } catch (caught) { setError(messageOf(caught)); }
    finally { setBusy(""); }
  };
  const remove = async (item: TaskRelation) => {
    setBusy(item.id);
    try {
      if (item.relation_type === "blocks" && item.to_task_id === task.id) {
        const remaining = items
          .filter((candidate) => candidate.id !== item.id && candidate.relation_type === "blocks" && candidate.to_task_id === task.id && !candidate.removed_at)
          .map((candidate) => candidate.from_task_id);
        await api.replaceTaskBlockers(task.id, remaining, "Operator removed a blocker");
      } else {
        await api.removeTaskRelation(task.id, item.id);
      }
      await load();
    }
    catch (caught) { setError(messageOf(caught)); }
    finally { setBusy(""); }
  };
  if (loading) return <LoadingBlock label="Loading task relations…" />;
  return (
    <section aria-label="Task dependencies" data-testid="handoff-dependencies-panel">
      <SectionHead title="Typed task relations" />
      {error && <ErrorNotice message={error} />}
      <div className={`${CARD} mb-3 grid gap-2 p-3 sm:grid-cols-[1fr_10rem_auto]`}>
        <input className={INPUT} aria-label="Related task ID" placeholder="Other task ID" value={otherTaskId} onChange={(event) => setOtherTaskId(event.target.value)} />
        <select className={INPUT} value={relationType} onChange={(event) => setRelationType(event.target.value)}>
          {['blocks', 'reviews', 'related', 'supersedes'].map((value) => <option key={value}>{value}</option>)}
        </select>
        <button className={BUTTON} disabled={!otherTaskId.trim() || !!busy} onClick={() => void add()}>Add relation</button>
      </div>
      {!items.length ? <EmptyState title="No task relations" /> : <div className="space-y-2">{items.map((item) => (
        <article key={item.id} className={`${CARD} flex items-center gap-3 p-3`}>
          <StatusBadge status={item.relation_type} />
          <code className="min-w-0 flex-1 break-all text-[11px] text-muted">{item.from_task_id} → {item.to_task_id}</code>
          {!item.removed_at && item.relation_type !== "parent" && <button className={DANGER_BUTTON} disabled={!!busy} onClick={() => void remove(item)}>Remove</button>}
        </article>
      ))}</div>}
    </section>
  );
}

function CommunicationPanel({ api, task }: { api: OrchestrationApi; task: OrchestrationTaskDetail }) {
  const [items, setItems] = useState<TaskComment[]>([]);
  const [latest, setLatest] = useState(0);
  const [body, setBody] = useState("");
  const [mentions, setMentions] = useState("");
  const [requestResponse, setRequestResponse] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async (incremental = false) => {
    try {
      const delta = await api.listTaskComments(task.id, incremental ? latest : 0);
      setItems((current) => incremental ? [...current, ...delta.comments.filter((item) => !current.some((old) => old.id === item.id))] : delta.comments);
      setLatest(delta.latest_sequence);
      setError(null);
    } catch (caught) { setError(messageOf(caught)); }
    finally { setLoading(false); }
  }, [api, latest, task.id]);
  useEffect(() => { void load(false); }, [api, task.id]);
  const post = async () => {
    if (!body.trim()) return;
    setBusy(true);
    try {
      const targets = mentions.split(",").map((item) => item.trim()).filter(Boolean);
      await api.postTaskComment(task.id, body.trim(), { request_response: requestResponse, mentions: targets });
      setBody(""); setMentions(""); setRequestResponse(false); await load(true);
    }
    catch (caught) { setError(messageOf(caught)); }
    finally { setBusy(false); }
  };
  if (loading) return <LoadingBlock label="Loading task communication…" />;
  return (
    <section aria-label="Task communication" data-testid="handoff-communication-panel">
      <SectionHead title="Task-scoped communication" aside={<button className={BUTTON} onClick={() => void load(true)}>Fetch new after #{latest}</button>} />
      {error && <ErrorNotice message={error} />}
      <div className={`${CARD} mb-3 p-3`}>
        <textarea className={`${INPUT} min-h-20 resize-y`} aria-label="Task comment" placeholder="Post a concise task-scoped update." value={body} onChange={(event) => setBody(event.target.value)} />
        <input className={`${INPUT} mt-2`} aria-label="Structured mention targets" placeholder="Mention profile IDs or task:<id>, comma-separated" value={mentions} onChange={(event) => setMentions(event.target.value)} />
        <div className="mt-2 flex items-center justify-between gap-3"><label className="flex items-center gap-2 text-[11px] text-muted"><input type="checkbox" checked={requestResponse} onChange={(event) => setRequestResponse(event.target.checked)} />Request response</label><button className={PRIMARY_BUTTON} disabled={busy || !body.trim()} onClick={() => void post()}>{busy ? "Posting…" : "Post comment"}</button></div>
      </div>
      {!items.length ? <EmptyState title="No task comments" detail="Comments are durable, incremental, and separate from raw runtime transcripts." /> : <div className="space-y-2">{items.map((item) => (
        <article key={item.id} className={`${CARD} p-3`}>
          <div className="flex flex-wrap gap-2 text-[10.5px] text-faint"><span>#{item.sequence}</span><span>{item.author_type}:{item.author_id}</span>{item.created_by_run_id && <code>{item.created_by_run_id}</code>}{item.reply_to_comment_id && <span>reply to {item.reply_to_comment_id}</span>}<span className="ml-auto">{formatTime(item.created_at)}</span></div>
          <div className="mt-1 whitespace-pre-wrap break-words text-[12px] leading-relaxed text-ink">{item.body_markdown}</div>
        </article>
      ))}</div>}
    </section>
  );
}

function ProductsPanel({
  api,
  task,
  apiDownload,
  onSelectTask,
}: {
  api: OrchestrationApi;
  task: OrchestrationTaskDetail;
  apiDownload?: ApiDownload;
  onSelectTask?: (taskId: string) => void;
}) {
  const [items, setItems] = useState<WorkProduct[]>([]);
  const [questions, setQuestions] = useState<ResultQuestion[]>([]);
  const [question, setQuestion] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [questionError, setQuestionError] = useState<string | null>(null);
  const load = useCallback(async () => {
    setLoading(true);
    try { setItems(await api.listTaskWorkProducts(task.id)); setError(null); }
    catch (caught) { setError(messageOf(caught)); }
    finally { setLoading(false); }
  }, [api, task.id]);
  const loadQuestions = useCallback(async () => {
    try { setQuestions(await api.listResultQuestions(task.id)); setQuestionError(null); }
    catch (caught) { setQuestionError(messageOf(caught)); }
  }, [api, task.id]);
  useEffect(() => { void load(); void loadQuestions(); }, [load, loadQuestions]);
  useEffect(() => {
    const terminal = new Set(["completed", "failed", "canceled", "cancelled", "archived"]);
    if (!questions.some((item) => !terminal.has(item.status))) return undefined;
    const timer = window.setInterval(() => void loadQuestions(), 2_500);
    return () => window.clearInterval(timer);
  }, [loadQuestions, questions]);
  const verify = async (id: string) => {
    setBusy(`verify:${id}`);
    try { await api.verifyWorkProduct(id); await load(); }
    catch (caught) { setError(messageOf(caught)); }
    finally { setBusy(""); }
  };
  const ask = async () => {
    const value = question.trim();
    if (!value) return;
    setBusy("ask");
    setQuestionError(null);
    try {
      const created = await api.askResultQuestion(task.id, value);
      setQuestions((current) => [created, ...current.filter((item) => item.id !== created.id)]);
      setQuestion("");
    } catch (caught) {
      setQuestionError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };
  const download = async (item: WorkProduct) => {
    const reference = item.artifact_id || item.uri || "";
    if (!apiDownload || !reference.startsWith("sha256:")) return;
    setBusy(`download:${item.id}`);
    try {
      await apiDownload(
        `/v1/orchestration/blobs/${encodeURIComponent(reference.slice("sha256:".length))}`,
        `${item.title || "result"}.json`,
      );
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy("");
    }
  };
  if (loading) return <LoadingBlock label="Loading results…" />;

  const declared = task.brief?.deliverables || [];
  const declaredIds = new Set(declared.map((item) => item.id));
  const primaryIds = new Set(
    items
      .filter((item) => declaredIds.has(String(item.metadata.deliverable_id || "")))
      .map((item) => item.id),
  );
  for (const deliverable of declared) {
    if (items.some((item) => primaryIds.has(item.id) && String(item.metadata.deliverable_id || "") === deliverable.id)) continue;
    const match = items.find((item) =>
      !primaryIds.has(item.id)
      && (item.kind === deliverable.kind || item.title === deliverable.title),
    );
    if (match) primaryIds.add(match.id);
  }
  const intermediateKinds = new Set(["plan", "progress_report", "review_report", "test_result", "evaluation"]);
  if (!primaryIds.size) {
    for (const item of items) {
      if (!intermediateKinds.has(item.kind)) primaryIds.add(item.id);
    }
  }
  if (!primaryIds.size && items.length) primaryIds.add(items[items.length - 1].id);
  const primary = items.filter((item) => primaryIds.has(item.id));
  const supporting = items.filter((item) => !primaryIds.has(item.id));

  const productCard = (item: WorkProduct, final: boolean) => {
    const downloadable = Boolean(apiDownload && (item.artifact_id || item.uri || "").startsWith("sha256:"));
    return (
      <article key={item.id} className={`${CARD} ${final ? "border-okLine bg-okSoft/40" : ""} p-4`}>
        <div className="flex flex-wrap items-start gap-2">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <div className="text-[13.5px] font-semibold text-ink">{item.title}</div>
              {final && <StatusBadge status="completed" label="Final deliverable" />}
            </div>
            <div className="mt-0.5 text-[10.5px] text-muted">{humanize(item.kind)} · {item.created_by} · {formatTime(item.created_at)}</div>
          </div>
          <StatusBadge status={item.verification_status} />
          {item.summary && <button className={BUTTON} onClick={() => void navigator.clipboard?.writeText(item.summary)}>Copy</button>}
          {downloadable && <button className={BUTTON} disabled={!!busy} onClick={() => void download(item)}>{busy === `download:${item.id}` ? "Downloading…" : "Download artifact"}</button>}
          <button className={BUTTON} disabled={!!busy} onClick={() => void verify(item.id)}>{busy === `verify:${item.id}` ? "Verifying…" : "Verify"}</button>
        </div>
        {item.summary && <div className="mt-3 break-words text-[12px] leading-relaxed text-ink"><Markdown text={item.summary} /></div>}
        {(item.uri || item.content_hash) && <code className="mt-3 block break-all text-[10px] text-faint">{item.uri || item.content_hash}</code>}
      </article>
    );
  };

  return (
    <section aria-label="Results" data-testid="handoff-products-panel">
      <SectionHead title="Final results" />
      {error && <ErrorNotice message={error} />}
      {!items.length ? <EmptyState title="No final result has been published" detail="Structured completion publishes the declared deliverable and its immutable evidence here." /> : (
        <div className="space-y-3">
          {primary.map((item) => productCard(item, true))}
          {!!supporting.length && (
            <details className={`${CARD} p-3`}>
              <summary className="cursor-pointer text-[12px] font-medium text-muted">Supporting work and audit products · {supporting.length}</summary>
              <div className="mt-3 space-y-2">{supporting.map((item) => productCard(item, false))}</div>
            </details>
          )}
        </div>
      )}

      {!!items.length && (
        <div className="mt-5" aria-label="Ask about this result">
          <SectionHead title="Ask about this result" aside={<button className={BUTTON} onClick={() => void loadQuestions()}>Refresh answers</button>} />
          <div className={`${CARD} p-3`}>
            <textarea
              className={`${INPUT} min-h-24 resize-y`}
              value={question}
              maxLength={4_000}
              placeholder="Ask for an explanation, evidence location, comparison, risk, or implication from this result…"
              onChange={(event) => setQuestion(event.target.value)}
            />
            <div className="mt-2 flex items-center gap-3">
              <span className="min-w-0 flex-1 text-[10.5px] text-muted">A separate read-only follow-up Agent answers from the final result. The completed task is not restarted.</span>
              <button className={PRIMARY_BUTTON} disabled={busy === "ask" || !question.trim()} onClick={() => void ask()}>{busy === "ask" ? "Starting…" : "Ask"}</button>
            </div>
          </div>
          {questionError && <div className="mt-2"><ErrorNotice message={questionError} /></div>}
          {!!questions.length && <div className="mt-3 space-y-2">{questions.map((item) => (
            <article key={item.id} className={`${CARD} p-3`}>
              <div className="flex flex-wrap items-start gap-2">
                <div className="min-w-0 flex-1 text-[12.5px] font-medium text-ink">{item.question}</div>
                <StatusBadge status={item.status} />
                {onSelectTask && <button className={BUTTON} onClick={() => onSelectTask(item.task_id)}>Open follow-up</button>}
              </div>
              {item.answer ? (
                <div className="mt-3 break-words text-[12px] leading-relaxed text-ink"><Markdown text={item.answer} /></div>
              ) : (
                <div className="mt-2 text-[11px] text-muted">{["failed", "canceled", "cancelled"].includes(item.status) ? "This follow-up did not produce an answer. Open it for diagnostics." : `Answer in progress · ${item.progress ?? 0}% · ${humanize(item.stage)}`}</div>
              )}
            </article>
          ))}</div>}
        </div>
      )}
    </section>
  );
}

function WakesPanel({ api, task }: { api: OrchestrationApi; task: OrchestrationTaskDetail }) {
  const [items, setItems] = useState<WakeRequest[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    setLoading(true);
    try { setItems(await api.listTaskWakes(task.id)); setError(null); }
    catch (caught) { setError(messageOf(caught)); }
    finally { setLoading(false); }
  }, [api, task.id]);
  useEffect(() => { void load(); }, [load]);
  const act = async (id: string, action: "retry" | "cancel") => {
    setBusy(`${action}:${id}`);
    try { action === "retry" ? await api.retryWake(id) : await api.cancelWake(id); await load(); }
    catch (caught) { setError(messageOf(caught)); }
    finally { setBusy(""); }
  };
  if (loading) return <LoadingBlock label="Loading durable wake diagnostics…" />;
  return (
    <section aria-label="Wake diagnostics" data-testid="handoff-wakes-panel">
      <SectionHead title="Durable wake queue" aside={<button className={BUTTON} onClick={() => void load()}>Refresh</button>} />
      {error && <ErrorNotice message={error} />}
      {!items.length ? <EmptyState title="No wake requests" /> : <div className="space-y-2">{items.map((item) => (
        <article key={item.id} className={`${CARD} p-3`}>
          <div className="flex flex-wrap items-start gap-2"><StatusBadge status={item.status} /><div className="min-w-0 flex-1"><div className="text-[12px] font-medium text-ink">{humanize(item.reason)}</div><div className="mt-0.5 text-[10.5px] text-muted">Attempts {item.attempts} · Coalesced {item.coalesced_count} · target {item.target_run_id || item.target_task_id} · {formatTime(item.updated_at)}</div>{(item.source_event_id || item.source_task_id || item.claimed_by) && <div className="mt-0.5 text-[10px] text-faint">Source {item.source_event_id || item.source_task_id} · claimed by {item.claimed_by || "—"} · not before {formatTime(item.not_before)}</div>}</div>
            {["failed", "canceled"].includes(item.status) && <button className={BUTTON} disabled={!!busy} onClick={() => void act(item.id, "retry")}>Retry</button>}
            {["pending", "deferred", "failed"].includes(item.status) && <button className={DANGER_BUTTON} disabled={!!busy} onClick={() => void act(item.id, "cancel")}>Cancel</button>}
          </div>
          {item.last_error && <pre className="mt-2 whitespace-pre-wrap break-words rounded-lg bg-dangerSoft p-2 text-[10.5px] text-danger">{item.last_error}</pre>}
          <details className="mt-2 text-[10.5px] text-muted"><summary>Payload metadata</summary><pre className="mt-1 overflow-auto">{pretty(item.payload)}</pre></details>
        </article>
      ))}</div>}
    </section>
  );
}

function Field({ label, value, disabled, onChange }: { label: string; value: string; disabled: boolean; onChange: (value: string) => void }) {
  return <label><span className="mb-1 block text-[11px] font-medium text-muted">{label}</span><input className={INPUT} value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)} /></label>;
}
function TextArea({ label, value, disabled, onChange }: { label: string; value: string; disabled: boolean; onChange: (value: string) => void }) {
  return <label><span className="mb-1 block text-[11px] font-medium text-muted">{label}</span><textarea className={`${INPUT} min-h-24 resize-y`} value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)} /></label>;
}
function LinesArea({ label, value, disabled, onChange }: { label: string; value: string[]; disabled: boolean; onChange: (value: string[]) => void }) {
  return <TextArea label={`${label} · one per line`} value={value.join("\n")} disabled={disabled} onChange={(next) => onChange(lines(next))} />;
}
function JsonArea({ label, value, disabled, onChange }: { label: string; value: unknown; disabled: boolean; onChange: (value: Record<string, unknown> | unknown[]) => void }) {
  const [raw, setRaw] = useState(() => pretty(value));
  const [invalid, setInvalid] = useState(false);
  useEffect(() => { setRaw(pretty(value)); setInvalid(false); }, [value]);
  return <label><span className="mb-1 block text-[11px] font-medium text-muted">{label}</span><textarea className={`${INPUT} min-h-28 resize-y font-mono ${invalid ? "border-danger" : ""}`} value={raw} disabled={disabled} onChange={(event) => { const next = event.target.value; setRaw(next); try { const parsed = JSON.parse(next); if (!parsed || typeof parsed !== "object") throw new Error(); setInvalid(false); onChange(parsed); } catch { setInvalid(true); } }} /></label>;
}
