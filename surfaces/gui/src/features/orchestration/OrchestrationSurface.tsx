import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { Icon } from "../../components/Icon";
import {
  createClientIdempotencyKey,
  createOrchestrationApi,
  type ApiDownload,
  type ApiRequest,
  type OrchestrationApi,
} from "./api";
import { TaskHandoffPanel, type HandoffPanelKind } from "./HandoffPanels";
import type {
  AgentRole,
  AgentProfileSummary,
  AgentRun,
  AuditPage,
  AttentionAction,
  AttentionGate,
  ContextRefInput,
  CreateOrchestrationTask,
  ModelPolicySummary,
  OrchestrationHealth,
  OrchestrationTaskDetail,
  OrchestrationTaskSummary,
  OutboxDeadLetter,
  RoutingModelDescriptor,
  RuntimePresetDescriptor,
  RunActivity,
  RunTranscript,
  TaskEvidence,
  TaskNode,
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
  Segmented,
  StageTimeline,
  StatusBadge,
} from "./ui";

type DetailTab = "brief" | "context" | "dependencies" | "communication" | "products" | "wakes" | "graph" | "runs" | "evidence" | "activity";
type GraphMode = "dag" | "list";
type TaskAction = "submit" | "pause" | "resume" | "cancel" | "archive" | "restore";
type TaskFilter = "active" | "finished" | "archived" | "all";

const TASK_PAGE_SIZE = 20;
const DEFAULT_RUNTIME_PRESET_ID = "production-codex-led-mixed-v1";
const CODEX_MAX_RUNTIME_ID = "codex-subscription:gpt-5.6-sol@max";
const CLAUDE_HIGH_RUNTIME_ID = "claude-code-subscription:claude-opus-5@high";
const CLAUDE_MAX_RUNTIME_ID = "claude-code-subscription:claude-opus-5@max";
const UNAVAILABLE_MODEL_STATES = new Set(["unconfigured", "offline", "blocked_by_policy", "unavailable"]);
const ERROR_KIND_DISPLAY_LIMIT = 120;
const ERROR_MESSAGE_DISPLAY_LIMIT = 1_600;
const RECONCILIATION_RUN_DISPLAY_LIMIT = 8;
const PRIMARY_ROLE_CONFLICT_MESSAGE = "Writable code tasks require a Worker primary profile. Turn on Read-only task or select a Worker profile.";
const WRITABLE_DELIVERABLE_DEFAULT = "implementation_patch:Completed outcome";
const READ_ONLY_DELIVERABLE_DEFAULT = "artifact:Read-only analysis report";

const PROFILE_ROLE_GUIDANCE: Record<AgentRole, { responsibility: string; permission: string }> = {
  worker: {
    responsibility: "Implements the requested outcome and may delegate bounded work to another Worker or Tester.",
    permission: "May read and modify the workspace when Read-only task is off; profile tools and runtime policy still apply.",
  },
  orchestrator: {
    responsibility: "Breaks down work, delegates it to child agents, and summarizes their results.",
    permission: "Has no direct repository read/write or shell access; repository work must be performed by a preset-assigned or delegated role.",
  },
  planner: {
    responsibility: "Builds an executable plan with read-only repository tools and may delegate implementation to a Worker.",
    permission: "Cannot directly modify the workspace; under Automatic routing, select Worker for a writable code task.",
  },
  explorer: {
    responsibility: "Reads repository evidence and reports findings before implementation.",
    permission: "Has read-only repository access and cannot delegate; under Automatic routing, select Worker for a writable code task.",
  },
  reviewer: {
    responsibility: "Independently reviews a candidate implementation and records findings.",
    permission: "Review identities are isolated from writable primary code execution.",
  },
  tester: {
    responsibility: "Runs isolated checks and records reproducible test evidence.",
    permission: "Test identities are isolated from writable primary code execution.",
  },
  evaluator: {
    responsibility: "Evaluates step outcomes and acceptance evidence independently.",
    permission: "Evaluator identities are isolated from writable primary code execution.",
  },
  scorer: {
    responsibility: "Scores candidate results against the task's acceptance criteria.",
    permission: "Scorer identities are isolated from writable primary code execution.",
  },
  integrator: {
    responsibility: "Assesses how candidate changes fit together before publication.",
    permission: "This form reserves writable primary code execution for Worker identities.",
  },
};

const DEFAULT_PRESET_ROLE_GROUPS = [
  {
    label: "Semantic understanding, repository exploration, planning, implementation & integration",
    runtimeId: CODEX_MAX_RUNTIME_ID,
    fallbackName: "Codex · GPT-5.6 Sol · Max",
  },
  {
    label: "Independent reviewer",
    runtimeId: CLAUDE_HIGH_RUNTIME_ID,
    fallbackName: "Claude Code · Opus 5 · High",
  },
  {
    label: "Isolated tester & evaluator",
    runtimeId: CLAUDE_MAX_RUNTIME_ID,
    fallbackName: "Claude Code · Opus 5 · Max",
  },
] as const;
const TASK_FILTER_STATUSES: Record<Exclude<TaskFilter, "all">, Array<OrchestrationTaskSummary["status"]>> = {
  active: ["draft", "queued", "running", "waiting_human", "waiting_child", "paused", "blocked", "needs_reconciliation", "canceling"],
  finished: ["completed", "failed", "canceled"],
  archived: ["archived"],
};

export interface OrchestrationSurfaceProps {
  apiRequest: ApiRequest;
  apiDownload: ApiDownload;
  currentWorkspace?: string;
  initialTaskId?: string;
  onOpenProfile?: (profileId: string) => void;
  onOpenPolicy?: (policyId: string) => void;
  subscribeEvents?: (
    onEvent: (event: { type: string; data?: Record<string, unknown> }) => void,
  ) => () => void;
}

const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : typeof error === "string" ? error : "The orchestration service could not complete the request.";

/** Keep diagnostics readable and inert when they originate in an external runtime. */
const boundedDisplayText = (value: string | undefined, limit: number): string => {
  if (!value) return "";
  const sanitized = value
    .replace(/\r\n?/g, "\n")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "�")
    .trim();
  if (sanitized.length <= limit) return sanitized;
  return `${sanitized.slice(0, limit).trimEnd()}…`;
};

const mergeOlderById = <T extends { id: string }>(older: T[], current: T[]): T[] => {
  const seen = new Set<string>();
  return [...older, ...current].filter((item) => {
    if (seen.has(item.id)) return false;
    seen.add(item.id);
    return true;
  });
};

export function OrchestrationSurface({
  apiRequest,
  apiDownload,
  currentWorkspace,
  initialTaskId,
  onOpenProfile,
  onOpenPolicy,
  subscribeEvents,
}: OrchestrationSurfaceProps) {
  const api = useMemo(() => createOrchestrationApi(apiRequest), [apiRequest]);
  const [tasks, setTasks] = useState<OrchestrationTaskSummary[]>([]);
  const [selectedId, setSelectedId] = useState(initialTaskId || "");
  const [detail, setDetail] = useState<OrchestrationTaskDetail | null>(null);
  const [loadingList, setLoadingList] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [listError, setListError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [taskFilter, setTaskFilter] = useState<TaskFilter>("active");
  const [taskPage, setTaskPage] = useState(0);
  const [hasNextPage, setHasNextPage] = useState(false);
  const [runDetailsId, setRunDetailsId] = useState<string | null>(null);
  const [transcript, setTranscript] = useState<RunTranscript | null>(null);
  const [transcriptLoading, setTranscriptLoading] = useState(false);
  const [transcriptError, setTranscriptError] = useState<string | null>(null);
  const [runActivity, setRunActivity] = useState<RunActivity[]>([]);
  const [activityLoading, setActivityLoading] = useState(false);
  const [activityError, setActivityError] = useState<string | null>(null);
  const [activityHasOlder, setActivityHasOlder] = useState(false);
  const [health, setHealth] = useState<OrchestrationHealth | null>(null);
  const [healthError, setHealthError] = useState<string | null>(null);
  const [deadLetters, setDeadLetters] = useState<OutboxDeadLetter[]>([]);
  const [deadLettersHaveMore, setDeadLettersHaveMore] = useState(false);
  const [requeueBusy, setRequeueBusy] = useState("");
  const [requeueActor, setRequeueActor] = useState("");
  const [requeueReason, setRequeueReason] = useState("");
  const [auditPageLoading, setAuditPageLoading] = useState<"attention" | "runs" | "evidence" | "">("");
  const [auditPageError, setAuditPageError] = useState<{ kind: "attention" | "runs" | "evidence"; message: string } | null>(null);
  const listRequestId = useRef(0);
  const listInitialized = useRef(false);
  const detailRequestId = useRef(0);
  const transcriptRequestId = useRef(0);
  const activityRequestId = useRef(0);
  const activityInFlight = useRef(0);
  const activityCursor = useRef(0);
  const activityOldestCursor = useRef<number | null>(null);
  const healthRequestId = useRef(0);
  const requeueIntents = useRef(new Map<string, {
    actor: string;
    reason: string;
    idempotencyKey: string;
  }>());
  const selectedIdRef = useRef(selectedId);
  selectedIdRef.current = selectedId;
  const runDetailsIdRef = useRef(runDetailsId);
  runDetailsIdRef.current = runDetailsId;

  const loadTasks = useCallback(async () => {
    const requestId = ++listRequestId.current;
    const blocking = !listInitialized.current;
    if (blocking) {
      setLoadingList(true);
      setListError(null);
    }
    try {
      const next = await api.listTasks({
        ...(taskFilter === "all" ? {} : { statuses: TASK_FILTER_STATUSES[taskFilter] }),
        limit: TASK_PAGE_SIZE + 1,
        offset: taskPage * TASK_PAGE_SIZE,
      });
      if (listRequestId.current !== requestId) return;
      setHasNextPage(next.length > TASK_PAGE_SIZE);
      const page = next.slice(0, TASK_PAGE_SIZE);
      setTasks(page);
      listInitialized.current = true;
      setListError(null);
      setSelectedId((current) => current || initialTaskId || page[0]?.id || "");
    } catch (error) {
      // Event-driven refreshes keep the last good page mounted. Replacing it with a
      // loading/error panel for every outbox burst made the task list visibly flash.
      if (listRequestId.current === requestId && blocking) {
        setListError(errorMessage(error));
      }
    } finally {
      if (listRequestId.current === requestId && blocking) setLoadingList(false);
    }
  }, [api, initialTaskId, taskFilter, taskPage]);

  const loadDetail = useCallback(async (taskId: string) => {
    const requestId = ++detailRequestId.current;
    if (!taskId) {
      setDetail(null);
      setLoadingDetail(false);
      return;
    }
    setLoadingDetail(true);
    setDetailError(null);
    setDetail((current) => current?.id === taskId ? current : null);
    try {
      const next = await api.getTask(taskId);
      if (detailRequestId.current === requestId) setDetail(next);
    } catch (error) {
      if (detailRequestId.current === requestId) setDetailError(errorMessage(error));
    } finally {
      if (detailRequestId.current === requestId) setLoadingDetail(false);
    }
  }, [api]);

  const loadHealth = useCallback(async () => {
    const requestId = ++healthRequestId.current;
    setHealthError(null);
    try {
      const next = await api.getHealth();
      if (healthRequestId.current !== requestId) return;
      setHealth(next);
      if ((next.outbox?.dead_letters || 0) > 0) {
        const page = await api.listDeadLetters(0, 100);
        if (healthRequestId.current !== requestId) return;
        setDeadLetters(page.items);
        setDeadLettersHaveMore(page.has_more);
      } else {
        setDeadLetters([]);
        setDeadLettersHaveMore(false);
      }
    } catch (error) {
      if (healthRequestId.current === requestId) setHealthError(errorMessage(error));
    }
  }, [api]);

  const loadOlderRuns = useCallback(async () => {
    const current = detail;
    if (!current || !current.runs_page?.has_more || auditPageLoading) return;
    const taskId = current.id;
    const offset = current.runs_page.next_offset
      ?? current.runs_page.page_size
      ?? current.runs?.length
      ?? 0;
    setAuditPageLoading("runs");
    setAuditPageError(null);
    try {
      const page = await api.listTaskRuns(taskId, offset, 500);
      if (selectedIdRef.current !== taskId) return;
      setDetail((latest) => latest?.id === taskId ? {
        ...latest,
        runs: mergeOlderById(page.runs, latest.runs || []),
        runs_page: {
          has_more: page.has_more,
          page_size: page.limit ?? page.page_size,
          offset: page.offset,
          limit: page.limit,
          next_offset: page.next_offset,
          order: page.order,
        },
      } : latest);
    } catch (error) {
      if (selectedIdRef.current === taskId) setAuditPageError({ kind: "runs", message: errorMessage(error) });
    } finally {
      if (selectedIdRef.current === taskId) setAuditPageLoading("");
    }
  }, [api, auditPageLoading, detail]);

  const loadOlderAttention = useCallback(async () => {
    const current = detail;
    if (!current || !current.attention_page?.has_more || auditPageLoading) return;
    const taskId = current.id;
    const offset = current.attention_page.next_offset
      ?? current.attention_page.page_size
      ?? current.attention?.length
      ?? 0;
    setAuditPageLoading("attention");
    setAuditPageError(null);
    try {
      const page = await api.listTaskGates(taskId, offset, 500);
      if (selectedIdRef.current !== taskId) return;
      setDetail((latest) => latest?.id === taskId ? {
        ...latest,
        attention: mergeOlderById(page.gates, latest.attention || []),
        attention_page: {
          has_more: page.has_more,
          page_size: page.limit ?? page.page_size,
          offset: page.offset,
          limit: page.limit,
          next_offset: page.next_offset,
          order: page.order,
        },
      } : latest);
    } catch (error) {
      if (selectedIdRef.current === taskId) setAuditPageError({ kind: "attention", message: errorMessage(error) });
    } finally {
      if (selectedIdRef.current === taskId) setAuditPageLoading("");
    }
  }, [api, auditPageLoading, detail]);

  const loadOlderEvidence = useCallback(async () => {
    const current = detail;
    if (!current || !current.evidence_page?.has_more || auditPageLoading) return;
    const taskId = current.id;
    const offset = current.evidence_page.next_offset
      ?? current.evidence_page.page_size
      ?? current.evidence?.length
      ?? 0;
    setAuditPageLoading("evidence");
    setAuditPageError(null);
    try {
      const page = await api.listTaskEvidence(taskId, offset, 500);
      if (selectedIdRef.current !== taskId) return;
      setDetail((latest) => latest?.id === taskId ? {
        ...latest,
        evidence: mergeOlderById(page.evidence, latest.evidence || []),
        evidence_page: {
          has_more: page.has_more,
          page_size: page.limit ?? page.page_size,
          offset: page.offset,
          limit: page.limit,
          next_offset: page.next_offset,
          order: page.order,
        },
      } : latest);
    } catch (error) {
      if (selectedIdRef.current === taskId) setAuditPageError({ kind: "evidence", message: errorMessage(error) });
    } finally {
      if (selectedIdRef.current === taskId) setAuditPageLoading("");
    }
  }, [api, auditPageLoading, detail]);

  const requeueDeadLetter = useCallback(async (outboxId: string) => {
    const actor = requeueActor.trim();
    const reason = requeueReason.trim();
    if (!actor || !reason) {
      setHealthError("Operator identity and a recovery reason are required for an audited requeue.");
      return;
    }
    const intent = requeueIntents.current.get(outboxId) || {
      actor,
      reason,
      idempotencyKey: createClientIdempotencyKey(`outbox-requeue-${outboxId}`),
    };
    // Retain the exact command across an ambiguous network failure. A retry must
    // reuse its key/body so the server can replay the first committed result.
    requeueIntents.current.set(outboxId, intent);
    setRequeueBusy(outboxId);
    setHealthError(null);
    try {
      await api.requeueDeadLetter(outboxId, intent);
      requeueIntents.current.delete(outboxId);
      await loadHealth();
    } catch (error) {
      setHealthError(errorMessage(error));
    } finally {
      setRequeueBusy("");
    }
  }, [api, loadHealth, requeueActor, requeueReason]);

  useEffect(() => {
    void loadTasks();
  }, [loadTasks]);

  useEffect(() => {
    void loadDetail(selectedId);
  }, [loadDetail, selectedId]);

  useEffect(() => {
    void loadHealth();
    const timer = window.setInterval(() => void loadHealth(), 10_000);
    return () => {
      healthRequestId.current += 1;
      window.clearInterval(timer);
    };
  }, [loadHealth]);

  useEffect(() => {
    setAuditPageLoading("");
    setAuditPageError(null);
  }, [selectedId]);

  useEffect(() => {
    // Run details belong to the task that initiated them. Switching tasks must
    // invalidate both the visible modal and any transcript response still in flight.
    transcriptRequestId.current += 1;
    setRunDetailsId(null);
    setTranscript(null);
    setTranscriptError(null);
    setTranscriptLoading(false);
  }, [selectedId]);

  useEffect(() => () => {
    transcriptRequestId.current += 1;
  }, []);

  useEffect(() => {
    if (!subscribeEvents) return;
    let refreshTimer: number | undefined;
    const unsubscribe = subscribeEvents((event) => {
      if (event.type !== "orchestration_event") return;
      if (refreshTimer !== undefined) window.clearTimeout(refreshTimer);
      // One domain command can emit several outbox messages. Coalesce the burst while
      // still keeping the task timeline live without polling.
      refreshTimer = window.setTimeout(() => {
        void loadTasks();
        if (selectedId) void loadDetail(selectedId);
      }, 75);
    });
    return () => {
      if (refreshTimer !== undefined) window.clearTimeout(refreshTimer);
      unsubscribe();
    };
  }, [loadDetail, loadTasks, selectedId, subscribeEvents]);

  const refresh = async () => {
    await Promise.all([loadTasks(), loadHealth(), selectedId ? loadDetail(selectedId) : Promise.resolve()]);
  };

  const resolveGate = async (
    gateId: string,
    decision: string,
    response?: string,
    expectedVersion?: number,
    idempotencyKey?: string,
  ) => {
    if (!selectedId) return;
    await api.resolveAttention(selectedId, gateId, decision, response, expectedVersion, idempotencyKey);
    await refresh();
  };

  const createTask = async (spec: CreateOrchestrationTask) => {
    const created = await api.createTask(spec);
    setShowCreate(false);
    setSelectedId(created.id);
    setDetail(created);
    await loadTasks();
  };

  const runTaskAction = async (action: TaskAction) => {
    if (!selectedId) return;
    const operation = {
      submit: api.submitTask,
      pause: api.pauseTask,
      resume: api.resumeTask,
      cancel: api.cancelTask,
      archive: api.archiveTask,
      restore: api.restoreTask,
    }[action];
    const next = await operation(selectedId);
    setDetail(next);
    await loadTasks();
  };

  const loadRunTranscript = useCallback(async (taskId: string, runId: string) => {
    const requestId = ++transcriptRequestId.current;
    setTranscriptError(null);
    setTranscriptLoading(true);
    try {
      const next = await api.getRunTranscript(taskId, runId);
      if (
        transcriptRequestId.current === requestId
        && selectedIdRef.current === taskId
      ) {
        setTranscript(next);
      }
    } catch (error) {
      if (
        transcriptRequestId.current === requestId
        && selectedIdRef.current === taskId
      ) {
        setTranscriptError(errorMessage(error));
      }
    } finally {
      if (
        transcriptRequestId.current === requestId
        && selectedIdRef.current === taskId
      ) {
        setTranscriptLoading(false);
      }
    }
  }, [api]);

  const loadRunActivity = useCallback(async (
    taskId: string,
    runId: string,
    reset = false,
  ) => {
    if (activityInFlight.current) return;
    const requestId = ++activityRequestId.current;
    activityInFlight.current = requestId;
    const afterSequence = reset ? 0 : activityCursor.current;
    setActivityLoading(true);
    if (reset) setActivityError(null);
    try {
      const page = await api.getRunActivity(taskId, runId, {
        ...(afterSequence ? { afterSequence } : {}),
        latest: afterSequence === 0,
        limit: 500,
      });
      if (
        activityRequestId.current !== requestId
        || selectedIdRef.current !== taskId
        || runDetailsIdRef.current !== runId
      ) return;
      setRunActivity((current) => {
        const merged = new Map<string, RunActivity>();
        if (!reset) current.forEach((item) => merged.set(item.id, item));
        page.activity.forEach((item) => merged.set(item.id, item));
        return [...merged.values()]
          .sort((left, right) => left.sequence - right.sequence)
          .slice(-5_001);
      });
      activityCursor.current = Math.max(
        activityCursor.current,
        ...page.activity.map((item) => item.sequence),
      );
      if (reset) {
        setActivityHasOlder(page.has_more);
        activityOldestCursor.current = page.activity[0]?.sequence ?? null;
      }
      setActivityError(null);
    } catch (error) {
      if (
        activityRequestId.current === requestId
        && selectedIdRef.current === taskId
        && runDetailsIdRef.current === runId
      ) {
        setActivityError(errorMessage(error));
      }
    } finally {
      if (activityInFlight.current === requestId) {
        activityInFlight.current = 0;
      }
      if (
        activityRequestId.current === requestId
        && selectedIdRef.current === taskId
        && runDetailsIdRef.current === runId
      ) {
        setActivityLoading(false);
      }
    }
  }, [api]);

  const loadOlderRunActivity = useCallback(async (taskId: string, runId: string) => {
    const beforeSequence = activityOldestCursor.current;
    if (activityInFlight.current || beforeSequence == null) return;
    const requestId = ++activityRequestId.current;
    activityInFlight.current = requestId;
    setActivityLoading(true);
    try {
      const page = await api.getRunActivity(taskId, runId, {
        beforeSequence,
        latest: true,
        limit: 500,
      });
      if (
        activityRequestId.current !== requestId
        || selectedIdRef.current !== taskId
        || runDetailsIdRef.current !== runId
      ) return;
      setRunActivity((current) => {
        const merged = new Map(current.map((item) => [item.id, item]));
        page.activity.forEach((item) => merged.set(item.id, item));
        return [...merged.values()]
          .sort((left, right) => left.sequence - right.sequence)
          .slice(-5_001);
      });
      activityOldestCursor.current = page.activity[0]?.sequence ?? beforeSequence;
      setActivityHasOlder(page.has_more);
      setActivityError(null);
    } catch (error) {
      if (
        activityRequestId.current === requestId
        && selectedIdRef.current === taskId
        && runDetailsIdRef.current === runId
      ) {
        setActivityError(errorMessage(error));
      }
    } finally {
      if (activityInFlight.current === requestId) activityInFlight.current = 0;
      if (
        activityRequestId.current === requestId
        && selectedIdRef.current === taskId
        && runDetailsIdRef.current === runId
      ) {
        setActivityLoading(false);
      }
    }
  }, [api]);

  const viewRunDetails = (runId: string) => {
    if (!selectedId) return;
    transcriptRequestId.current += 1;
    setRunDetailsId(runId);
    setTranscript(null);
    setTranscriptError(null);
    setTranscriptLoading(false);
    activityRequestId.current += 1;
    activityInFlight.current = 0;
    activityCursor.current = 0;
    activityOldestCursor.current = null;
    setRunActivity([]);
    setActivityError(null);
    setActivityLoading(false);
    setActivityHasOlder(false);
  };

  // The modal stores only the durable run id. Every fresh task-detail snapshot updates
  // both its derived metadata and retained transcript, so a running Agent cannot remain
  // visually frozen after an orchestration event marks it complete.
  useEffect(() => {
    if (!runDetailsId || !detail || detail.id !== selectedId) return;
    void loadRunTranscript(selectedId, runDetailsId);
  }, [detail, loadRunTranscript, runDetailsId, selectedId]);

  useEffect(() => {
    if (!runDetailsId || !selectedId) return;
    activityCursor.current = 0;
    activityOldestCursor.current = null;
    setRunActivity([]);
    setActivityHasOlder(false);
    void loadRunActivity(selectedId, runDetailsId, true);
    const timer = window.setInterval(() => {
      void loadRunActivity(selectedId, runDetailsId);
    }, 1_500);
    return () => {
      window.clearInterval(timer);
      activityRequestId.current += 1;
      activityInFlight.current = 0;
    };
  }, [loadRunActivity, runDetailsId, selectedId]);

  const runDetails = useMemo(
    () => runDetailsId && detail ? taskRuns(detail).find((run) => run.id === runDetailsId) : undefined,
    [detail, runDetailsId],
  );

  const closeRunDetails = () => {
    transcriptRequestId.current += 1;
    setRunDetailsId(null);
    setTranscript(null);
    setTranscriptError(null);
    setTranscriptLoading(false);
    activityRequestId.current += 1;
    activityInFlight.current = 0;
    activityCursor.current = 0;
    activityOldestCursor.current = null;
    setRunActivity([]);
    setActivityError(null);
    setActivityLoading(false);
    setActivityHasOlder(false);
  };

  return (
    <main className="flex min-h-0 flex-1 bg-paper" data-testid="orchestration-surface">
      <aside className="flex w-[280px] shrink-0 flex-col border-r border-line bg-panel/50">
        <div className="border-b border-line px-4 py-4">
          <div className="flex items-center gap-2 text-[13.5px] font-semibold">
            <Icon name="branch" size={16} /> Tasks
            <button
              className="ml-auto flex items-center gap-1 rounded-md px-1.5 py-1 text-[11px] font-medium text-accent hover:bg-accentSoft"
              onClick={() => setShowCreate(true)}
            >
              <Icon name="plus" size={12} /> New
            </button>
            <button className="text-muted hover:text-ink" aria-label="Refresh tasks" onClick={() => void refresh()}>
              <Icon name="refresh" size={14} />
            </button>
          </div>
          <div className="mt-1 text-[11.5px] text-muted">Coordinated work across agents</div>
          <label className="mt-3 block">
            <span className="sr-only">Task filter</span>
            <select
              className={`${INPUT} py-1.5 text-[11.5px]`}
              aria-label="Task filter"
              value={taskFilter}
              onChange={(event) => {
                setTaskFilter(event.target.value as TaskFilter);
                setTaskPage(0);
              }}
            >
              <option value="active">Active tasks</option>
              <option value="finished">Finished tasks</option>
              <option value="archived">Archived tasks</option>
              <option value="all">All tasks</option>
            </select>
          </label>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto p-2.5">
          {loadingList ? (
            <div className="px-2 py-4 text-[12px] text-muted">Loading tasks…</div>
          ) : listError ? (
            <ErrorNotice message={listError} onRetry={() => void loadTasks()} />
          ) : tasks.length === 0 ? (
            <div className="px-2 py-6 text-center text-[12px] text-muted">
              {taskFilter === "active" ? "No active orchestration tasks." : `No ${taskFilter} orchestration tasks.`}
            </div>
          ) : (
            <div className="space-y-1">
              {tasks.map((task) => (
                <TaskRow
                  key={task.id}
                  task={task}
                  selected={task.id === selectedId}
                  onSelect={() => {
                    setShowCreate(false);
                    setSelectedId(task.id);
                  }}
                />
              ))}
            </div>
          )}
        </div>
        <div className="flex items-center justify-between border-t border-line px-3 py-2 text-[10.5px] text-muted">
          <button
            type="button"
            className={BUTTON}
            disabled={loadingList || taskPage === 0}
            onClick={() => setTaskPage((page) => Math.max(0, page - 1))}
          >
            Previous
          </button>
          <span aria-label="Task page">Page {taskPage + 1}</span>
          <button
            type="button"
            className={BUTTON}
            disabled={loadingList || !hasNextPage}
            onClick={() => setTaskPage((page) => page + 1)}
          >
            Next
          </button>
        </div>
      </aside>

      <section className="min-w-0 flex-1 overflow-y-auto">
        <div className="mx-auto max-w-6xl px-6 py-5">
          <OperationalHealthPanel
            health={health}
            error={healthError}
            deadLetters={deadLetters}
            deadLettersHaveMore={deadLettersHaveMore}
            requeueBusy={requeueBusy}
            requeueActor={requeueActor}
            requeueReason={requeueReason}
            onRequeueActorChange={setRequeueActor}
            onRequeueReasonChange={setRequeueReason}
            onRefresh={() => void loadHealth()}
            onRequeue={(outboxId) => void requeueDeadLetter(outboxId)}
          />
          {showCreate ? (
            <CreateTaskForm
              api={api}
              initialWorkspace={currentWorkspace}
              onCreate={createTask}
              onCancel={() => setShowCreate(false)}
            />
          ) : !selectedId ? (
            <EmptyState title="Select a task" detail="Task plans, agent runs, evidence, and decisions appear here." />
          ) : loadingDetail && !detail ? (
            <LoadingBlock label="Loading task…" />
          ) : detailError ? (
            <ErrorNotice message={detailError} onRetry={() => void loadDetail(selectedId)} />
          ) : detail ? (
            <TaskDetailView
              api={api}
              task={detail}
              resolving={loadingDetail}
              onResolve={resolveGate}
              onRefresh={() => void refresh()}
              onAction={runTaskAction}
              onViewRun={viewRunDetails}
              onOpenProfile={onOpenProfile}
              onOpenPolicy={onOpenPolicy}
              apiDownload={apiDownload}
              onSelectTask={(taskId) => setSelectedId(taskId)}
              auditPageLoading={auditPageLoading}
              auditPageError={auditPageError}
              onLoadOlderAttention={() => void loadOlderAttention()}
              onLoadOlderRuns={() => void loadOlderRuns()}
              onLoadOlderEvidence={() => void loadOlderEvidence()}
            />
          ) : null}
        </div>
      </section>
      {runDetailsId && (
        <RunDetailsModal
          runId={runDetailsId}
          run={runDetails}
          transcript={transcript}
          loading={transcriptLoading}
          error={transcriptError}
          activity={runActivity}
          activityLoading={activityLoading}
          activityError={activityError}
          activityHasOlder={activityHasOlder}
          onRefreshActivity={() => void loadRunActivity(selectedId, runDetailsId)}
          onLoadOlderActivity={() => void loadOlderRunActivity(selectedId, runDetailsId)}
          onClose={closeRunDetails}
        />
      )}
    </main>
  );
}

function OperationalHealthPanel({
  health,
  error,
  deadLetters,
  deadLettersHaveMore,
  requeueBusy,
  requeueActor,
  requeueReason,
  onRequeueActorChange,
  onRequeueReasonChange,
  onRefresh,
  onRequeue,
}: {
  health: OrchestrationHealth | null;
  error: string | null;
  deadLetters: OutboxDeadLetter[];
  deadLettersHaveMore: boolean;
  requeueBusy: string;
  requeueActor: string;
  requeueReason: string;
  onRequeueActorChange: (value: string) => void;
  onRequeueReasonChange: (value: string) => void;
  onRefresh: () => void;
  onRequeue: (outboxId: string) => void;
}) {
  if (!error && (!health || health.ready)) return null;
  const leader = health?.leader;
  const outbox = health?.outbox;
  return (
    <section
      className="mb-4 rounded-xl border border-warnInk/25 bg-warnSoft p-4"
      aria-label="Orchestration health"
      role="alert"
    >
      <div className="flex items-start gap-3">
        <Icon name="shield" size={16} className="mt-0.5 shrink-0 text-warnInk" />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <h2 className="text-[13px] font-semibold text-warnInk">Orchestration recovery</h2>
            {health && <span className="text-[10px] uppercase tracking-wide text-warnInk/70">{health.state}</span>}
            <button type="button" className="ml-auto text-[11px] font-medium text-accent hover:underline" onClick={onRefresh}>
              Refresh health
            </button>
          </div>
          {error && <div className="mt-1 text-[11.5px] text-danger">Health check failed: {error}</div>}
          {health && (
            <div className="mt-2 grid gap-2 text-[11px] text-warnInk/85 sm:grid-cols-2 lg:grid-cols-3">
              <div className="rounded-lg border border-warnInk/15 bg-panel/50 px-3 py-2">
                <div className="font-medium">Scheduler leader</div>
                <div>{leader?.held ? `Held${leader.epoch == null ? "" : ` · epoch ${leader.epoch}`}` : "Lease not held"}</div>
                {leader && !leader.heartbeat_alive && <div className="text-danger">Heartbeat stopped</div>}
              </div>
              <div className="rounded-lg border border-warnInk/15 bg-panel/50 px-3 py-2">
                <div className="font-medium">Outbox</div>
                <div>{outbox?.pending || 0} pending · {outbox?.dead_letters || 0} dead letter{outbox?.dead_letters === 1 ? "" : "s"}</div>
                {outbox?.stale && <div className="text-danger">Delivery loop is stale</div>}
              </div>
              <div className="rounded-lg border border-warnInk/15 bg-panel/50 px-3 py-2">
                <div className="font-medium">Scheduler</div>
                <div>{health.loop_alive ? "Loop running" : "Loop stopped"}</div>
                {!!health.consecutive_failures && <div>{health.consecutive_failures} consecutive failures</div>}
              </div>
            </div>
          )}
          {(health?.last_error || outbox?.last_error) && (
            <div className="mt-2 break-words rounded-lg bg-panel/50 px-3 py-2 font-mono text-[10px] text-danger">
              {health?.last_error || outbox?.last_error}
            </div>
          )}
          {deadLetters.length > 0 && (
            <div className="mt-3" aria-label="Dead-letter recovery">
              <div className="mb-1.5 text-[11px] font-semibold text-warnInk">Dead-letter recovery</div>
              <div className="mb-2 grid gap-2 sm:grid-cols-2">
                <label className="text-[10.5px] text-muted">
                  Operator identity
                  <input
                    aria-label="Dead-letter operator identity"
                    className={`${INPUT} mt-1 w-full`}
                    maxLength={200}
                    value={requeueActor}
                    onChange={(event) => onRequeueActorChange(event.target.value)}
                    placeholder="on-call@example.com"
                  />
                </label>
                <label className="text-[10.5px] text-muted">
                  Recovery reason
                  <input
                    aria-label="Dead-letter recovery reason"
                    className={`${INPUT} mt-1 w-full`}
                    maxLength={2000}
                    value={requeueReason}
                    onChange={(event) => onRequeueReasonChange(event.target.value)}
                    placeholder="Subscriber repaired and verified"
                  />
                </label>
              </div>
              <div className="space-y-1.5">
                {deadLetters.map((item) => (
                  <div key={item.id} className="flex items-start gap-3 rounded-lg border border-warnInk/15 bg-panel/60 px-3 py-2">
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-[11px] font-medium text-ink">{item.topic || item.event_id}</div>
                      <div className="mt-0.5 text-[10px] text-muted">{item.attempts} attempts · {formatTime(item.dead_lettered_at)}</div>
                      {item.last_error && <div className="mt-0.5 line-clamp-2 text-[10px] text-danger">{item.last_error}</div>}
                    </div>
                    <button
                      type="button"
                      className={BUTTON}
                      disabled={!!requeueBusy || !requeueActor.trim() || !requeueReason.trim()}
                      onClick={() => onRequeue(item.id)}
                    >
                      {requeueBusy === item.id ? "Requeueing…" : "Requeue"}
                    </button>
                  </div>
                ))}
              </div>
              {deadLettersHaveMore && <div className="mt-1.5 text-[10px] text-warnInk/75">Showing the newest 100 dead letters. Resolve these, then refresh to continue.</div>}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

function CreateTaskForm({
  api,
  initialWorkspace,
  onCreate,
  onCancel,
}: {
  api: OrchestrationApi;
  initialWorkspace?: string;
  onCreate: (spec: CreateOrchestrationTask) => Promise<void>;
  onCancel: () => void;
}) {
  const [title, setTitle] = useState("");
  const [objective, setObjective] = useState("");
  const [domain, setDomain] = useState<"code" | "knowledge">("code");
  const [readOnly, setReadOnly] = useState(false);
  const [workspace, setWorkspace] = useState(initialWorkspace || "");
  const [idempotencyKey] = useState(() => createClientIdempotencyKey("task-create"));
  const [criteria, setCriteria] = useState("");
  const [constraints, setConstraints] = useState("");
  const [background, setBackground] = useState("");
  const [includedScope, setIncludedScope] = useState("");
  const [excludedScope, setExcludedScope] = useState("");
  const [instructions, setInstructions] = useState("");
  const [nonGoals, setNonGoals] = useState("");
  const [deliverables, setDeliverables] = useState(WRITABLE_DELIVERABLE_DEFAULT);
  const [contextRefs, setContextRefs] = useState<ContextRefInput[]>([]);
  const [contextPath, setContextPath] = useState("");
  const [contextReason, setContextReason] = useState("");
  const [wizardStep, setWizardStep] = useState(1);
  const [requireReview, setRequireReview] = useState(false);
  const [requireTests, setRequireTests] = useState(false);
  const [profiles, setProfiles] = useState<AgentProfileSummary[]>([]);
  const [policies, setPolicies] = useState<ModelPolicySummary[]>([]);
  const [models, setModels] = useState<RoutingModelDescriptor[]>([]);
  const [runtimePresets, setRuntimePresets] = useState<RuntimePresetDescriptor[]>([]);
  const [profileId, setProfileId] = useState("worker");
  const [policyId, setPolicyId] = useState("quality-first");
  const [runtimePresetId, setRuntimePresetId] = useState("");
  const [requestedModel, setRequestedModel] = useState("");
  const [presetSelectionInitialized, setPresetSelectionInitialized] = useState(false);
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (initialWorkspace) setWorkspace((current) => current || initialWorkspace);
  }, [initialWorkspace]);

  useEffect(() => {
    setDeliverables((current) => {
      if (readOnly && current === WRITABLE_DELIVERABLE_DEFAULT) {
        return READ_ONLY_DELIVERABLE_DEFAULT;
      }
      if (!readOnly && current === READ_ONLY_DELIVERABLE_DEFAULT) {
        return WRITABLE_DELIVERABLE_DEFAULT;
      }
      return current;
    });
  }, [readOnly]);

  useEffect(() => {
    let active = true;
    setCatalogLoading(true);
    setCatalogError(null);
    void Promise.all([
      api.listAgentProfiles(),
      api.listModelPolicies(),
      api.getModelCatalog().catch(() => []),
      // Additive endpoint: legacy OpenWorker servers simply keep Automatic routing.
      api.getRuntimePresets().catch(() => []),
    ])
      .then(([profileRows, policyRows, modelRows, presetRows]) => {
        if (!active) return;
        const publishedProfiles = profileRows.filter((item) =>
          !item.archived
          && item.current_version != null
          && (!item.role || ["worker", "planner", "explorer", "orchestrator"].includes(item.role)),
        );
        const publishedPolicies = policyRows.filter((item) => !item.archived && item.current_version != null);
        setProfiles(publishedProfiles);
        setPolicies(publishedPolicies);
        setModels(modelRows);
        setRuntimePresets(presetRows);
        setProfileId((current) => publishedProfiles.some((item) => item.id === current) ? current : publishedProfiles[0]?.id || current);
        setPolicyId((current) => publishedPolicies.some((item) => item.id === current) ? current : publishedPolicies[0]?.id || current);
      })
      .catch((caught) => {
        if (active) setCatalogError(errorMessage(caught));
      })
      .finally(() => {
        if (active) setCatalogLoading(false);
      });
    return () => { active = false; };
  }, [api]);

  const defaultCodePreset = useMemo(() => runtimePresets.find((preset) => preset.id === DEFAULT_RUNTIME_PRESET_ID)
    || runtimePresets.find((preset) => preset.is_default && (preset.default_for_domains.length === 0 || preset.default_for_domains.includes("code"))), [runtimePresets]);

  useEffect(() => {
    if (catalogLoading || presetSelectionInitialized) return;
    if (domain === "code" && defaultCodePreset) setRuntimePresetId(defaultCodePreset.id);
    setPresetSelectionInitialized(true);
  }, [catalogLoading, defaultCodePreset, domain, presetSelectionInitialized]);

  const selectedPreset = runtimePresets.find((preset) => preset.id === runtimePresetId);
  const selectedProfile = profiles.find((profile) => profile.id === profileId);
  const selectedProfileRole = selectedProfile?.role;
  const selectedProfileGuidance = selectedProfileRole ? PROFILE_ROLE_GUIDANCE[selectedProfileRole] : undefined;
  const profileFieldLabel = runtimePresetId ? "Task root profile" : "Primary agent profile";
  const writableCodeRoleConflict = !runtimePresetId && domain === "code" && !readOnly
    && Boolean(selectedProfileRole && selectedProfileRole !== "worker");
  const presetRoleGroups = useMemo(() => {
    if (!selectedPreset) return [];
    if (selectedPreset.id === DEFAULT_RUNTIME_PRESET_ID) return [...DEFAULT_PRESET_ROLE_GROUPS];
    const byRuntime = new Map<string, string[]>();
    for (const assignment of selectedPreset.roles) {
      byRuntime.set(assignment.runtime_id, [...(byRuntime.get(assignment.runtime_id) || []), humanize(assignment.role)]);
    }
    return [...byRuntime.entries()].map(([runtimeId, roles]) => ({
      label: roles.join(", "),
      runtimeId,
      fallbackName: runtimeId,
    }));
  }, [selectedPreset]);
  const presetUnavailableRuntimeIds = useMemo(() => {
    if (!selectedPreset) return [];
    const unavailable = new Set(selectedPreset.unavailable_runtime_ids);
    for (const runtimeId of selectedPreset.required_runtime_ids) {
      const model = models.find((candidate) => candidate.id === runtimeId);
      if (model && UNAVAILABLE_MODEL_STATES.has(model.availability)) unavailable.add(runtimeId);
    }
    return [...unavailable];
  }, [models, selectedPreset]);
  const presetUnavailable = Boolean(selectedPreset && (selectedPreset.available === false || presetUnavailableRuntimeIds.length > 0));

  const lines = (value: string) => value.split("\n").map((item) => item.trim()).filter(Boolean);
  const submit = async (event: FormEvent | null, publishAndStart = true) => {
    event?.preventDefault();
    if (!objective.trim()) return;
    if (writableCodeRoleConflict) return;
    setSubmitting(true);
    setError(null);
    try {
      const criterionLines = lines(criteria);
      const deliverableRows = lines(deliverables).map((item, index) => {
        const [kind, ...titleParts] = item.split(":");
        return {
          id: `deliverable-${index + 1}`,
          kind: kind.trim() || "other",
          title: titleParts.join(":").trim() || item,
          required: true,
        };
      });
      const briefTitle = title.trim() || objective.trim().slice(0, 120);
      await onCreate({
        idempotency_key: idempotencyKey,
        ...(title.trim() ? { title: title.trim() } : {}),
        objective: objective.trim(),
        domain,
        read_only: readOnly,
        ...(domain === "code" && workspace.trim()
          ? { workspace: workspace.trim() }
          : {}),
        acceptance_criteria: criterionLines,
        constraints: lines(constraints),
        profile_id: profileId,
        model_policy_id: policyId,
        ...(runtimePresetId
          ? { runtime_preset_id: runtimePresetId }
          : requestedModel
            ? { requested_model: requestedModel }
            : {}),
        require_review: Boolean(runtimePresetId) || requireReview,
        require_tests: Boolean(runtimePresetId) || requireTests,
        auto_start: publishAndStart,
        publish_brief: publishAndStart,
        brief: {
          title: briefTitle,
          objective: objective.trim(),
          background: background.trim(),
          scope: {
            domain,
            include: lines(includedScope),
            exclude: lines(excludedScope),
            ...(lines(includedScope).length === 0 ? {
              whole_task: true,
              reason: "The root Brief objective defines the bounded task scope.",
            } : {}),
            ...(domain === "code" && workspace.trim() ? { workspace: workspace.trim() } : {}),
          },
          instructions: lines(instructions).length
            ? lines(instructions)
            : ["Complete the objective within the declared scope and provide attributable verification."],
          constraints: lines(constraints),
          non_goals: lines(nonGoals),
          acceptance_criteria: (criterionLines.length ? criterionLines : ["The requested outcome is complete and verified."]).map((text, index) => ({
            id: `criterion-${index + 1}`,
            text,
            required: true,
            verification: "Provide attributable evidence in the structured result.",
          })),
          deliverables: deliverableRows.length ? deliverableRows : [{ id: "deliverable-1", kind: "other", title: "Completed outcome", required: true }],
          result_contract: {
            schema_id: readOnly ? "analysis_result_v1" : "implementation_result_v1",
            schema_version: 1,
            required_fields: ["summary", "criterion_results", "work_products", "risks"],
          },
        },
        context_refs: contextRefs,
      });
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form className={`${CARD} mx-auto max-w-3xl p-5`} onSubmit={(event) => void submit(event, true)} data-testid="create-orchestration-task">
      <SectionHead title="New orchestrated task" />
      <p className="-mt-1 mb-4 text-[12px] leading-relaxed text-muted">
        The coordinator will score complexity, clarify ambiguity, freeze a DAG, isolate execution/review/testing, and request formal acceptance when policy requires it.
      </p>
      {error && <ErrorNotice message={error} />}
      <nav className="mb-4 grid grid-cols-5 gap-1 rounded-xl border border-line bg-paper p-1" aria-label="Create task wizard">
        {["Goal", "Brief", "Context", "Execution", "Preview"].map((label, index) => (
          <button
            key={label}
            type="button"
            className={`rounded-lg px-2 py-1.5 text-[10.5px] ${wizardStep === index + 1 ? "bg-panel font-semibold text-accent shadow-sm" : "text-muted hover:text-ink"}`}
            aria-current={wizardStep === index + 1 ? "step" : undefined}
            onClick={() => setWizardStep(index + 1)}
          >
            {index + 1}. {label}
          </button>
        ))}
      </nav>
      <div className="grid gap-3 sm:grid-cols-2">
        <label className="sm:col-span-2">
          <span className="mb-1 block text-[11.5px] font-medium text-muted">Objective</span>
          <textarea
            className={`${INPUT} min-h-24 resize-y`}
            value={objective}
            autoFocus
            required
            placeholder="Describe the finished outcome, not just the first step."
            onChange={(event) => setObjective(event.target.value)}
          />
        </label>
        <label>
          <span className="mb-1 block text-[11.5px] font-medium text-muted">Title (optional)</span>
          <input className={INPUT} value={title} onChange={(event) => setTitle(event.target.value)} />
        </label>
        <label>
          <span className="mb-1 block text-[11.5px] font-medium text-muted">Domain</span>
          <select className={INPUT} value={domain} onChange={(event) => {
            const nextDomain = event.target.value as "code" | "knowledge";
            setDomain(nextDomain);
            if (nextDomain === "knowledge") {
              setRuntimePresetId("");
            } else if (!requestedModel && defaultCodePreset) {
              setRuntimePresetId(defaultCodePreset.id);
            }
          }}>
            <option value="code">Code / workspace</option>
            <option value="knowledge">Knowledge work</option>
          </select>
        </label>
        <label>
          <span className="mb-1 block text-[11.5px] font-medium text-muted">{profileFieldLabel}</span>
          <select
            className={INPUT}
            value={profileId}
            disabled={catalogLoading || profiles.length === 0}
            aria-label={profileFieldLabel}
            aria-describedby="selected-profile-guidance"
            aria-invalid={writableCodeRoleConflict || undefined}
            aria-errormessage={writableCodeRoleConflict ? "primary-role-conflict" : undefined}
            onChange={(event) => setProfileId(event.target.value)}
          >
            {profiles.length === 0
              ? <option value={profileId}>{catalogLoading ? "Loading profiles…" : profileId}</option>
              : profiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}{profile.role ? ` · ${humanize(profile.role)}` : ""} · v{profile.current_version}</option>)}
          </select>
          <span id="selected-profile-guidance" className="mt-1.5 block rounded-lg border border-line bg-paper px-2.5 py-2 text-[10px] leading-relaxed text-muted" aria-label="Selected agent profile guidance">
            {selectedProfileRole && selectedProfileGuidance ? (
              <>
                <span className="block font-semibold text-ink">{runtimePresetId ? "Task root role" : "Primary role"} · {humanize(selectedProfileRole)}</span>
                <span className="mt-0.5 block"><span className="font-medium">Responsibility:</span> {selectedProfile?.description || selectedProfileGuidance.responsibility}</span>
                <span className="mt-0.5 block"><span className="font-medium">Workspace permission:</span> {selectedProfileGuidance.permission}</span>
              </>
            ) : (
              <span className="block">Role and permission guidance will appear when the selected published profile exposes its catalog role.</span>
            )}
            {runtimePresetId && (
              <span className="mt-1 block text-accent">
                With a role-aware preset, this profile labels the task runtime-tree root and task summary; it is not assigned to every DAG node. Each node is assigned independently by its role through the preset.
              </span>
            )}
            {runtimePresetId && readOnly && (
              <span className="mt-1 block text-warnInk">
                Read-only remains a global hard boundary: preset-assigned Worker and execute nodes may inspect the workspace, but cannot modify or publish it.
              </span>
            )}
          </span>
        </label>
        <label>
          <span className="mb-1 block text-[11.5px] font-medium text-muted">Model routing policy</span>
          <select className={INPUT} value={policyId} disabled={catalogLoading || policies.length === 0} onChange={(event) => setPolicyId(event.target.value)}>
            {policies.length === 0
              ? <option value={policyId}>{catalogLoading ? "Loading policies…" : policyId}</option>
              : policies.map((policy) => <option key={policy.id} value={policy.id}>{policy.name} · v{policy.current_version}</option>)}
          </select>
        </label>
        <label className="sm:col-span-2">
          <span className="mb-1 block text-[11.5px] font-medium text-muted">Runtime orchestration preset</span>
          <select
            className={INPUT}
            value={runtimePresetId}
            disabled={catalogLoading}
            onChange={(event) => {
              const value = event.target.value;
              setRuntimePresetId(value);
              if (value) setRequestedModel("");
            }}
          >
            <option value="">Automatic / legacy routing</option>
            {runtimePresets.map((preset) => (
              <option key={preset.id} value={preset.id}>
                {preset.name}{preset.id === DEFAULT_RUNTIME_PRESET_ID ? " · Recommended default" : ""}
              </option>
            ))}
          </select>
          <span className="mt-1 block text-[10px] leading-relaxed text-faint">A preset assigns isolated runtimes by role. Choose Automatic to use one requested model or the frozen routing policy.</span>
        </label>
        {selectedPreset && (
          <section className="sm:col-span-2 rounded-xl border border-border/70 bg-surface2/45 p-3" aria-label="Mixed runtime role mapping">
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div>
                <div className="text-[12px] font-semibold text-ink">{selectedPreset.name}</div>
                <div className="mt-0.5 text-[10.5px] leading-relaxed text-muted">
                  {selectedPreset.description || "Codex leads semantic understanding, repository exploration, planning, implementation and integration; Claude independently reviews, tests and evaluates."}
                </div>
              </div>
              <span className="rounded-full border border-border bg-surface px-2 py-0.5 text-[9.5px] font-semibold uppercase tracking-wide text-muted">Role-isolated</span>
            </div>
            <div className="mt-2.5 grid gap-2">
              {presetRoleGroups.map((group) => {
                const model = models.find((candidate) => candidate.id === group.runtimeId);
                const unavailable = presetUnavailableRuntimeIds.includes(group.runtimeId)
                  || Boolean(model && UNAVAILABLE_MODEL_STATES.has(model.availability));
                return (
                  <div key={`${group.runtimeId}-${group.label}`} className="grid gap-0.5 rounded-lg border border-border/60 bg-surface px-2.5 py-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)] sm:gap-3">
                    <div className="text-[10.5px] font-medium text-ink">{group.label}</div>
                    <div className="min-w-0 text-[10px] text-muted sm:text-right">
                      <span className="break-all">{model?.label || group.fallbackName}</span>
                      {unavailable && <span className="ml-1 font-semibold text-warnInk">· {humanize(model?.availability || "unavailable")}</span>}
                    </div>
                  </div>
                );
              })}
            </div>
            {presetUnavailable && (
              <div className="mt-2 rounded-lg border border-warnInk/30 bg-warn/10 px-2.5 py-2 text-[10.5px] leading-relaxed text-warnInk" role="alert">
                <div className="font-semibold">This preset cannot start until every required subscription runtime is available.</div>
                {presetUnavailableRuntimeIds.map((runtimeId) => {
                  const model = models.find((candidate) => candidate.id === runtimeId);
                  return <div key={runtimeId} className="mt-1 break-all">{model?.label || runtimeId}: {model?.availability_reason || selectedPreset.availability_reason || "Runtime unavailable"}</div>;
                })}
                {presetUnavailableRuntimeIds.length === 0 && selectedPreset.availability_reason && <div className="mt-1">{selectedPreset.availability_reason}</div>}
              </div>
            )}
          </section>
        )}
        <label className="sm:col-span-2">
          <span className="mb-1 block text-[11.5px] font-medium text-muted">Requested model / Subscription Agent runtime</span>
          <select className={INPUT} value={requestedModel} disabled={catalogLoading || Boolean(runtimePresetId)} onChange={(event) => setRequestedModel(event.target.value)}>
            <option value="">Automatic · let the frozen routing policy decide</option>
            {models.map((model) => {
              const unavailable = ["unconfigured", "offline", "blocked_by_policy", "unavailable"].includes(model.availability);
              const runtime = model.runtime ? ` · ${model.runtime.model} / ${humanize(model.runtime.reasoning_effort)}` : "";
              const state = unavailable ? ` · ${humanize(model.availability)}` : "";
              return <option key={model.id} value={model.id} disabled={unavailable}>{model.label || model.id}{runtime}{state}</option>;
            })}
          </select>
          <span className="mt-1 block text-[10px] leading-relaxed text-faint">{runtimePresetId ? "The selected role-aware preset controls models; switch the preset to Automatic to pin one model." : "This pins every generated node that has no explicit per-node model."}</span>
        </label>
        {domain === "code" && (
          <label className="sm:col-span-2">
            <span className="mb-1 block text-[11.5px] font-medium text-muted">Workspace (optional when a default workspace is open)</span>
            <input className={INPUT} value={workspace} placeholder="C:/work/project" onChange={(event) => setWorkspace(event.target.value)} />
          </label>
        )}
        <fieldset className="sm:col-span-2 rounded-xl border border-line bg-paper px-3 py-2.5">
          <legend className="px-1 text-[11.5px] font-medium text-muted">Workspace permission</legend>
          <label className="flex items-start gap-2">
            <input
              className="mt-0.5"
              type="checkbox"
              checked={readOnly}
              aria-label="Read-only task"
              aria-describedby="read-only-task-help"
              onChange={(event) => setReadOnly(event.target.checked)}
            />
            <span>
              <span className="block text-[11.5px] font-medium text-ink">Read-only task</span>
              <span id="read-only-task-help" className="mt-0.5 block text-[10px] leading-relaxed text-muted">
                Hard permission boundary: agents may inspect and analyze, but cannot modify or publish workspace files. This is explicit and is never inferred from the Objective.
              </span>
            </span>
          </label>
        </fieldset>
        {writableCodeRoleConflict && (
          <div id="primary-role-conflict" className="sm:col-span-2 rounded-lg border border-warnInk/30 bg-warn/10 px-3 py-2 text-[10.5px] leading-relaxed text-warnInk" role="alert">
            <span className="font-semibold">Primary profile cannot start writable code work.</span> {PRIMARY_ROLE_CONFLICT_MESSAGE}
          </div>
        )}
        <label>
          <span className="mb-1 block text-[11.5px] font-medium text-muted">Acceptance criteria · one per line</span>
          <textarea className={`${INPUT} min-h-28 resize-y`} value={criteria} onChange={(event) => setCriteria(event.target.value)} />
        </label>
        <label>
          <span className="mb-1 block text-[11.5px] font-medium text-muted">Constraints · one per line</span>
          <textarea className={`${INPUT} min-h-28 resize-y`} value={constraints} onChange={(event) => setConstraints(event.target.value)} />
        </label>
      </div>
      {wizardStep === 2 && (
        <section className={`${CARD} mt-4 grid gap-3 bg-paper p-4 sm:grid-cols-2`} aria-label="Structured Brief fields">
          <div className="sm:col-span-2"><SectionHead title="Step 2 · Structured Brief" /></div>
          <label className="sm:col-span-2"><span className="mb-1 block text-[11.5px] font-medium text-muted">Background</span><textarea className={`${INPUT} min-h-20 resize-y`} value={background} onChange={(event) => setBackground(event.target.value)} /></label>
          <label><span className="mb-1 block text-[11.5px] font-medium text-muted">Included paths/components · one per line</span><textarea className={`${INPUT} min-h-24 resize-y`} value={includedScope} onChange={(event) => setIncludedScope(event.target.value)} /></label>
          <label><span className="mb-1 block text-[11.5px] font-medium text-muted">Excluded paths/components · one per line</span><textarea className={`${INPUT} min-h-24 resize-y`} value={excludedScope} onChange={(event) => setExcludedScope(event.target.value)} /></label>
          <label><span className="mb-1 block text-[11.5px] font-medium text-muted">Ordered instructions · one per line</span><textarea className={`${INPUT} min-h-24 resize-y`} value={instructions} onChange={(event) => setInstructions(event.target.value)} /></label>
          <label><span className="mb-1 block text-[11.5px] font-medium text-muted">Non-goals · one per line</span><textarea className={`${INPUT} min-h-24 resize-y`} value={nonGoals} onChange={(event) => setNonGoals(event.target.value)} /></label>
          <label className="sm:col-span-2"><span className="mb-1 block text-[11.5px] font-medium text-muted">Deliverables · kind:title, one per line</span><textarea className={`${INPUT} min-h-20 resize-y`} value={deliverables} onChange={(event) => setDeliverables(event.target.value)} /></label>
          <div className="sm:col-span-2 grid grid-cols-4 gap-2 text-[10px]">
            {([['Objective', Boolean(objective.trim())], ['Scope', true], ['Criteria', Boolean(lines(criteria).length)], ['Deliverables', Boolean(lines(deliverables).length)]] as Array<[string, boolean]>).map(([label, complete]) => <span key={label} className={`rounded-lg border px-2 py-1.5 ${complete ? "border-okLine bg-okSoft text-ok" : "border-warnInk/20 bg-warnSoft text-warnInk"}`}>{complete ? "✓" : "○"} {label}</span>)}
          </div>
        </section>
      )}
      {wizardStep === 3 && (
        <section className={`${CARD} mt-4 bg-paper p-4`} aria-label="Context picker">
          <SectionHead title="Step 3 · Context manifest" aside={<span className="text-[10.5px] text-muted">{contextRefs.length} refs · on-demand by default</span>} />
          <div className="grid gap-2 sm:grid-cols-[1fr_1fr_auto]"><input className={INPUT} aria-label="New context path" placeholder="Workspace-relative file path" value={contextPath} onChange={(event) => setContextPath(event.target.value)} /><input className={INPUT} aria-label="New context selection reason" placeholder="Selection reason" value={contextReason} onChange={(event) => setContextReason(event.target.value)} /><button type="button" className={BUTTON} disabled={!contextPath.trim() || !contextReason.trim()} onClick={() => { setContextRefs((current) => [...current, { requirement: "recommended", ref_type: "file", display_name: contextPath.trim(), selection_reason: contextReason.trim(), locator: { relative_path: contextPath.trim() }, delivery_mode: "on_demand", trust_level: "operator_provided" }]); setContextPath(""); setContextReason(""); }}>Add ref</button></div>
          <div className="mt-3 space-y-1.5">{contextRefs.map((ref, index) => <div key={`${ref.display_name}-${index}`} className="flex items-center gap-2 rounded-lg border border-line bg-panel px-3 py-2 text-[11px]"><StatusBadge status={ref.requirement} /><span className="min-w-0 flex-1 truncate">{ref.display_name}</span><span className="text-faint">{humanize(ref.delivery_mode || "on_demand")}</span><button type="button" className={DANGER_BUTTON} onClick={() => setContextRefs((current) => current.filter((_, itemIndex) => itemIndex !== index))}>Remove</button></div>)}</div>
          <p className="mt-3 text-[10.5px] leading-relaxed text-muted">Only metadata enters the initial envelope. File bodies remain outside the prompt until an explicit read.</p>
        </section>
      )}
      {wizardStep === 4 && <div className={`${CARD} mt-4 bg-paper p-4 text-[11.5px] text-muted`}><SectionHead title="Step 4 · Execution" />The profile, routing policy, role-aware preset, workspace boundary, review, and test controls above define the frozen execution policy.</div>}
      {wizardStep === 5 && (
        <section className={`${CARD} mt-4 bg-paper p-4`} aria-label="Execution envelope preview">
          <SectionHead title="Step 5 · ExecutionEnvelope preview" />
          <dl className="grid gap-2 text-[11.5px] sm:grid-cols-[10rem_1fr]"><dt className="text-faint">Brief</dt><dd className="text-ink">{title.trim() || objective.trim().slice(0, 120) || "Incomplete"}</dd><dt className="text-faint">Context manifest</dt><dd className="text-ink">{contextRefs.length} refs; contents excluded</dd><dt className="text-faint">Role/profile</dt><dd className="text-ink">{selectedProfileRole ? humanize(selectedProfileRole) : "Unknown"} · {profileId}</dd><dt className="text-faint">Runtime</dt><dd className="text-ink">{runtimePresetId || requestedModel || "Automatic"}</dd><dt className="text-faint">Expected products</dt><dd className="text-ink">{lines(deliverables).join(", ") || "Incomplete"}</dd></dl>
        </section>
      )}
      {catalogError && <div className="mt-2 text-[11px] text-warnInk">Profiles and policies could not be refreshed; built-in defaults will be used. {catalogError}</div>}
      <div className="mt-3 flex flex-wrap gap-4 text-[11.5px] text-muted">
        <label className="flex items-center gap-2"><input type="checkbox" checked={Boolean(runtimePresetId) || requireReview} disabled={Boolean(runtimePresetId)} onChange={(event) => setRequireReview(event.target.checked)} /> Require reviewer</label>
        <label className="flex items-center gap-2"><input type="checkbox" checked={Boolean(runtimePresetId) || requireTests} disabled={Boolean(runtimePresetId)} onChange={(event) => setRequireTests(event.target.checked)} /> Require tester</label>
      </div>
      <div className="mt-5 flex justify-end gap-2">
        <button type="button" className={BUTTON} disabled={submitting} onClick={onCancel}>Cancel</button>
        <button type="button" className={BUTTON} disabled={submitting || !objective.trim() || writableCodeRoleConflict} onClick={() => void submit(null, false)}>
          {submitting ? "Saving…" : "Save draft"}
        </button>
        <button type="submit" className={PRIMARY_BUTTON} disabled={submitting || !objective.trim() || presetUnavailable || writableCodeRoleConflict}>
          {submitting ? "Creating…" : "Create and start"}
        </button>
      </div>
    </form>
  );
}

function TaskRow({ task, selected, onSelect }: { task: OrchestrationTaskSummary; selected: boolean; onSelect: () => void }) {
  return (
    <button
      className={`w-full rounded-xl px-3 py-2.5 text-left transition-colors ${
        selected ? "bg-accentSoft text-ink" : "text-ink hover:bg-paper"
      }`}
      onClick={onSelect}
    >
      <div className="flex items-start gap-2">
        <span className="min-w-0 flex-1 truncate text-[12.5px] font-medium">{task.title}</span>
        {!!task.attention_count && (
          <span className="min-w-5 rounded-full bg-warnSoft px-1.5 text-center text-[10px] font-semibold leading-5 text-warnInk">
            {task.attention_count}
          </span>
        )}
      </div>
      <div className="mt-1 flex items-center gap-1.5">
        <StatusBadge status={task.status} />
        <span className="truncate text-[10.5px] text-faint">{task.stage === "archive" ? (task.status === "running" ? "Finalizing" : "Finalized") : humanize(task.stage)}</span>
        <span className="ml-auto shrink-0 text-[10px] text-faint">{formatTime(task.updated_at)}</span>
      </div>
    </button>
  );
}

function TaskDetailView({
  api,
  task,
  resolving,
  onResolve,
  onRefresh,
  onAction,
  onViewRun,
  onOpenProfile,
  onOpenPolicy,
  apiDownload,
  onSelectTask,
  auditPageLoading,
  auditPageError,
  onLoadOlderAttention,
  onLoadOlderRuns,
  onLoadOlderEvidence,
}: {
  api: OrchestrationApi;
  task: OrchestrationTaskDetail;
  resolving: boolean;
  onResolve: (
    gateId: string,
    decision: string,
    response?: string,
    expectedVersion?: number,
    idempotencyKey?: string,
  ) => Promise<void>;
  onRefresh: () => void;
  onAction: (action: TaskAction) => Promise<void>;
  onViewRun: (runId: string) => void;
  onOpenProfile?: (profileId: string) => void;
  onOpenPolicy?: (policyId: string) => void;
  apiDownload: ApiDownload;
  onSelectTask: (taskId: string) => void;
  auditPageLoading: "attention" | "runs" | "evidence" | "";
  auditPageError: { kind: "attention" | "runs" | "evidence"; message: string } | null;
  onLoadOlderAttention: () => void;
  onLoadOlderRuns: () => void;
  onLoadOlderEvidence: () => void;
}) {
  const [tab, setTab] = useState<DetailTab>("graph");
  const [graphMode, setGraphMode] = useState<GraphMode>("dag");
  const [actionBusy, setActionBusy] = useState<TaskAction | "">("");
  const [actionError, setActionError] = useState<string | null>(null);
  const workProductCount = task.handoff_summary?.work_products?.count || 0;
  const previousTaskState = useRef({ id: "", status: "" });
  useEffect(() => {
    const previous = previousTaskState.current;
    const switchedTask = previous.id !== task.id;
    const resultBecameReady = (
      previous.id === task.id
      && previous.status !== task.status
      && ["completed", "archived"].includes(task.status)
    );
    if (switchedTask || resultBecameReady) {
      setTab(
        ["waiting_human", "completed", "archived"].includes(task.status) && workProductCount > 0
          ? "products"
          : "graph",
      );
      setActionBusy("");
      setActionError(null);
    }
    previousTaskState.current = { id: task.id, status: task.status };
  }, [task.id, task.status, workProductCount]);
  const pendingAttention = (task.attention || []).filter((gate) => gate.status === "pending");
  const canSubmit = task.status === "draft";
  const canPause = ["queued", "running", "waiting_human"].includes(task.status);
  const canResume = ["paused", "blocked", "needs_reconciliation"].includes(task.status);
  const canCancel = !["completed", "failed", "canceled", "cancelled", "archived", "canceling"].includes(task.status);
  const canArchive = ["completed", "failed", "canceled", "cancelled"].includes(task.status);
  const canRestore = task.status === "archived" && task.terminal_outcome === "completed";

  const act = async (action: TaskAction) => {
    if (action === "cancel" && !window.confirm("Cancel this task and all active descendants?")) return;
    setActionBusy(action);
    setActionError(null);
    try {
      await onAction(action);
    } catch (caught) {
      setActionError(errorMessage(caught));
    } finally {
      setActionBusy("");
    }
  };

  return (
    <div data-testid="task-detail">
      <header className="mb-4">
        <div className="flex items-start gap-4">
          <div className="min-w-0 flex-1">
            <div className="mb-1.5 flex items-center gap-2">
              <StatusBadge status={task.status} />
              {task.handoff_summary?.protocol === "legacy" && <StatusBadge status="draft" label="Legacy handoff" />}
              {!!task.handoff_summary?.wakes?.pending && <StatusBadge status="pending" label={`${task.handoff_summary.wakes.pending} pending wake${task.handoff_summary.wakes.pending === 1 ? "" : "s"}`} />}
              {!!task.handoff_summary?.wakes?.failed && <button onClick={() => setTab("wakes")}><StatusBadge status="failed" label={`${task.handoff_summary.wakes.failed} failed wake${task.handoff_summary.wakes.failed === 1 ? "" : "s"}`} /></button>}
              <span className="text-[11px] text-faint">Updated {formatTime(task.updated_at)}</span>
            </div>
            <h1 className="truncate text-[20px] font-semibold tracking-[-0.01em] text-ink">{task.title}</h1>
            {task.objective && <p className="mt-1 max-w-3xl text-[12.5px] leading-relaxed text-muted">{task.objective}</p>}
            <div className="mt-1 flex flex-wrap gap-2 text-[10.5px] text-faint">
              <code>{task.id}</code>
              {task.status === "waiting_child" && <span>Waiting for {(task.children || []).filter((child) => !["completed", "failed", "canceled", "archived"].includes(child.status)).length} active child task(s)</span>}
              {task.status === "blocked" && <button className="text-accent" onClick={() => setTab("dependencies")}>Open blocker details</button>}
              {task.status === "waiting_human" && <span>Waiting on {pendingAttention.map((gate) => humanize(gate.kind)).join(", ") || "operator input"}</span>}
              {task.status === "needs_reconciliation" && <button className="text-accent" onClick={() => setTab("wakes")}>Open wake diagnostics</button>}
            </div>
          </div>
          <div className="flex shrink-0 flex-wrap justify-end gap-2">
            {canSubmit && <button className={PRIMARY_BUTTON} disabled={!!actionBusy} onClick={() => void act("submit")}>{actionBusy === "submit" ? "Starting…" : "Start"}</button>}
            {canPause && <button className={BUTTON} disabled={!!actionBusy} onClick={() => void act("pause")}>{actionBusy === "pause" ? "Pausing…" : "Pause"}</button>}
            {canResume && <button className={PRIMARY_BUTTON} disabled={!!actionBusy} onClick={() => void act("resume")}>{actionBusy === "resume" ? "Resuming…" : "Resume"}</button>}
            {canCancel && <button className={DANGER_BUTTON} aria-label="Cancel task" disabled={!!actionBusy} onClick={() => void act("cancel")}>{actionBusy === "cancel" ? "Canceling…" : "Cancel"}</button>}
            {canArchive && <button className={BUTTON} disabled={!!actionBusy} onClick={() => void act("archive")}>{actionBusy === "archive" ? "Archiving…" : "Archive"}</button>}
            {canRestore && <button className={PRIMARY_BUTTON} disabled={!!actionBusy} onClick={() => void act("restore")}>{actionBusy === "restore" ? "Restoring…" : "Restore to Completed"}</button>}
            <button className={BUTTON} disabled={resolving || !!actionBusy} onClick={onRefresh}>
              <Icon name="refresh" size={13} /> Refresh
            </button>
            <button className={BUTTON} onClick={() => void navigator.clipboard?.writeText(task.id)}>Copy task ID</button>
          </div>
        </div>
        {actionError && <div className="mt-3"><ErrorNotice message={actionError} /></div>}
        {(task.profile_snapshot || task.model_policy_snapshot) && (
          <div className="mt-3 flex flex-wrap gap-2 text-[11px] text-muted">
            {task.profile_snapshot && (
              <button
                className="rounded-full border border-line bg-panel px-2.5 py-1 hover:border-lineStrong"
                onClick={() => onOpenProfile?.(task.profile_snapshot!.id)}
              >
                Profile: {task.profile_snapshot.name || task.profile_snapshot.id} v{task.profile_snapshot.version} · snapshot
              </button>
            )}
            {task.model_policy_snapshot && (
              <button
                className="rounded-full border border-line bg-panel px-2.5 py-1 hover:border-lineStrong"
                onClick={() => onOpenPolicy?.(task.model_policy_snapshot!.id)}
              >
                Routing: {task.model_policy_snapshot.name || task.model_policy_snapshot.id} v{task.model_policy_snapshot.version}
              </button>
            )}
          </div>
        )}
      </header>

      <StageTimeline current={task.stage} taskStatus={task.status} states={task.stages} />

      {["waiting_human", "completed", "archived"].includes(task.status) && workProductCount > 0 && (
        <section
          className="mt-4 flex flex-wrap items-center gap-3 rounded-xl2 border border-okLine bg-okSoft px-4 py-3"
          aria-label="Completed results"
          data-testid="completed-results-banner"
        >
          <div className="min-w-0 flex-1">
            <div className="text-[12.5px] font-medium text-ink">
              Final result is ready
            </div>
            <div className="mt-0.5 text-[11px] text-muted">
              {task.status === "waiting_human"
                ? `The declared deliverable and ${workProductCount} preserved work product${workProductCount === 1 ? "" : "s"} remain available while a decision is pending.`
                : task.status === "archived"
                  ? "This successfully completed result was filed in Archive. Restore it to keep it in the Completed list."
                  : `The declared deliverable is ready, with ${workProductCount} immutable work product${workProductCount === 1 ? "" : "s"} retained as evidence.`}
            </div>
          </div>
          <button className={PRIMARY_BUTTON} onClick={() => setTab("products")}>
            View final result
          </button>
        </section>
      )}

      {(!!task.children?.length || task.children_page?.truncated) && (
        <section className="mt-4" aria-label="Child agents">
          <SectionHead title={`Child agents · ${task.children_page?.total ?? task.children?.length ?? 0}`} />
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
            {(task.children || []).map((child) => (
              <button
                key={child.id}
                className={`${CARD} p-3 text-left hover:border-lineStrong`}
                onClick={() => onSelectTask(child.id)}
              >
                <div className="flex items-center gap-2">
                  <span className="min-w-0 flex-1 truncate text-[12px] font-medium text-ink">{child.title}</span>
                  <StatusBadge status={child.status} />
                </div>
                <div className="mt-1 text-[10.5px] text-muted">{humanize(child.stage)}</div>
              </button>
            ))}
          </div>
          {task.children_page?.truncated && (
            <div className="mt-2 rounded-lg border border-warnInk/20 bg-warnSoft px-3 py-2 text-[10.5px] text-warnInk">
              {task.children_page.returned < task.children_page.total
                ? `Showing ${task.children_page.returned} of ${task.children_page.total} direct child agents.`
                : `Nested hierarchy is bounded to ${task.children_page.tree_row_limit} tasks and depth ${task.detail_limits?.child_depth ?? 3}.`}
              {" "}Open a child task to retrieve its independently bounded audit view.
            </div>
          )}
        </section>
      )}

      {pendingAttention.length > 0 && (
        <section className="mt-4" aria-label="Needs attention">
          <SectionHead title={`Needs attention · ${pendingAttention.length}`} />
          <div className="space-y-2.5">
            {pendingAttention.map((gate) => (
              <AttentionGateCard key={gate.id} gate={gate} onResolve={onResolve} />
            ))}
          </div>
        </section>
      )}

      {task.attention_page && (
        <AuditPaginationNotice
          kind="attention gates"
          loaded={(task.attention || []).length}
          hasMore={Boolean(task.attention_page.has_more)}
          nestedTruncated={false}
          snapshotLimit={task.detail_limits?.attention}
          loading={auditPageLoading === "attention"}
          error={auditPageError?.kind === "attention" ? auditPageError.message : null}
          onLoadOlder={onLoadOlderAttention}
        />
      )}

      <div className="mt-5 flex items-center overflow-x-auto border-b border-line">
        {([
          ["brief", "Brief", task.brief?.revision || 0],
          ["context", "Context", task.handoff_summary?.context?.ref_count || 0],
          ["dependencies", "Dependencies", Object.values(task.handoff_summary?.relations || {}).reduce((sum, value) => sum + Number(value || 0), 0)],
          ["communication", "Communication", task.handoff_summary?.comments?.count || 0],
          ["products", "Results", task.handoff_summary?.work_products?.count || 0],
          ["wakes", "Wakes", task.handoff_summary?.wakes?.count || 0],
          ["graph", "Work graph", task.nodes?.length || 0],
          ["runs", "Agent runs", taskRuns(task).length],
          ["evidence", "Evidence", task.evidence?.length || 0],
          ["activity", "Activity", task.activity?.length || 0],
        ] as Array<[DetailTab, string, number]>).map(([value, label, count]) => {
          const truncated = value === "runs"
            ? Boolean(task.runs_page?.has_more || (task.children_details || []).some((child) => child.runs_page?.has_more))
            : value === "evidence"
              ? Boolean(task.evidence_page?.has_more)
              : false;
          return (
          <button
            key={value}
            className={`shrink-0 border-b-2 px-3 py-2 text-[12.5px] ${
              tab === value ? "border-accent font-medium text-accent" : "border-transparent text-muted hover:text-ink"
            }`}
            onClick={() => setTab(value)}
          >
            {label} <span className="text-[10px] text-faint">{count}{truncated ? "+" : ""}</span>
          </button>
          );
        })}
      </div>

      <div className="py-4">
        {(["brief", "context", "dependencies", "communication", "products", "wakes"] as HandoffPanelKind[]).includes(tab as HandoffPanelKind) && (
          <TaskHandoffPanel
            key={`${task.id}:${tab}`}
            api={api}
            task={task}
            kind={tab as HandoffPanelKind}
            onTaskRefresh={onRefresh}
            apiDownload={apiDownload}
            onSelectTask={onSelectTask}
          />
        )}
        {tab === "graph" && (
          <WorkGraph nodes={task.nodes || []} mode={graphMode} onModeChange={setGraphMode} />
        )}
        {tab === "runs" && (
          <RunTree
            runs={taskRuns(task)}
            page={task.runs_page}
            snapshotLimit={task.detail_limits?.runs}
            nestedTruncated={(task.children_details || []).some((child) => Boolean(child.runs_page?.has_more))}
            loading={auditPageLoading === "runs"}
            error={auditPageError?.kind === "runs" ? auditPageError.message : null}
            onLoadOlder={onLoadOlderRuns}
            onViewRun={onViewRun}
          />
        )}
        {tab === "evidence" && (
          <EvidenceList
            task={task}
            snapshotLimit={task.detail_limits?.evidence}
            loading={auditPageLoading === "evidence"}
            error={auditPageError?.kind === "evidence" ? auditPageError.message : null}
            onLoadOlder={onLoadOlderEvidence}
            onViewRun={onViewRun}
            apiDownload={apiDownload}
          />
        )}
        {tab === "activity" && <ActivityList task={task} />}
      </div>
    </div>
  );
}

function AttentionGateCard({
  gate,
  onResolve,
}: {
  gate: AttentionGate;
  onResolve: (
    gateId: string,
    decision: string,
    response?: string,
    expectedVersion?: number,
    idempotencyKey?: string,
  ) => Promise<void>;
}) {
  const [response, setResponse] = useState("");
  const [busy, setBusy] = useState("");
  const [error, setError] = useState<string | null>(null);
  const operationKeys = useRef(new Map<string, string>());
  const actions: AttentionAction[] = gate.actions?.length
    ? gate.actions
    : gate.kind === "question"
      ? [{ id: "answer", label: "Submit answer", tone: "primary", requires_response: true }]
      : [
          { id: "approve", label: "Approve", tone: "primary" },
          { id: "reject", label: "Reject", tone: "danger", requires_response: gate.kind === "review" },
        ];
  const asksForResponse = gate.kind === "question" || gate.kind === "conflict" || actions.some((action) => action.requires_response);

  const resolve = async (action: AttentionAction) => {
    if (action.requires_response && !response.trim()) return;
    setBusy(action.id);
    setError(null);
    try {
      const normalizedResponse = response.trim();
      const fingerprint = JSON.stringify([action.id, normalizedResponse, gate.version ?? null]);
      const idempotencyKey = operationKeys.current.get(fingerprint)
        || createClientIdempotencyKey(`gate-${gate.id}`);
      operationKeys.current.set(fingerprint, idempotencyKey);
      await onResolve(gate.id, action.id, normalizedResponse || undefined, gate.version, idempotencyKey);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy("");
    }
  };

  return (
    <article className="rounded-xl2 border border-warnInk/25 bg-warnSoft/60 px-4 py-3" data-testid={`attention-${gate.id}`}>
      <div className="flex items-start gap-3">
        <span className="mt-0.5 grid h-7 w-7 shrink-0 place-items-center rounded-full bg-panel text-warnInk">!</span>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <h3 className="text-[13px] font-medium text-ink">{gate.title}</h3>
            <span className="text-[10px] uppercase tracking-wide text-warnInk">{humanize(gate.kind)}</span>
          </div>
          {gate.description && <p className="mt-1 whitespace-pre-wrap text-[12px] leading-relaxed text-muted">{gate.description}</p>}
          <ReconciliationFailureDetails gate={gate} />
          <AcceptanceGateDetails gate={gate} />
          {asksForResponse && (
            <textarea
              className={`${INPUT} mt-2 min-h-16 resize-y bg-panel`}
              aria-label={`Response for ${gate.title}`}
              placeholder={gate.response_placeholder || "Add context or feedback…"}
              value={response}
              onChange={(event) => setResponse(event.target.value)}
            />
          )}
          {error && <div role="alert" className="mt-2 text-[11.5px] text-danger">{error}</div>}
          <div className="mt-2.5 flex flex-wrap gap-2">
            {actions.map((action) => (
              <button
                key={action.id}
                className={action.tone === "primary" ? PRIMARY_BUTTON : action.tone === "danger" ? DANGER_BUTTON : BUTTON}
                disabled={!!busy || (!!action.requires_response && !response.trim())}
                onClick={() => void resolve(action)}
              >
                {busy === action.id ? "Working…" : action.label}
              </button>
            ))}
          </div>
        </div>
      </div>
    </article>
  );
}

function RunDiagnostic({ run, compact = false }: { run: AgentRun; compact?: boolean }) {
  const errorKind = boundedDisplayText(run.error_kind, ERROR_KIND_DISPLAY_LIMIT);
  const summary = boundedDisplayText(run.summary, ERROR_MESSAGE_DISPLAY_LIMIT);
  const error = boundedDisplayText(run.error_message, ERROR_MESSAGE_DISPLAY_LIMIT);
  const separateError = error && error !== summary ? error : "";
  if (!errorKind && !summary && !separateError) return null;
  const failed = Boolean(errorKind) || ["failed", "timed_out", "lost"].includes(run.status);
  return (
    <span
      className={`mt-1.5 block rounded-md border px-2 py-1.5 ${failed ? "border-danger/20 bg-dangerSoft/70" : "border-line bg-paper"}`}
      aria-label={`${failed ? "Failure details" : "Summary"} for ${boundedDisplayText(run.title, 240)}`}
    >
      {errorKind && (
        <span className="block font-mono text-[10px] text-danger">
          <span className="font-sans font-medium">Error kind: </span>{errorKind}
        </span>
      )}
      {summary && <span className={`block whitespace-pre-wrap break-words text-muted ${compact ? "text-[10.5px]" : "text-[11px]"}`}>{summary}</span>}
      {separateError && <span className={`block whitespace-pre-wrap break-words text-danger ${summary ? "mt-1" : ""} ${compact ? "text-[10.5px]" : "text-[11px]"}`}>{separateError}</span>}
    </span>
  );
}

function ReconciliationFailureDetails({ gate }: { gate: AttentionGate }) {
  const groups = [
    { label: "Failed runs", runs: gate.failed_runs || [] },
    { label: "Workspace publication failures", runs: gate.workspace_commit_failures || [] },
  ].filter((group) => group.runs.length > 0);
  if (!groups.length) return null;
  return (
    <div className="mt-3 space-y-2.5" data-testid="reconciliation-failures">
      {groups.map((group) => {
        const visible = group.runs.slice(0, RECONCILIATION_RUN_DISPLAY_LIMIT);
        return (
          <section key={group.label} className={`${CARD} bg-panel/80 p-3`} aria-label={group.label}>
            <h4 className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-faint">{group.label}</h4>
            <div className="space-y-2">
              {visible.map((run) => (
                <article key={run.id} className="border-b border-line pb-2 last:border-0 last:pb-0">
                  <div className="flex items-start gap-2">
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-[11.5px] font-medium text-ink">{boundedDisplayText(run.title, 240)}</div>
                      <div className="mt-0.5 text-[10px] text-faint">
                        {[run.agent_name, run.model_id, run.attempt ? `attempt ${run.attempt}` : ""]
                          .filter(Boolean)
                          .map((value) => boundedDisplayText(value, 240))
                          .join(" · ")}
                      </div>
                    </div>
                    <StatusBadge status={run.status} />
                  </div>
                  <RunDiagnostic run={run} compact />
                </article>
              ))}
            </div>
            {group.runs.length > visible.length && (
              <div className="mt-2 text-[10px] text-muted">
                {group.runs.length - visible.length} additional failures are available in the run ledger.
              </div>
            )}
          </section>
        );
      })}
    </div>
  );
}

function AcceptanceStatus({ status }: { status: string }) {
  const normalized = status.toLowerCase();
  const tone = normalized === "pass"
    ? "bg-okSoft text-ok"
    : normalized === "fail"
      ? "bg-dangerSoft text-danger"
      : "bg-paper text-muted";
  return <span className={`rounded-full px-2 py-0.5 text-[9.5px] font-semibold uppercase tracking-wide ${tone}`}>{humanize(normalized)}</span>;
}

function AcceptanceGateDetails({ gate }: { gate: AttentionGate }) {
  const criteria = Object.entries(gate.criteria || {});
  const verification = gate.verification || [];
  const policyReasons = gate.policy_reasons || [];
  if (!criteria.length && !verification.length && !policyReasons.length) return null;

  return (
    <div className="mt-3 grid gap-2.5 lg:grid-cols-2" data-testid="final-acceptance-details">
      {criteria.length > 0 && (
        <section className={`${CARD} bg-panel/80 p-3`} aria-label="Acceptance criteria">
          <h4 className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-faint">Acceptance criteria</h4>
          <ul className="space-y-1.5">
            {criteria.map(([criterion, status]) => (
              <li key={criterion} className="flex items-start gap-2 text-[11.5px] text-ink">
                <span className="min-w-0 flex-1">{criterion}</span>
                <AcceptanceStatus status={status} />
              </li>
            ))}
          </ul>
        </section>
      )}
      {verification.length > 0 && (
        <section className={`${CARD} bg-panel/80 p-3`} aria-label="Independent verification">
          <h4 className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-faint">Independent verification</h4>
          <div className="space-y-2">
            {verification.map((report, index) => (
              <article key={`${report.node_id}:${report.run_id || index}`} className="border-b border-line pb-2 last:border-0 last:pb-0">
                <div className="flex items-center gap-2">
                  <span className="min-w-0 flex-1 truncate text-[11.5px] font-medium text-ink">
                    {humanize(report.role || report.node_key || report.node_id || "verification")}
                    {report.node_key && report.role ? ` · ${humanize(report.node_key)}` : ""}
                  </span>
                  <AcceptanceStatus status={report.status} />
                </div>
                {report.summary && <p className="mt-1 whitespace-pre-wrap break-words text-[10.5px] leading-relaxed text-muted">{report.summary}</p>}
                {report.findings.length > 0 && (
                  <ul className="mt-1 list-disc space-y-0.5 pl-4 text-[10.5px] text-muted">
                    {report.findings.map((finding, findingIndex) => <li key={`${findingIndex}:${finding}`}>{finding}</li>)}
                  </ul>
                )}
                {report.source && <div className="mt-1 truncate font-mono text-[9.5px] text-faint">Source: {report.source}</div>}
              </article>
            ))}
          </div>
        </section>
      )}
      {policyReasons.length > 0 && (
        <section className={`${CARD} bg-panel/80 p-3 lg:col-span-2`} aria-label="Acceptance policy reasons">
          <h4 className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-faint">Policy reasons</h4>
          <ul className="list-disc space-y-0.5 pl-4 text-[10.5px] text-muted">
            {policyReasons.map((reason, index) => <li key={`${index}:${reason}`}>{reason}</li>)}
          </ul>
        </section>
      )}
    </div>
  );
}

function nodeLayers(nodes: TaskNode[]): TaskNode[][] {
  const pending = new Map(nodes.map((node) => [node.id, node]));
  const placed = new Set<string>();
  const layers: TaskNode[][] = [];
  while (pending.size) {
    const layer = [...pending.values()].filter((node) => node.depends_on.every((id) => placed.has(id) || !pending.has(id)));
    const next = layer.length ? layer : [...pending.values()];
    layers.push(next);
    for (const node of next) {
      pending.delete(node.id);
      placed.add(node.id);
    }
    if (!layer.length) break;
  }
  return layers;
}

function taskRuns(task: OrchestrationTaskDetail): AgentRun[] {
  const runs = new Map<string, AgentRun>();
  const visit = (current: OrchestrationTaskDetail) => {
    for (const run of current.runs || []) runs.set(run.id, run);
    for (const child of current.children_details || []) visit(child);
  };
  visit(task);
  return [...runs.values()];
}

function WorkGraph({ nodes, mode, onModeChange }: { nodes: TaskNode[]; mode: GraphMode; onModeChange: (mode: GraphMode) => void }) {
  if (!nodes.length) return <EmptyState title="No work graph yet" detail="The graph appears after planning creates executable steps." />;
  const layers = nodeLayers(nodes);
  return (
    <section>
      <SectionHead
        title="Plan structure"
        aside={
          <Segmented
            value={mode}
            onChange={onModeChange}
            label="Graph view"
            options={[{ value: "dag", label: "DAG" }, { value: "list", label: "List" }]}
          />
        }
      />
      {mode === "list" ? (
        <div className={`${CARD} divide-y divide-line`}>
          {nodes.map((node, index) => <WorkNodeRow key={node.id} node={node} index={index} />)}
        </div>
      ) : (
        <div className={`${CARD} overflow-x-auto p-4`} data-testid="dag-view">
          <div className="flex min-w-max items-stretch gap-8">
            {layers.map((layer, layerIndex) => (
              <div key={layerIndex} className="relative w-56 space-y-3">
                <div className="text-[10px] font-semibold uppercase tracking-wide text-faint">Wave {layerIndex + 1}</div>
                {layer.map((node) => (
                  <article key={node.id} className="relative rounded-xl border border-line bg-paper px-3 py-2.5">
                    {layerIndex < layers.length - 1 && <span aria-hidden className="absolute -right-8 top-1/2 w-8 border-t border-lineStrong" />}
                    <div className="flex items-start gap-2">
                      <span className="min-w-0 flex-1 text-[12px] font-medium text-ink">{node.title}</span>
                      <StatusBadge status={node.status} />
                    </div>
                    {node.kind && <div className="mt-1 text-[10.5px] text-muted">{humanize(node.kind)}</div>}
                    {!!node.depends_on.length && <div className="mt-1.5 truncate text-[10px] text-faint">After: {node.depends_on.join(", ")}</div>}
                    {node.profile_name && (
                      <div className="mt-1.5 text-[10px] text-muted">{node.profile_name}{node.profile_version ? ` v${node.profile_version}` : ""}</div>
                    )}
                  </article>
                ))}
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function WorkNodeRow({ node, index }: { node: TaskNode; index: number }) {
  return (
    <div className="flex items-start gap-3 px-3.5 py-3">
      <span className="grid h-6 w-6 shrink-0 place-items-center rounded-full bg-paper text-[10px] text-muted">{index + 1}</span>
      <div className="min-w-0 flex-1">
        <div className="text-[12.5px] font-medium text-ink">{node.title}</div>
        {node.description && <div className="mt-0.5 text-[11.5px] text-muted">{node.description}</div>}
        {!!node.depends_on.length && <div className="mt-1 text-[10.5px] text-faint">Depends on {node.depends_on.join(", ")}</div>}
      </div>
      <StatusBadge status={node.status} />
    </div>
  );
}

function AuditPaginationNotice({
  kind,
  loaded,
  hasMore,
  nestedTruncated,
  snapshotLimit,
  loading,
  error,
  onLoadOlder,
}: {
  kind: string;
  loaded: number;
  hasMore: boolean;
  nestedTruncated: boolean;
  snapshotLimit?: number;
  loading: boolean;
  error: string | null;
  onLoadOlder: () => void;
}) {
  if (!hasMore && !nestedTruncated && !error) return null;
  return (
    <div className="mb-3 rounded-lg border border-warnInk/20 bg-warnSoft/60 px-3 py-2 text-[11px] text-warnInk" role="status">
      <div className="flex flex-wrap items-center gap-2">
        <span className="min-w-0 flex-1">
          {hasMore
            ? `Showing ${loaded} loaded ${kind}; older records are not shown yet.`
            : `All ${loaded} loaded ${kind} for this task are shown.`}
          {nestedTruncated ? " Some child-task run summaries are bounded; open the child task for its complete history." : ""}
          {snapshotLimit ? ` Detail snapshot limit: ${snapshotLimit}.` : ""}
        </span>
        {hasMore && (
          <button type="button" className={BUTTON} disabled={loading} onClick={onLoadOlder}>
            {loading ? "Loading older…" : "Load older"}
          </button>
        )}
      </div>
      {error && <div className="mt-1 text-danger">Could not load older records: {error}</div>}
    </div>
  );
}

function RunTree({
  runs,
  page,
  snapshotLimit,
  nestedTruncated,
  loading,
  error,
  onLoadOlder,
  onViewRun,
}: {
  runs: AgentRun[];
  page?: AuditPage;
  snapshotLimit?: number;
  nestedTruncated: boolean;
  loading: boolean;
  error: string | null;
  onLoadOlder: () => void;
  onViewRun: (runId: string) => void;
}) {
  if (!runs.length) return <EmptyState title="No agent runs yet" detail="Runs appear when execution begins." />;
  const byParent = new Map<string, AgentRun[]>();
  const runIds = new Set(runs.map((run) => run.id));
  for (const run of runs) {
    const key = run.parent_run_id || "__root__";
    byParent.set(key, [...(byParent.get(key) || []), run]);
  }
  const naturalRoots = runs.filter((run) => !run.parent_run_id || !runIds.has(run.parent_run_id));
  const roots = naturalRoots.length ? naturalRoots : runs;
  return (
    <section>
      <SectionHead title="Run tree" />
      <AuditPaginationNotice
        kind="runs"
        loaded={runs.length}
        hasMore={Boolean(page?.has_more)}
        nestedTruncated={nestedTruncated}
        snapshotLimit={snapshotLimit}
        loading={loading}
        error={error}
        onLoadOlder={onLoadOlder}
      />
      <div className={`${CARD} p-3`}>
        {roots.map((run) => (
          <RunTreeNode key={run.id} run={run} childrenByParent={byParent} depth={0} onViewRun={onViewRun} visited={new Set()} />
        ))}
      </div>
    </section>
  );
}

function RunTreeNode({
  run,
  childrenByParent,
  depth,
  onViewRun,
  visited,
}: {
  run: AgentRun;
  childrenByParent: Map<string, AgentRun[]>;
  depth: number;
  onViewRun: (runId: string) => void;
  visited: Set<string>;
}) {
  if (visited.has(run.id) || depth > 12) return null;
  const nextVisited = new Set(visited).add(run.id);
  const children = childrenByParent.get(run.id) || [];
  const displayTitle = boundedDisplayText(run.title, 240);
  return (
    <div className={depth ? "ml-6 border-l border-line pl-3" : ""}>
      <div className="mb-1.5 flex items-start gap-1 rounded-lg hover:bg-paper">
      <button
        className="flex min-w-0 flex-1 items-start gap-3 px-2.5 py-2 text-left disabled:cursor-default"
        title="View agent run details"
        onClick={() => onViewRun(run.id)}
      >
        <span className="mt-0.5 grid h-6 w-6 shrink-0 place-items-center rounded-full bg-accentSoft text-accent">
          <Icon name="sparkle" size={12} />
        </span>
        <span className="min-w-0 flex-1">
          <span className="block text-[12.5px] font-medium text-ink">{displayTitle}</span>
          <span className="mt-0.5 block text-[10.5px] text-muted">
            {[run.agent_name, run.model_id, run.attempt ? `attempt ${run.attempt}` : ""]
              .filter(Boolean)
              .map((value) => boundedDisplayText(value, 240))
              .join(" · ")}
          </span>
          {run.routing_reason && <span className="mt-0.5 block text-[10.5px] text-faint">Auto-routed: {boundedDisplayText(run.routing_reason, 400)}</span>}
          <RunDiagnostic run={run} />
        </span>
        <StatusBadge status={run.status} />
      </button>
      <button
        type="button"
        className="mr-2 mt-2 shrink-0 rounded px-2 py-1 text-[10.5px] text-accent hover:bg-accentSoft"
        aria-label="View Agent progress"
        title={`View live progress for ${displayTitle}`}
        onClick={() => onViewRun(run.id)}
      >
        Progress
      </button>
      </div>
      {children.map((child) => (
        <RunTreeNode
          key={child.id}
          run={child}
          childrenByParent={childrenByParent}
          depth={depth + 1}
          onViewRun={onViewRun}
          visited={nextVisited}
        />
      ))}
    </div>
  );
}

function EvidenceList({
  task,
  snapshotLimit,
  loading,
  error,
  onLoadOlder,
  onViewRun,
  apiDownload,
}: {
  task: OrchestrationTaskDetail;
  snapshotLimit?: number;
  loading: boolean;
  error: string | null;
  onLoadOlder: () => void;
  onViewRun: (runId: string) => void;
  apiDownload: ApiDownload;
}) {
  const evidence = task.evidence || [];
  if (!evidence.length) return <EmptyState title="No evidence captured" detail="Artifacts, sources, tests, and accepted claims appear here." />;
  return (
    <section>
      <SectionHead title="Evidence ledger" />
      <AuditPaginationNotice
        kind="evidence records"
        loaded={evidence.length}
        hasMore={Boolean(task.evidence_page?.has_more)}
        nestedTruncated={false}
        snapshotLimit={snapshotLimit}
        loading={loading}
        error={error}
        onLoadOlder={onLoadOlder}
      />
      <div className="grid grid-cols-1 gap-2.5 lg:grid-cols-2">
        {evidence.map((item) => (
          <article key={item.id} className={`${CARD} p-3.5`}>
            <div className="flex items-start gap-3">
              <span className="grid h-8 w-8 shrink-0 place-items-center rounded-lg bg-paper text-muted">
                <Icon name={item.kind === "file" || item.kind === "artifact" ? "file" : item.kind === "test" ? "code" : "audit"} size={14} />
              </span>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <h4 className="truncate text-[12.5px] font-medium text-ink">{item.title}</h4>
                  <span className="text-[9.5px] uppercase tracking-wide text-faint">{humanize(item.kind)}</span>
                </div>
                {item.summary && <p className="mt-1 text-[11.5px] leading-relaxed text-muted">{item.summary}</p>}
                {item.actor && <div className="mt-1 text-[10px] text-faint">Recorded by {item.actor}</div>}
                {item.subject_matches != null && (
                  <div className={`mt-1 text-[10.5px] ${item.subject_matches ? "text-accent" : "text-danger"}`}>
                    Subject {item.subject_matches ? "matches accepted candidate" : "does not match accepted candidate"}
                  </div>
                )}
                {!!item.missing_criteria?.length && (
                  <div className="mt-1 text-[10.5px] text-danger">
                    Missing criteria: {item.missing_criteria.join(", ")}
                  </div>
                )}
                {item.content_hash && (
                  <div className="mt-1 truncate font-mono text-[9.5px] text-faint" title={item.content_hash}>
                    sha256:{item.content_hash}
                  </div>
                )}
                <div className="mt-2 flex items-center gap-2 text-[10.5px]">
                  {item.uri && <EvidenceUri item={item} apiDownload={apiDownload} />}
                  {item.run_id && (
                    <button
                      className="ml-auto shrink-0 text-accent hover:underline disabled:cursor-not-allowed disabled:text-faint disabled:no-underline"
                      title="View agent run details"
                      onClick={() => {
                        onViewRun(item.run_id!);
                      }}
                    >
                      View run
                    </button>
                  )}
                  {item.run_id && (
                    <button
                      className="shrink-0 text-accent hover:underline"
                      onClick={() => {
                        onViewRun(item.run_id!);
                      }}
                    >
                      Transcript
                    </button>
                  )}
                </div>
                {item.subject && Object.keys(item.subject).length > 0 && (
                  <details className="mt-2 text-[10.5px] text-muted">
                    <summary className="cursor-pointer text-accent">Evidence subject</summary>
                    <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap rounded bg-paper p-2 font-mono text-[9.5px]">{JSON.stringify(item.subject, null, 2)}</pre>
                  </details>
                )}
                {item.payload && Object.keys(item.payload).length > 0 && (
                  <details className="mt-2 text-[10.5px] text-muted">
                    <summary className="cursor-pointer text-accent">Audit payload</summary>
                    <pre className="mt-1 max-h-48 overflow-auto whitespace-pre-wrap rounded bg-paper p-2 font-mono text-[9.5px]">{JSON.stringify(item.payload, null, 2)}</pre>
                  </details>
                )}
              </div>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

function EvidenceUri({ item, apiDownload }: { item: TaskEvidence; apiDownload: ApiDownload }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const digest = /^sha256:([a-f0-9]{64})$/i.exec(item.uri || "")?.[1]?.toLowerCase();
  if (!item.uri) return null;

  if (!digest) {
    if (/^https?:\/\//i.test(item.uri)) {
      return (
        <a className="min-w-0 truncate font-mono text-accent hover:underline" href={item.uri} target="_blank" rel="noreferrer">
          {item.uri}
        </a>
      );
    }
    return <span className="min-w-0 truncate font-mono text-faint">{item.uri}</span>;
  }

  const download = async () => {
    const path = `/v1/orchestration/blobs/${encodeURIComponent(digest)}`;
    setBusy(true);
    setError(null);
    try {
      await apiDownload(path, item.title);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  };

  return (
    <span className="min-w-0">
      <button
        type="button"
        className="block max-w-full truncate font-mono text-accent hover:underline disabled:opacity-50"
        title={item.uri}
        aria-label={`Download ${item.title}`}
        disabled={busy}
        onClick={() => void download()}
      >
        {busy ? "Downloading…" : item.uri}
      </button>
      {error && <span role="alert" className="mt-1 block text-danger">{error}</span>}
    </span>
  );
}

function ActivityList({ task }: { task: OrchestrationTaskDetail }) {
  const activity = task.activity || [];
  if (!activity.length) return <EmptyState title="No activity yet" />;
  return (
    <section>
      <SectionHead title="Activity" />
      {task.activity_page?.has_more && (
        <div className="mb-2 text-[10.5px] text-muted">
          Showing the latest {activity.length} events. Older audit events remain available through the paginated event API.
        </div>
      )}
      <div className={`${CARD} divide-y divide-line`}>
        {activity.map((item) => (
          <div key={item.id} className="flex gap-3 px-3.5 py-3">
            <span className="mt-1 h-2 w-2 shrink-0 rounded-full bg-accent" />
            <div className="min-w-0 flex-1">
              <div className="text-[12.5px] text-ink">{boundedDisplayText(item.summary, 400)}</div>
              {item.error_kind && (
                <div className="mt-1 font-mono text-[10px] text-danger">
                  <span className="font-sans font-medium">Error kind: </span>
                  {boundedDisplayText(item.error_kind, ERROR_KIND_DISPLAY_LIMIT)}
                </div>
              )}
              {item.error_message && (
                <div className="mt-0.5 whitespace-pre-wrap break-words text-[11.5px] text-danger">
                  {boundedDisplayText(item.error_message, ERROR_MESSAGE_DISPLAY_LIMIT)}
                </div>
              )}
              {item.detail && (
                <div className="mt-0.5 whitespace-pre-wrap break-words text-[11.5px] text-muted">
                  {boundedDisplayText(item.detail, ERROR_MESSAGE_DISPLAY_LIMIT)}
                </div>
              )}
              <div className="mt-1 text-[10px] text-faint">
                {[item.actor, item.stage ? humanize(item.stage) : "", formatTime(item.created_at)].filter(Boolean).join(" · ")}
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function groupedRunActivity(items: RunActivity[]): RunActivity[] {
  const grouped = new Map<string, RunActivity>();
  for (const item of items.filter((candidate) => candidate.kind !== "usage")) {
    const canCombine = item.kind === "tool"
      || item.kind === "reasoning_summary"
      || item.detail.content_withheld === true;
    const key = canCombine ? `${item.kind}:${item.source_id}` : item.id;
    const current = grouped.get(key);
    if (!current) {
      grouped.set(key, { ...item, detail: { ...item.detail } });
      continue;
    }
    const summary = item.kind === "reasoning_summary"
      ? `${current.summary}${item.summary}`
      : item.summary || current.summary;
    grouped.set(key, {
      ...current,
      sequence: item.sequence,
      status: item.status,
      title: item.title || current.title,
      summary,
      detail: { ...current.detail, ...item.detail },
      created_at: item.created_at,
    });
  }
  return [...grouped.values()].sort((left, right) => left.sequence - right.sequence);
}

function activityDetailText(value: unknown): string {
  if (typeof value === "string") return value;
  const serialized = JSON.stringify(value, null, 2);
  return serialized === undefined ? String(value ?? "") : serialized;
}

function RunDetailsModal({
  runId,
  run,
  transcript,
  loading,
  error,
  activity,
  activityLoading,
  activityError,
  activityHasOlder,
  onRefreshActivity,
  onLoadOlderActivity,
  onClose,
}: {
  runId: string;
  run?: AgentRun;
  transcript: RunTranscript | null;
  loading: boolean;
  error: string | null;
  activity: RunActivity[];
  activityLoading: boolean;
  activityError: string | null;
  activityHasOlder: boolean;
  onRefreshActivity: () => void;
  onLoadOlderActivity: () => void;
  onClose: () => void;
}) {
  const content = (value: unknown) => {
    if (typeof value === "string") return value;
    const serialized = JSON.stringify(value, null, 2);
    return serialized === undefined ? String(value ?? "") : serialized;
  };
  const metadata = [
    ["Agent", run?.agent_name],
    ["Model / runtime", run?.model_id],
    ["Node", run?.node_id],
    ["Attempt", run?.attempt == null ? undefined : String(run.attempt)],
    ["Parent run", run?.parent_run_id || undefined],
    ["Session", run?.session_id || transcript?.session_id || undefined],
    ["Started", run?.started_at ? formatTime(run.started_at) : undefined],
    ["Completed", run?.completed_at ? formatTime(run.completed_at) : undefined],
  ].filter((entry): entry is [string, string] => Boolean(entry[1]));
  const timeline = useMemo(() => groupedRunActivity(activity), [activity]);
  const latestUsage = [...activity].reverse().find((item) => item.kind === "usage");
  const totalTokens = Number(latestUsage?.detail.total_tokens || 0);
  const inputTokens = Number(latestUsage?.detail.input_tokens || 0);
  const cachedTokens = Number(latestUsage?.detail.cached_input_tokens || 0);
  const outputTokens = Number(latestUsage?.detail.output_tokens || 0);
  const tokenLimit = Number(run?.budget?.tokens || 0);
  const hasTokenLimit = Boolean(run?.budget && tokenLimit > 0);
  const completedTools = timeline.filter((item) => item.kind === "tool" && item.status === "completed").length;
  const activeSteps = timeline.filter((item) => item.status === "running" || item.status === "pending").length;
  return (
    <div className="fixed inset-0 z-50 grid place-items-center bg-black/30 p-6" role="dialog" aria-modal="true" aria-label="Agent run details">
      <section className="flex max-h-[88vh] w-full max-w-4xl flex-col rounded-xl border border-line bg-panel shadow-xl">
        <header className="flex items-center gap-3 border-b border-line px-4 py-3">
          <div className="min-w-0 flex-1">
            <div className="mb-0.5 text-[9.5px] font-semibold uppercase tracking-wide text-faint">Agent progress and run details</div>
            <div className="flex min-w-0 items-center gap-2">
              <h3 className="truncate text-[13.5px] font-semibold text-ink">{run?.title || transcript?.title || "Agent run"}</h3>
              {run && <StatusBadge status={run.status} />}
            </div>
            <div className="truncate font-mono text-[9.5px] text-faint">{runId}</div>
          </div>
          <button type="button" className={BUTTON} onClick={onClose}>Close</button>
        </header>
        <div className="min-h-40 overflow-y-auto p-4">
          <section aria-label="Run metadata">
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
              <div className={`${CARD} p-2.5`}>
                <div className="text-[9.5px] font-semibold uppercase tracking-wide text-faint">Run ID</div>
                <div className="mt-1 break-all font-mono text-[10.5px] text-ink">{runId}</div>
              </div>
              {metadata.map(([label, value]) => (
                <div key={label} className={`${CARD} p-2.5`}>
                  <div className="text-[9.5px] font-semibold uppercase tracking-wide text-faint">{label}</div>
                  <div className="mt-1 break-all text-[10.5px] text-ink">{value}</div>
                </div>
              ))}
            </div>
            {run?.routing_reason && (
              <div className={`${CARD} mt-2 p-2.5`}>
                <div className="text-[9.5px] font-semibold uppercase tracking-wide text-faint">Routing reason</div>
                <div className="mt-1 whitespace-pre-wrap break-words text-[10.5px] text-muted">{boundedDisplayText(run.routing_reason, 800)}</div>
              </div>
            )}
            {run && <RunDiagnostic run={run} />}
          </section>

          <section className="mt-4 border-t border-line pt-4" aria-label="Live Agent activity">
            <SectionHead
              title="Live Agent activity"
              aside={(
                <div className="flex items-center gap-2 text-[10px] text-faint">
                  <span>{activityLoading ? "Refreshing..." : "Live · refreshes every 1.5s"}</span>
                  <button
                    type="button"
                    className="rounded px-1.5 py-0.5 text-accent hover:bg-accentSoft"
                    onClick={onRefreshActivity}
                    disabled={activityLoading}
                  >
                    Refresh
                  </button>
                </div>
              )}
            />
            <div className="mb-3 rounded-lg border border-accent/20 bg-accentSoft/40 px-3 py-2 text-[10.5px] leading-relaxed text-muted">
              “Reasoning summary” is the provider’s safe summary, not private chain-of-thought. Tool rows retain execution metadata only; raw tool output and file contents are excluded.
            </div>
            {activityError && <ErrorNotice message={activityError} onRetry={onRefreshActivity} />}
            {activityHasOlder && (
              <div className="mb-3 text-center">
                <button
                  type="button"
                  className={BUTTON}
                  onClick={onLoadOlderActivity}
                  disabled={activityLoading}
                >
                  Load earlier steps
                </button>
              </div>
            )}
            {activity.length > 0 && (
              <div className="mb-3 grid grid-cols-2 gap-2 sm:grid-cols-4" aria-label="Live run metrics">
                <div className={`${CARD} p-2.5`}>
                  <div className="text-[9.5px] font-semibold uppercase tracking-wide text-faint">
                    {hasTokenLimit ? "Reported / run limit" : "Reported tokens · no run cap"}
                  </div>
                  <div className="mt-1 text-[13px] font-semibold text-ink">
                    {totalTokens ? totalTokens.toLocaleString() : "—"}
                    {hasTokenLimit ? ` / ${tokenLimit.toLocaleString()}` : ""}
                  </div>
                </div>
                <div className={`${CARD} p-2.5`}>
                  <div className="text-[9.5px] font-semibold uppercase tracking-wide text-faint">Input / cached</div>
                  <div className="mt-1 text-[12px] font-semibold text-ink">{inputTokens.toLocaleString()} / {cachedTokens.toLocaleString()}</div>
                </div>
                <div className={`${CARD} p-2.5`}>
                  <div className="text-[9.5px] font-semibold uppercase tracking-wide text-faint">Output tokens</div>
                  <div className="mt-1 text-[13px] font-semibold text-ink">{outputTokens.toLocaleString()}</div>
                </div>
                <div className={`${CARD} p-2.5`}>
                  <div className="text-[9.5px] font-semibold uppercase tracking-wide text-faint">Tools / active</div>
                  <div className="mt-1 text-[13px] font-semibold text-ink">{completedTools} / {activeSteps}</div>
                </div>
              </div>
            )}
            {activityLoading && !activity.length ? (
              <LoadingBlock label="Loading live Agent activity..." />
            ) : timeline.length ? (
              <div className="space-y-2" role="list" aria-label="Agent activity timeline">
                {timeline.map((item) => {
                  const detailEntries = Object.entries(item.detail)
                    .filter(([, value]) => value !== null && value !== undefined && value !== "")
                    .slice(0, 20);
                  return (
                    <details key={`${item.id}:${item.sequence}`} className={`${CARD} group`} role="listitem">
                      <summary
                        className="flex cursor-pointer list-none items-start gap-2.5 px-3 py-2.5 hover:bg-paper"
                        aria-label={`Expand ${item.title}`}
                      >
                        <span
                          aria-hidden="true"
                          className={`mt-1.5 h-2 w-2 shrink-0 rounded-full ${
                            item.status === "failed"
                              ? "bg-danger"
                              : item.status === "running" || item.status === "pending"
                                ? "bg-accent"
                                : item.status === "completed"
                                  ? "bg-ok"
                                  : "bg-faint"
                          }`}
                        />
                        <span className="min-w-0 flex-1">
                          <span className="flex flex-wrap items-center gap-2">
                            <span className="text-[11.5px] font-semibold text-ink">{item.title}</span>
                            <span className="rounded bg-paper px-1.5 py-0.5 text-[9px] uppercase tracking-wide text-faint">{humanize(item.kind)}</span>
                          </span>
                          {item.summary && (
                            <span className="mt-0.5 block truncate text-[10.5px] text-muted">{boundedDisplayText(item.summary, 240)}</span>
                          )}
                        </span>
                        <span className="shrink-0 text-right">
                          <StatusBadge status={item.status} />
                          <span className="mt-1 block text-[9px] text-faint">{formatTime(item.created_at)}</span>
                        </span>
                      </summary>
                      <div className="border-t border-line px-3 py-3">
                        {item.summary && (
                          <div className="whitespace-pre-wrap break-words text-[11px] leading-relaxed text-ink">{boundedDisplayText(item.summary, 4_000)}</div>
                        )}
                        {detailEntries.length > 0 && (
                          <dl className="mt-3 grid grid-cols-1 gap-x-4 gap-y-2 sm:grid-cols-2">
                            {detailEntries.map(([key, value]) => (
                              <div key={key} className="min-w-0">
                                <dt className="text-[9px] font-semibold uppercase tracking-wide text-faint">{humanize(key)}</dt>
                                <dd className="mt-0.5 whitespace-pre-wrap break-all font-mono text-[10px] text-muted">{boundedDisplayText(activityDetailText(value), 1_200)}</dd>
                              </div>
                            ))}
                          </dl>
                        )}
                      </div>
                    </details>
                  );
                })}
              </div>
            ) : (
              <EmptyState title="No live activity yet" detail="Queued runs will start reporting once an Agent claims them." />
            )}
          </section>

          <section className="mt-4 border-t border-line pt-4" aria-label="Retained transcript">
            <SectionHead
              title="Retained transcript"
              aside={transcript ? (
                <span className="text-[10px] text-faint">
                  {loading ? "Refreshing..." : `${transcript.message_count} messages`}
                </span>
              ) : undefined}
            />
          {loading && !transcript ? (
            <LoadingBlock label="Loading transcript..." />
          ) : error ? (
            <ErrorNotice message={error} />
          ) : transcript && !transcript.available ? (
            <EmptyState title="Transcript unavailable" detail="This durable run has no retained session transcript." />
          ) : transcript?.messages.length ? (
            <div className="space-y-3">
              {transcript.messages.map((message, index) => (
                <article key={index} className={`${CARD} p-3`}>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-faint">{message.role || "message"}</div>
                  <pre className="whitespace-pre-wrap break-words font-sans text-[11.5px] leading-relaxed text-ink">{content(message.content)}</pre>
                </article>
              ))}
              {transcript.has_more && <div className="text-center text-[10.5px] text-muted">Additional messages are available through the paginated transcript API.</div>}
            </div>
          ) : (
            <EmptyState title="No transcript messages" />
          )}
          </section>
        </div>
      </section>
    </div>
  );
}
