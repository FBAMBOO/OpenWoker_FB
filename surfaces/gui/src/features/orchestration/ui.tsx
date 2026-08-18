import type { ReactNode } from "react";
import { ORCHESTRATION_STAGES, type OrchestrationStage, type TaskStageState, type WorkStatus } from "./types";

export const CARD = "rounded-xl2 border border-line bg-panel";
export const INPUT =
  "w-full min-w-0 rounded-lg border border-line bg-paper px-3 py-2 text-[13px] text-ink outline-none placeholder:text-faint focus:border-accent disabled:opacity-60";
export const BUTTON =
  "inline-flex items-center justify-center gap-1.5 rounded-lg border border-line bg-paper px-3 py-1.5 text-[12.5px] text-ink hover:border-lineStrong disabled:cursor-not-allowed disabled:opacity-40";
export const PRIMARY_BUTTON =
  "inline-flex items-center justify-center gap-1.5 rounded-lg border border-accent bg-accent px-3 py-1.5 text-[12.5px] font-medium text-white disabled:cursor-not-allowed disabled:opacity-40";
export const DANGER_BUTTON =
  "inline-flex items-center justify-center gap-1.5 rounded-lg border border-danger/30 bg-dangerSoft px-3 py-1.5 text-[12.5px] text-danger disabled:opacity-40";

export const STAGE_LABELS: Record<OrchestrationStage, string> = {
  intake: "Intake",
  complexity_assessment: "Complexity",
  clarification: "Clarification",
  planning: "Planning",
  execution_review_test: "Execute · review · test",
  inter_step_evaluation: "Evaluation",
  final_acceptance: "Final acceptance",
  archive: "Finalize",
};

export function humanize(value: string): string {
  return value
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export function formatTime(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

const STATUS_CLASS: Record<string, string> = {
  completed: "bg-okSoft text-ok border-okLine",
  succeeded: "bg-okSoft text-ok border-okLine",
  running: "bg-accentSoft text-accent border-accent/20",
  ready: "bg-accentSoft text-accent border-accent/20",
  claimed: "bg-accentSoft text-accent border-accent/20",
  queued: "bg-paper text-muted border-line",
  waiting: "bg-warnSoft text-warnInk border-warnInk/20",
  waiting_human: "bg-warnSoft text-warnInk border-warnInk/20",
  waiting_child: "bg-warnSoft text-warnInk border-warnInk/20",
  waiting_gate: "bg-warnSoft text-warnInk border-warnInk/20",
  paused: "bg-warnSoft text-warnInk border-warnInk/20",
  blocked: "bg-warnSoft text-warnInk border-warnInk/20",
  needs_reconciliation: "bg-warnSoft text-warnInk border-warnInk/20",
  canceling: "bg-paper text-muted border-line",
  failed: "bg-dangerSoft text-danger border-danger/20",
  timed_out: "bg-dangerSoft text-danger border-danger/20",
  lost: "bg-dangerSoft text-danger border-danger/20",
  cancelled: "bg-paper text-muted border-line",
  canceled: "bg-paper text-muted border-line",
  archived: "bg-paper text-faint border-line",
  pending: "bg-paper text-muted border-line",
  draft: "bg-paper text-muted border-line",
  skipped: "bg-paper text-faint border-line",
};

export function StatusBadge({ status, label }: { status: string; label?: string }) {
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10.5px] font-medium ${
        STATUS_CLASS[status] || STATUS_CLASS.pending
      }`}
    >
      {label || humanize(status)}
    </span>
  );
}

export function SectionHead({ title, aside }: { title: string; aside?: ReactNode }) {
  return (
    <div className="mb-2.5 flex min-h-7 items-center gap-3">
      <h3 className="text-[12px] font-semibold uppercase tracking-[0.04em] text-muted">{title}</h3>
      {aside && <div className="ml-auto">{aside}</div>}
    </div>
  );
}

export function EmptyState({ title, detail }: { title: string; detail?: string }) {
  return (
    <div className={`${CARD} grid min-h-36 place-items-center px-5 py-8 text-center`}>
      <div>
        <div className="text-[13.5px] font-medium text-ink">{title}</div>
        {detail && <div className="mx-auto mt-1 max-w-md text-[12px] leading-relaxed text-muted">{detail}</div>}
      </div>
    </div>
  );
}

export function ErrorNotice({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div role="alert" className="rounded-xl border border-danger/20 bg-dangerSoft px-3.5 py-3 text-[12.5px] text-danger">
      <div className="flex items-center gap-3">
        <span className="min-w-0 flex-1">{message}</span>
        {onRetry && (
          <button className={BUTTON} onClick={onRetry}>
            Try again
          </button>
        )}
      </div>
    </div>
  );
}

export function LoadingBlock({ label = "Loading…" }: { label?: string }) {
  return <div className={`${CARD} px-4 py-8 text-center text-[12.5px] text-muted`}>{label}</div>;
}

function inferredStageStatus(
  stage: OrchestrationStage,
  current: string,
  taskStatus: string,
): WorkStatus {
  const stageIndex = ORCHESTRATION_STAGES.indexOf(stage);
  const currentIndex = ORCHESTRATION_STAGES.indexOf(current as OrchestrationStage);
  if (taskStatus === "completed") return "completed";
  if (stage === current) return taskStatus === "failed" ? "failed" : taskStatus === "blocked" ? "blocked" : "running";
  if (currentIndex >= 0 && stageIndex < currentIndex) return "completed";
  return "pending";
}

export function StageTimeline({
  current,
  taskStatus,
  states = [],
}: {
  current: string;
  taskStatus: string;
  states?: TaskStageState[];
}) {
  return (
    <div className={`${CARD} overflow-x-auto p-3.5`} aria-label="Task stages">
      <ol className="grid min-w-[920px] grid-cols-8 gap-0">
        {ORCHESTRATION_STAGES.map((stage, index) => {
          const attempts = states.filter((item) => item.stage === stage);
          const explicit = attempts[attempts.length - 1];
          const status = explicit?.status || inferredStageStatus(stage, current, taskStatus);
          const active = status === "running" || status === "waiting" || status === "blocked";
          return (
            <li key={stage} className="relative px-1 text-center" aria-current={active ? "step" : undefined}>
              {index > 0 && (
                <span
                  aria-hidden="true"
                  className={`absolute left-0 right-1/2 top-[9px] h-px ${status === "completed" ? "bg-ok" : "bg-lineStrong"}`}
                />
              )}
              {index < ORCHESTRATION_STAGES.length - 1 && (
                <span
                  aria-hidden="true"
                  className={`absolute left-1/2 right-0 top-[9px] h-px ${status === "completed" ? "bg-ok" : "bg-lineStrong"}`}
                />
              )}
              <span
                className={`relative z-[1] mx-auto grid h-[19px] w-[19px] place-items-center rounded-full border text-[9px] font-semibold ${
                  status === "completed"
                    ? "border-ok bg-ok text-white"
                    : active
                      ? "border-accent bg-accent text-white ring-4 ring-accentSoft"
                      : status === "failed"
                        ? "border-danger bg-danger text-white"
                        : status === "skipped"
                          ? "border-lineStrong bg-paper text-faint"
                          : "border-lineStrong bg-panel text-faint"
                }`}
              >
                {status === "completed" ? "✓" : index + 1}
              </span>
              <div className={`mt-2 text-[10.5px] leading-tight ${active ? "font-semibold text-accent" : "text-muted"}`}>
                {STAGE_LABELS[stage]}
              </div>
              <div className="mt-1 flex min-h-4 items-center justify-center gap-1">
                {status === "skipped" && <span className="text-[9.5px] text-faint">Skipped</span>}
                {stage === "inter_step_evaluation" && attempts.length > 1 && (
                  <span className="rounded-full bg-paper px-1.5 text-[9.5px] text-muted">×{attempts.length}</span>
                )}
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

export function Segmented<T extends string>({
  value,
  options,
  onChange,
  label,
}: {
  value: T;
  options: Array<{ value: T; label: string }>;
  onChange: (value: T) => void;
  label: string;
}) {
  return (
    <div role="tablist" aria-label={label} className="inline-flex rounded-lg bg-paper p-0.5">
      {options.map((option) => (
        <button
          key={option.value}
          role="tab"
          aria-selected={value === option.value}
          className={`rounded-md px-2.5 py-1 text-[11.5px] ${
            value === option.value ? "bg-panel font-medium text-ink shadow-sm" : "text-muted hover:text-ink"
          }`}
          onClick={() => onChange(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}
