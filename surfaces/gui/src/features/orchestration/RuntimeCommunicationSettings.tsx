import { useEffect, useMemo, useState } from "react";
import { createOrchestrationApi, type ApiRequest } from "./api";
import type { HandoffRuntimeSettings } from "./types";
import {
  CARD,
  ErrorNotice,
  INPUT,
  LoadingBlock,
  PRIMARY_BUTTON,
  SectionHead,
} from "./ui";

export interface RuntimeCommunicationSettingsProps {
  apiRequest: ApiRequest;
}

const BOOLEAN_FIELDS: Array<{
  key: keyof HandoffRuntimeSettings;
  label: string;
  help: string;
}> = [
  { key: "structured_handoff_enabled", label: "Structured handoff enabled", help: "Use published Briefs, ContextRefs, relations, durable wakes, comments, and work products." },
  { key: "structured_handoff_required_for_new_tasks", label: "Require structured handoff for new Agent tasks", help: "Reject legacy child delegation when a complete Brief is required." },
  { key: "legacy_spawn_agent_enabled", label: "Legacy spawn_agent enabled", help: "Keep the compatibility adapter available during staged rollout." },
  { key: "context_read_audit_enabled", label: "Audit context reads", help: "Record every explicit ContextRef read in the task event chain." },
  { key: "transcript_sharing_default", label: "Share transcripts by default", help: "High-risk compatibility option. Keep disabled for fresh reviewer and tester sessions." },
];

const NUMBER_FIELDS: Array<{
  key: keyof HandoffRuntimeSettings;
  label: string;
  min: number;
  max: number;
}> = [
  { key: "default_context_token_budget", label: "Initial context token budget", min: 0, max: 1_000_000 },
  { key: "max_context_refs", label: "Maximum ContextRefs", min: 0, max: 1_000 },
  { key: "max_inline_bytes_per_ref", label: "Inline bytes per ref", min: 0, max: 65_536 },
  { key: "max_inline_bytes_total", label: "Total inline bytes", min: 0, max: 65_536 },
  { key: "max_comment_batch", label: "Comment delta batch", min: 1, max: 1_000 },
  { key: "wake_coalesce_window_ms", label: "Wake coalesce window (ms)", min: 0, max: 60_000 },
  { key: "wake_max_attempts", label: "Wake delivery attempts", min: 1, max: 100 },
  { key: "wake_backoff_seconds", label: "Wake retry backoff (seconds)", min: 1, max: 3_600 },
];

const messageOf = (error: unknown) =>
  error instanceof Error ? error.message : "The communication settings request failed.";

export function RuntimeCommunicationSettings({ apiRequest }: RuntimeCommunicationSettingsProps) {
  const api = useMemo(() => createOrchestrationApi(apiRequest), [apiRequest]);
  const [settings, setSettings] = useState<HandoffRuntimeSettings | null>(null);
  const [dirty, setDirty] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    let active = true;
    void api.getHandoffSettings()
      .then((value) => { if (active) setSettings(value); })
      .catch((caught) => { if (active) setError(messageOf(caught)); });
    return () => { active = false; };
  }, [api]);

  if (!settings) {
    return error ? <ErrorNotice message={error} /> : <LoadingBlock label="Loading runtime communication settings…" />;
  }

  const update = <K extends keyof HandoffRuntimeSettings>(key: K, value: HandoffRuntimeSettings[K]) => {
    setSettings((current) => current ? { ...current, [key]: value } : current);
    setDirty(true);
    setSaved(false);
  };
  const save = async () => {
    setBusy(true);
    setError(null);
    try {
      setSettings(await api.updateHandoffSettings(settings));
      setDirty(false);
      setSaved(true);
    } catch (caught) {
      setError(messageOf(caught));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section aria-label="Runtime communication settings">
      <SectionHead
        title="Runtime communication"
        aside={<button className={PRIMARY_BUTTON} disabled={!dirty || busy} onClick={() => void save()}>{busy ? "Saving…" : "Save settings"}</button>}
      />
      <p className="mb-4 text-[12px] leading-relaxed text-muted">
        These limits apply to new handoffs immediately. Existing runs keep their immutable Brief and context snapshots.
      </p>
      {error && <ErrorNotice message={error} />}
      {saved && <div role="status" className="mb-3 rounded-lg border border-success/30 bg-success/10 px-3 py-2 text-[12px] text-success">Settings saved and applied.</div>}
      <div className={`${CARD} divide-y divide-line`}>
        {BOOLEAN_FIELDS.map((field) => (
          <label key={field.key} className="flex items-start gap-3 p-4">
            <input
              type="checkbox"
              className="mt-0.5"
              aria-label={field.label}
              checked={Boolean(settings[field.key])}
              onChange={(event) => update(field.key, event.target.checked as never)}
            />
            <span><span className="block text-[12.5px] font-medium text-ink">{field.label}</span><span className="mt-0.5 block text-[11.5px] text-muted">{field.help}</span></span>
          </label>
        ))}
      </div>
      <div className={`${CARD} mt-4 grid grid-cols-1 gap-3 p-4 sm:grid-cols-2`}>
        {NUMBER_FIELDS.map((field) => (
          <label key={field.key}>
            <span className="mb-1 block text-[11.5px] font-medium text-muted">{field.label}</span>
            <input
              className={INPUT}
              type="number"
              aria-label={field.label}
              min={field.min}
              max={field.max}
              value={Number(settings[field.key])}
              onChange={(event) => update(field.key, Number(event.target.value) as never)}
            />
          </label>
        ))}
      </div>
      {settings.transcript_sharing_default && (
        <div role="alert" className="mt-3 rounded-lg border border-warn/30 bg-warnSoft px-3 py-2 text-[11.5px] text-warnInk">
          Transcript sharing weakens role isolation and should be enabled only for an explicit compatibility requirement.
        </div>
      )}
    </section>
  );
}
