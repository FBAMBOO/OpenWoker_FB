# Task Quality V2

Task Quality V2 turns a goal-only repository task into a frozen, auditable execution and
acceptance contract. It is opt-in for new tasks. Existing Task-Centric Handoff tasks and
historical Work Products remain readable through the legacy path.

## Non-negotiable invariants

- The published `TaskContractV2`, `RepositorySnapshot`, `ExecutionStrategy`, and
  `BudgetLedger` are independent frozen identities.
- The primary deliverable is an immutable task-owned artifact. It is never replaced by
  an evaluator summary or a legacy Work Product projection.
- Read-only means the source workspace is read-only. Task-owned artifacts, evidence,
  findings, receipts, and audit events may still be written to OpenWorker state.
- Review acceptance requires a fresh server-derived 100% read receipt for the exact
  artifact hash and current attempt.
- Hard gates and open blocking findings are authoritative. Missing, unknown, truncated,
  stale, or schema-mismatched data never becomes `pass`.
- The server sums rubric dimensions and derives identity, fencing, and waiver actor
  fields. Model- or request-supplied authority fields are rejected.
- Repair creates an immutable child version and is bounded to two attempts.

## Create and start a task

In the GUI, open **Tasks → New → Task Quality V2** and complete:

1. **Goal** — enter objective/workspace and permissions. Source write and network are
   false by default; task artifact write remains allowed.
2. **Contract** — inspect derived requirements, source spans, constraints, deliverable,
   and semantic-lint issues. Publish is disabled until required currentness, evidence,
   relationship, limitations, safety, and format semantics are complete.
3. **Target** — inspect repository candidates, HEAD/default/upstream refs,
   ahead/behind, dirty state, worktrees, confidence, and the recommendation. An
   ambiguous candidate must be selected explicitly.
4. **Strategy** — inspect complexity, operation risk, evidence-workload axes, the DAG,
   direct input bindings, policy provenance, feature flags, and effective hard budget.
5. **Publish & Start** — commit the Brief compatibility bridge, budget ledger, plan,
   queue transition, and first wake in one transaction.

The equivalent REST flow under `/v1/orchestration` is:

```text
POST /task-drafts                         Idempotency-Key required
POST /task-drafts/{task}:analyze          Idempotency-Key required
PUT  /task-drafts/{task}/contract         If-Match required
POST /task-drafts/{task}/contract:publish If-Match required
POST /task-drafts/{task}/snapshots
POST /task-drafts/{task}/strategy:generate
POST /task-drafts/{task}:start
```

Retry an uncertain create/analyze/start with the same idempotency identity and body.
Reusing a key with different input fails. Contract edits use the exact content hash as
`If-Match`; published rows cannot be edited or deleted.

## Read task truth

Task detail has four orthogonal states:

| Axis | Examples | Meaning |
|---|---|---|
| Workflow | `draft`, `running`, `reviewing`, `repairing`, `needs_attention`, `completed` | Where the V2 workflow is. |
| Quality | `pending`, `checking`, `pass`, `fail`, `unknown`, `waived` | Server-authoritative acceptance result. |
| Artifact | `none`, `uploading`, `draft`, `validating`, `verified`, `rejected`, `superseded` | Exact primary artifact lifecycle. |
| Budget | `unconfigured`, `within_budget`, `warning`, `over_budget`, `exhausted`, `unlimited` | Effective ledger state, not a display-only estimate. |

`coworker/orchestration/quality/state_machine.py` is the only workflow event/transition
source. Every accepted transition updates the projection and appends a hash-chained
`quality_workflow_transition` event in one transaction; rejected public attempts append
`invalid_transition`. `workflow_resume_status` is a server-owned persisted checkpoint,
never an Agent/API input. `GET /task-quality/schema` exposes the typed OpenAPI snapshot,
and `scripts/generate_task_quality_types.py --check` verifies the checked-in GUI types.

Do not render `completed + quality=fail/unknown` as an ordinary success. `waived` is
publishable only when every failed subject is itself waivable and covered by an active,
exact waiver.

Useful bounded read endpoints are:

```text
GET /tasks/{task}/contract
GET /tasks/{task}/snapshot
GET /tasks/{task}/strategy
GET /tasks/{task}/coverage?limit=...
GET /tasks/{task}/claims?limit=...&cursor=...
GET /tasks/{task}/quality?limit=...&finding_cursor=...
GET /tasks/{task}/deliverables?limit=...&cursor=...
GET /tasks/{task}/export
GET /artifacts/{artifact}
GET /artifacts/{artifact}/content   Range / ETag / If-None-Match
GET /artifacts/{artifact}/download
GET /artifacts/{artifact}/diff?base_artifact_id=...
```

Cursor tokens are versioned and bound to task, stream, and scope. Do not transfer a
cursor between endpoints or tasks. Executable, HTML, and SVG artifacts are
download-only; the API and GUI never mark them safe for inline execution.

## Quality gates and repairs

Repository analysis evaluates QG-001 through QG-016: workspace integrity, frozen
baseline, required-domain coverage/evidence, relationships/control plane, claim and
negative-evidence discipline, limitations, citations, inventory reconciliation,
artifact contract, independent full read, finding authority, schema integrity, and
budget integrity. The quality-first semantic threshold is 85/100.

When a repairable failure occurs and auto repair is disabled, the task enters
`needs_attention` with `repair_requires_operator_request`. Use the Quality panel or:

```http
POST /v1/orchestration/tasks/<task-id>/repairs
Content-Type: application/json

{"finding_ids":["finding_..."]}
```

Only open, blocking, repairable findings for the current artifact are accepted. A repair
request freezes allowed sections, required global validators, budget allocation, source
artifact, and target child version. The child must be fully reread and revalidated.
After two ineffective attempts the task remains `needs_attention`; create a new task or
make a documented operator decision instead of resetting history.

## Waivers

Waivers preserve the original failed gate/finding/score and add a signed exact exception.
They bind task, artifact ID/hash, contract/version, subject type/ID/version, and—when
applicable—rubric/version and expiry. The authenticated local sidecar derives the actor;
payload fields `actor_id` or `actor_role` are rejected.

Never waive artifact/hash integrity, path escape, secret exfiltration, run identity or
fencing, unauthorized source-workspace writes, QG-001, QG-002, QG-010, QG-012, QG-013,
QG-015, or QG-016. For an allowed exception, record a concrete reason, external
reference if available, and a short expiry. The UI permanently labels the result
`waived`; it never rewrites the failed verdict to pass.

## Budget exhaustion and resume

Hard mode reserves root capacity before work and fences every reservation/consume pair.
The N+1 tool call is rejected before execution. A provider overrun may finish the current
atomic turn only long enough to checkpoint and account; the task then becomes
`budget_status=exhausted`, `workflow_status=needs_attention`, and
`quality_reason_code=budget_exhausted`.

Do not edit the exhausted ledger or mark the task completed. To continue, the UI/API
must submit every canonical limit plus an audit reason to `POST /tasks/{task}:resume`.
The authenticated server identity—not request JSON—is the actor. At least one limit must
increase. The service supersedes the old immutable ledger, cancels/fences outstanding
reservations, creates revision N+1 with cumulative consumption, and only then applies the
reason-specific `resume_requested` transition. Exact reservation replay is idempotent; a
different usage replay or stale fencing token is rejected.

```json
{
  "effective_limits": {
    "model_calls": 100,
    "tool_calls": 500,
    "reported_tokens": 2000000,
    "active_seconds": 7200,
    "tool_payload_bytes": 268435456
  },
  "reason": "Approved continuation for the final independent review"
}
```

## Recovery runbook

| Symptom/reason | Check | Safe action |
|---|---|---|
| `needs_target_selection` | Candidate roots, refs, confidence and duplicate worktrees | Select an explicit local candidate/ref and freeze it; never network-fetch implicitly. |
| Contract publish blocked | Located semantic lint issues and permission conflicts | Edit the draft with `If-Match`, preserve source spans, and republish. |
| `budget_exhausted` | Effective mode/source/limits, reservations, provider usage events | Preserve the exhausted ledger; cancel, create a revised task/strategy, or explicitly resume only after policy resolution. |
| `repair_requires_operator_request` | Current artifact, open repairable findings, remaining attempts/budget | Submit one bounded repair request. |
| `repair_exhausted` | Finding fingerprints and both child versions | Keep `needs_attention`; do not reset attempts. Escalate or start a new task. |
| Artifact hash alert | Upload/final blob metadata and content-free hash-failure event | Quarantine the artifact, restore from a trusted backup or regenerate a new version; never suppress the integrity gate. |
| `recovering` / `needs_reconciliation` | Latest run/lease/fence, persisted `workflow_resume_status`, checkpoint, event chain and provider turn certainty | Reconcile once; only the server may return to the exact allowlisted checkpoint. Do not double-settle uncertain provider usage or feed an old-attempt artifact downstream. |
| Incomplete reviewer read | Exact candidate ID/hash and receipt ranges | Resume the same authorized reviewer attempt or start a fresh review and read all bytes. |

Crash recovery is retry-oriented. Contract publication, snapshot activation, artifact
finalization, evaluation/task projection, budget reservation/event, repair version/task
projection, and initial start are transactionally fail-closed. A blob may be durably
written before its database reference; the integrity scan reports such residue and never
deletes it automatically.

## Backup, restore, and integrity

Back up `orchestration.db` with the SQLite backup API and copy the complete `blobs/`
tree as the same recovery set. Do not copy only the database or only the blobs. After a
restore, construct `ArtifactService` against the restored store/blob root and run
`integrity_scan()`. A passing scan verifies artifact rows, upload chunks, and nested
working-tree/directory-pack blob references. Its output is content-free and reports
failures and orphan counts; investigate orphans before any manual cleanup.

Never hand-edit `orchestration.db`, immutable JSON manifests, hash-addressed files, or
hash-chain events. Use the normal APIs and retain the restored set until the task export
and event chain are independently verified.

## Release benchmark and observability

Open **Settings → Task quality** to run the registered offline Test12 corpus plus Python,
TypeScript, Go, and Java stacks. Fixtures contain sanitized snapshot hashes and metrics,
not host paths, prompt bodies, repository bodies, or provider transcripts. A V2 release
candidate must pass every hard gate, score at least 85, resolve 100% benchmark citations,
read 100% of the artifact, stay within 3M reported tokens/120 tool calls/20 minutes, and
keep duplicate non-cached scans at or below 20%.

Only a passing run can be promoted. Baseline promotion is an authenticated admin action
with a non-empty audit reason; request bodies cannot forge actor identity. Promotion is
durable and quality regression greater than five points becomes a no-go alert.

`GET /v1/orchestration/health` exposes content-free Task Quality metrics and alerts,
including missing primary deliverables, passed artifacts without complete reads, pass or
waived tasks with uncovered blocking findings, hard-budget overrun, low-confidence target
starts, artifact hash failures, repair exhaustion, duplicate scans, schema-adapter warning
rate increases, and release quality regression.

## Rollout and rollback

The shipped GUI is Stage 2 opt-in. Strategy feature flags and budget policy are frozen at
generation and affect only that task. Use characterization → shadow → opt-in → canary
10/25/50/100% → allowlisted auto repair/hard budget → legacy deprecation. Observe at
least one release period and the complete benchmark/security/migration gate at each
canary step.

Rollback changes new-task routing only. Running tasks finish or are manually canceled
under their frozen strategy. Migrations 0011–0017 are additive; downgrade the application
without dropping V2 tables, artifacts, snapshots, evaluations, waivers, ledgers, or audit
events. Hard-budget rollout may move to observe for new tasks, but an existing exhausted
task never becomes completed automatically. Schema and artifact integrity rails cannot
be disabled by a feature flag.

## Verification

Run the release gates from the repository root:

```shell
python -m pytest -q tests/test_orchestration_*.py
python -m pytest -q tests/benchmarks/task_quality
python -m pytest -q tests/test_task_quality_*.py
cd surfaces/gui
npm test -- --run
npm run build
cd ../..
python -m compileall -q coworker
python scripts/generate_task_quality_types.py --check
git diff --check
```

On Windows, also run the path/symlink, Git injection, subscription-runtime provider,
Claude MCP sandbox, and native runtime integration tests recorded in the acceptance
matrix. Any skip or unrelated failure remains explicit; do not silently reclassify it as
a pass.
