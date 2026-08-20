# Hierarchical orchestration

OpenWorker's orchestration control plane turns a normal agent turn into a durable,
evidence-backed task lifecycle. It is deliberately additive: the existing session engine,
tools, provider router, connectors, and GUI remain the execution substrate, while the
`coworker.orchestration` package owns workflow truth.

The implementation targets a local, single-host production deployment. SQLite WAL with
`synchronous=FULL`, leases, fencing tokens, idempotent commands, immutable snapshots,
isolated candidate workspaces, and a hash-chained event ledger make process crashes
recoverable and actions auditable. A durable leader lease rejects a second active
scheduler against the same database, so per-process concurrency limits cannot be
multiplied accidentally. Scaling across multiple hosts would require replacing the local
workspace coordinator, leader lease, and SQLite claim mechanism with shared
infrastructure; the domain model and executor contract are intentionally provider-neutral.

## The fixed eight-stage lifecycle

Stages are persisted independently from task status. Every legal stage transition is
validated and appended to stage history.

| # | Stage | Responsibility | Durable output or gate |
|---|---|---|---|
| 1 | `intake` | Validate objective, workspace, constraints, and acceptance criteria. | Task record and intake event. |
| 2 | `complexity_assessment` | Score scope, uncertainty, dependencies, side effects, parallelism, and verification cost; classify risk. | Complexity/risk evidence and derived policy. |
| 3 | `clarification` | Stop ambiguous work before planning. | Optional `clarification` human gate. |
| 4 | `planning` | Create and validate an immutable, versioned DAG with frozen profile and model-policy snapshots. | Plan revision and optional `plan_approval` gate. |
| 5 | `execution_review_test` | Schedule dependency-ready work, execute isolated roles, retry safe failures, and record artifacts. | Runs, leases, transcripts, patches, review/test evidence, and permission gates. |
| 6 | `inter_step_evaluation` | Reconcile all terminal node outcomes before acceptance. | Evaluation evidence or `reconciliation` gate. |
| 7 | `final_acceptance` | Evaluate every acceptance criterion and require a formal verdict when policy demands it. | Policy verdict and optional `final_acceptance` gate. |
| 8 | `archive` | Seal the result and expose its evidence/event history. | Completed then archived task record. |

The stage order is fixed. A revision request is an explicit, audited back-edge from
planning/evaluation/acceptance to a new plan revision; it is not an untracked mutation of
an already approved plan.

Task status describes operational state and is orthogonal to the current stage:
`draft`, `queued`, `running`, `waiting_human`, `waiting_child`, `paused`, `blocked`,
`needs_reconciliation`, `canceling`, `completed`, `failed`, `canceled`, or `archived`.

## Architecture and source map

The feature has one narrow integration seam at each existing OpenWorker layer.

| File | Responsibility |
|---|---|
| `coworker/orchestration/__init__.py` | Stable public exports for the additive orchestration package. |
| `coworker/orchestration/errors.py` | Typed validation, conflict, stale-lease, integrity, and not-found errors used at the API boundary. |
| `coworker/orchestration/models.py` | Immutable domain records and enums for tasks, plans, DAG nodes/edges, runs, gates, leases, evidence, commands, events, and outbox items. |
| `coworker/orchestration/state_machine.py` | Legal task-status and eight-stage transitions. All store transitions pass through these validators. |
| `coworker/orchestration/dag.py` | Structural validation, cycle detection, roots, descendants, joins, and dependency rules. |
| `coworker/orchestration/migrations.py` | Forward-only, checksummed migration loader. A historical migration that changes after deployment is rejected. |
| `coworker/orchestration/migrations/0001_initial.sql` | Independent orchestration schema, indexes, foreign keys, immutability triggers, and event/outbox tables. |
| `coworker/orchestration/migrations/0002_operational_indexes.sql` | Upgrade-only indexes for parent/child traversal and interrupted candidate-commit recovery. |
| `coworker/orchestration/migrations/0003_evidence_blob_lookup.sql` | Indexed authorization lookup for content-addressed evidence downloads. |
| `coworker/orchestration/migrations/0004_scheduler_and_outbox_resilience.sql` | Single-active scheduler lease plus durable outbox dead-letter state and indexes. |
| `coworker/orchestration/migrations/0005_outbox_requeue_audit.sql` | Append-only dead-letter requeue snapshots, operator attribution, reasons, and command-ledger linkage. |
| `coworker/orchestration/migrations/0006_prepared_run_gates.sql` | Adds the non-resolvable `preparing` state and nullable publication timestamp used to publish run gates only after checkpoint and cleanup settlement. |
| `coworker/orchestration/migrations/0007_structured_handoff.sql` | Adds immutable, versioned Task Briefs, ContextRefs, active Brief projection, and per-run Brief snapshots. |
| `coworker/orchestration/migrations/0008_task_relations_and_wakes.sql` | Adds first-class task relations plus indexed, deduplicated, leased wake requests. |
| `coworker/orchestration/migrations/0009_comments_and_work_products.sql` | Adds ordered immutable comments and immutable, verifiable Work Product records. |
| `coworker/orchestration/migrations/0010_run_activity.sql` | Adds a fenced, append-only, incrementally pageable live activity ledger for each Agent run. |
| `coworker/orchestration/migrations/0011_task_quality_contracts.sql`–`0017_budget_ledgers.sql` | Add canonical V2 contracts, immutable artifact versions/read receipts, authoritative evaluations/repairs/waivers, repository snapshots, typed evidence/inventory, frozen strategies, and fenced effective-budget ledgers. All seven migrations are additive and retain data during application rollback. |
| `coworker/orchestration/quality/` | Task Quality V2 engine: contract compilation/linting, target resolution/snapshots, adaptive strategy and direct bindings, artifact/evidence ledgers, deterministic validators, semantic adjudication, bounded repair, budgets, offline benchmarks, observability, and the authenticated Claude MCP bridge. |
| `coworker/orchestration/activity.py` | Bounds and redacts operator-visible activity while excluding private reasoning and raw tool results. |
| `coworker/orchestration/handoff_models.py` | Task-centric handoff enums, immutable records, canonical hashes, validation, and structured result contracts. |
| `coworker/orchestration/context.py` | Context policy, manifest budgeting, workspace-safe resolution, secret checks, staleness verification, and audited reads. |
| `coworker/orchestration/envelope.py` | Deterministic compact ExecutionEnvelope and UTF-8-safe initial prompt rendering. |
| `coworker/orchestration/relations.py`, `wakes.py`, `communications.py`, `work_products.py` | Focused services for dependencies, durable delivery, comment deltas, and result references. |
| `coworker/orchestration/runtime_tools.py` | Run-bound handoff tools whose mutation authority is closed over the current lease and fencing token. |
| `coworker/orchestration/handoff_settings.py`, `observability.py` | Persisted rollout/limit settings and content-free handoff metrics. |
| `coworker/orchestration/store.py` | Transaction boundary and source of truth. Implements optimistic versions, idempotent commands, leases/fencing, immutable plans/evidence/activity, gates, the event hash chain, and transactional outbox. |
| `coworker/orchestration/policy.py` | Deterministic complexity/risk scoring, gate policy, and formal acceptance evaluation. |
| `coworker/orchestration/profiles.py` | Versioned Agent contracts and built-in roles: orchestrator, planner, worker, reviewer, tester, evaluator, scorer, explorer, and integrator. |
| `coworker/orchestration/catalogs.py` | Atomic JSON catalogs for editable drafts and immutable published profile/routing-policy versions. Cross-process locking plus ETags prevent lost updates. |
| `coworker/orchestration/routing.py` | Reproducible model selection. Capability, configuration, availability, verification, context, provider, and cost are hard filters before quality ranking. |
| `coworker/orchestration/presets.py` | Immutable built-in role-to-runtime presets, exact model assignments, default-domain metadata, strict fallback behavior, and plan-template identity. |
| `coworker/orchestration/runtime.py` | Provider-neutral parent/child runtime ledger. Tracks usage, optionally enforces budgets, and enforces permission intersection, attempts, work units, hierarchy depth, child count, concurrency, dependencies, and cancellation. |
| `coworker/orchestration/workspace.py` | Per-run git worktree or snapshot isolation, exact manifests, candidate collection, advisory locks, journaled delivery, rollback, and interrupted-delivery recovery. |
| `coworker/orchestration/blobs.py` | Content-addressed SHA-256 storage for patches and larger artifacts. |
| `coworker/orchestration/executor.py` | Adapter from a claimed orchestration run to OpenWorker's `TurnEngine`; creates hidden sessions, applies role tool ceilings, emits safe live activity, suspends at durable gates, and resumes the same attempt. |
| `coworker/orchestration/subscription_runtime.py` | Common subscription Agent runtime contract, sanitized health catalog, safe provider activity mapping, process containment, durable vendor-session checkpoints, sealed recovery results, and the Codex/Claude/Kimi adapters. |
| `coworker/orchestration/service.py` | Restart-safe lifecycle coordinator, DAG scheduler, run worker pool, parent/child task operations, recovery, routing, evidence capture, and GUI read model. |
| `coworker/orchestration/api.py`, `api_schemas.py` | Typed `/v1/orchestration` REST contract for lifecycle and structured-handoff objects. |
| `coworker/engine.py` | Adds a generic deferred-interaction result and recoverable turn state; existing interactive behavior remains the default. |
| `coworker/tools/registry.py` | Adds a fail-closed registry subset used to construct a role-specific execution surface. |
| `coworker/agent.py` | Exposes the optional tool-filter seam after built-ins, skills, MCP, and connector tools have been assembled. |
| `coworker/server/manager.py` | Owns and starts/stops one orchestration service beside existing sessions and automations. |
| `coworker/server/app.py` | Mounts the orchestration router without mixing its handlers into the existing API. |
| `surfaces/gui/src/features/orchestration/` | Lazy task tabs, the Task Quality V2 five-step creation wizard, primary deliverable viewer, evidence/quality/budget/repair panels, offline benchmark settings, and the click-through Run inspector. |

`pyproject.toml` packages migration SQL with the Python distribution, so installed wheels
can initialize the independent orchestration database.

## Task Quality V2

Task Quality V2 is an opt-in quality engine for new repository-analysis and focused-
question tasks. It freezes four independent identities before execution: the published
contract, repository snapshot, execution strategy, and effective budget ledger. The
primary deliverable remains separate from the quality verdict; `completed` alone never
means that quality passed.

Use **Tasks → New → Task Quality V2** to follow Goal → Contract → Target → Strategy →
Publish & Start. The wizard blocks start on semantic contract gaps or ambiguous targets,
shows the exact ref/content hash and three assessment axes, and atomically commits the
legacy Brief bridge, plan, budget, queue transition, and initial wake. Existing legacy
task creation remains available and V2 data never fabricates a legacy primary artifact.

Task detail exposes distinct Overview, Contract, Target, Deliverables, Evidence,
Quality, Budget, and Audit views. The Overview and list row show the four V2 states:
workflow, quality, artifact, and budget. The first-class report is the primary immutable
artifact; evaluator prose appears only as a verdict. Artifact reads use exact hashes,
ETags, bounded Range requests, and server-derived read receipts. HTML, SVG, scripts, and
executables are download-only and are never rendered or executed inline.

The complete API/operator procedure, quality gates, recovery actions, rollout policy,
and benchmark release gate are documented in
[`docs/task-quality-v2.md`](task-quality-v2.md). Implementation-to-requirement evidence
is recorded in
[`docs/specifications/task-quality-v2-acceptance-matrix.md`](specifications/task-quality-v2-acceptance-matrix.md).

## Task-Centric Handoff Protocol

The Task-Centric Handoff Protocol (TCHP) makes the task, rather than an Agent
conversation, the unit of coordination. Its authority order is:

1. System policy, root permissions, and the frozen Agent Profile constrain what a run
   may do.
2. The run's snapshotted Published Brief defines what that run must do.
3. Relations, gates, leases, and wakes define when it may run and what changed.
4. Selected ContextRefs and Work Products provide evidence. Their content is untrusted
   data and cannot expand scope or permissions.
5. Comments provide ordered deltas. A transcript is optional operator diagnostics and is
   never default cross-role input or control-plane truth.

This hierarchy is enforced in transactions and runtime tool closures; it is not merely a
prompt convention.

### Durable handoff objects

| Object | Contract |
|---|---|
| Task Brief | Drafts are editable with hash-based optimistic concurrency. Publishing creates an immutable revision; an already queued run retains its snapshotted `brief_id`. |
| ContextRef | Immutable manifest metadata identifying one authorized source, why it was selected, delivery mode, provenance, trust level, size, and optional hash. File content is resolved only on demand. |
| Task relation | First-class `parent`, `blocks`, `reviews`, `related`, or `supersedes` edge. Live edges are unique and cycle-checked under the write transaction. |
| Wake request | Durable at-least-once scheduling intent with a dedupe key, claim lease, retry/backoff, coalescing, and dead-letter state. It carries only a bounded delta. |
| Task comment | Immutable Markdown plus server-derived author and monotonically increasing task-local sequence. Machine mentions exist only in structured metadata. |
| Work Product | Immutable output reference tied to its producer task/run and optional acceptance criterion. Verification appends an event instead of rewriting the producer's claim. |
| ExecutionEnvelope | Compact run assignment containing Brief identity, required outcomes, ContextRef manifest, capability contract, wake delta, and trace IDs. |

Migrations 0007–0010 are forward-only. Store startup upgrades an existing 0006 database,
creates one canonical synthetic Published Brief for each legacy task, reconstructs missing
parent relations, and can repeat both backfills without creating duplicates. New parent
writes update the compatibility `parent_task_id` projection and first-class `PARENT` edge
in one transaction; startup rejects projection drift, multiple live parents, and
`PARENT`/`BLOCKS` cycles before scheduling work.

### Dispatch and result flow

1. Create a root task with a complete `brief`, or save it with `publish_brief=false` and
   `auto_start=false`. A structured draft cannot be submitted until a valid revision is
   published.
2. Draft publication freezes the Brief and ContextRefs. Moving a draft task to `queued`
   atomically creates its assignment wake.
3. The scheduler claims a wake, derives or finds exactly one runnable run, binds the wake
   to it, and atomically claims the run with a lease and monotonically increasing fencing
   token.
4. The executor creates a fresh role session and renders a prompt capped at 32 KiB by
   default and 64 KiB absolutely. The prompt has manifest summaries but no unselected
   repository body, upstream input, or role transcript.
5. The Agent lists metadata and reads only authorized ContextRefs. Every content read can
   append a `context_ref_read` event. Workspace paths are canonicalized, escaping
   symlinks are rejected, secret-like inline content is blocked, and a URL never grants
   ambient network authority.
6. `delegate_task` atomically creates the child, its Published Brief and ContextRefs,
   parent/blocker relations, audit events, and assignment wake. The operation ID makes a
   lost-response retry safe.
7. Comments wake an owner with an ordered, coalesced delta. Structured mentions create
   notice-only wakes but never change the task owner or run lease. Raw `@Name` text does
   not produce a machine wake, and task mentions cannot cross the orchestration tree.
   A body larger than 65,536 UTF-8 bytes is stored as a content-addressed Markdown
   artifact and the comment contains only its immutable Work Product reference. A
   secret-shaped body is rejected before the artifact write.
8. A parent waiting for children, a blocked task, or a suspended gate releases execution
   authority. Child completion, resolution of all successful blockers, or gate resolution
   creates the durable wake that resumes work; no model busy-polling is needed. A canceled
   blocker remains unresolved and opens attention for replacement or removal.
9. The Agent registers immutable Work Products, then calls `complete_task` with criterion
   results and remaining risks, or `fail_task`. Required deliverables and criteria are
   validated before run/task/result events commit together. Parent wakes contain only
   bounded result summaries and references, never child transcripts.

The run-bound tool surface is `get_task_context`, `list_context_refs`,
`read_context_ref`, `delegate_task`, `post_task_comment`, `list_task_comments`,
`add_task_blockers`, `remove_task_blocker`, `create_work_product`, `complete_task`, and
`fail_task`. The runtime supplies task/run/lease/fencing identity; the model cannot choose
another principal. Compatibility tools `spawn_agent`, `wait_agent`, and `cancel_agent`
remain available subject to profile and rollout policy. Legacy spawn creates a minimal
Brief and emits `legacy_delegation_used`; it never restores raw upstream prompt copying.

### Communication policy and runtime settings

Published Profile schema v2 contains `communication_policy`: delegation permission,
allowed child roles, required Brief fields, ContextRef types and limits, transcript
reference policy, comment/mention permissions, relation permissions, and result contract.
Schema v1 profiles receive conservative defaults at load time. Built-ins remain immutable;
clone one before changing policy.

The default deployment is Stage C: structured handoff is enabled for new explicit Briefs,
it is not yet mandatory for every Agent-created child, and the legacy wrapper remains
enabled. Configure it in **Settings → Agent communication** or with
`GET/PUT /v1/orchestration/handoff-settings`.

| Setting | Default | Valid range or behavior |
|---|---:|---|
| `structured_handoff_enabled` | `true` | Enables structured root-task execution and compact envelopes. |
| `structured_handoff_required_for_new_tasks` | `false` | Rejects child delegation without a complete Brief; cannot be true while structured handoff is disabled. |
| `legacy_spawn_agent_enabled` | `true` | Retains the compatibility adapter during rollout. |
| `default_context_token_budget` | `8000` | 0–1,000,000, intersected with the Profile limit. |
| `max_context_refs` | `50` | 0–1,000, intersected with the Profile limit. |
| `max_inline_bytes_per_ref` | `8192` | 0–65,536; oversized excerpts downgrade to on-demand. |
| `max_inline_bytes_total` | `32768` | 0–65,536 across initial excerpts. |
| `max_comment_batch` | `100` | 1–1,000 ordered comments per delta. |
| `wake_coalesce_window_ms` | `1000` | 0–60,000; zero disables time-window coalescing. |
| `wake_max_attempts` | `5` | 1–100 delivery attempts before dead-letter. |
| `wake_backoff_seconds` | `1` | 1–3,600 exponential-backoff base, capped at five minutes. |
| `context_read_audit_enabled` | `true` | Appends content-read audit events. |
| `transcript_sharing_default` | `false` | High-risk compatibility preference; the current initial-prompt path still never injects ambient transcripts. |

Setting updates are validated, persisted in manager preferences, and applied live to the
context resolver, comment batching/coalescing, and wake retry policy. Health includes the
effective settings and the complete content-free metric contract:
`orchestration_handoff_initial_prompt_bytes`,
`orchestration_handoff_context_refs`,
`orchestration_handoff_context_tokens_estimated`,
`orchestration_context_reads_total`,
`orchestration_context_bytes_read_total`, `orchestration_wakes_pending`,
`orchestration_wake_coalesced_total`,
`orchestration_wake_delivery_latency_seconds`,
`orchestration_wake_failures_total`,
`orchestration_task_blocked_duration_seconds`,
`orchestration_work_products_total`,
`orchestration_legacy_delegation_total`, and
`orchestration_transcript_cross_role_reads_total`. A fresh process exposes zero values
instead of omitting never-observed metrics.

The task detail UI loads Briefs, Context Manifest, dependencies, Communication, Work
Products, and Wake diagnostics only when their tabs are opened. It does not load a
transcript or ContextRef body by default. Failed wakes can be retried explicitly after
their cause is corrected.

## Durable truth and recovery

No scheduler coroutine is authoritative. On every tick, the service derives the next
action from records in `orchestration.db`.

1. A command is applied in a SQLite transaction and keyed by `command_id` or the
   caller's `idempotency_key`.
2. Every externally observable mutation appends a domain event and outbox record in the
   same transaction. An internal Gate preparation is intentionally not observable until
   its commit/abort settlement records the corresponding audit event.
3. A worker claims a queued run with an expiring lease and monotonically increasing
   fencing token.
4. Heartbeats extend only the matching lease. A stale worker cannot complete a run after
   a newer claimant receives the next fencing token. A run reaped as `lost` is treated
   as cleanup-unknown: the former worker no longer has authority to attest that its
   process tree stopped, so automatic retry is forbidden until formal reconciliation.
5. Permission prompts, questions, plan approval, directory access, and formal lifecycle
   decisions create durable gates. The engine returns `suspended`; it does not fabricate
   an answer or lose unanswered tool calls. Run-owned gates use a two-phase protocol:
   `preparing` retains the active run lease and cannot be resolved by any caller. After
   the content-addressed checkpoint is durable and process-tree cleanup succeeds, one
   transaction stores its checkpoint reference, changes the gate to `open`, changes the
   run to `waiting_gate`, moves the task to its waiting state, and releases the lease.
   Cleanup failure atomically cancels the unpublished gate and fails the run; a crash
   leaves the lease authoritative until reaping marks cleanup state unknown and requires
   formal reconciliation. `published_at` remains null for internal preparations, so
   normal gate lists and attention pages cannot expose their prompt. The public
   `gate.prepared` audit event is appended only in the same transaction that opens the
   gate; an aborted preparation records `gate.preparation_aborted` after fenced
   settlement.
6. Resolving an open run gate requeues that exact run attempt and hidden session.
   Resolving a lifecycle gate wakes the deterministic coordinator. A run gate can
   requeue an attempt only when its blob hash, pending call identities, run id, and gate
   id still match the durable checkpoint. Before commit, the service verifies the blob
   and its run id, attempt, fencing token, session, gate, and pending-call list. The
   former single-phase run/child Gate entry points fail closed and cannot release a
   lease without this protocol.
7. Startup first acquires and heartbeats the single-active scheduler lease so a canceled
   recovery cannot overlap a replacement process. Event-chain verification is the first
   recovery action under that fence, and no scheduler work starts before it succeeds.
   The lease epoch is monotonic across graceful release, and every ordinary write
   verifies owner, token, epoch, and expiry inside the same `BEGIN IMMEDIATE`
   transaction as the mutation. Expired leaders cannot resurrect their epoch. Shutdown
   keeps heartbeating while cancellation-resistant filesystem/database threads drain,
   then releases leadership. A stopped store is tombstoned while retaining its stale
   scheduler identity, so a delayed API worker or accidental reuse cannot fall back to
   unfenced writes. Restart creates a new service/store instance. At startup, the full
   exact migration prefix is verified, expired run leases and wake claims are reaped,
   prepared gates are reconciled, relation projections and cycles are checked,
   interrupted workspace deliveries and pending candidate commits are
   recovered, and runtime ledgers are reconstructed from durable task/run state. Each
   runtime-tree projection reads its task hierarchy, runs, immutable plan topology, and
   usage evidence in one SQLite read transaction, so concurrent child settlement cannot
   combine rows from different WAL commits into a false dependency failure. If a
   final filesystem publication reached its durable journal but crashed before the
   SQLite audit append, startup requires the sealed acceptance subject, delivery journal,
   and retained candidate manifest/patch to agree before reconstructing the missing
   publication evidence. A mismatch enters reconciliation instead of publishing again.
8. WebSocket publication runs in a separate outbox worker with a per-send timeout,
   exponential backoff and jitter. It renews the scheduler fence immediately before the
   bounded external send; `event_id` is the consumer deduplication key for unavoidable
   crash-boundary replay. Ten failed attempts create a durable dead letter,
   make readiness fail, and require an authenticated operator requeue. Every requeue
   preserves the prior attempt count, last error, and dead-letter timestamp in immutable
   history, records the actor/reason, and emits an `outbox.requeued` domain audit event;
   workflow state is never rolled backward.

`pause` is a scheduling barrier: it prevents new attempts and lifecycle transitions but
does not kill an already running tool/model call. The in-flight attempt may finish and be
persisted while the task remains paused; `cancel` is the explicit interrupting operation.
Pause/cancel and final publication share the same cross-process source fence, so their
order is linear: whichever wins completes its durable transition before the other may
cross the user-workspace boundary.
Archived tasks keep their immutable database history and blob evidence, while disposable
per-run candidate workspaces are removed after interrupted delivery recovery has settled.

The event chain stores `previous_hash` and `event_hash` for every sequence. Startup
verifies genesis to tip. Interactive event pages verify only returned rows and their
immediate global predecessors, keeping request cost bounded as history grows; the response
states this verification scope and tip hash explicitly. Evidence also stores content
hashes, actor, task/plan/node/run linkage, and timestamps. Plans, nodes,
edges, and evidence are append-only at the database level.

Each event API row also exposes a content-free observability projection: `task_id`,
`run_id`, Brief id/revision, wake id/reason, ContextRef id, relation id, Work Product id,
`correlation_id`, `causation_id`, actor, timestamp, and the hash-chain fields. Missing
resource identities remain explicit nulls; raw comment, context, artifact, transcript,
and secret bodies are never copied into these trace fields.

Published Gate history is ordered by `published_at`, not by an internal preparation's
creation time. Its dedicated endpoint uses bounded SQL `LIMIT/OFFSET` pages without an
artificial 10,001-row completion boundary, so `has_more=false` means that the requested
published history is actually exhausted.

The default task detail is also resource-bounded: lifecycle projection keeps only the
newest visits needed per stage, root/child run and evidence arrays have hard row caps, and
child expansion stops at depth 3 and a 256-row lookahead limit. `children_page` and
`runtime_page` expose tree/runtime truncation, while `runs_page`, `evidence_page`,
`attention_page`, and `activity_page` report ledger truncation. Dedicated runs, evidence,
events, and transcript endpoints retrieve older audit data in bounded pages.

## Hierarchical Agent runtime

An Agent profile is an executable contract, not just a prompt. Its published version and
content hash are frozen into each plan node. The generic runtime then binds a concrete
attempt to:

- a logical work unit and attempt number;
- a parent runtime and dependency runtimes;
- a profile id/version/content hash;
- model/tool/token/wall-time usage accounting (with an optional finite budget);
- an effective permission set; and
- an immutable metadata snapshot.

Default hard rails are depth 3, concurrency 8, eight direct children per Agent, 64 logical
work units per task tree, and three attempts per work unit. The desktop product starts the
service with `runtime_budget_mode=unlimited`: model calls, tool calls, reported tokens,
profile iterations, and run wall time are recorded but do not terminate a run. Historical
task budget values remain durable audit data and are ignored while this mode is active.
Every runtime usage segment records its node key, role, profile id/version, run id, and
the four observed counters; task and run APIs continue to expose those counters while
the corresponding finite `budget` field is hidden.
Finite-budget behavior remains available to tests and embedders through
`OrchestrationService(..., enforce_runtime_budgets=True)`.

Child permissions are always the intersection of parent ceiling and child request.
Requested-but-denied capabilities are recorded. The executable child spec contains only
the effective grant, preventing an executor from accidentally treating the request as
authority. A parent must explicitly have `can_delegate`. In finite-budget compatibility
mode, a child also cannot reserve more than the parent's remaining budget.

Dynamic delegation uses three run tools:

- `spawn_agent(role, task, operation_id, child_key=None)` creates an idempotent durable
  child task. Agent calls must provide `operation_id`; `child_key` is a compatibility
  alias and, when both are supplied, they must match. Its stable identity is independent
  of a retry-specific parent run id;
- `wait_agent(task_id)` reads the child's persisted stage, status, and result; and
- `cancel_agent(task_id)` requests durable cancellation.

The first creating run remains the immutable `parent_run_id`. A LOST attempt's successor
may reuse, wait on, or cancel that child only when it owns the same task, plan, and node;
the rebind is captured as immutable delegation evidence. Reusing an explicit operation id
with different role/objective input is a conflict. Child allocation always reserves
positive parent settlement headroom, so even a profile with `max_children=1` can durably
charge the in-flight parent's model, tool, token, and wall-time usage.

A terminal child exposes a bounded schema-versioned result envelope containing the exact
accepted subject/publication hashes, latest node outcomes, and evidence references. The
envelope hash is stored with the archived child and verified again when the parent
consumes it; a missing or altered hash fails closed. Consumption itself is immutable
checkpoint evidence, so the parent/child hand-off remains auditable across restart.

Reviewer and Tester do not reuse the Worker's engine, memory, transcript, or candidate
workspace process. They receive fresh hidden sessions and role-specific read/verification
tools. They must report through the structured `submit_verdict` tool. A missing verdict is
treated as `unknown`, never as a pass; their verdicts are separate evidence and a Worker's
assertion is never promoted to a review or test result. Runtime ceilings are enforced at
four layers: registry tools, shell-command prefixes, network-tool removal, and filesystem
roots, so a later human approval cannot accidentally grant a role more than its frozen
profile permits. Hidden session identity is derived from the immutable plan-node id and
attempt; replanning with the same human-readable node key therefore cannot inherit an old
role's conversation. Immediately before any registered tool can cross a side-effect
boundary, the executor renews/verifies both the current scheduler epoch and the run's
lease/fencing token. Losing either authority prevents the tool invocation.

Shell-capable orchestration roles additionally use disposable process-tree containment:
background mode is rejected, each foreground command receives an owned POSIX process
group or Windows Job Object, and normal completion, timeout, cancellation, and lease
shutdown all reap descendants. Output uses a bounded tail buffer, and cleanup has an
absolute drain deadline even if a descendant escapes while holding stdout. A cleanup
failure is a sticky containment breach: even if the Agent turn otherwise completed, its
run is forced to `failed`, automatic retry is disabled, and an explicit human
reconciliation decision is required. It is never reported as success. This contains
ordinary test/build toolchains; it is not an
adversarial-code sandbox. Code able to escape a process group, exploit the host, or run
with the desktop user's full privileges still requires an external container/VM, OS-level
low-privilege account, network policy, and secret isolation. The local production target
is trusted repository code under least authority, not hostile arbitrary binaries.

## Task creation: workspace authority and the root profile

The **Read-only task**, **Agent profile**, and **Runtime orchestration preset** fields
control different boundaries. They should not be treated as three names for the same
choice:

- **Read-only task** is the task-wide workspace-authority ceiling. The GUI sends
  `read_only=true`; REST clients must send the same Boolean explicitly. It removes write
  authority and shell-command authority, makes the workspace root read-only, disables
  external writes, and prevents candidate publication. The ceiling is re-applied during
  recovery and inherited by every static or dynamically delegated child Agent, including
  a preset-assigned Worker.
- OpenWorker never infers `read_only` from the objective, acceptance criteria, or
  constraints. Text such as `Do not edit files`, `不要修改任何文件`, or `analysis only`
  remains an instruction that the model should follow; it is not a permission grant or
  revocation. When the field is omitted it defaults to `false`. A task that must be
  technically unable to write therefore has to select **Read-only task** in the GUI or
  send `"read_only": true` through REST. `read_only=true` and
  `external_writes=true` are rejected as contradictory.
- **Agent profile** selects the top-level task/root contract. In Automatic/legacy
  routing it also supplies the generated plan's first executable profile. A profile
  freezes instructions, tools, child roles, iteration limits, and permission mode; it
  does not select a subscription model by itself.
- **Runtime orchestration preset** freezes a role-aware DAG and the runtime assigned to
  each node. The requested-model selector controls model routing only when the preset is
  Automatic; one uniform requested model and a role-aware preset are mutually exclusive.

### Top-level profiles in Automatic/legacy routing

Only four profile roles can be selected as the top-level task contract. Their behavior
when **Runtime orchestration preset** is **Automatic / legacy routing** is:

| Top-level role | Generated primary work | Repository and delegation authority | Recommended use |
|---|---|---|---|
| **Worker** | Creates the first `execute` node and performs the scoped outcome. | Reads and writes the isolated candidate, applies patches, runs bounded commands, and may delegate bounded Worker/Tester children. A task-wide read-only ceiling still removes all write/command authority. | Any code task intended to modify files. This is the required primary role for a writable code task when no validated custom/preset DAG supplies its own Worker `execute` node. |
| **Orchestrator** | Creates a general Agent node that decomposes, delegates, waits, and synthesizes results. | Its built-in profile has delegation, question, and task-list tools, but no direct repository read/write or shell tools. It may delegate allowed roles within the inherited task ceiling. | A read-only hierarchical investigation, or the task root for a role-aware preset/custom DAG whose executable nodes are assigned separately. |
| **Planner** | Creates a general Agent node that turns repository evidence into a dependency-aware plan. | Read-only file/search/Git tools; may delegate a Worker, but child authority can never exceed the Planner/root permission intersection. | Planning-only work. In Automatic routing, do not select it as the primary profile for a writable code task. |
| **Explorer** | Creates a general Agent node that inspects the repository and returns findings/open questions. | Read-only file/search/Git tools; no write, shell, or delegation authority. | Focused architecture discovery, evidence collection, and other read-only repository analysis. |

`Reviewer`, `Tester`, `Evaluator`, `Scorer`, and `Integrator` are formal isolated
lifecycle roles, not top-level task profiles. They can appear only as validated DAG nodes
whose kind and topology preserve the role's lifecycle meaning; formal review, test, and
evaluation nodes additionally require the appropriate producer ancestry and joins. In
particular, an Integrator is writable but must operate before independent verification;
making it the task primary would bypass that lifecycle meaning. The API rejects any of
these five roles as a top-level selection rather than silently changing its role.

A writable code task in Automatic routing therefore needs a Worker primary profile. A
non-Worker root is valid when the whole task is explicitly read-only, or when a selected
role-aware preset/custom plan already contains a validated Worker-backed `execute` node.
This validation is structural: merely writing "ask a Worker to implement it" in the
objective does not create write authority or a producer node.

### Agent profile under a role-aware preset

When a role-aware preset is selected, the field is shown as **Task root profile**. It is
the task runtime-tree's root identity and the profile name shown in task
summaries; it is not the Agent copied onto every node, and it is not a model-provider
fallback. The preset template names every executable node profile, freezes those profile
snapshots into the approved plan, and routes each node independently. Selecting
Orchestrator at the task root therefore does not make the Worker, Reviewer, or Tester run
with Orchestrator tools.

For `production-codex-led-mixed-v1`, the frozen role/model mapping is:

| Preset nodes/roles | Subscription runtime |
|---|---|
| `understand` / Orchestrator, `explore` / Explorer, `plan` / Planner, and `execute` / Worker | `codex-subscription:gpt-5.6-sol@max` |
| Optional custom-plan Scorer and Integrator roles | `codex-subscription:gpt-5.6-sol@max` |
| `review` / Reviewer | `claude-code-subscription:claude-opus-5@high` |
| `test` / Tester and `evaluate` / Evaluator | `claude-code-subscription:claude-opus-5@max` |

The selected task root profile does not alter this table. The task-wide read-only switch
does: when enabled, even the preset's Worker can only inspect the workspace and cannot
publish a candidate. The preset is strict, so an unavailable Codex or Claude runtime is
reported as a routing failure instead of silently falling back to another vendor.

### Recommended read-only architecture report

For a production-audited report that inspects a Fabric/dbt repository without modifying
it, use **Tasks -> New** with:

| Field | Selection |
|---|---|
| Domain | **Code / workspace** |
| Agent profile / Task root profile | **Orchestrator** |
| Runtime orchestration preset | **Production - Codex-led mixed Agents** (`production-codex-led-mixed-v1`) |
| Requested model / Subscription Agent runtime | **Automatic** (disabled and controlled by the preset) |
| Workspace | The existing Fabric/dbt project directory |
| Read-only task | **On** |
| Reviewer and Tester | On; the production preset requires both isolated roles |

Put file-backed architecture findings in the acceptance criteria and keep `Do not edit
files`, `Do not commit or push`, and `Do not access external network` as additional
semantic constraints. Those constraints improve the acceptance contract, while the
explicit **Read-only task** switch supplies the non-bypassable file permission. With this
combination, Orchestrator is a valid task root, Codex performs isolated understanding,
exploration, planning, and read-only execution, and Claude independently reviews,
verifies, and evaluates the cited evidence.

## DAG execution semantics

Every plan revision contains immutable `nodes` and `edges`. A node has a stable key,
Agent profile, kind, join policy, effect-safety classification, retry policy, timeout,
priority, optional concurrency key, and instructions. Edges support `success`, `failure`,
`terminal`, and `always` conditions and may be required or advisory.

Kahn validation rejects cycles before publication. The scheduler enqueues only nodes whose
required incoming edge conditions are satisfied. An unsatisfied terminal branch becomes
an explicit `skipped` run, so the graph cannot hang with an unexplained pending node.
`join_policy=all` waits for every applicable predecessor; `join_policy=any` may start as
soon as one required terminal edge matches, without waiting for unrelated predecessors.
Automatic retry is limited to the node's immutable policy and safe/idempotent effects;
retrying a non-idempotent failure requires an explicit reconciliation decision.

Failure policies take effect after the node has no eligible automatic retry and have
distinct scheduler semantics:

| Policy | Durable DAG effect |
|---|---|
| `fail_fast` | Stop admitting new work for the plan and mark every absent or still-queued node `skipped`, including unrelated branches. Claimed/running/gate-waiting attempts are never rewritten and may settle normally. |
| `continue` | Add no global control effect. Normal edge conditions and `all`/`any` joins decide which successors run or become `skipped`; unrelated work continues. |
| `skip_dependents` | Mark every absent or still-queued transitive descendant `skipped`, regardless of its edge condition, while unrelated branches continue. |
| `manual` | Open a restart-safe reconciliation gate before further queued work is claimed. The operator must choose `retry` (when attempts remain), `continue`, `skip_dependents`, or `cancel`. |

Every policy-driven skip is a terminal run with `failure_policy` provenance and an event
ledger entry. A retry remains part of the same logical work unit with an incremented
attempt; a MANUAL gate source includes the immutable plan, node, and failed run so crash
recovery cannot duplicate the decision.

Custom plans are also checked semantically. A node's kind must match its frozen profile
role. Reviewer and Tester nodes must be reachable through required success edges from
every profile that can mutate the candidate, including an Integrator. Consequently an
Integrator that changes the result must run before independent review/testing. An
Evaluator must be downstream of all producers and all Reviewer/Tester nodes. Formal
verifier joins must use `all`; these rules prevent self-review, post-review mutation, and
verification bypass.

For code tasks, the root task owns a staging candidate created from an exact baseline
fingerprint. Every run executes in a disposable isolated workspace and commits its
candidate into task staging with an advisory lock, three-way merge checks, a journal, and
fsynced recovery records. A dynamic child receives another staging layer whose formal
target is its parent's task candidate—never the user's workspace. Thus child and run
completion cannot leak unaccepted files into the project. Reviewer and Tester sessions
observe the hashed staged subject but cannot mutate it. Only final acceptance re-hashes
that exact subject and performs a journaled, rollback-capable publication across the
user-workspace boundary. Manifest and patch hashes are rechecked while snapshot and source
fences are held and before the first mutation; an observed external edit or stale evidence
fails closed and produces reconciliation evidence.

## Formal gates and acceptance

There are two gate scopes:

- lifecycle gates are attached to a task and stage (clarification, plan approval,
  reconciliation, final acceptance);
- execution gates are attached to a task, plan node, and run (permission, question, plan,
  or directory requests from `TurnEngine`).

Every gate is versioned. API callers may send the observed version; a stale resolution is
rejected rather than overwriting another decision. Resolution stores the decision,
response, actor, and a separate evidence record. Gate actions are validated against the
persisted prompt, including required response text, rather than trusting a GUI button.
For response-loss-safe clients, send `idempotency_key` (or the compatibility alias
`command_id`). Repeating the same resolution returns the original result and evidence;
reusing that key with a different decision, response, actor, or version returns `409`.
The current single-user sidecar records the authenticated launch-token principal as
`local-user`; request JSON cannot forge an actor. Multi-user identity and cryptographic
signatures require a future identity-provider integration.

Several sibling runs may already be executing when one reaches a gate. The task-level
waiting status blocks new claims but does not revoke an existing sibling's lease: fenced
siblings may still persist their outcome and commit isolated candidates into
orchestration staging, and another sibling may publish its own gate. Resolving one of
several open gates recomputes the aggregate `waiting_human`/`waiting_child` state and
resumes the task only after the final blocking gate is resolved. Thus successful or
non-idempotent in-flight work is never misclassified and retried merely because a sibling
requested input.

Final acceptance evaluates all declared criteria, unresolved gates, required-node
completion, and risk policy. Low-risk, fully evidenced work may be accepted by policy.
Medium/high-risk or semantically uncertain work opens a human gate with `accept`,
`request_changes`, or `reject`. Overrides and final rejection require a non-empty reason.
Only acceptance of the exact manifest and acceptance-contract hashes can publish the
candidate and advance to archive.

## Subscription Agent runtimes

Subscription runtimes adapt a complete local coding Agent into the same durable
`RunExecutionContext -> ExecutionOutcome` boundary as OpenWorker's native executor.
They are not ordinary chat-completion providers: each CLI owns its own tool loop,
conversation/session store, event protocol, and authentication. OpenWorker therefore
routes to them only after the DAG node, role, budget, workspace, lease, and fencing token
have already been frozen.

### Interactive New Session use

The same runtime catalog is available to ordinary foreground conversations; this path
does not create an orchestration task or DAG:

1. Start OpenWorker on `127.0.0.1` or `localhost` as the OS user signed in to the CLI.
2. Open **New Session**, select the desired workspace and Agent profile, then open the
   model selector in the Composer.
3. Choose **Codex Subscription · GPT-5.6 Sol · Max**, **Claude Code Subscription ·
   Opus 5 · High/Max**, or **Kimi Code Subscription · K3 · Max (interactive only)**.
4. Choose Discuss/Plan for read-only work, Ask for owner-reviewed writes/commands, or
   Auto where the runtime's enforceable sandbox permits it, and send the message.

The model selector keeps API models and Subscription Agent runtimes in one menu, but
the backend preserves their different contracts. API models continue through
OpenWorker's provider/tool loop. A subscription entry starts the vendor's complete
native Agent loop and resumes its opaque vendor session ID on later turns. Switching to
an API model clears the stale vendor binding; switching to another subscription runtime
starts a separate vendor conversation.

Foreground native sessions have these deliberate boundaries:

- OpenWorker attachments and `/skill` execution are disabled and rejected visibly;
  native Agents cannot silently pretend to consume OpenWorker-only tool schemas.
- Codex receives OpenWorker control text through app-server developer instructions and
  Claude through the SDK system prompt. Kimi ACP exposes no true system or developer
  message hierarchy: its JSON-escaped compatibility envelope is ordinary prompt text,
  not a privileged instruction boundary. Consequently Kimi Subscription is restricted
  to foreground personal interaction and cannot be assigned any production
  orchestration role.
- Built-in external network tools and generic permission expansion are denied. Codex's
  sandbox also has network access disabled. Explicit path arguments outside the bound
  workspace are denied. Kimi native shell execution is disabled on Windows, macOS, and
  Linux. Claude Code's native Bash is disabled on Windows in every permission mode; on
  macOS/Linux it is available only within the Claude SDK sandbox and remains subject to
  OpenWorker host approval and workspace policy.
- Every native tool start/finish is written to the live transcript, the reloadable
  `native_tool` transcript records, and `AuditStore`; approval requests and final
  decisions are audited separately.
- Stop returns control promptly but leaves the session fenced until the native process
  actually unwinds. A second turn during that interval is rejected rather than allowed
  to race the old process or overwrite its vendor-session checkpoint.
- The external session ID and pre-submission state are checkpointed before a prompt can
  cause side effects. If OpenWorker restarts after submission but before a terminal
  result is atomically persisted, it refuses to replay that uncertain turn; start a new
  session or switch runtime/API model to reconcile deliberately.

Kimi managed OAuth is eligible for this foreground personal path only. Its lack of a
privileged ACP instruction layer and its subscription-use policy independently exclude
it from production task roles, scheduled delivery, unattended automation, and
orchestration DAG execution. Codex and Claude foreground execution is likewise
local-owner-only even when their background DAG health is otherwise eligible.

### Default mixed preset: `production-codex-led-mixed-v1`

`production-codex-led-mixed-v1` is the built-in production default for mixed
subscription-Agent orchestration. The GUI selects it by default for a new code task; an
API client opts in explicitly with `runtime_preset_id` so older clients that omit the
field keep their pre-preset routing behavior. The preset deliberately gives one model
family continuity across understanding, planning, and implementation, then uses a
different vendor for independent verification:

| Lifecycle responsibility | Owner | Runtime and authority |
|---|---|---|
| Intake validation and persistence | OpenWorker deterministic lifecycle | No model may create or bypass lifecycle state. |
| Lifecycle clarification | OpenWorker deterministic policy plus human gate | The current clarification question is deterministic; the user's answer is persisted before the immutable DAG is created. No model can answer or bypass this gate. |
| Semantic task understanding | Codex orchestrator | `codex-subscription:gpt-5.6-sol@max`, read-only. The `understand` node runs after plan approval and produces an execution handoff; it does not replace lifecycle intake or clarification. |
| Complexity and risk classification | OpenWorker deterministic policy | The policy result is authoritative. A model assessment can only be advisory evidence. |
| Optional semantic scoring role | Codex scorer | `codex-subscription:gpt-5.6-sol@max`, read-only and advisory when a custom plan includes it; the default template does not replace deterministic scoring with a scorer node. |
| Repository exploration | Codex explorer | `codex-subscription:gpt-5.6-sol@max`, read-only. |
| Implementation planning handoff | Codex planner | `codex-subscription:gpt-5.6-sol@max`, read-only. It consumes upstream durable evidence for the worker; it cannot create or mutate the already-approved DAG. |
| Implementation | Codex worker | `codex-subscription:gpt-5.6-sol@max`, writable disposable candidate only. |
| Candidate integration (when a custom plan includes it) | Codex integrator | `codex-subscription:gpt-5.6-sol@max`, writable integration candidate only. |
| Independent review | Claude reviewer | `claude-code-subscription:claude-opus-5@high`, fresh read-only session. |
| Independent tests | Claude tester | `claude-code-subscription:claude-opus-5@max`, fresh disposable test snapshot; test-side writes cannot be published. |
| Evidence evaluation | Claude evaluator | `claude-code-subscription:claude-opus-5@max`, fresh read-only session with an `all` join. |
| Final acceptance and publication | OpenWorker deterministic policy plus the required human gate | Models provide evidence, but cannot accept or publish their own result. |
| Archive | OpenWorker deterministic lifecycle | Seals the accepted subject, verdicts, evidence, event chain, and publication receipt. |

The default responsibility flow is:

```text
deterministic intake / complexity policy
                   |
 deterministic clarification / human answer when required
                   |
 deterministic DAG template validation / plan approval
                   |
  execution stage: Codex understand
                   |
          Codex exploration
                   |
  Codex implementation-plan handoff
                   |
      Codex worker(s) / integrator
                 /   \
     Claude High       Claude Max
       reviewer           tester
                 \   /
          Claude Max evaluator
                   |
    deterministic acceptance policy ---- human final gate when required
                   |
       deterministic publication / archive
```

Using Codex for several responsibilities does **not** mean reusing one conversation.
The default graph's orchestrator, explorer, planner, worker, reviewer, tester, and
evaluator runs are distinct task-root sibling runtime identities, each with a separate
hidden OpenWorker session and separate vendor session/thread. They are not a private
vendor-created subagent chain. Only persisted task input, dependency evidence, the
approved immutable plan, and explicitly authorized artifacts cross role boundaries. The
planner never lends its thread or write authority to the worker. The worker and
integrator never lend their context, transcript, or workspace to Claude's formal
verifier sessions.

Permission isolation remains authoritative regardless of model choice:

- semantic understanding, repository exploration, implementation planning, review, and
  evaluation are read-only; lifecycle clarification remains a deterministic human gate;
- worker writes are confined to an isolated candidate, and integrator writes are confined
  to the orchestration-owned integration candidate;
- tester-side writes are confined to a disposable verification snapshot and are excluded
  from the publication manifest; and
- final publication still requires the exact accepted manifest and acceptance-contract
  hashes. A model response cannot publish files.

The preset has no silent vendor substitution. In particular,
`kimi-code-subscription:kimi-code/k3@max` is neither a default role nor a fallback. It
remains visible in health/catalog output so an operator can diagnose the local CLI, but a
managed OAuth Kimi subscription is policy-blocked for unattended DAG execution. If a
required Codex or Claude runtime is unavailable, the task fails routing visibly rather
than weakening role independence. The preset freezes `fallback_mode=strict`; the normal
`quality-first` policy also disables fallback for an explicit model by default. A future
Kimi Platform API or separately authorized enterprise automation credential must be
configured as an explicit API-backed policy; it does not implicitly alter this preset.

The preset's built-in `codex-led-code-v1` template deterministically creates and freezes
the audited topology
`understand -> explore -> plan -> execute`, fans `execute` out to independent `review`
and `test` nodes, then joins both at `evaluate`. This is still a deterministic graph
template: selecting a preset does not let any vendor Agent privately manufacture nodes.
A plan-approval gate, when required, approves this graph before any of those nodes run;
the Codex node named `plan` produces the Worker's durable implementation handoff and does
not revise the DAG or trigger a second plan gate.
A custom plan may replace the template or add worker/integrator branches; the preset
supplies the appropriate role runtime wherever a node does not already declare an
explicit `model`.

### Exact runtime catalog

The logical runtime ID is an OpenWorker routing identifier. The vendor model and
reasoning effort remain separate audited fields; for example, `max` is an effort value,
not part of the Codex model slug.

| OpenWorker runtime ID | CLI | Exact vendor model | Effort | Background DAG eligibility |
|---|---|---|---|---|
| `codex-subscription:gpt-5.6-sol@max` | Codex | `gpt-5.6-sol` | `max` | Available when the local CLI, ChatGPT login, model catalog, and loopback-owner checks pass. |
| `claude-code-subscription:claude-opus-5@high` | Claude Code | `claude-opus-5` | `high` | Available with a supported first-party `claude.ai` login on the loopback owner's machine. |
| `claude-code-subscription:claude-opus-5@max` | Claude Code | `claude-opus-5` | `max` | Same runtime with a distinct, auditable effort contract. |
| `kimi-code-subscription:kimi-code/k3@max` | Kimi Code | `kimi-code/k3` | `max` through `KIMI_MODEL_THINKING_EFFORT` | Foreground personal interaction only. Production roles/background execution are blocked because ACP has no privileged system/developer layer and managed OAuth is not authorized for unattended automation. |

Do not use invented IDs such as `gpt-5.6-sol-max` or
`codex-subscription:gpt-5.6-sol-max`. Routing must use the complete logical ID from the
first column.

The adapters currently require Codex CLI `>= 0.146.0`, Claude Code `>= 2.1.219`, and
Kimi Code `>= 0.29.2`. A minimum version is not treated as proof by itself: the health
probe also checks required command capabilities, the expected subscription auth kind,
and, where the CLI exposes it without a model call, the requested model/effort catalog.
There is no silent model downgrade. A provider reroute, mismatched returned model, or
missing required effort fails the run.

### 1. Install and sign in to the three CLIs

Install each vendor CLI using its official instructions, then authenticate it as the
same OS user that launches OpenWorker:

```powershell
codex login
codex login status

claude auth login
claude auth status --json

kimi login
kimi provider list
```

Codex health requires a ChatGPT subscription login, not an API-key login. Claude health
requires `authMethod=claude.ai` and `apiProvider=firstParty`. Kimi discovery requires the
managed `kimi-code` OAuth provider and the `kimi-code/k3` model, but successful discovery
does not make that subscription eligible for unattended DAG execution.

OpenWorker does not import, copy, serialize, return, or log vendor access tokens. The
child process reads the vendor CLI's own user-scoped credential store. API-key and model
override variables such as `OPENAI_API_KEY`, `CODEX_MODEL`, `ANTHROPIC_API_KEY`,
`ANTHROPIC_MODEL`, `MOONSHOT_API_KEY`, and `KIMI_API_KEY` are removed from the child
environment so a subscription node cannot silently become an API-billed run or switch
models. Generic credential-shaped variables—including the OpenWorker launch token,
GitHub/cloud tokens, access keys, passwords, client secrets, cookies, Git askpass, and
SSH agent sockets—are stripped as well. Normal vendor subscription limits and usage
policies still apply.

These runtimes are intentionally local-owner-only. Start the server on `127.0.0.1` or
`localhost`; binding it to `0.0.0.0`, `::`, or another non-loopback address makes Codex
and Claude subscription candidates policy-ineligible even when their logins are valid.
This prevents a personal subscription login from becoming a shared hosted backend.

### 2. Start OpenWorker and inspect readiness

Set a long random launch token and start the server in one PowerShell window. Use the
same token value in the client window:

```powershell
$env:COWORKER_API_TOKEN = "replace-with-a-long-random-local-token"
openworker-server --host 127.0.0.1 --port 8765 --cwd "C:\work\repository"
```

In another PowerShell window:

```powershell
$env:COWORKER_API_TOKEN = "replace-with-the-same-long-random-local-token"
$headers = @{ "X-OpenWorker-Token" = $env:COWORKER_API_TOKEN }

Invoke-RestMethod `
  -Uri "http://127.0.0.1:8765/v1/orchestration/subscription-runtimes?refresh=true" `
  -Headers $headers
```

The endpoint is a zero-model-call probe. It returns, for every runtime, the installed,
authenticated, available, and policy-eligible flags; CLI filename/version; sanitized auth
kind; exact model/effort mapping; protocol; and a human-readable failure reason. It never
returns an access token, full credential record, or a model response. Results are cached
for 30 seconds; `refresh=true` forces another local CLI probe.

The same information appears in **Settings → Model routing → Subscription Agent
Runtimes**. The Refresh button calls the same endpoint. The ordinary model catalog also
marks these entries with `source=subscription-runtime`:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8765/v1/orchestration/model-catalog" `
  -Headers $headers
```

Interpret the most important health combinations as follows:

| Health | Meaning | Action |
|---|---|---|
| `installed=true`, `authenticated=true`, `available=true`, `policy_eligible=true` | The runtime can be selected now. | Create a task or assign it to a plan node. |
| `installed=false` | CLI executable is absent from the server process's `PATH`. | Install it or correct the service environment, then refresh. |
| `authenticated=false` | The expected subscription login was not detected. | Run the matching login command as the OpenWorker OS user. |
| `available=false`, `policy_eligible=false`, loopback reason | The server is remotely bound. | Restart on `127.0.0.1`/`localhost`. |
| Kimi authenticated but `blocked_by_policy` | Managed OAuth exists, but unattended subscription execution is prohibited. | Use the Kimi Platform API provider or obtain a separate enterprise automation agreement. |

For default mixed routing through the GUI, open **Tasks → New** for a code task and keep
**Runtime preset** on `production-codex-led-mixed-v1`. Leave **Requested model /
Subscription Agent runtime** on Automatic: a uniform requested model and a runtime preset
are mutually exclusive. Unavailable and policy-blocked entries remain visible in
Settings but are disabled for execution. Selecting one uniform runtime is an explicit
alternative to the mixed preset and applies to every generated node without an explicit
node model.

### 3. Select and inspect the default mixed preset

Runtime presets have a read-only discovery endpoint. It does not execute a model:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8765/v1/orchestration/runtime-presets" `
  -Headers $headers
```

The response is a top-level array. Each entry includes its immutable preset snapshot,
role assignments, required logical runtime IDs, default-domain flag, and sanitized
`available`/`unavailable_runtime_ids`/`availability_reason` readiness. Add
`?refresh=true` to repeat the zero-model-call local CLI health probes instead of using
their short cache. Discovery can describe an unavailable preset. The GUI prevents a new
task from starting with it; an API-created workflow still fails routing closed at the
required node rather than substituting another runtime.

For REST task creation, pass the stable preset ID explicitly. Runtime presets are
currently valid only for `domain=code`:

```powershell
$headers = @{
  "X-OpenWorker-Token" = $env:COWORKER_API_TOKEN
  "Idempotency-Key" = "codex-led-mixed-demo-20260803-1"
}
$body = @{
  title = "Repair with the default mixed runtime"
  objective = "Implement the scoped repair and prove every acceptance criterion."
  domain = "code"
  workspace = "C:\work\repository"
  runtime_preset_id = "production-codex-led-mixed-v1"
  acceptance_criteria = @(
    "The defect is repaired with a regression test",
    "Independent review reports no blocking issue",
    "The targeted test suite passes"
  )
  require_review = $true
  require_tests = $true
  budget = @{
    model_calls = 12
    tool_calls = 80
    tokens = 600000
    wall_seconds = 3600
  }
} | ConvertTo-Json -Depth 20

$task = Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8765/v1/orchestration/tasks" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $body
```

This production preset requires both independent review and independent testing. The
example spells out `require_review=true` and `require_tests=true` for readability, but
omitting them (or sending `false`) cannot weaken the preset's required verifier roles.
The `budget` object in this example is retained for compatibility and audit replay, but
the desktop product does not enforce it while `runtime_budget_mode=unlimited`. An embedder
that explicitly enables finite budgets verifies that the task budget can admit all seven
template nodes before creating the task.

Do not put top-level `requested_model` in the same request: it means "use one runtime"
and is therefore incompatible with a role-based mixed preset. Omitting
`runtime_preset_id` from an API request intentionally retains legacy automatic routing;
the server does not silently reinterpret existing integrations. The task and plan audit
snapshots record the selected preset and the concrete runtime chosen for each node.
Because this preset is strict, task creation also rejects a selected model policy with
`fallback_for_explicit=true` rather than allowing a role to drift to another model.

### 4. Route every node of a task to one subscription runtime

Set top-level `requested_model` when every node without an explicit model should use the
same runtime. This is an alternative to `runtime_preset_id`, not a preset override. This
minimal code task pins Codex GPT-5.6 Sol at max effort:

```powershell
$headers = @{
  "X-OpenWorker-Token" = $env:COWORKER_API_TOKEN
  "Idempotency-Key" = "codex-subscription-demo-20260803-1"
}
$body = @{
  title = "Subscription runtime smoke test"
  objective = "Inspect the repository and write a short architecture note."
  domain = "code"
  workspace = "C:\work\repository"
  acceptance_criteria = @(
    "The architecture note names the main execution boundary",
    "No file outside the workspace is modified"
  )
  constraints = @("Do not commit or push")
  requested_model = "codex-subscription:gpt-5.6-sol@max"
  require_review = $true
  require_tests = $false
  budget = @{
    model_calls = 8
    tool_calls = 40
    tokens = 300000
    wall_seconds = 1800
  }
} | ConvertTo-Json -Depth 20

$task = Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8765/v1/orchestration/tasks" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $body

$task.id
```

Top-level `requested_model` is a routing constraint, not a mutable provider preference.
If it is unavailable or excluded by the published model policy, routing fails visibly;
OpenWorker does not fall back unless the frozen policy explicitly permits fallback for
an explicit request. Once a run checkpoint is bound to a subscription runtime, that
runtime ID is pinned for the attempt even if health changes later.

To use Claude instead, change only the logical ID:

```text
claude-code-subscription:claude-opus-5@high
claude-code-subscription:claude-opus-5@max
```

### 5. Use the preset with a custom isolated DAG

With `runtime_preset_id`, each custom-plan node that omits `model` receives the preset's
runtime for that Agent role. An explicit node-level `model` still wins for that node. The
following graph adds Codex exploration and planning before implementation, then joins
independent Claude review and testing at a Claude evaluator while preserving
OpenWorker's parent/child ledger and role isolation:

```powershell
$headers = @{
  "X-OpenWorker-Token" = $env:COWORKER_API_TOKEN
  "Idempotency-Key" = "mixed-subscription-dag-20260803-1"
}
$body = @{
  title = "Repair with independent subscription Agents"
  objective = "Implement the scoped repair, independently review and test it, then evaluate every acceptance criterion."
  domain = "code"
  workspace = "C:\work\repository"
  runtime_preset_id = "production-codex-led-mixed-v1"
  acceptance_criteria = @(
    "The defect is repaired with a regression test",
    "The Reviewer reports no blocking issue",
    "The Tester reports the targeted suite passes"
  )
  constraints = @("Do not commit or push", "Do not publish externally")
  require_review = $true
  require_tests = $true
  max_parallel_runs = 2
  budget = @{
    model_calls = 12
    tool_calls = 80
    tokens = 600000
    wall_seconds = 3600
  }
  plan = @{
    nodes = @(
      @{
        key = "understand"
        kind = "agent"
        agent = "orchestrator"
        instructions = "Structure the objective, constraints, and acceptance contract without changing files."
        effect_safety = "read_only"
      },
      @{
        key = "explore"
        kind = "agent"
        agent = "explorer"
        instructions = "Inspect the repository read-only and collect evidence for the requested change."
        effect_safety = "read_only"
      },
      @{
        key = "plan"
        kind = "agent"
        agent = "planner"
        instructions = "Turn the objective and repository evidence into an implementation plan."
        effect_safety = "read_only"
      },
      @{
        key = "execute"
        kind = "execute"
        agent = "worker"
        instructions = "Implement the smallest compatible repair and its regression test."
        effect_safety = "idempotent"
      },
      @{
        key = "review"
        kind = "review"
        agent = "reviewer"
        instructions = "Review the staged candidate independently against every criterion."
      },
      @{
        key = "test"
        kind = "test"
        agent = "tester"
        instructions = "Run the regression and targeted suites in an isolated test workspace."
      },
      @{
        key = "evaluate"
        kind = "evaluate"
        agent = "evaluator"
        instructions = "Evaluate Worker, Reviewer, and Tester evidence against every acceptance criterion."
      }
    )
    edges = @(
      @{ from = "understand"; to = "explore"; condition = "success" },
      @{ from = "explore"; to = "plan"; condition = "success" },
      @{ from = "plan"; to = "execute"; condition = "success" },
      @{ from = "execute"; to = "review"; condition = "success" },
      @{ from = "execute"; to = "test"; condition = "success" },
      @{ from = "review"; to = "evaluate"; condition = "success" },
      @{ from = "test"; to = "evaluate"; condition = "success" }
    )
  }
} | ConvertTo-Json -Depth 30

$task = Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8765/v1/orchestration/tasks" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $body
```

The omitted node models resolve as follows:

| Node roles | Resolved runtime |
|---|---|
| orchestrator, scorer, explorer, planner, worker, integrator | `codex-subscription:gpt-5.6-sol@max` |
| reviewer | `claude-code-subscription:claude-opus-5@high` |
| tester, evaluator | `claude-code-subscription:claude-opus-5@max` |

The older fully explicit per-node API remains supported. To pin or deliberately replace
one role, add its exact logical runtime as that node's `model`; do not also add a
top-level `requested_model`. Explicit node selection is captured in the immutable plan
and is not rewritten by the preset.

OpenWorker still owns the hierarchy. Codex and Claude are forbidden from creating
private subagents, and their built-in Agent/Task capabilities are disabled or removed.
The Worker receives a writable disposable candidate. Reviewer and Evaluator receive
read-only surfaces. Tester may write only to its disposable test snapshot for build
caches and test output; its changes are never published. Each role uses a separate
hidden OpenWorker session and a separate vendor session/thread, so Worker memory and
transcripts are not reused as independent review evidence.

The result schema is mandatory. Every runtime must return `summary`, overall
`pass|fail|unknown`, per-criterion verdicts, touched files, checks, and remaining risks.
Reviewer, Tester, Evaluator, and Scorer results are mapped into formal verdict evidence;
a missing or malformed verdict fails the node and is never treated as approval.
This provider-neutral Subscription Agent payload is stored as `structured_result`. It is
not a `complete_task` submission: only an explicit `handoff_result` (or the legacy
`criterion_results` plus `work_products` shape) enters atomic Brief/deliverable
settlement. Keeping those contracts distinct prevents criterion text from being
misread as a missing Brief criterion ID.

The task wizard defaults writable tasks to an `implementation_patch` deliverable and
read-only tasks to an `artifact` report. The backend rejects a read-only Brief that
requires a patch, commit, branch, or pull request instead of allowing an impossible
contract to fail after model execution.

### 6. Recovery, cancellation, and audit behavior

The subscription runtime layer adds the following durable boundaries:

| Boundary | Codex | Claude Code |
|---|---|---|
| Before work | Validate CLI version, ChatGPT auth, app-server, exact model and max effort. | Validate CLI version/headless flags and first-party `claude.ai` auth. |
| Session binding | Persist durable `thread.id` before `turn/start`. | Persist deterministic session reservation before process launch; persist process/prompt states under the live lease. |
| Active work | Persist `turn.id`; consume bounded JSONL events; keep stderr separate; deny unexpected approvals; pin model/provider. | Consume bounded `stream-json`; validate session/model; enforce tool allow/deny sets and `dontAsk`; keep stderr separate. |
| Successful terminal | Seal structured output, usage, sanitized events, and identity in a content-addressed recovery blob before the run commit. | Seal the same terminal result before the run commit. |
| Restart after terminal | Recover the sealed result without another model call; persistent thread history is an additional reconciliation source. | Recover the sealed result without another model call. |
| Restart during uncertain in-flight work | Resume the persistent thread and reconcile the checkpointed turn; never replay an unknown active non-idempotent turn. | Fail with `recovery_state_uncertain` because the headless CLI has no zero-model-call turn-status query; formal reconciliation is required instead of resubmitting the prompt. |
| Cancellation | Send `turn/interrupt`, wait briefly for terminal status, then reap the full process tree. | Reap the full process tree. |

Every checkpoint write requires the exact active lease token and fencing token. It binds
runtime ID, vendor model, effort, protocol, task/run attempt, workspace hash, prompt hash,
and output-schema hash. A stale worker cannot replace it. A checkpoint identity mismatch,
corrupt recovery blob, provider reroute, unauthorized built-in subagent event, malformed
JSONL, output overflow, or process-tree cleanup failure fails closed.

Protocol output is bounded to 8 MiB and stderr to 256 KiB. The immutable audit artifact
redacts credential-shaped values and hidden reasoning content, stores a SHA-256 content
hash, and appears as normal run evidence. Fetch its authorized blob through:

```text
GET /v1/orchestration/blobs/<sha256>
```

Use the task, run, transcript, evidence, and event endpoints from the REST section below
to audit the full chain. Subscription usage is reported into the same model-call,
tool-call, token, and wall-time usage ledger as native runs; in the desktop product this
ledger is observational rather than an interruption ceiling.

### Live per-run Agent activity

Clicking **Progress** beside a Run opens a live inspector. The GUI loads the newest
bounded page and then polls incremental rows every 1.5 seconds, so a long subscription
turn is visible before its final `turn/completed` event. Rows with the same provider
source identity are grouped into an expandable step: a tool's start and completion form
one card, and streamed reasoning-summary chunks form one readable summary. The inspector
also shows the latest cumulative input, cached-input, output, and total token counts.

This stream is an operator diagnostic, not chain-of-thought storage. Codex contributes
only its explicit `item/reasoning/summaryTextDelta` summary channel. Native
`REASONING_DELTA` text, vendor thinking blocks, raw tool results, command stdout/stderr,
patch bodies, and file contents are never written to `orch_run_activity`. Native runs
may show a content-free “model is reasoning” lifecycle marker while reasoning is in
progress, but never its text. Tool rows may
contain bounded, credential-redacted metadata such as the tool name, command preview,
working directory, duration, exit code, and terminal status. Every append requires the
current lease and fencing token, is idempotent by `(run_id, event_key)`, is immutable at
the database layer, and is capped at 5,000 retained rows plus one truncation marker.

The endpoint supports both newest-page and forward-delta reads:

```text
GET /v1/orchestration/tasks/<task-id>/runs/<run-id>/activity?latest=true&limit=500
GET /v1/orchestration/tasks/<task-id>/runs/<run-id>/activity?after_sequence=<cursor>&latest=false&limit=500
```

The response carries `privacy.reasoning=provider_summary_only` and
`privacy.tool_output=metadata_only` so other clients do not mistake it for a raw trace.

### 7. Kimi Code compliance boundary

`kimi-code-subscription:kimi-code/k3@max` is intentionally present in health and catalog
responses so operators can see the installed K3/max mapping and the exact reason its use
is limited. Kimi ACP supplies no true system/developer message hierarchy: the
compatibility envelope sent by OpenWorker has user-prompt priority and cannot safely
carry production control instructions. The runtime is therefore restricted to
foreground personal interaction and cannot be enabled for a production task role by a
task field, environment variable, model policy, or UI action. Its executor returns
`subscription_noninteractive_automation_forbidden` before building a command or spawning
a process.

Kimi's native shell is disabled on every supported platform, including foreground
sessions and every OpenWorker permission mode. This is a host-enforced capability
boundary, not guidance for the model. By comparison, Claude Code native Bash is disabled
on Windows; on macOS/Linux it remains bounded by the Claude SDK sandbox and OpenWorker's
host approval/workspace policy. All Subscription Agent runtimes reject OpenWorker
attachments and `/skill` sidecars because those belong to OpenWorker's API-provider tool
loop, not the vendors' native session protocols.

For unattended Kimi DAG nodes, configure OpenWorker's normal Kimi/Moonshot API provider
with a Platform API credential, or use a credential covered by a separate enterprise
automation agreement. Do not place a managed OAuth subscription token into an API-key
field. See the current [Kimi Code community guidelines](https://www.kimi.com/code/docs/en/kimi-code/community-guidelines.html)
and [Kimi Code configuration reference](https://moonshotai.github.io/kimi-code/en/configuration/config-files).

The Codex adapter follows the official [Codex app-server protocol](https://learn.chatgpt.com/docs/app-server)
and its persistent thread/turn model; `max` is sent as turn effort rather than appended
to the model name. Claude model/effort and headless flags follow the official
[Claude Code model configuration](https://code.claude.com/docs/en/model-config) and
[headless mode](https://code.claude.com/docs/en/headless) documentation. Because these
vendor contracts evolve independently, health is always the source of truth on the
machine that will execute the node.

## REST example

All requests use the existing OpenWorker launch token in `X-OpenWorker-Token`. The
following custom plan runs execution and integration first, then isolates review and
testing, and joins their evidence at a read-only evaluator.

```http
POST /v1/orchestration/tasks
Content-Type: application/json
X-OpenWorker-Token: <launch-token>
Idempotency-Key: release-audit-2026-08-03

{
  "title": "Audit and repair release candidate",
  "objective": "Inspect the repository, implement the repair, independently review and test it, then formally accept the result.",
  "domain": "code",
  "workspace": "C:/work/repository",
  "brief": {
    "title": "Audit and repair release candidate",
    "objective": "Inspect the repository, implement the repair, independently review and test it, then formally accept the result.",
    "background": "A release candidate needs an isolated implementation and independent verification.",
    "scope": {"whole_task": true, "reason": "The release-candidate workspace is the explicit task subject."},
    "instructions": ["Locate the regression", "Implement the smallest repair", "Review and test the final candidate"],
    "constraints": ["Do not modify generated files", "Do not publish externally"],
    "non_goals": ["Do not redesign unrelated modules"],
    "acceptance_criteria": [
      {"id": "AC-01", "text": "The regression test fails before and passes after the repair", "required": true},
      {"id": "AC-02", "text": "Reviewer finds no blocking issue", "required": true},
      {"id": "AC-03", "text": "The complete targeted test suite passes", "required": true}
    ],
    "deliverables": [
      {"id": "DEL-01", "kind": "implementation_patch", "title": "Bounded repair", "required": true},
      {"id": "DEL-02", "kind": "test_result", "title": "Verification evidence", "required": true}
    ],
    "result_contract": {"schema_id": "implementation_result_v1"}
  },
  "runtime_preset_id": "production-codex-led-mixed-v1",
  "require_review": true,
  "require_tests": true,
  "max_parallel_runs": 4,
  "budget": {
    "model_calls": 16,
    "tool_calls": 80,
    "tokens": 500000,
    "wall_seconds": 3600
  },
  "plan": {
    "nodes": [
      {"key": "inspect", "kind": "agent", "agent": "explorer", "instructions": "Locate the defect and collect evidence."},
      {"key": "implement", "kind": "execute", "agent": "worker", "instructions": "Implement the smallest compatible repair.", "effect_safety": "idempotent"},
      {"key": "integrate", "kind": "integrate", "agent": "integrator", "instructions": "Integrate the implementation into the final staged candidate."},
      {"key": "review", "kind": "review", "agent": "reviewer", "instructions": "Review the final staged candidate independently."},
      {"key": "test", "kind": "test", "agent": "tester", "instructions": "Run independent tests against the final staged candidate."},
      {"key": "evaluate", "kind": "evaluate", "agent": "evaluator", "instructions": "Evaluate all evidence against every acceptance criterion."}
    ],
    "edges": [
      {"from": "inspect", "to": "implement", "condition": "success"},
      {"from": "implement", "to": "integrate", "condition": "success"},
      {"from": "integrate", "to": "review", "condition": "success"},
      {"from": "integrate", "to": "test", "condition": "success"},
      {"from": "review", "to": "evaluate", "condition": "success"},
      {"from": "test", "to": "evaluate", "condition": "success"}
    ]
  }
}
```

If the response contains a pending attention item, resolve it with the version shown in
the task detail:

```http
POST /v1/orchestration/tasks/<task-id>/gates/<gate-id>/resolve
Content-Type: application/json
X-OpenWorker-Token: <launch-token>

{
  "decision": "approve",
  "response": "Plan and permission boundary reviewed.",
  "expected_version": 1,
  "idempotency_key": "approve-release-audit-plan-v1"
}
```

Useful control-plane endpoints are:

- `GET /v1/orchestration/tasks?status=running&limit=100&offset=0`
- `GET /v1/orchestration/tasks/<task-id>`
- `GET /v1/orchestration/tasks/<task-id>/heartbeat-context?after_sequence=0`
- `GET /v1/orchestration/tasks/<task-id>/briefs`
- `POST /v1/orchestration/tasks/<task-id>/briefs` and `POST .../<revision>/publish`
- `GET /v1/orchestration/tasks/<task-id>/context-refs?limit=100&offset=0`
- `GET /v1/orchestration/context-refs/<ref-id>/content`
- `GET/POST /v1/orchestration/tasks/<task-id>/relations`
- `PUT /v1/orchestration/tasks/<task-id>/blockers`
- `GET/POST /v1/orchestration/tasks/<task-id>/comments`
- `GET/POST /v1/orchestration/tasks/<task-id>/work-products`
- `GET /v1/orchestration/tasks/<task-id>/wakes`
- `GET /v1/orchestration/wakes?status=failed&limit=100&offset=0`
- `POST /v1/orchestration/wakes/<wake-id>/retry`
- `GET/PUT /v1/orchestration/handoff-settings`
- `GET /v1/orchestration/tasks/by-idempotency-key/<key>`
- `GET /v1/orchestration/tasks/by-idempotency-key?idempotency_key=<opaque-key>`
- `GET /v1/orchestration/tasks/<task-id>/events?latest=true&limit=1000`
- `GET /v1/orchestration/tasks/<task-id>/runs?limit=200&offset=0`
- `GET /v1/orchestration/tasks/<task-id>/evidence?limit=200&offset=0`
- `GET /v1/orchestration/tasks/<task-id>/runs/<run-id>/activity?latest=true&limit=500`
- `GET /v1/orchestration/tasks/<task-id>/runs/<run-id>/transcript`
- `GET /v1/health` or `GET /v1/orchestration/health` (`503` for a stale/failed
  scheduler, lost leader, stopped outbox, or any dead letter)
- `GET /v1/orchestration/outbox/dead-letters`
- `GET /v1/orchestration/outbox/dead-letters/<id>`
- `POST /v1/orchestration/outbox/dead-letters/<id>/requeue`
- `GET /v1/orchestration/blobs/<sha256>`
- `GET /v1/orchestration/capabilities`
- `GET /v1/orchestration/agent-profiles`
- `GET /v1/orchestration/model-policies`
- `POST /v1/orchestration/model-policies/<id>/draft/simulate`
- `GET /v1/orchestration/model-catalog`
- `GET /v1/orchestration/subscription-runtimes?refresh=true`
- `GET /v1/orchestration/runtime-presets`

Task creation requires either the `Idempotency-Key` header or an equivalent body
`idempotency_key`; a lost HTTP response can be recovered by querying that key. Draft
profile and policy updates use ETags. Send the draft's exact `etag` in `If-Match`
when saving or publishing; stale editors receive a conflict rather than silently winning.
Structured-handoff mutations likewise accept `Idempotency-Key` or their typed body
operation field. A completed create command returns `200` on replay (`201` on first
creation), and reusing a key with different input returns `409`. Task Brief draft updates
and publication require the loaded content hash in `If-Match`.

Dead-letter requeue is a formal audited command. It requires an `Idempotency-Key`
header plus non-empty operator attribution and justification:

```http
POST /v1/orchestration/outbox/dead-letters/outbox_123/requeue
Idempotency-Key: requeue-outbox-123-after-subscriber-repair
Content-Type: application/json

{
  "actor": "on-call@example.com",
  "reason": "Subscriber repair was deployed and its health check passed."
}
```

Repeating the exact command key and body safely returns the original audit record with
`replayed: true`; reusing that key for another outbox item, actor, or reason returns
`409 Conflict`. The response and both dead-letter read endpoints expose the bounded
requeue history. Each entry is an append-only snapshot of `attempts`, `last_error`, and
`dead_lettered_at`, so a later delivery failure cannot erase the evidence for an earlier
operator decision.

## Upstream compatibility rules

To keep future OpenWorker merges tractable:

1. Domain code lives under `coworker/orchestration`; it does not fork `TurnEngine`, the
   provider router, session storage, or the existing automation scheduler.
2. The existing engine changes are generic optional seams: deferred interaction,
   recoverable state, and a post-assembly tool filter. Normal sessions do not opt in and
   retain their behavior.
3. Server integration is one manager-owned service and one router include.
4. GUI code is feature-scoped; shared changes are limited to navigation, settings tabs,
   authenticated JSON access, and WebSocket fan-out.
5. Orchestration has its own database, migrations, blobs, catalog, and workspaces. It does
   not alter the schema of `coworker.db` or `automation.db`.
6. Published migrations are append-only. Add the next contiguous migration, never edit one already
   shipped to users.
7. Executors depend on `RunExecutionContext -> ExecutionOutcome`. A future upstream Agent
   implementation can be adapted behind this boundary without changing lifecycle truth.

## Verification

Backend coverage is split by invariant: state machine, DAG, store/migrations, blobs,
policy, profiles/catalogs, routing, runtime, workspace recovery, executor suspension,
service lifecycle, and API contracts. GUI tests cover typed API mapping, the orchestration
surface, settings editors, and shared integration. Run:

```shell
python -m pytest
cd surfaces/gui
npm test
npm run build
```

For a focused TCHP change, run this shorter gate first:

```shell
python -m pytest tests/test_orchestration_handoff.py tests/test_orchestration_handoff_performance.py tests/test_orchestration_migrations.py
cd surfaces/gui
npm test -- --run src/features/orchestration/HandoffPanels.test.tsx src/features/orchestration/Settings.test.tsx src/features/orchestration/OrchestrationSurface.test.tsx
npm run build
```

The implementation-to-requirement evidence is maintained in
[`docs/specifications/structured-handoff-acceptance-matrix.md`](specifications/structured-handoff-acceptance-matrix.md).
The executable Agent procedure is
[`orchestration-handoff`](../.agents/skills/orchestration-handoff/SKILL.md).

For any failed run, inspect task detail in this order: open gate, latest run attempt,
evidence artifact hash, event-chain verification, then workspace recovery journal. Do not
manually edit `orchestration.db`; recovery decisions are commands and must remain audited.

## Explicit production boundaries

- The source fence coordinates OpenWorker processes. An editor or hostile process that
  ignores it can still race an individual filesystem replace in a very small window;
  require repository quiescence, Git ref/index CAS, a container, or an external workspace
  lock when non-cooperating writers are in scope.
- The hash chain detects accidental corruption and makes ordinary tampering evident, but
  it is stored in the same local database. Protection against an administrator who can
  rewrite the entire database and recompute every hash requires exporting signed tip
  hashes to an external transparency or audit system.
- Connectors and user-installed skills are intentionally excluded from the default
  orchestration runtime. They need an explicit future adapter and permission policy; the
  core currently concentrates on durable local-file, bounded-shell, and built-in
  verification workflows.
- SQLite and local workspaces define the supported single-host deployment. Horizontal
  execution needs a shared database, distributed leases, shared artifact storage, and a
  stronger workspace isolation backend.
