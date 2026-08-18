# Structured Handoff Acceptance Matrix

Validated on 2026-08-17 against
[`Detailed_Implementation_Specification.md`](Detailed_Implementation_Specification.md).
The accepted scope is OpenWorker orchestration's Task-Centric Handoff Protocol (TCHP),
including migrations, compatibility, runtime behavior, REST/UI surfaces, security, and
performance contracts.

## Verdict

All TCHP acceptance criteria and its scoped regression gates pass. No known P0/P1 defect
remains in this scope. The repository-wide Python suite is not globally green in this
Windows environment: 25 unrelated tests require optional Bedrock/Slack packages, assume
POSIX permission/path casing, or exercise pre-existing connector timing. Those failures
are recorded under **Validation results** and are not counted as TCHP passes.

## Definition of Done

| Requirement | Status | Evidence |
|---|---|---|
| 0007–0009 migration and legacy backfill | Pass | `test_0006_store_upgrade_backfills_briefs_and_parent_relations_idempotently`; all 1–10 current ledger entries and FK checks pass after two opens. `test_legacy_upstream_input_is_externalized_once_without_mutating_compatibility_data` covers legacy input externalization. |
| Brief, ContextRef, Relation, Wake, Comment, Work Product domains | Pass | `test_orchestration_handoff.py`; immutable triggers and canonical records in migrations 0007–0009. |
| REST API and run-bound tools | Pass | `test_handoff_api_exposes_lazy_metadata_without_transcript_or_file_body`; typed request schemas; `Idempotency-Key`/body mismatch, create replay `200`, ETag, and run-bound identity are asserted. |
| No raw upstream data in `_user_prompt()` | Pass | Envelope compactness tests plus `test_current_subscription_prompt_excludes_raw_upstream_but_accepts_frozen_v2_hash`; frozen old hashes remain recovery-only. |
| Legacy `spawn_agent` compatibility | Pass | `test_child_delegation_is_stable_across_lost_attempts_and_keeps_logical_ownership`; synthetic Published Brief and `legacy_delegation_used` are asserted. |
| Parent/child and blocker wakes | Pass | `test_last_terminal_child_wakes_parent_with_bounded_result_refs_only`; blocker completion/cancellation tests. |
| GUI tabs, settings, and diagnostics | Pass | `HandoffPanels.test.tsx`, `Settings.test.tsx`, `OrchestrationSurface.test.tsx`; complete GUI suite passes. |
| Agent skill and operator docs | Pass | `.agents/skills/orchestration-handoff/SKILL.md` passes `quick_validate.py`; `docs/orchestration.md` documents protocol, settings, APIs, recovery, and compatibility. |
| Unit, integration, security, performance, and UI tests | Pass | 247 orchestration tests and 147 GUI tests pass; PERF-01–05 have dedicated fixtures. |
| Feature-flag rollout | Pass | Runtime settings persist, validate, and hot-apply; default is Stage C (`enabled=true`, `required=false`, legacy enabled). |
| Observability contract | Pass | All 13 specified metric names are present from process start; event rows expose content-free resource IDs, actor, correlation/causation, timestamp, and hash chain. |
| No known P0/P1 defect in TCHP | Pass | Scoped full regression, production build, diff integrity, and acceptance audit complete. |

## Functional acceptance

| ID | Status | Primary automated evidence |
|---|---|---|
| AC-F-001 Structured Delegation | Pass | `test_incomplete_delegation_is_rejected_without_partial_rows` also verifies successful atomic child + Brief + relation + wake creation. |
| AC-F-002 Incomplete Brief rejected | Pass | `test_incomplete_delegation_is_rejected_without_partial_rows`. |
| AC-F-003 Published Brief immutable | Pass | `test_brief_revisions_are_validated_immutable_and_run_snapshotted`; API hash-conflict checks. |
| AC-F-004 Context on-demand | Pass | Manifest/lazy API tests, stale required/recommended semantics, MIME sniffing, verification events, and `test_required_context_preflight_reconciles_without_executor_call`. |
| AC-F-005 Prompt independent of repository size | Pass | `test_perf_01_prompt_construction_never_traverses_workspace`. |
| AC-F-006 Legacy spawn | Pass | `test_child_delegation_is_stable_across_lost_attempts_and_keeps_logical_ownership`. |
| AC-F-007 Atomic checkout | Pass | `test_atomic_run_claim_fencing_and_completion`; `test_perf_05_eight_concurrent_agents_cannot_duplicate_run_claim`. |
| AC-F-008 Comment delta wake | Pass | `test_comment_wakes_coalesce_and_mentions_do_not_change_task_owner`; configurable zero-window test. |
| AC-F-009 Mention does not transfer ownership | Pass | comment/mention test; same-tree and receiver-policy tests. |
| AC-F-010 Child completion wake | Pass | `test_last_terminal_child_wakes_parent_with_bounded_result_refs_only`; parent projection/edge atomicity and startup consistency are separately asserted. |
| AC-F-011 Blocker resolution | Pass | `test_relation_cycles_block_and_terminal_resolution_wakes_dependents`. |
| AC-F-012 Canceled blocker | Pass | `test_canceled_blocker_keeps_dependent_blocked_and_opens_attention`. |
| AC-F-013 No busy polling | Pass | `test_parent_waits_for_unjoined_child_and_parent_run_is_auditable`; the parent releases into durable child-wait state and resumes after completion. |
| AC-F-014 Structured result | Pass | `test_structured_completion_validates_products_and_commits_atomically`. |
| AC-F-015 Verification-role isolation | Pass | `test_code_task_requires_plan_and_final_gates_with_isolated_roles`; runtime-preset fresh-session assertions; prompt/transcript exclusion tests. |
| AC-F-016 Gate resume delta | Pass | `test_permission_gate_releases_and_resumes_the_same_hidden_session` asserts the `gate_resolved` wake's answer delta and checkpoint ref. |
| AC-F-017 Restart recovery | Pass | wake claim recovery/dead-letter test; `test_permission_gate_and_hidden_session_resume_after_process_restart`; child-result restart test. |
| AC-F-018 Dead letter | Pass | `test_wake_claim_recovery_and_dead_letter_are_durable`; GUI failed-wake retry component test. |
| AC-F-019 UI handoff visibility | Pass | `HandoffPanels.test.tsx` verifies lazy Brief/context/dependency/comment/product/wake tabs and explicit content read. |
| AC-F-020 Profile policy enforcement | Pass | `test_reviewer_policy_rejects_delegation_before_any_side_effect`; mention receiver policy test. |

## Security acceptance

| ID | Status | Enforcement/evidence |
|---|---|---|
| SEC-01 path traversal | Pass | `test_context_workspace_path_rejects_absolute_unc_and_nul`, `../` resolution, and direct Work Product traversal rejection. |
| SEC-02 escaping symlink | Pass | `test_context_and_comment_security_boundaries_fail_closed`; resolved path must remain under the canonical root. |
| SEC-03 cloud-metadata/SSRF URL | Pass | URL ContextRefs never fetch directly; the metadata endpoint fails with network disabled and guarded-web guidance. |
| SEC-04 prompt injection in context | Pass | Every content read is prefixed with the untrusted-data boundary; Brief/profile/tool policy remains authoritative. |
| SEC-05 forged actor | Pass | Agent comment/product/blocker tools derive author from the run-bound Profile and validate current lease/fence; no actor argument is exposed. |
| SEC-06 mutate parent/published Brief | Pass | Published-row update/delete triggers plus service-level revision/hash enforcement. |
| SEC-07 reviewer checkout of Worker task | Pass | Claims are scheduler-owned; a role receives a frozen node/profile and cannot select another task/run identity. |
| SEC-08 stale fencing token | Pass | `test_atomic_run_claim_fencing_and_completion` and tool scheduler-fence tests. |
| SEC-09 raw `@Reviewer` | Pass | `test_raw_at_name_is_not_a_machine_mention_and_receiver_policy_is_enforced`. |
| SEC-10 mention storm | Pass | Maximum 10 structured targets/comment and 100 live mention wakes/source task; over-limit writes roll back. |
| SEC-11 secret-like inline value | Pass | Secret excerpt downgrades to metadata-only and content read fails closed; Brief, ContextRef, relation, blocker, comment, and Work Product metadata reject secret-shaped values. Legacy synthetic Briefs redact them. |
| SEC-12 transcript ref without policy | Pass | No transcript ContextRef type exists; profile default forbids full transcript reference and initial envelopes exclude transcripts. |
| SEC-13 relation cycle | Pass | Direct and multi-hop cycle checks run inside the write transaction. |
| SEC-14 executable artifact auto-open | Pass | Work Products expose metadata/reference only; UI neither executes nor auto-opens artifacts. |
| SEC-15 oversized comment/context | Pass | `test_large_comment_is_externalized_as_content_addressed_work_product`; 65,536-byte inline ceiling, content-addressed Markdown artifact, per-ref/total inline limits, and read ceiling. |

## Performance acceptance

Dedicated tests live in `tests/test_orchestration_handoff_performance.py`.

| ID | Status | Fixture and bounded behavior |
|---|---|---|
| PERF-01 | Pass | Workspace traversal is trapped; equivalent 100-file/50,000-file workspace identities produce identical prompts below 32 KiB. |
| PERF-02 | Pass | Policy rejects 51 normal refs; REST pagination returns the requested final 100 rows of a 1,000-ref historical fixture. |
| PERF-03 | Pass | A 10,000-comment fixture uses `orch_comments_delta` and returns only sequences 9,901–10,000. |
| PERF-04 | Pass | A 10,000-wake fixture uses `orch_wakes_ready` and claims the deterministic oldest ready wake. |
| PERF-05 | Pass | Eight concurrent claimers produce one claim and one active lease under WAL/busy timeout. |

## Validation results

| Command | Result |
|---|---|
| All 23 `tests/test_orchestration_*.py` files | **247 passed**, 1 deprecation warning, 89.07 s. |
| `npm test -- --run` in `surfaces/gui` | **147 passed** across 23 files, 4.31 s. |
| `npm run build` in `surfaces/gui` | TypeScript and Vite production build passed; only existing chunk-size/static+dynamic import warnings. |
| `quick_validate.py .agents/skills/orchestration-handoff` | **Skill is valid**. |
| `python -m compileall -q ...` and `git diff --check` | Passed; Git emitted only Windows LF→CRLF notices. |
| Repository-wide `python -m pytest -q --tb=short` | 1421 passed, 2 skipped, 25 unrelated failures in 275.13 s. Failures are missing optional `boto3`/`botocore`/`slack-bolt`/`acp`, Windows permission/path casing/handle behavior, and pre-existing connector/automation timing; no orchestration test failed. |

The repository-wide failures are not silently waived: a clean global suite requires the
project's optional provider/messaging dependencies and cross-platform baseline fixes.
They do not alter the scoped TCHP verdict because the complete orchestration and GUI
suites, including all new security/performance cases, pass independently.
