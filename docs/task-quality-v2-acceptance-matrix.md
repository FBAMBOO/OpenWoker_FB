# Task Quality V2 验收矩阵

验收日期：2026-08-19（Asia/Shanghai）\
实施规范：`docs/specifications/OpenWorker_Task_Quality_V2_End_to_End_Implementation_Spec_2026-08-18.md`
结论：**PASS（Stage 2 opt-in 工作树实现）**

## 1. 可复现身份与环境

本次实现未在未经用户明确要求的情况下创建 Git commit。下列 base commit 与内容指纹共同唯一标识本次被验收的工作树；内容指纹覆盖所有 modified/untracked 实现文件，但有意排除本验收矩阵自身，避免自引用哈希。

| 项目 | 值 |
|---|---|
| Branch | `main` |
| Base commit | `8df02f973d2961e47bd81baac10b4b77b0f85060` |
| Implementation payload | 127 files |
| Payload fingerprint | `sha256:52770f7047c1e70dae429bcbd31e4a3449e00b2c268ed1c151ddbf958890e8f1` |
| Fingerprint recipe | 对 127 个 payload 文件按 repo-relative path 做 ordinal 排序；每行写入 `path<TAB>sha256(file bytes)<LF>`，再对 UTF-8 manifest 做 SHA-256；排除本矩阵自身。 |
| OS | Windows 11, `10.0.26200.0` |
| PowerShell | `5.1.26100.9168` |
| Python | `3.12.10` |
| SQLite | `3.49.1` |
| FastAPI / Pydantic / pytest | `0.141.1` / `2.13.4` / `9.1.0` |
| Node / npm | `v24.16.0` / `11.13.0` |
| Time zone | `China Standard Time` |

## 2. 发布命令证据

| 命令 | 结果 | 证据 |
|---|---|---|
| `python -m pytest -q tests/test_orchestration_*.py` | FAIL（未执行测试） | PowerShell 不展开该 Bash 风格 glob，pytest 报 `file or directory not found`。失败被保留，不作为产品失败。 |
| PowerShell 显式列举 `test_orchestration_*.py` 后执行 pytest | PASS | 276 passed，1 warning，143.36s。 |
| `python -m pytest -q tests/test_task_quality_*.py` | FAIL（未执行测试） | 同上，PowerShell 将 glob 原样传给 pytest。 |
| PowerShell 显式列举 `test_task_quality_*.py` 后执行 pytest | PASS | 98 passed，1 warning，93.20s。 |
| `python -m pytest -q tests/benchmarks/task_quality` | PASS | 7 passed，8.98s。 |
| `python -m pytest -q tests/test_subscription_runtime.py tests/test_orchestration_runtime_presets.py` | PASS | Windows path/sandbox/provider 专项 57 passed，1 warning，13.47s。 |
| `npm test -- --run`（`surfaces/gui`） | PASS | 26 files，164 tests。 |
| `npm run build`（`surfaces/gui`） | PASS | TypeScript + Vite，402 modules。 |
| `python -m compileall -q coworker` | PASS | 无输出。 |
| `python scripts/generate_task_quality_types.py --check` | PASS | checked-in TypeScript 枚举/转移表无漂移。 |
| `git diff --check` | PASS | 无 whitespace error；仅 Git autocrlf 提示。 |
| skill-creator `quick_validate.py .agents/skills/orchestration-handoff` | PASS | `Skill is valid!` |

非阻断警告：

- Starlette TestClient 提示未来需从当前 `httpx` 适配迁移到 `httpx2`；不影响断言。
- Vite 报告 `src/api.ts` 同时被动态/静态导入，并提示主 chunk 大于 500 kB；构建成功。
- Git 在 Windows 上提示部分 tracked 文件未来可能由 LF 转 CRLF；`git diff --check` 通过。

## 3. Test12 与规模基准

| 指标 | 实测 | 门槛 | 结果 |
|---|---:|---:|---|
| Quality score | 91 | >=85 | PASS |
| Required areas | 7/7 | 7/7 | PASS |
| Citation resolution | 100% | 100% benchmark | PASS |
| Reviewer artifact read | 100% | 100% | PASS |
| Priority direct evidence | 100% | 100% | PASS |
| Hard-gate failures | 0 | 0 | PASS |
| Snapshot correct | true | true | PASS |
| Inventory mismatches | 0 | 0 | PASS |
| Schema field loss | 0 | 0 | PASS |
| Reported tokens | 1,800,000 | <=3,000,000 | PASS |
| Tool calls | 88 | <=120 | PASS |
| Elapsed | 720s | <=1,200s | PASS |
| Duplicate scan ratio | 11.1111% | <=20% | PASS |
| Repair | 1 attempt / 90% injected success | >=90% | PASS |
| Primary / source integrity | present / unchanged | required | PASS |
| Fixed-fixture inventory | 228 models / 52 macros / 42 tests / 5 seeds / 2 snapshots / 15 pipeline YAML | exact oracle | PASS |

五个离线栈 `fabric-dbt`、`python-fastapi`、`typescript-react`、`go-service`、`java-spring` 的 V2 candidate 全部通过；legacy candidate 均按 characterization 失败。绝对生产路径和 provider transcript 被 suite loader 拒绝。

| 规模场景 | 本机证据 | 结果 |
|---|---|---|
| 50k file preflight | pytest call 0.04s；正文读取被 monkeypatch 明确禁止；内部上限 5s | PASS |
| 1M EvidenceRef | 测试 call 6.67s；真实 SQLite schema、首尾 keyset page 各 200 条；内部 insert <60s、page <2s 断言 | PASS |
| 100 findings | 测试 call 0.13s；200 candidates fingerprint 去重为 100；25 条 cursor page、响应 <128 KiB | PASS |
| 8 MiB artifact | exact hash、Range read、ETag 与 bounded prompt 路径通过 | PASS |

CI p95 历史比较属于具体 CI 环境的持续发布门禁；本地验收保存上述实测值并通过规范中的绝对防退化 rail。

## 4. 全局 hard-gate 验收

`test_complete_validator_set_passes_and_verifies_artifact` 对 QG-001..QG-016 恰好各执行一次并全部通过。缺 section、partial read、错误 citation/inventory/snapshot、空 findings、强制 evaluator ACCEPT、缺 primary、预算超限等 fault injection 均 fail closed。`test_gate_set_requires_all_sixteen_once` 拒绝缺失、重复或未知 gate。

| ID | 结果 | 验收证据 |
|---|---|---|
| QG-001 | PASS | source workspace 前后 integrity 一致，Agent mutation 注入被阻断。 |
| QG-002 | PASS | project root、snapshot kind/ref/fixed hash、scope/method 均冻结；错误 snapshot 注入失败。 |
| QG-003 | PASS | entry/models/macros/tests/seeds/snapshots/deployment 七域 7/7；删除章节失败并触发 repair。 |
| QG-004 | PASS | 每个 required domain 均有可解析 evidence；缺域 evidence fail closed。 |
| QG-005 | PASS | 三层跨组件 lineage 及节点 evidence 通过；缺关系注入失败。 |
| QG-006 | PASS | profiles/ADO/Notebook 等执行控制面有证据覆盖。 |
| QG-007 | PASS | P0/P1 claim direct-evidence ratio 100%；unsupported claim 失败。 |
| QG-008 | PASS | negative evidence 携 query/scope/exclusions/result hash/limitations。 |
| QG-009 | PASS | static/runtime、未执行与环境未知限制明确存在。 |
| QG-010 | PASS | citation 对 frozen snapshot 解析率 100%；错误行段注入失败。 |
| QG-011 | PASS | 228/52/42/5/2/15 inventory oracle 可回算；错误 model count 注入失败。 |
| QG-012 | PASS | 命名 Markdown primary 的 MIME/size/hash/sections/status 均符合 contract；缺 primary 失败。 |
| QG-013 | PASS | exact candidate hash 的 server-observed byte coverage 100%；40% read 注入失败。 |
| QG-014 | PASS | open blocking findings 为 0；Evaluator 强制 ACCEPT 不能绕过。 |
| QG-015 | PASS | contract/result/evaluation schema version 一致且 field loss=0；unknown/missing 拒绝。 |
| QG-016 | PASS | effective budget 的 mode/source/limits 与 ledger/runtime/UI 一致；hard overrun 不 completed。 |

## 5. 功能验收 AC-F

| ID | 结果 | 自动化证据 / 实现证据 |
|---|---|---|
| AC-F-001 | PASS | `test_test12_goal_compiles_complete_traceable_seven_domain_contract`：typed requirements、source span、prompt hash。 |
| AC-F-002 | PASS | `test_read_only_as_only_requirement_fails_semantic_lint` 与 contract linter。 |
| AC-F-003 | PASS | `test_explicit_current_checkout_freezes_dirty_overlay`；显式 current checkout 优先。 |
| AC-F-004 | PASS | `test_multiple_equal_repositories_require_target_selection`。 |
| AC-F-005 | PASS | `test_ref_move_after_commit_freeze_does_not_change_reads`。 |
| AC-F-006 | PASS | working-tree overlay 与 non-git directory pack 测试；均为 content-addressed immutable artifact。 |
| AC-F-007 | PASS | repo-analysis 并行 collector 与 focused-question 单 producer/独立 scorer 测试。 |
| AC-F-008 | PASS | `test_plan_proposal_cycle_and_authority_escalation_are_rejected`；仅 admitted acyclic proposal 冻结。 |
| AC-F-009 | PASS | `test_direct_bindings_do_not_implicitly_forward_ancestor_summaries`。 |
| AC-F-010 | PASS | immutable inventory、normalized query cache、4-collector singleflight。 |
| AC-F-011 | PASS | `test_typed_claim_evidence_negative_search_and_metric_ledger`。 |
| AC-F-012 | PASS | QG-001 workspace integrity + artifact store；read-only source 仍可产出 task-owned report。 |
| AC-F-013 | PASS | immutable artifact trigger、parent/child v2、source superseded 与 repair crash rollback。 |
| AC-F-014 | PASS | exact candidate receipt 只有 server-observed 100% byte union 才 fresh/pass。 |
| AC-F-015 | PASS | `test_evaluator_accept_cannot_override_open_blocking_finding`。 |
| AC-F-016 | PASS | fingerprint dedupe、同缺陷 delta、最多两次 repair、并发/幂等约束。 |
| AC-F-017 | PASS | deterministic workflow 原子发布 verified primary；task projection 将 primary 与 verdict 分列。 |
| AC-F-018 | PASS | Range/download/diff/export 测试；export 含 primary、hash、contract、snapshot、strategy、evidence、verdict。 |
| AC-F-019 | PASS | actual service startup hooks 保存/恢复 exact checkpoint；old producer attempt artifact 明确拒绝。 |
| AC-F-020 | PASS | exact active waiver 保留原 fail verdict；非 waivable gate 拒绝；GUI 永久显示 signed waiver。 |
| AC-F-021 | PASS | draft/analyze/publish/start 全程相同 task ID；原子 start exactly-once。 |
| AC-F-022 | PASS | freeze 后修改 live dirty file，read 仍返回 overlay 原 bytes/hash。 |
| AC-F-023 | PASS | 非 Strategy scorer 提交维度分被拒；total 由服务端维度求和为 99。 |
| AC-F-024 | PASS | section-scoped Markdown v2 后旧 receipt 不匹配；Reviewer 重新 bind 并产生 fresh 100% v2 receipt。 |

## 6. Schema 与 API 验收 AC-S

| ID | 结果 | 自动化证据 / 实现证据 |
|---|---|---|
| AC-S-001 | PASS | Python model、OpenAPI response model、API payload 与 generated TS round-trip/snapshot。 |
| AC-S-002 | PASS | legacy criteria exact unique mapping；missing/ambiguous fail closed。 |
| AC-S-003 | PASS | `remaining_risks` 精确映射 canonical `risks`，空数组保持空。 |
| AC-S-004 | PASS | completed result 缺 primary artifact 被 schema 拒绝；contract 要求恰好一个 primary。 |
| AC-S-005 | PASS | unknown/missing schema version 错误含 expected/observed。 |
| AC-S-006 | PASS | artifact Range、ETag、If-None-Match、416、download、diff。 |
| AC-S-007 | PASS | draft PUT 要求 If-Match；stale ETag 不覆盖。 |
| AC-S-008 | PASS | Idempotency-Key 同 body replay、异 body conflict。 |
| AC-S-009 | PASS | task detail 四轴、resume checkpoint 与 effective budget source 完整。 |
| AC-S-010 | PASS | append-stable、stream/scope-bound cursor；百万 evidence 首尾页。 |
| AC-S-011 | PASS | model identity/read/total 字段拒绝；非 scorer 维度分拒绝。 |
| AC-S-012 | PASS | Python/OpenAPI/generated TS 四轴 enum、event 与 transition snapshot 完全一致。 |
| AC-S-013 | PASS | completed/partial/failed 合法样本通过；混合字段和 partial+primary 均拒绝。 |

## 7. Budget 验收 AC-B

| ID | 结果 | 自动化证据 / 实现证据 |
|---|---|---|
| AC-B-001 | PASS | Manager 生产构造显式 `enforce_runtime_budgets=True`；V2 strategy 默认 hard profile。 |
| AC-B-002 | PASS | 8 并发 reservation 中仅 5×20 可进入 100 root ledger，无 oversell。 |
| AC-B-003 | PASS | provider usage overrun 原子记账并转 `exhausted + needs_attention + budget_exhausted`。 |
| AC-B-004 | PASS | Codex N+1 dynamic tool request 在 callback dispatch 前拒绝；无第二个 response。 |
| AC-B-005 | PASS | soft overrun 为 `over_budget`，发布策略显式；不伪装 hard success。 |
| AC-B-006 | PASS | unlimited 的 limits/remaining 为 null，API/UI 明示 unlimited，ledger 保留 source profile/audit event。 |
| AC-B-007 | PASS | retry/ledger revision 保留累计 consumption；不因 attempt 清零。 |
| AC-B-008 | PASS | repair 有独立 purpose/allocation，但 consumed+reserved 仍受同一 root 200 上限。 |
| AC-B-009 | PASS | reserve crash 全回滚；exact replay 幂等；不同 usage/stale fence 拒绝。 |
| AC-B-010 | PASS | input/cached/output/reasoning/provider reported 分列；provider contract 决定 reported total。 |

预算扩展不修改 exhausted ledger：服务端取消并 fence 活跃 reservation，旧 ledger 变 immutable superseded，创建 N+1 revision 携带累计 consumption，再由服务端根据持久化 reason/checkpoint 选择 resume target。API body 只接受完整五维 limits 与 audit reason，actor/role/target 不接受客户端伪造。

## 8. 安全验收 AC-SEC

| ID | 结果 | 自动化证据 / 实现证据 |
|---|---|---|
| AC-SEC-001 | PASS | traversal、absolute、UNC、drive-relative、NUL 对 snapshot/artifact path 全拒绝。 |
| AC-SEC-002 | PASS | root 外 symlink realpath 检查拒绝。 |
| AC-SEC-003 | PASS | cross-task artifact/evidence namespace 拒绝，不返回目标 metadata。 |
| AC-SEC-004 | PASS | current-attempt subject check 拒绝旧 producer attempt artifact。 |
| AC-SEC-005 | PASS | finalize mismatch 与 final blob swap 均失败并追加 content-free security event。 |
| AC-SEC-006 | PASS | 模型不得提交 `read_complete/read_ranges`；无真实 100% receipt 时 QG-013 fail。 |
| AC-SEC-007 | PASS | contract/strategy 安全 ceiling 不可由 prompt/request 提权；V2 provider 仅暴露 role-bound MCP。 |
| AC-SEC-008 | PASS | secret-bearing draft/artifact denied；reasoning/activity/audit redaction 不回显原文。 |
| AC-SEC-009 | PASS | V2 Claude 为 MCP-only、无 WebFetch/WebSearch；Codex read-only sandbox `networkAccess=false`；resolver 不 fetch。 |
| AC-SEC-010 | PASS | HTML/SVG/PS1/EXE 仅 download，不 inline/execute。 |
| AC-SEC-011 | PASS | waiver 必须 exact subject/artifact/rubric/version、授权 actor；nonwaivable 始终拒绝。 |
| AC-SEC-012 | PASS | concurrent root reservation、repair fingerprint dedupe、fencing 与 max 2 attempts。 |
| AC-SEC-013 | PASS | read-only permissions 去除 shell/write tools；Windows sandbox setup 失败时 model token=0。 |
| AC-SEC-014 | PASS | Git hooks/fsmonitor/textconv/pager/credential helper/env injection 全禁用，`shell=False`。 |

## 9. UI 验收

| 场景 | 结果 | 证据 |
|---|---|---|
| Goal -> Contract Preview | PASS | Wizard 只需 objective 即分析，并绑定最终 task identity。 |
| Semantic incomplete | PASS | Publish disabled；alert 显示具体 `requirement_id` 与 conflict message。 |
| Target detail | PASS | 同屏显示 HEAD、default ref、ahead/behind、dirty、worktrees、recommendation/reason。 |
| Strategy detail | PASS | 三轴 assessment、rationale、adaptive DAG、effective policy provenance、budget mode/limits。 |
| Task status | PASS | V2 row 将 fail/waived 显示为 warning，而非普通绿色完成；四轴独立 badge。 |
| Primary overview | PASS | 首卡为 primary deliverable；verdict/score 独立。 |
| Deliverable viewer | PASS | bounded Range、完整滚动、hash/version、download、section diff；executable 不 inline。 |
| Evidence explorer | PASS | coverage/claims/files 分 tab，fixed snapshot path + line range 可见。 |
| Quality/waiver/repair | PASS | hard gates、findings、read coverage、repair action、signed waiver 永久显示。 |
| Budget | PASS | hard utilization 与 unlimited 独立；exhausted resume 要求五维新 limits + reason。 |
| Recovery actions | PASS | retry/load-more/resume/repair 等动作有明确错误与可执行恢复；V2 archived 不显示非法 restore。 |
| Accessibility | PASS | semantic section/list/table、button/label/tab/alert/aria-label；状态同时有文本，不依赖颜色；8 MiB 内容按 Range 加载。 |

## 10. Migration 与恢复验收

| 场景 | 结果 | 证据 |
|---|---|---|
| 0006 / 0010 / latest -> 0017 | PASS | 参数化 upgrade + double-open idempotency；FK check 为空。 |
| Legacy history | PASS | legacy task/Brief 保留，V2 active refs 全为 null，不伪造 primary artifact。 |
| Legacy in-flight / adapter | PASS | V2 路径仅由 active contract 选择；legacy 继续原 service path；显式 adapter result 带 version + compatibility warnings，静默丢字段失败。 |
| Immutable rows | PASS | published contract/snapshot/strategy、verified/final artifact/evaluation、superseded budget revision triggers。 |
| Crash injection | PASS | contract publish、snapshot manifest、artifact finalize、evaluation commit、budget reserve、repair publish、atomic start。 |
| Startup reconciliation | PASS | service startup hook 先保存 exact resume status，再 success/uncertain；old attempt 不喂 downstream。 |
| DB restore/blob scan | PASS | backup/restore 后 blob reference integrity scan 通过并报告 orphan，不静默删除。 |

所有 migration 均 additive；旧应用可忽略新表。回滚不删除 V2 artifact/ledger truth，也不把 exhausted 自动改 completed。

## 11. 性能验收 PERF-Q

| ID | 结果 | 证据 |
|---|---|---|
| PERF-Q-01 | PASS | 50k metadata-only preflight，正文读取禁止，本机 call 0.04s。 |
| PERF-Q-02 | PASS | 1M real EvidenceRef rows，keyset cursor 首尾分页，TaskDetail 不嵌入全量 evidence。 |
| PERF-Q-03 | PASS | 8 MiB artifact Range read；initial assignment 仅含 bindings/metadata，正文 on-demand。 |
| PERF-Q-04 | PASS | 4 collectors singleflight，仅一次底层 normalized query scan。 |
| PERF-Q-05 | PASS | 100 authoritative findings、fingerprint dedupe、有界 25-row cursor page。 |
| PERF-Q-06 | PASS | Test12 1.8M tokens / 88 tools / 720s / 11.1111% duplicate。 |
| PERF-Q-07 | PASS | identical compiler input hash 返回 `cache_hit=true`，无重复规则/模型调用。 |
| PERF-Q-08 | PASS | citation validator 只解析 frozen snapshot/hash；inventory/query/citation path 有 index/cache。 |

## 12. 状态机规范调和

`coworker/orchestration/quality/state_machine.py` 是唯一 transition source，并生成 OpenAPI/TypeScript snapshot。规范 §6.2 表未列 `running + attention_required -> needs_attention`，但规范预算条款、fault injection 和 AC-B-003 明确要求 provider 在 running turn 超 hard limit 后进入 `needs_attention`。实现加入这一条最小转移并持久化 `workflow_resume_status=running`；这是对同一规范内部冲突的显式调和，不是 waiver。其余未列转移全部拒绝并记录 `invalid_transition`。

## 13. Waiver、Skip 与遗留项

- 验收 waiver：**无**。
- 跳过测试：**无**。
- 测试 fixture 中的 waiver 仅用于验证 exact-scope 行为，不是对本次发布的豁免。
- 已知非阻断遗留：Starlette/httpx2 迁移提示；Vite chunk/code-split 提示；Windows autocrlf 提示。它们未造成测试、构建、schema、security 或质量门禁失败。
- 原 Bash glob 命令在 PowerShell 下的两次 FAIL 已在 §2 原样记录，并用显式文件列表完成等价全量执行。

## 14. 验收签结

Task Quality V2 的 contract、target/snapshot、adaptive strategy、typed evidence、immutable artifact、independent full review、authoritative adjudication、bounded repair、versioned root budget、canonical state machine、API、GUI、observability、offline benchmark、migration/recovery 和 orchestration-handoff skill 均有实现与自动化证据。当前工作树满足规范 Stage 2 opt-in 的本地发布门槛。
