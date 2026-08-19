from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.errors import ConflictError
from coworker.orchestration.models import TaskSpec
from coworker.orchestration.quality.artifacts import ArtifactService
from coworker.orchestration.quality.budgets import BudgetExceeded, BudgetService, ProviderUsage
from coworker.orchestration.quality.contract_compiler import ContractCompiler
from coworker.orchestration.quality.contracts import ContractRepository
from coworker.orchestration.quality.models import BudgetLimits, BudgetMode, BudgetProfile
from coworker.orchestration.quality.repository_resolver import RepositoryResolver
from coworker.orchestration.quality.repository_snapshot import RepositorySnapshotService
from coworker.orchestration.quality.strategy_selector import StrategySelector
from coworker.orchestration.quality.state_machine import WorkflowEvent, apply_workflow_event
from coworker.orchestration.store import OrchestrationStore


PROMPT = (
    "Read-only analyze the Fabric/dbt project entry, models, macros, tests, seeds, snapshots, "
    "deployment and relationships. Produce a Markdown report with file evidence."
)


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True,
        encoding="utf-8",
    ).stdout.strip()


def _repo(root: Path) -> None:
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.email", "quality@example.test")
    _git(root, "config", "user.name", "Quality Test")
    (root / "dbt_project.yml").write_text("name: fixture\n", encoding="utf-8")
    (root / "models").mkdir()
    (root / "models" / "a.sql").write_text("select 1\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    _git(root, "update-ref", "refs/remotes/origin/main", "HEAD")


def _profile(mode: str = "hard", *, tokens: int = 100) -> BudgetProfile:
    return BudgetProfile(
        id=f"test-{mode}@1",
        mode=mode,
        limits=(
            BudgetLimits()
            if mode == "unlimited"
            else BudgetLimits(
                model_calls=100, tool_calls=100, reported_tokens=tokens,
                active_seconds=1_000, tool_payload_bytes=1_000_000,
            )
        ),
    )


@pytest.fixture
def budget_context(tmp_path):
    repo = tmp_path / "repo"
    _repo(repo)
    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(store, ContentAddressedBlobStore(tmp_path / "state" / "blobs"))
    snapshots = RepositorySnapshotService(store, artifacts)
    task = store.create_task(TaskSpec(idempotency_key="budget", objective=PROMPT))
    contracts = ContractRepository(store)
    draft = ContractCompiler().compile(task_id=task.id, objective=PROMPT).contract
    contracts.save_draft(draft)
    contract = contracts.publish(draft.id, if_match=draft.content_hash)
    snapshot = snapshots.freeze(
        task_id=task.id, resolution=RepositoryResolver().resolve(repo, objective=PROMPT)
    )
    selector = StrategySelector(store)
    strategy = selector.select(contract=contract, snapshot=snapshot)
    selector.publish(strategy)
    apply_workflow_event(
        store, task_id=task.id, event=WorkflowEvent.START_REQUESTED
    )
    try:
        yield store, task, strategy
    finally:
        store.close()


def test_eight_concurrent_reservations_cannot_oversell_root(budget_context) -> None:
    store, _, strategy = budget_context
    budgets = BudgetService(store)
    ledger = budgets.create(
        task_id=strategy.task_id, strategy_id=strategy.id, profile=_profile(tokens=100)
    )

    def reserve(index: int) -> bool:
        try:
            budgets.reserve(
                ledger.id,
                amounts={"reported_tokens": 20},
                purpose=f"collector-{index}",
                reservation_id=f"reservation-{index}",
            )
            return True
        except BudgetExceeded:
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(reserve, range(8)))
    assert sum(outcomes) == 5
    observed = budgets.get(ledger.id)
    assert observed.reserved["reported_tokens"] == 100
    assert observed.reserved["reported_tokens"] <= observed.effective_limits.reported_tokens
    with store._read() as connection:
        active = connection.execute(
            "SELECT COUNT(*) AS count FROM orch_budget_reservations WHERE ledger_id=?",
            (ledger.id,),
        ).fetchone()["count"]
    assert active == 5


def test_repair_allocation_is_independent_but_still_charged_to_root(
    budget_context,
) -> None:
    store, task, strategy = budget_context
    budgets = BudgetService(store)
    ledger = budgets.create(
        task_id=task.id,
        strategy_id=strategy.id,
        profile=_profile(tokens=200),
    )
    regular, regular_fence = budgets.reserve(
        ledger.id,
        amounts={"reported_tokens": 80},
        purpose="strategy-node:synthesize:attempt:1",
    )
    budgets.consume(
        regular,
        fencing_token=regular_fence,
        usage=ProviderUsage(provider_reported_tokens=80),
    )
    repair, _repair_fence = budgets.reserve(
        ledger.id,
        amounts={"reported_tokens": 120},
        purpose="repair:1",
    )
    observed = budgets.get(ledger.id)
    assert observed.consumed["reported_tokens"] == 80
    assert observed.reserved["reported_tokens"] == 120
    with pytest.raises(BudgetExceeded):
        budgets.reserve(
            ledger.id,
            amounts={"reported_tokens": 1},
            purpose="repair:overflow",
        )
    with store._read() as connection:
        purpose = connection.execute(
            "SELECT purpose FROM orch_budget_reservations WHERE id=?",
            (repair,),
        ).fetchone()["purpose"]
    assert purpose == "repair:1"


def test_reservation_crash_rolls_back_ledger_event_and_retries_exactly_once(
    budget_context,
) -> None:
    store, task, strategy = budget_context
    budgets = BudgetService(store)
    ledger = budgets.create(
        task_id=task.id, strategy_id=strategy.id, profile=_profile(tokens=100)
    )
    with store._write() as connection:
        connection.execute(
            """
            CREATE TRIGGER inject_budget_reserve_crash
            AFTER INSERT ON orch_budget_reservations
            BEGIN SELECT RAISE(ABORT, 'injected budget reserve crash'); END
            """
        )
    with pytest.raises(Exception, match="injected budget reserve crash"):
        budgets.reserve(
            ledger.id,
            amounts={"reported_tokens": 40},
            purpose="crash-safe",
            reservation_id="reservation-crash-safe",
        )
    observed = budgets.get(ledger.id)
    assert observed.reserved["reported_tokens"] == 0
    assert observed.fencing_token == 0
    with store._read() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM orch_budget_reservations WHERE ledger_id=?",
            (ledger.id,),
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM orch_budget_events WHERE ledger_id=? AND event_type='reserved'",
            (ledger.id,),
        ).fetchone()[0] == 0
    with store._write() as connection:
        connection.execute("DROP TRIGGER inject_budget_reserve_crash")
    reservation_id, fence = budgets.reserve(
        ledger.id,
        amounts={"reported_tokens": 40},
        purpose="crash-safe",
        reservation_id="reservation-crash-safe",
    )
    assert reservation_id == "reservation-crash-safe"
    assert fence == 1
    replayed_id, replayed_fence = budgets.reserve(
        ledger.id,
        amounts={"reported_tokens": 40},
        purpose="crash-safe",
        reservation_id="reservation-crash-safe",
    )
    assert (replayed_id, replayed_fence) == (reservation_id, fence)
    assert budgets.get(ledger.id).reserved["reported_tokens"] == 40


def test_provider_usage_thresholds_and_hard_overrun_stop_task(budget_context) -> None:
    store, task, strategy = budget_context
    budgets = BudgetService(store)
    ledger = budgets.create(
        task_id=task.id, strategy_id=strategy.id, profile=_profile(tokens=100),
        provider_usage_semantics={"reported_tokens": "provider_total"},
    )
    reservation, fence = budgets.reserve(
        ledger.id, amounts={"reported_tokens": 100}, purpose="synthesize"
    )
    after = budgets.consume(
        reservation,
        fencing_token=fence,
        usage=ProviderUsage(
            model_calls=1, input_tokens=50, cached_input_tokens=20,
            output_tokens=20, reasoning_tokens=11,
            provider_reported_tokens=81,
            provider_reported_includes_cached=True,
        ),
    )
    assert after.consumed["reported_tokens"] == 81
    with store._read() as connection:
        thresholds = connection.execute(
            "SELECT COUNT(*) AS count FROM orch_budget_events WHERE ledger_id=? AND event_type='threshold'",
            (ledger.id,),
        ).fetchone()["count"]
        status = connection.execute(
            "SELECT budget_status FROM orch_tasks WHERE id=?", (task.id,)
        ).fetchone()["budget_status"]
    assert thresholds == 1
    assert status == "warning"

    reservation2, fence2 = budgets.reserve(
        ledger.id, amounts={"reported_tokens": 19}, purpose="review"
    )
    exhausted = budgets.consume(
        reservation2,
        fencing_token=fence2,
        usage=ProviderUsage(input_tokens=10, output_tokens=10, provider_reported_tokens=20),
    )
    assert exhausted.over_budget is True
    with store._read() as connection:
        task_row = connection.execute(
            "SELECT budget_status, workflow_status, quality_reason_code FROM orch_tasks WHERE id=?",
            (task.id,),
        ).fetchone()
    assert dict(task_row) == {
        "budget_status": "exhausted",
        "workflow_status": "needs_attention",
        "quality_reason_code": "budget_exhausted",
    }


def test_cached_and_reasoning_tokens_remain_separately_auditable(budget_context) -> None:
    store, task, strategy = budget_context
    budgets = BudgetService(store)
    ledger = budgets.create(task_id=task.id, strategy_id=strategy.id, profile=_profile(tokens=1_000))
    reservation, fence = budgets.reserve(
        ledger.id, amounts={"reported_tokens": 500}, purpose="collector"
    )
    budgets.consume(
        reservation,
        fencing_token=fence,
        usage=ProviderUsage(
            input_tokens=300, cached_input_tokens=240, output_tokens=60,
            reasoning_tokens=40, provider_reported_tokens=400,
            provider_reported_includes_cached=True,
        ),
    )
    assert budgets.usage_breakdown(ledger.id) == {
        "input_tokens": 300,
        "cached_input_tokens": 240,
        "output_tokens": 60,
        "reasoning_tokens": 40,
        "provider_reported_tokens": 400,
        "provider_reported_includes_cached": True,
    }


def test_soft_overrun_and_unlimited_are_explicit_not_fake_limits(budget_context) -> None:
    store, task, strategy = budget_context
    soft = BudgetService(store)
    soft_ledger = soft.create(task_id=task.id, strategy_id=strategy.id, profile=_profile("soft", tokens=10))
    reservation, fence = soft.reserve(
        soft_ledger.id, amounts={"reported_tokens": 20}, purpose="atomic-turn"
    )
    result = soft.consume(
        reservation, fencing_token=fence,
        usage=ProviderUsage(provider_reported_tokens=20),
    )
    assert result.over_budget is True
    with store._read() as connection:
        assert connection.execute(
            "SELECT budget_status FROM orch_tasks WHERE id=?", (task.id,)
        ).fetchone()["budget_status"] == "over_budget"

    # A separate task is needed because active ledgers are immutable budget history.
    other = store.create_task(TaskSpec(idempotency_key="unlimited", objective=PROMPT))
    # The production service requires task/strategy ownership; clone a minimal published strategy row.
    with store._write() as connection:
        source = connection.execute(
            "SELECT * FROM orch_execution_strategies WHERE id=?", (strategy.id,)
        ).fetchone()
        clone_id = "strategy_unlimited"
        columns = [item[1] for item in connection.execute("PRAGMA table_info(orch_execution_strategies)")]
        values = [source[name] for name in columns]
        values[columns.index("id")] = clone_id
        values[columns.index("task_id")] = other.id
        values[columns.index("version")] = 1
        values[columns.index("content_hash")] = "sha256:" + "f" * 64
        connection.execute(
            f"INSERT INTO orch_execution_strategies({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            values,
        )
    unlimited = BudgetService(store).create(
        task_id=other.id, strategy_id="strategy_unlimited", profile=_profile("unlimited")
    )
    assert unlimited.mode is BudgetMode.UNLIMITED
    assert all(value is None for value in unlimited.remaining.values())
    assert unlimited.effective_limits.model_dump() == {
        "model_calls": None, "tool_calls": None, "reported_tokens": None,
        "active_seconds": None, "tool_payload_bytes": None,
    }


def test_stale_fencing_token_cannot_double_consume(budget_context) -> None:
    store, task, strategy = budget_context
    budgets = BudgetService(store)
    ledger = budgets.create(task_id=task.id, strategy_id=strategy.id, profile=_profile(tokens=100))
    reservation, fence = budgets.reserve(
        ledger.id, amounts={"reported_tokens": 50}, purpose="run"
    )
    budgets.consume(
        reservation, fencing_token=fence, usage=ProviderUsage(provider_reported_tokens=25)
    )
    # Exact replay is idempotent, but a different accounting replay is rejected.
    with pytest.raises(ConflictError, match="different usage"):
        budgets.consume(
            reservation, fencing_token=fence, usage=ProviderUsage(provider_reported_tokens=26)
        )


def test_budget_extension_creates_immutable_revision_and_preserves_usage(
    budget_context,
) -> None:
    store, task, strategy = budget_context
    budgets = BudgetService(store)
    ledger = budgets.create(
        task_id=task.id, strategy_id=strategy.id, profile=_profile(tokens=100)
    )
    consumed_reservation, consumed_fence = budgets.reserve(
        ledger.id, amounts={"reported_tokens": 40}, purpose="completed-turn"
    )
    budgets.consume(
        consumed_reservation,
        fencing_token=consumed_fence,
        usage=ProviderUsage(provider_reported_tokens=40),
    )
    active_reservation, _active_fence = budgets.reserve(
        ledger.id, amounts={"reported_tokens": 20}, purpose="stale-turn"
    )
    with pytest.raises(BudgetExceeded):
        budgets.reserve(
            ledger.id, amounts={"reported_tokens": 50}, purpose="exhaust-root"
        )

    limits = ledger.effective_limits.model_dump(mode="json")
    limits["reported_tokens"] = 200
    revision = budgets.extend(
        ledger.id,
        effective_limits=limits,
        actor_id="local-user",
        reason="approved continuation",
    )
    assert revision.id != ledger.id
    assert revision.version == 2
    assert revision.consumed["reported_tokens"] == 40
    assert revision.reserved["reported_tokens"] == 0
    assert budgets.usage_breakdown(revision.id)["provider_reported_tokens"] == 40

    with store._read() as connection:
        old = connection.execute(
            """
            SELECT status, effective_limits_json, consumed_json, reserved_json
            FROM orch_budget_ledgers WHERE id=?
            """,
            (ledger.id,),
        ).fetchone()
        current = connection.execute(
            """
            SELECT active_budget_ledger_id, budget_status
            FROM orch_tasks WHERE id=?
            """,
            (task.id,),
        ).fetchone()
        reservation = connection.execute(
            "SELECT status FROM orch_budget_reservations WHERE id=?",
            (active_reservation,),
        ).fetchone()
    assert old["status"] == "superseded"
    assert json.loads(old["effective_limits_json"])["reported_tokens"] == 100
    assert json.loads(old["consumed_json"])["reported_tokens"] == 40
    assert json.loads(old["reserved_json"])["reported_tokens"] == 20
    assert dict(current) == {
        "active_budget_ledger_id": revision.id,
        "budget_status": "within_budget",
    }
    assert reservation["status"] == "canceled"

    with store._write() as connection, pytest.raises(Exception, match="immutable"):
        connection.execute(
            "UPDATE orch_budget_ledgers SET effective_limits_json='{}' WHERE id=?",
            (ledger.id,),
        )
