from __future__ import annotations

import hashlib
import sqlite3
import threading

import pytest

from coworker.orchestration.errors import MigrationError
from coworker.orchestration.migrations import (
    Migration,
    applied_migrations,
    apply_migrations,
    load_migrations,
)
from coworker.orchestration.store import OrchestrationStore


def _migration(version: int, name: str, sql: str) -> Migration:
    return Migration(
        version=version,
        name=name,
        sql=sql,
        checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
    )


def test_database_at_0001_upgrades_to_current_bundle(tmp_path) -> None:
    bundle = load_migrations()
    assert bundle[0].version == 1
    connection = sqlite3.connect(tmp_path / "from-0001.db")
    try:
        assert apply_migrations(connection, (bundle[0],)) == (1,)

        expected = tuple(migration.version for migration in bundle[1:])
        assert apply_migrations(connection, bundle) == expected
        assert tuple(item.version for item in applied_migrations(connection)) == tuple(
            migration.version for migration in bundle
        )
        assert apply_migrations(connection, bundle) == ()
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("orch_evidence_blob_lookup",),
        ).fetchone() == ("orch_evidence_blob_lookup",)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("orch_scheduler_leader",),
        ).fetchone() == ("orch_scheduler_leader",)
        assert "dead_lettered_at" in {
            row[1]
            for row in connection.execute("PRAGMA table_info(orch_outbox)").fetchall()
        }
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("orch_outbox_requeue_history",),
        ).fetchone() == ("orch_outbox_requeue_history",)
        assert {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger' AND tbl_name = 'orch_outbox_requeue_history'
                """
            ).fetchall()
        } == {
            "orch_outbox_requeue_history_no_update",
            "orch_outbox_requeue_history_no_delete",
        }
        gate_schema = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'orch_gates'"
        ).fetchone()[0]
        assert "'preparing'" in gate_schema
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("orch_run_activity",),
        ).fetchone() == ("orch_run_activity",)
        assert {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger' AND tbl_name = 'orch_run_activity'
                """
            ).fetchall()
        } == {"orch_run_activity_no_update", "orch_run_activity_no_delete"}
    finally:
        connection.close()


def test_0006_preserves_historical_gates_and_marks_them_published(tmp_path) -> None:
    bundle = load_migrations()
    connection = sqlite3.connect(tmp_path / "from-0005-with-gates.db")
    try:
        assert apply_migrations(connection, bundle[:5]) == (1, 2, 3, 4, 5)
        now = "2026-08-03T00:00:00.000000Z"
        connection.execute(
            """
            INSERT INTO orch_tasks(
                id, idempotency_key, creation_hash, title, objective, domain,
                risk_tier, status, current_stage, created_at, updated_at
            ) VALUES ('task-history', 'history', 'hash', 'History', 'Preserve gates',
                      'knowledge', 'low', 'waiting_human', 'planning', ?, ?)
            """,
            (now, now),
        )
        for index, status in enumerate(("open", "approved", "canceled"), start=1):
            resolved = None if status == "open" else now
            connection.execute(
                """
                INSERT INTO orch_gates(
                    id, task_id, kind, status, source_key, prompt_json,
                    resolution_json, resolved_by, opened_at, resolved_at
                ) VALUES (?, 'task-history', 'plan_approval', ?, ?, '{}', ?, ?, ?, ?)
                """,
                (
                    f"gate-{index}",
                    status,
                    f"history-{status}",
                    None if status == "open" else '{"decision":"approve"}',
                    None if status == "open" else "owner",
                    now,
                    resolved,
                ),
            )
        connection.commit()

        assert apply_migrations(connection, bundle) == tuple(
            migration.version for migration in bundle[5:]
        )
        rows = connection.execute(
            """
            SELECT id, status, opened_at, published_at
            FROM orch_gates ORDER BY id
            """
        ).fetchall()
        assert rows == [
            ("gate-1", "open", now, now),
            ("gate-2", "approved", now, now),
            ("gate-3", "canceled", now, now),
        ]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
            ("orch_gates_task_status",),
        ).fetchone() == ("orch_gates_task_status",)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
            ("orch_gates_task_publication",),
        ).fetchone() == ("orch_gates_task_publication",)
    finally:
        connection.close()


def test_0006_store_upgrade_backfills_briefs_and_parent_relations_idempotently(
    tmp_path,
) -> None:
    database = tmp_path / "from-0006-with-task-tree.db"
    bundle = load_migrations()
    connection = sqlite3.connect(database)
    try:
        assert apply_migrations(connection, bundle[:6]) == (1, 2, 3, 4, 5, 6)
        now = "2026-08-17T00:00:00.000000Z"
        connection.execute(
            """
            INSERT INTO orch_tasks(
                id, idempotency_key, creation_hash, title, objective, domain,
                risk_tier, status, current_stage, created_at, updated_at
            ) VALUES ('legacy-parent', 'legacy-parent-key', 'parent-hash',
                      'Legacy parent', 'Coordinate legacy work', 'knowledge',
                      'low', 'draft', 'intake', ?, ?)
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO orch_tasks(
                id, idempotency_key, creation_hash, title, objective, domain,
                risk_tier, status, current_stage, parent_task_id,
                created_at, updated_at
            ) VALUES ('legacy-child', 'legacy-child-key', 'child-hash',
                      'Legacy child', 'Perform legacy work', 'knowledge',
                      'low', 'draft', 'intake', 'legacy-parent', ?, ?)
            """,
            (now, now),
        )
        connection.commit()
    finally:
        connection.close()

    first = OrchestrationStore(database)
    try:
        assert first.backfill_legacy_briefs() == 0
        diagnostic = first.connect()
        try:
            assert diagnostic.execute(
                "SELECT COUNT(*) FROM orch_task_briefs"
            ).fetchone()[0] == 2
            assert diagnostic.execute(
                "SELECT COUNT(*) FROM orch_task_relations WHERE relation_type='parent'"
            ).fetchone()[0] == 1
            assert diagnostic.execute(
                "SELECT COUNT(*) FROM orch_events WHERE event_type='relation_added'"
            ).fetchone()[0] == 1
            assert diagnostic.execute("PRAGMA foreign_key_check").fetchall() == []
        finally:
            diagnostic.close()
    finally:
        first.close()

    reopened = OrchestrationStore(database)
    try:
        diagnostic = reopened.connect()
        try:
            assert diagnostic.execute(
                "SELECT COUNT(*) FROM orch_task_briefs"
            ).fetchone()[0] == 2
            assert diagnostic.execute(
                "SELECT COUNT(*) FROM orch_task_relations WHERE relation_type='parent'"
            ).fetchone()[0] == 1
            assert tuple(
                row[0]
                for row in diagnostic.execute(
                    "SELECT version FROM orch_schema_migrations ORDER BY version"
                )
            ) == tuple(migration.version for migration in bundle)
        finally:
            diagnostic.close()
    finally:
        reopened.close()


@pytest.mark.parametrize("starting_version", [6, 10, 17])
def test_v2_upgrade_from_supported_versions_and_double_open_is_idempotent(
    tmp_path, starting_version: int
) -> None:
    bundle = load_migrations()
    assert bundle[-1].version == 17
    database = tmp_path / f"from-{starting_version:04d}.db"
    connection = sqlite3.connect(database)
    try:
        assert apply_migrations(connection, bundle[:starting_version]) == tuple(
            range(1, starting_version + 1)
        )
        now = "2026-08-19T00:00:00.000000Z"
        connection.execute(
            """
            INSERT INTO orch_tasks(
                id, idempotency_key, creation_hash, title, objective, domain,
                risk_tier, status, current_stage, created_at, updated_at
            ) VALUES (?, ?, 'legacy-hash', 'Legacy task', 'Keep legacy truth',
                      'knowledge', 'low', 'draft', 'intake', ?, ?)
            """,
            (
                f"legacy-from-{starting_version}",
                f"legacy-from-{starting_version}",
                now,
                now,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    for _open_number in range(2):
        store = OrchestrationStore(database)
        try:
            diagnostic = store.connect()
            try:
                assert diagnostic.execute("PRAGMA foreign_keys").fetchone()[0] == 1
                assert diagnostic.execute("PRAGMA foreign_key_check").fetchall() == []
                assert tuple(
                    row[0]
                    for row in diagnostic.execute(
                        "SELECT version FROM orch_schema_migrations ORDER BY version"
                    )
                ) == tuple(range(1, 18))
                for table in (
                    "orch_quality_contracts",
                    "orch_artifact_versions",
                    "orch_quality_evaluations",
                    "orch_repository_snapshots",
                    "orch_evidence_refs",
                    "orch_execution_strategies",
                    "orch_budget_ledgers",
                ):
                    found = diagnostic.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (table,),
                    ).fetchone()
                    assert found is not None and found[0] == table
                legacy = diagnostic.execute(
                    """
                    SELECT active_contract_id, active_snapshot_id, active_strategy_id,
                           primary_artifact_id, active_budget_ledger_id
                    FROM orch_tasks WHERE id=?
                    """,
                    (f"legacy-from-{starting_version}",),
                ).fetchone()
                assert tuple(legacy) == (None, None, None, None, None)
                assert diagnostic.execute(
                    "SELECT COUNT(*) FROM orch_task_briefs WHERE task_id=?",
                    (f"legacy-from-{starting_version}",),
                ).fetchone()[0] == 1
            finally:
                diagnostic.close()
        finally:
            store.close()


def test_bundle_with_migration_hole_is_rejected_before_schema_changes(tmp_path) -> None:
    first = _migration(1, "initial", "CREATE TABLE first_table (id INTEGER);")
    third = _migration(3, "skipped_second", "CREATE TABLE third_table (id INTEGER);")
    connection = sqlite3.connect(tmp_path / "hole.db")
    try:
        with pytest.raises(MigrationError, match="contiguous and start at 0001"):
            apply_migrations(connection, (first, third))
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'first_table'"
        ).fetchone() is None
        assert connection.execute(
            "SELECT COUNT(*) FROM orch_schema_migrations"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_non_prefix_database_history_is_rejected_without_repairing_it(tmp_path) -> None:
    first = _migration(1, "initial", "CREATE TABLE first_table (id INTEGER);")
    second = _migration(2, "second", "CREATE TABLE second_table (id INTEGER);")
    connection = sqlite3.connect(tmp_path / "non-prefix.db")
    try:
        assert apply_migrations(connection, (first,)) == (1,)
        connection.execute("DELETE FROM orch_schema_migrations WHERE version = 1")
        connection.execute(
            """
            INSERT INTO orch_schema_migrations(version, name, checksum, applied_at)
            VALUES (?, ?, ?, ?)
            """,
            (2, second.name, second.checksum, "2026-08-03T00:00:00.000Z"),
        )
        connection.commit()

        with pytest.raises(MigrationError, match="not an exact bundle prefix"):
            apply_migrations(connection, (first, second))

        assert connection.execute(
            "SELECT version FROM orch_schema_migrations ORDER BY version"
        ).fetchall() == [(2,)]
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'second_table'"
        ).fetchone() is None
    finally:
        connection.close()


def test_two_connections_apply_the_same_bundle_once_under_lock(tmp_path) -> None:
    database = tmp_path / "concurrent.db"
    bundle = (
        _migration(1, "initial", "CREATE TABLE stable (id INTEGER PRIMARY KEY);"),
        _migration(2, "second", "ALTER TABLE stable ADD COLUMN value TEXT;"),
    )
    barrier = threading.Barrier(2)
    results: list[tuple[int, ...]] = []
    errors: list[BaseException] = []

    def migrate() -> None:
        connection = sqlite3.connect(database, timeout=10)
        try:
            barrier.wait(timeout=5)
            results.append(apply_migrations(connection, bundle))
        except BaseException as exc:
            errors.append(exc)
        finally:
            connection.close()

    threads = [threading.Thread(target=migrate) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert sorted(results) == [(), (1, 2)]
    connection = sqlite3.connect(database)
    try:
        assert connection.execute(
            "SELECT version FROM orch_schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,)]
        assert {
            row[1] for row in connection.execute("PRAGMA table_info(stable)").fetchall()
        } == {"id", "value"}
    finally:
        connection.close()


def test_database_newer_than_bundle_fails_closed(tmp_path) -> None:
    bundle = load_migrations()
    future_version = bundle[-1].version + 1
    connection = sqlite3.connect(tmp_path / "future.db")
    try:
        assert apply_migrations(connection, bundle) == tuple(
            migration.version for migration in bundle
        )
        connection.execute(
            """
            INSERT INTO orch_schema_migrations(version, name, checksum, applied_at)
            VALUES (?, ?, ?, ?)
            """,
            (future_version, "future", "f" * 64, "2026-08-03T00:00:00.000Z"),
        )
        connection.commit()
        ledger_before = connection.execute(
            "SELECT version, name, checksum FROM orch_schema_migrations ORDER BY version"
        ).fetchall()

        with pytest.raises(
            MigrationError,
            match=rf"version {future_version:04d} is newer.*downgrade",
        ):
            apply_migrations(connection, bundle)

        assert connection.execute(
            "SELECT version, name, checksum FROM orch_schema_migrations ORDER BY version"
        ).fetchall() == ledger_before
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'orch_tasks'"
        ).fetchone() == ("orch_tasks",)
    finally:
        connection.close()


def test_failed_migration_rolls_back_schema_and_ledger(tmp_path) -> None:
    initial = _migration(1, "initial", "CREATE TABLE stable (id INTEGER PRIMARY KEY);")
    broken = _migration(
        2,
        "broken",
        """
        CREATE TABLE must_rollback (id INTEGER PRIMARY KEY);
        INSERT INTO must_rollback(id) VALUES (1);
        THIS IS NOT VALID SQL;
        """,
    )
    repaired = _migration(
        2,
        "broken",
        "CREATE TABLE recovered (id INTEGER PRIMARY KEY);",
    )
    connection = sqlite3.connect(tmp_path / "rollback.db")
    try:
        assert apply_migrations(connection, (initial,)) == (1,)

        with pytest.raises(MigrationError, match=r"migration 0002_broken failed"):
            apply_migrations(connection, (initial, broken))

        assert not connection.in_transaction
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'must_rollback'"
        ).fetchone() is None
        assert connection.execute(
            "SELECT version FROM orch_schema_migrations ORDER BY version"
        ).fetchall() == [(1,)]

        assert apply_migrations(connection, (initial, repaired)) == (2,)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'recovered'"
        ).fetchone() == ("recovered",)
    finally:
        connection.close()
