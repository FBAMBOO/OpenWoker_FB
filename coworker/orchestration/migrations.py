"""Checksummed, forward-only SQLite migrations for orchestration.db."""

from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from .errors import MigrationError

_MIGRATION_NAME = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str


def load_migrations(path: Path | None = None) -> tuple[Migration, ...]:
    directory = path or Path(__file__).with_name("migrations")
    migrations: list[Migration] = []
    for source in sorted(directory.glob("*.sql")):
        match = _MIGRATION_NAME.match(source.name)
        if match is None:
            continue
        raw = source.read_bytes()
        if b"\r" in raw:
            raise MigrationError(
                f"migration {source.name} must use LF line endings; "
                "check .gitattributes and renormalize the checkout"
            )
        migrations.append(
            Migration(
                version=int(match.group(1)),
                name=match.group(2),
                sql=raw.decode("utf-8"),
                checksum=hashlib.sha256(raw).hexdigest(),
            )
        )
    if not migrations:
        raise MigrationError(f"no migrations found in {directory}")
    versions = [migration.version for migration in migrations]
    if versions != sorted(set(versions)):
        raise MigrationError("migration versions must be unique and increasing")
    return tuple(migrations)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _execute_sql_script(connection: sqlite3.Connection, sql: str) -> None:
    """Execute a migration script without letting ``executescript`` commit our lock.

    ``sqlite3.Connection.executescript`` issues an implicit COMMIT before running its
    input, which opens a race between reading the migration ledger and applying DDL.
    ``complete_statement`` understands quoted semicolons and trigger bodies, allowing
    us to execute complete statements under one caller-owned ``BEGIN IMMEDIATE``.
    """

    pending = ""
    for character in sql:
        pending += character
        if character == ";" and sqlite3.complete_statement(pending):
            connection.execute(pending)
            pending = ""
    if pending.strip():
        if not sqlite3.complete_statement(pending + ";"):
            raise sqlite3.OperationalError("incomplete SQL statement in migration")
        connection.execute(pending)


def _enable_wal_with_busy_retry(connection: sqlite3.Connection) -> None:
    """Enable WAL while another fresh process may initialize the same file."""

    # SQLite does not consistently invoke its busy handler while changing journal
    # mode (notably on Windows), so use the same bounded five-second startup budget.
    deadline = time.monotonic() + 5.0
    delay = 0.01
    while True:
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 0.25)


def apply_migrations(
    connection: sqlite3.Connection,
    migrations: tuple[Migration, ...] | None = None,
) -> tuple[int, ...]:
    """Apply missing migrations atomically and reject changed historical SQL."""

    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    _enable_wal_with_busy_retry(connection)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS orch_schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )
    connection.commit()
    chosen = tuple(migrations) if migrations is not None else load_migrations()
    if not chosen:
        raise MigrationError("no migrations available for this application bundle")
    chosen_versions = [migration.version for migration in chosen]
    if chosen_versions != sorted(set(chosen_versions)):
        raise MigrationError("migration versions must be unique and increasing")
    expected_versions = list(range(1, chosen_versions[-1] + 1))
    if chosen_versions != expected_versions:
        raise MigrationError("migration versions must be contiguous and start at 0001")

    applied_now: list[int] = []
    current: Migration | None = None
    try:
        # This lock is acquired before reading the ledger. A concurrent initializer
        # waits here, then observes the winner's committed prefix instead of replaying
        # CREATE TABLE statements from a stale pre-lock snapshot.
        connection.execute("BEGIN IMMEDIATE")
        applied_rows = {
            int(row[0]): (str(row[1]), str(row[2]))
            for row in connection.execute(
                "SELECT version, name, checksum FROM orch_schema_migrations"
            )
        }
        available_versions = set(chosen_versions)
        unknown_versions = sorted(set(applied_rows).difference(available_versions))
        if unknown_versions:
            database_version = max(applied_rows)
            bundle_version = chosen_versions[-1]
            if database_version > bundle_version:
                raise MigrationError(
                    f"database schema version {database_version:04d} is newer than "
                    f"this application bundle (latest {bundle_version:04d}); "
                    "downgrade is not supported"
                )
            formatted = ", ".join(
                f"{version:04d}" for version in unknown_versions
            )
            raise MigrationError(
                "database contains migration versions absent from this application "
                f"bundle: {formatted}"
            )
        applied_versions = sorted(applied_rows)
        if applied_versions != chosen_versions[: len(applied_versions)]:
            raise MigrationError(
                "database migration history is not an exact bundle prefix"
            )
        for migration in chosen:
            current = migration
            existing = applied_rows.get(migration.version)
            if existing is not None:
                if existing != (migration.name, migration.checksum):
                    raise MigrationError(
                        f"migration {migration.version:04d} checksum/name mismatch"
                    )
                continue
            _execute_sql_script(connection, migration.sql)
            connection.execute(
                """
                INSERT INTO orch_schema_migrations(
                    version, name, checksum, applied_at
                ) VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration.version, migration.name, migration.checksum),
            )
            applied_now.append(migration.version)
        connection.commit()
        return tuple(applied_now)
    except MigrationError:
        if connection.in_transaction:
            connection.rollback()
        raise
    except sqlite3.Error as exc:
        if connection.in_transaction:
            connection.rollback()
        label = (
            f"{current.version:04d}_{current.name}"
            if current is not None
            else "ledger_validation"
        )
        raise MigrationError(f"migration {label} failed: {exc}") from exc


def applied_migrations(connection: sqlite3.Connection) -> tuple[Migration, ...]:
    rows = connection.execute(
        "SELECT version, name, checksum FROM orch_schema_migrations ORDER BY version"
    ).fetchall()
    return tuple(Migration(int(row[0]), str(row[1]), "", str(row[2])) for row in rows)
