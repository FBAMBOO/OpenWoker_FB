from __future__ import annotations

import sqlite3

from coworker.conversations import ConversationStore
from coworker.sessions import SessionRecord


def test_subscription_runtime_state_round_trips(tmp_path):
    store = ConversationStore(tmp_path)
    state = {
        "schema_version": 1,
        "runtime_id": "codex-subscription:gpt-5.6-sol@max",
        "external_session_id": "thread_123",
        "generation": 2,
    }
    store.save(
        SessionRecord(
            session_id="subscription-session",
            workspace=str(tmp_path),
            model=state["runtime_id"],
            mode="interactive",
            messages=[{"role": "user", "content": "hello"}],
            runtime_state=state,
        )
    )

    loaded = store.load("subscription-session")
    assert loaded is not None
    assert loaded.runtime_state == state


def test_legacy_database_migrates_runtime_state_column(tmp_path):
    db_path = tmp_path / "coworker.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE sessions ("
        "session_id TEXT PRIMARY KEY, workspace TEXT, model TEXT, mode TEXT, "
        "messages TEXT, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.execute(
        "INSERT INTO sessions(session_id, workspace, model, mode, messages) "
        "VALUES ('legacy', '', 'gpt-5.5', 'interactive', '[]')"
    )
    conn.commit()
    conn.close()

    store = ConversationStore(tmp_path)
    loaded = store.load("legacy")
    assert loaded is not None
    assert loaded.runtime_state == {}
    assert "runtime_state" in {
        row[1]
        for row in store._conn.execute("PRAGMA table_info(sessions)").fetchall()
    }
