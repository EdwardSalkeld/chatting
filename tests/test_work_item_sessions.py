"""Work item identity survives transport changes and upgrades."""

from dataclasses import replace
import hashlib
import json
import sqlite3

import pytest

from app.state import SQLiteStateStore
from app.worker.executor.app_server import AppServerExecutor
from app.worker.executor import app_server
from app.worker.executor.app_server_session import Session
from tests.test_worker_inbox import _telegram_task


def test_transport_cannot_override_item_session(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    first = _telegram_task(1)
    other_transport = replace(
        _telegram_task(2, target="different-address"), work_item_id=first.work_item_id
    )
    for task in (first, other_transport):
        store.stage_inbox_task(task)
        assert (
            store.get_inbox_task(task_id=task.task_id).conversation_id
            == first.work_item_id
        )
    store.set_conversation_executor(
        conversation_id=first.work_item_id, backend="app-server"
    )
    assert store.claim_next_inbox_task().task_message.task_id == first.task_id
    assert store.claim_next_inbox_task() is None
    followups = store.claim_conversation_followups(parent_task_id=first.task_id)
    assert [task.task_message.task_id for task in followups] == [
        other_transport.task_id
    ]


@pytest.mark.parametrize(
    "source", ["im", "webhook", "email", "cron", "reminder", "internal"]
)
@pytest.mark.parametrize("existing_backend", [None, "exec", "app-server"])
def test_new_items_use_app_server_without_changing_existing_items(
    tmp_path, source, existing_backend
):
    path = tmp_path / "state.db"
    store = SQLiteStateStore(str(path))
    first = _telegram_task(1)
    store.stage_inbox_task(first)
    # Simulate an item created before app-server became the default.
    with sqlite3.connect(path) as db:
        db.execute("DELETE FROM conversation_executors")
        if existing_backend:
            db.execute(
                "INSERT INTO conversation_executors VALUES (?, ?)",
                (first.work_item_id, existing_backend),
            )
    store = SQLiteStateStore(str(path))
    event = replace(_telegram_task(2), work_item_id="item_new")
    event = replace(event, envelope=replace(event.envelope, source=source))
    store.stage_inbox_task(event)
    assert store.get_conversation_executor(conversation_id="item_new") == "app-server"
    assert store.get_conversation_executor(conversation_id=first.work_item_id) == (
        existing_backend or "exec"
    )
    returning = replace(
        event, task_id="task:returning", work_item_id=first.work_item_id
    )
    store.stage_inbox_task(returning)
    assert store.get_conversation_executor(conversation_id=first.work_item_id) == (
        existing_backend or "exec"
    )


def test_legacy_unassigned_messages_keep_exec_default(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    task = replace(_telegram_task(1), work_item_id=None)
    store.stage_inbox_task(task)
    conversation = store.get_inbox_task(task_id=task.task_id).conversation_id
    assert store.get_conversation_executor(conversation_id=conversation) == "exec"


def test_upgrade_preserves_settings_and_old_thread_and_journals(tmp_path):
    path = tmp_path / "state.db"
    store = SQLiteStateStore(str(path))
    task = _telegram_task(1)
    store.stage_inbox_task(task)
    legacy = "conv_before_upgrade"
    with sqlite3.connect(path) as db:
        db.execute("DELETE FROM worker_schema_migrations")
        db.execute("DELETE FROM conversation_executors")
        db.execute("UPDATE work_items SET origin_conversation_id = ?", (legacy,))
        db.execute("UPDATE worker_inbox SET conversation_id = ?", (legacy,))
        db.execute("UPDATE work_item_events SET conversation_id = ?", (legacy,))
        db.execute(
            "INSERT INTO conversation_executors VALUES (?, 'app-server')", (legacy,)
        )
    store = SQLiteStateStore(str(path))
    assert (
        store.get_inbox_task(task_id=task.task_id).conversation_id == task.work_item_id
    )
    assert (
        store.get_conversation_executor(conversation_id=task.work_item_id)
        == "app-server"
    )
    root = tmp_path / "sessions"
    old = root / hashlib.sha256(legacy.encode()).hexdigest()[:24]
    old.mkdir(parents=True, mode=0o700)
    (old / "thread.json").write_text(json.dumps({"thread_id": "old-thread"}))
    (old / "request-abc.json").write_text('{"state":"submitted"}')
    (old / "pending.json").write_text('{"request_id":"abc"}')
    new = root / hashlib.sha256(task.work_item_id.encode()).hexdigest()[:24]
    new.mkdir(mode=0o700)
    executor = AppServerExecutor(
        session_root=str(root), history_store=store, work_item_id=task.work_item_id
    )
    executor._migrate_session(new)
    assert json.loads((new / "thread.json").read_text())["thread_id"] == "old-thread"
    assert (new / "pending.json").read_text() == (old / "pending.json").read_text()
    assert (new / "request-abc.json").exists()
    # A second startup may not overwrite newer canonical state.
    (new / "thread.json").write_text('{"thread_id":"newer-thread"}')
    executor._migrate_session(new)
    assert json.loads((new / "thread.json").read_text())["thread_id"] == "newer-thread"
    # Historical General items could contain several conversation threads.
    # Neither is chosen: the item keeps its own thread, or starts a new one.
    other_legacy = "conv_competing"
    other = root / hashlib.sha256(other_legacy.encode()).hexdigest()[:24]
    other.mkdir(mode=0o700)
    (other / "thread.json").write_text('{"thread_id":"competing-thread"}')
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO work_item_session_aliases VALUES (?, ?)",
            (task.work_item_id, other_legacy),
        )
    executor._migrate_session(new)
    assert json.loads((new / "thread.json").read_text())["thread_id"] == "newer-thread"
    (new / "thread.json").unlink()
    (new / "request-abc.json").unlink()
    (new / "pending.json").unlink()
    # The first old session still holds a request with no recorded outcome.
    with pytest.raises(ValueError, match="unfinished"):
        executor._migrate_session(new)
    (old / "request-abc.json").write_text('{"result":{"errors":[]}}')
    executor._migrate_session(new)
    assert not (new / "thread.json").exists()
    assert (new / "request-abc.json").read_bytes() == (
        old / "request-abc.json"
    ).read_bytes()


@pytest.fixture
def ambiguous_sessions(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    task = _telegram_task(1)
    store.stage_inbox_task(task)
    root = tmp_path / "sessions"
    sessions = []
    with sqlite3.connect(tmp_path / "state.db") as db:
        for number in range(2):
            legacy = f"conv_old_{number}"
            db.execute(
                "INSERT INTO work_item_session_aliases VALUES (?, ?)",
                (task.work_item_id, legacy),
            )
            old = root / hashlib.sha256(legacy.encode()).hexdigest()[:24]
            old.mkdir(parents=True, mode=0o700)
            (old / "thread.json").write_text(json.dumps({"thread_id": legacy}))
            key = str(number) * 64
            (old / f"request-{key}.json").write_text(
                json.dumps({"result": {"errors": [], "stdout": f"saved-{number}"}})
            )
            sessions.append(old)
    new = root / hashlib.sha256(task.work_item_id.encode()).hexdigest()[:24]
    new.mkdir(mode=0o700)
    executor = AppServerExecutor(
        session_root=str(root), history_store=store, work_item_id=task.work_item_id
    )
    return executor, new, sessions


@pytest.mark.parametrize(
    "problem", ["unfinished", "missing_pending", "conflict", "private", "claimed"]
)
def test_ambiguous_migration_fails_closed(ambiguous_sessions, problem):
    executor, new, (first, second) = ambiguous_sessions
    if problem == "unfinished":
        (second / f"request-{'1' * 64}.json").write_text('{"state":"submitted"}')
        error = "unfinished"
    elif problem == "missing_pending":
        (second / "pending.json").write_text(json.dumps({"request_id": "2" * 64}))
        error = "unfinished"
    elif problem == "conflict":
        (second / f"request-{'0' * 64}.json").write_text('{"result":{"errors":[]}}')
        error = "conflicting"
    elif problem == "private":
        second.chmod(0o755)
        error = "private"
    else:
        (second / "migrated-to").write_text("item_elsewhere")
        error = "another item"
    with pytest.raises(ValueError, match=error):
        executor._migrate_session(new)
    assert not list(new.glob("request-*.json"))
    assert not (new / "legacy-migrated").exists()


def test_interrupted_ambiguous_migration_can_retry(ambiguous_sessions, monkeypatch):
    executor, new, sessions = ambiguous_sessions
    write = app_server._write_migration_file

    def interrupted(path, content):
        write(path, content)
        if path.parent == new and path.name.startswith("request-"):
            raise OSError("simulated crash")

    with monkeypatch.context() as patch:
        patch.setattr(app_server, "_write_migration_file", interrupted)
        with pytest.raises(OSError, match="simulated crash"):
            executor._migrate_session(new)
    assert not (new / "legacy-migrated").exists()
    executor._migrate_session(new)
    # All cached results work without any app-server process or thread.
    session = Session(new, {"command": ["does-not-exist"]})
    for number in range(2):
        result = session.execute({"request_id": str(number) * 64}, lambda *_: None)
        assert result.errors == []
        assert result.stdout == f"saved-{number}"
    assert session.server is None
    assert not (new / "thread.json").exists()
    assert not (new / "pending.json").exists()
    # Once finished, migration must not re-import stale legacy state.
    (sessions[0] / f"request-{'0' * 64}.json").write_text('{"state":"submitted"}')
    executor._migrate_session(new)
    assert session.execute({"request_id": "0" * 64}, lambda *_: None).errors == []
