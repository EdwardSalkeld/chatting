"""Work item identity survives transport changes and upgrades."""

from dataclasses import replace
import hashlib
import json

import pytest
import sqlite3

from app.state import SQLiteStateStore
from app.worker.executor.app_server import AppServerExecutor
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


def test_new_github_item_uses_app_server_without_changing_existing_item(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    first = _telegram_task(1)
    store.stage_inbox_task(first)
    event = replace(_telegram_task(2), work_item_id="item_github")
    event = replace(event, envelope=replace(event.envelope, source="webhook"))
    store.stage_inbox_task(event)
    assert (
        store.get_conversation_executor(conversation_id="item_github") == "app-server"
    )
    assert store.get_conversation_executor(conversation_id=first.work_item_id) == "exec"
    returning = replace(
        event, task_id="task:returning", work_item_id=first.work_item_id
    )
    store.stage_inbox_task(returning)
    assert store.get_conversation_executor(conversation_id=first.work_item_id) == "exec"


def test_upgrade_preserves_settings_and_old_thread_and_journals(tmp_path):
    path = tmp_path / "state.db"
    store = SQLiteStateStore(str(path))
    task = _telegram_task(1)
    store.stage_inbox_task(task)
    legacy = "conv_before_upgrade"
    with sqlite3.connect(path) as db:
        db.execute("DELETE FROM worker_schema_migrations")
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
    # The first old session still holds a request with no recorded outcome.
    with pytest.raises(ValueError, match="unfinished"):
        executor._migrate_session(new)
    (old / "request-abc.json").write_text('{"result":{"errors":[]}}')
    executor._migrate_session(new)
    assert not (new / "thread.json").exists()
