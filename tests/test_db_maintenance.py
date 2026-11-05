import json
import sqlite3
from datetime import datetime, timezone

import pytest

from app.state.maintenance import (
    prune_worker_diagnostics,
    strip_audit_transcripts,
    maintain_worker,
)
from app.state import SQLiteStateStore


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "worker.db"
    SQLiteStateStore(str(path))
    connection = sqlite3.connect(path)
    yield connection
    connection.close()


def add_run(connection, task, date, state=None, payload=None):
    detail = payload or {
        "task_id": task,
        "reason_codes": ["executor_reported_errors"],
        "execution_result": {
            "stdout": 'output with "quotes" and\nnewlines',
            "stderr": "diagnostic output",
            "errors": ["real error"],
            "escalation_reason": "needs a larger model",
            "schema_version": "1.0",
        },
    }
    connection.execute(
        """INSERT INTO run_records
           (run_id,envelope_id,source,workflow,latency_ms,result_status,created_at,schema_version)
           VALUES (?,?, 'im','default',1,'success',?,'1.0')""",
        (task, task, date),
    )
    connection.execute(
        """INSERT INTO audit_events
           (run_id,envelope_id,source,workflow,result_status,detail_json,created_at,schema_version)
           VALUES (?,?,'im','default','success',?,?,'1.0')""",
        (task, task, json.dumps(detail), date),
    )
    connection.execute(
        """INSERT INTO worker_activity_events
           (task_id,occurred_at,phase,summary,detail_json)
           VALUES (?,?,'executor_stdout','output','{"content":"full original"}')""",
        (task, date),
    )
    if state:
        connection.execute(
            """INSERT INTO worker_inbox
               (task_id,conversation_id,task_payload_json,state,staged_at,updated_at)
               VALUES (?,'conversation','{}',?,?,?)""",
            (task, state, date, date),
        )
    connection.commit()
    return detail


def test_strip_preserves_metadata_activity_and_is_restartable(database):
    expected = add_run(database, "old", "2026-10-01T00:00:00Z", "completed")
    add_run(database, "other", "2026-11-01T00:00:00Z", "active")
    add_run(
        database, "no-output", "2026-11-01T00:00:00Z", payload={"task_id": "no-output"}
    )
    activity_before = database.execute(
        "SELECT * FROM worker_activity_events"
    ).fetchall()
    assert strip_audit_transcripts(database, batch_size=1) == 2
    del expected["execution_result"]["stdout"]
    del expected["execution_result"]["stderr"]
    actual = json.loads(
        database.execute(
            "SELECT detail_json FROM audit_events WHERE run_id='old'"
        ).fetchone()[0]
    )
    assert actual == expected
    assert (
        database.execute("SELECT * FROM worker_activity_events").fetchall()
        == activity_before
    )
    assert strip_audit_transcripts(database, batch_size=2) == 0
    # A late result from an old supervisor is cleaned by repeating the command.
    add_run(database, "late", "2026-11-05T00:00:00Z", "completed")
    assert strip_audit_transcripts(database) == 1


def test_prune_age_boundary_protects_unfinished_work_and_durable_state(database):
    old = "2026-10-01T00:00:00Z"
    add_run(database, "old", old, "completed")
    add_run(database, "internal-no-inbox", old)
    # Exact cutoff expressed in a different timezone must be retained.
    add_run(database, "boundary", "2026-10-22T02:00:00+02:00", "completed")
    add_run(database, "recent", "2026-11-01T00:00:00Z", "completed")
    for state in ("pending", "attached", "active", "closing"):
        add_run(database, state, old, state)
    database.execute(
        """INSERT INTO worker_telegram_history
        (target,message_id,role,attachments_json,occurred_at,created_at)
        VALUES ('chat',1,'user','[]',?,?)""",
        (old, old),
    )
    database.execute(
        """INSERT INTO egress_outbox
        (event_id,task_id,sequence,event_kind,payload_json,publish_state,created_at,updated_at)
        VALUES ('pending','old',1,'incremental','{}','pending',?,?)""",
        (old, old),
    )
    database.commit()
    inbox_before = database.execute("SELECT * FROM worker_inbox").fetchall()
    counts = prune_worker_diagnostics(
        database,
        retention_days=14,
        now=datetime(2026, 11, 5, tzinfo=timezone.utc),
        batch_size=1,
    )
    assert counts == {"run_records": 2, "audit_events": 2, "worker_activity_events": 2}
    assert database.execute("SELECT * FROM worker_inbox").fetchall() == inbox_before
    assert (
        database.execute("SELECT count(*) FROM worker_telegram_history").fetchone()[0]
        == 1
    )
    assert (
        database.execute("SELECT publish_state FROM egress_outbox").fetchone()[0]
        == "pending"
    )
    assert all(
        value == 0
        for value in prune_worker_diagnostics(
            database,
            retention_days=14,
            now=datetime(2026, 11, 5, tzinfo=timezone.utc),
        ).values()
    )
    # Changing the configured window expires the formerly recent completed run.
    assert prune_worker_diagnostics(
        database,
        retention_days=3,
        now=datetime(2026, 11, 5, tzinfo=timezone.utc),
    ) == {"run_records": 2, "audit_events": 2, "worker_activity_events": 2}


@pytest.mark.parametrize("days,batch", [(0, 1), (-1, 1), (14, 0)])
def test_invalid_windows_do_not_delete_data(database, days, batch):
    add_run(database, "old", "2026-10-01T00:00:00Z", "completed")
    with pytest.raises(ValueError):
        prune_worker_diagnostics(database, retention_days=days, batch_size=batch)
    assert database.execute("SELECT count(*) FROM run_records").fetchone()[0] == 1


def test_worker_delivery_cleanup_keeps_pending_recent_and_active_sends(database):
    old = "2020-01-01T00:00:00Z"
    add_run(database, "active", old, "active")
    for event, task, state, updated in (
        ("expire", "done", "acked", old),
        ("pending", "done", "pending", old),
        ("active", "active", "acked", old),
        ("recent", "done", "acked", "2099-01-01T00:00:00Z"),
    ):
        database.execute(
            """INSERT INTO egress_outbox
            (event_id,task_id,sequence,event_kind,payload_json,publish_state,created_at,updated_at)
            VALUES (?,?,1,'incremental','{}',?,?,?)""",
            (event, task, state, old, updated),
        )
    database.commit()
    assert (
        maintain_worker(database, retention_days=14, delivery_retention_days=30)[
            "egress_outbox"
        ]
        == 1
    )
    assert {
        row[0] for row in database.execute("SELECT event_id FROM egress_outbox")
    } == {"pending", "active", "recent"}
