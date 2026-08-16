"""SQLite-backed state store for idempotency and run history."""

from __future__ import annotations

import sqlite3
import json
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, cast

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import (
    AttachmentRef,
    AuditEvent,
    DeadLetterRecord,
    PromptContext,
    ReplyChannel,
    RunRecord,
    TaskEnvelope,
)


@dataclass(frozen=True)
class InboxTask:
    task_message: TaskQueueMessage
    conversation_id: str
    state: str
    parent_task_id: str | None


class SQLiteStateStore:
    """Persist dedupe keys and run records in SQLite."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            self._initialize_idempotency_table(connection)
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS run_records (
                    run_id TEXT PRIMARY KEY,
                    envelope_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    workflow TEXT NOT NULL,
                    latency_ms INTEGER NOT NULL,
                    result_status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    schema_version TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    envelope_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    workflow TEXT NOT NULL,
                    result_status TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    schema_version TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS dead_letters (
                    dead_letter_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    reason_codes_json TEXT NOT NULL,
                    last_error TEXT,
                    attempt_count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    replayed_run_id TEXT,
                    schema_version TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_turns (
                    turn_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    target TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    run_id TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS dispatched_events (
                    run_id TEXT NOT NULL,
                    event_index INTEGER NOT NULL,
                    dispatched_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, event_index)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS dispatched_event_ids (
                    task_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    dispatched_at TEXT NOT NULL,
                    PRIMARY KEY (task_id, event_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS egress_outbox (
                    event_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    event_kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    publish_state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS worker_activity_events (
                    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    task_id TEXT,
                    envelope_id TEXT,
                    run_id TEXT,
                    source TEXT,
                    workflow TEXT,
                    phase TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    is_internal INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_routes (
                    route_type TEXT NOT NULL,
                    route_key TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (route_type, route_key),
                    UNIQUE (conversation_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS worker_inbox (
                    task_id TEXT PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    task_payload_json TEXT NOT NULL,
                    broker_guid TEXT,
                    state TEXT NOT NULL,
                    parent_task_id TEXT,
                    staged_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    reply_delivered_at TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS worker_inbox_conversation_state
                ON worker_inbox (conversation_id, state, staged_at)
                """
            )
            connection.commit()

    def stage_inbox_task(
        self, task_message: TaskQueueMessage, *, broker_guid: str | None = None
    ) -> bool:
        """Durably stage a broker task before its BBMB message is acknowledged."""
        route_type, route_key = _conversation_route(task_message)
        now = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT conversation_id FROM conversation_routes
                WHERE route_type = ? AND route_key = ?
                """,
                (route_type, route_key),
            ).fetchone()
            if row is None:
                conversation_id = f"conv_{uuid.uuid4().hex}"
                connection.execute(
                    """
                    INSERT INTO conversation_routes (
                        route_type, route_key, conversation_id, created_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (route_type, route_key, conversation_id, now),
                )
            else:
                conversation_id = str(row["conversation_id"])
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO worker_inbox (
                    task_id, conversation_id, task_payload_json, broker_guid,
                    state, parent_task_id, staged_at, updated_at, reply_delivered_at
                ) VALUES (?, ?, ?, ?, 'pending', NULL, ?, ?, NULL)
                """,
                (
                    task_message.task_id,
                    conversation_id,
                    json.dumps(task_message.to_dict(), sort_keys=True),
                    broker_guid,
                    now,
                    now,
                ),
            )
            connection.commit()
        return cursor.rowcount > 0

    def claim_next_inbox_task(self) -> InboxTask | None:
        """Atomically lease the oldest pending inbox task to this worker."""
        now = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM worker_inbox
                WHERE state = 'pending'
                   OR (state = 'closing' AND parent_task_id IS NULL)
                ORDER BY CASE state WHEN 'closing' THEN 0 ELSE 1 END,
                         staged_at ASC, task_id ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            claimed_state = str(row["state"])
            if claimed_state == "pending":
                connection.execute(
                    """
                    UPDATE worker_inbox SET state = 'active', updated_at = ?
                    WHERE task_id = ? AND state = 'pending'
                    """,
                    (now, row["task_id"]),
                )
            connection.commit()
        return _inbox_task_from_row(
            row, state="active" if claimed_state == "pending" else claimed_state
        )

    def recover_inbox_tasks(self) -> None:
        """Recover leases after restart without replaying already-delivered work."""
        now = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE worker_inbox
                SET state = 'pending', updated_at = ?
                WHERE state = 'active' AND reply_delivered_at IS NULL
                """,
                (now,),
            )
            connection.execute(
                """
                UPDATE worker_inbox
                SET state = 'closing', updated_at = ?
                WHERE state = 'active' AND reply_delivered_at IS NOT NULL
                """,
                (now,),
            )
            connection.execute(
                """
                UPDATE worker_inbox
                SET state = 'pending', parent_task_id = NULL, updated_at = ?
                WHERE state = 'attached'
                  AND parent_task_id NOT IN (
                      SELECT task_id FROM worker_inbox WHERE state = 'closing'
                  )
                """,
                (now,),
            )
            connection.commit()

    def release_active_inbox_task(self, *, task_id: str) -> None:
        now = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute(
                """
                UPDATE worker_inbox SET state = 'pending', updated_at = ?
                WHERE task_id = ? AND state = 'active'
                """,
                (now, task_id),
            )
            connection.commit()

    def claim_conversation_followups(self, *, parent_task_id: str) -> list[InboxTask]:
        """Attach all currently pending Telegram turns in the parent's conversation."""
        if not parent_task_id:
            raise ValueError("parent_task_id is required")
        now = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            parent = connection.execute(
                "SELECT conversation_id, state FROM worker_inbox WHERE task_id = ?",
                (parent_task_id,),
            ).fetchone()
            if parent is None or parent["state"] not in {"active", "attached"}:
                connection.commit()
                return []
            rows = connection.execute(
                """
                SELECT * FROM worker_inbox
                WHERE conversation_id = ? AND state = 'pending' AND task_id != ?
                ORDER BY staged_at ASC, task_id ASC
                """,
                (parent["conversation_id"], parent_task_id),
            ).fetchall()
            for row in rows:
                message = TaskQueueMessage.from_dict(
                    json.loads(row["task_payload_json"])
                )
                if message.envelope.reply_channel.type != "telegram":
                    continue
                connection.execute(
                    """
                    UPDATE worker_inbox
                    SET state = 'attached', parent_task_id = ?, updated_at = ?
                    WHERE task_id = ? AND state = 'pending'
                    """,
                    (parent_task_id, now, row["task_id"]),
                )
            connection.commit()
        return [
            _inbox_task_from_row(row, state="attached", parent_task_id=parent_task_id)
            for row in rows
            if TaskQueueMessage.from_dict(
                json.loads(row["task_payload_json"])
            ).envelope.reply_channel.type
            == "telegram"
        ]

    def has_unresolved_attached_followups(self, *, parent_task_id: str) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT 1 FROM worker_inbox
                WHERE parent_task_id = ? AND state = 'attached'
                LIMIT 1
                """,
                (parent_task_id,),
            ).fetchone()
        return row is not None

    def mark_inbox_reply_delivered(self, *, parent_task_id: str) -> bool:
        """Record delivery, closing only when incorporated turns are resolved.

        An early pickup reaction or progress message must not stop later calls
        from peeking at the conversation. With no attached turns the parent
        therefore stays active until normal executor completion; the timestamp
        still lets restart recovery avoid rerunning already-visible work.
        """
        now = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE worker_inbox
                SET reply_delivered_at = ?, updated_at = ?
                WHERE task_id = ? AND state IN ('active', 'attached')
                """,
                (now, now, parent_task_id),
            )
            if cursor.rowcount:
                attached = connection.execute(
                    """
                    SELECT 1 FROM worker_inbox
                    WHERE parent_task_id = ? AND state = 'attached' LIMIT 1
                    """,
                    (parent_task_id,),
                ).fetchone()
                if attached is not None:
                    connection.execute(
                        """
                        UPDATE worker_inbox SET state = 'closing', updated_at = ?
                        WHERE task_id = ?
                        """,
                        (now, parent_task_id),
                    )
                    connection.execute(
                        """
                        UPDATE worker_inbox
                        SET state = 'closing', reply_delivered_at = ?, updated_at = ?
                        WHERE parent_task_id = ? AND state = 'attached'
                        """,
                        (now, now, parent_task_id),
                    )
            connection.commit()
        return cursor.rowcount > 0

    def finish_inbox_task(self, *, parent_task_id: str) -> list[InboxTask]:
        """Complete a delivered bundle, or release unresolved children separately."""
        now = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            parent = connection.execute(
                "SELECT state FROM worker_inbox WHERE task_id = ?",
                (parent_task_id,),
            ).fetchone()
            if parent is None:
                connection.commit()
                return []
            children = connection.execute(
                """
                SELECT * FROM worker_inbox
                WHERE parent_task_id = ? AND state IN ('attached', 'closing')
                ORDER BY staged_at ASC, task_id ASC
                """,
                (parent_task_id,),
            ).fetchall()
            delivered = parent["state"] == "closing"
            if delivered:
                connection.execute(
                    """
                    UPDATE worker_inbox SET state = 'completed', updated_at = ?
                    WHERE task_id = ? OR parent_task_id = ?
                    """,
                    (now, parent_task_id, parent_task_id),
                )
            else:
                connection.execute(
                    """
                    UPDATE worker_inbox SET state = 'completed', updated_at = ?
                    WHERE task_id = ?
                    """,
                    (now, parent_task_id),
                )
                connection.execute(
                    """
                    UPDATE worker_inbox
                    SET state = 'pending', parent_task_id = NULL, updated_at = ?
                    WHERE parent_task_id = ? AND state = 'attached'
                    """,
                    (now, parent_task_id),
                )
            connection.commit()
        if not delivered:
            return []
        return [_inbox_task_from_row(row, state="completed") for row in children]

    def list_closing_inbox_followups(self, *, parent_task_id: str) -> list[InboxTask]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT * FROM worker_inbox
                WHERE parent_task_id = ? AND state = 'closing'
                ORDER BY staged_at ASC, task_id ASC
                """,
                (parent_task_id,),
            ).fetchall()
        return [_inbox_task_from_row(row) for row in rows]

    def count_inbox_followups(self, *, parent_task_id: str) -> int:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count FROM worker_inbox
                WHERE parent_task_id = ?
                """,
                (parent_task_id,),
            ).fetchone()
        return 0 if row is None else int(row["count"])

    def get_inbox_task(self, *, task_id: str) -> InboxTask | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM worker_inbox WHERE task_id = ?", (task_id,)
            ).fetchone()
        return None if row is None else _inbox_task_from_row(row)

    def _initialize_idempotency_table(self, connection: sqlite3.Connection) -> None:
        idempotency_columns = connection.execute(
            "PRAGMA table_info(idempotency_keys)"
        ).fetchall()
        if not idempotency_columns:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    source TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL,
                    seen_at TEXT NOT NULL,
                    PRIMARY KEY (source, dedupe_key)
                )
                """
            )
            return

        column_names = {row["name"] for row in idempotency_columns}
        if column_names == {"source", "dedupe_key", "seen_at"}:
            return

        raise ValueError(
            "unsupported idempotency_keys schema; expected columns "
            "{source, dedupe_key, seen_at}"
        )

    def seen(self, source: str, dedupe_key: str) -> bool:
        if not source:
            raise ValueError("source is required")
        if not dedupe_key:
            raise ValueError("dedupe_key is required")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT 1 FROM idempotency_keys WHERE source = ? AND dedupe_key = ?",
                (source, dedupe_key),
            ).fetchone()
        return row is not None

    def mark_seen(self, source: str, dedupe_key: str) -> None:
        if not source:
            raise ValueError("source is required")
        if not dedupe_key:
            raise ValueError("dedupe_key is required")
        seen_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO idempotency_keys (source, dedupe_key, seen_at)
                VALUES (?, ?, ?)
                """,
                (source, dedupe_key, seen_at),
            )
            connection.commit()

    def append_run(self, record: RunRecord) -> None:
        payload = record.to_dict()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO run_records (
                    run_id,
                    envelope_id,
                    source,
                    workflow,
                    latency_ms,
                    result_status,
                    created_at,
                    schema_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["run_id"],
                    payload["envelope_id"],
                    payload["source"],
                    payload["workflow"],
                    payload["latency_ms"],
                    payload["result_status"],
                    payload["created_at"],
                    payload["schema_version"],
                ),
            )
            connection.commit()

    def list_runs(self) -> list[RunRecord]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM run_records ORDER BY created_at ASC"
            ).fetchall()

        return [
            RunRecord(
                run_id=row["run_id"],
                envelope_id=row["envelope_id"],
                source=row["source"],
                workflow=row["workflow"],
                latency_ms=row["latency_ms"],
                result_status=row["result_status"],
                created_at=_parse_rfc3339_utc(row["created_at"]),
                schema_version=row["schema_version"],
            )
            for row in rows
        ]

    def list_recent_runs(
        self, *, limit: int, include_internal: bool = False
    ) -> list[RunRecord]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        # Internal runs (heartbeats) dominate the table, so exclude them in SQL
        # rather than after the limit — otherwise the limited window is almost
        # entirely heartbeats and real runs never surface.
        where = "" if include_internal else "WHERE source != 'internal'"
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT * FROM run_records {where} ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            RunRecord(
                run_id=row["run_id"],
                envelope_id=row["envelope_id"],
                source=row["source"],
                workflow=row["workflow"],
                latency_ms=row["latency_ms"],
                result_status=row["result_status"],
                created_at=_parse_rfc3339_utc(row["created_at"]),
                schema_version=row["schema_version"],
            )
            for row in rows
        ]

    def get_run(self, *, run_id: str) -> RunRecord | None:
        if not run_id:
            raise ValueError("run_id is required")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM run_records WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return RunRecord(
            run_id=row["run_id"],
            envelope_id=row["envelope_id"],
            source=row["source"],
            workflow=row["workflow"],
            latency_ms=row["latency_ms"],
            result_status=row["result_status"],
            created_at=_parse_rfc3339_utc(row["created_at"]),
            schema_version=row["schema_version"],
        )

    def append_audit_event(self, event: AuditEvent) -> None:
        payload = event.to_dict()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO audit_events (
                    run_id,
                    envelope_id,
                    source,
                    workflow,
                    result_status,
                    detail_json,
                    created_at,
                    schema_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["run_id"],
                    payload["envelope_id"],
                    payload["source"],
                    payload["workflow"],
                    payload["result_status"],
                    json.dumps(payload["detail"], sort_keys=True),
                    payload["created_at"],
                    payload["schema_version"],
                ),
            )
            connection.commit()

    def list_audit_events(self) -> list[AuditEvent]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM audit_events ORDER BY event_id ASC"
            ).fetchall()

        return [
            AuditEvent(
                run_id=row["run_id"],
                envelope_id=row["envelope_id"],
                source=row["source"],
                workflow=row["workflow"],
                result_status=row["result_status"],
                detail=json.loads(row["detail_json"]),
                created_at=_parse_rfc3339_utc(row["created_at"]),
                schema_version=row["schema_version"],
            )
            for row in rows
        ]

    def get_audit_event_for_run(self, *, run_id: str) -> AuditEvent | None:
        if not run_id:
            raise ValueError("run_id is required")
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT *
                FROM audit_events
                WHERE run_id = ?
                ORDER BY event_id DESC
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return AuditEvent(
            run_id=row["run_id"],
            envelope_id=row["envelope_id"],
            source=row["source"],
            workflow=row["workflow"],
            result_status=row["result_status"],
            detail=json.loads(row["detail_json"]),
            created_at=_parse_rfc3339_utc(row["created_at"]),
            schema_version=row["schema_version"],
        )

    def append_dead_letter(
        self,
        *,
        run_id: str,
        envelope: TaskEnvelope,
        reason_codes: list[str],
        last_error: str | None,
        attempt_count: int,
    ) -> int:
        if not run_id:
            raise ValueError("run_id is required")
        if not reason_codes:
            raise ValueError("reason_codes are required")
        if attempt_count <= 0:
            raise ValueError("attempt_count must be positive")
        payload = envelope.to_dict()
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                INSERT INTO dead_letters (
                    run_id,
                    envelope_json,
                    reason_codes_json,
                    last_error,
                    attempt_count,
                    status,
                    created_at,
                    replayed_run_id,
                    schema_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    json.dumps(payload, sort_keys=True),
                    json.dumps(reason_codes),
                    last_error,
                    attempt_count,
                    "pending",
                    created_at,
                    None,
                    payload["schema_version"],
                ),
            )
            connection.commit()
            if cursor.lastrowid is None:
                raise RuntimeError("failed to retrieve dead letter ID after insert")
            return cursor.lastrowid

    def list_dead_letters(self, *, status: str | None = None) -> list[DeadLetterRecord]:
        with closing(self._connect()) as connection:
            if status is None:
                rows = connection.execute(
                    "SELECT * FROM dead_letters ORDER BY dead_letter_id ASC"
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM dead_letters
                    WHERE status = ?
                    ORDER BY dead_letter_id ASC
                    """,
                    (status,),
                ).fetchall()

        return [
            DeadLetterRecord(
                dead_letter_id=row["dead_letter_id"],
                run_id=row["run_id"],
                envelope=_task_envelope_from_dict(json.loads(row["envelope_json"])),
                reason_codes=json.loads(row["reason_codes_json"]),
                last_error=row["last_error"],
                attempt_count=row["attempt_count"],
                status=row["status"],
                created_at=_parse_rfc3339_utc(row["created_at"]),
                replayed_run_id=row["replayed_run_id"],
                schema_version=row["schema_version"],
            )
            for row in rows
        ]

    def mark_dead_letter_replayed(
        self, dead_letter_id: int, replayed_run_id: str
    ) -> None:
        if dead_letter_id <= 0:
            raise ValueError("dead_letter_id must be positive")
        if not replayed_run_id:
            raise ValueError("replayed_run_id is required")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                UPDATE dead_letters
                SET status = ?, replayed_run_id = ?
                WHERE dead_letter_id = ?
                """,
                ("replayed", replayed_run_id, dead_letter_id),
            )
            connection.commit()

    def append_conversation_turn(
        self,
        *,
        channel: str,
        target: str,
        role: str,
        content: str,
        run_id: str | None = None,
    ) -> None:
        if not channel:
            raise ValueError("channel is required")
        if not target:
            raise ValueError("target is required")
        if role not in {"user", "assistant"}:
            raise ValueError("role must be user or assistant")
        if not content.strip():
            raise ValueError("content is required")
        if run_id is not None and not run_id:
            raise ValueError("run_id must not be empty")

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO conversation_turns (
                    channel,
                    target,
                    role,
                    content,
                    run_id,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (channel, target, role, content, run_id, created_at),
            )
            connection.commit()

    def list_recent_conversation_turns(
        self,
        *,
        channel: str,
        target: str,
        limit: int,
    ) -> list[tuple[str, str]]:
        if not channel:
            raise ValueError("channel is required")
        if not target:
            raise ValueError("target is required")
        if limit <= 0:
            raise ValueError("limit must be positive")

        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT role, content
                FROM conversation_turns
                WHERE channel = ? AND target = ?
                ORDER BY turn_id DESC
                LIMIT ?
                """,
                (channel, target, limit),
            ).fetchall()
        return [(row["role"], row["content"]) for row in reversed(rows)]

    def mark_dispatched_event(self, *, run_id: str, event_index: int) -> None:
        if not run_id:
            raise ValueError("run_id is required")
        if event_index < 0:
            raise ValueError("event_index must be non-negative")
        dispatched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO dispatched_events (run_id, event_index, dispatched_at)
                VALUES (?, ?, ?)
                """,
                (run_id, event_index, dispatched_at),
            )
            connection.commit()

    def list_dispatched_event_indices(self, *, run_id: str) -> list[int]:
        if not run_id:
            raise ValueError("run_id is required")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT event_index
                FROM dispatched_events
                WHERE run_id = ?
                ORDER BY event_index ASC
                """,
                (run_id,),
            ).fetchall()
        return [int(row["event_index"]) for row in rows]

    def mark_dispatched_event_id(self, *, task_id: str, event_id: str) -> None:
        if not task_id:
            raise ValueError("task_id is required")
        if not event_id:
            raise ValueError("event_id is required")
        dispatched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO dispatched_event_ids (task_id, event_id, dispatched_at)
                VALUES (?, ?, ?)
                """,
                (task_id, event_id, dispatched_at),
            )
            connection.commit()

    def has_dispatched_event_id(self, *, task_id: str, event_id: str) -> bool:
        if not task_id:
            raise ValueError("task_id is required")
        if not event_id:
            raise ValueError("event_id is required")
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM dispatched_event_ids
                WHERE task_id = ? AND event_id = ?
                """,
                (task_id, event_id),
            ).fetchone()
        return row is not None

    def queue_egress_outbox_event(self, message: EgressQueueMessage) -> None:
        if message.event_id is None:
            raise ValueError("event_id is required")
        if message.sequence is None:
            raise ValueError("sequence is required")
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO egress_outbox (
                    event_id,
                    task_id,
                    sequence,
                    event_kind,
                    payload_json,
                    publish_state,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.event_id,
                    message.task_id,
                    message.sequence,
                    message.event_kind,
                    json.dumps(message.to_dict(), sort_keys=True),
                    "pending_publish",
                    now,
                    now,
                ),
            )
            connection.commit()

    def mark_egress_outbox_event_published(self, *, event_id: str) -> None:
        if not event_id:
            raise ValueError("event_id is required")
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                UPDATE egress_outbox
                SET publish_state = ?, updated_at = ?
                WHERE event_id = ?
                """,
                ("published_unacked", now, event_id),
            )
            connection.commit()

    def mark_egress_outbox_event_acked(self, *, event_id: str) -> None:
        if not event_id:
            raise ValueError("event_id is required")
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                UPDATE egress_outbox
                SET publish_state = ?, updated_at = ?
                WHERE event_id = ?
                """,
                ("acked", now, event_id),
            )
            connection.commit()

    def list_replayable_egress_outbox_events(self) -> list[EgressQueueMessage]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT payload_json
                FROM egress_outbox
                WHERE publish_state = 'pending_publish'
                ORDER BY task_id ASC, sequence ASC, created_at ASC
                """
            ).fetchall()
        return [
            EgressQueueMessage.from_dict(json.loads(row["payload_json"]))
            for row in rows
        ]

    def append_worker_activity(
        self,
        *,
        occurred_at: datetime,
        phase: str,
        summary: str,
        detail: dict[str, object],
        task_id: str | None = None,
        envelope_id: str | None = None,
        run_id: str | None = None,
        source: str | None = None,
        workflow: str | None = None,
        is_internal: bool = False,
    ) -> None:
        if not phase:
            raise ValueError("phase is required")
        if not summary:
            raise ValueError("summary is required")
        payload = _json_safe_dict(detail)
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO worker_activity_events (
                    occurred_at,
                    task_id,
                    envelope_id,
                    run_id,
                    source,
                    workflow,
                    phase,
                    summary,
                    detail_json,
                    is_internal
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    occurred_at.astimezone(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    task_id,
                    envelope_id,
                    run_id,
                    source,
                    workflow,
                    phase,
                    summary,
                    json.dumps(payload, sort_keys=True),
                    1 if is_internal else 0,
                ),
            )
            connection.commit()

    def count_task_main_reply_egress_events(self, *, task_id: str) -> int:
        if not task_id:
            raise ValueError("task_id is required")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT detail_json
                FROM worker_activity_events
                WHERE task_id = ? AND phase = 'egress_incremental'
                """,
                (task_id,),
            ).fetchall()
        count = 0
        for row in rows:
            detail = json.loads(row["detail_json"])
            if detail.get("publish_source") == "main_reply":
                count += 1
        return count

    def list_recent_worker_activity(
        self,
        *,
        limit: int,
        include_internal: bool = False,
    ) -> list[dict[str, object]]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with closing(self._connect()) as connection:
            if include_internal:
                rows = connection.execute(
                    """
                    SELECT activity_id, occurred_at, task_id, envelope_id, run_id, source, workflow, phase, summary, detail_json, is_internal
                    FROM worker_activity_events
                    ORDER BY occurred_at DESC, activity_id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT activity_id, occurred_at, task_id, envelope_id, run_id, source, workflow, phase, summary, detail_json, is_internal
                    FROM worker_activity_events
                    WHERE is_internal = 0
                    ORDER BY occurred_at DESC, activity_id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return [
            {
                "activity_id": row["activity_id"],
                "occurred_at": row["occurred_at"],
                "task_id": row["task_id"],
                "envelope_id": row["envelope_id"],
                "run_id": row["run_id"],
                "source": row["source"],
                "workflow": row["workflow"],
                "phase": row["phase"],
                "summary": row["summary"],
                "detail": json.loads(row["detail_json"]),
                "is_internal": bool(row["is_internal"]),
            }
            for row in rows
        ]

    def list_worker_activity_for_run(
        self,
        *,
        run_id: str,
        task_id: str,
        envelope_id: str,
        include_internal: bool = False,
    ) -> list[dict[str, object]]:
        if not run_id:
            raise ValueError("run_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if not envelope_id:
            raise ValueError("envelope_id is required")
        with closing(self._connect()) as connection:
            if include_internal:
                rows = connection.execute(
                    """
                    SELECT activity_id, occurred_at, task_id, envelope_id, run_id, source, workflow, phase, summary, detail_json, is_internal
                    FROM worker_activity_events
                    WHERE run_id = ?
                       OR (task_id = ? AND envelope_id = ?)
                    ORDER BY occurred_at ASC, activity_id ASC
                    """,
                    (run_id, task_id, envelope_id),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT activity_id, occurred_at, task_id, envelope_id, run_id, source, workflow, phase, summary, detail_json, is_internal
                    FROM worker_activity_events
                    WHERE (run_id = ?
                       OR (task_id = ? AND envelope_id = ?))
                      AND is_internal = 0
                    ORDER BY occurred_at ASC, activity_id ASC
                    """,
                    (run_id, task_id, envelope_id),
                ).fetchall()
        return [
            {
                "activity_id": row["activity_id"],
                "occurred_at": row["occurred_at"],
                "task_id": row["task_id"],
                "envelope_id": row["envelope_id"],
                "run_id": row["run_id"],
                "source": row["source"],
                "workflow": row["workflow"],
                "phase": row["phase"],
                "summary": row["summary"],
                "detail": json.loads(row["detail_json"]),
                "is_internal": bool(row["is_internal"]),
            }
            for row in rows
        ]


def _parse_rfc3339_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def _serialize_rfc3339_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _conversation_route(task_message: TaskQueueMessage) -> tuple[str, str]:
    """Return the transport mapping used to look up an opaque conversation ID.

    Conversation IDs deliberately do not encode Telegram identity. A later route
    can therefore point email, web, or another transport at the same ID without
    changing inbox rows or the reply-time claiming protocol.
    """
    reply = task_message.envelope.reply_channel
    thread_metadata = {
        key: reply.metadata[key]
        for key in ("message_thread_id", "thread_id")
        if key in reply.metadata
    }
    route_key = json.dumps(
        {"target": reply.target, "thread": thread_metadata}, sort_keys=True
    )
    return reply.type, route_key


def _inbox_task_from_row(
    row: sqlite3.Row,
    *,
    state: str | None = None,
    parent_task_id: str | None = None,
) -> InboxTask:
    return InboxTask(
        task_message=TaskQueueMessage.from_dict(json.loads(row["task_payload_json"])),
        conversation_id=str(row["conversation_id"]),
        state=state or str(row["state"]),
        parent_task_id=(
            parent_task_id
            if parent_task_id is not None
            else (
                str(row["parent_task_id"])
                if row["parent_task_id"] is not None
                else None
            )
        ),
    )


def _task_envelope_from_dict(payload: dict[str, object]) -> TaskEnvelope:
    reply_channel = payload.get("reply_channel")
    if not isinstance(reply_channel, dict):
        raise ValueError("invalid dead letter envelope payload")
    raw_context_refs = payload.get("context_refs", [])
    if not isinstance(raw_context_refs, list):
        raise ValueError("invalid dead letter envelope payload")
    raw_attachments = payload.get("attachments", [])
    if not isinstance(raw_attachments, list):
        raise ValueError("invalid dead letter envelope payload")
    raw_prompt_context = payload.get("prompt_context", {})
    if not isinstance(raw_prompt_context, dict):
        raise ValueError("invalid dead letter envelope payload")
    context_refs = [str(value) for value in raw_context_refs]
    attachments: list[AttachmentRef] = []
    for raw_attachment in raw_attachments:
        if not isinstance(raw_attachment, dict):
            raise ValueError("invalid dead letter envelope payload")
        uri = raw_attachment.get("uri")
        if not isinstance(uri, str) or not uri.strip():
            raise ValueError("invalid dead letter envelope payload")
        name = raw_attachment.get("name")
        if name is not None and not isinstance(name, str):
            raise ValueError("invalid dead letter envelope payload")
        attachments.append(AttachmentRef(uri=uri, name=name))
    return TaskEnvelope(
        id=str(payload["id"]),
        source=cast(
            Literal["cron", "email", "im", "webhook", "internal", "reminder"],
            str(payload["source"]),
        ),
        received_at=_parse_rfc3339_utc(str(payload["received_at"])),
        actor=str(payload["actor"]) if isinstance(payload.get("actor"), str) else None,
        content=str(payload["content"]),
        attachments=attachments,
        context_refs=context_refs,
        reply_channel=ReplyChannel(
            type=str(reply_channel["type"]),
            target=str(reply_channel["target"]),
            metadata=dict(reply_channel.get("metadata", {}))
            if isinstance(reply_channel.get("metadata"), dict)
            else {},
        ),
        dedupe_key=str(payload["dedupe_key"]),
        prompt_context=PromptContext(
            global_instructions=_string_list_from_payload(
                raw_prompt_context.get("global_instructions", [])
            ),
            source_instructions=_string_list_from_payload(
                raw_prompt_context.get("source_instructions", [])
            ),
            reply_channel_instructions=_string_list_from_payload(
                raw_prompt_context.get("reply_channel_instructions", [])
            ),
            task_instructions=_string_list_from_payload(
                raw_prompt_context.get("task_instructions", [])
            ),
        ),
        schema_version=str(payload.get("schema_version", "1.0")),
    )


def _string_list_from_payload(value: object) -> list[str]:
    if not isinstance(value, list):
        raise ValueError("invalid dead letter envelope payload")
    return [str(item) for item in value]


def _json_safe_dict(value: dict[str, object]) -> dict[str, object]:
    return json.loads(json.dumps(value, sort_keys=True))
