"""Message-handler runtime helpers for ingress task ledger and strict egress validation."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.broker import EgressQueueMessage, TaskQueueMessage


@dataclass(frozen=True)
class TaskLedgerRecord:
    task_id: str
    envelope_id: str
    trace_id: str
    task_message: TaskQueueMessage
    created_at: datetime


@dataclass(frozen=True)
class StagedEgressRecord:
    task_id: str
    event_id: str
    sequence: int
    egress_message: EgressQueueMessage
    created_at: datetime


class TaskLedgerStore:
    """Persist ingress task ledger for strict egress verification."""

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
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS task_ledger (
                    task_id TEXT PRIMARY KEY,
                    envelope_id TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    task_payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS egress_sequence_state (
                    task_id TEXT PRIMARY KEY,
                    next_sequence INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS staged_egress_events (
                    task_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (task_id, event_id)
                )
                """
            )
            connection.commit()

    def record_task(self, task_message: TaskQueueMessage) -> None:
        payload = task_message.to_dict()
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO task_ledger (
                    task_id,
                    envelope_id,
                    trace_id,
                    task_payload_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    task_message.task_id,
                    task_message.envelope.id,
                    task_message.trace_id,
                    json.dumps(payload, sort_keys=True),
                    created_at,
                ),
            )
            connection.commit()

    def get_task(self, task_id: str) -> TaskLedgerRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT task_id, envelope_id, trace_id, task_payload_json, created_at
                FROM task_ledger
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()

        if row is None:
            return None

        return TaskLedgerRecord(
            task_id=row["task_id"],
            envelope_id=row["envelope_id"],
            trace_id=row["trace_id"],
            task_message=TaskQueueMessage.from_dict(json.loads(row["task_payload_json"])),
            created_at=_parse_rfc3339_utc(row["created_at"]),
        )

    def stage_egress_event(self, egress_message: EgressQueueMessage) -> None:
        if egress_message.event_id is None:
            raise ValueError("egress_message.event_id is required")
        if egress_message.sequence is None:
            raise ValueError("egress_message.sequence is required")
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO staged_egress_events (
                    task_id,
                    event_id,
                    sequence,
                    payload_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    egress_message.task_id,
                    egress_message.event_id,
                    egress_message.sequence,
                    json.dumps(egress_message.to_dict(), sort_keys=True),
                    created_at,
                ),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO egress_sequence_state (task_id, next_sequence)
                VALUES (?, 0)
                """,
                (egress_message.task_id,),
            )
            connection.commit()

    def expected_sequence(self, task_id: str) -> int:
        if not task_id:
            raise ValueError("task_id is required")
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT next_sequence
                FROM egress_sequence_state
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()
        return int(row["next_sequence"]) if row is not None else 0

    def get_staged_event_by_sequence(self, *, task_id: str, sequence: int) -> StagedEgressRecord | None:
        if not task_id:
            raise ValueError("task_id is required")
        if sequence < 0:
            raise ValueError("sequence must be non-negative")
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT task_id, event_id, sequence, payload_json, created_at
                FROM staged_egress_events
                WHERE task_id = ? AND sequence = ?
                """,
                (task_id, sequence),
            ).fetchone()

        if row is None:
            return None

        return StagedEgressRecord(
            task_id=row["task_id"],
            event_id=row["event_id"],
            sequence=int(row["sequence"]),
            egress_message=EgressQueueMessage.from_dict(json.loads(row["payload_json"])),
            created_at=_parse_rfc3339_utc(row["created_at"]),
        )

    def mark_staged_event_dispatched(self, *, task_id: str, event_id: str, sequence: int) -> None:
        if not task_id:
            raise ValueError("task_id is required")
        if not event_id:
            raise ValueError("event_id is required")
        if sequence < 0:
            raise ValueError("sequence must be non-negative")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                DELETE FROM staged_egress_events
                WHERE task_id = ? AND event_id = ? AND sequence = ?
                """,
                (task_id, event_id, sequence),
            )
            connection.execute(
                """
                INSERT INTO egress_sequence_state (task_id, next_sequence)
                VALUES (?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    next_sequence = CASE
                        WHEN egress_sequence_state.next_sequence <= excluded.next_sequence
                        THEN excluded.next_sequence
                        ELSE egress_sequence_state.next_sequence
                    END
                """,
                (task_id, sequence + 1),
            )
            connection.commit()


def _parse_rfc3339_utc(raw_value: str) -> datetime:
    return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).astimezone(timezone.utc)


__all__ = [
    "TaskLedgerRecord",
    "StagedEgressRecord",
    "TaskLedgerStore",
]
