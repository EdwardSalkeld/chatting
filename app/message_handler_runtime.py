"""Message-handler runtime helpers for ingress task ledger and strict egress validation."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.broker import TaskQueueMessage


@dataclass(frozen=True)
class TaskLedgerRecord:
    task_id: str
    envelope_id: str
    trace_id: str
    task_message: TaskQueueMessage
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


def _parse_rfc3339_utc(raw_value: str) -> datetime:
    return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).astimezone(timezone.utc)


__all__ = [
    "TaskLedgerRecord",
    "TaskLedgerStore",
]
