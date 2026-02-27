"""SQLite-backed state store for idempotency and run history."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.models import RunRecord


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
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    dedupe_key TEXT PRIMARY KEY,
                    seen_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS run_records (
                    run_id TEXT PRIMARY KEY,
                    envelope_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    workflow TEXT NOT NULL,
                    policy_profile TEXT NOT NULL,
                    latency_ms INTEGER NOT NULL,
                    result_status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    schema_version TEXT NOT NULL
                )
                """
            )

    def seen(self, dedupe_key: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM idempotency_keys WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
        return row is not None

    def mark_seen(self, dedupe_key: str) -> None:
        seen_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO idempotency_keys (dedupe_key, seen_at)
                VALUES (?, ?)
                """,
                (dedupe_key, seen_at),
            )

    def append_run(self, record: RunRecord) -> None:
        payload = record.to_dict()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO run_records (
                    run_id,
                    envelope_id,
                    source,
                    workflow,
                    policy_profile,
                    latency_ms,
                    result_status,
                    created_at,
                    schema_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["run_id"],
                    payload["envelope_id"],
                    payload["source"],
                    payload["workflow"],
                    payload["policy_profile"],
                    payload["latency_ms"],
                    payload["result_status"],
                    payload["created_at"],
                    payload["schema_version"],
                ),
            )

    def list_runs(self) -> list[RunRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM run_records ORDER BY created_at ASC"
            ).fetchall()

        return [
            RunRecord(
                run_id=row["run_id"],
                envelope_id=row["envelope_id"],
                source=row["source"],
                workflow=row["workflow"],
                policy_profile=row["policy_profile"],
                latency_ms=row["latency_ms"],
                result_status=row["result_status"],
                created_at=_parse_rfc3339_utc(row["created_at"]),
                schema_version=row["schema_version"],
            )
            for row in rows
        ]


def _parse_rfc3339_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)
