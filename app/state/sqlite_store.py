"""SQLite-backed state store for idempotency and run history."""

from __future__ import annotations

import sqlite3
import json
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from app.models import AuditEvent, DeadLetterRecord, ReplyChannel, RunRecord, TaskEnvelope


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
                    policy_profile TEXT NOT NULL,
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
                    policy_profile TEXT NOT NULL,
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
            connection.commit()

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

        # Migrate legacy schema keyed only by dedupe_key.
        connection.execute("ALTER TABLE idempotency_keys RENAME TO idempotency_keys_legacy")
        connection.execute(
            """
            CREATE TABLE idempotency_keys (
                source TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                seen_at TEXT NOT NULL,
                PRIMARY KEY (source, dedupe_key)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO idempotency_keys (source, dedupe_key, seen_at)
            SELECT 'legacy', dedupe_key, seen_at
            FROM idempotency_keys_legacy
            """
        )
        connection.execute("DROP TABLE idempotency_keys_legacy")

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
                policy_profile=row["policy_profile"],
                latency_ms=row["latency_ms"],
                result_status=row["result_status"],
                created_at=_parse_rfc3339_utc(row["created_at"]),
                schema_version=row["schema_version"],
            )
            for row in rows
        ]

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
                    policy_profile,
                    result_status,
                    detail_json,
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
                policy_profile=row["policy_profile"],
                result_status=row["result_status"],
                detail=json.loads(row["detail_json"]),
                created_at=_parse_rfc3339_utc(row["created_at"]),
                schema_version=row["schema_version"],
            )
            for row in rows
        ]

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
            return int(cursor.lastrowid)

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

    def mark_dead_letter_replayed(self, dead_letter_id: int, replayed_run_id: str) -> None:
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


def _parse_rfc3339_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def _task_envelope_from_dict(payload: dict[str, object]) -> TaskEnvelope:
    reply_channel = payload.get("reply_channel")
    if not isinstance(reply_channel, dict):
        raise ValueError("invalid dead letter envelope payload")
    raw_context_refs = payload.get("context_refs", [])
    if not isinstance(raw_context_refs, list):
        raise ValueError("invalid dead letter envelope payload")
    context_refs = [str(value) for value in raw_context_refs]
    return TaskEnvelope(
        id=str(payload["id"]),
        source=str(payload["source"]),
        received_at=_parse_rfc3339_utc(str(payload["received_at"])),
        actor=payload.get("actor") if isinstance(payload.get("actor"), str) else None,
        content=str(payload["content"]),
        attachments=[],
        context_refs=context_refs,
        policy_profile=str(payload["policy_profile"]),
        reply_channel=ReplyChannel(
            type=str(reply_channel["type"]),
            target=str(reply_channel["target"]),
        ),
        dedupe_key=str(payload["dedupe_key"]),
        schema_version=str(payload.get("schema_version", "1.0")),
    )
