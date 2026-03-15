"""Message-handler runtime helpers for ingress task ledger and attachment cleanup."""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from app.broker import EgressQueueMessage, TaskQueueMessage

LOGGER = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class CompletedTaskRecord:
    task_id: str
    envelope_id: str
    trace_id: str
    completed_at: datetime


@dataclass(frozen=True)
class TelegramAttachmentRecord:
    attachment_path: str
    attachment_uri: str
    task_id: str
    envelope_id: str
    created_at: datetime
    eligible_after: datetime | None
    deleted_at: datetime | None
    cleanup_attempts: int
    last_cleanup_error: str | None


@dataclass(frozen=True)
class TelegramAttachmentCleanupResult:
    deleted_count: int = 0
    missing_count: int = 0
    failed_count: int = 0
    reclaimed_bytes: int = 0


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
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS completed_task_ledger (
                    task_id TEXT PRIMARY KEY,
                    envelope_id TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    completed_at TEXT NOT NULL
                )
                """
            )
            connection.commit()

    def record_task(self, task_message: TaskQueueMessage) -> None:
        payload = task_message.to_dict()
        created_at = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute(
                """
                DELETE FROM completed_task_ledger
                WHERE task_id = ?
                """,
                (task_message.task_id,),
            )
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
        created_at = _serialize_rfc3339_utc(datetime.now(timezone.utc))
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

    def get_completed_task(self, task_id: str) -> CompletedTaskRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT task_id, envelope_id, trace_id, completed_at
                FROM completed_task_ledger
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()

        if row is None:
            return None

        return CompletedTaskRecord(
            task_id=row["task_id"],
            envelope_id=row["envelope_id"],
            trace_id=row["trace_id"],
            completed_at=_parse_rfc3339_utc(row["completed_at"]),
        )

    def is_task_completed(self, *, task_id: str, envelope_id: str) -> bool:
        completed_record = self.get_completed_task(task_id)
        if completed_record is None:
            return False
        return completed_record.envelope_id == envelope_id

    def mark_task_completed(self, *, task_id: str, envelope_id: str, trace_id: str) -> None:
        completed_at = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO completed_task_ledger (
                    task_id,
                    envelope_id,
                    trace_id,
                    completed_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (task_id, envelope_id, trace_id, completed_at),
            )
            connection.execute(
                """
                DELETE FROM task_ledger
                WHERE task_id = ?
                """,
                (task_id,),
            )
            connection.execute(
                """
                DELETE FROM egress_sequence_state
                WHERE task_id = ?
                """,
                (task_id,),
            )
            connection.execute(
                """
                DELETE FROM staged_egress_events
                WHERE task_id = ?
                """,
                (task_id,),
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


class TelegramAttachmentStore:
    """Persist Telegram-downloaded local attachment lifecycle state."""

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
                CREATE TABLE IF NOT EXISTS telegram_attachment_ledger (
                    attachment_path TEXT PRIMARY KEY,
                    attachment_uri TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    envelope_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    eligible_after TEXT,
                    deleted_at TEXT,
                    cleanup_attempts INTEGER NOT NULL,
                    last_cleanup_error TEXT
                )
                """
            )
            connection.commit()

    def record_task_attachments(
        self,
        *,
        task_message: TaskQueueMessage,
        attachment_root_dir: str,
    ) -> int:
        envelope = task_message.envelope
        if envelope.source != "im" or envelope.reply_channel.type != "telegram":
            return 0
        root_dir = Path(attachment_root_dir).resolve()
        created_at = _serialize_rfc3339_utc(datetime.now(timezone.utc))
        tracked_paths: list[tuple[Path, str]] = []
        for attachment in envelope.attachments:
            tracked_path = _tracked_attachment_path(
                attachment_uri=attachment.uri,
                attachment_root_dir=root_dir,
            )
            if tracked_path is None:
                continue
            tracked_paths.append((tracked_path, attachment.uri))
        if not tracked_paths:
            return 0
        with closing(self._connect()) as connection:
            for tracked_path, attachment_uri in tracked_paths:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO telegram_attachment_ledger (
                        attachment_path,
                        attachment_uri,
                        task_id,
                        envelope_id,
                        created_at,
                        eligible_after,
                        deleted_at,
                        cleanup_attempts,
                        last_cleanup_error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(tracked_path),
                        attachment_uri,
                        task_message.task_id,
                        envelope.id,
                        created_at,
                        None,
                        None,
                        0,
                        None,
                    ),
                )
            connection.commit()
        return len(tracked_paths)

    def mark_task_attachments_eligible(
        self,
        *,
        task_id: str,
        eligible_after: datetime,
    ) -> int:
        if not task_id:
            raise ValueError("task_id is required")
        if eligible_after.tzinfo is None:
            raise ValueError("eligible_after must be timezone-aware")
        with closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE telegram_attachment_ledger
                SET eligible_after = ?, last_cleanup_error = NULL
                WHERE task_id = ? AND deleted_at IS NULL
                """,
                (_serialize_rfc3339_utc(eligible_after), task_id),
            )
            connection.commit()
        return int(cursor.rowcount)

    def list_cleanup_candidates(
        self,
        *,
        not_after: datetime,
        max_age_cutoff: datetime,
    ) -> list[TelegramAttachmentRecord]:
        if not_after.tzinfo is None:
            raise ValueError("not_after must be timezone-aware")
        if max_age_cutoff.tzinfo is None:
            raise ValueError("max_age_cutoff must be timezone-aware")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT
                    attachment_path,
                    attachment_uri,
                    task_id,
                    envelope_id,
                    created_at,
                    eligible_after,
                    deleted_at,
                    cleanup_attempts,
                    last_cleanup_error
                FROM telegram_attachment_ledger
                WHERE deleted_at IS NULL
                  AND (
                    (eligible_after IS NOT NULL AND eligible_after <= ?)
                    OR created_at <= ?
                  )
                ORDER BY created_at ASC
                """,
                (
                    _serialize_rfc3339_utc(not_after),
                    _serialize_rfc3339_utc(max_age_cutoff),
                ),
            ).fetchall()
        return [_attachment_record_from_row(row) for row in rows]

    def mark_attachment_deleted(self, *, attachment_path: str) -> None:
        if not attachment_path:
            raise ValueError("attachment_path is required")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                UPDATE telegram_attachment_ledger
                SET deleted_at = ?, last_cleanup_error = NULL
                WHERE attachment_path = ?
                """,
                (_serialize_rfc3339_utc(datetime.now(timezone.utc)), attachment_path),
            )
            connection.commit()

    def mark_cleanup_failed(self, *, attachment_path: str, error: str) -> None:
        if not attachment_path:
            raise ValueError("attachment_path is required")
        if not error:
            raise ValueError("error is required")
        with closing(self._connect()) as connection:
            connection.execute(
                """
                UPDATE telegram_attachment_ledger
                SET cleanup_attempts = cleanup_attempts + 1, last_cleanup_error = ?
                WHERE attachment_path = ?
                """,
                (error, attachment_path),
            )
            connection.commit()

    def list_records(self) -> list[TelegramAttachmentRecord]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT
                    attachment_path,
                    attachment_uri,
                    task_id,
                    envelope_id,
                    created_at,
                    eligible_after,
                    deleted_at,
                    cleanup_attempts,
                    last_cleanup_error
                FROM telegram_attachment_ledger
                ORDER BY created_at ASC, attachment_path ASC
                """
            ).fetchall()
        return [_attachment_record_from_row(row) for row in rows]


def cleanup_telegram_attachments(
    *,
    attachment_store: TelegramAttachmentStore,
    attachment_root_dir: str,
    completion_grace_period: timedelta,
    max_attachment_age: timedelta,
    now: datetime | None = None,
) -> TelegramAttachmentCleanupResult:
    """Delete tracked Telegram attachments once they are safely eligible."""
    if completion_grace_period.total_seconds() <= 0:
        raise ValueError("completion_grace_period must be positive")
    if max_attachment_age.total_seconds() <= 0:
        raise ValueError("max_attachment_age must be positive")
    cleanup_now = now or datetime.now(timezone.utc)
    if cleanup_now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    candidates = attachment_store.list_cleanup_candidates(
        not_after=cleanup_now,
        max_age_cutoff=cleanup_now - max_attachment_age,
    )
    result = TelegramAttachmentCleanupResult()
    root_dir = Path(attachment_root_dir).resolve()

    for candidate in candidates:
        attachment_path = Path(candidate.attachment_path)
        cleanup_reason = (
            "completed_grace_elapsed"
            if candidate.eligible_after is not None and candidate.eligible_after <= cleanup_now
            else "max_age_elapsed"
        )
        if not _is_relative_to(attachment_path, root_dir):
            attachment_store.mark_cleanup_failed(
                attachment_path=candidate.attachment_path,
                error="attachment_path_outside_root",
            )
            LOGGER.error(
                "telegram_attachment_cleanup_failed task_id=%s path=%s error=%s",
                candidate.task_id,
                candidate.attachment_path,
                "attachment_path_outside_root",
            )
            result = TelegramAttachmentCleanupResult(
                deleted_count=result.deleted_count,
                missing_count=result.missing_count,
                failed_count=result.failed_count + 1,
                reclaimed_bytes=result.reclaimed_bytes,
            )
            continue
        if not attachment_path.exists():
            attachment_store.mark_attachment_deleted(attachment_path=candidate.attachment_path)
            LOGGER.warning(
                "telegram_attachment_cleanup_missing task_id=%s path=%s reason=%s",
                candidate.task_id,
                candidate.attachment_path,
                cleanup_reason,
            )
            result = TelegramAttachmentCleanupResult(
                deleted_count=result.deleted_count,
                missing_count=result.missing_count + 1,
                failed_count=result.failed_count,
                reclaimed_bytes=result.reclaimed_bytes,
            )
            continue
        try:
            reclaimed_bytes = attachment_path.stat().st_size if attachment_path.is_file() else 0
            attachment_path.unlink()
            attachment_store.mark_attachment_deleted(attachment_path=candidate.attachment_path)
            LOGGER.info(
                "telegram_attachment_cleanup_deleted task_id=%s path=%s reason=%s bytes=%s",
                candidate.task_id,
                candidate.attachment_path,
                cleanup_reason,
                reclaimed_bytes,
            )
            result = TelegramAttachmentCleanupResult(
                deleted_count=result.deleted_count + 1,
                missing_count=result.missing_count,
                failed_count=result.failed_count,
                reclaimed_bytes=result.reclaimed_bytes + reclaimed_bytes,
            )
        except OSError as error:
            attachment_store.mark_cleanup_failed(
                attachment_path=candidate.attachment_path,
                error=f"{type(error).__name__}: {error}",
            )
            LOGGER.error(
                "telegram_attachment_cleanup_failed task_id=%s path=%s error=%s",
                candidate.task_id,
                candidate.attachment_path,
                f"{type(error).__name__}: {error}",
            )
            result = TelegramAttachmentCleanupResult(
                deleted_count=result.deleted_count,
                missing_count=result.missing_count,
                failed_count=result.failed_count + 1,
                reclaimed_bytes=result.reclaimed_bytes,
            )
    return result


def _parse_rfc3339_utc(raw_value: str) -> datetime:
    return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _serialize_rfc3339_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _attachment_record_from_row(row: sqlite3.Row) -> TelegramAttachmentRecord:
    eligible_after = row["eligible_after"]
    deleted_at = row["deleted_at"]
    return TelegramAttachmentRecord(
        attachment_path=row["attachment_path"],
        attachment_uri=row["attachment_uri"],
        task_id=row["task_id"],
        envelope_id=row["envelope_id"],
        created_at=_parse_rfc3339_utc(row["created_at"]),
        eligible_after=_parse_rfc3339_utc(eligible_after) if eligible_after is not None else None,
        deleted_at=_parse_rfc3339_utc(deleted_at) if deleted_at is not None else None,
        cleanup_attempts=int(row["cleanup_attempts"]),
        last_cleanup_error=row["last_cleanup_error"],
    )


def _tracked_attachment_path(*, attachment_uri: str, attachment_root_dir: Path) -> Path | None:
    parsed = urlparse(attachment_uri)
    if parsed.scheme != "file":
        return None
    candidate_path = Path(unquote(parsed.path)).resolve()
    if not _is_relative_to(candidate_path, attachment_root_dir):
        return None
    return candidate_path


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


__all__ = [
    "CompletedTaskRecord",
    "StagedEgressRecord",
    "TaskLedgerRecord",
    "TaskLedgerStore",
    "TelegramAttachmentCleanupResult",
    "TelegramAttachmentRecord",
    "TelegramAttachmentStore",
    "cleanup_telegram_attachments",
]
