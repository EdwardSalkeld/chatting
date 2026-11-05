"""Remove duplicate audit transcripts or expire worker run diagnostics."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.timefmt import format_utc


def strip_audit_transcripts(
    connection: sqlite3.Connection, *, batch_size: int = 100
) -> int:
    """Remove only persisted stdout/stderr; restartable with bounded write locks."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    high_water = connection.execute(
        "SELECT COALESCE(max(event_id), 0) FROM audit_events"
    ).fetchone()[0]
    after_id = 0
    changed = 0
    while after_id < high_water:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            ids = connection.execute(
                """SELECT event_id FROM audit_events
                   WHERE event_id > ? AND event_id <= ?
                   ORDER BY event_id LIMIT ?""",
                (after_id, high_water, batch_size),
            ).fetchall()
            if not ids:
                break
            last_id = ids[-1][0]
            changed += connection.execute(
                """UPDATE audit_events
                   SET detail_json = json_remove(detail_json,
                       '$.execution_result.stdout', '$.execution_result.stderr')
                   WHERE event_id > ? AND event_id <= ? AND (
                       json_type(detail_json, '$.execution_result.stdout') IS NOT NULL
                       OR json_type(detail_json, '$.execution_result.stderr') IS NOT NULL
                   )""",
                (after_id, last_id),
            ).rowcount
        after_id = last_id
    return changed


def prune_worker_diagnostics(
    connection: sqlite3.Connection,
    *,
    retention_days: int,
    now: datetime | None = None,
    batch_size: int = 100,
) -> dict[str, int]:
    """Expire diagnostic rows, protecting all unfinished inbox tasks.

    Run records are terminal outcomes. Delete these before their audit task links;
    every batch rechecks inbox state under the same lock as the deletion.
    Delivery, deduplication, conversations, token accounting and workspaces have
    independent lifetimes and are deliberately outside this operation.
    """
    if retention_days <= 0 or batch_size <= 0:
        raise ValueError("retention_days and batch_size must be positive")
    cutoff = format_utc(
        (now or datetime.now(timezone.utc)) - timedelta(days=retention_days)
    )
    predicates = {
        "run_records": """julianday(created_at) < julianday(?) AND NOT EXISTS (
            SELECT 1 FROM audit_events AS audit
            JOIN worker_inbox AS inbox
              ON inbox.task_id = json_extract(audit.detail_json, '$.task_id')
            WHERE audit.run_id = run_records.run_id AND inbox.state != 'completed'
        )""",
        "audit_events": """julianday(created_at) < julianday(?) AND NOT EXISTS (
            SELECT 1 FROM worker_inbox AS inbox
            WHERE inbox.task_id = json_extract(audit_events.detail_json, '$.task_id')
              AND inbox.state != 'completed'
        )""",
        "worker_activity_events": """julianday(occurred_at) < julianday(?)
            AND NOT EXISTS (SELECT 1 FROM worker_inbox AS inbox
                WHERE inbox.task_id = worker_activity_events.task_id
                  AND inbox.state != 'completed')""",
    }
    counts = {}
    for table, predicate in predicates.items():
        deleted = 0
        while True:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                count = connection.execute(
                    f"DELETE FROM {table} WHERE rowid IN "
                    f"(SELECT rowid FROM {table} WHERE {predicate} LIMIT ?)",
                    (cutoff, batch_size),
                ).rowcount
            deleted += count
            if count < batch_size:
                break
        counts[table] = deleted
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["strip-audit-transcripts", "prune-worker"])
    parser.add_argument("--db", required=True, help="existing worker SQLite database")
    parser.add_argument("--retention-days", type=int, default=14)
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()
    if args.retention_days <= 0 or args.batch_size <= 0:
        parser.error("retention-days and batch-size must be positive")
    # Never create or initialize a DB because a maintenance path was mistyped.
    uri = Path(args.db).resolve().as_uri() + "?mode=rw"
    connection = sqlite3.connect(uri, uri=True, timeout=60)
    try:
        if args.command == "strip-audit-transcripts":
            result = {
                "audit_transcripts_removed": strip_audit_transcripts(
                    connection, batch_size=args.batch_size
                )
            }
        else:
            result = prune_worker_diagnostics(
                connection,
                retention_days=args.retention_days,
                batch_size=args.batch_size,
            )
        print(json.dumps(result, sort_keys=True))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
