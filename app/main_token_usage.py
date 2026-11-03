"""Backfill or export the worker's model-response token ledger."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone

from app.state import token_usage
from app.timefmt import format_utc


def backfill(connection: sqlite3.Connection, *, since: str) -> int:
    """Replay retained events in small transactions; repeated runs are safe."""
    # Historical metadata must never rewind a running attempt's live context.
    # A connection-local table shadows the persistent projection state.
    connection.execute("""CREATE TEMP TABLE IF NOT EXISTS model_usage_context (
        task_id TEXT PRIMARY KEY, attempt INTEGER, model TEXT, model_source TEXT
    )""")
    connection.execute("DELETE FROM temp.model_usage_context")
    before = connection.execute("SELECT count(*) FROM model_token_usage").fetchone()[0]
    high_water = connection.execute(
        "SELECT COALESCE(max(activity_id), 0) FROM worker_activity_events"
    ).fetchone()[0]
    after_id = 0
    while True:
        rows = connection.execute(
            """SELECT * FROM worker_activity_events
               WHERE activity_id > ? AND activity_id <= ? AND occurred_at >= ?
               AND phase IN ('executor_started', 'executor_stdout', 'task_finished')
               ORDER BY activity_id LIMIT 500""",
            (after_id, high_water, since),
        ).fetchall()
        if not rows:
            break
        with connection:
            for event in rows:
                token_usage.project(connection, event)
        after_id = rows[-1]["activity_id"]
    connection.execute("DROP TABLE temp.model_usage_context")
    return (
        connection.execute("SELECT count(*) FROM model_token_usage").fetchone()[0]
        - before
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["backfill", "export"])
    parser.add_argument("--db", required=True, help="worker SQLite database path")
    parser.add_argument(
        "--days", type=int, default=7, choices=range(1, 15), metavar="1..14"
    )
    args = parser.parse_args()
    since = format_utc(datetime.now(timezone.utc) - timedelta(days=args.days))
    # Export never initializes or modifies a database.
    uri = f"file:{args.db}?mode={'rw' if args.command == 'backfill' else 'ro'}"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        if args.command == "backfill":
            token_usage.initialize(connection)
            added = backfill(connection, since=since)
            print(json.dumps({"since": since, "inserted": added}))
        else:
            for row in connection.execute(
                "SELECT * FROM model_token_usage WHERE occurred_at >= ? ORDER BY activity_id",
                (since,),
            ):
                print(json.dumps(dict(row)))


if __name__ == "__main__":
    main()
