"""Remove duplicate audit transcripts or expire worker run diagnostics."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from app.state.maintenance import prune_worker_diagnostics, strip_audit_transcripts


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
