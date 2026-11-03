"""Evaluate labelled JSONL examples without broker access or agent execution."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import tempfile
from pathlib import Path

from app.broker import TaskQueueMessage
from app.worker.notification_triage import NotificationTriage


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        required=True,
        help="JSONL: task (queue payload), expected_forward (boolean)",
    )
    parser.add_argument("--rules-repo", required=True)
    args = parser.parse_args()
    key = os.environ.get("TYPESAFE_API_KEY", "")
    if not key:
        parser.error("TYPESAFE_API_KEY is required")
    mismatches = 0
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "replay.db")
        triage = NotificationTriage(path, repo=args.rules_repo, key=key, mode="enforce")
        with Path(args.input).open() as source:
            for line in source:
                example = json.loads(line)
                if type(example["expected_forward"]) is not bool:
                    raise ValueError("expected_forward must be boolean")
                task = TaskQueueMessage.from_dict(example["task"])
                forward = triage.forward(task)
                matches = forward == example["expected_forward"]
                with sqlite3.connect(path) as db:
                    row = db.execute(
                        "SELECT detail FROM decisions WHERE task_id=?", (task.task_id,)
                    ).fetchone()
                detail = json.loads(row[0]) if row else None
                assessed = detail is not None and "probability" in detail
                mismatches += not matches or not assessed
                print(
                    json.dumps(
                        {
                            "task_id": task.task_id,
                            "forward": forward,
                            "matches": matches,
                            "assessed": assessed,
                            "decision": detail,
                        }
                    )
                )
    return int(mismatches > 0)


if __name__ == "__main__":
    raise SystemExit(main())
