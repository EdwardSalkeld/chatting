"""Inspect prototype routing and register PRs produced by a work item."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request

from app.egress_client import DEFAULT_HANDLER_EGRESS_URL
from app.state import SQLiteStateStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="worker SQLite database")
    parser.add_argument(
        "--handler-url",
        default=DEFAULT_HANDLER_EGRESS_URL.rsplit("/", 1)[0],
        help="handler loopback API base URL",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    show = commands.add_parser("show")
    show.add_argument("task_id")
    register = commands.add_parser("register-pr")
    register.add_argument("task_id")
    register.add_argument("pr_url")
    args = parser.parse_args()
    store = SQLiteStateStore(args.db)
    assignment = store.get_work_assignment(task_id=args.task_id)
    if assignment is None:
        parser.error("unknown task")
    if args.command == "register-pr":
        request = urllib.request.Request(
            args.handler_url.rstrip("/") + "/work-items/register-pr",
            data=json.dumps({"task_id": args.task_id, "pr_url": args.pr_url}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10):
                pass
        except urllib.error.HTTPError as error:
            parser.error(f"handler rejected PR registration: {error.read().decode()}")
        except urllib.error.URLError as error:
            parser.error(f"handler unavailable: {error.reason}")
    preferred_reply = store.preferred_work_reply(work_item_id=assignment.work_item_id)
    print(
        json.dumps(
            {
                "work_item_id": assignment.work_item_id,
                "reason": assignment.reason,
                "preferred_reply": {
                    "type": preferred_reply.type,
                    "target": preferred_reply.target,
                }
                if preferred_reply
                else None,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
