"""Inspect prototype routing and register PRs produced by a work item."""

from __future__ import annotations

import argparse
import json

from app.state import SQLiteStateStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="worker SQLite database")
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
        if assignment.work_item_id is None:
            parser.error("ambiguous task must be resolved before registering a PR")
        store.register_work_artifact(
            work_item_id=assignment.work_item_id, kind="github_pr", key=args.pr_url
        )
    preferred_reply = (
        store.preferred_work_reply(work_item_id=assignment.work_item_id)
        if assignment.work_item_id
        else None
    )
    print(
        json.dumps(
            {
                "work_item_id": assignment.work_item_id,
                "workspace_id": assignment.workspace_id,
                "reason": assignment.reason,
                "candidates": assignment.candidates,
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
