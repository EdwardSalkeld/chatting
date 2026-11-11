"""Inspect prototype routing and register PRs produced by a work item."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request

from app.egress_client import DEFAULT_HANDLER_EGRESS_URL
from app.state import SQLiteStateStore
from app.worker.main import WORKER_CONFIG_PATH_ENV_VAR, _load_config, worker_db_path
from app.worker.pr_registration import register_github_artifact, register_pr


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=f"Without --db, settings come from the config in {WORKER_CONFIG_PATH_ENV_VAR}.",
    )
    parser.add_argument("--db", help="worker SQLite database")
    parser.add_argument("--handler-url", help="handler loopback API base URL")
    commands = parser.add_subparsers(dest="command", required=True)
    show = commands.add_parser("show")
    show.add_argument("task_id")
    executor = commands.add_parser("executor")
    executor.add_argument("task_id")
    executor.add_argument(
        "mode", choices=("exec", "app-server", "show"), default="show", nargs="?"
    )
    finish = commands.add_parser("explicit-finish")
    finish.add_argument("task_id")
    finish.add_argument(
        "mode", choices=("on", "off", "show"), default="show", nargs="?"
    )
    register = commands.add_parser("register-pr")
    register.add_argument("task_id")
    register.add_argument("pr_url")
    for command in ("subject", "repository", "run", "workflow"):
        artifact = commands.add_parser(f"register-{command}")
        artifact.add_argument("task_id")
        artifact.add_argument("value")
    topic = commands.add_parser(
        "ensure-telegram-topic",
        help="Reuse or create and attach a Telegram conversation to this workspace",
    )
    topic.add_argument("task_id")
    topic.add_argument(
        "--name", default="", help="Title for a new topic, 1–128 characters"
    )
    topic.add_argument(
        "--chat-id",
        default="",
        help="Allowed forum group; otherwise use the handler's configured forum",
    )
    topic.add_argument(
        "--topic-id",
        type=int,
        help="Attach a known existing topic, including recovery after an uncertain creation",
    )
    args = parser.parse_args()
    if (
        args.command == "ensure-telegram-topic"
        and args.topic_id is not None
        and args.topic_id <= 0
    ):
        parser.error("--topic-id must be positive")
    config = (
        _load_config(None, os.environ)
        if os.environ.get(WORKER_CONFIG_PATH_ENV_VAR)
        else {}
    )
    db_path = args.db or worker_db_path(config)
    if not db_path:
        parser.error(f"pass --db or set {WORKER_CONFIG_PATH_ENV_VAR}")
    egress_url = config.get("handler_egress_url") or DEFAULT_HANDLER_EGRESS_URL
    handler_url = args.handler_url or str(egress_url).rsplit("/", 1)[0]
    store = SQLiteStateStore(db_path)
    assignment = store.get_work_assignment(task_id=args.task_id)
    if assignment is None:
        parser.error("unknown task")
    if args.command == "ensure-telegram-topic":
        request = urllib.request.Request(
            handler_url.rstrip("/") + "/work-items/telegram-topic",
            data=json.dumps(
                {
                    "task_id": args.task_id,
                    "name": args.name,
                    "chat_id": args.chat_id,
                    "topic_id": args.topic_id or 0,
                }
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                print(json.dumps(json.load(response)))
        except urllib.error.HTTPError as error:
            parser.error(
                f"handler rejected Telegram topic request: {error.read().decode()}"
            )
        except urllib.error.URLError:
            parser.error(
                "handler unavailable; creation outcome may be unknown; rerun ensure to check existing state"
            )
        return 0
    inbox_task = store.get_inbox_task(task_id=args.task_id)
    if args.command == "executor" and args.mode != "show":
        if inbox_task is None:
            parser.error("task has no conversation")
        store.set_conversation_executor(
            conversation_id=inbox_task.conversation_id, backend=args.mode
        )
    if args.command == "explicit-finish" and args.mode != "show":
        store.set_work_explicit_finish(
            work_item_id=assignment.work_item_id, enabled=args.mode == "on"
        )
    if args.command.startswith("register-"):
        try:
            if args.command == "register-pr":
                register_pr(handler_url, task_id=args.task_id, pr_url=args.pr_url)
            else:
                register_github_artifact(
                    handler_url,
                    task_id=args.task_id,
                    kind=args.command.removeprefix("register-"),
                    value=args.value,
                )
        except urllib.error.HTTPError as error:
            parser.error(
                f"handler rejected GitHub registration: {error.read().decode()}"
            )
        except urllib.error.URLError as error:
            parser.error(f"handler unavailable: {error.reason}")
    origin_channel = store.work_origin_channel(work_item_id=assignment.work_item_id)
    print(
        json.dumps(
            {
                "work_item_id": assignment.work_item_id,
                "reason": assignment.reason,
                "conversation_id": inbox_task.conversation_id if inbox_task else None,
                "executor": store.get_conversation_executor(
                    conversation_id=inbox_task.conversation_id
                )
                if inbox_task
                else "exec",
                "explicit_finish": store.get_work_explicit_finish(
                    work_item_id=assignment.work_item_id
                ),
                "origin_channel": {
                    "type": origin_channel.type,
                    "target": origin_channel.target,
                }
                if origin_channel
                else None,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
