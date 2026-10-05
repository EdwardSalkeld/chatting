"""Worker-owned conversation history lookup CLI."""

from __future__ import annotations

import argparse
import json
import os
import sys

from datetime import datetime, time, timezone

from app.state import SQLiteStateStore
from app.worker.main import WORKER_CONFIG_PATH_ENV_VAR, _load_config, _resolve_str


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search or retrieve worker-owned Telegram conversation history."
    )
    parser.add_argument("--channel", required=True, choices=("telegram",))
    parser.add_argument("--target", required=True, help="Telegram chat id.")
    parser.add_argument(
        "--topic-id", type=_positive_int, help="Telegram forum topic id."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--around-message-id", type=_positive_int)
    mode.add_argument("--query", help="Words to find in a message (all must match).")
    parser.add_argument("--before", type=_non_negative_int, default=12)
    parser.add_argument("--after", type=_non_negative_int, default=12)
    parser.add_argument("--sender", help="Filter search by sender.")
    parser.add_argument("--since", help="Search from this ISO 8601 date/time.")
    parser.add_argument("--until", help="Search through this ISO 8601 date/time.")
    parser.add_argument("--limit", type=_positive_int, default=20)
    parser.add_argument("--config", help="Path to worker config JSON.")
    parser.add_argument("--db-path", help="Worker SQLite state DB override.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    config = _load_config(args.config, os.environ)
    db_path = _resolve_str(
        args.db_path,
        config.get("db_path"),
        default_value="",
        setting_name="db_path",
    ).strip()
    if not db_path:
        raise ValueError(
            "db_path is required via --db-path, --config, or "
            f"{WORKER_CONFIG_PATH_ENV_VAR}"
        )
    store = SQLiteStateStore(db_path)
    if args.query is not None:
        turns = store.search_telegram_history(
            target=args.target,
            topic_id=args.topic_id,
            query=args.query,
            sender=args.sender,
            since=_parse_date(args.since),
            until=_parse_date(args.until, end_of_day=True),
            limit=args.limit,
        )
    else:
        turns = store.list_telegram_history_around(
            target=args.target,
            topic_id=args.topic_id,
            message_id=args.around_message_id,
            before=args.before,
            after=args.after,
        )
    print(
        json.dumps(
            {
                "channel": args.channel,
                "target": args.target,
                "topic_id": args.topic_id,
                "anchor_message_id": args.around_message_id,
                "anchor_found": bool(turns) if args.query is None else None,
                "query": args.query,
                "turns": [
                    {
                        "message_id": turn.message_id,
                        "is_anchor": turn.message_id == args.around_message_id,
                        "topic_id": turn.topic_id,
                        "reply_to_message_id": turn.reply_to_message_id,
                        "role": turn.role,
                        "sender": turn.sender,
                        "occurred_at": turn.occurred_at.isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "content": _excerpt(turn.content, args.query),
                        "attachments": [
                            {"uri": item.uri, "name": item.name}
                            for item in turn.attachments
                        ],
                    }
                    for turn in turns
                ],
            },
            sort_keys=True,
        )
    )
    return 0


def _parse_date(value: str | None, *, end_of_day: bool = False) -> datetime | None:
    if value is None:
        return None
    if len(value) == 10:
        day = datetime.fromisoformat(value).date()
        return datetime.combine(
            day, time.max if end_of_day else time.min, tzinfo=timezone.utc
        )
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("date filters must include a timezone")
    return parsed


def _excerpt(content: str | None, query: str | None) -> str | None:
    if content is None or query is None or len(content) <= 240:
        return content
    match = content.lower().find(query.split()[0].lower())
    start = max(0, match - 80) if match >= 0 else 0
    end = min(len(content), start + 240)
    return (
        ("…" if start else "")
        + content[start:end]
        + ("…" if end < len(content) else "")
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(2)
