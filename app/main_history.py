"""Worker-owned conversation history lookup CLI."""

from __future__ import annotations

import argparse
import json
import os
import sys

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
        description="Retrieve worker-owned history around a transport message id."
    )
    parser.add_argument("--channel", required=True, choices=("telegram",))
    parser.add_argument("--target", required=True, help="Telegram chat id.")
    parser.add_argument(
        "--around-message-id", required=True, type=_positive_int
    )
    parser.add_argument("--before", type=_non_negative_int, default=12)
    parser.add_argument("--after", type=_non_negative_int, default=12)
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
    turns = SQLiteStateStore(db_path).list_telegram_history_around(
        target=args.target,
        message_id=args.around_message_id,
        before=args.before,
        after=args.after,
    )
    print(
        json.dumps(
            {
                "channel": args.channel,
                "target": args.target,
                "anchor_message_id": args.around_message_id,
                "anchor_found": bool(turns),
                "turns": [
                    {
                        "message_id": turn.message_id,
                        "is_anchor": turn.message_id == args.around_message_id,
                        "reply_to_message_id": turn.reply_to_message_id,
                        "role": turn.role,
                        "sender": turn.sender,
                        "occurred_at": turn.occurred_at.isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "content": turn.content,
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


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(2)
