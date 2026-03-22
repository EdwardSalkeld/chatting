"""Worker-side CLI to publish visible egress messages."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from app.broker import BBMBQueueAdapter, EGRESS_QUEUE_NAME, EgressQueueMessage
from app.message_handler_runtime import TaskLedgerStore
from app.main_worker import WORKER_CONFIG_PATH_ENV_VAR, _load_config, _resolve_str
from app.models import AttachmentRef, OutboundMessage


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish one visible egress message. "
            "Executors should use this for user-visible acknowledgements and final replies."
        )
    )
    parser.add_argument("task_id", help="Task identifier (for example: task:email:53).")
    parser.add_argument("--message", help="Reply body text.")
    parser.add_argument("--attachment-path", help="Absolute path to a local file to send as an attachment.")
    parser.add_argument("--attachment-name", help="Optional attachment filename override.")
    parser.add_argument("--channel", required=True, help="Outbound channel (for example: email, telegram, github).")
    parser.add_argument("--target", required=True, help="Outbound channel target.")
    parser.add_argument("--telegram-reaction", help="React to the Telegram source message instead of sending a text message.")
    parser.add_argument("--telegram-message-id", type=int, help="Telegram message id to react to.")
    parser.add_argument("--event-id", help="Optional stable event id for idempotency.")
    parser.add_argument("--envelope-id", help="Envelope id (defaults to task_id without 'task:' prefix).")
    parser.add_argument("--trace-id", help="Trace id (defaults to trace:<envelope_id>).")
    parser.add_argument("--bbmb-address", help="BBMB broker address host:port.")
    parser.add_argument(
        "--config",
        help=(
            "Path to worker config JSON. If omitted, this command also checks "
            f"{WORKER_CONFIG_PATH_ENV_VAR}."
        ),
    )
    return parser.parse_args()


def _resolve_envelope_id(task_id: str, explicit_value: str | None) -> str:
    if explicit_value is not None:
        if not explicit_value.strip():
            raise ValueError("envelope_id must not be empty")
        return explicit_value
    if not task_id.startswith("task:"):
        raise ValueError("envelope_id is required when task_id does not start with 'task:'")
    return task_id[len("task:") :]


def _resolve_trace_id(envelope_id: str, explicit_value: str | None) -> str:
    if explicit_value is not None:
        if not explicit_value.strip():
            raise ValueError("trace_id must not be empty")
        return explicit_value
    return f"trace:{envelope_id}"


def _resolve_event_id(task_id: str, explicit_value: str | None) -> str:
    if explicit_value is not None:
        if not explicit_value.strip():
            raise ValueError("event_id must not be empty")
        return explicit_value
    return f"evt:{task_id}:adhoc:{time.time_ns()}"


def _resolve_telegram_message_id(
    *,
    task_id: str,
    explicit_value: int | None,
    config: dict[str, object],
) -> int:
    if explicit_value is not None:
        if explicit_value <= 0:
            raise ValueError("telegram_message_id must be positive")
        return explicit_value

    db_path = _resolve_str(
        None,
        config.get("db_path"),
        default_value="",
        setting_name="db_path",
    ).strip()
    if not db_path:
        raise ValueError("telegram_message_id is required")

    ledger_record = TaskLedgerStore(db_path).get_task(task_id)
    if ledger_record is None:
        raise ValueError("telegram_message_id is required")

    message_id = ledger_record.task_message.envelope.reply_channel.metadata.get("message_id")
    if isinstance(message_id, int) and message_id > 0:
        return message_id

    raise ValueError("telegram_message_id is required")


def _resolve_reply_message(args: argparse.Namespace, config: dict[str, object]) -> OutboundMessage:
    if args.telegram_reaction is not None:
        if args.channel != "telegram":
            raise ValueError("telegram reactions require --channel telegram")
        emoji = args.telegram_reaction.strip()
        if not emoji:
            raise ValueError("telegram_reaction must not be empty")
        message_id = _resolve_telegram_message_id(
            task_id=args.task_id,
            explicit_value=args.telegram_message_id,
            config=config,
        )
        return OutboundMessage(
            channel="telegram_reaction",
            target=args.target,
            body=emoji,
            metadata={"message_id": message_id},
        )

    attachment: AttachmentRef | None = None
    if args.attachment_path is not None:
        attachment_path = Path(args.attachment_path.strip())
        if not attachment_path.is_absolute():
            raise ValueError("attachment_path must be absolute")
        if not attachment_path.is_file():
            raise ValueError("attachment_path must point to an existing file")

        attachment_name = None
        if args.attachment_name is not None:
            attachment_name = args.attachment_name.strip()
            if not attachment_name:
                raise ValueError("attachment_name must not be empty")

        attachment = AttachmentRef(
            uri=attachment_path.as_uri(),
            name=attachment_name,
        )

    body: str | None = None
    if args.message is not None:
        body = args.message.strip()
        if not body:
            raise ValueError("message must not be empty")

    if body is None and attachment is None:
        raise ValueError("message or attachment is required")

    return OutboundMessage(channel=args.channel, target=args.target, body=body, attachment=attachment)


def main() -> int:
    args = _parse_args()
    config = _load_config(args.config, os.environ)

    bbmb_address = _resolve_str(
        args.bbmb_address,
        config.get("bbmb_address"),
        default_value="127.0.0.1:9876",
        setting_name="bbmb_address",
    )

    envelope_id = _resolve_envelope_id(args.task_id, args.envelope_id)
    trace_id = _resolve_trace_id(envelope_id, args.trace_id)
    event_id = _resolve_event_id(args.task_id, args.event_id)

    outbound_message = _resolve_reply_message(args, config)

    egress_message = EgressQueueMessage(
        task_id=args.task_id,
        envelope_id=envelope_id,
        trace_id=trace_id,
        event_index=0,
        event_count=1,
        message=outbound_message,
        emitted_at=datetime.now(timezone.utc),
        event_id=event_id,
        sequence=None,
        event_kind="incremental",
        message_type="chatting.egress.v2",
    )

    broker = BBMBQueueAdapter(address=bbmb_address)
    broker.ensure_queue(EGRESS_QUEUE_NAME)
    guid = broker.publish_json(EGRESS_QUEUE_NAME, egress_message.to_dict())

    print(
        json.dumps(
            {
                "status": "published",
                "queue": EGRESS_QUEUE_NAME,
                "guid": guid,
                "task_id": egress_message.task_id,
                "event_id": egress_message.event_id,
                "event_kind": egress_message.event_kind,
                "sequence": egress_message.sequence,
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
