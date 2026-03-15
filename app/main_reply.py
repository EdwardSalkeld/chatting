"""Worker-side CLI to publish immediate incremental egress messages."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from app.broker import BBMBQueueAdapter, EGRESS_QUEUE_NAME, EgressQueueMessage
from app.main_worker import WORKER_CONFIG_PATH_ENV_VAR, _load_config, _resolve_str
from app.models import OutboundMessage


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish one immediate incremental reply message.")
    parser.add_argument("task_id", help="Task identifier (for example: task:email:53).")
    parser.add_argument("--message", required=True, help="Reply body text.")
    parser.add_argument("--channel", required=True, help="Outbound channel (for example: email, telegram, github).")
    parser.add_argument("--target", required=True, help="Outbound channel target.")
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

    body = args.message.strip()
    if not body:
        raise ValueError("message must not be empty")

    egress_message = EgressQueueMessage(
        task_id=args.task_id,
        envelope_id=envelope_id,
        trace_id=trace_id,
        event_index=0,
        event_count=1,
        message=OutboundMessage(channel=args.channel, target=args.target, body=body),
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
