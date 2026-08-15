"""Worker-side CLI to publish visible egress messages."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from app.broker import EgressQueueMessage
from app.egress_client import DEFAULT_HANDLER_EGRESS_URL, submit_egress
from app.models import AttachmentRef, OutboundMessage
from app.state import SQLiteStateStore
from app.task_ledger import TaskLedgerStore
from app.telegram_text import normalize_telegram_outbound_text
from app.worker.main import WORKER_CONFIG_PATH_ENV_VAR, _load_config, _resolve_str

# Exit codes the executor can act on. 0 = delivered; DROPPED = the handler
# rejected the message for good (bad payload, unknown task, disallowed channel,
# dispatch failure such as a missing attachment) so the caller should fall back
# rather than retry the same send; TRANSIENT = the handler was unreachable or
# returned a server error, so a retry may succeed. (2 stays the usage/validation
# error from the ValueError path.)
EXIT_DROPPED = 1
EXIT_TRANSIENT = 3

# Telegram's Bot API accepts only these values for ReactionTypeEmoji. Keep the
# validation here, at the executor boundary, so an unsupported Unicode emoji is
# rejected locally instead of becoming handler alert noise.
# Source: https://core.telegram.org/bots/api#reactiontypeemoji
TELEGRAM_STANDARD_REACTION_EMOJI = tuple(
    "❤ 👍 👎 🔥 🥰 👏 😁 🤔 🤯 😱 🤬 😢 🎉 🤩 🤮 💩 🙏 👌 🕊 🤡 🥱 🥴 😍 🐳 "
    "❤‍🔥 🌚 🌭 💯 🤣 ⚡ 🍌 🏆 💔 🤨 😐 🍓 🍾 💋 🖕 😈 😴 😭 🤓 👻 👨‍💻 👀 🎃 "
    "🙈 😇 😨 🤝 ✍ 🤗 🫡 🎅 🎄 ☃ 💅 🤪 🗿 🆒 💘 🙉 🦄 😘 💊 🙊 😎 👾 🤷‍♂ "
    "🤷 🤷‍♀ 😡".split()
)


# Reply fields that a --spec-file JSON may set. These map 1:1 onto the argparse
# namespace attributes below so the rest of the module is agnostic to whether a
# reply came from a spec file or from flags.
_SPEC_FIELDS = (
    "task_id",
    "channel",
    "target",
    "message",
    "attachment_path",
    "attachment_name",
    "telegram_reaction",
    "telegram_message_id",
    "event_id",
    "envelope_id",
    "trace_id",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish visible egress. "
            "Executors should use this for user-visible acknowledgements and final replies. "
            "A Telegram reaction may be combined with a message or attachment; both are sent. "
            "Prefer --spec-file: it carries the whole reply (including the body) as a JSON "
            "file so nothing is passed through the shell."
        )
    )
    # Reply body text is deliberately NOT a flag: passing arbitrary prose on the
    # command line let the shell mangle backticks/quotes/$ and corrupt or split
    # replies. The body only ever comes from --spec-file (a file the executor
    # writes directly, never through the shell).
    parser.add_argument(
        "--spec-file",
        help=(
            "Path to a JSON file describing the whole reply "
            f"({', '.join(_SPEC_FIELDS)}). The only way to send a text body."
        ),
    )
    # task_id/channel/target may come from the spec file, so they are optional at
    # the argparse layer and validated after the spec is applied.
    parser.add_argument(
        "task_id", nargs="?", help="Task identifier (for example: task:email:53)."
    )
    parser.add_argument(
        "--attachment-path",
        help="Absolute path to a local file to send as an attachment.",
    )
    parser.add_argument(
        "--attachment-name", help="Optional attachment filename override."
    )
    parser.add_argument(
        "--channel",
        help="Outbound channel (for example: email, telegram, github).",
    )
    parser.add_argument("--target", help="Outbound channel target.")
    parser.add_argument(
        "--telegram-reaction",
        help="React to the Telegram source message instead of sending a text message.",
    )
    parser.add_argument(
        "--telegram-message-id", type=int, help="Telegram message id to react to."
    )
    parser.add_argument("--event-id", help="Optional stable event id for idempotency.")
    parser.add_argument(
        "--envelope-id",
        help="Envelope id (defaults to task_id without 'task:' prefix).",
    )
    parser.add_argument(
        "--trace-id", help="Trace id (defaults to trace:<envelope_id>)."
    )
    parser.add_argument(
        "--handler-egress-url",
        help="Handler synchronous egress endpoint URL (default the worker config's handler_egress_url).",
    )
    parser.add_argument(
        "--config",
        help=(
            "Path to worker config JSON. If omitted, this command also checks "
            f"{WORKER_CONFIG_PATH_ENV_VAR}."
        ),
    )
    args = parser.parse_args()
    # message only exists via the spec file; give the namespace the attribute so
    # the resolution code below can read it uniformly.
    args.message = None
    _apply_spec_file(args)
    if not args.task_id:
        parser.error("task_id is required (as a positional argument or in --spec-file)")
    if not args.channel:
        parser.error("channel is required (via --channel or in --spec-file)")
    if not args.target:
        parser.error("target is required (via --target or in --spec-file)")
    return args


def _apply_spec_file(args: argparse.Namespace) -> None:
    if args.spec_file is None:
        return
    try:
        raw = Path(args.spec_file).read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"spec-file could not be read: {error}") from error
    try:
        spec = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"spec-file is not valid JSON: {error}") from error
    if not isinstance(spec, dict):
        raise ValueError("spec-file must contain a JSON object")
    unknown = sorted(set(spec) - set(_SPEC_FIELDS))
    if unknown:
        raise ValueError("spec-file contains unknown fields: " + ", ".join(unknown))
    # The spec file is authoritative: it supplies the whole reply, so it wins
    # over any overlapping flags (there should be none in normal use).
    for field in _SPEC_FIELDS:
        if field in spec and spec[field] is not None:
            setattr(args, field, spec[field])
    if isinstance(args.telegram_message_id, str) and args.telegram_message_id.strip():
        args.telegram_message_id = int(args.telegram_message_id)


def _resolve_envelope_id(task_id: str, explicit_value: str | None) -> str:
    if explicit_value is not None:
        if not explicit_value.strip():
            raise ValueError("envelope_id must not be empty")
        return explicit_value
    if not task_id.startswith("task:"):
        raise ValueError(
            "envelope_id is required when task_id does not start with 'task:'"
        )
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

    message_id = ledger_record.task_message.envelope.reply_channel.metadata.get(
        "message_id"
    )
    if isinstance(message_id, int) and message_id > 0:
        return message_id

    raise ValueError("telegram_message_id is required")


def _resolve_reply_messages(
    args: argparse.Namespace, config: dict[str, object]
) -> list[OutboundMessage]:
    messages: list[OutboundMessage] = []
    if args.telegram_reaction is not None:
        if args.channel != "telegram":
            raise ValueError("telegram reactions require --channel telegram")
        emoji = args.telegram_reaction.strip()
        if not emoji:
            raise ValueError("telegram_reaction must not be empty")
        if emoji not in TELEGRAM_STANDARD_REACTION_EMOJI:
            raise ValueError(
                f"telegram_reaction {emoji!r} is not supported by Telegram; "
                "choose one of: " + " ".join(TELEGRAM_STANDARD_REACTION_EMOJI)
            )
        message_id = _resolve_telegram_message_id(
            task_id=args.task_id,
            explicit_value=args.telegram_message_id,
            config=config,
        )
        messages.append(
            OutboundMessage(
                channel="telegram_reaction",
                target=args.target,
                body=emoji,
                metadata={"message_id": message_id},
            )
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
        body = normalize_telegram_outbound_text(args.message).strip()
        if not body and not messages and attachment is None:
            raise ValueError("message must not be empty")
        if not body:
            body = None

    if body is not None or attachment is not None:
        messages.append(
            OutboundMessage(
                channel=args.channel,
                target=args.target,
                body=body,
                attachment=attachment,
            )
        )

    if not messages:
        raise ValueError("message or attachment is required")

    return messages


def _message_event_id(
    base_event_id: str, message: OutboundMessage, *, message_count: int
) -> str:
    if message_count == 1:
        return base_event_id
    suffix = "reaction" if message.channel == "telegram_reaction" else "message"
    return f"{base_event_id}:{suffix}"


def main() -> int:
    args = _parse_args()
    config = _load_config(args.config, os.environ)

    handler_egress_url = _resolve_str(
        args.handler_egress_url,
        config.get("handler_egress_url"),
        default_value=DEFAULT_HANDLER_EGRESS_URL,
        setting_name="handler_egress_url",
    )

    envelope_id = _resolve_envelope_id(args.task_id, args.envelope_id)
    trace_id = _resolve_trace_id(envelope_id, args.trace_id)
    base_event_id = _resolve_event_id(args.task_id, args.event_id)

    outbound_messages = _resolve_reply_messages(args, config)
    db_path = _resolve_optional_db_path(config)
    results: list[dict[str, object]] = []
    all_delivered = True
    has_transient_failure = False

    for outbound_message in outbound_messages:
        egress_message = EgressQueueMessage(
            task_id=args.task_id,
            envelope_id=envelope_id,
            trace_id=trace_id,
            event_index=0,
            event_count=1,
            message=outbound_message,
            emitted_at=datetime.now(timezone.utc),
            event_id=_message_event_id(
                base_event_id,
                outbound_message,
                message_count=len(outbound_messages),
            ),
            sequence=None,
            event_kind="incremental",
            message_type="chatting.egress.v2",
        )

        status_code, response = submit_egress(
            handler_egress_url, egress_message.to_dict()
        )
        result_status = (
            str(response.get("status", "")) if isinstance(response, dict) else ""
        )
        reason = str(response.get("reason", "")) if isinstance(response, dict) else ""
        delivered = status_code in (200, 202)
        all_delivered = all_delivered and delivered
        has_transient_failure = has_transient_failure or (
            not delivered and status_code != 422
        )

        if delivered and db_path is not None:
            # Only record the activity event on confirmed delivery: the supervised
            # reply-recovery loop counts these to decide whether visible egress was
            # actually sent, so a dropped send must not look like a success.
            SQLiteStateStore(db_path).append_worker_activity(
                occurred_at=egress_message.emitted_at,
                task_id=egress_message.task_id,
                envelope_id=egress_message.envelope_id,
                phase=f"egress_{egress_message.event_kind}",
                summary=(
                    f"{egress_message.event_kind} egress to "
                    f"{egress_message.message.channel}"
                ),
                detail={
                    "channel": egress_message.message.channel,
                    "target": egress_message.message.target,
                    "body": egress_message.message.body,
                    "event_id": egress_message.event_id,
                    "event_kind": egress_message.event_kind,
                    "event_count": egress_message.event_count,
                    "event_index": egress_message.event_index,
                    "message_type": egress_message.message_type,
                    "publish_source": "main_reply",
                    "sequence": egress_message.sequence,
                    "result_status": result_status,
                },
                is_internal=egress_message.message.channel in {"internal", "log"},
            )

        results.append(
            {
                "status": result_status or ("dispatched" if delivered else "error"),
                "http_status": status_code,
                "task_id": egress_message.task_id,
                "event_id": egress_message.event_id,
                "event_kind": egress_message.event_kind,
                "channel": egress_message.message.channel,
                "sequence": egress_message.sequence,
                "reason": reason,
            }
        )

    payload = results[0] if len(results) == 1 else {"results": results}
    if all_delivered:
        print(json.dumps(payload, sort_keys=True))
        return 0

    # Not fully delivered: surface loudly on stderr so the executor transcript
    # shows every outcome. Both sends are attempted even if the first one fails.
    print(json.dumps(payload, sort_keys=True), file=sys.stderr)
    if has_transient_failure:
        return EXIT_TRANSIENT
    return EXIT_DROPPED


def _resolve_optional_db_path(config: dict[str, object]) -> str | None:
    raw_db_path = config.get("db_path")
    if raw_db_path is None:
        return None
    if not isinstance(raw_db_path, str) or not raw_db_path.strip():
        raise ValueError("config db_path must be a non-empty string")
    return raw_db_path


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(2)
