"""Message-handler entrypoint: ingress connectors + strict egress dispatch."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable, Mapping

from app.applier import IntegratedApplier
from app.broker import (
    BBMBQueueAdapter,
    EGRESS_QUEUE_NAME,
    EgressQueueMessage,
    TASK_QUEUE_NAME,
    TaskQueueMessage,
)
from app.main import (
    TELEGRAM_MEMORY_TURN_LIMIT,
    _build_email_sender,
    _build_live_connectors,
    _build_telegram_sender,
    _enrich_telegram_envelope_with_memory,
    _should_store_telegram_memory,
)
from app.message_handler_runtime import TaskLedgerStore
from app.models import ConfigUpdateDecision, PolicyDecision, TaskEnvelope
from app.state import SQLiteStateStore

MESSAGE_HANDLER_CONFIG_PATH_ENV_VAR = "CHATTING_MESSAGE_HANDLER_CONFIG_PATH"
LOGGER = logging.getLogger(__name__)
_ALLOWED_CONFIG_KEYS = frozenset(
    {
        "bbmb_address",
        "db_path",
        "max_loops",
        "poll_interval_seconds",
        "poll_timeout_seconds",
        "allowed_egress_channels",
        "schedule_file",
        "imap_host",
        "imap_port",
        "imap_username",
        "imap_password_env",
        "imap_mailbox",
        "imap_search",
        "context_ref",
        "context_refs",
        "smtp_host",
        "smtp_port",
        "smtp_username",
        "smtp_password_env",
        "smtp_from",
        "smtp_starttls",
        "telegram_enabled",
        "telegram_bot_token_env",
        "telegram_api_base_url",
        "telegram_poll_timeout_seconds",
        "telegram_allowed_chat_ids",
        "telegram_allowed_channel_ids",
        "telegram_context_refs",
    }
)


def _configure_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ingress+egress message handler.")
    parser.add_argument("--config", help="Path to JSON config file.")
    parser.add_argument("--db-path", help="Path to message-handler SQLite DB.")
    parser.add_argument("--bbmb-address", help="BBMB address host:port.")
    parser.add_argument("--max-loops", type=_positive_int, help="Optional loop limit for smoke runs.")
    parser.add_argument("--poll-interval-seconds", type=_positive_float, help="Loop interval.")
    parser.add_argument("--poll-timeout-seconds", type=_positive_int, help="Egress pickup timeout seconds.")
    parser.add_argument(
        "--allowed-egress-channel",
        action="append",
        default=[],
        help="Allowed egress channel (repeatable).",
    )
    parser.add_argument("--schedule-file", help="Schedule jobs JSON file.")
    parser.add_argument("--imap-host", help="IMAP host for polling.")
    parser.add_argument("--imap-port", type=_positive_int, help="IMAP port.")
    parser.add_argument("--imap-username", help="IMAP username.")
    parser.add_argument("--imap-password-env", help="IMAP password env var name.")
    parser.add_argument("--imap-mailbox", help="IMAP mailbox.")
    parser.add_argument("--imap-search", help="IMAP search criterion.")
    parser.add_argument("--smtp-host", help="SMTP host for outbound dispatch.")
    parser.add_argument("--smtp-port", type=_positive_int, help="SMTP port.")
    parser.add_argument("--smtp-username", help="SMTP username.")
    parser.add_argument("--smtp-password-env", help="SMTP password env var name.")
    parser.add_argument("--smtp-from", help="SMTP from address.")
    parser.add_argument("--smtp-starttls", action="store_true", help="Use SMTP STARTTLS.")
    parser.add_argument("--telegram-enabled", action="store_true", help="Enable Telegram connector+sender.")
    parser.add_argument("--telegram-bot-token-env", help="Telegram token env var name.")
    parser.add_argument("--telegram-api-base-url", help="Telegram API base URL.")
    parser.add_argument("--telegram-poll-timeout-seconds", type=_positive_int, help="Telegram poll timeout.")
    parser.add_argument(
        "--telegram-allowed-chat-id",
        action="append",
        default=[],
        help="Allowed Telegram inbound chat id (repeatable).",
    )
    parser.add_argument(
        "--telegram-allowed-channel-id",
        action="append",
        default=[],
        help="Allowed Telegram inbound channel id (repeatable).",
    )
    parser.add_argument(
        "--telegram-context-ref",
        action="append",
        default=[],
        help="Telegram context ref (repeatable).",
    )
    parser.add_argument(
        "--context-ref",
        action="append",
        default=[],
        help="Generic context ref (repeatable).",
    )
    return parser.parse_args()


def _load_config(config_path: str | None, environ: Mapping[str, str] | None = None) -> dict[str, object]:
    env = os.environ if environ is None else environ
    path = config_path
    if path is None:
        raw = env.get(MESSAGE_HANDLER_CONFIG_PATH_ENV_VAR)
        if raw is not None:
            if not raw.strip():
                raise ValueError(f"{MESSAGE_HANDLER_CONFIG_PATH_ENV_VAR} must not be empty")
            path = raw

    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("config file must contain a JSON object")
    unknown_keys = sorted(set(payload.keys()) - _ALLOWED_CONFIG_KEYS)
    if unknown_keys:
        raise ValueError("config contains unknown keys: " + ", ".join(unknown_keys))
    return payload


def _resolve_str(cli_value: str | None, config_value: object, *, default_value: str, setting_name: str) -> str:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        return default_value
    if not isinstance(config_value, str) or not config_value.strip():
        raise ValueError(f"config {setting_name} must be a non-empty string")
    return config_value


def _resolve_positive_int(cli_value: int | None, config_value: object, *, default_value: int, setting_name: str) -> int:
    if cli_value is not None:
        return cli_value
    if config_value is None:
        return default_value
    if not isinstance(config_value, int) or isinstance(config_value, bool) or config_value <= 0:
        raise ValueError(f"config {setting_name} must be a positive integer")
    return config_value


def _resolve_positive_float(
    cli_value: float | None,
    config_value: object,
    *,
    default_value: float,
    setting_name: str,
) -> float:
    if cli_value is not None:
        return cli_value
    candidate = default_value if config_value is None else config_value
    if isinstance(candidate, bool) or not isinstance(candidate, (int, float)):
        raise ValueError(f"config {setting_name} must be numeric")
    parsed = float(candidate)
    if parsed <= 0:
        raise ValueError(f"config {setting_name} must be positive")
    return parsed


def _resolve_allowed_egress_channels(args: argparse.Namespace, config: dict[str, object]) -> set[str]:
    config_values: list[str] = []
    raw_config_values = config.get("allowed_egress_channels")
    if raw_config_values is not None:
        if not isinstance(raw_config_values, list) or not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError("config allowed_egress_channels must be a list of strings")
        config_values = list(raw_config_values)

    merged = [*config_values, *args.allowed_egress_channel]
    if not merged:
        return {"email", "telegram", "log"}
    if any(not item.strip() for item in merged):
        raise ValueError("allowed_egress_channel entries must not be empty")
    return set(merged)


def _build_policy_decision_for_message(egress_message: EgressQueueMessage) -> PolicyDecision:
    return PolicyDecision(
        approved_actions=[],
        blocked_actions=[],
        approved_messages=[egress_message.message],
        config_updates=ConfigUpdateDecision(),
        reason_codes=[],
    )


def _prepare_ingress_envelope(
    *,
    store: SQLiteStateStore,
    envelope: TaskEnvelope,
    run_id: str,
) -> TaskEnvelope:
    if not _should_store_telegram_memory(envelope):
        return envelope

    enriched_envelope = _enrich_telegram_envelope_with_memory(
        store=store,
        envelope=envelope,
        turn_limit=TELEGRAM_MEMORY_TURN_LIMIT,
    )
    store.append_conversation_turn(
        channel="telegram",
        target=envelope.reply_channel.target,
        role="user",
        content=envelope.content,
        run_id=run_id,
    )
    return enriched_envelope


def _handle_egress_message(
    *,
    picked_guid: str,
    picked_payload: dict[str, object],
    ledger: TaskLedgerStore,
    store: SQLiteStateStore,
    allowed_egress_channels: set[str],
    applier: IntegratedApplier,
    ack_callback: Callable[[str], None],
) -> None:
    try:
        egress_message = EgressQueueMessage.from_dict(picked_payload)
    except Exception:
        LOGGER.exception("egress_payload_invalid guid=%s", picked_guid)
        ack_callback(picked_guid)
        return

    ledger_record = ledger.get_task(egress_message.task_id)
    if ledger_record is None or ledger_record.envelope_id != egress_message.envelope_id:
        LOGGER.error(
            "egress_drop_unknown_task task_id=%s envelope_id=%s guid=%s",
            egress_message.task_id,
            egress_message.envelope_id,
            picked_guid,
        )
        ack_callback(picked_guid)
        return

    if egress_message.message.channel not in allowed_egress_channels:
        LOGGER.error(
            "egress_drop_disallowed_channel task_id=%s channel=%s guid=%s",
            egress_message.task_id,
            egress_message.message.channel,
            picked_guid,
        )
        ack_callback(picked_guid)
        return

    if egress_message.event_id is None:
        LOGGER.error(
            "egress_drop_missing_event_id task_id=%s guid=%s",
            egress_message.task_id,
            picked_guid,
        )
        ack_callback(picked_guid)
        return

    if store.has_dispatched_event_id(task_id=egress_message.task_id, event_id=egress_message.event_id):
        LOGGER.info(
            "egress_skip_already_dispatched task_id=%s event_id=%s guid=%s",
            egress_message.task_id,
            egress_message.event_id,
            picked_guid,
        )
        ack_callback(picked_guid)
        return

    ledger.stage_egress_event(egress_message)
    ack_callback(picked_guid)
    _flush_task_egress_in_sequence(
        task_id=egress_message.task_id,
        ledger=ledger,
        store=store,
        applier=applier,
    )


def _flush_task_egress_in_sequence(
    *,
    task_id: str,
    ledger: TaskLedgerStore,
    store: SQLiteStateStore,
    applier: IntegratedApplier,
) -> None:
    ledger_record = ledger.get_task(task_id)
    if ledger_record is None:
        return

    while True:
        expected_sequence = ledger.expected_sequence(task_id)
        staged = ledger.get_staged_event_by_sequence(task_id=task_id, sequence=expected_sequence)
        if staged is None:
            return

        egress_message = staged.egress_message
        if store.has_dispatched_event_id(task_id=task_id, event_id=staged.event_id):
            ledger.mark_staged_event_dispatched(
                task_id=task_id,
                event_id=staged.event_id,
                sequence=staged.sequence,
            )
            continue

        decision = _build_policy_decision_for_message(egress_message)
        apply_result = applier.apply(
            decision,
            envelope=ledger_record.task_message.envelope,
        )
        store.mark_dispatched_event_id(
            task_id=task_id,
            event_id=staged.event_id,
        )
        store.mark_dispatched_event(
            run_id=task_id,
            event_index=egress_message.event_index,
        )
        ledger.mark_staged_event_dispatched(
            task_id=task_id,
            event_id=staged.event_id,
            sequence=staged.sequence,
        )
        if _should_store_telegram_memory(ledger_record.task_message.envelope):
            for message in apply_result.dispatched_messages:
                if message.channel != "telegram":
                    continue
                if message.target != ledger_record.task_message.envelope.reply_channel.target:
                    continue
                store.append_conversation_turn(
                    channel="telegram",
                    target=message.target,
                    role="assistant",
                    content=message.body,
                    run_id=task_id,
                )
        LOGGER.info(
            "egress_dispatched task_id=%s event_id=%s sequence=%s event_kind=%s channel=%s",
            egress_message.task_id,
            egress_message.event_id,
            egress_message.sequence,
            egress_message.event_kind,
            egress_message.message.channel,
        )


def main() -> int:
    _configure_logging()
    args = _parse_args()
    config = _load_config(args.config, os.environ)

    db_path = _resolve_str(
        args.db_path,
        config.get("db_path"),
        default_value=str(Path(tempfile.gettempdir()) / "chatting-message-handler-state.db"),
        setting_name="db_path",
    )
    bbmb_address = _resolve_str(
        args.bbmb_address,
        config.get("bbmb_address"),
        default_value="127.0.0.1:9876",
        setting_name="bbmb_address",
    )
    max_loops = _resolve_positive_int(
        args.max_loops,
        config.get("max_loops"),
        default_value=0,
        setting_name="max_loops",
    )
    poll_interval_seconds = _resolve_positive_float(
        args.poll_interval_seconds,
        config.get("poll_interval_seconds"),
        default_value=30.0,
        setting_name="poll_interval_seconds",
    )
    poll_timeout_seconds = _resolve_positive_int(
        args.poll_timeout_seconds,
        config.get("poll_timeout_seconds"),
        default_value=2,
        setting_name="poll_timeout_seconds",
    )
    allowed_egress_channels = _resolve_allowed_egress_channels(args, config)

    store = SQLiteStateStore(db_path)
    ledger = TaskLedgerStore(db_path)
    broker = BBMBQueueAdapter(address=bbmb_address)
    broker.ensure_queue(TASK_QUEUE_NAME)
    broker.ensure_queue(EGRESS_QUEUE_NAME)

    connectors = _build_live_connectors(args, config)
    applier = IntegratedApplier(
        base_dir=".",
        email_sender=_build_email_sender(args, config),
        telegram_sender=_build_telegram_sender(args, config),
    )

    loop_count = 0
    while True:
        loop_count += 1

        ingress_published = 0
        for connector in connectors:
            for envelope in connector.poll():
                if store.seen(envelope.source, envelope.dedupe_key):
                    continue
                task_id = f"task:{envelope.id}"
                task_envelope = _prepare_ingress_envelope(
                    store=store,
                    envelope=envelope,
                    run_id=task_id,
                )
                task_message = TaskQueueMessage.from_envelope(
                    task_envelope,
                    trace_id=f"trace:{envelope.id}",
                )
                broker.publish_json(TASK_QUEUE_NAME, task_message.to_dict())
                ledger.record_task(task_message)
                store.mark_seen(envelope.source, envelope.dedupe_key)
                ingress_published += 1

        egress_picked = broker.pickup_json(EGRESS_QUEUE_NAME, timeout_seconds=poll_timeout_seconds)
        if egress_picked is not None:
            _handle_egress_message(
                picked_guid=egress_picked.guid,
                picked_payload=egress_picked.payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels=allowed_egress_channels,
                applier=applier,
                ack_callback=lambda guid: broker.ack(EGRESS_QUEUE_NAME, guid),
            )

        LOGGER.info(
            "message_handler_loop_completed loop=%s ingress_published=%s",
            loop_count,
            ingress_published,
        )

        if max_loops and loop_count >= max_loops:
            break
        time.sleep(poll_interval_seconds)

    return 0


if __name__ == "__main__":
    sys.exit(main())
