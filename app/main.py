"""Shared split-mode helpers plus the admin/query CLI compatibility entrypoint."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from app.applier import (
    IntegratedApplier,
    MessageDispatchError,
    NoOpApplier,
    SmtpEmailSender,
    TelegramMessageSender,
)
from app.connectors import (
    Connector,
    ImapEmailConnector,
    IntervalScheduleConnector,
    IntervalScheduleJob,
    TelegramConnector,
)
from app.executor import EXECUTION_RESULT_JSON_SCHEMA, CodexExecutor, Executor
from app.models import AuditEvent, DeadLetterRecord, OutboundMessage, PolicyDecision, RunRecord, TaskEnvelope
from app.policy import AllowlistPolicyEngine
from app.router import RuleBasedRouter
from app.state import SQLiteStateStore, StateStore

CONFIG_PATH_ENV_VAR = "CHATTING_CONFIG_PATH"
ALLOWED_RUNTIME_CONFIG_KEYS = frozenset(
    {
        "base_dir",
        "claude_command",
        "codex_command",
        "context_ref",
        "context_refs",
        "db_path",
        "imap_host",
        "imap_mailbox",
        "imap_password_env",
        "imap_port",
        "imap_search",
        "imap_use_ssl",
        "imap_username",
        "max_attempts",
        "max_loops",
        "poll_interval_seconds",
        "schedule_file",
        "error_notify_email",
        "smtp_from",
        "smtp_host",
        "smtp_password_env",
        "smtp_port",
        "smtp_starttls",
        "smtp_use_ssl",
        "smtp_username",
        "telegram_allowed_chat_ids",
        "telegram_allowed_channel_ids",
        "telegram_attachment_dir",
        "telegram_attachment_cleanup_grace_seconds",
        "telegram_attachment_max_age_seconds",
        "telegram_api_base_url",
        "telegram_bot_token_env",
        "telegram_context_refs",
        "telegram_enabled",
        "telegram_poll_timeout_seconds",
        "worker_count",
    }
)
ALLOWED_SCHEDULE_JOB_KEYS = frozenset(
    {
        "content",
        "cron",
        "context_refs",
        "interval_seconds",
        "job_name",
        "reply_channel_target",
        "reply_channel_type",
        "start_at",
        "timezone",
    }
)
REQUIRED_SCHEDULE_JOB_KEYS = frozenset({"content", "job_name"})
TELEGRAM_MEMORY_TURN_LIMIT = 30
LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def _is_int_like(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_numeric_like(value: object) -> bool:
    return (isinstance(value, int) and not isinstance(value, bool)) or isinstance(value, float)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("max_attempts must be a positive integer")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("poll_interval_seconds must be positive")
    return parsed


def _process_envelope(
    *,
    store: StateStore,
    envelope: TaskEnvelope,
    router: RuleBasedRouter,
    executor_impl: Executor,
    policy: AllowlistPolicyEngine,
    applier: NoOpApplier | IntegratedApplier,
    max_attempts: int,
    ignore_dedupe: bool = False,
    run_id_suffix: str | None = None,
    emit_logs: bool = True,
    email_sender: SmtpEmailSender | None = None,
    error_notify_email: str | None = None,
) -> RunRecord | None:
    if not ignore_dedupe and store.seen(envelope.source, envelope.dedupe_key):
        run_id = f"run:{envelope.id}:duplicate:{time.time_ns()}"
        trace_id = f"trace:{run_id}"
        created_at = datetime.now(timezone.utc)
        record = RunRecord(
            run_id=run_id,
            envelope_id=envelope.id,
            source=envelope.source,
            workflow="duplicate_skip",
            latency_ms=0,
            result_status="duplicate_skipped",
            created_at=created_at,
        )
        store.append_run(record)
        store.append_audit_event(
            AuditEvent(
                run_id=record.run_id,
                envelope_id=record.envelope_id,
                source=record.source,
                workflow=record.workflow,
                result_status=record.result_status,
                detail={
                    "trace_id": trace_id,
                    "reason_codes": ["duplicate_dedupe_key"],
                    "dedupe_key": envelope.dedupe_key,
                    "attempt_count": 0,
                    "max_attempts": max_attempts,
                    "last_error": None,
                },
                created_at=record.created_at,
            )
        )
        if emit_logs:
            LOGGER.info(
                "skip duplicate trace_id=%s run_id=%s source=%s dedupe_key=%s",
                trace_id,
                run_id,
                envelope.source,
                envelope.dedupe_key,
            )
            LOGGER.info(
                "run_observed trace_id=%s run_id=%s envelope_id=%s source=%s workflow=%s "
                "latency_ms=%s result_status=%s",
                trace_id,
                record.run_id,
                record.envelope_id,
                record.source,
                record.workflow,
                record.latency_ms,
                record.result_status,
            )
        return record

    if not ignore_dedupe:
        store.mark_seen(envelope.source, envelope.dedupe_key)

    base_run_id = f"run:{envelope.id}"
    if run_id_suffix is not None:
        base_run_id = f"{base_run_id}:{run_id_suffix}"

    trace_reference = envelope.id if run_id_suffix is None else base_run_id
    trace_id = f"trace:{trace_reference}"

    task_envelope = envelope
    store_telegram_memory = _should_store_telegram_memory(envelope)
    if store_telegram_memory:
        task_envelope = _enrich_telegram_envelope_with_memory(
            store=store,
            envelope=envelope,
            turn_limit=TELEGRAM_MEMORY_TURN_LIMIT,
        )
        store.append_conversation_turn(
            channel="telegram",
            target=envelope.reply_channel.target,
            role="user",
            content=envelope.content,
            run_id=base_run_id,
        )

    started = time.perf_counter()
    task = router.route(task_envelope)
    reason_codes: list[str] = []
    result_status = "dead_letter"
    approved_action_count = 0
    blocked_action_count = 0
    approved_message_count = 0
    execution_message_count = 0
    execution_action_count = 0
    execution_error_count = 0
    execution_payload: dict[str, object] | None = None
    execution_action_types: list[str] = []
    applied_action_count = 0
    skipped_action_count = 0
    dispatched_message_count = 0
    apply_reason_codes: list[str] = []
    policy_decision_payload: dict[str, object] | None = None
    apply_result_payload: dict[str, object] | None = None
    attempt_count = 0
    last_error: str | None = None
    last_error_stage: str | None = None

    for attempt in range(1, max_attempts + 1):
        attempt_count = attempt
        error_stage = "executor"
        try:
            execution_result = executor_impl.execute(task)
            execution_action_count = len(execution_result.actions)
            execution_error_count = len(execution_result.errors)
            execution_payload = execution_result.to_dict()
            execution_action_types = [action.type for action in execution_result.actions]
            error_stage = "policy"
            decision = policy.evaluate(execution_result)
            policy_decision_payload = decision.to_dict()
            pending_messages: list[OutboundMessage] = []
            apply_decision = PolicyDecision(
                approved_actions=decision.approved_actions,
                blocked_actions=decision.blocked_actions,
                approved_messages=pending_messages,
                reason_codes=decision.reason_codes,
                schema_version=decision.schema_version,
            )
            error_stage = "applier"
            try:
                apply_result = applier.apply(apply_decision, envelope=envelope)
            except MessageDispatchError as dispatch_error:
                for event_index in _resolve_dispatched_event_indices(
                    original_messages=pending_messages,
                    dispatched_messages=dispatch_error.dispatched_messages,
                    envelope=envelope,
                    start_index=0,
                ):
                    store.mark_dispatched_event(run_id=base_run_id, event_index=event_index)
                raise
            for event_index in _resolve_dispatched_event_indices(
                original_messages=pending_messages,
                dispatched_messages=apply_result.dispatched_messages,
                envelope=envelope,
                start_index=0,
            ):
                store.mark_dispatched_event(run_id=base_run_id, event_index=event_index)
            apply_result_payload = apply_result.to_dict()
            if store_telegram_memory:
                for message in apply_result.dispatched_messages:
                    if message.channel != "telegram":
                        continue
                    if message.target != envelope.reply_channel.target:
                        continue
                    store.append_conversation_turn(
                        channel="telegram",
                        target=message.target,
                        role="assistant",
                        content=_message_content_for_telegram_memory(message),
                        run_id=base_run_id,
                    )
            reason_codes = decision.reason_codes
            approved_action_count = len(decision.approved_actions)
            blocked_action_count = len(decision.blocked_actions)
            applied_action_count = len(apply_result.applied_actions)
            skipped_action_count = len(apply_result.skipped_actions)
            dispatched_message_count = len(
                store.list_dispatched_event_indices(run_id=base_run_id)
            )
            apply_reason_codes = apply_result.reason_codes
            result_status = _result_status(reason_codes)
            break
        except Exception as exc:  # noqa: BLE001 - convert failures into retry/DLQ state
            last_error = f"{type(exc).__name__}: {exc}"
            last_error_stage = error_stage
            if attempt < max_attempts:
                if emit_logs:
                    LOGGER.warning(
                        "retry_scheduled trace_id=%s run_id=%s attempt=%s next_attempt=%s "
                        "max_attempts=%s error=%s",
                        trace_id,
                        base_run_id,
                        attempt,
                        attempt + 1,
                        max_attempts,
                        last_error,
                    )
            else:
                reason_codes = ["retry_exhausted"]
                if emit_logs:
                    LOGGER.error(
                        "dead_letter trace_id=%s run_id=%s attempts=%s max_attempts=%s error=%s",
                        trace_id,
                        base_run_id,
                        attempt,
                        max_attempts,
                        last_error,
                    )

    latency_ms = int((time.perf_counter() - started) * 1000)

    record = RunRecord(
        run_id=base_run_id,
        envelope_id=envelope.id,
        source=envelope.source,
        workflow=task.workflow,
        latency_ms=latency_ms,
        result_status=result_status,
        created_at=datetime.now(timezone.utc),
    )
    store.append_run(record)
    store.append_audit_event(
        AuditEvent(
            run_id=record.run_id,
            envelope_id=record.envelope_id,
            source=record.source,
            workflow=record.workflow,
            result_status=record.result_status,
            detail={
                "trace_id": trace_id,
                "reason_codes": reason_codes,
                "approved_action_count": approved_action_count,
                "blocked_action_count": blocked_action_count,
                "approved_message_count": approved_message_count,
                "execution_summary": {
                    "message_count": execution_message_count,
                    "action_count": execution_action_count,
                    "error_count": execution_error_count,
                    "action_types": execution_action_types,
                },
                "execution_result": execution_payload,
                "policy_decision": policy_decision_payload,
                "apply_result": apply_result_payload,
                "applied_action_count": applied_action_count,
                "skipped_action_count": skipped_action_count,
                "dispatched_message_count": dispatched_message_count,
                "apply_reason_codes": apply_reason_codes,
                "attempt_count": attempt_count,
                "max_attempts": max_attempts,
                "last_error": last_error,
                "last_error_stage": last_error_stage,
            },
            created_at=record.created_at,
        )
    )
    if result_status == "dead_letter":
        dead_letter_id = store.append_dead_letter(
            run_id=record.run_id,
            envelope=envelope,
            reason_codes=reason_codes,
            last_error=last_error,
            attempt_count=attempt_count,
        )
        if emit_logs:
            LOGGER.error(
                "dead_letter_recorded dead_letter_id=%s run_id=%s envelope_id=%s",
                dead_letter_id,
                record.run_id,
                record.envelope_id,
            )
        if (
            last_error_stage == "executor"
            and email_sender is not None
            and error_notify_email
        ):
            try:
                subject = f"Executor error: {record.run_id}"
                body = (
                    f"An executor error occurred and the task was placed in the dead-letter queue.\n\n"
                    f"run_id: {record.run_id}\n"
                    f"envelope_id: {record.envelope_id}\n"
                    f"source: {record.source}\n"
                    f"attempts: {attempt_count}\n"
                    f"error: {last_error}\n"
                )
                email_sender.send(error_notify_email, body, subject=subject)
                if emit_logs:
                    print(
                        f"error_notification_sent run_id={record.run_id} "
                        f"notify_email={error_notify_email}"
                    )
            except Exception as notify_exc:  # noqa: BLE001 - swallow notification failures
                if emit_logs:
                    print(
                        f"error_notification_failed run_id={record.run_id} "
                        f"notify_email={error_notify_email} error={notify_exc}"
                    )
    if emit_logs:
        LOGGER.info(
            "run_observed trace_id=%s run_id=%s envelope_id=%s source=%s workflow=%s "
            "latency_ms=%s result_status=%s",
            trace_id,
            record.run_id,
            record.envelope_id,
            record.source,
            record.workflow,
            record.latency_ms,
            record.result_status,
        )
    return record


def _result_status(reason_codes: list[str]) -> str:
    if "action_not_allowed" in reason_codes:
        return "blocked_action"
    if "executor_reported_errors" in reason_codes:
        return "execution_error"
    return "success"


def _first_undelivered_event_index(dispatched_event_indices: list[int]) -> int:
    expected_index = 0
    for index in dispatched_event_indices:
        if index != expected_index:
            break
        expected_index += 1
    return expected_index


def _resolve_dispatched_event_indices(
    *,
    original_messages: list[OutboundMessage],
    dispatched_messages: list[OutboundMessage],
    envelope: TaskEnvelope,
    start_index: int,
) -> list[int]:
    if not original_messages or not dispatched_messages:
        return []

    matched_indices: list[int] = []
    dispatched_cursor = 0
    for offset, message in enumerate(original_messages):
        if dispatched_cursor >= len(dispatched_messages):
            break
        normalized_message = _normalize_outbound_message_for_dispatch(
            message=message,
            envelope=envelope,
        )
        if not _outbound_messages_match(normalized_message, dispatched_messages[dispatched_cursor]):
            continue
        matched_indices.append(start_index + offset)
        dispatched_cursor += 1
    return matched_indices


def _normalize_outbound_message_for_dispatch(
    *,
    message: OutboundMessage,
    envelope: TaskEnvelope,
) -> OutboundMessage:
    if message.channel != "final":
        return message
    return OutboundMessage(
        channel=envelope.reply_channel.type,
        target=envelope.reply_channel.target,
        body=message.body,
        attachment=message.attachment,
    )


def _outbound_messages_match(expected: OutboundMessage, actual: OutboundMessage) -> bool:
    return (
        expected.channel == actual.channel
        and expected.target == actual.target
        and expected.body == actual.body
        and expected.attachment == actual.attachment
    )


def _should_store_telegram_memory(envelope: TaskEnvelope) -> bool:
    return envelope.reply_channel.type == "telegram"


def _message_content_for_telegram_memory(message: OutboundMessage) -> str:
    if message.body is not None:
        return message.body
    if message.attachment is None:
        raise ValueError("telegram memory requires message body or attachment")
    attachment_name = message.attachment.name
    if attachment_name is None:
        attachment_name = Path(message.attachment.uri).name or message.attachment.uri
    return f"[Attachment sent: {attachment_name}]"


def _enrich_telegram_envelope_with_memory(
    *,
    store: StateStore,
    envelope: TaskEnvelope,
    turn_limit: int,
) -> TaskEnvelope:
    turns = store.list_recent_conversation_turns(
        channel="telegram",
        target=envelope.reply_channel.target,
        limit=turn_limit,
    )
    if not turns:
        return envelope

    lines = ["Recent conversation context (oldest first):"]
    for role, content in turns:
        lines.append(f"{role}: {content}")
    lines.extend(
        [
            "",
            "Current user message:",
            envelope.content,
        ]
    )
    return TaskEnvelope(
        id=envelope.id,
        source=envelope.source,
        received_at=envelope.received_at,
        actor=envelope.actor,
        content="\n".join(lines),
        attachments=envelope.attachments,
        context_refs=envelope.context_refs,
        reply_channel=envelope.reply_channel,
        dedupe_key=envelope.dedupe_key,
        schema_version=envelope.schema_version,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query chatting state and replay dead letters.")
    parser.add_argument(
        "--config",
        help="Path to JSON config file. CLI flags override config values.",
    )
    parser.add_argument(
        "--db-path",
        help="Path to SQLite state database. Uses a temp file when omitted.",
    )
    parser.add_argument(
        "--list-runs",
        action="store_true",
        help="List persisted run records as JSON and exit.",
    )
    parser.add_argument(
        "--list-audit-events",
        action="store_true",
        help="List persisted audit events as JSON and exit.",
    )
    parser.add_argument(
        "--list-dead-letters",
        action="store_true",
        help="List pending/replayed dead-letter entries as JSON and exit.",
    )
    parser.add_argument(
        "--replay-dead-letters",
        action="store_true",
        help="Replay pending dead-letter envelopes through the worker pipeline.",
    )
    parser.add_argument(
        "--list-metrics",
        action="store_true",
        help="Output computed run metrics as JSON and exit.",
    )
    parser.add_argument(
        "--result-status",
        help="Optional run status filter used with --list-runs.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        help="Optional max number of run records returned by --list-runs.",
    )
    parser.add_argument(
        "--max-attempts",
        type=_positive_int,
        help="Maximum executor attempts per task before marking dead-letter.",
    )
    parser.add_argument(
        "--codex-command",
        help="Command used for Codex execution during replay-dead-letters.",
    )
    return parser.parse_args()


def main() -> int:
    _configure_logging()
    args = _parse_args()
    config = _load_runtime_config(args.config, os.environ)
    db_path = _resolve_str(
        cli_value=args.db_path,
        config_value=config.get("db_path"),
        default_value=str(Path(tempfile.gettempdir()) / "chatting-cli-state.db"),
        setting_name="db_path",
    )
    max_attempts = _resolve_positive_int(
        cli_value=args.max_attempts,
        config_value=config.get("max_attempts"),
        default_value=2,
        setting_name="max_attempts",
    )
    list_mode_count = sum(
        [
            args.list_runs,
            args.list_audit_events,
            args.list_dead_letters,
            args.replay_dead_letters,
            args.list_metrics,
        ]
    )
    if list_mode_count > 1:
        raise ValueError(
            "--list-runs/--list-audit-events/--list-dead-letters/--replay-dead-letters/"
            "--list-metrics cannot be combined"
        )

    if (
        args.list_runs
        or args.list_audit_events
        or args.list_dead_letters
        or args.replay_dead_letters
        or args.list_metrics
    ):
        result_status = args.result_status
        if result_status is not None:
            if not result_status.strip():
                raise ValueError("result_status must not be empty")
            result_status = result_status.strip()
        if args.list_runs:
            runs = _query_runs(db_path, limit=args.limit, result_status=result_status)
            payload = [run.to_dict() for run in runs]
        elif args.list_audit_events:
            audit_events = _query_audit_events(
                db_path,
                limit=args.limit,
                result_status=result_status,
            )
            payload = [event.to_dict() for event in audit_events]
        elif args.list_dead_letters:
            dead_letters = _query_dead_letters(db_path, limit=args.limit, status=result_status)
            payload = [entry.to_dict() for entry in dead_letters]
        elif args.list_metrics:
            payload = _build_metrics_payload(db_path)
        else:
            replayed = _replay_dead_letters(
                db_path,
                limit=args.limit,
                max_attempts=max_attempts,
                executor=_build_codex_executor(args, config),
            )
            payload = [record.to_dict() for record in replayed]
        sys.stdout.write(f"{json.dumps(payload, sort_keys=True)}\n")
        return 0

    raise ValueError(
        "non-split runtime has been removed; run app.main_message_handler and app.main_worker instead"
    )


def _build_live_connectors(args: argparse.Namespace, config: dict[str, object]) -> list[Connector]:
    connectors: list[Connector] = []

    schedule_file = _resolve_optional_str(
        cli_value=args.schedule_file,
        config_value=config.get("schedule_file"),
        setting_name="schedule_file",
    )
    if schedule_file:
        jobs = _load_schedule_jobs(schedule_file)
        connectors.append(IntervalScheduleConnector(jobs=jobs))

    imap_host = _resolve_optional_str(
        cli_value=args.imap_host,
        config_value=config.get("imap_host"),
        setting_name="imap_host",
    )
    if imap_host:
        imap_username = _resolve_optional_str(
            cli_value=args.imap_username,
            config_value=config.get("imap_username"),
            setting_name="imap_username",
        )
        if not imap_username:
            raise ValueError("--imap-username is required when --imap-host is set")
        imap_password_env = _resolve_str(
            cli_value=args.imap_password_env,
            config_value=config.get("imap_password_env"),
            default_value="CHATTING_IMAP_PASSWORD",
            setting_name="imap_password_env",
        )
        password = os.environ.get(imap_password_env, "")
        if not password:
            raise ValueError(f"missing IMAP password env var: {imap_password_env}")
        context_refs = _resolve_context_refs(args.context_ref, config)
        imap_use_ssl = _resolve_bool(
            cli_value=None,
            config_value=config.get("imap_use_ssl"),
            default_value=True,
            setting_name="imap_use_ssl",
        )
        connectors.append(
            ImapEmailConnector(
                host=imap_host,
                port=_resolve_positive_int(
                    cli_value=args.imap_port,
                    config_value=config.get("imap_port"),
                    default_value=993,
                    setting_name="imap_port",
                ),
                username=imap_username,
                password=password,
                mailbox=_resolve_str(
                    cli_value=args.imap_mailbox,
                    config_value=config.get("imap_mailbox"),
                    default_value="INBOX",
                    setting_name="imap_mailbox",
                ),
                search_criterion=_resolve_str(
                    cli_value=args.imap_search,
                    config_value=config.get("imap_search"),
                    default_value="UNSEEN",
                    setting_name="imap_search",
                ),
                use_ssl=imap_use_ssl,
                context_refs=context_refs,
            )
        )

    telegram_enabled = _resolve_bool(
        cli_value=args.telegram_enabled,
        config_value=config.get("telegram_enabled"),
        default_value=False,
        setting_name="telegram_enabled",
    )
    if telegram_enabled:
        telegram_bot_token_env = _resolve_str(
            cli_value=args.telegram_bot_token_env,
            config_value=config.get("telegram_bot_token_env"),
            default_value="CHATTING_TELEGRAM_BOT_TOKEN",
            setting_name="telegram_bot_token_env",
        )
        bot_token = os.environ.get(telegram_bot_token_env, "")
        if not bot_token:
            raise ValueError(f"missing Telegram bot token env var: {telegram_bot_token_env}")
        telegram_context_refs = _resolve_telegram_context_refs(args, config)
        connectors.append(
            TelegramConnector(
                bot_token=bot_token,
                api_base_url=_resolve_str(
                    cli_value=args.telegram_api_base_url,
                    config_value=config.get("telegram_api_base_url"),
                    default_value="https://api.telegram.org",
                    setting_name="telegram_api_base_url",
                ),
                poll_timeout_seconds=_resolve_positive_int(
                    cli_value=args.telegram_poll_timeout_seconds,
                    config_value=config.get("telegram_poll_timeout_seconds"),
                    default_value=20,
                    setting_name="telegram_poll_timeout_seconds",
                ),
                allowed_chat_ids=_resolve_telegram_allowed_chat_ids(args, config),
                allowed_channel_ids=_resolve_telegram_allowed_channel_ids(args, config),
                context_refs=telegram_context_refs,
                attachment_root_dir=_resolve_telegram_attachment_dir(args, config),
            )
        )

    if not connectors:
        raise ValueError(
            "live mode requires at least one connector (--schedule-file and/or --imap-host)"
    )
    return connectors


def _build_email_sender(args: argparse.Namespace, config: dict[str, object]) -> SmtpEmailSender | None:
    smtp_host = _resolve_optional_str(
        cli_value=args.smtp_host,
        config_value=config.get("smtp_host"),
        setting_name="smtp_host",
    )
    if not smtp_host:
        return None

    smtp_username = _resolve_optional_str(
        cli_value=args.smtp_username,
        config_value=config.get("smtp_username"),
        setting_name="smtp_username",
    )
    from_address = _resolve_optional_str(
        cli_value=args.smtp_from,
        config_value=config.get("smtp_from"),
        setting_name="smtp_from",
    ) or smtp_username
    if not from_address:
        raise ValueError("--smtp-from or --smtp-username is required when --smtp-host is set")

    password = None
    if smtp_username:
        smtp_password_env = _resolve_str(
            cli_value=args.smtp_password_env,
            config_value=config.get("smtp_password_env"),
            default_value="CHATTING_SMTP_PASSWORD",
            setting_name="smtp_password_env",
        )
        password = os.environ.get(smtp_password_env, "")
        if not password:
            raise ValueError(f"missing SMTP password env var: {smtp_password_env}")

    smtp_starttls = _resolve_bool(
        cli_value=args.smtp_starttls,
        config_value=config.get("smtp_starttls"),
        default_value=False,
        setting_name="smtp_starttls",
    )

    raw_smtp_use_ssl = config.get("smtp_use_ssl")
    if raw_smtp_use_ssl is not None:
        if not isinstance(raw_smtp_use_ssl, bool):
            raise ValueError("smtp_use_ssl must be a boolean")
        smtp_use_ssl = raw_smtp_use_ssl
    else:
        smtp_use_ssl = not smtp_starttls

    return SmtpEmailSender(
        host=smtp_host,
        port=_resolve_positive_int(
            cli_value=args.smtp_port,
            config_value=config.get("smtp_port"),
            default_value=465,
            setting_name="smtp_port",
        ),
        from_address=from_address,
        username=smtp_username,
        password=password,
        use_ssl=smtp_use_ssl,
        starttls=smtp_starttls,
    )


def _build_telegram_sender(
    args: argparse.Namespace,
    config: dict[str, object],
) -> TelegramMessageSender | None:
    telegram_enabled = _resolve_bool(
        cli_value=args.telegram_enabled,
        config_value=config.get("telegram_enabled"),
        default_value=False,
        setting_name="telegram_enabled",
    )
    if not telegram_enabled:
        return None

    telegram_bot_token_env = _resolve_str(
        cli_value=args.telegram_bot_token_env,
        config_value=config.get("telegram_bot_token_env"),
        default_value="CHATTING_TELEGRAM_BOT_TOKEN",
        setting_name="telegram_bot_token_env",
    )
    bot_token = os.environ.get(telegram_bot_token_env, "")
    if not bot_token:
        raise ValueError(f"missing Telegram bot token env var: {telegram_bot_token_env}")

    return TelegramMessageSender(
        bot_token=bot_token,
        api_base_url=_resolve_str(
            cli_value=args.telegram_api_base_url,
            config_value=config.get("telegram_api_base_url"),
            default_value="https://api.telegram.org",
            setting_name="telegram_api_base_url",
        ),
    )


def _build_codex_executor(args: argparse.Namespace, config: dict[str, object]) -> Executor:
    codex_raw = _resolve_optional_str(
        cli_value=args.codex_command,
        config_value=config.get("codex_command"),
        setting_name="codex_command",
    )
    claude_raw = _resolve_optional_str(
        cli_value=getattr(args, "claude_command", None),
        config_value=config.get("claude_command"),
        setting_name="claude_command",
    )

    if codex_raw:
        command = tuple(shlex.split(codex_raw)) + ("--json",)
    elif claude_raw:
        command = tuple(shlex.split(claude_raw)) + (
            "-p",
            "--output-format", "json",
            "--json-schema", EXECUTION_RESULT_JSON_SCHEMA,
        )
    else:
        command = ("codex", "exec", "--json")

    return CodexExecutor(command=command)


def _load_schedule_jobs(schedule_file: str) -> list[IntervalScheduleJob]:
    payload = json.loads(Path(schedule_file).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("schedule file must contain a JSON array")

    jobs: list[IntervalScheduleJob] = []
    for index, raw_job in enumerate(payload):
        if not isinstance(raw_job, dict):
            raise ValueError(f"schedule job at index {index} must be an object")
        unknown_keys = sorted(set(raw_job.keys()) - ALLOWED_SCHEDULE_JOB_KEYS)
        if unknown_keys:
            keys = ", ".join(unknown_keys)
            raise ValueError(
                f"schedule job at index {index} contains unknown keys: {keys}"
            )

        missing_keys = sorted(REQUIRED_SCHEDULE_JOB_KEYS - set(raw_job.keys()))
        if missing_keys:
            keys = ", ".join(missing_keys)
            raise ValueError(
                f"schedule job at index {index} is missing required keys: {keys}"
            )

        job_name = raw_job["job_name"]
        if not isinstance(job_name, str) or not job_name.strip():
            raise ValueError(f"schedule job at index {index} job_name must be a non-empty string")

        content = raw_job["content"]
        if not isinstance(content, str) or not content.strip():
            raise ValueError(f"schedule job at index {index} content must be a non-empty string")

        cron = raw_job.get("cron")
        if cron is not None and (not isinstance(cron, str) or not cron.strip()):
            raise ValueError(f"schedule job at index {index} cron must be a non-empty string")

        raw_interval = raw_job.get("interval_seconds")
        interval_seconds: int | None = None
        if cron is None:
            if not isinstance(raw_interval, int) or isinstance(raw_interval, bool):
                raise ValueError(
                    f"schedule job at index {index} interval_seconds must be a positive integer"
                )
            interval_seconds = raw_interval
            if interval_seconds <= 0:
                raise ValueError(
                    f"schedule job at index {index} interval_seconds must be a positive integer"
                )
        elif isinstance(raw_interval, int) and not isinstance(raw_interval, bool) and raw_interval <= 0:
            raise ValueError(
                f"schedule job at index {index} interval_seconds must be a positive integer"
            )

        timezone_name = raw_job.get("timezone")
        if cron is not None:
            if timezone_name is None:
                timezone_name = "UTC"
            elif not isinstance(timezone_name, str) or not timezone_name.strip():
                raise ValueError(
                    f"schedule job at index {index} timezone must be a non-empty string"
                )
        elif timezone_name is not None:
            raise ValueError(f"schedule job at index {index} timezone is only valid with cron")

        if interval_seconds is None and cron is None:
            raise ValueError(
                f"schedule job at index {index} must define interval_seconds or cron"
            )

        raw_context_refs = raw_job.get("context_refs", [])
        if not isinstance(raw_context_refs, list):
            raise ValueError(
                f"schedule job at index {index} context_refs must be a list of non-empty strings"
            )
        if not all(isinstance(item, str) and item.strip() for item in raw_context_refs):
            raise ValueError(
                f"schedule job at index {index} context_refs must be a list of non-empty strings"
            )

        raw_start_at = raw_job.get("start_at")
        if cron is None:
            if raw_start_at is not None and not isinstance(raw_start_at, str):
                raise ValueError(
                    f"schedule job at index {index} start_at must be an RFC3339 string"
                )
            start_at = _parse_optional_rfc3339(raw_start_at) if raw_start_at is not None else None
        else:
            start_at = None

        reply_channel_type = raw_job.get("reply_channel_type")
        if reply_channel_type is not None and (
            not isinstance(reply_channel_type, str) or not reply_channel_type.strip()
        ):
            raise ValueError(
                f"schedule job at index {index} reply_channel_type must be a non-empty string"
            )
        reply_channel_target = raw_job.get("reply_channel_target")
        if reply_channel_target is not None and (
            not isinstance(reply_channel_target, str) or not reply_channel_target.strip()
        ):
            raise ValueError(
                f"schedule job at index {index} reply_channel_target must be a non-empty string"
            )
        if (reply_channel_type is None) != (reply_channel_target is None):
            raise ValueError(
                f"schedule job at index {index} reply_channel_type and "
                "reply_channel_target must be provided together"
            )
        jobs.append(
            IntervalScheduleJob(
                job_name=job_name.strip(),
                content=content.strip(),
                context_refs=list(raw_context_refs),
                interval_seconds=interval_seconds if isinstance(interval_seconds, int) else None,
                cron=cron.strip() if isinstance(cron, str) else None,
                timezone_name=timezone_name.strip() if isinstance(timezone_name, str) else None,
                start_at=start_at,
                reply_channel_type=reply_channel_type.strip()
                if isinstance(reply_channel_type, str)
                else None,
                reply_channel_target=reply_channel_target.strip()
                if isinstance(reply_channel_target, str)
                else None,
            )
        )
    return jobs


def _parse_optional_rfc3339(value: str) -> datetime:
    parsed_value = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(parsed_value)
    if parsed.tzinfo is None:
        raise ValueError("schedule job start_at must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _load_runtime_config(
    config_path: str | None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    config_source = config_path
    env = os.environ if environ is None else environ

    if config_source is None:
        raw_env_path = env.get(CONFIG_PATH_ENV_VAR)
        if raw_env_path is not None:
            if not raw_env_path.strip():
                raise ValueError(f"{CONFIG_PATH_ENV_VAR} must not be empty")
            config_source = raw_env_path

    if not config_source:
        return {}
    payload = json.loads(Path(config_source).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("config file must contain a JSON object")
    unknown_keys = sorted(set(payload.keys()) - ALLOWED_RUNTIME_CONFIG_KEYS)
    if unknown_keys:
        keys = ", ".join(unknown_keys)
        raise ValueError(f"config contains unknown keys: {keys}")
    return payload


def _resolve_str(
    *,
    cli_value: str | None,
    config_value: object,
    default_value: str,
    setting_name: str,
) -> str:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        if not default_value.strip():
            raise ValueError(f"default {setting_name} must not be empty")
        return default_value
    if not isinstance(config_value, str):
        raise ValueError(f"config {setting_name} must be a string")
    if not config_value.strip():
        raise ValueError(f"config {setting_name} must not be empty")
    return config_value


def _resolve_optional_str(
    *,
    cli_value: str | None,
    config_value: object,
    setting_name: str,
) -> str | None:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        return None
    if not isinstance(config_value, str):
        raise ValueError(f"config {setting_name} must be a string")
    if not config_value.strip():
        raise ValueError(f"config {setting_name} must not be empty")
    return config_value


def _resolve_positive_int(
    *,
    cli_value: int | None,
    config_value: object,
    default_value: int,
    setting_name: str,
) -> int:
    if cli_value is not None:
        return cli_value
    candidate = config_value if config_value is not None else default_value
    if not isinstance(candidate, int) or isinstance(candidate, bool):
        raise ValueError(f"config {setting_name} must be an integer")
    if candidate <= 0:
        raise ValueError(f"config {setting_name} must be positive")
    return candidate


def _resolve_optional_positive_int(
    *,
    cli_value: int | None,
    config_value: object,
    setting_name: str,
) -> int | None:
    if cli_value is not None:
        return cli_value
    if config_value is None:
        return None
    if not isinstance(config_value, int) or isinstance(config_value, bool):
        raise ValueError(f"config {setting_name} must be an integer")
    if config_value <= 0:
        raise ValueError(f"config {setting_name} must be positive")
    return config_value


def _resolve_positive_float(
    *,
    cli_value: float | None,
    config_value: object,
    default_value: float,
    setting_name: str,
) -> float:
    if cli_value is not None:
        return cli_value
    candidate = config_value if config_value is not None else default_value
    if not isinstance(candidate, (int, float)) or isinstance(candidate, bool):
        raise ValueError(f"config {setting_name} must be numeric")
    parsed = float(candidate)
    if parsed <= 0:
        raise ValueError(f"config {setting_name} must be positive")
    return parsed


def _resolve_bool(
    *,
    cli_value: bool | None,
    config_value: object,
    default_value: bool,
    setting_name: str,
) -> bool:
    if cli_value:
        return True
    if config_value is None:
        return default_value
    if not isinstance(config_value, bool):
        raise ValueError(f"config {setting_name} must be a boolean")
    return config_value


def _resolve_context_refs(cli_values: list[str], config: dict[str, object]) -> list[str]:
    raw_config_values = config.get("context_ref")
    if raw_config_values is None:
        raw_config_values = config.get("context_refs")

    if raw_config_values is None:
        config_values: list[str] = []
    else:
        if not isinstance(raw_config_values, list):
            raise ValueError("config context_ref/context_refs must be a list of strings")
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError("config context_ref/context_refs must be a list of strings")
        config_values = list(raw_config_values)

    merged_values = [*config_values, *cli_values]
    if any(not value.strip() for value in merged_values):
        raise ValueError("context_ref/context_refs entries must not be empty")
    return merged_values


def _resolve_telegram_allowed_chat_ids(
    args: argparse.Namespace,
    config: dict[str, object],
) -> list[str] | None:
    raw_config_values = config.get("telegram_allowed_chat_ids")
    config_values: list[str]
    if raw_config_values is None:
        config_values = []
    else:
        if not isinstance(raw_config_values, list):
            raise ValueError("config telegram_allowed_chat_ids must be a list of strings")
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError("config telegram_allowed_chat_ids must be a list of strings")
        config_values = list(raw_config_values)

    merged_values = [*config_values, *args.telegram_allowed_chat_id]
    if any(not value.strip() for value in merged_values):
        raise ValueError("telegram_allowed_chat_id(s) entries must not be empty")
    if not merged_values:
        return None
    return merged_values


def _resolve_telegram_context_refs(
    args: argparse.Namespace,
    config: dict[str, object],
) -> list[str]:
    raw_config_values = config.get("telegram_context_refs")
    config_values: list[str]
    if raw_config_values is None:
        config_values = _resolve_context_refs(args.context_ref, config)
    else:
        if not isinstance(raw_config_values, list):
            raise ValueError("config telegram_context_refs must be a list of strings")
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError("config telegram_context_refs must be a list of strings")
        config_values = list(raw_config_values)

    merged_values = [*config_values, *args.telegram_context_ref]
    if any(not value.strip() for value in merged_values):
        raise ValueError("telegram_context_ref(s) entries must not be empty")
    return merged_values


def _resolve_telegram_attachment_dir(
    args: argparse.Namespace,
    config: dict[str, object],
) -> str:
    return _resolve_str(
        cli_value=getattr(args, "telegram_attachment_dir", None),
        config_value=config.get("telegram_attachment_dir"),
        default_value=str(Path(tempfile.gettempdir()) / "chatting-telegram-attachments"),
        setting_name="telegram_attachment_dir",
    )


def _resolve_telegram_allowed_channel_ids(
    args: argparse.Namespace,
    config: dict[str, object],
) -> list[str] | None:
    raw_config_values = config.get("telegram_allowed_channel_ids")
    config_values: list[str]
    if raw_config_values is None:
        config_values = []
    else:
        if not isinstance(raw_config_values, list):
            raise ValueError("config telegram_allowed_channel_ids must be a list of strings")
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError("config telegram_allowed_channel_ids must be a list of strings")
        config_values = list(raw_config_values)

    merged_values = [*config_values, *args.telegram_allowed_channel_id]
    if any(not value.strip() for value in merged_values):
        raise ValueError("telegram_allowed_channel_id(s) entries must not be empty")
    if not merged_values:
        return None
    return merged_values


def _query_runs(
    db_path: str,
    *,
    limit: int | None,
    result_status: str | None,
) -> list[RunRecord]:
    store = SQLiteStateStore(db_path)
    runs = store.list_runs()
    if result_status is not None:
        runs = [run for run in runs if run.result_status == result_status]
    if limit is not None:
        runs = runs[-limit:]
    return runs


def _query_audit_events(
    db_path: str,
    *,
    limit: int | None,
    result_status: str | None,
) -> list[AuditEvent]:
    store = SQLiteStateStore(db_path)
    audit_events = store.list_audit_events()
    if result_status is not None:
        audit_events = [event for event in audit_events if event.result_status == result_status]
    if limit is not None:
        audit_events = audit_events[-limit:]
    return audit_events


def _query_dead_letters(
    db_path: str,
    *,
    limit: int | None,
    status: str | None,
) -> list[DeadLetterRecord]:
    store = SQLiteStateStore(db_path)
    dead_letters = store.list_dead_letters(status=status)
    if limit is not None:
        dead_letters = dead_letters[-limit:]
    return dead_letters


def _build_metrics_payload(db_path: str) -> dict[str, object]:
    runs = SQLiteStateStore(db_path).list_runs()
    by_status: dict[str, int] = {}
    for run in runs:
        by_status[run.result_status] = by_status.get(run.result_status, 0) + 1
    total_runs = len(runs)
    total_latency_ms = sum(run.latency_ms for run in runs)
    average_latency_ms = (total_latency_ms / total_runs) if total_runs > 0 else 0.0
    return {
        "total_runs": total_runs,
        "average_latency_ms": average_latency_ms,
        "by_status": by_status,
    }


def _replay_dead_letters(
    db_path: str,
    *,
    limit: int | None,
    max_attempts: int,
    executor: Executor,
) -> list[RunRecord]:
    store: StateStore = SQLiteStateStore(db_path)
    router = RuleBasedRouter()
    policy = AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"}))
    applier = NoOpApplier()
    dead_letters = store.list_dead_letters(status="pending")
    if limit is not None:
        dead_letters = dead_letters[:limit]

    replayed: list[RunRecord] = []
    for dead_letter in dead_letters:
        run = _process_envelope(
            store=store,
            envelope=dead_letter.envelope,
            router=router,
            executor_impl=executor,
            policy=policy,
            applier=applier,
            max_attempts=max_attempts,
            ignore_dedupe=True,
            run_id_suffix=f"replay:{dead_letter.dead_letter_id}:{time.time_ns()}",
            emit_logs=False,
        )
        if run is None:
            continue
        store.mark_dead_letter_replayed(dead_letter.dead_letter_id, run.run_id)
        replayed.append(run)
    return replayed


if __name__ == "__main__":
    raise SystemExit(main())
