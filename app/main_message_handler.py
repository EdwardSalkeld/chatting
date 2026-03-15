"""Message-handler entrypoint: ingress connectors + strict egress dispatch."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
from app.github_ingress_runtime import (
    AssignmentCheckpoint,
    GitHubAssignmentCheckpointStore,
    checkpoint_scope_key,
    default_graphql_runner,
    expand_repository_patterns,
    fetch_assignment_events_for_repository,
    fetch_authenticated_viewer_login,
    parse_repo_slug,
    publish_assignment_events,
    select_events_after_checkpoint,
)
from app.main import (
    TELEGRAM_MEMORY_TURN_LIMIT,
    _build_email_sender,
    _build_live_connectors,
    _build_telegram_sender,
    _enrich_telegram_envelope_with_memory,
    _message_content_for_telegram_memory,
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
        "github_repositories",
        "github_assignee_login",
        "github_reply_channel_type",
        "github_reply_channel_target",
        "github_context_refs",
        "github_policy_profile",
        "github_max_issues",
        "github_max_timeline_events",
    }
)
BBMB_EGRESS_PICKUP_WAIT_SECONDS = 5


@dataclass(frozen=True)
class GitHubIngressSettings:
    repositories: list[str]
    assignee_login: str
    reply_channel_type: str
    reply_channel_target: str
    context_refs: list[str]
    policy_profile: str
    max_issues: int
    max_timeline_events: int


@dataclass
class EgressTelemetryRollup:
    """In-memory telemetry rollup for split-mode egress lifecycle signals."""

    received_total: int = 0
    dispatched_total: int = 0
    deduped_total: int = 0
    dropped_total: int = 0
    dropped_unknown_task_total: int = 0
    dropped_disallowed_channel_total: int = 0
    dropped_missing_event_id_total: int = 0
    dispatch_latency_ms_total: int = 0
    dispatch_latency_ms_count: int = 0
    dispatch_latency_ms_max: int = 0
    incremental_dispatched_total: int = 0
    final_dispatched_total: int = 0

    def record_received(self) -> None:
        self.received_total += 1

    def record_deduped(self) -> None:
        self.deduped_total += 1

    def record_dropped(self, *, reason: str) -> None:
        self.dropped_total += 1
        if reason == "unknown_task":
            self.dropped_unknown_task_total += 1
        elif reason == "disallowed_channel":
            self.dropped_disallowed_channel_total += 1
        elif reason == "missing_event_id":
            self.dropped_missing_event_id_total += 1

    def record_dispatched(self, *, event_kind: str, latency_ms: int) -> None:
        self.dispatched_total += 1
        if event_kind == "incremental":
            self.incremental_dispatched_total += 1
        else:
            self.final_dispatched_total += 1
        self.dispatch_latency_ms_total += latency_ms
        self.dispatch_latency_ms_count += 1
        self.dispatch_latency_ms_max = max(self.dispatch_latency_ms_max, latency_ms)

    def snapshot(self) -> dict[str, float | int]:
        dedupe_base = self.dispatched_total + self.deduped_total
        dedupe_hit_rate_pct = (
            round((self.deduped_total / dedupe_base) * 100.0, 2) if dedupe_base else 0.0
        )
        avg_dispatch_latency_ms = (
            round(self.dispatch_latency_ms_total / self.dispatch_latency_ms_count, 2)
            if self.dispatch_latency_ms_count
            else 0.0
        )
        return {
            "received_total": self.received_total,
            "dispatched_total": self.dispatched_total,
            "deduped_total": self.deduped_total,
            "dedupe_hit_rate_pct": dedupe_hit_rate_pct,
            "dropped_total": self.dropped_total,
            "dropped_unknown_task_total": self.dropped_unknown_task_total,
            "dropped_disallowed_channel_total": self.dropped_disallowed_channel_total,
            "dropped_missing_event_id_total": self.dropped_missing_event_id_total,
            "incremental_dispatched_total": self.incremental_dispatched_total,
            "final_dispatched_total": self.final_dispatched_total,
            "dispatch_latency_ms_avg": avg_dispatch_latency_ms,
            "dispatch_latency_ms_max": self.dispatch_latency_ms_max,
        }


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
    parser.add_argument(
        "--github-repository",
        action="append",
        default=[],
        help="GitHub repository in owner/repo format (repeatable).",
    )
    parser.add_argument("--github-assignee-login", help="GitHub login to filter assigned events for.")
    parser.add_argument("--github-reply-channel-type", help="Reply channel type for generated tasks.")
    parser.add_argument("--github-reply-channel-target", help="Reply channel target for generated tasks.")
    parser.add_argument(
        "--github-context-ref",
        action="append",
        default=[],
        help="Context ref to attach to generated tasks (repeatable).",
    )
    parser.add_argument("--github-policy-profile", help="Policy profile for generated tasks.")
    parser.add_argument("--github-max-issues", type=_positive_int, help="Per-repo issue scan limit.")
    parser.add_argument(
        "--github-max-timeline-events",
        type=_positive_int,
        help="Per-issue assigned-event scan limit.",
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


def _resolve_required_str(cli_value: str | None, config_value: object, *, setting_name: str) -> str:
    resolved = _resolve_str(
        cli_value,
        config_value,
        default_value="",
        setting_name=setting_name,
    ).strip()
    if not resolved:
        raise ValueError(f"{setting_name} is required when github_repositories is configured")
    return resolved


def _resolve_github_repositories(args: argparse.Namespace, config: dict[str, object]) -> list[str]:
    repositories: list[str] = []
    config_values = config.get("github_repositories")
    if config_values is not None:
        if not isinstance(config_values, list) or not all(isinstance(item, str) for item in config_values):
            raise ValueError("config github_repositories must be a list of owner/repo strings")
        repositories.extend(config_values)
    repositories.extend(args.github_repository)

    deduped: list[str] = []
    seen: set[str] = set()
    for repository in repositories:
        if not repository.strip():
            raise ValueError("github_repositories entries must not be empty")
        parts = repository.strip().split("/", maxsplit=1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError("github_repositories entries must be owner/repo or owner/*")
        owner, name = parts
        if name != "*" and "*" in name:
            raise ValueError("github_repositories entries must be owner/repo or owner/*")
        normalized = f"{owner}/{name}"
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _resolve_github_context_refs(args: argparse.Namespace, config: dict[str, object]) -> list[str]:
    context_refs: list[str] = []
    config_values = config.get("github_context_refs")
    if config_values is not None:
        if not isinstance(config_values, list) or not all(isinstance(item, str) for item in config_values):
            raise ValueError("config github_context_refs must be a list of strings")
        context_refs.extend(config_values)
    context_refs.extend(args.github_context_ref)
    if any(not item.strip() for item in context_refs):
        raise ValueError("github_context_ref entries must not be empty")
    return [item.strip() for item in context_refs]


def _resolve_github_ingress_settings(
    args: argparse.Namespace,
    config: dict[str, object],
) -> GitHubIngressSettings | None:
    repositories = _resolve_github_repositories(args, config)
    if not repositories:
        return None

    assignee_login = _resolve_str(
        args.github_assignee_login,
        config.get("github_assignee_login"),
        default_value="",
        setting_name="github_assignee_login",
    ).strip()
    if not assignee_login:
        try:
            assignee_login = fetch_authenticated_viewer_login(
                graphql_runner=default_graphql_runner,
            )
        except Exception as error:  # noqa: BLE001
            raise ValueError(
                "github_assignee_login is required when github_repositories is configured "
                "unless it can be derived from authenticated gh user"
            ) from error

    return GitHubIngressSettings(
        repositories=repositories,
        assignee_login=assignee_login,
        reply_channel_type=_resolve_required_str(
            args.github_reply_channel_type,
            config.get("github_reply_channel_type"),
            setting_name="github_reply_channel_type",
        ),
        reply_channel_target=_resolve_required_str(
            args.github_reply_channel_target,
            config.get("github_reply_channel_target"),
            setting_name="github_reply_channel_target",
        ),
        context_refs=_resolve_github_context_refs(args, config),
        policy_profile=_resolve_str(
            args.github_policy_profile,
            config.get("github_policy_profile"),
            default_value="default",
            setting_name="github_policy_profile",
        ).strip(),
        max_issues=_resolve_positive_int(
            args.github_max_issues,
            config.get("github_max_issues"),
            default_value=25,
            setting_name="github_max_issues",
        ),
        max_timeline_events=_resolve_positive_int(
            args.github_max_timeline_events,
            config.get("github_max_timeline_events"),
            default_value=10,
            setting_name="github_max_timeline_events",
        ),
    )


def _poll_github_assignment_ingress(
    *,
    settings: GitHubIngressSettings,
    checkpoint_store: GitHubAssignmentCheckpointStore,
    store: SQLiteStateStore,
    broker: BBMBQueueAdapter,
) -> tuple[int, int, int, str]:
    scope_key = checkpoint_scope_key(
        repositories=settings.repositories,
        assignee_login=settings.assignee_login,
    )
    checkpoint = checkpoint_store.get_checkpoint(scope_key)
    events = []
    scanned_event_count = 0
    repositories_to_scan = expand_repository_patterns(
        repository_patterns=settings.repositories,
        graphql_runner=default_graphql_runner,
    )
    for repository in repositories_to_scan:
        owner, name = parse_repo_slug(repository)
        try:
            repository_events = fetch_assignment_events_for_repository(
                repo_owner=owner,
                repo_name=name,
                assignee_login=settings.assignee_login,
                issue_limit=settings.max_issues,
                timeline_limit=settings.max_timeline_events,
                graphql_runner=default_graphql_runner,
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception(
                "github_assignment_poll_failed repository=%s assignee=%s",
                repository,
                settings.assignee_login,
            )
            continue
        scanned_event_count += len(repository_events)
        events.extend(repository_events)

    new_events = select_events_after_checkpoint(events, checkpoint=checkpoint)
    published_count = publish_assignment_events(
        events=new_events,
        store=store,
        broker=broker,
        reply_channel_type=settings.reply_channel_type,
        reply_channel_target=settings.reply_channel_target,
        context_refs=settings.context_refs,
        policy_profile=settings.policy_profile,
    )
    checkpoint_id = checkpoint.event_id if checkpoint else "none"
    if new_events:
        latest = new_events[-1]
        checkpoint_store.set_checkpoint(
            scope_key,
            checkpoint=AssignmentCheckpoint(
                event_created_at=latest.event_created_at,
                event_id=latest.event_id,
            ),
        )
        checkpoint_id = latest.event_id

    return scanned_event_count, len(new_events), published_count, checkpoint_id


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
    telemetry: EgressTelemetryRollup | None = None,
) -> None:
    def _ack_and_mark_outbox(event_id: str | None = None) -> None:
        ack_callback(picked_guid)
        if event_id:
            store.mark_egress_outbox_event_acked(event_id=event_id)

    if telemetry is not None:
        telemetry.record_received()
    try:
        egress_message = EgressQueueMessage.from_dict(picked_payload)
    except Exception:
        LOGGER.exception("egress_payload_invalid guid=%s", picked_guid)
        _ack_and_mark_outbox()
        return

    ledger_record = ledger.get_task(egress_message.task_id)
    if ledger_record is None or ledger_record.envelope_id != egress_message.envelope_id:
        if telemetry is not None:
            telemetry.record_dropped(reason="unknown_task")
        LOGGER.error(
            "egress_drop_unknown_task task_id=%s envelope_id=%s guid=%s",
            egress_message.task_id,
            egress_message.envelope_id,
            picked_guid,
        )
        _ack_and_mark_outbox(egress_message.event_id)
        return

    if egress_message.message.channel not in allowed_egress_channels:
        if telemetry is not None:
            telemetry.record_dropped(reason="disallowed_channel")
        LOGGER.error(
            "egress_drop_disallowed_channel task_id=%s channel=%s guid=%s",
            egress_message.task_id,
            egress_message.message.channel,
            picked_guid,
        )
        _ack_and_mark_outbox(egress_message.event_id)
        return

    if egress_message.event_id is None:
        if telemetry is not None:
            telemetry.record_dropped(reason="missing_event_id")
        LOGGER.error(
            "egress_drop_missing_event_id task_id=%s guid=%s",
            egress_message.task_id,
            picked_guid,
        )
        _ack_and_mark_outbox()
        return

    if store.has_dispatched_event_id(task_id=egress_message.task_id, event_id=egress_message.event_id):
        if telemetry is not None:
            telemetry.record_deduped()
        LOGGER.info(
            "egress_skip_already_dispatched task_id=%s event_id=%s guid=%s",
            egress_message.task_id,
            egress_message.event_id,
            picked_guid,
        )
        _ack_and_mark_outbox(egress_message.event_id)
        return

    ledger.stage_egress_event(egress_message)
    _ack_and_mark_outbox(egress_message.event_id)
    _flush_task_egress_in_sequence(
        task_id=egress_message.task_id,
        ledger=ledger,
        store=store,
        applier=applier,
        telemetry=telemetry,
    )


def _flush_task_egress_in_sequence(
    *,
    task_id: str,
    ledger: TaskLedgerStore,
    store: SQLiteStateStore,
    applier: IntegratedApplier,
    telemetry: EgressTelemetryRollup | None = None,
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
            if telemetry is not None:
                telemetry.record_deduped()
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
        dispatch_latency_ms = int(
            (datetime.now(timezone.utc) - egress_message.emitted_at.astimezone(timezone.utc)).total_seconds() * 1000
        )
        if telemetry is not None:
            telemetry.record_dispatched(
                event_kind=egress_message.event_kind,
                latency_ms=max(dispatch_latency_ms, 0),
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
                    content=_message_content_for_telegram_memory(message),
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
    github_ingress_settings = _resolve_github_ingress_settings(args, config)

    store = SQLiteStateStore(db_path)
    ledger = TaskLedgerStore(db_path)
    broker = BBMBQueueAdapter(address=bbmb_address)
    broker.ensure_queue(TASK_QUEUE_NAME)
    broker.ensure_queue(EGRESS_QUEUE_NAME)
    github_checkpoint_store = GitHubAssignmentCheckpointStore(db_path) if github_ingress_settings else None

    connectors = _build_live_connectors(args, config)
    applier = IntegratedApplier(
        base_dir=".",
        email_sender=_build_email_sender(args, config),
        telegram_sender=_build_telegram_sender(args, config),
    )

    loop_count = 0
    telemetry = EgressTelemetryRollup()
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

        github_scanned_events = 0
        github_new_events = 0
        github_published = 0
        github_checkpoint = "disabled"
        if github_ingress_settings is not None and github_checkpoint_store is not None:
            (
                github_scanned_events,
                github_new_events,
                github_published,
                github_checkpoint,
            ) = _poll_github_assignment_ingress(
                settings=github_ingress_settings,
                checkpoint_store=github_checkpoint_store,
                store=store,
                broker=broker,
            )

        egress_picked = broker.pickup_json(
            EGRESS_QUEUE_NAME,
            timeout_seconds=poll_timeout_seconds,
            wait_seconds=BBMB_EGRESS_PICKUP_WAIT_SECONDS,
        )
        if egress_picked is not None:
            _handle_egress_message(
                picked_guid=egress_picked.guid,
                picked_payload=egress_picked.payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels=allowed_egress_channels,
                applier=applier,
                ack_callback=lambda guid: broker.ack(EGRESS_QUEUE_NAME, guid),
                telemetry=telemetry,
            )

        telemetry_snapshot = telemetry.snapshot()
        LOGGER.info(
            (
                "message_handler_loop_completed loop=%s ingress_published=%s "
                "github_scanned_events=%s github_new_events=%s github_published=%s github_checkpoint=%s "
                "egress_received_total=%s egress_dispatched_total=%s egress_deduped_total=%s "
                "egress_dedupe_hit_rate_pct=%s egress_dropped_total=%s "
                "egress_dispatch_latency_ms_avg=%s egress_dispatch_latency_ms_max=%s "
                "egress_incremental_dispatched_total=%s egress_final_dispatched_total=%s"
            ),
            loop_count,
            ingress_published,
            github_scanned_events,
            github_new_events,
            github_published,
            github_checkpoint,
            telemetry_snapshot["received_total"],
            telemetry_snapshot["dispatched_total"],
            telemetry_snapshot["deduped_total"],
            telemetry_snapshot["dedupe_hit_rate_pct"],
            telemetry_snapshot["dropped_total"],
            telemetry_snapshot["dispatch_latency_ms_avg"],
            telemetry_snapshot["dispatch_latency_ms_max"],
            telemetry_snapshot["incremental_dispatched_total"],
            telemetry_snapshot["final_dispatched_total"],
        )

        if max_loops and loop_count >= max_loops:
            break
        time.sleep(poll_interval_seconds)

    return 0


if __name__ == "__main__":
    sys.exit(main())
