"""Message-handler entrypoint: ingress connectors + strict egress dispatch."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Callable, Mapping

from app.handler.applier import (
    GitHubIssueCommentSender,
    IntegratedApplier,
    MessageDispatchError,
    SmtpEmailSender,
    TelegramMessageSender,
)
from app.broker import (
    BBMBQueueAdapter,
    EGRESS_QUEUE_NAME,
    EgressQueueMessage,
    TASK_QUEUE_NAME,
    TaskQueueMessage,
)
from app.handler.connectors import (
    AuxiliaryIngressConnector,
    Connector,
    GitHubIssueAssignmentConnector,
    GitHubPullRequestReviewConnector,
    ImapEmailConnector,
    InternalHeartbeatConnector,
    IntervalScheduleConnector,
    IntervalScheduleJob,
    TelegramConnector,
)
from app.handler.github_ingress import (
    GitHubAssignmentCheckpointStore,
    default_graphql_runner,
    fetch_authenticated_viewer_login,
)
from app.internal_heartbeat import (
    INTERNAL_HEARTBEAT_TARGET,
    is_internal_heartbeat_envelope,
)
from app.handler.runtime import (
    TaskLedgerRecord,
    TaskLedgerStore,
    TelegramAttachmentCleanupResult,
    TelegramAttachmentStore,
    cleanup_telegram_attachments,
)
from app.models import OutboundMessage, PolicyDecision, PromptContext, TaskEnvelope
from app.state import SQLiteStateStore
from app.handler.telemetry import (
    EgressTelemetryRollup,
    HeartbeatTelemetryRollup,
    MessageHandlerMetrics,
    _start_metrics_server,
)

MESSAGE_HANDLER_CONFIG_PATH_ENV_VAR = "CHATTING_MESSAGE_HANDLER_CONFIG_PATH"
LOGGER = logging.getLogger("app.main_message_handler")
_ALLOWED_CONFIG_KEYS = frozenset(
    {
        "bbmb_address",
        "auxiliary_ingress_bbmb_address",
        "db_path",
        "max_loops",
        "poll_interval_seconds",
        "poll_timeout_seconds",
        "metrics_host",
        "metrics_port",
        "allowed_egress_channels",
        "auxiliary_ingress_enabled",
        "auxiliary_ingress_routes",
        "auxiliary_ingress_context_refs",
        "prompt_context",
        "schedule_file",
        "imap_host",
        "imap_port",
        "imap_username",
        "imap_password_env",
        "imap_mailbox",
        "imap_search",
        "imap_use_ssl",
        "email_prompt_context",
        "context_ref",
        "context_refs",
        "smtp_host",
        "smtp_port",
        "smtp_username",
        "smtp_password_env",
        "smtp_from",
        "smtp_starttls",
        "smtp_use_ssl",
        "error_email_to",
        "telegram_enabled",
        "telegram_bot_token_env",
        "telegram_api_base_url",
        "telegram_poll_timeout_seconds",
        "telegram_allowed_chat_ids",
        "telegram_allowed_channel_ids",
        "telegram_attachment_dir",
        "telegram_attachment_cleanup_grace_seconds",
        "telegram_attachment_max_age_seconds",
        "telegram_prompt_context",
        "telegram_context_refs",
        "github_repositories",
        "github_assignee_login",
        "github_context_refs",
        "github_max_issues",
        "github_max_timeline_events",
        "cron_prompt_context",
    }
)
BBMB_EGRESS_PICKUP_WAIT_SECONDS = 5
BBMB_EGRESS_DRAIN_WAIT_SECONDS = 0
DEFAULT_METRICS_HOST = "127.0.0.1"
DEFAULT_METRICS_PORT = 9464
DEFAULT_TELEGRAM_ATTACHMENT_CLEANUP_GRACE_SECONDS = 7 * 24 * 60 * 60
DEFAULT_TELEGRAM_ATTACHMENT_MAX_AGE_SECONDS = 30 * 24 * 60 * 60
ALLOWED_SCHEDULE_JOB_KEYS = frozenset(
    {
        "content",
        "cron",
        "context_refs",
        "job_name",
        "prompt_context",
        "reply_channel_target",
        "reply_channel_type",
        "timezone",
    }
)
REQUIRED_SCHEDULE_JOB_KEYS = frozenset({"content", "job_name"})
TELEGRAM_MEMORY_TURN_LIMIT = 30
INGRESS_POLL_FAILURE_LOG_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True)
class GitHubIngressSettings:
    repositories: list[str]
    assignee_login: str
    context_refs: list[str]
    max_issues: int
    max_timeline_events: int


@dataclass(frozen=True)
class TelegramAttachmentCleanupSettings:
    attachment_root_dir: str
    cleanup_grace_seconds: int
    max_age_seconds: int


@dataclass(frozen=True)
class DisabledIngressComponent:
    component: str
    error: str


@dataclass
class IngressPollFailureLogState:
    """Track per-connector exception log suppression windows."""

    next_emit_at_monotonic: float
    suppressed_repeats: int = 0


def _log_ingress_poll_failure(
    *,
    connector_name: str,
    loop_count: int,
    state_by_connector: dict[str, IngressPollFailureLogState],
    interval_seconds: float = INGRESS_POLL_FAILURE_LOG_INTERVAL_SECONDS,
) -> None:
    """Log connector poll exceptions with per-connector suppression."""

    now = time.monotonic()
    state = state_by_connector.get(connector_name)
    if state is None or now >= state.next_emit_at_monotonic:
        suppressed_repeats = 0 if state is None else state.suppressed_repeats
        if suppressed_repeats:
            LOGGER.exception(
                "ingress_connector_poll_failed connector=%s loop=%s suppressed_repeats=%s",
                connector_name,
                loop_count,
                suppressed_repeats,
            )
        else:
            LOGGER.exception(
                "ingress_connector_poll_failed connector=%s loop=%s",
                connector_name,
                loop_count,
            )
        state_by_connector[connector_name] = IngressPollFailureLogState(
            next_emit_at_monotonic=now + interval_seconds
        )
        return

    state.suppressed_repeats += 1


def _configure_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def _serialize_optional_datetime(value: datetime | None) -> str:
    if value is None:
        return "none"
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
    parser.add_argument(
        "--auxiliary-ingress-bbmb-address",
        help="Auxiliary-ingress BBMB address host:port.",
    )
    parser.add_argument(
        "--max-loops", type=_positive_int, help="Optional loop limit for smoke runs."
    )
    parser.add_argument(
        "--poll-interval-seconds", type=_positive_float, help="Loop interval."
    )
    parser.add_argument(
        "--poll-timeout-seconds",
        type=_positive_int,
        help="Egress pickup timeout seconds.",
    )
    parser.add_argument("--metrics-host", help="Metrics bind host.")
    parser.add_argument("--metrics-port", type=_positive_int, help="Metrics bind port.")
    parser.add_argument(
        "--allowed-egress-channel",
        action="append",
        default=[],
        help="Allowed egress channel (repeatable).",
    )
    parser.add_argument(
        "--auxiliary-ingress-enabled",
        action="store_true",
        help="Enable auxiliary BBMB-backed JSON ingress.",
    )
    parser.add_argument(
        "--auxiliary-ingress-route",
        action="append",
        default=[],
        help="Auxiliary ingress route in queue_name:path form; handler consumes queue_name.",
    )
    parser.add_argument(
        "--auxiliary-ingress-context-ref",
        action="append",
        default=[],
        help="Context ref to attach to auxiliary ingress tasks (repeatable).",
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
    parser.add_argument(
        "--smtp-starttls", action="store_true", help="Use SMTP STARTTLS."
    )
    parser.add_argument(
        "--error-email-to", help="Email recipient for handler dispatch failures."
    )
    parser.add_argument(
        "--telegram-enabled",
        action="store_true",
        help="Enable Telegram connector+sender.",
    )
    parser.add_argument("--telegram-bot-token-env", help="Telegram token env var name.")
    parser.add_argument("--telegram-api-base-url", help="Telegram API base URL.")
    parser.add_argument(
        "--telegram-poll-timeout-seconds",
        type=_positive_int,
        help="Telegram poll timeout.",
    )
    parser.add_argument(
        "--telegram-attachment-dir",
        help="Local directory for downloaded Telegram photo attachments.",
    )
    parser.add_argument(
        "--telegram-attachment-cleanup-grace-seconds",
        type=_positive_int,
        help="Grace period after task completion before Telegram attachment cleanup.",
    )
    parser.add_argument(
        "--telegram-attachment-max-age-seconds",
        type=_positive_int,
        help="Absolute max age before Telegram attachments are cleaned even without completion.",
    )
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
        help="GitHub repository in owner/repo format for issue-assignment and PR-review ingress (repeatable).",
    )
    parser.add_argument(
        "--github-assignee-login",
        help="GitHub login to filter assigned issues and authored pull request reviews for.",
    )
    parser.add_argument(
        "--github-reply-channel-type", help="Reply channel type for generated tasks."
    )
    parser.add_argument(
        "--github-reply-channel-target",
        help="Reply channel target for generated tasks.",
    )
    parser.add_argument(
        "--github-context-ref",
        action="append",
        default=[],
        help="Context ref to attach to generated tasks (repeatable).",
    )
    parser.add_argument(
        "--github-max-issues",
        type=_positive_int,
        help="Per-repo issue and pull request scan limit.",
    )
    parser.add_argument(
        "--github-max-timeline-events",
        type=_positive_int,
        help="Per-item assigned-event and review scan limit.",
    )
    return parser.parse_args()


def _load_config(
    config_path: str | None, environ: Mapping[str, str] | None = None
) -> dict[str, object]:
    env = os.environ if environ is None else environ
    path = config_path
    if path is None:
        raw = env.get(MESSAGE_HANDLER_CONFIG_PATH_ENV_VAR)
        if raw is not None:
            if not raw.strip():
                raise ValueError(
                    f"{MESSAGE_HANDLER_CONFIG_PATH_ENV_VAR} must not be empty"
                )
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


def _resolve_error_email_recipient(
    args: argparse.Namespace, config: dict[str, object]
) -> str | None:
    if args.error_email_to is not None:
        stripped = args.error_email_to.strip()
        return stripped or None

    configured = config.get("error_email_to")
    if configured is not None:
        if not isinstance(configured, str):
            raise ValueError("config error_email_to must be a string")
        stripped = configured.strip()
        return stripped or None

    for key in ("smtp_username", "imap_username"):
        raw_value = config.get(key)
        if isinstance(raw_value, str):
            stripped = raw_value.strip()
            if stripped:
                return stripped
    return None


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
    store: SQLiteStateStore,
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
    lines.extend(["", "Current user message:", envelope.content])
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
        prompt_context=envelope.prompt_context,
        schema_version=envelope.schema_version,
    )


def _send_egress_dispatch_error_email(
    *,
    email_sender: SmtpEmailSender | None,
    recipient: str | None,
    egress_message: EgressQueueMessage,
    error: MessageDispatchError,
) -> None:
    if email_sender is None or recipient is None:
        return

    subject = f"Chatting handler dispatch error: {error.reason_code}"
    body = "\n".join(
        [
            "Message-handler egress dispatch failed.",
            f"task_id: {egress_message.task_id}",
            f"envelope_id: {egress_message.envelope_id}",
            f"trace_id: {egress_message.trace_id}",
            f"event_id: {egress_message.event_id}",
            f"sequence: {egress_message.sequence}",
            f"event_kind: {egress_message.event_kind}",
            f"channel: {egress_message.message.channel}",
            f"target: {egress_message.message.target}",
            f"reason_code: {error.reason_code}",
            "",
            "Traceback:",
            "".join(traceback.format_exception(error)).rstrip(),
        ]
    )
    try:
        email_sender.send(recipient, body, subject=subject)
    except Exception:  # noqa: BLE001
        LOGGER.exception(
            "egress_dispatch_error_email_failed task_id=%s event_id=%s reason=%s recipient=%s",
            egress_message.task_id,
            egress_message.event_id,
            error.reason_code,
            recipient,
        )


def _resolve_str(
    cli_value: str | None,
    config_value: object,
    *,
    default_value: str,
    setting_name: str,
) -> str:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        return default_value
    if not isinstance(config_value, str) or not config_value.strip():
        raise ValueError(f"config {setting_name} must be a non-empty string")
    return config_value


def _resolve_optional_str(
    cli_value: str | None,
    config_value: object,
    *,
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
    cli_value: int | None,
    config_value: object,
    *,
    default_value: int,
    setting_name: str,
) -> int:
    if cli_value is not None:
        return cli_value
    if config_value is None:
        return default_value
    if (
        not isinstance(config_value, int)
        or isinstance(config_value, bool)
        or config_value <= 0
    ):
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


def _resolve_bool(
    cli_value: bool | None,
    config_value: object,
    *,
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


def _resolve_allowed_egress_channels(
    args: argparse.Namespace, config: dict[str, object]
) -> set[str]:
    config_values: list[str] = []
    raw_config_values = config.get("allowed_egress_channels")
    if raw_config_values is not None:
        if not isinstance(raw_config_values, list) or not all(
            isinstance(item, str) for item in raw_config_values
        ):
            raise ValueError("config allowed_egress_channels must be a list of strings")
        config_values = list(raw_config_values)

    merged = [*config_values, *args.allowed_egress_channel]
    if not merged:
        return {"email", "telegram", "telegram_reaction", "log"}
    if any(not item.strip() for item in merged):
        raise ValueError("allowed_egress_channel entries must not be empty")
    return set(merged)


def _resolve_required_str(
    cli_value: str | None, config_value: object, *, setting_name: str
) -> str:
    resolved = _resolve_str(
        cli_value,
        config_value,
        default_value="",
        setting_name=setting_name,
    ).strip()
    if not resolved:
        raise ValueError(
            f"{setting_name} is required when github_repositories is configured"
        )
    return resolved


def _resolve_context_refs(
    args_values: list[str], config: dict[str, object]
) -> list[str]:
    raw_config_values = config.get("context_ref")
    if raw_config_values is None:
        raw_config_values = config.get("context_refs")

    if raw_config_values is None:
        config_values: list[str] = []
    else:
        if not isinstance(raw_config_values, list):
            raise ValueError(
                "config context_ref/context_refs must be a list of strings"
            )
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError(
                "config context_ref/context_refs must be a list of strings"
            )
        config_values = list(raw_config_values)

    merged_values = [*config_values, *args_values]
    if any(not value.strip() for value in merged_values):
        raise ValueError("context_ref/context_refs entries must not be empty")
    return merged_values


def _resolve_auxiliary_ingress_context_refs(
    args: argparse.Namespace,
    config: dict[str, object],
) -> list[str]:
    raw_config_values = config.get("auxiliary_ingress_context_refs")
    if raw_config_values is None:
        config_values = _resolve_context_refs(args.context_ref, config)
    else:
        if not isinstance(raw_config_values, list):
            raise ValueError(
                "config auxiliary_ingress_context_refs must be a list of strings"
            )
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError(
                "config auxiliary_ingress_context_refs must be a list of strings"
            )
        config_values = list(raw_config_values)

    merged_values = [*config_values, *args.auxiliary_ingress_context_ref]
    if any(not value.strip() for value in merged_values):
        raise ValueError("auxiliary_ingress_context_ref entries must not be empty")
    return [value.strip() for value in merged_values]


def _parse_auxiliary_ingress_route(raw_value: str) -> tuple[str, str]:
    raw = raw_value.strip()
    if not raw:
        raise ValueError("auxiliary_ingress_routes entries must not be empty")
    queue_name, separator, path_value = raw.partition(":")
    if not separator:
        raise ValueError(
            "auxiliary_ingress_routes entries must use queue_name:path format"
        )
    queue_name = queue_name.strip()
    if not queue_name:
        raise ValueError("auxiliary_ingress_routes queue_name must not be empty")
    if not path_value.strip():
        raise ValueError("auxiliary_ingress_routes path must not be empty")
    return queue_name, path_value.strip()


def _resolve_auxiliary_ingress_queue_names(
    args: argparse.Namespace,
    config: dict[str, object],
) -> list[str]:
    queue_names: list[str] = []
    raw_config_routes = config.get("auxiliary_ingress_routes")
    if raw_config_routes is not None:
        if not isinstance(raw_config_routes, list) or not all(
            isinstance(item, str) for item in raw_config_routes
        ):
            raise ValueError(
                "config auxiliary_ingress_routes must be a list of queue_name:path strings"
            )
        queue_names.extend(
            queue_name
            for queue_name, _path in (
                _parse_auxiliary_ingress_route(item) for item in raw_config_routes
            )
        )

    queue_names.extend(
        queue_name
        for queue_name, _path in (
            _parse_auxiliary_ingress_route(item)
            for item in getattr(args, "auxiliary_ingress_route", [])
        )
    )

    if not queue_names:
        raise ValueError(
            "auxiliary ingress requires auxiliary_ingress_routes or --auxiliary-ingress-route"
        )

    deduped: list[str] = []
    seen: set[str] = set()
    for queue_name in queue_names:
        if queue_name in seen:
            continue
        seen.add(queue_name)
        deduped.append(queue_name)
    return deduped


def _resolve_auxiliary_ingress_bbmb_address(
    args: argparse.Namespace,
    config: dict[str, object],
) -> str:
    return _resolve_str(
        getattr(args, "auxiliary_ingress_bbmb_address", None),
        config.get("auxiliary_ingress_bbmb_address"),
        default_value=_resolve_str(
            args.bbmb_address,
            config.get("bbmb_address"),
            default_value="127.0.0.1:9876",
            setting_name="bbmb_address",
        ),
        setting_name="auxiliary_ingress_bbmb_address",
    )


def _resolve_prompt_context_values(
    config: dict[str, object], *, setting_name: str
) -> list[str]:
    raw_values = config.get(setting_name, [])
    if raw_values is None:
        return []
    if not isinstance(raw_values, list):
        raise ValueError(f"config {setting_name} must be a list of strings")
    if not all(isinstance(item, str) for item in raw_values):
        raise ValueError(f"config {setting_name} must be a list of strings")
    if any(not item.strip() for item in raw_values):
        raise ValueError(f"config {setting_name} entries must not be empty")
    return [item.strip() for item in raw_values]


def _resolve_github_repositories(
    args: argparse.Namespace, config: dict[str, object]
) -> list[str]:
    repositories: list[str] = []
    config_values = config.get("github_repositories")
    if config_values is not None:
        if not isinstance(config_values, list) or not all(
            isinstance(item, str) for item in config_values
        ):
            raise ValueError(
                "config github_repositories must be a list of owner/repo strings"
            )
        repositories.extend(config_values)
    repositories.extend(args.github_repository)

    deduped: list[str] = []
    seen: set[str] = set()
    for repository in repositories:
        if not repository.strip():
            raise ValueError("github_repositories entries must not be empty")
        parts = repository.strip().split("/", maxsplit=1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                "github_repositories entries must be owner/repo or owner/*"
            )
        owner, name = parts
        if name != "*" and "*" in name:
            raise ValueError(
                "github_repositories entries must be owner/repo or owner/*"
            )
        normalized = f"{owner}/{name}"
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _resolve_github_context_refs(
    args: argparse.Namespace, config: dict[str, object]
) -> list[str]:
    context_refs: list[str] = []
    config_values = config.get("github_context_refs")
    if config_values is not None:
        if not isinstance(config_values, list) or not all(
            isinstance(item, str) for item in config_values
        ):
            raise ValueError("config github_context_refs must be a list of strings")
        context_refs.extend(config_values)
    context_refs.extend(args.github_context_ref)
    if any(not item.strip() for item in context_refs):
        raise ValueError("github_context_ref entries must not be empty")
    return [item.strip() for item in context_refs]


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
            raise ValueError(
                "config telegram_allowed_chat_ids must be a list of strings"
            )
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError(
                "config telegram_allowed_chat_ids must be a list of strings"
            )
        config_values = list(raw_config_values)

    merged_values = [*config_values, *args.telegram_allowed_chat_id]
    if any(not value.strip() for value in merged_values):
        raise ValueError("telegram_allowed_chat_id(s) entries must not be empty")
    if not merged_values:
        return None
    return merged_values


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
            raise ValueError(
                "config telegram_allowed_channel_ids must be a list of strings"
            )
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError(
                "config telegram_allowed_channel_ids must be a list of strings"
            )
        config_values = list(raw_config_values)

    merged_values = [*config_values, *args.telegram_allowed_channel_id]
    if any(not value.strip() for value in merged_values):
        raise ValueError("telegram_allowed_channel_id(s) entries must not be empty")
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
        getattr(args, "telegram_attachment_dir", None),
        config.get("telegram_attachment_dir"),
        default_value=str(
            Path(tempfile.gettempdir()) / "chatting-telegram-attachments"
        ),
        setting_name="telegram_attachment_dir",
    )


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
        context_refs=_resolve_github_context_refs(args, config),
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


def _resolve_telegram_attachment_cleanup_settings(
    args: argparse.Namespace,
    config: dict[str, object],
) -> TelegramAttachmentCleanupSettings | None:
    cleanup_keys = {
        "telegram_attachment_dir",
        "telegram_attachment_cleanup_grace_seconds",
        "telegram_attachment_max_age_seconds",
    }
    explicit_cleanup_configured = (
        args.telegram_attachment_dir is not None
        or args.telegram_attachment_cleanup_grace_seconds is not None
        or args.telegram_attachment_max_age_seconds is not None
        or any(key in config for key in cleanup_keys)
    )
    if (
        not _is_connector_configured(args, config, connector="telegram")
        and not explicit_cleanup_configured
    ):
        return None

    cleanup_grace_seconds = _resolve_positive_int(
        args.telegram_attachment_cleanup_grace_seconds,
        config.get("telegram_attachment_cleanup_grace_seconds"),
        default_value=DEFAULT_TELEGRAM_ATTACHMENT_CLEANUP_GRACE_SECONDS,
        setting_name="telegram_attachment_cleanup_grace_seconds",
    )
    max_age_seconds = _resolve_positive_int(
        args.telegram_attachment_max_age_seconds,
        config.get("telegram_attachment_max_age_seconds"),
        default_value=DEFAULT_TELEGRAM_ATTACHMENT_MAX_AGE_SECONDS,
        setting_name="telegram_attachment_max_age_seconds",
    )
    if max_age_seconds < cleanup_grace_seconds:
        raise ValueError(
            "telegram_attachment_max_age_seconds must be greater than or equal to "
            "telegram_attachment_cleanup_grace_seconds"
        )
    return TelegramAttachmentCleanupSettings(
        attachment_root_dir=_resolve_telegram_attachment_dir(args, config),
        cleanup_grace_seconds=cleanup_grace_seconds,
        max_age_seconds=max_age_seconds,
    )


def _is_connector_configured(
    args: argparse.Namespace, config: dict[str, object], *, connector: str
) -> bool:
    if connector == "auxiliary_ingress":
        if args.auxiliary_ingress_enabled:
            return True
        if "auxiliary_ingress_enabled" not in config:
            return False
        configured_value = config.get("auxiliary_ingress_enabled")
        return configured_value is not None and configured_value is not False
    if connector == "schedule":
        return args.schedule_file is not None or config.get("schedule_file") is not None
    if connector == "imap":
        return args.imap_host is not None or config.get("imap_host") is not None
    if connector == "telegram":
        if args.telegram_enabled:
            return True
        if "telegram_enabled" not in config:
            return False
        configured_value = config.get("telegram_enabled")
        return configured_value is not None and configured_value is not False
    if connector == "github":
        return (
            bool(args.github_repository)
            or config.get("github_repositories") is not None
        )
    raise ValueError(f"unsupported connector: {connector}")


def _build_live_connectors(
    args: argparse.Namespace, config: dict[str, object]
) -> list[Connector]:
    connectors: list[Connector] = []
    global_prompt_context = _resolve_prompt_context_values(
        config, setting_name="prompt_context"
    )
    email_prompt_context = _resolve_prompt_context_values(
        config,
        setting_name="email_prompt_context",
    )
    telegram_prompt_context = _resolve_prompt_context_values(
        config,
        setting_name="telegram_prompt_context",
    )
    cron_prompt_context = _resolve_prompt_context_values(
        config, setting_name="cron_prompt_context"
    )

    auxiliary_ingress_enabled = _resolve_bool(
        getattr(args, "auxiliary_ingress_enabled", None),
        config.get("auxiliary_ingress_enabled"),
        default_value=False,
        setting_name="auxiliary_ingress_enabled",
    )
    if auxiliary_ingress_enabled:
        auxiliary_queue_names = _resolve_auxiliary_ingress_queue_names(args, config)
        auxiliary_broker = BBMBQueueAdapter(
            address=_resolve_auxiliary_ingress_bbmb_address(args, config)
        )
        auxiliary_context_refs = _resolve_auxiliary_ingress_context_refs(args, config)
        auxiliary_prompt_context = PromptContext(
            global_instructions=global_prompt_context
        )
        for auxiliary_queue_name in auxiliary_queue_names:
            auxiliary_broker.ensure_queue(auxiliary_queue_name)
            connectors.append(
                AuxiliaryIngressConnector(
                    broker=auxiliary_broker,
                    queue_name=auxiliary_queue_name,
                    reply_target=auxiliary_queue_name,
                    context_refs=auxiliary_context_refs,
                    prompt_context=auxiliary_prompt_context,
                )
            )

    schedule_file = _resolve_optional_str(
        args.schedule_file,
        config.get("schedule_file"),
        setting_name="schedule_file",
    )
    if schedule_file:
        connectors.append(
            IntervalScheduleConnector(
                jobs=_load_schedule_jobs(schedule_file),
                global_prompt_context=global_prompt_context,
                source_prompt_context=cron_prompt_context,
            )
        )

    imap_host = _resolve_optional_str(
        args.imap_host,
        config.get("imap_host"),
        setting_name="imap_host",
    )
    if imap_host:
        imap_username = _resolve_optional_str(
            args.imap_username,
            config.get("imap_username"),
            setting_name="imap_username",
        )
        if not imap_username:
            raise ValueError("--imap-username is required when --imap-host is set")
        imap_password_env = _resolve_str(
            args.imap_password_env,
            config.get("imap_password_env"),
            default_value="CHATTING_IMAP_PASSWORD",
            setting_name="imap_password_env",
        )
        password = os.environ.get(imap_password_env, "")
        if not password:
            raise ValueError(f"missing IMAP password env var: {imap_password_env}")
        connectors.append(
            ImapEmailConnector(
                host=imap_host,
                port=_resolve_positive_int(
                    args.imap_port,
                    config.get("imap_port"),
                    default_value=993,
                    setting_name="imap_port",
                ),
                username=imap_username,
                password=password,
                mailbox=_resolve_str(
                    args.imap_mailbox,
                    config.get("imap_mailbox"),
                    default_value="INBOX",
                    setting_name="imap_mailbox",
                ),
                search_criterion=_resolve_str(
                    args.imap_search,
                    config.get("imap_search"),
                    default_value="UNSEEN",
                    setting_name="imap_search",
                ),
                use_ssl=_resolve_bool(
                    None,
                    config.get("imap_use_ssl"),
                    default_value=True,
                    setting_name="imap_use_ssl",
                ),
                context_refs=_resolve_context_refs(args.context_ref, config),
                prompt_context=PromptContext(
                    global_instructions=global_prompt_context,
                    reply_channel_instructions=email_prompt_context,
                ),
            )
        )

    telegram_enabled = _resolve_bool(
        args.telegram_enabled,
        config.get("telegram_enabled"),
        default_value=False,
        setting_name="telegram_enabled",
    )
    if telegram_enabled:
        telegram_bot_token_env = _resolve_str(
            args.telegram_bot_token_env,
            config.get("telegram_bot_token_env"),
            default_value="CHATTING_TELEGRAM_BOT_TOKEN",
            setting_name="telegram_bot_token_env",
        )
        bot_token = os.environ.get(telegram_bot_token_env, "")
        if not bot_token:
            raise ValueError(
                f"missing Telegram bot token env var: {telegram_bot_token_env}"
            )
        connectors.append(
            TelegramConnector(
                bot_token=bot_token,
                api_base_url=_resolve_str(
                    args.telegram_api_base_url,
                    config.get("telegram_api_base_url"),
                    default_value="https://api.telegram.org",
                    setting_name="telegram_api_base_url",
                ),
                poll_timeout_seconds=_resolve_positive_int(
                    args.telegram_poll_timeout_seconds,
                    config.get("telegram_poll_timeout_seconds"),
                    default_value=20,
                    setting_name="telegram_poll_timeout_seconds",
                ),
                allowed_chat_ids=_resolve_telegram_allowed_chat_ids(args, config),
                allowed_channel_ids=_resolve_telegram_allowed_channel_ids(args, config),
                context_refs=_resolve_telegram_context_refs(args, config),
                prompt_context=PromptContext(
                    global_instructions=global_prompt_context,
                    reply_channel_instructions=telegram_prompt_context,
                ),
                attachment_root_dir=_resolve_telegram_attachment_dir(args, config),
            )
        )

    if not connectors:
        raise ValueError("live mode requires at least one configured ingress connector")
    return connectors


def _build_email_sender(
    args: argparse.Namespace, config: dict[str, object]
) -> SmtpEmailSender | None:
    smtp_host = _resolve_optional_str(
        args.smtp_host,
        config.get("smtp_host"),
        setting_name="smtp_host",
    )
    if not smtp_host:
        return None

    smtp_username = _resolve_optional_str(
        args.smtp_username,
        config.get("smtp_username"),
        setting_name="smtp_username",
    )
    from_address = (
        _resolve_optional_str(
            args.smtp_from,
            config.get("smtp_from"),
            setting_name="smtp_from",
        )
        or smtp_username
    )
    if not from_address:
        raise ValueError(
            "--smtp-from or --smtp-username is required when --smtp-host is set"
        )

    password = None
    if smtp_username:
        smtp_password_env = _resolve_str(
            args.smtp_password_env,
            config.get("smtp_password_env"),
            default_value="CHATTING_SMTP_PASSWORD",
            setting_name="smtp_password_env",
        )
        password = os.environ.get(smtp_password_env, "")
        if not password:
            raise ValueError(f"missing SMTP password env var: {smtp_password_env}")

    smtp_starttls = _resolve_bool(
        args.smtp_starttls,
        config.get("smtp_starttls"),
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
            args.smtp_port,
            config.get("smtp_port"),
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
        args.telegram_enabled,
        config.get("telegram_enabled"),
        default_value=False,
        setting_name="telegram_enabled",
    )
    if not telegram_enabled:
        return None

    telegram_bot_token_env = _resolve_str(
        args.telegram_bot_token_env,
        config.get("telegram_bot_token_env"),
        default_value="CHATTING_TELEGRAM_BOT_TOKEN",
        setting_name="telegram_bot_token_env",
    )
    bot_token = os.environ.get(telegram_bot_token_env, "")
    if not bot_token:
        raise ValueError(
            f"missing Telegram bot token env var: {telegram_bot_token_env}"
        )

    return TelegramMessageSender(
        bot_token=bot_token,
        api_base_url=_resolve_str(
            args.telegram_api_base_url,
            config.get("telegram_api_base_url"),
            default_value="https://api.telegram.org",
            setting_name="telegram_api_base_url",
        ),
    )


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
            raise ValueError(
                f"schedule job at index {index} contains unknown keys: {', '.join(unknown_keys)}"
            )

        missing_keys = sorted(REQUIRED_SCHEDULE_JOB_KEYS - set(raw_job.keys()))
        if missing_keys:
            raise ValueError(
                f"schedule job at index {index} is missing required keys: {', '.join(missing_keys)}"
            )

        job_name = raw_job["job_name"]
        if not isinstance(job_name, str) or not job_name.strip():
            raise ValueError(
                f"schedule job at index {index} job_name must be a non-empty string"
            )
        content = raw_job["content"]
        if not isinstance(content, str) or not content.strip():
            raise ValueError(
                f"schedule job at index {index} content must be a non-empty string"
            )

        cron = raw_job.get("cron")
        if not isinstance(cron, str) or not cron.strip():
            raise ValueError(
                f"schedule job at index {index} cron must be a non-empty string"
            )

        timezone_name = raw_job.get("timezone")
        if timezone_name is None:
            timezone_name = "UTC"
        elif not isinstance(timezone_name, str) or not timezone_name.strip():
            raise ValueError(
                f"schedule job at index {index} timezone must be a non-empty string"
            )

        raw_context_refs = raw_job.get("context_refs", [])
        if not isinstance(raw_context_refs, list) or not all(
            isinstance(item, str) and item.strip() for item in raw_context_refs
        ):
            raise ValueError(
                f"schedule job at index {index} context_refs must be a list of non-empty strings"
            )

        raw_prompt_context = raw_job.get("prompt_context", [])
        if not isinstance(raw_prompt_context, list) or not all(
            isinstance(item, str) and item.strip() for item in raw_prompt_context
        ):
            raise ValueError(
                f"schedule job at index {index} prompt_context must be a list of non-empty strings"
            )

        reply_channel_type = raw_job.get("reply_channel_type")
        if reply_channel_type is not None and (
            not isinstance(reply_channel_type, str) or not reply_channel_type.strip()
        ):
            raise ValueError(
                f"schedule job at index {index} reply_channel_type must be a non-empty string"
            )
        reply_channel_target = raw_job.get("reply_channel_target")
        if reply_channel_target is not None and (
            not isinstance(reply_channel_target, str)
            or not reply_channel_target.strip()
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
                prompt_context=list(raw_prompt_context),
                cron=cron.strip(),
                timezone_name=timezone_name.strip()
                if isinstance(timezone_name, str)
                else None,
                reply_channel_type=reply_channel_type.strip()
                if isinstance(reply_channel_type, str)
                else None,
                reply_channel_target=reply_channel_target.strip()
                if isinstance(reply_channel_target, str)
                else None,
            )
        )
    return jobs
def _build_live_connectors_fail_open(
    args: argparse.Namespace,
    config: dict[str, object],
    *,
    db_path: str,
) -> tuple[list[Connector], list[DisabledIngressComponent]]:
    connectors: list[Connector] = [InternalHeartbeatConnector()]
    disabled_components: list[DisabledIngressComponent] = []

    connector_args: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
        "schedule": (
            ("schedule_file",),
            ("schedule_file", "prompt_context", "cron_prompt_context"),
        ),
        "imap": (
            (
                "imap_host",
                "imap_port",
                "imap_username",
                "imap_password_env",
                "imap_mailbox",
                "imap_search",
                "context_ref",
            ),
            (
                "prompt_context",
                "email_prompt_context",
                "imap_host",
                "imap_port",
                "imap_username",
                "imap_password_env",
                "imap_mailbox",
                "imap_search",
                "imap_use_ssl",
                "context_ref",
                "context_refs",
            ),
        ),
        "telegram": (
            (
                "telegram_enabled",
                "telegram_bot_token_env",
                "telegram_api_base_url",
                "telegram_poll_timeout_seconds",
                "telegram_attachment_dir",
                "telegram_allowed_chat_id",
                "telegram_allowed_channel_id",
                "telegram_context_ref",
                "context_ref",
            ),
            (
                "telegram_enabled",
                "telegram_bot_token_env",
                "telegram_api_base_url",
                "telegram_poll_timeout_seconds",
                "telegram_attachment_dir",
                "telegram_allowed_chat_ids",
                "telegram_allowed_channel_ids",
                "prompt_context",
                "telegram_prompt_context",
                "telegram_context_refs",
                "context_ref",
                "context_refs",
            ),
        ),
        "auxiliary_ingress": (
            (
                "auxiliary_ingress_enabled",
                "auxiliary_ingress_route",
                "auxiliary_ingress_context_ref",
                "context_ref",
            ),
            (
                "auxiliary_ingress_enabled",
                "auxiliary_ingress_routes",
                "auxiliary_ingress_context_refs",
                "prompt_context",
                "context_ref",
                "context_refs",
                "bbmb_address",
                "auxiliary_ingress_bbmb_address",
            ),
        ),
        "github": (
            (
                "github_repository",
                "github_assignee_login",
                "github_context_ref",
                "github_max_issues",
                "github_max_timeline_events",
            ),
            (
                "github_repositories",
                "github_assignee_login",
                "github_context_refs",
                "github_max_issues",
                "github_max_timeline_events",
            ),
        ),
    }

    base_args = vars(args).copy()
    base_args.update(
        {
            "schedule_file": None,
            "imap_host": None,
            "imap_port": None,
            "imap_username": None,
            "imap_password_env": None,
            "imap_mailbox": None,
            "imap_search": None,
            "auxiliary_ingress_enabled": False,
            "auxiliary_ingress_route": [],
            "auxiliary_ingress_context_ref": [],
            "telegram_enabled": False,
            "telegram_bot_token_env": None,
            "telegram_api_base_url": None,
            "telegram_poll_timeout_seconds": None,
            "telegram_attachment_dir": None,
            "telegram_allowed_chat_id": [],
            "telegram_allowed_channel_id": [],
            "telegram_context_ref": [],
            "context_ref": [],
            "github_repository": [],
            "github_assignee_login": None,
            "github_context_ref": [],
            "github_max_issues": None,
            "github_max_timeline_events": None,
        }
    )

    for connector_name in (
        "schedule",
        "imap",
        "telegram",
        "auxiliary_ingress",
        "github",
    ):
        if not _is_connector_configured(args, config, connector=connector_name):
            continue
        selected_arg_keys, selected_config_keys = connector_args[connector_name]
        scoped_args_payload = base_args.copy()
        for key in selected_arg_keys:
            value = getattr(args, key)
            if isinstance(value, list):
                scoped_args_payload[key] = list(value)
            else:
                scoped_args_payload[key] = value
        scoped_args = argparse.Namespace(**scoped_args_payload)
        scoped_config = {
            key: config[key] for key in selected_config_keys if key in config
        }
        try:
            if connector_name == "github":
                settings = _resolve_github_ingress_settings(scoped_args, scoped_config)
                if settings is None:
                    continue
                connectors.append(
                    GitHubIssueAssignmentConnector(
                        repository_patterns=settings.repositories,
                        assignee_login=settings.assignee_login,
                        context_refs=settings.context_refs,
                        max_issues=settings.max_issues,
                        max_timeline_events=settings.max_timeline_events,
                        checkpoint_store=GitHubAssignmentCheckpointStore(db_path),
                        graphql_runner=default_graphql_runner,
                    )
                )
                connectors.append(
                    GitHubPullRequestReviewConnector(
                        repository_patterns=settings.repositories,
                        author_login=settings.assignee_login,
                        context_refs=settings.context_refs,
                        max_pull_requests=settings.max_issues,
                        max_reviews=settings.max_timeline_events,
                        checkpoint_store=GitHubAssignmentCheckpointStore(db_path),
                        graphql_runner=default_graphql_runner,
                    )
                )
                continue
            connectors.extend(_build_live_connectors(scoped_args, scoped_config))
        except Exception as error:  # noqa: BLE001
            LOGGER.exception(
                "ingress_connector_startup_failed connector=%s", connector_name
            )
            disabled_components.append(
                DisabledIngressComponent(
                    component=connector_name,
                    error=str(error),
                )
            )
    return connectors, disabled_components


def _build_policy_decision_for_message(
    egress_message: EgressQueueMessage,
) -> PolicyDecision:
    return PolicyDecision(
        approved_actions=[],
        blocked_actions=[],
        approved_messages=[egress_message.message],
        reason_codes=[],
    )


def _build_github_sender() -> GitHubIssueCommentSender | None:
    if shutil.which("gh") is None:
        return None
    return GitHubIssueCommentSender()


def _is_completion_event(egress_message: EgressQueueMessage) -> bool:
    return egress_message.event_kind == "completion"


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
    attachment_store: TelegramAttachmentStore | None = None,
    attachment_cleanup_settings: TelegramAttachmentCleanupSettings | None = None,
    error_email_sender: SmtpEmailSender | None = None,
    error_email_recipient: str | None = None,
    heartbeat_telemetry: HeartbeatTelemetryRollup | None = None,
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

    if ledger.is_task_completed(
        task_id=egress_message.task_id,
        envelope_id=egress_message.envelope_id,
    ):
        if telemetry is not None:
            telemetry.record_dropped(reason="completed_task")
        LOGGER.error(
            "egress_drop_completed_task task_id=%s envelope_id=%s guid=%s",
            egress_message.task_id,
            egress_message.envelope_id,
            picked_guid,
        )
        _ack_and_mark_outbox(egress_message.event_id)
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

    internal_heartbeat = is_internal_heartbeat_envelope(
        ledger_record.task_message.envelope
    )
    heartbeat_log_message = (
        internal_heartbeat
        and egress_message.message.channel == "log"
        and egress_message.message.target == INTERNAL_HEARTBEAT_TARGET
    )
    completion_event = _is_completion_event(egress_message)
    if (
        egress_message.message.channel not in allowed_egress_channels
        and not heartbeat_log_message
        and not completion_event
    ):
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

    if store.has_dispatched_event_id(
        task_id=egress_message.task_id, event_id=egress_message.event_id
    ):
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

    if egress_message.sequence is None:
        _ack_and_mark_outbox(egress_message.event_id)
        try:
            _dispatch_unsequenced_egress(
                egress_message=egress_message,
                ledger_record=ledger_record,
                store=store,
                attachment_store=attachment_store,
                attachment_cleanup_settings=attachment_cleanup_settings,
                applier=applier,
                telemetry=telemetry,
            )
        except MessageDispatchError as error:
            if telemetry is not None:
                telemetry.record_dropped(reason="dispatch_failed")
            LOGGER.exception(
                "egress_drop_dispatch_failed task_id=%s event_id=%s sequence=%s event_kind=%s channel=%s reason=%s",
                egress_message.task_id,
                egress_message.event_id,
                egress_message.sequence,
                egress_message.event_kind,
                egress_message.message.channel,
                error.reason_code,
            )
            _send_egress_dispatch_error_email(
                email_sender=error_email_sender,
                recipient=error_email_recipient,
                egress_message=egress_message,
                error=error,
            )
        return

    ledger.stage_egress_event(egress_message)
    _ack_and_mark_outbox(egress_message.event_id)
    _flush_task_egress_in_sequence(
        task_id=egress_message.task_id,
        ledger=ledger,
        store=store,
        attachment_store=attachment_store,
        attachment_cleanup_settings=attachment_cleanup_settings,
        applier=applier,
        error_email_sender=error_email_sender,
        error_email_recipient=error_email_recipient,
        heartbeat_telemetry=heartbeat_telemetry,
        telemetry=telemetry,
    )


def _drain_egress_queue(
    *,
    broker: BBMBQueueAdapter,
    poll_timeout_seconds: int,
    ledger: TaskLedgerStore,
    store: SQLiteStateStore,
    allowed_egress_channels: set[str],
    applier: IntegratedApplier,
    attachment_store: TelegramAttachmentStore | None = None,
    attachment_cleanup_settings: TelegramAttachmentCleanupSettings | None = None,
    error_email_sender: SmtpEmailSender | None = None,
    error_email_recipient: str | None = None,
    heartbeat_telemetry: HeartbeatTelemetryRollup | None = None,
    telemetry: EgressTelemetryRollup | None = None,
) -> int:
    drained = 0
    wait_seconds = BBMB_EGRESS_PICKUP_WAIT_SECONDS

    while True:
        egress_picked = broker.pickup_json(
            EGRESS_QUEUE_NAME,
            timeout_seconds=poll_timeout_seconds,
            wait_seconds=wait_seconds,
        )
        if egress_picked is None:
            return drained

        drained += 1
        _handle_egress_message(
            picked_guid=egress_picked.guid,
            picked_payload=egress_picked.payload,
            ledger=ledger,
            store=store,
            attachment_store=attachment_store,
            attachment_cleanup_settings=attachment_cleanup_settings,
            allowed_egress_channels=allowed_egress_channels,
            applier=applier,
            ack_callback=lambda guid: broker.ack(EGRESS_QUEUE_NAME, guid),
            error_email_sender=error_email_sender,
            error_email_recipient=error_email_recipient,
            heartbeat_telemetry=heartbeat_telemetry,
            telemetry=telemetry,
        )
        wait_seconds = BBMB_EGRESS_DRAIN_WAIT_SECONDS


def _dispatch_unsequenced_egress(
    *,
    egress_message: EgressQueueMessage,
    ledger_record: TaskLedgerRecord,
    store: SQLiteStateStore,
    attachment_store: TelegramAttachmentStore | None,
    attachment_cleanup_settings: TelegramAttachmentCleanupSettings | None,
    applier: IntegratedApplier,
    telemetry: EgressTelemetryRollup | None,
) -> None:
    decision = _build_policy_decision_for_message(egress_message)
    apply_result = applier.apply(
        decision,
        envelope=ledger_record.task_message.envelope,
    )
    _record_outbound_telegram_attachments(
        egress_message=egress_message,
        attachment_store=attachment_store,
        attachment_cleanup_settings=attachment_cleanup_settings,
        dispatched_messages=apply_result.dispatched_messages,
    )
    if egress_message.event_id is not None:
        store.mark_dispatched_event_id(
            task_id=egress_message.task_id,
            event_id=egress_message.event_id,
        )
    dispatch_latency_ms = int(
        (
            datetime.now(timezone.utc)
            - egress_message.emitted_at.astimezone(timezone.utc)
        ).total_seconds()
        * 1000
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
            if (
                message.target
                != ledger_record.task_message.envelope.reply_channel.target
            ):
                continue
            if message.body is None:
                continue
            store.append_conversation_turn(
                channel="telegram",
                target=message.target,
                role="assistant",
                content=message.body,
                run_id=egress_message.task_id,
            )
    LOGGER.info(
        "egress_dispatched task_id=%s event_id=%s sequence=%s event_kind=%s channel=%s",
        egress_message.task_id,
        egress_message.event_id,
        egress_message.sequence,
        egress_message.event_kind,
        egress_message.message.channel,
    )


def _apply_completion_event(
    *,
    egress_message: EgressQueueMessage,
    ledger: TaskLedgerStore,
    store: SQLiteStateStore,
    attachment_store: TelegramAttachmentStore | None,
    attachment_cleanup_settings: TelegramAttachmentCleanupSettings | None,
    telemetry: EgressTelemetryRollup | None,
) -> None:
    if egress_message.event_id is not None:
        store.mark_dispatched_event_id(
            task_id=egress_message.task_id,
            event_id=egress_message.event_id,
        )
    dispatch_latency_ms = int(
        (
            datetime.now(timezone.utc)
            - egress_message.emitted_at.astimezone(timezone.utc)
        ).total_seconds()
        * 1000
    )
    if telemetry is not None:
        telemetry.record_dispatched(
            event_kind=egress_message.event_kind,
            latency_ms=max(dispatch_latency_ms, 0),
        )
    ledger.mark_task_completed(
        task_id=egress_message.task_id,
        envelope_id=egress_message.envelope_id,
        trace_id=egress_message.trace_id,
    )
    if attachment_store is not None and attachment_cleanup_settings is not None:
        eligible_after = datetime.now(timezone.utc) + timedelta(
            seconds=attachment_cleanup_settings.cleanup_grace_seconds
        )
        tracked_count = attachment_store.mark_task_attachments_eligible(
            task_id=egress_message.task_id,
            eligible_after=eligible_after,
        )
        if tracked_count:
            LOGGER.info(
                "telegram_attachment_cleanup_eligible task_id=%s tracked_attachments=%s eligible_after=%s",
                egress_message.task_id,
                tracked_count,
                _serialize_optional_datetime(eligible_after),
            )
    LOGGER.info(
        "egress_completion_applied task_id=%s event_id=%s sequence=%s",
        egress_message.task_id,
        egress_message.event_id,
        egress_message.sequence,
    )


def _record_outbound_telegram_attachments(
    *,
    egress_message: EgressQueueMessage,
    attachment_store: TelegramAttachmentStore | None,
    attachment_cleanup_settings: TelegramAttachmentCleanupSettings | None,
    dispatched_messages: list[OutboundMessage],
) -> None:
    if attachment_store is None or attachment_cleanup_settings is None:
        return

    tracked_count = 0
    for dispatched_message in dispatched_messages:
        attachment = getattr(dispatched_message, "attachment", None)
        channel = getattr(dispatched_message, "channel", None)
        if channel != "telegram" or attachment is None:
            continue
        if attachment_store.record_outbound_attachment(
            task_id=egress_message.task_id,
            envelope_id=egress_message.envelope_id,
            attachment=attachment,
            attachment_root_dir=attachment_cleanup_settings.attachment_root_dir,
        ):
            tracked_count += 1

    if tracked_count:
        LOGGER.info(
            "telegram_attachment_tracked task_id=%s tracked_attachments=%s source=egress",
            egress_message.task_id,
            tracked_count,
        )


def _flush_task_egress_in_sequence(
    *,
    task_id: str,
    ledger: TaskLedgerStore,
    store: SQLiteStateStore,
    attachment_store: TelegramAttachmentStore | None,
    attachment_cleanup_settings: TelegramAttachmentCleanupSettings | None,
    applier: IntegratedApplier,
    error_email_sender: SmtpEmailSender | None = None,
    error_email_recipient: str | None = None,
    heartbeat_telemetry: HeartbeatTelemetryRollup | None = None,
    telemetry: EgressTelemetryRollup | None = None,
) -> None:
    ledger_record = ledger.get_task(task_id)
    if ledger_record is None:
        return

    while True:
        expected_sequence = ledger.expected_sequence(task_id)
        staged = ledger.get_staged_event_by_sequence(
            task_id=task_id, sequence=expected_sequence
        )
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

        if egress_message.event_kind == "completion":
            ledger.mark_staged_event_dispatched(
                task_id=task_id,
                event_id=staged.event_id,
                sequence=staged.sequence,
            )
            _apply_completion_event(
                egress_message=egress_message,
                ledger=ledger,
                store=store,
                attachment_store=attachment_store,
                attachment_cleanup_settings=attachment_cleanup_settings,
                telemetry=telemetry,
            )
            return

        decision = _build_policy_decision_for_message(egress_message)
        try:
            apply_result = applier.apply(
                decision,
                envelope=ledger_record.task_message.envelope,
            )
        except MessageDispatchError as error:
            if telemetry is not None:
                telemetry.record_dropped(reason="dispatch_failed")
            ledger.mark_staged_event_dispatched(
                task_id=task_id,
                event_id=staged.event_id,
                sequence=staged.sequence,
            )
            LOGGER.exception(
                "egress_drop_dispatch_failed task_id=%s event_id=%s sequence=%s event_kind=%s channel=%s reason=%s",
                egress_message.task_id,
                egress_message.event_id,
                egress_message.sequence,
                egress_message.event_kind,
                egress_message.message.channel,
                error.reason_code,
            )
            _send_egress_dispatch_error_email(
                email_sender=error_email_sender,
                recipient=error_email_recipient,
                egress_message=egress_message,
                error=error,
            )
            continue
        _record_outbound_telegram_attachments(
            egress_message=egress_message,
            attachment_store=attachment_store,
            attachment_cleanup_settings=attachment_cleanup_settings,
            dispatched_messages=apply_result.dispatched_messages,
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
            (
                datetime.now(timezone.utc)
                - egress_message.emitted_at.astimezone(timezone.utc)
            ).total_seconds()
            * 1000
        )
        if telemetry is not None:
            telemetry.record_dispatched(
                event_kind=egress_message.event_kind,
                latency_ms=max(dispatch_latency_ms, 0),
            )
        if (
            is_internal_heartbeat_envelope(ledger_record.task_message.envelope)
            and heartbeat_telemetry is not None
        ):
            heartbeat_received_at = datetime.now(timezone.utc)
            heartbeat_telemetry.record_received(
                sent_at=ledger_record.task_message.emitted_at,
                received_at=heartbeat_received_at,
            )
            LOGGER.info(
                "heartbeat_roundtrip task_id=%s sent_at=%s received_at=%s latency_ms=%s",
                egress_message.task_id,
                _serialize_optional_datetime(ledger_record.task_message.emitted_at),
                _serialize_optional_datetime(heartbeat_received_at),
                heartbeat_telemetry.latest_latency_ms,
            )
        if _should_store_telegram_memory(ledger_record.task_message.envelope):
            for message in apply_result.dispatched_messages:
                if message.channel != "telegram":
                    continue
                if (
                    message.target
                    != ledger_record.task_message.envelope.reply_channel.target
                ):
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


def _run_ingress_loop(
    *,
    stop_event: Event,
    store: SQLiteStateStore,
    ledger: TaskLedgerStore,
    broker: BBMBQueueAdapter,
    connectors: list[Connector],
    disabled_ingress_components: list[DisabledIngressComponent],
    attachment_store: TelegramAttachmentStore | None,
    attachment_cleanup_settings: TelegramAttachmentCleanupSettings | None,
    heartbeat_telemetry: HeartbeatTelemetryRollup,
    metrics: MessageHandlerMetrics,
    poll_interval_seconds: float,
    max_loops: int,
) -> None:
    loop_count = 0
    poll_failure_log_state_by_connector: dict[str, IngressPollFailureLogState] = {}
    while not stop_event.is_set():
        loop_count += 1

        for failure in disabled_ingress_components:
            LOGGER.error(
                "ingress_connector_disabled connector=%s loop=%s error=%s",
                failure.component,
                loop_count,
                failure.error,
            )

        ingress_published = 0
        github_scanned_events = 0
        github_new_events = 0
        github_published = 0
        github_checkpoint = "disabled"
        attachment_cleanup_result: TelegramAttachmentCleanupResult | None = None
        for connector in connectors:
            connector_name = type(connector).__name__
            try:
                envelopes = connector.poll()
            except Exception:  # noqa: BLE001
                _log_ingress_poll_failure(
                    connector_name=connector_name,
                    loop_count=loop_count,
                    state_by_connector=poll_failure_log_state_by_connector,
                )
                continue
            if isinstance(
                connector,
                (GitHubIssueAssignmentConnector, GitHubPullRequestReviewConnector),
            ):
                github_scanned_events += connector.last_poll_scanned_events
                github_new_events += connector.last_poll_new_events
                github_checkpoint = connector.last_poll_checkpoint_id
            for envelope in envelopes:
                already_seen = store.seen(envelope.source, envelope.dedupe_key)
                if already_seen:
                    if isinstance(connector, AuxiliaryIngressConnector):
                        connector.ack_envelope(envelope.id)
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
                if (
                    attachment_store is not None
                    and attachment_cleanup_settings is not None
                ):
                    tracked_attachments = attachment_store.record_task_attachments(
                        task_message=task_message,
                        attachment_root_dir=attachment_cleanup_settings.attachment_root_dir,
                    )
                    if tracked_attachments:
                        LOGGER.info(
                            "telegram_attachment_tracked task_id=%s tracked_attachments=%s",
                            task_id,
                            tracked_attachments,
                        )
                store.mark_seen(envelope.source, envelope.dedupe_key)
                if isinstance(connector, AuxiliaryIngressConnector):
                    connector.ack_envelope(envelope.id)
                if is_internal_heartbeat_envelope(task_message.envelope):
                    heartbeat_telemetry.record_sent(sent_at=task_message.emitted_at)
                ingress_published += 1
                if isinstance(
                    connector,
                    (GitHubIssueAssignmentConnector, GitHubPullRequestReviewConnector),
                ):
                    github_published += 1
        if attachment_store is not None and attachment_cleanup_settings is not None:
            attachment_cleanup_result = cleanup_telegram_attachments(
                attachment_store=attachment_store,
                attachment_root_dir=attachment_cleanup_settings.attachment_root_dir,
                completion_grace_period=timedelta(
                    seconds=attachment_cleanup_settings.cleanup_grace_seconds
                ),
                max_attachment_age=timedelta(
                    seconds=attachment_cleanup_settings.max_age_seconds
                ),
            )
            if (
                attachment_cleanup_result.deleted_count
                or attachment_cleanup_result.missing_count
                or attachment_cleanup_result.failed_count
            ):
                LOGGER.info(
                    "telegram_attachment_cleanup deleted=%s missing=%s failed=%s reclaimed_bytes=%s",
                    attachment_cleanup_result.deleted_count,
                    attachment_cleanup_result.missing_count,
                    attachment_cleanup_result.failed_count,
                    attachment_cleanup_result.reclaimed_bytes,
                )

        metrics.record_loop(
            ingress_published=ingress_published,
            github_scanned_events=github_scanned_events,
            github_new_events=github_new_events,
            github_published=github_published,
            telegram_attachment_cleanup=attachment_cleanup_result,
            telemetry_snapshot=metrics.snapshot(),
        )
        heartbeat_snapshot = heartbeat_telemetry.snapshot()
        egress_snapshot = metrics.snapshot()
        LOGGER.info(
            (
                "ingress_loop_completed loop=%s ingress_published=%s "
                "github_scanned_events=%s github_new_events=%s github_published=%s github_checkpoint=%s "
                "telegram_attachment_cleanup_deleted=%s telegram_attachment_cleanup_missing=%s "
                "telegram_attachment_cleanup_failed=%s telegram_attachment_cleanup_reclaimed_bytes=%s "
                "heartbeat_sent_total=%s heartbeat_received_total=%s "
                "heartbeat_latest_sent_at=%s heartbeat_latest_received_at=%s heartbeat_latest_latency_ms=%s "
                "egress_received_total=%s egress_dispatched_total=%s egress_deduped_total=%s "
                "egress_dedupe_hit_rate_pct=%s egress_dropped_total=%s "
                "egress_dispatch_latency_ms_avg=%s egress_dispatch_latency_ms_max=%s "
                "egress_incremental_dispatched_total=%s egress_message_dispatched_total=%s "
                "egress_completion_applied_total=%s"
            ),
            loop_count,
            ingress_published,
            github_scanned_events,
            github_new_events,
            github_published,
            github_checkpoint,
            attachment_cleanup_result.deleted_count
            if attachment_cleanup_result is not None
            else 0,
            attachment_cleanup_result.missing_count
            if attachment_cleanup_result is not None
            else 0,
            attachment_cleanup_result.failed_count
            if attachment_cleanup_result is not None
            else 0,
            attachment_cleanup_result.reclaimed_bytes
            if attachment_cleanup_result is not None
            else 0,
            heartbeat_snapshot["sent_total"],
            heartbeat_snapshot["received_total"],
            heartbeat_snapshot["latest_sent_at"],
            heartbeat_snapshot["latest_received_at"],
            heartbeat_snapshot["latest_latency_ms"],
            egress_snapshot["received_total"],
            egress_snapshot["dispatched_total"],
            egress_snapshot["deduped_total"],
            egress_snapshot["dedupe_hit_rate_pct"],
            egress_snapshot["dropped_total"],
            egress_snapshot["dispatch_latency_ms_avg"],
            egress_snapshot["dispatch_latency_ms_max"],
            egress_snapshot["incremental_dispatched_total"],
            egress_snapshot["message_dispatched_total"],
            egress_snapshot["completion_applied_total"],
        )

        if max_loops and loop_count >= max_loops:
            stop_event.set()
            break
        if stop_event.wait(poll_interval_seconds):
            break


def _run_egress_loop(
    *,
    stop_event: Event,
    ledger: TaskLedgerStore,
    store: SQLiteStateStore,
    attachment_store: TelegramAttachmentStore | None,
    attachment_cleanup_settings: TelegramAttachmentCleanupSettings | None,
    broker: BBMBQueueAdapter,
    allowed_egress_channels: set[str],
    applier: IntegratedApplier,
    error_email_sender: SmtpEmailSender | None,
    error_email_recipient: str | None,
    heartbeat_telemetry: HeartbeatTelemetryRollup,
    metrics: MessageHandlerMetrics,
    poll_timeout_seconds: int,
) -> None:
    telemetry = EgressTelemetryRollup()
    while True:
        timeout_seconds = 1 if stop_event.is_set() else poll_timeout_seconds
        drained = _drain_egress_queue(
            broker=broker,
            poll_timeout_seconds=timeout_seconds,
            ledger=ledger,
            store=store,
            attachment_store=attachment_store,
            attachment_cleanup_settings=attachment_cleanup_settings,
            allowed_egress_channels=allowed_egress_channels,
            applier=applier,
            error_email_sender=error_email_sender,
            error_email_recipient=error_email_recipient,
            heartbeat_telemetry=heartbeat_telemetry,
            telemetry=telemetry,
        )
        if drained == 0 and not stop_event.is_set():
            stop_event.wait(0.01)
        metrics.record_egress_loop(telemetry_snapshot=telemetry.snapshot())
        if stop_event.is_set():
            break


def main() -> int:
    _configure_logging()
    args = _parse_args()
    config = _load_config(args.config, os.environ)

    db_path = _resolve_str(
        args.db_path,
        config.get("db_path"),
        default_value=str(
            Path(tempfile.gettempdir()) / "chatting-message-handler-state.db"
        ),
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
    metrics_host = _resolve_str(
        args.metrics_host,
        config.get("metrics_host"),
        default_value=DEFAULT_METRICS_HOST,
        setting_name="metrics_host",
    )
    metrics_port = _resolve_positive_int(
        args.metrics_port,
        config.get("metrics_port"),
        default_value=DEFAULT_METRICS_PORT,
        setting_name="metrics_port",
    )
    allowed_egress_channels = _resolve_allowed_egress_channels(args, config)
    attachment_cleanup_settings = _resolve_telegram_attachment_cleanup_settings(
        args, config
    )

    store = SQLiteStateStore(db_path)
    ledger = TaskLedgerStore(db_path)
    attachment_store = (
        TelegramAttachmentStore(db_path)
        if attachment_cleanup_settings is not None
        else None
    )
    broker = BBMBQueueAdapter(address=bbmb_address)
    broker.ensure_queue(TASK_QUEUE_NAME)
    broker.ensure_queue(EGRESS_QUEUE_NAME)

    connectors, disabled_ingress_components = _build_live_connectors_fail_open(
        args,
        config,
        db_path=db_path,
    )
    email_sender = _build_email_sender(args, config)
    error_email_recipient = _resolve_error_email_recipient(args, config)
    applier = IntegratedApplier(
        base_dir=".",
        email_sender=email_sender,
        telegram_sender=_build_telegram_sender(args, config),
        github_sender=_build_github_sender(),
    )
    metrics = MessageHandlerMetrics()
    metrics_server = _start_metrics_server(
        metrics,
        host=metrics_host,
        port=metrics_port,
    )
    heartbeat_telemetry = HeartbeatTelemetryRollup()
    stop_event = Event()
    thread_errors: list[BaseException] = []
    thread_errors_lock = Lock()

    def _run_thread(name: str, target: Callable[[], None]) -> None:
        try:
            target()
        except Exception as error:  # noqa: BLE001
            LOGGER.exception("%s_failed", name)
            with thread_errors_lock:
                thread_errors.append(error)
            stop_event.set()

    try:
        ingress_thread = Thread(
            target=lambda: _run_thread(
                "ingress_loop",
                lambda: _run_ingress_loop(
                    stop_event=stop_event,
                    store=store,
                    ledger=ledger,
                    broker=broker,
                    connectors=connectors,
                    disabled_ingress_components=disabled_ingress_components,
                    attachment_store=attachment_store,
                    attachment_cleanup_settings=attachment_cleanup_settings,
                    heartbeat_telemetry=heartbeat_telemetry,
                    metrics=metrics,
                    poll_interval_seconds=poll_interval_seconds,
                    max_loops=max_loops,
                ),
            ),
            name="chatting-ingress",
        )
        egress_thread = Thread(
            target=lambda: _run_thread(
                "egress_loop",
                lambda: _run_egress_loop(
                    stop_event=stop_event,
                    ledger=ledger,
                    store=store,
                    attachment_store=attachment_store,
                    attachment_cleanup_settings=attachment_cleanup_settings,
                    broker=broker,
                    allowed_egress_channels=allowed_egress_channels,
                    applier=applier,
                    error_email_sender=email_sender,
                    error_email_recipient=error_email_recipient,
                    heartbeat_telemetry=heartbeat_telemetry,
                    metrics=metrics,
                    poll_timeout_seconds=poll_timeout_seconds,
                ),
            ),
            name="chatting-egress",
        )
        ingress_thread.start()
        egress_thread.start()
        ingress_thread.join()
        stop_event.set()
        egress_thread.join()
        if thread_errors:
            raise thread_errors[0]
    finally:
        if metrics_server is not None:
            metrics_server.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())
