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
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Callable, Mapping

from app.applier import IntegratedApplier
from app.broker import (
    BBMBQueueAdapter,
    EGRESS_QUEUE_NAME,
    EgressQueueMessage,
    TASK_QUEUE_NAME,
    TaskQueueMessage,
)
from app.connectors import GitHubIssueAssignmentConnector, InternalHeartbeatConnector
from app.github_ingress_runtime import (
    GitHubAssignmentCheckpointStore,
    default_graphql_runner,
    fetch_authenticated_viewer_login,
)
from app.internal_heartbeat import INTERNAL_HEARTBEAT_TARGET, is_internal_heartbeat_envelope
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
        "metrics_host",
        "metrics_port",
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
DEFAULT_METRICS_HOST = "127.0.0.1"
DEFAULT_METRICS_PORT = 9464


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


@dataclass(frozen=True)
class DisabledIngressComponent:
    component: str
    error: str


@dataclass
class EgressTelemetryRollup:
    """In-memory telemetry rollup for split-mode egress lifecycle signals."""

    received_total: int = 0
    dispatched_total: int = 0
    deduped_total: int = 0
    dropped_total: int = 0
    dropped_unknown_task_total: int = 0
    dropped_completed_task_total: int = 0
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
        elif reason == "completed_task":
            self.dropped_completed_task_total += 1
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
            "dropped_completed_task_total": self.dropped_completed_task_total,
            "dropped_disallowed_channel_total": self.dropped_disallowed_channel_total,
            "dropped_missing_event_id_total": self.dropped_missing_event_id_total,
            "incremental_dispatched_total": self.incremental_dispatched_total,
            "final_dispatched_total": self.final_dispatched_total,
            "dispatch_latency_ms_avg": avg_dispatch_latency_ms,
            "dispatch_latency_ms_max": self.dispatch_latency_ms_max,
        }


@dataclass
class HeartbeatTelemetryRollup:
    """In-memory telemetry for the message-handler/worker heartbeat."""

    sent_total: int = 0
    received_total: int = 0
    latest_sent_at: datetime | None = None
    latest_received_at: datetime | None = None
    latest_latency_ms: int | None = None

    def record_sent(self, *, sent_at: datetime) -> None:
        self.sent_total += 1
        self.latest_sent_at = sent_at.astimezone(timezone.utc)

    def record_received(self, *, sent_at: datetime, received_at: datetime) -> None:
        self.received_total += 1
        self.latest_sent_at = sent_at.astimezone(timezone.utc)
        self.latest_received_at = received_at.astimezone(timezone.utc)
        self.latest_latency_ms = max(
            int((self.latest_received_at - self.latest_sent_at).total_seconds() * 1000),
            0,
        )

    def snapshot(self) -> dict[str, object]:
        return {
            "sent_total": self.sent_total,
            "received_total": self.received_total,
            "latest_sent_at": _serialize_optional_datetime(self.latest_sent_at),
            "latest_received_at": _serialize_optional_datetime(self.latest_received_at),
            "latest_latency_ms": self.latest_latency_ms,
        }


@dataclass(frozen=True)
class MessageHandlerMetricsServer:
    server: HTTPServer
    thread: Thread

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=1.0)


class MessageHandlerMetrics:
    """Thread-safe rollup for in-process message-handler Prometheus metrics."""

    def __init__(
        self,
        *,
        started_at: datetime | None = None,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lock = Lock()
        self._monotonic_fn = monotonic_fn
        self._started_at = started_at or datetime.now(timezone.utc)
        self._started_monotonic = monotonic_fn()
        self._loops_total = 0
        self._ingress_published_total = 0
        self._github_scanned_events_total = 0
        self._github_new_events_total = 0
        self._github_published_total = 0
        self._last_loop_completed_at: datetime | None = None
        self._egress_snapshot: dict[str, float | int] = EgressTelemetryRollup().snapshot()

    def record_loop(
        self,
        *,
        ingress_published: int,
        github_scanned_events: int,
        github_new_events: int,
        github_published: int,
        telemetry_snapshot: dict[str, float | int],
        completed_at: datetime | None = None,
    ) -> None:
        loop_completed_at = completed_at or datetime.now(timezone.utc)
        with self._lock:
            self._loops_total += 1
            self._ingress_published_total += ingress_published
            self._github_scanned_events_total += github_scanned_events
            self._github_new_events_total += github_new_events
            self._github_published_total += github_published
            self._last_loop_completed_at = loop_completed_at.astimezone(timezone.utc)
            self._egress_snapshot = dict(telemetry_snapshot)

    def snapshot(self) -> dict[str, float | int]:
        with self._lock:
            uptime_seconds = max(self._monotonic_fn() - self._started_monotonic, 0.0)
            last_loop_timestamp = (
                self._last_loop_completed_at.timestamp()
                if self._last_loop_completed_at is not None
                else 0.0
            )
            snapshot = {
                "uptime_seconds": round(uptime_seconds, 6),
                "process_start_time_seconds": round(self._started_at.timestamp(), 6),
                "loops_total": self._loops_total,
                "ingress_published_total": self._ingress_published_total,
                "github_scanned_events_total": self._github_scanned_events_total,
                "github_new_events_total": self._github_new_events_total,
                "github_published_total": self._github_published_total,
                "last_loop_completed_timestamp_seconds": round(last_loop_timestamp, 6),
            }
            snapshot.update(self._egress_snapshot)
            return snapshot


def _format_prometheus_value(value: float | int) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return f"{float(value):.6f}"


def _render_prometheus_metrics(snapshot: Mapping[str, float | int]) -> str:
    definitions = (
        (
            "chatting_message_handler_uptime_seconds",
            "gauge",
            "Seconds since the message handler process started.",
            "uptime_seconds",
        ),
        (
            "chatting_message_handler_process_start_time_seconds",
            "gauge",
            "Unix timestamp when the message handler process started.",
            "process_start_time_seconds",
        ),
        (
            "chatting_message_handler_loops_total",
            "counter",
            "Completed message handler loops.",
            "loops_total",
        ),
        (
            "chatting_message_handler_ingress_published_total",
            "counter",
            "Tasks published to the worker ingress queue.",
            "ingress_published_total",
        ),
        (
            "chatting_message_handler_github_scanned_events_total",
            "counter",
            "GitHub assignment events scanned by the handler.",
            "github_scanned_events_total",
        ),
        (
            "chatting_message_handler_github_new_events_total",
            "counter",
            "New GitHub assignment events discovered after checkpointing.",
            "github_new_events_total",
        ),
        (
            "chatting_message_handler_github_published_total",
            "counter",
            "GitHub assignment events published as tasks.",
            "github_published_total",
        ),
        (
            "chatting_message_handler_last_loop_completed_timestamp_seconds",
            "gauge",
            "Unix timestamp of the most recently completed loop.",
            "last_loop_completed_timestamp_seconds",
        ),
        (
            "chatting_message_handler_egress_received_total",
            "counter",
            "Egress messages received from the broker.",
            "received_total",
        ),
        (
            "chatting_message_handler_egress_dispatched_total",
            "counter",
            "Egress messages dispatched to downstream transports.",
            "dispatched_total",
        ),
        (
            "chatting_message_handler_egress_deduped_total",
            "counter",
            "Egress messages skipped due to deduplication.",
            "deduped_total",
        ),
        (
            "chatting_message_handler_egress_dedupe_hit_rate_pct",
            "gauge",
            "Percentage of handled egress events skipped by dedupe.",
            "dedupe_hit_rate_pct",
        ),
        (
            "chatting_message_handler_egress_dropped_total",
            "counter",
            "Egress messages dropped before dispatch.",
            "dropped_total",
        ),
        (
            "chatting_message_handler_egress_dropped_unknown_task_total",
            "counter",
            "Egress messages dropped because the task ledger entry was missing.",
            "dropped_unknown_task_total",
        ),
        (
            "chatting_message_handler_egress_dropped_completed_task_total",
            "counter",
            "Egress messages dropped because the task was already completed.",
            "dropped_completed_task_total",
        ),
        (
            "chatting_message_handler_egress_dropped_disallowed_channel_total",
            "counter",
            "Egress messages dropped because the channel is not allowed.",
            "dropped_disallowed_channel_total",
        ),
        (
            "chatting_message_handler_egress_dropped_missing_event_id_total",
            "counter",
            "Egress messages dropped because they had no event id.",
            "dropped_missing_event_id_total",
        ),
        (
            "chatting_message_handler_egress_incremental_dispatched_total",
            "counter",
            "Incremental egress events dispatched in order.",
            "incremental_dispatched_total",
        ),
        (
            "chatting_message_handler_egress_final_dispatched_total",
            "counter",
            "Final egress events dispatched in order.",
            "final_dispatched_total",
        ),
        (
            "chatting_message_handler_egress_dispatch_latency_ms_avg",
            "gauge",
            "Average egress dispatch latency in milliseconds.",
            "dispatch_latency_ms_avg",
        ),
        (
            "chatting_message_handler_egress_dispatch_latency_ms_max",
            "gauge",
            "Maximum egress dispatch latency in milliseconds.",
            "dispatch_latency_ms_max",
        ),
    )
    lines: list[str] = []
    for metric_name, metric_type, help_text, snapshot_key in definitions:
        lines.append(f"# HELP {metric_name} {help_text}")
        lines.append(f"# TYPE {metric_name} {metric_type}")
        lines.append(f"{metric_name} {_format_prometheus_value(snapshot[snapshot_key])}")
    return "\n".join(lines) + "\n"


def _start_metrics_server(
    metrics: MessageHandlerMetrics,
    *,
    host: str,
    port: int,
) -> MessageHandlerMetricsServer | None:
    class _MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path.split("?", maxsplit=1)[0] != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            payload = _render_prometheus_metrics(metrics.snapshot()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args
            return

    try:
        server = HTTPServer((host, port), _MetricsHandler)
    except OSError:
        LOGGER.exception("message_handler_metrics_server_failed host=%s port=%s", host, port)
        return None

    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.5}, daemon=True)
    thread.start()
    LOGGER.info(
        "message_handler_metrics_server_started host=%s port=%s",
        host,
        server.server_address[1],
    )
    return MessageHandlerMetricsServer(server=server, thread=thread)


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
    parser.add_argument("--max-loops", type=_positive_int, help="Optional loop limit for smoke runs.")
    parser.add_argument("--poll-interval-seconds", type=_positive_float, help="Loop interval.")
    parser.add_argument("--poll-timeout-seconds", type=_positive_int, help="Egress pickup timeout seconds.")
    parser.add_argument("--metrics-host", help="Metrics bind host.")
    parser.add_argument("--metrics-port", type=_positive_int, help="Metrics bind port.")
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


def _is_connector_configured(args: argparse.Namespace, config: dict[str, object], *, connector: str) -> bool:
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
        return bool(args.github_repository) or config.get("github_repositories") is not None
    raise ValueError(f"unsupported connector: {connector}")


def _build_live_connectors_fail_open(
    args: argparse.Namespace,
    config: dict[str, object],
    *,
    db_path: str,
) -> tuple[list[object], list[DisabledIngressComponent]]:
    connectors: list[object] = [InternalHeartbeatConnector()]
    disabled_components: list[DisabledIngressComponent] = []

    connector_args: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
        "schedule": (
            ("schedule_file",),
            ("schedule_file",),
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
                "imap_host",
                "imap_port",
                "imap_username",
                "imap_password_env",
                "imap_mailbox",
                "imap_search",
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
                "telegram_allowed_chat_ids",
                "telegram_allowed_channel_ids",
                "telegram_context_refs",
                "context_ref",
                "context_refs",
            ),
        ),
        "github": (
            (
                "github_repository",
                "github_assignee_login",
                "github_reply_channel_type",
                "github_reply_channel_target",
                "github_context_ref",
                "github_policy_profile",
                "github_max_issues",
                "github_max_timeline_events",
            ),
            (
                "github_repositories",
                "github_assignee_login",
                "github_reply_channel_type",
                "github_reply_channel_target",
                "github_context_refs",
                "github_policy_profile",
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
            "telegram_enabled": False,
            "telegram_bot_token_env": None,
            "telegram_api_base_url": None,
            "telegram_poll_timeout_seconds": None,
            "telegram_allowed_chat_id": [],
            "telegram_allowed_channel_id": [],
            "telegram_context_ref": [],
            "context_ref": [],
            "github_repository": [],
            "github_assignee_login": None,
            "github_reply_channel_type": None,
            "github_reply_channel_target": None,
            "github_context_ref": [],
            "github_policy_profile": None,
            "github_max_issues": None,
            "github_max_timeline_events": None,
        }
    )

    for connector_name in ("schedule", "imap", "telegram", "github"):
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
        scoped_config = {key: config[key] for key in selected_config_keys if key in config}
        try:
            if connector_name == "github":
                settings = _resolve_github_ingress_settings(scoped_args, scoped_config)
                if settings is None:
                    continue
                connectors.append(
                    GitHubIssueAssignmentConnector(
                        repository_patterns=settings.repositories,
                        assignee_login=settings.assignee_login,
                        reply_channel_type=settings.reply_channel_type,
                        reply_channel_target=settings.reply_channel_target,
                        context_refs=settings.context_refs,
                        policy_profile=settings.policy_profile,
                        max_issues=settings.max_issues,
                        max_timeline_events=settings.max_timeline_events,
                        checkpoint_store=GitHubAssignmentCheckpointStore(db_path),
                        graphql_runner=default_graphql_runner,
                    )
                )
                continue
            connectors.extend(_build_live_connectors(scoped_args, scoped_config))
        except Exception as error:  # noqa: BLE001
            LOGGER.exception("ingress_connector_startup_failed connector=%s", connector_name)
            disabled_components.append(
                DisabledIngressComponent(
                    component=connector_name,
                    error=str(error),
                )
            )
    return connectors, disabled_components


def _build_policy_decision_for_message(egress_message: EgressQueueMessage) -> PolicyDecision:
    return PolicyDecision(
        approved_actions=[],
        blocked_actions=[],
        approved_messages=[egress_message.message],
        config_updates=ConfigUpdateDecision(),
        reason_codes=[],
    )


def _is_terminal_drop_message(egress_message: EgressQueueMessage) -> bool:
    return (
        egress_message.event_kind == "final"
        and egress_message.message.channel == "drop"
        and egress_message.message.target == "task"
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

    internal_heartbeat = is_internal_heartbeat_envelope(ledger_record.task_message.envelope)
    heartbeat_log_message = (
        internal_heartbeat
        and egress_message.message.channel == "log"
        and egress_message.message.target == INTERNAL_HEARTBEAT_TARGET
    )
    terminal_drop_message = _is_terminal_drop_message(egress_message)
    if (
        egress_message.message.channel not in allowed_egress_channels
        and not heartbeat_log_message
        and not terminal_drop_message
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
        heartbeat_telemetry=heartbeat_telemetry,
        telemetry=telemetry,
    )


def _flush_task_egress_in_sequence(
    *,
    task_id: str,
    ledger: TaskLedgerStore,
    store: SQLiteStateStore,
    applier: IntegratedApplier,
    heartbeat_telemetry: HeartbeatTelemetryRollup | None = None,
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
        if is_internal_heartbeat_envelope(ledger_record.task_message.envelope) and heartbeat_telemetry is not None:
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
        if egress_message.event_kind == "final":
            ledger.mark_task_completed(
                task_id=task_id,
                envelope_id=egress_message.envelope_id,
                trace_id=egress_message.trace_id,
            )
            return


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

    store = SQLiteStateStore(db_path)
    ledger = TaskLedgerStore(db_path)
    broker = BBMBQueueAdapter(address=bbmb_address)
    broker.ensure_queue(TASK_QUEUE_NAME)
    broker.ensure_queue(EGRESS_QUEUE_NAME)

    connectors, disabled_ingress_components = _build_live_connectors_fail_open(
        args,
        config,
        db_path=db_path,
    )
    applier = IntegratedApplier(
        base_dir=".",
        email_sender=_build_email_sender(args, config),
        telegram_sender=_build_telegram_sender(args, config),
    )
    metrics = MessageHandlerMetrics()
    metrics_server = _start_metrics_server(
        metrics,
        host=metrics_host,
        port=metrics_port,
    )
    heartbeat_telemetry = HeartbeatTelemetryRollup()

    try:
        loop_count = 0
        telemetry = EgressTelemetryRollup()
        while True:
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
            for connector in connectors:
                connector_name = type(connector).__name__
                try:
                    envelopes = connector.poll()
                except Exception:  # noqa: BLE001
                    LOGGER.exception(
                        "ingress_connector_poll_failed connector=%s loop=%s",
                        connector_name,
                        loop_count,
                    )
                    continue
                if isinstance(connector, GitHubIssueAssignmentConnector):
                    github_scanned_events = connector.last_poll_scanned_events
                    github_new_events = connector.last_poll_new_events
                    github_checkpoint = connector.last_poll_checkpoint_id
                for envelope in envelopes:
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
                    if is_internal_heartbeat_envelope(task_message.envelope):
                        heartbeat_telemetry.record_sent(sent_at=task_message.emitted_at)
                    ingress_published += 1
                    if isinstance(connector, GitHubIssueAssignmentConnector):
                        github_published += 1

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
                    heartbeat_telemetry=heartbeat_telemetry,
                    telemetry=telemetry,
                )

            telemetry_snapshot = telemetry.snapshot()
            heartbeat_snapshot = heartbeat_telemetry.snapshot()
            metrics.record_loop(
                ingress_published=ingress_published,
                github_scanned_events=github_scanned_events,
                github_new_events=github_new_events,
                github_published=github_published,
                telemetry_snapshot=telemetry_snapshot,
            )
            LOGGER.info(
                (
                    "message_handler_loop_completed loop=%s ingress_published=%s "
                    "github_scanned_events=%s github_new_events=%s github_published=%s github_checkpoint=%s "
                    "heartbeat_sent_total=%s heartbeat_received_total=%s "
                    "heartbeat_latest_sent_at=%s heartbeat_latest_received_at=%s heartbeat_latest_latency_ms=%s "
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
                heartbeat_snapshot["sent_total"],
                heartbeat_snapshot["received_total"],
                heartbeat_snapshot["latest_sent_at"],
                heartbeat_snapshot["latest_received_at"],
                heartbeat_snapshot["latest_latency_ms"],
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
    finally:
        if metrics_server is not None:
            metrics_server.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())
