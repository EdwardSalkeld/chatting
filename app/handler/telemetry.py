"""Telemetry and metrics helpers for the split-mode message-handler."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Lock, Thread
from typing import Callable, Mapping

from app.handler.runtime import TelegramAttachmentCleanupResult

LOGGER = logging.getLogger(__name__)


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
    message_dispatched_total: int = 0
    completion_applied_total: int = 0

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
        elif event_kind == "message":
            self.message_dispatched_total += 1
        else:
            self.completion_applied_total += 1
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
            "message_dispatched_total": self.message_dispatched_total,
            "completion_applied_total": self.completion_applied_total,
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

    def __post_init__(self) -> None:
        self._lock = Lock()

    def record_sent(self, *, sent_at: datetime) -> None:
        with self._lock:
            self.sent_total += 1
            self.latest_sent_at = sent_at.astimezone(timezone.utc)

    def record_received(self, *, sent_at: datetime, received_at: datetime) -> None:
        with self._lock:
            self.received_total += 1
            self.latest_sent_at = sent_at.astimezone(timezone.utc)
            self.latest_received_at = received_at.astimezone(timezone.utc)
            self.latest_latency_ms = max(
                int((self.latest_received_at - self.latest_sent_at).total_seconds() * 1000),
                0,
            )

    def snapshot(self) -> dict[str, object]:
        with self._lock:
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
        self._telegram_attachment_cleanup_deleted_total = 0
        self._telegram_attachment_cleanup_missing_total = 0
        self._telegram_attachment_cleanup_failed_total = 0
        self._telegram_attachment_cleanup_reclaimed_bytes_total = 0
        self._egress_loops_total = 0
        self._last_loop_completed_at: datetime | None = None
        self._last_egress_loop_completed_at: datetime | None = None
        self._egress_snapshot: dict[str, float | int] = EgressTelemetryRollup().snapshot()

    def record_loop(
        self,
        *,
        ingress_published: int,
        github_scanned_events: int,
        github_new_events: int,
        github_published: int,
        telegram_attachment_cleanup: TelegramAttachmentCleanupResult | None,
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
            if telegram_attachment_cleanup is not None:
                self._telegram_attachment_cleanup_deleted_total += (
                    telegram_attachment_cleanup.deleted_count
                )
                self._telegram_attachment_cleanup_missing_total += (
                    telegram_attachment_cleanup.missing_count
                )
                self._telegram_attachment_cleanup_failed_total += (
                    telegram_attachment_cleanup.failed_count
                )
                self._telegram_attachment_cleanup_reclaimed_bytes_total += (
                    telegram_attachment_cleanup.reclaimed_bytes
                )
            self._last_loop_completed_at = loop_completed_at.astimezone(timezone.utc)
            self._egress_snapshot = dict(telemetry_snapshot)

    def record_egress_loop(
        self,
        *,
        telemetry_snapshot: dict[str, float | int],
        completed_at: datetime | None = None,
    ) -> None:
        loop_completed_at = completed_at or datetime.now(timezone.utc)
        with self._lock:
            self._egress_loops_total += 1
            self._last_egress_loop_completed_at = loop_completed_at.astimezone(timezone.utc)
            self._egress_snapshot = dict(telemetry_snapshot)

    def snapshot(self) -> dict[str, float | int]:
        with self._lock:
            uptime_seconds = max(self._monotonic_fn() - self._started_monotonic, 0.0)
            last_ingress_loop_timestamp = (
                self._last_loop_completed_at.timestamp()
                if self._last_loop_completed_at is not None
                else 0.0
            )
            last_egress_loop_timestamp = (
                self._last_egress_loop_completed_at.timestamp()
                if self._last_egress_loop_completed_at is not None
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
                "telegram_attachment_cleanup_deleted_total": (
                    self._telegram_attachment_cleanup_deleted_total
                ),
                "telegram_attachment_cleanup_missing_total": (
                    self._telegram_attachment_cleanup_missing_total
                ),
                "telegram_attachment_cleanup_failed_total": (
                    self._telegram_attachment_cleanup_failed_total
                ),
                "telegram_attachment_cleanup_reclaimed_bytes_total": (
                    self._telegram_attachment_cleanup_reclaimed_bytes_total
                ),
                "last_loop_completed_timestamp_seconds": round(last_ingress_loop_timestamp, 6),
                "egress_loops_total": self._egress_loops_total,
                "last_egress_loop_completed_timestamp_seconds": round(
                    last_egress_loop_timestamp,
                    6,
                ),
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
            "chatting_message_handler_telegram_attachment_cleanup_deleted_total",
            "counter",
            "Telegram attachment files deleted by handler-managed cleanup.",
            "telegram_attachment_cleanup_deleted_total",
        ),
        (
            "chatting_message_handler_telegram_attachment_cleanup_missing_total",
            "counter",
            "Tracked Telegram attachment files already missing from disk during cleanup.",
            "telegram_attachment_cleanup_missing_total",
        ),
        (
            "chatting_message_handler_telegram_attachment_cleanup_failed_total",
            "counter",
            "Telegram attachment cleanup attempts that failed.",
            "telegram_attachment_cleanup_failed_total",
        ),
        (
            "chatting_message_handler_telegram_attachment_cleanup_reclaimed_bytes_total",
            "counter",
            "Bytes reclaimed by Telegram attachment cleanup.",
            "telegram_attachment_cleanup_reclaimed_bytes_total",
        ),
        (
            "chatting_message_handler_last_loop_completed_timestamp_seconds",
            "gauge",
            "Unix timestamp of the most recently completed loop.",
            "last_loop_completed_timestamp_seconds",
        ),
        (
            "chatting_message_handler_egress_loops_total",
            "counter",
            "Completed egress polling loops.",
            "egress_loops_total",
        ),
        (
            "chatting_message_handler_last_egress_loop_completed_timestamp_seconds",
            "gauge",
            "Unix timestamp of the most recently completed egress loop.",
            "last_egress_loop_completed_timestamp_seconds",
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
            "Egress events applied by the message-handler.",
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
            "chatting_message_handler_egress_message_dispatched_total",
            "counter",
            "Task-scoped visible egress messages dispatched in order.",
            "message_dispatched_total",
        ),
        (
            "chatting_message_handler_egress_completion_applied_total",
            "counter",
            "Internal completion events applied in order.",
            "completion_applied_total",
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
            try:
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
            except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                return

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


def _serialize_optional_datetime(value: datetime | None) -> str:
    if value is None:
        return "none"
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
