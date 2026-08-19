"""Deterministic /usage command: report backend limits without running a task."""

from __future__ import annotations

import re
from datetime import datetime, timezone

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import OutboundMessage, TaskEnvelope, UsageReport, UsageWindow

USAGE_COMMAND = "/usage"

# The Telegram connector prefixes forum-topic messages with the thread id, so a
# command sent from a topic never arrives bare.
_THREAD_PREFIX_RE = re.compile(r"^\[thread_id=\d+\]\s*")


def is_usage_command_envelope(envelope: TaskEnvelope) -> bool:
    """True when the message body is nothing but the /usage command."""
    content = _THREAD_PREFIX_RE.sub("", envelope.content.strip(), count=1)
    return content.strip().lower() == USAGE_COMMAND


def format_usage_report(report: UsageReport, *, now: datetime) -> str:
    """Render a usage snapshot as plain text for a chat reply."""
    current_time = _ensure_utc(now)
    if report.errors or report.observed_at is None:
        return (
            "No usage snapshot available yet. The backend only publishes limits "
            "while it runs a task, so this fills in after the next real run."
        )

    observed = _format_instant(report.observed_at)
    staleness = _staleness(report.observed_at, current_time)
    lines = [f"Codex usage as of {observed}{staleness}."]

    identity = [
        label
        for label in (
            f"model {report.model}" if report.model else None,
            f"plan {report.plan_type}" if report.plan_type else None,
            f"limit {report.limit_id}" if report.limit_id else None,
        )
        if label is not None
    ]
    if identity:
        detail = ", ".join(identity)
        lines.append("")
        lines.append(detail[0].upper() + detail[1:])

    windows = [
        ("Primary", report.primary),
        ("Secondary", report.secondary),
    ]
    rendered_windows = [
        f"{label} window ({_format_window_length(window.window_minutes)}): "
        f"{window.used_percent:g}% used{_reset_suffix(window, current_time)}"
        for label, window in windows
        if window is not None
    ]
    if rendered_windows:
        lines.append("")
        lines.extend(rendered_windows)
    else:
        lines.append("")
        lines.append(
            "No window allowance reported, which is what the backend does once "
            "the plan allowance is spent."
        )

    lines.append("")
    lines.append(_format_credits(report))
    return "\n".join(lines)


def build_usage_egress(
    *,
    task_message: TaskQueueMessage,
    body: str,
    emitted_at: datetime,
) -> EgressQueueMessage:
    current_time = _ensure_utc(emitted_at)
    return EgressQueueMessage(
        task_id=task_message.task_id,
        envelope_id=task_message.envelope.id,
        trace_id=task_message.trace_id,
        event_index=0,
        event_count=2,
        message=OutboundMessage(
            channel=task_message.envelope.reply_channel.type,
            target=task_message.envelope.reply_channel.target,
            body=body,
        ),
        emitted_at=current_time,
        event_id=f"evt:{task_message.task_id}:0:message:usage-command",
        sequence=0,
        event_kind="message",
        message_type="chatting.egress.v2",
    )


def _format_credits(report: UsageReport) -> str:
    if report.unlimited_credits:
        return "Credits: unlimited."
    if report.credit_balance is None:
        return "Credits: not reported."
    if report.has_credits is False:
        return f"Credits: {report.credit_balance}, none left to spend."
    return f"Credits: {report.credit_balance}."


def _reset_suffix(window: UsageWindow, now: datetime) -> str:
    if window.resets_at is None:
        return ""
    remaining = _format_duration(
        int((window.resets_at - now).total_seconds()), zero="now"
    )
    return f", resets {_format_instant(window.resets_at)} (in {remaining})"


def _staleness(observed_at: datetime, now: datetime) -> str:
    elapsed = int((now - observed_at).total_seconds())
    if elapsed < 60:
        return ""
    return f", {_format_duration(elapsed, zero='just now')} ago"


def _format_window_length(window_minutes: int) -> str:
    if window_minutes % 1440 == 0:
        days = window_minutes // 1440
        return f"{days}d"
    if window_minutes % 60 == 0:
        return f"{window_minutes // 60}h"
    return f"{window_minutes}m"


def _format_duration(total_seconds: int, *, zero: str) -> str:
    if total_seconds <= 0:
        return zero
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    parts = [
        piece
        for piece in (
            f"{days}d" if days else None,
            f"{hours}h" if hours else None,
            f"{minutes}m" if minutes and not days else None,
        )
        if piece is not None
    ]
    return " ".join(parts) if parts else "under 1m"


def _format_instant(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%d %b %H:%M UTC")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = [
    "USAGE_COMMAND",
    "build_usage_egress",
    "format_usage_report",
    "is_usage_command_envelope",
]
