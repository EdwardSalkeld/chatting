"""Codex executor with timeout control and transcript capture."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from app.models import (
    ExecutionResult,
    SCHEMA_VERSION,
    TaskEnvelope,
    UsageReport,
    UsageWindow,
    parse_context_ref,
)

# Codex publishes rate-limit figures only inside the session rollouts it writes
# while running a task, so reading them back is a file scan rather than an API
# call. Cap how far back we look: if the newest handful of runs carry nothing,
# older ones are too stale to be worth reporting.
_USAGE_ROLLOUT_SCAN_LIMIT = 25


@dataclass(frozen=True)
class CodexExecutor:
    """Run Codex as a subprocess and capture stdout/stderr as transcript."""

    command: tuple[str, ...] = ("codex", "exec", "--json")
    cwd: str | None = None
    env: Mapping[str, str] | None = None
    timeout_seconds: int = 1800
    now_provider: Callable[[], datetime] = field(
        default=lambda: datetime.now(timezone.utc)
    )

    def execute(self, envelope: TaskEnvelope) -> ExecutionResult:
        payload = json.dumps(_task_payload(envelope, current_time=self.now_provider()))
        try:
            completed = subprocess.run(
                self.command,
                input=payload,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                check=False,
                cwd=self.cwd,
                env=dict(self.env) if self.env is not None else None,
            )
        except subprocess.TimeoutExpired:
            return _error_result("executor_timeout")

        if completed.returncode != 0:
            error = f"executor_exit_nonzero:{completed.returncode}"
            stderr = completed.stderr.strip()
            if stderr:
                error = f"{error}:{stderr}"
            return _error_result(
                error,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )

        return ExecutionResult(
            errors=[],
            stdout=completed.stdout,
            stderr=completed.stderr,
        )

    def usage_report(self) -> UsageReport:
        """Read the newest Codex rate-limit snapshot without invoking the model."""
        codex_home = self._codex_home()
        if codex_home is None:
            return UsageReport(errors=["codex_home_unresolved"])
        sessions_dir = codex_home / "sessions"
        if not sessions_dir.is_dir():
            return UsageReport(errors=["codex_sessions_missing"])

        # Rollout filenames embed the timestamp, so lexical order is chronological.
        rollouts = sorted(sessions_dir.glob("*/*/*/rollout-*.jsonl"))
        if not rollouts:
            return UsageReport(errors=["codex_sessions_empty"])

        for rollout in reversed(rollouts[-_USAGE_ROLLOUT_SCAN_LIMIT:]):
            report = _usage_report_from_rollout(rollout)
            if report is not None:
                return report
        return UsageReport(errors=["codex_usage_snapshot_absent"])

    def _codex_home(self) -> Path | None:
        env = self.env if self.env is not None else os.environ
        codex_home = env.get("CODEX_HOME")
        if codex_home:
            return Path(codex_home)
        home = env.get("HOME")
        if home:
            return Path(home) / ".codex"
        return None


def _usage_report_from_rollout(rollout: Path) -> UsageReport | None:
    """Pull the last rate-limit snapshot and the model out of one rollout file."""
    rate_limits: dict[str, Any] | None = None
    model: str | None = None
    observed_at: datetime | None = None
    try:
        with rollout.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                # Transcript lines dominate these files; only parse the few that
                # can carry what we need.
                wants_limits = "rate_limits" in line
                wants_model = model is None and '"model"' in line
                if not wants_limits and not wants_model:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(entry, dict):
                    continue
                payload = entry.get("payload")
                if not isinstance(payload, dict):
                    continue
                if wants_model:
                    candidate = payload.get("model")
                    if isinstance(candidate, str) and candidate:
                        model = candidate
                if wants_limits:
                    candidate_limits = payload.get("rate_limits")
                    if isinstance(candidate_limits, dict):
                        rate_limits = candidate_limits
                        observed_at = _parse_timestamp(entry.get("timestamp"))
    except OSError:
        return None

    if rate_limits is None:
        return None

    credits = rate_limits.get("credits")
    credits = credits if isinstance(credits, dict) else {}
    balance = credits.get("balance")
    has_credits = credits.get("has_credits")
    return UsageReport(
        observed_at=observed_at or _mtime(rollout),
        model=model,
        plan_type=_optional_str(rate_limits.get("plan_type")),
        limit_id=_optional_str(rate_limits.get("limit_id")),
        primary=_usage_window(rate_limits.get("primary")),
        secondary=_usage_window(rate_limits.get("secondary")),
        credit_balance=_optional_str(balance),
        has_credits=has_credits if isinstance(has_credits, bool) else None,
        unlimited_credits=credits.get("unlimited") is True,
    )


def _usage_window(value: object) -> UsageWindow | None:
    if not isinstance(value, dict):
        return None
    used_percent = value.get("used_percent")
    window_minutes = value.get("window_minutes")
    if not isinstance(used_percent, (int, float)) or isinstance(used_percent, bool):
        return None
    if not isinstance(window_minutes, int) or isinstance(window_minutes, bool):
        return None
    resets_at = value.get("resets_at")
    return UsageWindow(
        used_percent=float(used_percent),
        window_minutes=window_minutes,
        resets_at=_parse_epoch(resets_at),
    )


def _parse_epoch(value: object) -> datetime | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _mtime(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


def _task_payload(envelope: TaskEnvelope, *, current_time: datetime) -> dict[str, Any]:
    if current_time.tzinfo is None:
        raise ValueError("current_time must be timezone-aware")
    task_dict: dict[str, Any] = {
        "task_id": f"task:{envelope.id}",
        "envelope_id": envelope.id,
        "workflow": "default",
        "event_time": envelope.received_at.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "source": envelope.source,
        "content": envelope.content,
        "context": [parse_context_ref(ref).to_dict() for ref in envelope.context_refs],
        "reply_channel": {
            "type": envelope.reply_channel.type,
            "target": envelope.reply_channel.target,
        },
    }
    if envelope.actor is not None:
        task_dict["actor"] = envelope.actor
    if envelope.attachments:
        task_dict["attachments"] = [
            {"uri": item.uri, "name": item.name} for item in envelope.attachments
        ]
    if envelope.prompt_context.has_content():
        task_dict["prompt_context"] = envelope.prompt_context.to_dict()
    if envelope.reply_channel.metadata:
        task_dict["reply_channel"]["metadata"] = envelope.reply_channel.metadata
    payload = {
        "schema_version": SCHEMA_VERSION,
        "task": task_dict,
        "current_time": current_time.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "reply_contract": {
            # -P (safe path) stops Python prepending the cwd to sys.path, so this
            # always runs the deployed app.main_reply even when the cwd is a
            # chatting checkout in the workspace whose stale app/ would otherwise
            # shadow it (a shadowing copy publishes to BBMB and the reply is lost).
            "visible_replies_must_use": "python3 -P -m app.main_reply --spec-file <path>",
            "reply_spec_instructions": (
                "Write the whole reply as a JSON file using your file-editing tool (NOT a "
                "shell command like echo/printf/heredoc), then pass it with --spec-file. "
                "Never put the reply text on the command line: the shell mangles backticks, "
                "quotes, $, and newlines, which corrupts or splits the reply. Spec fields: "
                "task_id, channel, target, message, and optionally attachment_path, "
                "attachment_name, telegram_reaction, telegram_message_id, event_id. "
                "If telegram_reaction is supplied with message or attachment, main_reply "
                "sends both."
            ),
            "reply_spec_example": {
                "task_id": "task:telegram:123",
                "channel": "telegram",
                "target": "8605042448",
                "message": 'Any text is safe here: backticks, $vars, "quotes", newlines.',
            },
            "visible_replies_must_not_be_returned_in_executor_output": True,
            "executor_exit_status_drives_completion": True,
            "executor_stdout_stderr_are_operator_transcript": True,
            "visible_reply_exit_status": (
                "python3 -P -m app.main_reply --spec-file sends synchronously and its exit code "
                "tells you whether the user actually received the reply: 0 = delivered; 1 = the "
                "handler rejected it for good (for example a missing or unreadable "
                "attachment) so you must adjust and resend, e.g. without the attachment; "
                "3 = the handler was unreachable so you may retry; 4 = newer messages from "
                "this conversation were claimed and returned in stdout, so the drafted "
                "message/attachment was withheld: incorporate every follow-up and call "
                "main_reply again before exiting. Except for a reaction included alongside "
                "exit 4, a non-zero exit means the reply did NOT reach the user."
            ),
        },
        "scheduling_contract": {
            "schedule_api": "/api/schedules",
            "reminder_api": "/api/reminders",
            "instructions": (
                "Use the handler JSON APIs directly; there is no scheduling CLI. For POST/PUT, "
                "write JSON to a temporary file and use curl --data-binary @<path>. Copy the "
                "current task reply_channel unless the user explicitly requests another target. "
                "Reminder run_at values must include Z or an explicit offset and every reminder "
                "POST/PUT needs an idempotency_key. Any 4xx JSON response includes a usage object "
                "with endpoints, an example body, and correction notes."
            ),
        },
    }
    reply_to_message_id = envelope.reply_channel.metadata.get("reply_to_message_id")
    if (
        envelope.reply_channel.type == "telegram"
        and isinstance(reply_to_message_id, int)
        and not isinstance(reply_to_message_id, bool)
        and reply_to_message_id > 0
    ):
        payload["history_contract"] = {
            "anchor": {
                "channel": "telegram",
                "target": envelope.reply_channel.target,
                "message_id": reply_to_message_id,
            },
            "retrieve_command": (
                "python3 -P -m app.main_history --channel telegram "
                f"--target {envelope.reply_channel.target} "
                f"--around-message-id {reply_to_message_id}"
            ),
            "instructions": (
                "This message reply-quotes an earlier Telegram message. Use the supported "
                "history command if the quoted exchange or nearby turns would help; do not "
                "query SQLite directly. Worker-owned history starts at deployment, so an old "
                "anchor may not yet be present."
            ),
        }
    return payload


def _error_result(
    error: str,
    *,
    stdout: str | None = None,
    stderr: str | None = None,
) -> ExecutionResult:
    return ExecutionResult(
        errors=[error],
        stdout=stdout,
        stderr=stderr,
    )


__all__ = ["CodexExecutor"]
