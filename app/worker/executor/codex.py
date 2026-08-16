"""Codex executor with timeout control and transcript capture."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from app.models import ExecutionResult, SCHEMA_VERSION, TaskEnvelope, parse_context_ref


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
