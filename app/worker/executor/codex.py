"""Codex executor with timeout control and transcript capture."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from app.models import (
    ExecutionResult,
    SCHEMA_VERSION,
    TaskEnvelope,
    parse_context_ref,
)


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
        payload = json.dumps(
            _task_payload(envelope, current_time=self.now_provider())
        )
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


def _task_payload(
    envelope: TaskEnvelope, *, current_time: datetime
) -> dict[str, Any]:
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
        "context": [
            parse_context_ref(ref).to_dict() for ref in envelope.context_refs
        ],
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
    return {
        "schema_version": SCHEMA_VERSION,
        "task": task_dict,
        "current_time": current_time.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "reply_contract": {
            "visible_replies_must_use": "python3 -m app.main_reply",
            "visible_replies_must_not_be_returned_in_executor_output": True,
            "executor_exit_status_drives_completion": True,
            "executor_stdout_stderr_are_operator_transcript": True,
        },
    }


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
