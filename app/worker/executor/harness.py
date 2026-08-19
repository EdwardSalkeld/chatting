"""Running an agent harness as a subprocess.

Every harness chatting drives works the same way: hand it the task payload on
stdin, let it publish its own replies, and read completion from its exit status.
Only the argv differs, so that is all an executor needs to supply.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime
from typing import Mapping

from app.models import ExecutionResult, TaskEnvelope
from app.worker.executor.payload import build_task_payload


def run_harness(
    *,
    command: tuple[str, ...],
    envelope: TaskEnvelope,
    current_time: datetime,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    timeout_seconds: int,
) -> ExecutionResult:
    """Run one harness invocation and capture its streams as the transcript."""
    payload = json.dumps(build_task_payload(envelope, current_time=current_time))
    try:
        completed = subprocess.run(
            command,
            input=payload,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
            cwd=cwd,
            env=dict(env) if env is not None else None,
        )
    except subprocess.TimeoutExpired:
        return harness_error("executor_timeout")

    if completed.returncode != 0:
        error = f"executor_exit_nonzero:{completed.returncode}"
        stderr = completed.stderr.strip()
        if stderr:
            error = f"{error}:{stderr}"
        return harness_error(
            error,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )

    return ExecutionResult(
        errors=[],
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def harness_error(
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


__all__ = ["harness_error", "run_harness"]
