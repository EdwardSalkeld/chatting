"""goose executor, for trialling OpenRouter-backed models.

goose is configured entirely through its own environment — `GOOSE_PROVIDER`,
`GOOSE_MODEL`, `OPENROUTER_API_KEY`, `GOOSE_MODE=auto` — so which model is in use
is deliberately not chatting's business, exactly as it is not for Codex.

The default argv covers what an unattended run needs:

- `--no-session` keeps automated runs out of goose's session history
- `-i -` reads the task payload from stdin, the same transport Codex uses
- `--with-builtin developer` supplies the shell and file-editing tools the reply
  contract depends on
- `--max-turns` and `--max-tool-repetitions` bound a runaway loop, which Codex has
  no equivalent for; on pay-per-token billing an unbounded loop spends real money

`execute` is intentionally its own copy rather than shared with the Codex
executor. The two harnesses are expected to diverge — goose reports failure as a
Rust panic exiting 101 rather than a clean error, and while a non-zero status is
all the executor contract needs today, handling that shape properly is likely to
mean treating its streams differently. Sharing the body would make every such
tweak a change to the Codex path as well.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Mapping

from app.models import ExecutionResult, TaskEnvelope
from app.worker.executor.payload import build_task_payload

DEFAULT_MAX_TURNS = 40
DEFAULT_MAX_TOOL_REPETITIONS = 5

DEFAULT_GOOSE_COMMAND = (
    "goose",
    "run",
    "--no-session",
    "--with-builtin",
    "developer",
    "--max-turns",
    str(DEFAULT_MAX_TURNS),
    "--max-tool-repetitions",
    str(DEFAULT_MAX_TOOL_REPETITIONS),
    "-i",
    "-",
)


@dataclass(frozen=True)
class GooseExecutor:
    """Run goose as a subprocess and capture stdout/stderr as transcript."""

    command: tuple[str, ...] = DEFAULT_GOOSE_COMMAND
    cwd: str | None = None
    env: Mapping[str, str] | None = None
    timeout_seconds: int = 1800
    now_provider: Callable[[], datetime] = field(
        default=lambda: datetime.now(timezone.utc)
    )

    def execute(self, envelope: TaskEnvelope) -> ExecutionResult:
        payload = json.dumps(
            build_task_payload(envelope, current_time=self.now_provider())
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


__all__ = [
    "DEFAULT_GOOSE_COMMAND",
    "DEFAULT_MAX_TOOL_REPETITIONS",
    "DEFAULT_MAX_TURNS",
    "GooseExecutor",
]
