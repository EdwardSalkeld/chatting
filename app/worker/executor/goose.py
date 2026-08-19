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

Note goose reports failures as Rust panics exiting 101 rather than clean errors.
That satisfies the executor contract, which only needs a non-zero status, but the
panic text does not use the wording `_looks_like_credit_exhaustion` looks for, so
credit exhaustion on this executor surfaces as a generic failure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Mapping

from app.models import ExecutionResult, TaskEnvelope
from app.worker.executor.harness import run_harness

DEFAULT_MAX_TURNS = 40
DEFAULT_MAX_TOOL_REPETITIONS = 5


@dataclass(frozen=True)
class GooseExecutor:
    """Run goose as a subprocess and capture stdout/stderr as transcript."""

    command: tuple[str, ...] = (
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
    cwd: str | None = None
    env: Mapping[str, str] | None = None
    timeout_seconds: int = 1800
    now_provider: Callable[[], datetime] = field(
        default=lambda: datetime.now(timezone.utc)
    )

    def execute(self, envelope: TaskEnvelope) -> ExecutionResult:
        return run_harness(
            command=self.command,
            envelope=envelope,
            current_time=self.now_provider(),
            cwd=self.cwd,
            env=self.env,
            timeout_seconds=self.timeout_seconds,
        )


__all__ = ["DEFAULT_MAX_TOOL_REPETITIONS", "DEFAULT_MAX_TURNS", "GooseExecutor"]
