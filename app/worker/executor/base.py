"""Executor interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import ExecutionResult, TaskEnvelope


@runtime_checkable
class Executor(Protocol):
    """Execute a task envelope and return completion status plus transcript."""

    def execute(self, envelope: TaskEnvelope) -> ExecutionResult:
        """Run task logic and return any errors plus captured stdout/stderr."""
        ...


__all__ = ["Executor"]
