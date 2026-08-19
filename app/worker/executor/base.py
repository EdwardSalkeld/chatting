"""Executor interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import ExecutionResult, TaskEnvelope, UsageReport


@runtime_checkable
class Executor(Protocol):
    """Execute a task envelope and return completion status plus transcript."""

    def execute(self, envelope: TaskEnvelope) -> ExecutionResult:
        """Run task logic and return any errors plus captured stdout/stderr."""
        ...


# Kept separate from Executor so an executor can exist without one: reporting
# usage is a backend-specific lookup, not part of running a task.
@runtime_checkable
class UsageReporter(Protocol):
    """Report backend account usage without running any model work."""

    def usage_report(self) -> UsageReport:
        """Return the most recent usage snapshot the backend has published."""
        ...


__all__ = ["Executor", "UsageReporter"]
