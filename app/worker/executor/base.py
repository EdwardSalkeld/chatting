"""Executor interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import ExecutionResult, RoutedTask


@runtime_checkable
class Executor(Protocol):
    """Execute a routed task and return completion status plus transcript."""

    def execute(self, task: RoutedTask) -> ExecutionResult:
        """Run task logic and return any errors plus captured stdout/stderr."""
        ...


__all__ = ["Executor"]
