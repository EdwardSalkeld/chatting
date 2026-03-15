"""Executor interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import ExecutionResult, RoutedTask


@runtime_checkable
class Executor(Protocol):
    """Execute a routed task and return completion-only structured results."""

    def execute(self, task: RoutedTask) -> ExecutionResult:
        """Run task logic and return actions/config/errors only."""


__all__ = ["Executor"]
