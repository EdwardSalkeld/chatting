"""Executor interface contracts."""

from __future__ import annotations

from typing import Any, Callable, Protocol, runtime_checkable

from app.models import ExecutionResult, RoutedTask


@runtime_checkable
class Executor(Protocol):
    """Execute a routed task and return structured results."""

    def execute(
        self,
        task: RoutedTask,
        reply_send: Callable[[dict[str, Any]], None] | None = None,
    ) -> ExecutionResult:
        """Run task logic and emit execution outputs for policy evaluation."""


__all__ = ["Executor"]
