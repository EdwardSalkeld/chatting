"""Executor interface contracts."""

from __future__ import annotations

from typing import Protocol

from app.models import ExecutionResult, RoutedTask


class Executor(Protocol):
    """Execute a routed task and return structured results."""

    def execute(self, task: RoutedTask) -> ExecutionResult:
        """Run task logic and emit execution outputs for policy evaluation."""


__all__ = ["Executor"]
