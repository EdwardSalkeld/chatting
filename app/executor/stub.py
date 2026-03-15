"""Deterministic stub executor for milestone bootstrap flow."""

from __future__ import annotations

from dataclasses import dataclass

from app.models import ActionProposal, ExecutionResult, RoutedTask


@dataclass(frozen=True)
class StubExecutor:
    """Return deterministic outputs without invoking external tools."""

    def execute(self, task: RoutedTask) -> ExecutionResult:
        if "blocked" in task.envelope_id:
            return ExecutionResult(
                actions=[ActionProposal(type="run_shell", path="echo blocked")],
                config_updates=[],
                requires_human_review=False,
                errors=[],
            )

        return ExecutionResult(
            actions=[],
            config_updates=[],
            requires_human_review=False,
            errors=[],
        )


__all__ = ["StubExecutor"]
