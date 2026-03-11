"""Deterministic stub executor for milestone bootstrap flow."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.models import ActionProposal, ExecutionResult, OutboundMessage, RoutedTask


@dataclass(frozen=True)
class StubExecutor:
    """Return deterministic outputs without invoking external tools."""

    def execute(
        self,
        task: RoutedTask,
        reply_send: Callable[[dict[str, Any]], None] | None = None,
    ) -> ExecutionResult:
        del reply_send
        channel = "log"
        target = task.envelope_id
        if task.reply_channel is not None:
            channel = task.reply_channel.type
            target = task.reply_channel.target

        message = OutboundMessage(
            channel=channel,
            target=target,
            body=f"Handled workflow {task.workflow}",
        )

        if "blocked" in task.envelope_id:
            return ExecutionResult(
                messages=[message],
                actions=[ActionProposal(type="run_shell", path="echo blocked")],
                config_updates=[],
                requires_human_review=False,
                errors=[],
            )

        return ExecutionResult(
            messages=[message],
            actions=[],
            config_updates=[],
            requires_human_review=False,
            errors=[],
        )


__all__ = ["StubExecutor"]
