"""Policy engine interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import ExecutionResult, PolicyDecision


@runtime_checkable
class PolicyEngine(Protocol):
    """Evaluate execution output and return an enforceable policy decision."""

    def evaluate(self, result: ExecutionResult) -> PolicyDecision:
        """Gate actions/messages/config updates according to policy rules."""
        ...


__all__ = ["PolicyEngine"]
