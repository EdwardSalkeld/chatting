"""Applier interface contracts."""

from __future__ import annotations

from typing import Protocol

from app.models import ApplyResult, PolicyDecision


class Applier(Protocol):
    """Apply policy-approved changes and dispatch responses."""

    def apply(self, decision: PolicyDecision) -> ApplyResult:
        """Execute approved operations and return apply summary."""


__all__ = ["Applier"]
