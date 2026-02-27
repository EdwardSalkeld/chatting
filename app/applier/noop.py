"""No-op applier baseline for milestone bootstrap."""

from __future__ import annotations

from dataclasses import dataclass

from app.models import ApplyResult, PolicyDecision


@dataclass(frozen=True)
class NoOpApplier:
    """Baseline applier that reports intended effects without side effects."""

    def apply(self, decision: PolicyDecision) -> ApplyResult:
        reason_codes: list[str] = []

        if decision.approved_actions:
            reason_codes.append("noop_applier_skipped_actions")
        if decision.blocked_actions:
            reason_codes.append("policy_blocked_actions_present")

        return ApplyResult(
            applied_actions=[],
            skipped_actions=list(decision.approved_actions),
            dispatched_messages=list(decision.approved_messages),
            reason_codes=reason_codes,
        )


__all__ = ["NoOpApplier"]
