"""Allowlist policy engine baseline implementation."""

from __future__ import annotations

from dataclasses import dataclass, field

from app.models import (
    ConfigUpdate,
    ConfigUpdateDecision,
    ExecutionResult,
    PolicyDecision,
)


@dataclass(frozen=True)
class AllowlistPolicyEngine:
    """Deny-by-default policy with allowlisted action and config paths."""

    allowed_action_types: frozenset[str] = field(default_factory=frozenset)
    allowed_config_paths: frozenset[str] = field(default_factory=frozenset)
    sensitive_config_prefixes: tuple[str, ...] = ("secrets.", "credentials.", "auth.")

    def evaluate(self, result: ExecutionResult) -> PolicyDecision:
        approved_actions = []
        blocked_actions = []
        reason_codes: list[str] = []

        for action in result.actions:
            if action.type in self.allowed_action_types:
                approved_actions.append(action)
                continue
            blocked_actions.append(action)
            reason_codes.append("action_not_allowed")

        config_decision, config_reason_codes = self._evaluate_config_updates(
            result.config_updates
        )
        reason_codes.extend(config_reason_codes)

        if result.requires_human_review:
            reason_codes.append("executor_requires_human_review")
        if result.errors:
            reason_codes.append("executor_reported_errors")

        return PolicyDecision(
            approved_actions=approved_actions,
            blocked_actions=blocked_actions,
            approved_messages=result.messages,
            config_updates=config_decision,
            reason_codes=_dedupe_in_order(reason_codes),
        )

    def _evaluate_config_updates(
        self, updates: list[ConfigUpdate]
    ) -> tuple[ConfigUpdateDecision, list[str]]:
        approved: list[ConfigUpdate] = []
        pending_review: list[ConfigUpdate] = []
        rejected: list[ConfigUpdate] = []
        reason_codes: list[str] = []

        for update in updates:
            if self._is_sensitive_config(update.path):
                pending_review.append(update)
                reason_codes.append("config_update_requires_review")
                continue
            if update.path in self.allowed_config_paths:
                approved.append(update)
                continue
            rejected.append(update)
            reason_codes.append("config_update_not_allowed")

        return (
            ConfigUpdateDecision(
                approved=approved,
                pending_review=pending_review,
                rejected=rejected,
            ),
            reason_codes,
        )

    def _is_sensitive_config(self, path: str) -> bool:
        return any(path.startswith(prefix) for prefix in self.sensitive_config_prefixes)


def _dedupe_in_order(codes: list[str]) -> list[str]:
    seen: set[str] = set()
    unique_codes: list[str] = []
    for code in codes:
        if code in seen:
            continue
        seen.add(code)
        unique_codes.append(code)
    return unique_codes


__all__ = ["AllowlistPolicyEngine"]
