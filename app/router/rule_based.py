"""Rule-based router baseline for the bootstrap prototype."""

from __future__ import annotations

from dataclasses import dataclass

from app.models import ExecutionConstraints, RoutedTask, TaskEnvelope


@dataclass(frozen=True)
class RuleBasedRouter:
    """Map canonical envelopes to routed tasks using simple source rules."""

    default_timeout_seconds: int = 1800
    default_max_tokens: int = 12000
    cron_timeout_seconds: int = 1800
    cron_max_tokens: int = 8000

    def route(self, envelope: TaskEnvelope) -> RoutedTask:
        workflow = "respond_and_optionally_edit"
        priority = "normal"
        timeout_seconds = self.default_timeout_seconds
        max_tokens = self.default_max_tokens

        if envelope.source == "cron":
            workflow = "scheduled_automation"
            priority = "low"
            timeout_seconds = self.cron_timeout_seconds
            max_tokens = self.cron_max_tokens
        elif _contains_urgent_signal(envelope.content):
            priority = "high"

        return RoutedTask(
            task_id=f"task:{envelope.id}",
            envelope_id=envelope.id,
            workflow=workflow,
            priority=priority,
            execution_constraints=ExecutionConstraints(
                timeout_seconds=timeout_seconds,
                max_tokens=max_tokens,
            ),
            policy_profile=envelope.policy_profile,
            source=envelope.source,
            actor=envelope.actor,
            content=envelope.content,
            reply_channel=envelope.reply_channel,
        )


def _contains_urgent_signal(content: str) -> bool:
    normalized = content.lower()
    return "urgent" in normalized or "asap" in normalized


__all__ = ["RuleBasedRouter"]
