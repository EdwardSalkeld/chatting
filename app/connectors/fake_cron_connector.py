"""Fake cron connector used by bootstrap flow and tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.models import ReplyChannel, TaskEnvelope


@dataclass(frozen=True)
class CronTrigger:
    """In-memory representation of a scheduled trigger."""

    job_name: str
    content: str
    scheduled_for: datetime
    context_refs: list[str]
    policy_profile: str = "default"


class FakeCronConnector:
    """Convert fake scheduled triggers into canonical task envelopes."""

    source = "cron"

    def __init__(self, triggers: list[CronTrigger]) -> None:
        self._triggers = triggers

    def poll(self) -> list[TaskEnvelope]:
        envelopes: list[TaskEnvelope] = []
        for trigger in self._triggers:
            scheduled_for = _ensure_utc(trigger.scheduled_for)
            event_id = f"cron:{trigger.job_name}:{scheduled_for.isoformat()}"
            envelopes.append(
                TaskEnvelope(
                    id=event_id,
                    source="cron",
                    received_at=datetime.now(timezone.utc),
                    actor=None,
                    content=trigger.content,
                    attachments=[],
                    context_refs=trigger.context_refs,
                    policy_profile=trigger.policy_profile,
                    reply_channel=ReplyChannel(type="log", target=trigger.job_name),
                    dedupe_key=event_id,
                )
            )
        return envelopes



def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("scheduled_for must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = ["CronTrigger", "FakeCronConnector"]
