"""Interval-based scheduled-event connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from app.models import ReplyChannel, TaskEnvelope


@dataclass(frozen=True)
class IntervalScheduleJob:
    """Configuration for an interval-driven scheduled job."""

    job_name: str
    content: str
    interval_seconds: int
    context_refs: list[str]
    policy_profile: str = "default"
    start_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.job_name:
            raise ValueError("job_name is required")
        if not self.content:
            raise ValueError("content is required")
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.start_at is not None and self.start_at.tzinfo is None:
            raise ValueError("start_at must be timezone-aware")


class IntervalScheduleConnector:
    """Emit cron-source envelopes when configured interval jobs are due."""

    source = "cron"

    def __init__(
        self,
        jobs: list[IntervalScheduleJob],
        *,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._jobs = jobs
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._next_run_at_by_job: dict[str, datetime] = {}

    def poll(self) -> list[TaskEnvelope]:
        now = _ensure_utc(self._now_provider())
        envelopes: list[TaskEnvelope] = []

        for job in self._jobs:
            next_run_at = self._next_run_at_by_job.get(job.job_name)
            if next_run_at is None:
                baseline = job.start_at if job.start_at is not None else now
                next_run_at = _ensure_utc(baseline)

            if now < next_run_at:
                self._next_run_at_by_job[job.job_name] = next_run_at
                continue

            event_id = f"cron:{job.job_name}:{next_run_at.isoformat()}"
            envelopes.append(
                TaskEnvelope(
                    id=event_id,
                    source="cron",
                    received_at=now,
                    actor=None,
                    content=job.content,
                    attachments=[],
                    context_refs=job.context_refs,
                    policy_profile=job.policy_profile,
                    reply_channel=ReplyChannel(type="log", target=job.job_name),
                    dedupe_key=event_id,
                )
            )

            self._next_run_at_by_job[job.job_name] = _next_due_time(
                next_run_at=next_run_at,
                now=now,
                interval_seconds=job.interval_seconds,
            )

        return envelopes


def _next_due_time(*, next_run_at: datetime, now: datetime, interval_seconds: int) -> datetime:
    next_due = next_run_at + timedelta(seconds=interval_seconds)
    while next_due <= now:
        next_due += timedelta(seconds=interval_seconds)
    return next_due


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = ["IntervalScheduleJob", "IntervalScheduleConnector"]
