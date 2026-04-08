"""Cron-based scheduled-event connector."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadCronError, croniter

from app.models import PromptContext, ReplyChannel, TaskEnvelope


@dataclass(frozen=True)
class IntervalScheduleJob:
    """Configuration for a scheduled job."""

    job_name: str
    content: str
    context_refs: list[str]
    cron: str
    prompt_context: list[str] = field(default_factory=list)
    timezone_name: str | None = None
    reply_channel_type: str | None = None
    reply_channel_target: str | None = None

    def __post_init__(self) -> None:
        if not self.job_name:
            raise ValueError("job_name is required")
        if not self.content:
            raise ValueError("content is required")
        if not isinstance(self.prompt_context, list) or not all(
            isinstance(item, str) and item.strip() for item in self.prompt_context
        ):
            raise ValueError("prompt_context must be a list of non-empty strings")
        if not self.cron.strip():
            raise ValueError("cron must be non-empty when provided")
        if self.timezone_name is not None:
            if not self.timezone_name.strip():
                raise ValueError("timezone must be non-empty when provided")
            _load_timezone(self.timezone_name)
        _validate_cron(self.cron)
        if self.reply_channel_type is not None and not self.reply_channel_type.strip():
            raise ValueError("reply_channel_type must be non-empty when provided")
        if self.reply_channel_target is not None and not self.reply_channel_target.strip():
            raise ValueError("reply_channel_target must be non-empty when provided")
        if (self.reply_channel_type is None) != (self.reply_channel_target is None):
            raise ValueError(
                "reply_channel_type and reply_channel_target must be provided together"
            )


class IntervalScheduleConnector:
    """Emit cron-source envelopes when configured jobs are due."""

    source = "cron"

    def __init__(
        self,
        jobs: list[IntervalScheduleJob],
        *,
        global_prompt_context: list[str] | None = None,
        source_prompt_context: list[str] | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._jobs = jobs
        self._global_prompt_context = list(global_prompt_context or [])
        self._source_prompt_context = list(source_prompt_context or [])
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._next_run_at_by_job: dict[str, datetime] = {}

    def poll(self) -> list[TaskEnvelope]:
        now = _ensure_utc(self._now_provider())
        envelopes: list[TaskEnvelope] = []

        for job in self._jobs:
            next_run_at = self._next_run_at_by_job.get(job.job_name)
            if next_run_at is None:
                next_run_at = _initial_next_run_at(job=job, now=now)

            if now < next_run_at:
                self._next_run_at_by_job[job.job_name] = next_run_at
                continue

            event_id = f"cron:{job.job_name}:{next_run_at.isoformat()}"
            reply_channel = _job_reply_channel(job)
            envelopes.append(
                TaskEnvelope(
                    id=event_id,
                    source="cron",
                    received_at=now,
                    actor=None,
                    content=job.content,
                    attachments=[],
                    context_refs=job.context_refs,
                    reply_channel=reply_channel,
                    dedupe_key=event_id,
                    prompt_context=PromptContext(
                        global_instructions=self._global_prompt_context,
                        source_instructions=self._source_prompt_context,
                        task_instructions=job.prompt_context,
                    ),
                )
            )

            self._next_run_at_by_job[job.job_name] = _next_due_time(
                job=job,
                next_run_at=next_run_at,
                now=now,
            )

        return envelopes


def _initial_next_run_at(*, job: IntervalScheduleJob, now: datetime) -> datetime:
    return _find_next_cron_time(job=job, reference=now, inclusive=True)


def _next_due_time(*, job: IntervalScheduleJob, next_run_at: datetime, now: datetime) -> datetime:
    del next_run_at
    return _find_next_cron_time(job=job, reference=now, inclusive=False)


def _find_next_cron_time(*, job: IntervalScheduleJob, reference: datetime, inclusive: bool) -> datetime:
    tz = _job_timezone(job)
    local_ref = _ensure_utc(reference).astimezone(tz)
    if inclusive:
        truncated = local_ref.replace(second=0, microsecond=0)
        if croniter.match(job.cron, truncated):
            return truncated.astimezone(timezone.utc)
    return croniter(job.cron, local_ref).get_next(datetime).astimezone(timezone.utc)


def _validate_cron(expression: str) -> None:
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError("cron must contain exactly 5 fields")
    try:
        croniter(expression)
    except (CroniterBadCronError, ValueError, KeyError) as exc:
        raise ValueError(f"invalid cron expression: {exc}") from exc


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


def _load_timezone(value: str) -> ZoneInfo:
    try:
        return ZoneInfo(value.strip())
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"invalid timezone: {value}") from exc


def _job_timezone(job: IntervalScheduleJob) -> ZoneInfo:
    timezone_name = job.timezone_name if job.timezone_name is not None else "UTC"
    return _load_timezone(timezone_name)


def _job_reply_channel(job: IntervalScheduleJob) -> ReplyChannel:
    if job.reply_channel_type is None or job.reply_channel_target is None:
        return ReplyChannel(type="log", target=job.job_name)
    return ReplyChannel(
        type=job.reply_channel_type.strip(),
        target=job.reply_channel_target.strip(),
    )


__all__ = ["IntervalScheduleJob", "IntervalScheduleConnector"]
