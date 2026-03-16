"""Interval and cron-based scheduled-event connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.models import ReplyChannel, TaskEnvelope


@dataclass(frozen=True)
class IntervalScheduleJob:
    """Configuration for a scheduled job."""

    job_name: str
    content: str
    context_refs: list[str]
    interval_seconds: int | None = None
    cron: str | None = None
    timezone_name: str | None = None
    policy_profile: str = "default"
    start_at: datetime | None = None
    reply_channel_type: str | None = None
    reply_channel_target: str | None = None

    def __post_init__(self) -> None:
        if not self.job_name:
            raise ValueError("job_name is required")
        if not self.content:
            raise ValueError("content is required")
        if self.interval_seconds is None and self.cron is None:
            raise ValueError("interval_seconds or cron is required")
        if self.interval_seconds is not None and self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.cron is not None and not self.cron.strip():
            raise ValueError("cron must be non-empty when provided")
        if self.cron is None and self.timezone_name is not None:
            raise ValueError("timezone is only supported when cron is provided")
        if self.cron is not None and self.timezone_name is None:
            raise ValueError("timezone is required when cron is provided")
        if self.timezone_name is not None:
            if not self.timezone_name.strip():
                raise ValueError("timezone must be non-empty when provided")
            _load_timezone(self.timezone_name)
        if self.cron is not None:
            _CronExpression.parse(self.cron)
        if self.start_at is not None and self.start_at.tzinfo is None:
            raise ValueError("start_at must be timezone-aware")
        if self.reply_channel_type is not None and not self.reply_channel_type.strip():
            raise ValueError("reply_channel_type must be non-empty when provided")
        if self.reply_channel_target is not None and not self.reply_channel_target.strip():
            raise ValueError("reply_channel_target must be non-empty when provided")
        if (self.reply_channel_type is None) != (self.reply_channel_target is None):
            raise ValueError(
                "reply_channel_type and reply_channel_target must be provided together"
            )


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
                    policy_profile=job.policy_profile,
                    reply_channel=reply_channel,
                    dedupe_key=event_id,
                )
            )

            self._next_run_at_by_job[job.job_name] = _next_due_time(
                job=job,
                next_run_at=next_run_at,
                now=now,
            )

        return envelopes


def _initial_next_run_at(*, job: IntervalScheduleJob, now: datetime) -> datetime:
    if job.cron is not None:
        return _find_next_cron_time(job=job, reference=now, inclusive=True)
    baseline = job.start_at if job.start_at is not None else now
    return _ensure_utc(baseline)


def _next_due_time(*, job: IntervalScheduleJob, next_run_at: datetime, now: datetime) -> datetime:
    if job.cron is not None:
        return _find_next_cron_time(job=job, reference=now, inclusive=False)
    if job.interval_seconds is None:
        raise ValueError("interval_seconds must be configured for interval schedules")
    next_due = next_run_at + timedelta(seconds=job.interval_seconds)
    while next_due <= now:
        next_due += timedelta(seconds=job.interval_seconds)
    return next_due


def _find_next_cron_time(*, job: IntervalScheduleJob, reference: datetime, inclusive: bool) -> datetime:
    if job.cron is None:
        raise ValueError("cron must be configured for cron schedules")
    expression = _CronExpression.parse(job.cron)
    timezone_info = _job_timezone(job)
    reference_utc = _ensure_utc(reference)
    candidate = reference_utc.replace(second=0, microsecond=0)
    if not inclusive and candidate <= reference_utc:
        candidate += timedelta(minutes=1)

    for _ in range(366 * 24 * 60 * 2):
        localized = candidate.astimezone(timezone_info)
        if expression.matches(localized):
            return candidate
        candidate += timedelta(minutes=1)
    raise ValueError(f"unable to find next cron time for expression: {job.cron}")


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


@dataclass(frozen=True)
class _CronField:
    allowed: frozenset[int]
    wildcard: bool = False

    def matches(self, value: int) -> bool:
        return value in self.allowed


@dataclass(frozen=True)
class _CronExpression:
    minute: _CronField
    hour: _CronField
    day_of_month: _CronField
    month: _CronField
    day_of_week: _CronField

    @classmethod
    def parse(cls, value: str) -> "_CronExpression":
        fields = value.split()
        if len(fields) != 5:
            raise ValueError("cron must contain exactly 5 fields")
        minute, hour, day_of_month, month, day_of_week = fields
        return cls(
            minute=_parse_cron_field(minute, minimum=0, maximum=59, field_name="minute"),
            hour=_parse_cron_field(hour, minimum=0, maximum=23, field_name="hour"),
            day_of_month=_parse_cron_field(
                day_of_month,
                minimum=1,
                maximum=31,
                field_name="day_of_month",
            ),
            month=_parse_cron_field(month, minimum=1, maximum=12, field_name="month"),
            day_of_week=_parse_cron_field(
                day_of_week,
                minimum=0,
                maximum=7,
                field_name="day_of_week",
                normalize=lambda candidate: 0 if candidate == 7 else candidate,
            ),
        )

    def matches(self, local_dt: datetime) -> bool:
        cron_day_of_week = (local_dt.weekday() + 1) % 7
        day_of_month_matches = self.day_of_month.matches(local_dt.day)
        day_of_week_matches = self.day_of_week.matches(cron_day_of_week)
        if self.day_of_month.wildcard and self.day_of_week.wildcard:
            day_matches = True
        elif self.day_of_month.wildcard:
            day_matches = day_of_week_matches
        elif self.day_of_week.wildcard:
            day_matches = day_of_month_matches
        else:
            day_matches = day_of_month_matches or day_of_week_matches
        return (
            self.minute.matches(local_dt.minute)
            and self.hour.matches(local_dt.hour)
            and self.month.matches(local_dt.month)
            and day_matches
        )


def _parse_cron_field(
    raw_value: str,
    *,
    minimum: int,
    maximum: int,
    field_name: str,
    normalize: Callable[[int], int] | None = None,
) -> _CronField:
    normalize_value = normalize or (lambda candidate: candidate)
    if not raw_value:
        raise ValueError(f"cron {field_name} must not be empty")
    if raw_value == "*":
        return _CronField(
            frozenset(normalize_value(candidate) for candidate in range(minimum, maximum + 1)),
            wildcard=True,
        )

    allowed: set[int] = set()
    for chunk in raw_value.split(","):
        chunk = chunk.strip()
        if not chunk:
            raise ValueError(f"cron {field_name} contains an empty segment")

        step = 1
        base = chunk
        if "/" in chunk:
            base, raw_step = chunk.split("/", 1)
            if not raw_step.isdigit() or int(raw_step) <= 0:
                raise ValueError(f"cron {field_name} step must be a positive integer")
            step = int(raw_step)

        if base == "*":
            start = minimum
            end = maximum
        elif "-" in base:
            raw_start, raw_end = base.split("-", 1)
            start = _parse_cron_int(
                raw_start,
                minimum=minimum,
                maximum=maximum,
                field_name=field_name,
            )
            end = _parse_cron_int(
                raw_end,
                minimum=minimum,
                maximum=maximum,
                field_name=field_name,
            )
            if start > end:
                raise ValueError(f"cron {field_name} range start must be <= end")
        else:
            start = _parse_cron_int(
                base,
                minimum=minimum,
                maximum=maximum,
                field_name=field_name,
            )
            end = start

        for candidate in range(start, end + 1, step):
            allowed.add(normalize_value(candidate))

    return _CronField(frozenset(allowed), wildcard=False)


def _parse_cron_int(raw_value: str, *, minimum: int, maximum: int, field_name: str) -> int:
    if not raw_value.isdigit():
        raise ValueError(f"cron {field_name} must use numeric values")
    parsed = int(raw_value)
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"cron {field_name} must be between {minimum} and {maximum}")
    return parsed


def _job_reply_channel(job: IntervalScheduleJob) -> ReplyChannel:
    if job.reply_channel_type is None or job.reply_channel_target is None:
        return ReplyChannel(type="log", target=job.job_name)
    return ReplyChannel(
        type=job.reply_channel_type.strip(),
        target=job.reply_channel_target.strip(),
    )


__all__ = ["IntervalScheduleJob", "IntervalScheduleConnector"]
