"""Eligibility and rotation policy for speculative weekly-quota work."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, TypeVar
import math
import random

T = TypeVar("T")
WEEK_MINUTES = 7 * 24 * 60


def eligible(snapshot: dict, observed_at: float, now: float) -> bool:
    """Require a recent, unblocked reading and a future weekly reset."""
    if not all(math.isfinite(x) for x in (observed_at, now)):
        return False
    if not 0 <= now - observed_at <= 180:
        return False
    if snapshot.get("ordinary_usage_allowed") is not True:
        return False
    windows = snapshot.get("windows")
    if not isinstance(windows, list):
        return False
    weekly = [
        w
        for w in windows
        if isinstance(w, dict) and w.get("duration_minutes") == WEEK_MINUTES
    ]
    if len(weekly) != 1:
        return False
    used, reset = weekly[0].get("used_percent"), weekly[0].get("resets_at")
    if isinstance(used, bool) or not isinstance(used, (int, float)):
        return False
    if isinstance(reset, bool) or not isinstance(reset, (int, float)):
        return False
    return (
        math.isfinite(used)
        and math.isfinite(reset)
        and 0 <= used < 90
        and 0 < reset - now < 6 * 3600
    )


@dataclass(frozen=True)
class Job:
    name: str
    prompt: str
    model_tier: str


def choose_pair(
    jobs: Sequence[Job],
    repos: Sequence[str],
    recent: Sequence[tuple[str, str]],
    *,
    rng: random.Random,
) -> tuple[Job, str]:
    """Randomly choose among the least recently attempted job/repo pairs."""
    pairs = [(job, repo) for job in jobs for repo in repos]
    if not pairs:
        raise ValueError("at least one job and repository are required")
    rank = {pair: i for i, pair in enumerate(recent)}
    oldest = min(rank.get((job.name, repo), -1) for job, repo in pairs)
    return rng.choice(
        [(job, repo) for job, repo in pairs if rank.get((job.name, repo), -1) == oldest]
    )
