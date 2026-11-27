"""Eligibility and rotation policy for speculative weekly-quota work."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence
import math
import random
import json
import time
import uuid
import urllib.request
from datetime import datetime, timezone

from app.model_command import MODELS

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


def tick(store, config: dict, handler_url: str, *, now: float | None = None) -> bool:
    """Create at most one durable handler reminder; safe to retry after a crash."""
    now = time.time() if now is None else now
    if not config.get("enabled", False):
        return False
    repos = config.get("repositories", [])
    jobs_config = config.get("jobs", [])
    if (
        not isinstance(repos, list)
        or not repos
        or not all(isinstance(p, str) and p.startswith("/") for p in repos)
    ):
        raise ValueError("speculative repositories must be absolute paths")
    if not isinstance(jobs_config, list) or not jobs_config:
        raise ValueError("speculative jobs must be a nonempty list")
    jobs = [
        Job(j["name"], j["prompt"], j.get("model_tier", "high")) for j in jobs_config
    ]
    if any(
        not j.name
        or not j.prompt
        or j.model_tier not in ("high", "low", "speculative", "speculative_deep")
        for j in jobs
    ):
        raise ValueError("invalid speculative job")
    with store._transaction() as db:
        db.execute("BEGIN IMMEDIATE")
        quota = db.execute("SELECT * FROM codex_quota_snapshot WHERE id=1").fetchone()
        if not quota or not eligible(
            json.loads(quota["snapshot_json"]), quota["observed_at"], now
        ):
            return False
        if db.execute(
            "SELECT 1 FROM worker_inbox WHERE state IN ('pending','active','closing','attached') LIMIT 1"
        ).fetchone():
            return False
        pending = db.execute(
            "SELECT * FROM speculative_runs WHERE task_id IS NULL ORDER BY created_at LIMIT 1"
        ).fetchone()
        if pending is None:
            # A published reminder can take time to reach the worker inbox.
            outstanding = db.execute(
                "SELECT 1 FROM speculative_runs s LEFT JOIN worker_inbox i ON s.task_id=i.task_id WHERE s.task_id IS NOT NULL AND (i.task_id IS NULL OR i.state != 'completed') LIMIT 1"
            ).fetchone()
            if outstanding:
                return False
            last = db.execute(
                "SELECT i.updated_at FROM speculative_runs s JOIN worker_inbox i ON i.task_id=s.task_id WHERE i.state='completed' ORDER BY s.created_at DESC LIMIT 1"
            ).fetchone()
            if (
                last
                and quota["observed_at"]
                <= datetime.fromisoformat(
                    last["updated_at"].replace("Z", "+00:00")
                ).timestamp()
            ):
                return False
            history = [
                (r["job"], r["repo"])
                for r in db.execute(
                    "SELECT job,repo FROM speculative_runs ORDER BY created_at ASC"
                ).fetchall()
            ]
            job, repo = choose_pair(jobs, repos, history, rng=random.SystemRandom())
            run_id = uuid.uuid4().hex
            request = {
                "run_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                "prompt": job.prompt,
                "reply_channel": {
                    "type": "log",
                    "target": "speculative:" + run_id,
                    "metadata": {"speculative_run": run_id},
                },
                "context_refs": ["repo:" + repo],
                "prompt_context": [
                    f"This is speculative background work: job {job.name}, repository {repo}, model {MODELS[job.model_tier]}. Before investigating, use the task's Telegram topic contract to create or reuse a topic for this work item. Send a short start message to that topic naming the job, repository and model. Then use a fresh checkout or workspace and check recent issues and PRs for related findings. Open a PR for worthwhile changes. At the end, send the outcome to the same topic, even if nothing worth reporting was found. Describe any incomplete investigation honestly."
                ],
                "created_from_task_id": "speculative-worker",
                "created_by": "worker",
                "idempotency_key": "speculative:" + run_id,
            }
            payload = json.dumps(request, sort_keys=True)
            db.execute(
                "INSERT INTO speculative_runs VALUES (?, ?, ?, ?, ?, NULL, ?)",
                (run_id, job.name, repo, job.model_tier, payload, now),
            )
        else:
            run_id, payload = pending["id"], pending["request_json"]
    req = urllib.request.Request(
        handler_url.rstrip("/") + "/api/reminders",
        data=payload.encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        result = json.load(response)
    reminder = result["reminder"]
    task_id = f"task:reminder:{reminder['reminder_id']}:{reminder['revision']}"
    with store._transaction() as db:
        db.execute(
            "UPDATE speculative_runs SET task_id=? WHERE id=?", (task_id, run_id)
        )
    return True
