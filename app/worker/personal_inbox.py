"""Personal inbox classification, run as a thread of the ordinary worker.

It shares the worker's process and its TypeSafe key, but not its executor: the
only thing this code can do with an email is ask two fixed HTTPS endpoints about
it and publish the answer. It never touches the mailbox, the task queue the
executor reads, or the filesystem beyond its own cache.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
import sqlite3
from collections.abc import Callable
from threading import Event, Thread
from typing import Any

from app.broker.bbmb_client import BBMBQueueAdapter
from app.personal_inbox import (
    DEAD_QUEUE,
    RESULTS_QUEUE,
    TASKS_QUEUE,
    classify,
    digest_lines,
    validate_digest_task,
    validate_task,
)

LOGGER = logging.getLogger(__name__)


def prepare(
    task: object, *, jev_key: str, summary_model: str
) -> tuple[str, Callable[[], dict[str, Any]]] | None:
    """The cache key and the work for one task, or None if it is not ours."""
    if not isinstance(task, dict):
        return None
    kind = task.get("kind")
    if kind == "personal_inbox.classify.v1":
        validate_task(task)
        return task["message_id"], lambda: classify(
            task, jev_key=jev_key, summary_model=summary_model
        )
    if kind == "personal_inbox.digest.v1":
        validate_digest_task(task)
        return task["digest_id"], lambda: digest_lines(
            task, summary_model=summary_model
        )
    return None


def process_one(
    broker: BBMBQueueAdapter,
    cache: sqlite3.Connection,
    *,
    jev_key: str,
    summary_model: str,
) -> bool:
    picked = broker.pickup_json(TASKS_QUEUE, timeout_seconds=120, wait_seconds=1)
    if picked is None:
        return False
    try:
        prepared = prepare(picked.payload, jev_key=jev_key, summary_model=summary_model)
    except (ValueError, TypeError, KeyError):
        prepared = None
    if prepared is None:
        broker.publish_json(DEAD_QUEUE, {"guid": picked.guid, "reason": "invalid_task"})
        broker.ack(TASKS_QUEUE, picked.guid)
        return True
    task_id, work = prepared
    fingerprint = hashlib.sha256(
        json.dumps(picked.payload, sort_keys=True).encode()
    ).hexdigest()
    row = cache.execute(
        "SELECT fingerprint, result FROM classifications WHERE id=?", (task_id,)
    ).fetchone()
    if row is not None:
        if row[0] != fingerprint:
            broker.publish_json(
                DEAD_QUEUE, {"guid": picked.guid, "reason": "identity_conflict"}
            )
            broker.ack(TASKS_QUEUE, picked.guid)
            return True
        result = json.loads(row[1])
    else:
        result = work()
        with cache:
            cache.execute(
                "INSERT INTO classifications VALUES (?,?,?)",
                (task_id, fingerprint, json.dumps(result)),
            )
    # A publish/ACK failure replays the saved result rather than calling models
    # again. The handler accepts exactly one classification per mail identity.
    broker.publish_json(RESULTS_QUEUE, result)
    broker.ack(TASKS_QUEUE, picked.guid)
    return True


def open_cache(path: str) -> sqlite3.Connection:
    """Model output is cached before publication, so a failed publish or ACK
    replays the saved answer instead of paying for the call again."""
    Path(path).parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    cache = sqlite3.connect(path, timeout=5, check_same_thread=False)
    cache.execute("PRAGMA journal_mode=WAL")
    cache.execute(
        "CREATE TABLE IF NOT EXISTS classifications "
        "(id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL)"
    )
    return cache


@dataclass
class PersonalInboxCollector:
    broker: BBMBQueueAdapter
    cache: sqlite3.Connection
    jev_key: str
    summary_model: str
    _stop: Event = field(init=False, default_factory=Event)
    _thread: Thread | None = field(init=False, default=None)

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("personal inbox collector already started")
        self._thread = Thread(target=self._run, name="personal-inbox", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        for queue in (TASKS_QUEUE, RESULTS_QUEUE, DEAD_QUEUE):
            self.broker.ensure_queue(queue)
        while not self._stop.is_set():
            try:
                process_one(
                    self.broker,
                    self.cache,
                    jev_key=self.jev_key,
                    summary_model=self.summary_model,
                )
            except Exception as error:  # noqa: BLE001
                # Never log request bodies, mail, credentials or URLs with
                # tokens. An unacked task is leased for a later retry.
                LOGGER.error("personal_inbox_failed type=%s", type(error).__name__)
                self._stop.wait(5)


def collector_from_environment(
    broker: BBMBQueueAdapter,
) -> PersonalInboxCollector | None:
    """None when the personal inbox is not configured, which is the default."""
    jev_key = os.environ.get("TYPESAFE_API_KEY", "").strip()
    cache_path = os.environ.get("CHATTING_PERSONAL_CACHE_PATH", "").strip()
    if not (jev_key and cache_path):
        return None
    # Summaries use the logged-in Codex CLI, so an empty model is fine: Codex
    # falls back to its own configured default.
    return PersonalInboxCollector(
        broker=broker,
        cache=open_cache(cache_path),
        jev_key=jev_key,
        summary_model=os.environ.get("CHATTING_PERSONAL_SUMMARY_MODEL", "").strip(),
    )


__all__ = [
    "PersonalInboxCollector",
    "collector_from_environment",
    "open_cache",
    "process_one",
]
