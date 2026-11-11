"""Personal inbox classification, run as a thread of the ordinary worker.

It shares the worker's process and its TypeSafe key, but not its executor: the
only thing this code can do with an email is ask two fixed HTTPS endpoints about
it and publish the answer. It never touches the mailbox, the task queue the
executor reads. Owner-authored rules are read from a configured Git repository.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from uuid import uuid4
from dataclasses import dataclass, field
from pathlib import Path
import sqlite3
from collections.abc import Callable
from threading import Event, Thread
from typing import Any

from app.broker.bbmb_client import BBMBQueueAdapter
from app import personal_inbox_diagnostics as diagnostics
from app.worker.owner_rules import load_rules
from app.codex_usage import usage_sink
from app.state import SQLiteStateStore
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
    task: object, *, jev_key: str, summary_model: str, rules_repo: str = ""
) -> tuple[str, Callable[[], dict[str, Any]]] | None:
    """The cache key and the work for one task, or None if it is not ours."""
    if not isinstance(task, dict):
        return None
    kind = task.get("kind")
    if kind == "personal_inbox.classify.v1":
        validate_task(task)

        def classify_with_rules():
            rules = diagnostics.at_stage("rules_load", load_rules, rules_repo)
            return diagnostics.at_stage(
                "classify",
                classify,
                task,
                jev_key=jev_key,
                summary_model=summary_model,
                rules_context=rules.instructions,
                rules_revision=rules.revision,
                rule_ids=rules.ids,
            )

        return task["message_id"], classify_with_rules
    if kind == "personal_inbox.digest.v1":
        validate_digest_task(task)
        return task["digest_id"], lambda: diagnostics.at_stage(
            "digest", digest_lines, task, summary_model=summary_model
        )
    return None


def process_one(
    broker: BBMBQueueAdapter,
    cache: sqlite3.Connection,
    *,
    jev_key: str,
    summary_model: str,
    rules_repo: str = "",
    store: SQLiteStateStore | None = None,
) -> bool:
    context = diagnostics.ProcessingContext()
    token = diagnostics.current.set(context)
    started = time.monotonic()
    try:
        processed = _process_one(
            broker,
            cache,
            jev_key=jev_key,
            summary_model=summary_model,
            rules_repo=rules_repo,
            store=store,
        )
    except Exception as error:
        reason, location = diagnostics.failure_details(error)
        LOGGER.error(
            "personal_inbox_failed kind=%s task_ref=%s stage=%s type=%s reason=%s source=%s cached=%s elapsed_ms=%d retry=lease",
            context.kind,
            context.task_ref,
            context.stage,
            type(error).__name__,
            reason,
            location,
            context.cached,
            (time.monotonic() - started) * 1000,
        )
        raise
    else:
        if processed:
            LOGGER.info(
                "personal_inbox_completed kind=%s task_ref=%s stage=%s cached=%s elapsed_ms=%d",
                context.kind,
                context.task_ref,
                context.stage,
                context.cached,
                (time.monotonic() - started) * 1000,
            )
        return processed
    finally:
        diagnostics.current.reset(token)


def _process_one(
    broker: BBMBQueueAdapter,
    cache: sqlite3.Connection,
    *,
    jev_key: str,
    summary_model: str,
    rules_repo: str = "",
    store: SQLiteStateStore | None = None,
) -> bool:
    picked = broker.pickup_json(TASKS_QUEUE, timeout_seconds=120, wait_seconds=1)
    if picked is None:
        return False
    diagnostics.identify(picked.payload)
    diagnostics.mark_stage("validate_task")
    try:
        prepared = prepare(
            picked.payload,
            jev_key=jev_key,
            summary_model=summary_model,
            rules_repo=rules_repo,
        )
    except (ValueError, TypeError, KeyError) as error:
        reason, location = diagnostics.failure_details(error)
        context = diagnostics.current.get()
        LOGGER.warning(
            "personal_inbox_invalid task_ref=%s reason=%s source=%s",
            context.task_ref if context else "unknown",
            reason,
            location,
        )
        prepared = None
    if prepared is None:
        diagnostics.mark_stage("invalid_task_publish")
        broker.publish_json(DEAD_QUEUE, {"guid": picked.guid, "reason": "invalid_task"})
        diagnostics.mark_stage("invalid_task_ack")
        broker.ack(TASKS_QUEUE, picked.guid)
        return True
    task_id, work = prepared
    fingerprint = hashlib.sha256(
        json.dumps(picked.payload, sort_keys=True).encode()
    ).hexdigest()
    diagnostics.mark_stage("cache_read")
    row = cache.execute(
        "SELECT fingerprint, result FROM classifications WHERE id=?", (task_id,)
    ).fetchone()
    if row is not None and row[0] == fingerprint:
        if context := diagnostics.current.get():
            context.cached = True
        result = json.loads(row[1])
    else:
        # A cached answer to a different question is stale, not suspicious:
        # the same message reads differently when the parser improves. The
        # cache saves a model call, it does not decide identity — the handler
        # does that, by keeping only the first classification per message. So
        # answer the question actually asked and replace the stale row.
        def record_usage(detail: dict) -> None:
            if store is not None:
                store.append_worker_activity(
                    occurred_at=datetime.now(timezone.utc),
                    phase="personal_model_usage",
                    summary="Personal inbox Codex call",
                    detail={
                        **detail,
                        "call_id": str(uuid4()),
                        "kind": picked.payload["kind"],
                    },
                    task_id="personal-inbox:" + task_id,
                    source="personal_email",
                )

        token = usage_sink.set(record_usage)
        try:
            result = work()
        finally:
            usage_sink.reset(token)
        diagnostics.mark_stage("cache_write")
        with cache:
            cache.execute(
                "INSERT INTO classifications VALUES (?,?,?) ON CONFLICT(id) "
                "DO UPDATE SET fingerprint=excluded.fingerprint, result=excluded.result",
                (task_id, fingerprint, json.dumps(result)),
            )
    # A publish/ACK failure replays the saved result rather than calling models
    # again. The handler accepts exactly one classification per mail identity.
    diagnostics.mark_stage("result_publish")
    broker.publish_json(RESULTS_QUEUE, result)
    diagnostics.mark_stage("result_ack")
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
    rules_repo: str = ""
    store: SQLiteStateStore | None = None
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
                    rules_repo=self.rules_repo,
                    store=self.store,
                )
            except Exception:  # noqa: BLE001
                # Never log request bodies, mail, credentials or URLs with
                # tokens. An unacked task is leased for a later retry.
                # process_one already emitted safe stage/correlation diagnostics.
                self._stop.wait(5)


def collector_from_environment(
    broker: BBMBQueueAdapter,
    store: SQLiteStateStore | None = None,
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
        rules_repo=os.environ.get("CHATTING_PERSONAL_RULES_REPO", "").strip(),
        store=store,
    )


__all__ = [
    "PersonalInboxCollector",
    "collector_from_environment",
    "open_cache",
    "process_one",
]
