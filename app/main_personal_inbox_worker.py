"""Dedicated personal inbox worker; never imports or launches the executor."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import threading

from app.broker.bbmb_client import BBMBQueueAdapter
from app.personal_inbox import (
    DEAD_QUEUE,
    RESULTS_QUEUE,
    TASKS_QUEUE,
    classify,
    validate_task,
)

LOGGER = logging.getLogger(__name__)


def process_one(
    broker: BBMBQueueAdapter,
    cache: sqlite3.Connection,
    *,
    jev_key: str,
    summary_key: str,
    summary_model: str,
) -> bool:
    picked = broker.pickup_json(TASKS_QUEUE, timeout_seconds=120, wait_seconds=1)
    if picked is None:
        return False
    try:
        validate_task(picked.payload)
    except (ValueError, TypeError, KeyError):
        broker.publish_json(DEAD_QUEUE, {"guid": picked.guid, "reason": "invalid_task"})
        broker.ack(TASKS_QUEUE, picked.guid)
        return True
    task_id = picked.payload["message_id"]
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
        result = classify(
            picked.payload,
            jev_key=jev_key,
            summary_key=summary_key,
            summary_model=summary_model,
        )
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bbmb-address", default="127.0.0.1:9876")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--summary-model", required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    jev_key = os.environ.get("TYPESAFE_API_KEY", "").strip()
    summary_key = os.environ.get("CHATTING_PERSONAL_SUMMARY_API_KEY", "").strip()
    if not jev_key or not summary_key:
        parser.error(
            "TYPESAFE_API_KEY and CHATTING_PERSONAL_SUMMARY_API_KEY are required"
        )
    os.umask(0o077)
    Path(args.db_path).parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    cache = sqlite3.connect(args.db_path, timeout=5)
    cache.execute("PRAGMA journal_mode=WAL")
    cache.execute(
        "CREATE TABLE IF NOT EXISTS classifications "
        "(id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL)"
    )
    broker = BBMBQueueAdapter(address=args.bbmb_address)
    for queue in (TASKS_QUEUE, RESULTS_QUEUE, DEAD_QUEUE):
        broker.ensure_queue(queue)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        while not stop.is_set():
            try:
                process_one(
                    broker,
                    cache,
                    jev_key=jev_key,
                    summary_key=summary_key,
                    summary_model=args.summary_model,
                )
            except Exception as error:
                # Never print request bodies, mail, credentials or HTTP URLs
                # with tokens. An unacked task is leased for a later retry.
                LOGGER.error(
                    "personal_inbox_worker_failed type=%s", type(error).__name__
                )
                if args.once:
                    raise SystemExit(1) from None
                stop.wait(5)
            if args.once:
                break
    finally:
        cache.close()


if __name__ == "__main__":
    main()
