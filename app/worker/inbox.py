"""Durable task collection while an executor owns the worker's main thread."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from threading import Event, Thread

from app.broker import BBMBQueueAdapter, TASK_QUEUE_NAME, TaskQueueMessage
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor

LOGGER = logging.getLogger(__name__)


@dataclass
class InboxCollector:
    broker: BBMBQueueAdapter
    store: SQLiteStateStore
    activity_monitor: WorkerActivityMonitor
    pickup_timeout_seconds: int
    _stop: Event = field(init=False, default_factory=Event)
    _thread: Thread | None = field(init=False, default=None)

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("inbox collector already started")
        self._thread = Thread(
            target=self._run,
            name="worker-inbox-collector",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def collect_once(self) -> bool:
        picked = self.broker.pickup_json(
            TASK_QUEUE_NAME,
            timeout_seconds=self.pickup_timeout_seconds,
            wait_seconds=1,
        )
        if picked is None:
            return False
        task_message = TaskQueueMessage.from_dict(picked.payload)
        inserted = self.store.stage_inbox_task(task_message, broker_guid=picked.guid)
        # SQLite is the durable handoff point. A redelivered task is already
        # present, so it is equally safe to acknowledge without inserting again.
        self.broker.ack(TASK_QUEUE_NAME, picked.guid)
        if inserted:
            self.activity_monitor.record_task_received(task_message=task_message)
        return True

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.collect_once()
            except Exception:  # noqa: BLE001
                LOGGER.exception("worker_inbox_collection_failed")
                self._stop.wait(1.0)


__all__ = ["InboxCollector"]
