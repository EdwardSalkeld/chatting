"""Poll Codex account limits without starting a model turn."""

from __future__ import annotations

import logging
from threading import Event, Thread
import time

from app.state import SQLiteStateStore
from app.worker.executor.app_server_session import AppServer

LOGGER = logging.getLogger(__name__)


def normalize(response: dict) -> dict:
    """Keep only usage metadata; never persist account or credit identifiers."""
    limits = response.get("rateLimits")
    if not isinstance(limits, dict):
        raise ValueError("missing rate limits")
    allowed = response.get("ordinaryUsageAllowed")
    result: dict = {
        "ordinary_usage_allowed": allowed if type(allowed) is bool else None,
        "windows": [],
    }
    for key in ("primary", "secondary"):
        window = limits.get(key)
        if not isinstance(window, dict):
            continue
        used = window.get("usedPercent")
        duration = window.get("windowDurationMins")
        reset = window.get("resetsAt")
        if (
            not isinstance(used, (int, float))
            or isinstance(used, bool)
            or not 0 <= used <= 100
            or type(duration) is not int
            or duration <= 0
        ):
            continue
        result["windows"].append(
            {
                "duration_minutes": duration,
                "used_percent": used,
                "resets_at": reset if type(reset) is int else None,
            }
        )
    return result


class QuotaPoller:
    def __init__(self, store: SQLiteStateStore, command: str, interval: float = 60):
        self.store, self.command, self.interval = store, command, interval
        self.stop_event = Event()
        self.thread = Thread(target=self._run, daemon=True, name="codex-quota")

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=15)

    def poll(self) -> None:
        server = AppServer([self.command, "app-server"])
        try:
            deadline = time.monotonic() + 10

            def sink(*_args: object) -> None:
                pass

            server.rpc(
                "initialize",
                {"clientInfo": {"name": "chatting-quota-monitor", "version": "1"}},
                deadline,
                sink,
            )
            server.send({"method": "initialized"})
            response = server.rpc("account/rateLimits/read", {}, deadline, sink)
            self.store.record_codex_quota(normalize(response), time.time())
        finally:
            server.close()

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.poll()
            except (
                Exception
            ) as error:  # Do not interrupt the worker if monitoring fails.
                LOGGER.warning("codex_quota_poll_failed type=%s", type(error).__name__)
            self.stop_event.wait(self.interval)
