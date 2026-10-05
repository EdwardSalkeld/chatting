"""Small read-only HTTP surface for the worker activity application."""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, unquote, urlparse

if TYPE_CHECKING:
    from app.worker.activity import WorkerActivityMonitor

LOGGER = logging.getLogger(__name__)
_HTML = Path(__file__).with_name("activity_ui.html")


def build_handler(monitor: WorkerActivityMonitor) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/api/items":
                self._json(monitor.list_items_snapshot())
                return
            if path.startswith("/api/items/") and path.endswith("/runs"):
                item_id = unquote(path[len("/api/items/") : -len("/runs")])
                snapshot = monitor.get_item_snapshot(item_id)
                self._json(snapshot, status=200 if snapshot is not None else 404)
                return
            if path.startswith("/api/runs/"):
                run_id = unquote(path[len("/api/runs/") :])
                snapshot = monitor.get_run_header(run_id)
                self._json(snapshot, status=200 if snapshot is not None else 404)
                return
            if path.startswith("/api/tasks/") and path.endswith("/events"):
                task_id = unquote(path[len("/api/tasks/") : -len("/events")])
                try:
                    after_id = int(parse_qs(parsed.query).get("after", ["0"])[-1])
                except ValueError:
                    self._json({"error": "invalid after"}, status=400)
                    return
                if not task_id or after_id < 0:
                    self._json({"error": "invalid activity query"}, status=400)
                    return
                self._json(monitor.live_events(task_id=task_id, after_id=after_id))
                return
            if path == "/activity.json":
                include_internal = parse_qs(parsed.query).get("include_internal") == [
                    "1"
                ]
                self._json(monitor.snapshot(include_internal=include_internal))
                return
            if path == "/runs.json":
                include_internal = parse_qs(parsed.query).get("include_internal") == [
                    "1"
                ]
                self._json(
                    monitor.list_runs_snapshot(include_internal=include_internal)
                )
                return
            if path.startswith("/runs/") and path.endswith(".json"):
                run_id = unquote(path[len("/runs/") : -len(".json")])
                snapshot = monitor.get_run_snapshot(run_id=run_id)
                self._json(snapshot, status=200 if snapshot is not None else 404)
                return
            if path in {"/", "/items", "/runs"} or (
                path.startswith(("/items/", "/runs/", "/tasks/"))
                and path.count("/") == 2
            ):
                self._send(200, "text/html; charset=utf-8", _HTML.read_bytes())
                return
            self._send(404, "text/plain; charset=utf-8", b"not found")

        def _json(self, payload: object, *, status: int = 200) -> None:
            self._send(
                status,
                "application/json; charset=utf-8",
                json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            )

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            LOGGER.info("worker_activity_http " + format, *args)

    return Handler
