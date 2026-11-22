"""Small HTTP surface for the worker activity application."""

from __future__ import annotations

import json
import logging
import re
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, unquote, urlparse

from app.timefmt import format_utc, parse_utc
from app.worker.activity import DEFAULT_RUNS_LIMIT, MAX_RUNS_LIMIT

if TYPE_CHECKING:
    from app.worker.activity import WorkerActivityMonitor

LOGGER = logging.getLogger(__name__)
_HTML = Path(__file__).with_name("activity_ui.html")


def build_handler(monitor: WorkerActivityMonitor) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            match = re.fullmatch(r"/api/dead-letters/([1-9][0-9]*)/dismiss", self.path)
            if not match:
                self._json({"error": "not found"}, status=404)
                return
            dead_letter_id = int(match.group(1))
            if dead_letter_id > 2**63 - 1:
                self._json({"error": "not found"}, status=404)
                return
            if monitor.dismiss_dead_letter(dead_letter_id):
                self._json({"status": "dismissed"})
            else:
                self._json(
                    {"error": "dead letter not found or not pending"}, status=404
                )

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/metrics":
                self._send(
                    200,
                    "text/plain; version=0.0.4; charset=utf-8",
                    monitor.prometheus_metrics(),
                )
                return
            if path.startswith("/api/conversations/") and path.endswith("/messages"):
                conversation_id = unquote(
                    path[len("/api/conversations/") : -len("/messages")]
                )
                try:
                    query = parse_qs(parsed.query)
                    self._json(
                        monitor.conversation_messages(
                            conversation_id,
                            before=_param(query, "before"),
                            limit=min(_limit(query), 100),
                        )
                    )
                except ValueError:
                    self._json(
                        {"error": "invalid conversation or history query"}, status=400
                    )
                return
            if path == "/api/items":
                self._json(monitor.list_items_snapshot())
                return
            if path.startswith("/api/items/") and path.endswith("/runs"):
                item_id = unquote(path[len("/api/items/") : -len("/runs")])
                snapshot = monitor.get_item_snapshot(item_id)
                self._json(snapshot, status=200 if snapshot is not None else 404)
                return
            if path == "/api/runs":
                query = parse_qs(parsed.query)
                try:
                    limit = _limit(query)
                    since = _param(query, "since")
                    if since is not None:
                        since = format_utc(parse_utc(since))
                except ValueError:
                    self._json({"error": "invalid runs query"}, status=400)
                    return
                self._json(
                    monitor.list_runs(
                        limit=limit,
                        status=_param(query, "status"),
                        source=_param(query, "source"),
                        work_item_id=_param(query, "work_item_id"),
                        since=since,
                    )
                )
                return
            if path == "/api/dead-letters":
                try:
                    limit = _limit(parse_qs(parsed.query))
                except ValueError:
                    self._json({"error": "invalid limit"}, status=400)
                    return
                self._json(monitor.list_dead_letters(limit=limit))
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


def _param(query: dict[str, list[str]], name: str) -> str | None:
    value = query.get(name, [""])[-1].strip()
    return value or None


def _limit(query: dict[str, list[str]]) -> int:
    raw = _param(query, "limit")
    limit = DEFAULT_RUNS_LIMIT if raw is None else int(raw)
    if not 0 < limit <= MAX_RUNS_LIMIT:
        raise ValueError("limit out of range")
    return limit
