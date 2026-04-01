"""Worker-local runtime activity tracking and read-only HTTP UI."""

from __future__ import annotations

import html
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Lock, Thread
from typing import Callable
from urllib.parse import parse_qs, urlparse

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.state import SQLiteStateStore

LOGGER = logging.getLogger(__name__)
DEFAULT_ACTIVITY_HOST = "0.0.0.0"
DEFAULT_ACTIVITY_PORT = 9465
DEFAULT_ACTIVITY_HISTORY_LIMIT = 100


@dataclass(frozen=True)
class WorkerActivityServer:
    server: HTTPServer
    thread: Thread

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=1.0)


class WorkerActivityMonitor:
    """Persist recent worker-visible activity and expose live executor state."""

    def __init__(
        self,
        *,
        store: SQLiteStateStore,
        history_limit: int = DEFAULT_ACTIVITY_HISTORY_LIMIT,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        if history_limit <= 0:
            raise ValueError("history_limit must be positive")
        self._store = store
        self._history_limit = history_limit
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self._lock = Lock()
        self._active_executor: dict[str, object] | None = None

    @property
    def history_limit(self) -> int:
        return self._history_limit

    def record_task_received(self, *, task_message: TaskQueueMessage) -> None:
        envelope = task_message.envelope
        self._append(
            phase="task_received",
            summary=f"{envelope.source} task received",
            task_id=task_message.task_id,
            envelope_id=envelope.id,
            source=envelope.source,
            workflow=None,
            occurred_at=envelope.received_at,
            is_internal=envelope.source == "internal",
            detail={
                "actor": envelope.actor,
                "content": envelope.content,
                "reply_channel": envelope.reply_channel.type,
                "reply_target": envelope.reply_channel.target,
            },
        )

    def record_executor_started(
        self,
        *,
        task_message: TaskQueueMessage,
        attempt: int,
        workflow: str,
    ) -> None:
        envelope = task_message.envelope
        occurred_at = self._now_fn()
        state = {
            "active": True,
            "task_id": task_message.task_id,
            "envelope_id": envelope.id,
            "source": envelope.source,
            "workflow": workflow,
            "attempt": attempt,
            "started_at": _isoformat(occurred_at),
            "pid": None,
            "phase": "executor_running",
        }
        with self._lock:
            self._active_executor = state
        self._append(
            phase="executor_started",
            summary=f"executor started (attempt {attempt})",
            task_id=task_message.task_id,
            envelope_id=envelope.id,
            source=envelope.source,
            workflow=workflow,
            occurred_at=occurred_at,
            is_internal=envelope.source == "internal",
            detail={"attempt": attempt},
        )

    def record_executor_pid(self, *, pid: int | None) -> None:
        if pid is None:
            return
        with self._lock:
            if self._active_executor is None:
                return
            self._active_executor["pid"] = pid

    def record_executor_finished(
        self,
        *,
        task_message: TaskQueueMessage,
        run_id: str,
        workflow: str,
        result_status: str,
        attempt_count: int,
        reason_codes: list[str],
        latency_ms: int,
    ) -> None:
        envelope = task_message.envelope
        occurred_at = self._now_fn()
        with self._lock:
            self._active_executor = None
        self._append(
            phase="task_finished",
            summary=f"task finished with {result_status}",
            task_id=task_message.task_id,
            envelope_id=envelope.id,
            run_id=run_id,
            source=envelope.source,
            workflow=workflow,
            occurred_at=occurred_at,
            is_internal=envelope.source == "internal",
            detail={
                "attempt_count": attempt_count,
                "reason_codes": reason_codes,
                "result_status": result_status,
                "latency_ms": latency_ms,
            },
        )

    def record_executor_failure(
        self,
        *,
        task_message: TaskQueueMessage,
        attempt: int,
        workflow: str,
        error: str,
    ) -> None:
        envelope = task_message.envelope
        self._append(
            phase="executor_failed_attempt",
            summary=f"executor failed on attempt {attempt}",
            task_id=task_message.task_id,
            envelope_id=envelope.id,
            source=envelope.source,
            workflow=workflow,
            is_internal=envelope.source == "internal",
            detail={"attempt": attempt, "error": error},
        )

    def record_egress(
        self,
        *,
        egress_message: EgressQueueMessage,
        publish_source: str,
    ) -> None:
        phase = f"egress_{egress_message.event_kind}"
        summary = f"{egress_message.event_kind} egress to {egress_message.message.channel}"
        self._append(
            phase=phase,
            summary=summary,
            task_id=egress_message.task_id,
            envelope_id=egress_message.envelope_id,
            occurred_at=egress_message.emitted_at,
            detail={
                "channel": egress_message.message.channel,
                "target": egress_message.message.target,
                "body": egress_message.message.body,
                "event_id": egress_message.event_id,
                "event_kind": egress_message.event_kind,
                "event_count": egress_message.event_count,
                "event_index": egress_message.event_index,
                "message_type": egress_message.message_type,
                "publish_source": publish_source,
                "sequence": egress_message.sequence,
            },
            is_internal=egress_message.message.channel in {"internal", "log"},
        )

    def snapshot(self, *, include_internal: bool = False) -> dict[str, object]:
        with self._lock:
            current_executor = (
                {"active": False, "phase": "idle"}
                if self._active_executor is None
                else dict(self._active_executor)
            )
        activity = self._store.list_recent_worker_activity(
            limit=self._history_limit,
            include_internal=include_internal,
        )
        return {
            "current_executor": current_executor,
            "recent_activity": activity,
            "history_limit": self._history_limit,
            "history_truncated": len(activity) >= self._history_limit,
            "include_internal": include_internal,
        }

    def _append(
        self,
        *,
        phase: str,
        summary: str,
        detail: dict[str, object],
        task_id: str | None = None,
        envelope_id: str | None = None,
        run_id: str | None = None,
        source: str | None = None,
        workflow: str | None = None,
        occurred_at: datetime | None = None,
        is_internal: bool = False,
    ) -> None:
        self._store.append_worker_activity(
            occurred_at=occurred_at or self._now_fn(),
            task_id=task_id,
            envelope_id=envelope_id,
            run_id=run_id,
            source=source,
            workflow=workflow,
            phase=phase,
            summary=summary,
            detail=detail,
            is_internal=is_internal,
        )


def start_worker_activity_server(
    *,
    host: str,
    port: int,
    monitor: WorkerActivityMonitor,
) -> WorkerActivityServer:
    server = HTTPServer((host, port), _build_handler(monitor))
    thread = Thread(target=server.serve_forever, name="worker-activity-server", daemon=True)
    thread.start()
    LOGGER.info("worker_activity_server_started host=%s port=%s", host, port)
    return WorkerActivityServer(server=server, thread=thread)


def _build_handler(monitor: WorkerActivityMonitor) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            include_internal = _bool_query_flag(parsed.query, "include_internal")
            if parsed.path == "/activity.json":
                payload = monitor.snapshot(include_internal=include_internal)
                body = json.dumps(payload, sort_keys=True).encode("utf-8")
                self._write_response(
                    status_code=200,
                    content_type="application/json; charset=utf-8",
                    body=body,
                )
                return
            if parsed.path == "/":
                snapshot = monitor.snapshot(include_internal=include_internal)
                body = _render_html(snapshot=snapshot).encode("utf-8")
                self._write_response(
                    status_code=200,
                    content_type="text/html; charset=utf-8",
                    body=body,
                )
                return
            self._write_response(
                status_code=404,
                content_type="text/plain; charset=utf-8",
                body=b"not found",
            )

        def log_message(self, format: str, *args: object) -> None:
            LOGGER.info("worker_activity_http " + format, *args)

        def _write_response(
            self,
            *,
            status_code: int,
            content_type: str,
            body: bytes,
        ) -> None:
            self.send_response(status_code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def _render_html(*, snapshot: dict[str, object]) -> str:
    current_executor = snapshot["current_executor"]
    activity = snapshot["recent_activity"]
    assert isinstance(current_executor, dict)
    assert isinstance(activity, list)
    active = bool(current_executor.get("active"))
    state_title = "running" if active else "idle"
    state_lines = [
        f"<strong>state:</strong> {html.escape(state_title)}",
        f"<strong>phase:</strong> {html.escape(str(current_executor.get('phase', 'idle')))}",
    ]
    for key in ("task_id", "envelope_id", "workflow", "attempt", "pid", "started_at"):
        value = current_executor.get(key)
        if value is not None:
            if key.endswith("_at"):
                value = _friendly_timestamp(value)
            state_lines.append(f"<strong>{html.escape(key)}:</strong> {html.escape(str(value))}")

    rows = []
    for item in activity:
        assert isinstance(item, dict)
        detail = item.get("detail")
        detail_json = html.escape(json.dumps(_detail_without_message(detail), sort_keys=True))
        message_text = _message_text(item)
        message_html = (
            f"<div class='message'>{html.escape(message_text)}</div>"
            if message_text is not None
            else "<span class='muted'>-</span>"
        )
        occurred_at = str(item.get("occurred_at", ""))
        friendly_occurred_at = _friendly_timestamp(occurred_at)
        rows.append(
            "<tr>"
            f"<td title='{html.escape(occurred_at)}'>{html.escape(friendly_occurred_at)}</td>"
            f"<td>{html.escape(str(item.get('phase', '')))}</td>"
            f"<td>{html.escape(str(item.get('task_id', '')))}</td>"
            f"<td>{html.escape(str(item.get('summary', '')))}</td>"
            f"<td>{message_html}</td>"
            f"<td><code>{detail_json}</code></td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='6'>No recent worker activity.</td></tr>")

    showing_note = ""
    if snapshot.get("history_truncated"):
        showing_note = (
            f"<p class='note'>Showing the latest {html.escape(str(snapshot['history_limit']))} events. "
            "Older local history has been pruned from this view.</p>"
        )

    include_internal = bool(snapshot.get("include_internal"))
    toggle_href = "/?include_internal=1" if not include_internal else "/"
    toggle_label = "show internal traffic" if not include_internal else "hide internal traffic"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Chatting Worker Activity</title>
  <meta http-equiv="refresh" content="5">
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4efe6;
      --panel: #fffaf2;
      --border: #d5c7b3;
      --ink: #1f1d1a;
      --muted: #6b655d;
      --accent: #b85c38;
    }}
    body {{ background: linear-gradient(180deg, #efe4d2 0%, var(--bg) 100%); color: var(--ink); font: 16px/1.4 Georgia, serif; margin: 0; }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
    h1, h2 {{ font-family: "Iowan Old Style", Georgia, serif; margin: 0 0 12px; }}
    .panel {{ background: var(--panel); border: 1px solid var(--border); border-radius: 14px; box-shadow: 0 8px 30px rgba(56, 37, 18, 0.08); padding: 18px; margin-bottom: 18px; }}
    .note, .muted {{ color: var(--muted); }}
    a {{ color: var(--accent); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; vertical-align: top; padding: 10px 8px; border-top: 1px solid var(--border); }}
    th {{ border-top: none; }}
    code {{ white-space: pre-wrap; word-break: break-word; font-size: 12px; }}
    .message {{ white-space: pre-wrap; word-break: break-word; max-width: 34ch; }}
  </style>
</head>
<body>
  <main>
    <div class="panel">
      <h1>Worker Now</h1>
      <p>{"<br>".join(state_lines)}</p>
      <p class="muted"><a href="/activity.json{'?include_internal=1' if include_internal else ''}">JSON</a> · <a href="{toggle_href}">{toggle_label}</a></p>
    </div>
    <div class="panel">
      <h2>Recent Activity</h2>
      {showing_note}
      <table>
        <thead>
          <tr><th>When</th><th>Phase</th><th>Task</th><th>Summary</th><th>Message</th><th>Detail</th></tr>
        </thead>
        <tbody>
          {"".join(rows)}
        </tbody>
      </table>
    </div>
  </main>
</body>
</html>"""


def _bool_query_flag(query: str, name: str) -> bool:
    values = parse_qs(query).get(name, [])
    if not values:
        return False
    return values[-1].strip().lower() not in {"0", "false", "no", ""}


def _isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _friendly_timestamp(value: object) -> str:
    if not isinstance(value, str) or not value:
        return str(value)
    try:
        parsed = _parse_timestamp(value)
    except ValueError:
        return value
    return parsed.strftime("%a %d %b %Y %H:%M:%S UTC")


def _parse_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _message_text(item: dict[str, object]) -> str | None:
    detail = item.get("detail")
    if not isinstance(detail, dict):
        return None
    for key in ("content", "body"):
        value = detail.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _detail_without_message(detail: object) -> object:
    if not isinstance(detail, dict):
        return detail
    return {key: value for key, value in detail.items() if key not in {"body", "content"}}
