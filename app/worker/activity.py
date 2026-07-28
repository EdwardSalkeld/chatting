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
from urllib.parse import parse_qs, quote, unquote, urlparse

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
    ) -> None:
        envelope = task_message.envelope
        occurred_at = self._now_fn()
        state = {
            "active": True,
            "task_id": task_message.task_id,
            "envelope_id": envelope.id,
            "source": envelope.source,
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
            occurred_at=occurred_at,
            is_internal=envelope.source == "internal",
            detail={
                "attempt_count": attempt_count,
                "reason_codes": reason_codes,
                "result_status": result_status,
                "latency_ms": latency_ms,
            },
        )

    def record_executor_output(
        self,
        *,
        task_message: TaskQueueMessage,
        stream: str,
        content: str,
    ) -> None:
        envelope = task_message.envelope
        self._append(
            phase=f"executor_{stream}",
            summary=f"executor {stream}",
            task_id=task_message.task_id,
            envelope_id=envelope.id,
            source=envelope.source,
            is_internal=envelope.source == "internal",
            detail={"stream": stream, "content": content},
        )

    def record_executor_failure(
        self,
        *,
        task_message: TaskQueueMessage,
        attempt: int,
        error: str,
    ) -> None:
        envelope = task_message.envelope
        self._append(
            phase="executor_failed_attempt",
            summary=f"executor failed on attempt {attempt}",
            task_id=task_message.task_id,
            envelope_id=envelope.id,
            source=envelope.source,
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
        summary = (
            f"{egress_message.event_kind} egress to {egress_message.message.channel}"
        )
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

    def list_runs_snapshot(
        self, *, include_internal: bool = False
    ) -> dict[str, object]:
        with self._lock:
            current_executor = (
                {"active": False, "phase": "idle"}
                if self._active_executor is None
                else dict(self._active_executor)
            )
        runs = []
        for run in self._store.list_recent_runs(
            limit=self._history_limit,
            include_internal=include_internal,
        ):
            run_summary = self._build_run_summary(
                run_id=run.run_id,
                include_internal=include_internal,
            )
            if run_summary is not None:
                runs.append(run_summary)
        return {
            "current_executor": current_executor,
            "runs": runs,
            "history_limit": self._history_limit,
            "history_truncated": len(runs) >= self._history_limit,
            "include_internal": include_internal,
        }

    def get_run_snapshot(
        self,
        *,
        run_id: str,
        include_internal: bool = False,
    ) -> dict[str, object] | None:
        with self._lock:
            current_executor = (
                {"active": False, "phase": "idle"}
                if self._active_executor is None
                else dict(self._active_executor)
            )
        run_summary = self._build_run_summary(
            run_id=run_id,
            include_internal=include_internal,
        )
        if run_summary is None:
            return None
        return {
            "current_executor": current_executor,
            "run": run_summary,
            "include_internal": include_internal,
        }

    def _build_run_summary(
        self,
        *,
        run_id: str,
        include_internal: bool,
    ) -> dict[str, object] | None:
        run = self._store.get_run(run_id=run_id)
        if run is None:
            return None
        audit_event = self._store.get_audit_event_for_run(run_id=run_id)
        if audit_event is None:
            return None
        detail = audit_event.detail if isinstance(audit_event.detail, dict) else {}
        task_id = detail.get("task_id")
        if not isinstance(task_id, str) or not task_id:
            return None
        activity = self._store.list_worker_activity_for_run(
            run_id=run_id,
            task_id=task_id,
            envelope_id=run.envelope_id,
            include_internal=include_internal,
        )
        user_message = ""
        reply_parts: list[str] = []
        actor = None
        reply_target = None
        for item in activity:
            item_detail = item.get("detail")
            detail_map = item_detail if isinstance(item_detail, dict) else {}
            phase = str(item.get("phase", ""))
            if not user_message and phase == "task_received":
                content = detail_map.get("content")
                if isinstance(content, str):
                    user_message = _extract_current_message(content)
            if phase.startswith("egress_"):
                body = detail_map.get("body")
                channel = detail_map.get("channel")
                if (
                    isinstance(body, str)
                    and body.strip()
                    and channel not in {"internal", "log"}
                ):
                    reply_parts.append(body.strip())
            if actor is None and isinstance(detail_map.get("actor"), str):
                actor = detail_map.get("actor")
            if reply_target is None and isinstance(detail_map.get("reply_target"), str):
                reply_target = detail_map.get("reply_target")
        if not user_message:
            # Sources without the context-wrapped prompt (e.g. email) carry the
            # message directly; fall back to the first message text.
            for item in activity:
                message = _message_text(item)
                if message:
                    user_message = _extract_current_message(message)
                    break
        reply = "\n\n".join(reply_parts)
        last_event = activity[-1] if activity else None
        return {
            "run_id": run.run_id,
            "task_id": task_id,
            "envelope_id": run.envelope_id,
            "source": run.source,
            "workflow": run.workflow,
            "result_status": run.result_status,
            "latency_ms": run.latency_ms,
            "created_at": _isoformat(run.created_at),
            "attempt_count": detail.get("attempt_count"),
            "reason_codes": detail.get("reason_codes", []),
            "preview": user_message,
            "reply": reply,
            "actor": actor,
            "reply_target": reply_target,
            "event_count": len(activity),
            "latest_phase": last_event.get("phase")
            if isinstance(last_event, dict)
            else None,
            "events": activity,
            "audit_detail": detail,
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
    thread = Thread(
        target=server.serve_forever, name="worker-activity-server", daemon=True
    )
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
            if parsed.path in {"/", "/runs"}:
                snapshot = monitor.list_runs_snapshot(include_internal=include_internal)
                body = _render_runs_index_html(
                    snapshot=snapshot,
                    include_internal=include_internal,
                ).encode("utf-8")
                self._write_response(
                    status_code=200,
                    content_type="text/html; charset=utf-8",
                    body=body,
                )
                return
            if parsed.path == "/runs.json":
                payload = monitor.list_runs_snapshot(include_internal=include_internal)
                body = json.dumps(payload, sort_keys=True).encode("utf-8")
                self._write_response(
                    status_code=200,
                    content_type="application/json; charset=utf-8",
                    body=body,
                )
                return
            if parsed.path.startswith("/runs/"):
                encoded_run_id = parsed.path[len("/runs/") :]
                if not encoded_run_id:
                    self._write_response(
                        status_code=404,
                        content_type="text/plain; charset=utf-8",
                        body=b"not found",
                    )
                    return
                json_mode = encoded_run_id.endswith(".json")
                if json_mode:
                    encoded_run_id = encoded_run_id[: -len(".json")]
                run_id = unquote(encoded_run_id)
                snapshot = monitor.get_run_snapshot(
                    run_id=run_id,
                    include_internal=include_internal,
                )
                if snapshot is None:
                    self._write_response(
                        status_code=404,
                        content_type="text/plain; charset=utf-8",
                        body=b"run not found",
                    )
                    return
                if json_mode:
                    body = json.dumps(snapshot, sort_keys=True).encode("utf-8")
                    self._write_response(
                        status_code=200,
                        content_type="application/json; charset=utf-8",
                        body=body,
                    )
                    return
                body = _render_run_detail_html(
                    snapshot=snapshot,
                    include_internal=include_internal,
                ).encode("utf-8")
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


def _render_runs_index_html(
    *,
    snapshot: dict[str, object],
    include_internal: bool,
) -> str:
    current_executor = snapshot["current_executor"]
    runs = snapshot["runs"]
    assert isinstance(current_executor, dict)
    assert isinstance(runs, list)
    showing_note = ""
    if snapshot.get("history_truncated"):
        showing_note = f"<p class='note'>Showing the latest {html.escape(str(snapshot['history_limit']))} runs.</p>"
    runs_markup = _render_runs_index(runs, include_internal=include_internal)
    current_state_markup = _render_current_executor(current_executor)
    toggle_href = _with_query("/runs", include_internal=not include_internal)
    toggle_label = (
        "show internal traffic" if not include_internal else "hide internal traffic"
    )
    json_href = _with_query("/runs.json", include_internal=include_internal)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Chatting Worker Runs</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f1e8;
      --panel: #fffdf8;
      --border: #d6ccbc;
      --ink: #1c1915;
      --muted: #655d53;
      --accent: #1f6f78;
      --accent-soft: #d8eff2;
      --success: #2f6b3b;
      --warning: #8b5a1c;
      --danger: #8c2f39;
      --shadow: 0 20px 60px rgba(39, 30, 18, 0.08);
    }}
    body {{ background:
      radial-gradient(circle at top left, #e5f2ef 0, transparent 30%),
      linear-gradient(180deg, #efe7d9 0%, var(--bg) 100%);
      color: var(--ink); font: 16px/1.45 Georgia, serif; margin: 0; min-height: 100vh; }}
    main {{ max-width: 1040px; margin: 0 auto; padding: 20px 14px 40px; }}
    h1, h2, h3 {{ font-family: "Iowan Old Style", Georgia, serif; }}
    .hero {{ background: var(--panel); border: 1px solid var(--border); border-radius: 24px; box-shadow: var(--shadow); padding: 22px; margin-bottom: 18px; }}
    .hero-top {{ display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; align-items: flex-start; }}
    .eyebrow {{ color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; font-size: 12px; margin-bottom: 8px; }}
    .hero h1 {{ margin: 0 0 8px; font-size: clamp(28px, 5vw, 42px); }}
    .controls {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .button-link {{ border: 1px solid var(--border); background: white; color: var(--accent); border-radius: 999px; padding: 9px 14px; text-decoration: none; }}
    .button-link:hover {{ background: var(--accent-soft); }}
    .note, .muted {{ color: var(--muted); }}
    .detail-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px 16px; margin-top: 16px; }}
    .detail-block dt {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }}
    .detail-block dd {{ margin: 4px 0 0; }}
    .runs {{ list-style: none; padding: 0; margin: 0; display: grid; gap: 14px; }}
    .run-card {{ display: block; text-decoration: none; color: inherit; background: var(--panel); border: 1px solid var(--border); border-radius: 20px; padding: 18px; box-shadow: var(--shadow); }}
    .run-card:hover {{ border-color: var(--accent); transform: translateY(-1px); }}
    .run-kicker {{ display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }}
    .run-title {{ margin: 10px 0 8px; font-size: 23px; line-height: 1.2; }}
    .run-preview {{ margin: 0 0 12px; font-size: 16px; color: #2d2823; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
    .chips {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .chip {{ display: inline-flex; align-items: center; gap: 4px; border-radius: 999px; padding: 4px 9px; background: rgba(31, 111, 120, 0.08); font-size: 13px; color: var(--muted); }}
    .chip.status-success {{ background: rgba(47, 107, 59, 0.12); color: var(--success); }}
    .chip.status-execution_error, .chip.status-dead_letter {{ background: rgba(140, 47, 57, 0.12); color: var(--danger); }}
    .empty-state {{ background: var(--panel); border: 1px dashed var(--border); border-radius: 20px; padding: 28px; color: var(--muted); }}
    @media (max-width: 720px) {{
      main {{ padding: 12px 12px 28px; }}
      .hero {{ padding: 18px; border-radius: 18px; }}
      .detail-grid {{ grid-template-columns: 1fr; }}
      .run-title {{ font-size: 20px; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="hero-top">
        <div>
          <div class="eyebrow">Chatting Worker</div>
          <h1>Recent Runs</h1>
          <p class="muted">Stable URLs, grouped per run, and no live event list jumping around.</p>
          <div id="current-executor">{current_state_markup}</div>
        </div>
        <div class="controls">
          <a class="button-link" href="{json_href}">JSON</a>
          <a class="button-link" href="{toggle_href}">{toggle_label}</a>
          <a class="button-link" href="/activity.json">raw activity</a>
        </div>
      </div>
      {showing_note}
    </section>
    {runs_markup}
  </main>
</body>
</html>"""


def _render_current_executor(current_executor: dict[str, object]) -> str:
    active = bool(current_executor.get("active"))
    entries = [
        ("state", "running" if active else "idle"),
        ("phase", str(current_executor.get("phase", "idle"))),
    ]
    for key in ("task_id", "envelope_id", "attempt", "pid", "started_at"):
        value = current_executor.get(key)
        if value is not None:
            if key.endswith("_at"):
                value = _friendly_timestamp(value)
            entries.append((key, str(value)))
    blocks = []
    for label, value in entries:
        blocks.append(
            "<dl class='detail-block'>"
            f"<dt>{html.escape(label)}</dt>"
            f"<dd>{html.escape(value)}</dd>"
            "</dl>"
        )
    return f"<div class='detail-grid'>{''.join(blocks)}</div>"


def _render_runs_index(runs: list[object], *, include_internal: bool) -> str:
    if not runs:
        return "<div class='empty-state'>No completed runs yet.</div>"
    items = []
    for item in runs:
        assert isinstance(item, dict)
        run_id = str(item.get("run_id", ""))
        href = _with_query(
            f"/runs/{_quote_path_segment(run_id)}",
            include_internal=include_internal,
        )
        preview = _truncate(str(item.get("preview", ""))) or "No message captured."
        title = str(item.get("task_id", "")) or run_id
        # Keep the row scannable: status/source/latency only; the rest is on the
        # detail page.
        chips = [
            ("status", str(item.get("result_status", ""))),
            ("source", str(item.get("source", ""))),
            ("latency", f"{item.get('latency_ms', 0)} ms"),
        ]
        items.append(
            "<li>"
            f"<a class='run-card' href='{html.escape(href)}'>"
            "<div class='run-kicker'>"
            f"<span>{html.escape(_friendly_timestamp(item.get('created_at')))}</span>"
            "</div>"
            f"<h2 class='run-title'>{html.escape(title)}</h2>"
            f"<p class='run-preview'>{html.escape(preview)}</p>"
            f"{_render_chip_row(chips, status_value=str(item.get('result_status', '')))}"
            "</a>"
            "</li>"
        )
    return f"<ul class='runs'>{''.join(items)}</ul>"


def _render_run_detail_html(
    *,
    snapshot: dict[str, object],
    include_internal: bool,
) -> str:
    current_executor = snapshot["current_executor"]
    run = snapshot["run"]
    assert isinstance(current_executor, dict)
    assert isinstance(run, dict)
    current_state_markup = _render_current_executor(current_executor)
    events = run.get("events", [])
    assert isinstance(events, list)
    back_href = _with_query("/runs", include_internal=include_internal)
    json_href = _with_query(
        f"/runs/{_quote_path_segment(str(run.get('run_id', '')))}.json",
        include_internal=include_internal,
    )
    toggle_href = _with_query(
        f"/runs/{_quote_path_segment(str(run.get('run_id', '')))}",
        include_internal=not include_internal,
    )
    toggle_label = (
        "show internal traffic" if not include_internal else "hide internal traffic"
    )
    summary_entries = [
        ("Run", str(run.get("run_id", ""))),
        ("Task", str(run.get("task_id", ""))),
        ("Envelope", str(run.get("envelope_id", ""))),
        ("Status", str(run.get("result_status", ""))),
        ("Source", str(run.get("source", ""))),
        ("Workflow", str(run.get("workflow", ""))),
        ("Started", _friendly_timestamp(run.get("created_at"))),
        ("Latency", f"{run.get('latency_ms', 0)} ms"),
    ]
    timeline_markup = _render_run_timeline(events)
    audit_detail = run.get("audit_detail", {})
    audit_json = html.escape(json.dumps(audit_detail, indent=2, sort_keys=True))
    preview = str(run.get("preview", "")).strip()
    preview_markup = (
        f"<div class='preview-box'>{html.escape(preview)}</div>"
        if preview
        else "<div class='preview-box muted'>No message captured for this run.</div>"
    )
    reply = str(run.get("reply", "")).strip()
    reply_markup = (
        f"<div class='preview-box'>{html.escape(reply)}</div>"
        if reply
        else "<div class='preview-box muted'>No reply sent for this run.</div>"
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(str(run.get("task_id", run.get("run_id", "Run"))))}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4efe7;
      --panel: #fffdf9;
      --border: #d7cbbc;
      --ink: #1f1a16;
      --muted: #685f56;
      --accent: #9a3412;
      --accent-soft: #f5dfd3;
      --shadow: 0 18px 60px rgba(42, 30, 17, 0.08);
    }}
    body {{ margin: 0; background:
      radial-gradient(circle at top right, #f3dcc8 0, transparent 28%),
      linear-gradient(180deg, #ede5d8 0%, var(--bg) 100%);
      color: var(--ink); font: 16px/1.5 Georgia, serif; }}
    main {{ max-width: 980px; margin: 0 auto; padding: 18px 14px 42px; }}
    .hero, .panel, .timeline-item {{ background: var(--panel); border: 1px solid var(--border); box-shadow: var(--shadow); }}
    .hero {{ border-radius: 24px; padding: 22px; margin-bottom: 18px; }}
    .hero-top {{ display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; align-items: flex-start; }}
    .eyebrow {{ color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; font-size: 12px; margin-bottom: 8px; }}
    h1, h2, h3 {{ font-family: "Iowan Old Style", Georgia, serif; margin: 0 0 10px; }}
    h1 {{ font-size: clamp(28px, 5vw, 40px); }}
    .controls {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .button-link {{ border: 1px solid var(--border); border-radius: 999px; padding: 9px 14px; text-decoration: none; color: var(--accent); background: white; }}
    .button-link:hover {{ background: var(--accent-soft); }}
    .detail-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px 16px; }}
    .detail-block dt {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }}
    .detail-block dd {{ margin: 4px 0 0; }}
    .panel {{ border-radius: 20px; padding: 18px; margin-bottom: 18px; }}
    .preview-box {{ font-size: 18px; line-height: 1.55; white-space: pre-wrap; word-break: break-word; }}
    .muted {{ color: var(--muted); }}
    .timeline {{ list-style: none; margin: 0; padding: 0; display: grid; gap: 14px; }}
    .timeline-item {{ border-radius: 18px; padding: 16px; }}
    .timeline-kicker {{ display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }}
    .timeline-item h3 {{ margin-top: 8px; font-size: 21px; }}
    .timeline-message {{ margin-top: 10px; padding: 12px 14px; background: rgba(154, 52, 18, 0.06); border-radius: 14px; white-space: pre-wrap; word-break: break-word; max-height: 320px; overflow: auto; }}
    details summary {{ cursor: pointer; font-family: "Iowan Old Style", Georgia, serif; font-size: 21px; margin-bottom: 10px; }}
    .timeline-collapsible summary {{ cursor: pointer; color: var(--muted); font-size: 13px; font-family: inherit; margin: 8px 0 0; }}
    pre, code {{ white-space: pre-wrap; word-break: break-word; font-size: 12px; }}
    @media (max-width: 720px) {{
      main {{ padding: 12px 12px 28px; }}
      .hero, .panel, .timeline-item {{ border-radius: 18px; }}
      .detail-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="hero-top">
        <div>
          <div class="eyebrow">Run Detail</div>
          <h1>{html.escape(str(run.get("task_id", "")) or str(run.get("run_id", "")))}</h1>
          <div>{current_state_markup}</div>
        </div>
        <div class="controls">
          <a class="button-link" href="{back_href}">all runs</a>
          <a class="button-link" href="{json_href}">JSON</a>
          <a class="button-link" href="{toggle_href}">{toggle_label}</a>
        </div>
      </div>
    </section>
    <section class="panel">
      <h2>Current message</h2>
      {preview_markup}
    </section>
    <section class="panel">
      <h2>Billy's reply</h2>
      {reply_markup}
    </section>
    <section class="panel">
      <h2>Run Summary</h2>
      <div class="detail-grid">{_render_detail_blocks(summary_entries)}</div>
    </section>
    <section class="panel">
      <h2>Events In Order</h2>
      {timeline_markup}
    </section>
    <section class="panel">
      <details>
        <summary>Audit detail (raw JSON)</summary>
        <pre><code>{audit_json}</code></pre>
      </details>
    </section>
  </main>
</body>
</html>"""


def _render_run_timeline(events: list[object]) -> str:
    if not events:
        return "<div class='muted'>No worker activity captured for this run.</div>"
    items = []
    for item in events:
        assert isinstance(item, dict)
        message = _message_text(item)
        detail_json = html.escape(
            json.dumps(
                _detail_without_message(item.get("detail")), indent=2, sort_keys=True
            )
        )
        meta = [
            ("When", _friendly_timestamp(item.get("occurred_at"))),
            ("Phase", str(item.get("phase", ""))),
            ("Task", str(item.get("task_id", ""))),
            ("Envelope", str(item.get("envelope_id", ""))),
            ("Run", str(item.get("run_id", ""))),
            ("Source", str(item.get("source", ""))),
        ]
        if message is None:
            message_markup = ""
        elif len(message) > 2000 or str(item.get("phase", "")) in {
            "executor_stdout",
            "executor_stderr",
        }:
            # Keep bulky output (codex stdout/stderr, context-stuffed prompts)
            # available but collapsed so the page stays scannable.
            message_markup = (
                "<details class='timeline-collapsible'>"
                f"<summary>show message ({len(message):,} chars)</summary>"
                f"<div class='timeline-message'>{html.escape(message)}</div>"
                "</details>"
            )
        else:
            message_markup = f"<div class='timeline-message'>{html.escape(message)}</div>"
        items.append(
            "<li class='timeline-item'>"
            "<div class='timeline-kicker'>"
            f"<span>{html.escape(_event_id(item))}</span>"
            f"<span>{html.escape(_friendly_timestamp(item.get('occurred_at')))}</span>"
            "</div>"
            f"<h3>{html.escape(str(item.get('summary', '')))}</h3>"
            f"<div class='detail-grid'>{_render_detail_blocks(meta)}</div>"
            f"{message_markup}"
            "<h3>Detail JSON</h3>"
            f"<pre><code>{detail_json}</code></pre>"
            "</li>"
        )
    return f"<ol class='timeline'>{''.join(items)}</ol>"


def _render_detail_blocks(entries: list[tuple[str, str]]) -> str:
    blocks = []
    for label, value in entries:
        if not value:
            continue
        blocks.append(
            "<dl class='detail-block'>"
            f"<dt>{html.escape(label)}</dt>"
            f"<dd>{html.escape(value)}</dd>"
            "</dl>"
        )
    return "".join(blocks)


def _render_chip_row(
    entries: list[tuple[str, str]],
    *,
    status_value: str,
) -> str:
    chips = []
    for label, value in entries:
        if not value:
            continue
        classes = ["chip"]
        if label == "status":
            classes.append(f"status-{status_value}")
        chips.append(
            f"<span class='{' '.join(classes)}'><strong>{html.escape(label)}:</strong> {html.escape(value)}</span>"
        )
    return f"<div class='chips'>{''.join(chips)}</div>"


def _list_meta_entries(item: dict[str, object]) -> list[tuple[str, str]]:
    entries = [
        ("task", str(item.get("task_id", ""))),
        ("source", str(item.get("source", ""))),
    ]
    return [(label, value) for label, value in entries if value]


def _event_id(item: dict[str, object]) -> str:
    activity_id = item.get("activity_id")
    if isinstance(activity_id, int):
        return str(activity_id)
    parts = [
        str(item.get("occurred_at", "")),
        str(item.get("phase", "")),
        str(item.get("task_id", "")),
        str(item.get("envelope_id", "")),
        str(item.get("run_id", "")),
        str(item.get("summary", "")),
    ]
    return "|".join(parts)


def _json_script_value(value: object) -> str:
    return json.dumps(value, sort_keys=True).replace("</", "<\\/")


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


_CURRENT_MESSAGE_MARKER = "Current user message:"


def _extract_current_message(content: str | None) -> str:
    # The handler wraps context-carrying prompts as "<context>\n\nCurrent user
    # message:\n<message>"; show just the message. Sources without that wrapper
    # (e.g. email) fall through to the raw content.
    if not content:
        return ""
    if _CURRENT_MESSAGE_MARKER in content:
        return content.split(_CURRENT_MESSAGE_MARKER, 1)[1].strip()
    return content.strip()


def _truncate(text: str, *, limit: int = 200) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "…"


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
    return {
        key: value for key, value in detail.items() if key not in {"body", "content"}
    }


def _with_query(path: str, *, include_internal: bool) -> str:
    if include_internal:
        return f"{path}?include_internal=1"
    return path


def _quote_path_segment(value: str) -> str:
    return quote(value, safe="")
