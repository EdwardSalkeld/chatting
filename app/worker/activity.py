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

    def record_executor_output(
        self,
        *,
        task_message: TaskQueueMessage,
        workflow: str,
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
            workflow=workflow,
            is_internal=envelope.source == "internal",
            detail={"stream": stream, "content": content},
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
        conversation_feed = _build_conversation_feed(
            activity,
            limit=self._history_limit,
        )
        return {
            "current_executor": current_executor,
            "recent_activity": activity,
            "recent_feed": conversation_feed,
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
            auto_refresh = not _bool_query_flag(parsed.query, "refresh_off")
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
                body = _render_html(
                    snapshot=snapshot,
                    include_internal=include_internal,
                    auto_refresh=auto_refresh,
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


def _render_html(
    *,
    snapshot: dict[str, object],
    include_internal: bool,
    auto_refresh: bool,
) -> str:
    current_executor = snapshot["current_executor"]
    activity = snapshot["recent_activity"]
    feed = snapshot.get("recent_feed", activity)
    assert isinstance(current_executor, dict)
    assert isinstance(activity, list)
    assert isinstance(feed, list)
    activity_json = _json_script_value(snapshot)
    initial_list_markup = _render_activity_list(feed)
    initial_detail_markup = _render_detail_panel(feed[0] if feed else None)
    current_state_markup = _render_current_executor(current_executor)

    showing_note = ""
    if snapshot.get("history_truncated"):
        showing_note = (
            f"<p class='note'>Showing the latest {html.escape(str(snapshot['history_limit']))} events. "
            "Older local history has been pruned from this view.</p>"
        )

    query_parts: list[str] = []
    if include_internal:
        query_parts.append("include_internal=1")
    if not auto_refresh:
        query_parts.append("refresh_off=1")

    toggle_query_parts = list(query_parts)
    if include_internal:
        toggle_query_parts.remove("include_internal=1")
    else:
        toggle_query_parts.append("include_internal=1")

    refresh_query_parts = list(query_parts)
    if auto_refresh:
        refresh_query_parts.append("refresh_off=1")
    else:
        refresh_query_parts.remove("refresh_off=1")

    toggle_href = "/" if not toggle_query_parts else f"/?{'&'.join(toggle_query_parts)}"
    toggle_label = (
        "show internal traffic" if not include_internal else "hide internal traffic"
    )
    refresh_href = (
        "/" if not refresh_query_parts else f"/?{'&'.join(refresh_query_parts)}"
    )
    refresh_label = "pause refresh" if auto_refresh else "resume refresh"
    refresh_note = "Auto-refresh every 5s." if auto_refresh else "Auto-refresh paused."

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Chatting Worker Activity</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4efe6;
      --panel: #fffaf2;
      --border: #d5c7b3;
      --ink: #1f1d1a;
      --muted: #6b655d;
      --accent: #b85c38;
      --accent-soft: #f4d8c9;
      --shadow: 0 18px 45px rgba(56, 37, 18, 0.08);
    }}
    body {{ background: linear-gradient(180deg, #efe4d2 0%, var(--bg) 100%); color: var(--ink); font: 16px/1.4 Georgia, serif; margin: 0; }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
    h1, h2 {{ font-family: "Iowan Old Style", Georgia, serif; margin: 0 0 12px; }}
    h3 {{ margin: 0 0 8px; font-size: 15px; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); }}
    .panel {{ background: var(--panel); border: 1px solid var(--border); border-radius: 14px; box-shadow: var(--shadow); padding: 18px; margin-bottom: 18px; }}
    .topbar {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; flex-wrap: wrap; }}
    .controls {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
    .button-link {{ appearance: none; border: 1px solid var(--border); background: transparent; color: var(--accent); border-radius: 999px; padding: 8px 12px; font: inherit; text-decoration: none; cursor: pointer; }}
    .button-link:hover {{ background: var(--accent-soft); }}
    .note, .muted {{ color: var(--muted); }}
    a {{ color: var(--accent); }}
    code, pre {{ white-space: pre-wrap; word-break: break-word; font-size: 12px; }}
    .layout {{ display: grid; grid-template-columns: minmax(320px, 420px) minmax(0, 1fr); gap: 18px; align-items: stretch; }}
    .list-panel {{ padding: 0; overflow: hidden; display: flex; flex-direction: column; min-height: 72vh; max-height: calc(100vh - 180px); }}
    .list-header {{ padding: 18px 18px 0; }}
    .activity-list {{ list-style: none; margin: 0; padding: 12px; display: grid; gap: 10px; flex: 1 1 auto; min-height: 0; overflow: auto; }}
    .activity-item {{ border: 1px solid var(--border); border-radius: 14px; background: rgba(255,255,255,0.65); padding: 0; }}
    .activity-item button {{ width: 100%; text-align: left; background: transparent; border: none; padding: 14px; font: inherit; color: inherit; cursor: pointer; }}
    .activity-item.selected {{ border-color: var(--accent); box-shadow: inset 0 0 0 1px var(--accent); background: var(--accent-soft); }}
    .activity-kicker {{ display: flex; justify-content: space-between; gap: 12px; font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }}
    .activity-summary {{ font-size: 17px; margin: 6px 0; }}
    .activity-meta {{ display: grid; gap: 6px; font-size: 13px; color: var(--muted); }}
    .activity-meta-row {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .meta-chip {{ display: inline-flex; align-items: center; gap: 4px; padding: 3px 8px; border-radius: 999px; background: rgba(184, 92, 56, 0.08); }}
    .detail-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px 16px; margin-bottom: 16px; }}
    .detail-block dt {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }}
    .detail-block dd {{ margin: 4px 0 0; }}
    .detail-section {{ border-top: 1px solid var(--border); padding-top: 14px; margin-top: 14px; }}
    .detail-section:first-of-type {{ border-top: none; margin-top: 0; padding-top: 0; }}
    .detail-message {{ white-space: pre-wrap; word-break: break-word; font-size: 18px; line-height: 1.5; }}
    .empty-state {{ padding: 28px 18px; color: var(--muted); }}
    @media (max-width: 900px) {{
      main {{ padding: 16px; }}
      .layout {{ grid-template-columns: 1fr; }}
      .list-panel {{ min-height: 0; max-height: none; }}
      .detail-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main>
    <div class="panel">
      <div class="topbar">
        <div>
          <h1>Worker Now</h1>
          <div id="current-executor">{current_state_markup}</div>
          <p class="muted" id="refresh-note">{html.escape(refresh_note)}</p>
        </div>
        <div class="controls">
          <button class="button-link" id="refresh-toggle" type="button">{html.escape(refresh_label)}</button>
          <a class="button-link" href="/activity.json{"?include_internal=1" if include_internal else ""}">JSON</a>
          <a class="button-link" href="{toggle_href}">{toggle_label}</a>
          <a class="button-link" href="{refresh_href}">open current view</a>
        </div>
      </div>
    </div>
    <div class="layout">
      <section class="panel list-panel">
        <div class="list-header">
          <h2>Recent Conversations</h2>
          {showing_note}
        </div>
        <ul class="activity-list" id="activity-list">{initial_list_markup}</ul>
      </section>
      <section class="panel" id="detail-panel">{initial_detail_markup}</section>
    </div>
    <script id="activity-snapshot" type="application/json">{activity_json}</script>
    <script>
      (() => {{
        const snapshotElement = document.getElementById("activity-snapshot");
        const listElement = document.getElementById("activity-list");
        const detailElement = document.getElementById("detail-panel");
        const currentExecutorElement = document.getElementById("current-executor");
        const refreshToggleElement = document.getElementById("refresh-toggle");
        const refreshNoteElement = document.getElementById("refresh-note");
        const initialSnapshot = JSON.parse(snapshotElement.textContent);
        const includeInternal = {str(include_internal).lower()};
        let autoRefresh = {str(auto_refresh).lower()};
        let selectedId = null;
        let currentActivity = Array.isArray(initialSnapshot.recent_feed)
          ? initialSnapshot.recent_feed
          : Array.isArray(initialSnapshot.recent_activity)
          ? initialSnapshot.recent_activity
          : [];
        let timerId = null;

        function eventId(item) {{
          if (item && Number.isInteger(item.activity_id)) {{
            return String(item.activity_id);
          }}
          return [
            item.occurred_at || "",
            item.phase || "",
            item.task_id || "",
            item.envelope_id || "",
            item.run_id || "",
            item.summary || "",
          ].join("|");
        }}

        function escapeHtml(value) {{
          return String(value ?? "")
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
        }}

        function messageText(item) {{
          const detail = item && typeof item.detail === "object" ? item.detail : null;
          if (!detail) {{
            return null;
          }}
          for (const key of ["content", "body"]) {{
            const value = detail[key];
            if (typeof value === "string" && value.trim()) {{
              return value.trim();
            }}
          }}
          return null;
        }}

        function listMetaEntries(item) {{
          return [
            ["task", item && item.task_id ? item.task_id : null],
            ["source", item && item.source ? item.source : null],
            ["workflow", item && item.workflow ? item.workflow : null],
          ].filter(([, value]) => value);
        }}

        function detailWithoutMessage(detail) {{
          if (!detail || typeof detail !== "object") {{
            return detail;
          }}
          return Object.fromEntries(
            Object.entries(detail).filter(([key]) => key !== "content" && key !== "body")
          );
        }}

        function friendlyTimestamp(value) {{
          if (!value) {{
            return "";
          }}
          const parsed = new Date(value);
          if (Number.isNaN(parsed.getTime())) {{
            return String(value);
          }}
          return new Intl.DateTimeFormat("en-GB", {{
            weekday: "short",
            day: "2-digit",
            month: "short",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            timeZone: "UTC",
            timeZoneName: "short",
          }}).format(parsed);
        }}

        function renderCurrentExecutor(currentExecutor) {{
          const active = Boolean(currentExecutor && currentExecutor.active);
          const entries = [
            ["state", active ? "running" : "idle"],
            ["phase", currentExecutor && currentExecutor.phase ? currentExecutor.phase : "idle"],
            ["task_id", currentExecutor && currentExecutor.task_id],
            ["envelope_id", currentExecutor && currentExecutor.envelope_id],
            ["workflow", currentExecutor && currentExecutor.workflow],
            ["attempt", currentExecutor && currentExecutor.attempt],
            ["pid", currentExecutor && currentExecutor.pid],
            [
              "started_at",
              currentExecutor && currentExecutor.started_at
                ? friendlyTimestamp(currentExecutor.started_at)
                : null,
            ],
          ].filter(([, value]) => value !== null && value !== undefined && value !== "");
          currentExecutorElement.innerHTML = `<div class="detail-grid">${{entries
            .map(
              ([label, value]) =>
                `<dl class="detail-block"><dt>${{escapeHtml(label)}}</dt><dd>${{escapeHtml(value)}}</dd></dl>`
            )
            .join("")}}</div>`;
        }}

        function renderList() {{
          if (!currentActivity.length) {{
            listElement.innerHTML = `<li class="empty-state">No recent worker activity.</li>`;
            return;
          }}
          listElement.innerHTML = currentActivity
            .map((item) => {{
              const id = eventId(item);
              const metaMarkup = listMetaEntries(item)
                .map(
                  ([label, value]) =>
                    `<span class="meta-chip"><strong>${{escapeHtml(label)}}:</strong> ${{escapeHtml(value)}}</span>`
                )
                .join("");
              const selectedClass = id === selectedId ? " selected" : "";
              return `
                <li class="activity-item${{selectedClass}}" data-event-id="${{escapeHtml(id)}}">
                  <button type="button">
                    <div class="activity-kicker">
                      <span title="${{escapeHtml(item.occurred_at || "")}}">${{escapeHtml(
                        friendlyTimestamp(item.occurred_at)
                      )}}</span>
                      <span>${{escapeHtml(item.phase || "")}}</span>
                    </div>
                    <div class="activity-summary">${{escapeHtml(item.summary || "")}}</div>
                    <div class="activity-meta">
                      <div class="activity-meta-row">${{metaMarkup || '<span class="muted">No event metadata</span>'}}</div>
                    </div>
                  </button>
                </li>`;
            }})
            .join("");
        }}

        function renderDetail() {{
          const selectedItem =
            currentActivity.find((item) => eventId(item) === selectedId) ||
            currentActivity[0] ||
            null;
          if (selectedItem) {{
            selectedId = eventId(selectedItem);
          }}
          if (!selectedItem) {{
            detailElement.innerHTML =
              `<div class="empty-state">Select an event to inspect it.</div>`;
            return;
          }}
          const message = messageText(selectedItem);
          const detailJson = JSON.stringify(
            detailWithoutMessage(selectedItem.detail),
            null,
            2
          );
          const metaEntries = [
            ["When", friendlyTimestamp(selectedItem.occurred_at)],
            ["Phase", selectedItem.phase || ""],
            ["Task", selectedItem.task_id || ""],
            ["Envelope", selectedItem.envelope_id || ""],
            ["Run", selectedItem.run_id || ""],
            ["Source", selectedItem.source || ""],
            ["Workflow", selectedItem.workflow || ""],
          ].filter(([, value]) => value);
          detailElement.innerHTML = `
            <h2>Message Detail</h2>
            <div class="detail-section">
              <div class="detail-grid">
                ${{metaEntries
                  .map(
                    ([label, value]) =>
                      `<dl class="detail-block"><dt>${{escapeHtml(label)}}</dt><dd>${{escapeHtml(value)}}</dd></dl>`
                  )
                  .join("")}}
              </div>
            </div>
            <div class="detail-section">
              <h3>Summary</h3>
              <p>${{escapeHtml(selectedItem.summary || "")}}</p>
            </div>
            <div class="detail-section">
              <h3>Message</h3>
              <div class="detail-message">${{
                message
                  ? escapeHtml(message)
                  : '<span class="muted">No message text captured for this event.</span>'
              }}</div>
            </div>
            <div class="detail-section">
              <h3>Detail JSON</h3>
              <pre><code>${{escapeHtml(detailJson)}}</code></pre>
            </div>`;
        }}

        async function refreshData() {{
          const response = await fetch(`/activity.json${{includeInternal ? "?include_internal=1" : ""}}`, {{
            headers: {{ Accept: "application/json" }},
            cache: "no-store",
          }});
          if (!response.ok) {{
            throw new Error(`activity fetch failed: ${{response.status}}`);
          }}
          const snapshot = await response.json();
          currentActivity = Array.isArray(snapshot.recent_feed)
            ? snapshot.recent_feed
            : Array.isArray(snapshot.recent_activity)
            ? snapshot.recent_activity
            : [];
          renderCurrentExecutor(snapshot.current_executor || {{}});
          renderList();
          renderDetail();
        }}

        function updateRefreshControls() {{
          refreshToggleElement.textContent = autoRefresh ? "pause refresh" : "resume refresh";
          refreshNoteElement.textContent = autoRefresh
            ? "Auto-refresh every 5s in the background."
            : "Auto-refresh paused.";
        }}

        function resetTimer() {{
          if (timerId !== null) {{
            window.clearInterval(timerId);
            timerId = null;
          }}
          if (autoRefresh) {{
            timerId = window.setInterval(() => {{
              refreshData().catch((error) => {{
                console.error(error);
              }});
            }}, 5000);
          }}
        }}

        listElement.addEventListener("click", (event) => {{
          const target =
            event.target instanceof Element ? event.target.closest("[data-event-id]") : null;
          if (!target) {{
            return;
          }}
          const id = target.getAttribute("data-event-id");
          if (!id) {{
            return;
          }}
          selectedId = id;
          renderList();
          renderDetail();
        }});

        refreshToggleElement.addEventListener("click", () => {{
          autoRefresh = !autoRefresh;
          updateRefreshControls();
          resetTimer();
        }});

        renderCurrentExecutor(initialSnapshot.current_executor || {{}});
        if (currentActivity.length) {{
          selectedId = eventId(currentActivity[0]);
        }}
        renderList();
        renderDetail();
        updateRefreshControls();
        resetTimer();
      }})();
    </script>
  </main>
</body>
</html>"""


def _render_current_executor(current_executor: dict[str, object]) -> str:
    active = bool(current_executor.get("active"))
    entries = [
        ("state", "running" if active else "idle"),
        ("phase", str(current_executor.get("phase", "idle"))),
    ]
    for key in ("task_id", "envelope_id", "workflow", "attempt", "pid", "started_at"):
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


def _build_conversation_feed(
    activity: list[dict[str, object]],
    *,
    limit: int,
) -> list[dict[str, object]]:
    message_events = [item for item in activity if _is_conversation_event(item)]
    if message_events:
        return message_events[:limit]
    return activity[:limit]


def _render_activity_list(activity: list[object]) -> str:
    items = []
    for item in activity:
        assert isinstance(item, dict)
        event_id = _event_id(item)
        occurred_at = str(item.get("occurred_at", ""))
        meta_entries = _list_meta_entries(item)
        empty_meta_markup = "<span class='muted'>No event metadata</span>"
        meta_markup = "".join(
            "<span class='meta-chip'>"
            f"<strong>{html.escape(label)}:</strong> {html.escape(value)}"
            "</span>"
            for label, value in meta_entries
        )
        items.append(
            "<li class='activity-item' "
            f"data-event-id='{html.escape(event_id)}'>"
            "<button type='button'>"
            "<div class='activity-kicker'>"
            f"<span title='{html.escape(occurred_at)}'>{html.escape(_friendly_timestamp(occurred_at))}</span>"
            f"<span>{html.escape(str(item.get('phase', '')))}</span>"
            "</div>"
            f"<div class='activity-summary'>{html.escape(str(item.get('summary', '')))}</div>"
            "<div class='activity-meta'>"
            "<div class='activity-meta-row'>"
            f"{meta_markup or empty_meta_markup}"
            "</div>"
            "</div>"
            "</button>"
            "</li>"
        )
    if not items:
        return "<li class='empty-state'>No recent worker activity.</li>"
    items[0] = items[0].replace(
        "class='activity-item'", "class='activity-item selected'", 1
    )
    return "".join(items)


def _render_detail_panel(item: object) -> str:
    if not isinstance(item, dict):
        return "<div class='empty-state'>No recent worker activity.</div>"
    detail = item.get("detail")
    message = _message_text(item)
    detail_json = html.escape(
        json.dumps(_detail_without_message(detail), indent=2, sort_keys=True)
    )
    entries = [
        ("When", _friendly_timestamp(item.get("occurred_at"))),
        ("Phase", str(item.get("phase", ""))),
        ("Task", str(item.get("task_id", ""))),
        ("Envelope", str(item.get("envelope_id", ""))),
        ("Run", str(item.get("run_id", ""))),
        ("Source", str(item.get("source", ""))),
        ("Workflow", str(item.get("workflow", ""))),
    ]
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
    message_markup = (
        html.escape(message)
        if message is not None
        else "<span class='muted'>No message text captured for this event.</span>"
    )
    return (
        "<h2>Message Detail</h2>"
        "<div class='detail-section'>"
        f"<div class='detail-grid'>{''.join(blocks)}</div>"
        "</div>"
        "<div class='detail-section'>"
        "<h3>Summary</h3>"
        f"<p>{html.escape(str(item.get('summary', '')))}</p>"
        "</div>"
        "<div class='detail-section'>"
        "<h3>Message</h3>"
        f"<div class='detail-message'>{message_markup}</div>"
        "</div>"
        "<div class='detail-section'>"
        "<h3>Detail JSON</h3>"
        f"<pre><code>{detail_json}</code></pre>"
        "</div>"
    )


def _list_meta_entries(item: dict[str, object]) -> list[tuple[str, str]]:
    entries = [
        ("task", str(item.get("task_id", ""))),
        ("source", str(item.get("source", ""))),
        ("workflow", str(item.get("workflow", ""))),
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


def _is_conversation_event(item: dict[str, object]) -> bool:
    if bool(item.get("is_internal")):
        return False
    message = _message_text(item)
    if message is None:
        return False
    phase = str(item.get("phase", ""))
    if phase == "task_received":
        source = str(item.get("source", ""))
        return source not in {"cron", "internal"}
    if not phase.startswith("egress_"):
        return False
    detail = item.get("detail")
    if not isinstance(detail, dict):
        return False
    channel = str(detail.get("channel", ""))
    return channel not in {"internal", "log"}
