"""Worker-local runtime activity tracking and read-only HTTP UI."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import HTTPServer, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Callable

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
        self._active_executors: dict[str, dict[str, object]] = {}

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
            "work_item_id": (
                assignment.work_item_id
                if (
                    assignment := self._store.get_work_assignment(
                        task_id=task_message.task_id
                    )
                )
                else None
            ),
            "preview": _extract_current_message(envelope.content)[:240],
        }
        with self._lock:
            self._active_executors[task_message.task_id] = state
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

    def record_executor_pid(
        self, *, pid: int | None, task_id: str | None = None
    ) -> None:
        if pid is None:
            return
        with self._lock:
            active = self._active_executors.get(task_id) if task_id else None
            if active is None and len(self._active_executors) == 1:
                active = next(iter(self._active_executors.values()))
            if active is None:
                return
            active["pid"] = pid

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
            self._active_executors.pop(task_message.task_id, None)
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
        current_executor = self._current_executor()
        activity = self._store.list_recent_worker_activity(
            limit=self._history_limit,
            include_internal=include_internal,
        )
        return {
            "current_executor": current_executor,
            "active_executors": self._current_executors(),
            "queue": self._store.inbox_queue_summary(),
            "current_run": self._build_current_run_summary(
                current_executor=current_executor,
                include_internal=include_internal,
            ),
            "recent_activity": activity,
            "history_limit": self._history_limit,
            "history_truncated": len(activity) >= self._history_limit,
            "include_internal": include_internal,
        }

    def list_runs_snapshot(
        self, *, include_internal: bool = False
    ) -> dict[str, object]:
        current_executor = self._current_executor()
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
            "active_executors": self._current_executors(),
            "queue": self._store.inbox_queue_summary(),
            "current_run": self._build_current_run_summary(
                current_executor=current_executor,
                include_internal=include_internal,
            ),
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
        current_executor = self._current_executor()
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

    def list_items_snapshot(self) -> dict[str, object]:
        return {
            "items": self._store.list_work_item_overview(),
            "active_executors": self._current_executors(),
        }

    def get_item_snapshot(self, work_item_id: str) -> dict[str, object] | None:
        item = next(
            (
                item
                for item in self._store.list_work_item_overview()
                if item["work_item_id"] == work_item_id
            ),
            None,
        )
        if item is None:
            return None
        runs = self._store.list_work_item_run_cards(
            work_item_id=work_item_id, limit=self._history_limit
        )
        for run in runs:
            run["preview"] = _extract_current_message(run.pop("request_content", None))[
                :240
            ]
        return {
            "item": item,
            "runs": runs,
            "active_executors": [
                executor
                for executor in self._current_executors()
                if executor.get("work_item_id") == work_item_id
            ],
        }

    def live_events(self, *, task_id: str, after_id: int) -> dict[str, object]:
        events = self._store.list_worker_activity_since(
            task_id=task_id, after_id=after_id
        )
        return {
            "events": events,
            "active": any(
                item.get("task_id") == task_id for item in self._current_executors()
            ),
        }

    def get_run_header(self, run_id: str) -> dict[str, object] | None:
        run = self._store.get_run(run_id=run_id)
        audit = self._store.get_audit_event_for_run(run_id=run_id)
        if run is None or audit is None:
            return None
        detail = audit.detail if isinstance(audit.detail, dict) else {}
        task_id = detail.get("task_id")
        first_event = (
            self._store.list_worker_activity_since(task_id=task_id, after_id=0, limit=1)
            if isinstance(task_id, str) and task_id
            else []
        )
        first_detail = first_event[0].get("detail", {}) if first_event else {}
        request_content = (
            first_detail.get("content") if isinstance(first_detail, dict) else None
        )
        return {
            "run_id": run.run_id,
            "task_id": task_id,
            "preview": _extract_current_message(request_content)[:240],
            "work_item_id": run.work_item_id,
            "status": run.result_status,
            "source": run.source,
            "started_at": _isoformat(run.created_at),
            "duration_ms": run.latency_ms,
            "attempt_count": detail.get("attempt_count"),
            "reason_codes": detail.get("reason_codes", []),
        }

    def _current_executor(self) -> dict[str, object]:
        with self._lock:
            return (
                {"active": False, "phase": "idle"}
                if not self._active_executors
                else dict(next(iter(self._active_executors.values())))
            )

    def _current_executors(self) -> list[dict[str, object]]:
        with self._lock:
            return [dict(item) for item in self._active_executors.values()]

    def _build_current_run_summary(
        self,
        *,
        current_executor: dict[str, object],
        include_internal: bool,
    ) -> dict[str, object] | None:
        if not current_executor.get("active"):
            return None
        task_id = current_executor.get("task_id")
        envelope_id = current_executor.get("envelope_id")
        if not isinstance(task_id, str) or not isinstance(envelope_id, str):
            return None
        events = self._store.list_worker_activity_for_task(
            task_id=task_id,
            envelope_id=envelope_id,
            include_internal=include_internal,
        )
        task_event = next(
            (item for item in events if item.get("phase") == "task_received"), None
        )
        task_detail = (
            task_event.get("detail", {}) if isinstance(task_event, dict) else {}
        )
        if not isinstance(task_detail, dict):
            task_detail = {}
        content = task_detail.get("content")
        latest_event = events[-1] if events else None
        return {
            "task_id": task_id,
            "envelope_id": envelope_id,
            "source": current_executor.get("source", ""),
            "attempt": current_executor.get("attempt"),
            "pid": current_executor.get("pid"),
            "started_at": current_executor.get("started_at"),
            "preview": _extract_current_message(content)
            if isinstance(content, str)
            else "",
            "event_count": len(events),
            "latest_phase": latest_event.get("phase")
            if isinstance(latest_event, dict)
            else None,
            "events": events,
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
            "work_item_id": run.work_item_id,
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
    from app.worker.activity_web import build_handler

    server = ThreadingHTTPServer((host, port), build_handler(monitor))
    thread = Thread(
        target=server.serve_forever, name="worker-activity-server", daemon=True
    )
    thread.start()
    LOGGER.info("worker_activity_server_started host=%s port=%s", host, port)
    return WorkerActivityServer(server=server, thread=thread)


def _isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


_CURRENT_MESSAGE_MARKER = "Current user message:"
_CURRENT_MESSAGE_FROM_MARKER = "Current message from "


def _extract_current_message(content: str | None) -> str:
    # The handler wraps context-carrying prompts as either
    # "<context>\n\nCurrent message from <sender>:\n<message>" (attributed) or
    # the older "<context>\n\nCurrent user message:\n<message>"; show just the
    # message. Sources without that wrapper (e.g. email) fall through to the raw
    # content.
    if not content:
        return ""
    index = content.rfind(_CURRENT_MESSAGE_FROM_MARKER)
    if index != -1:
        # tail is "<sender>:\n<message>"; drop the sender label line.
        tail = content[index + len(_CURRENT_MESSAGE_FROM_MARKER) :]
        _, separator, message = tail.partition("\n")
        return message.strip() if separator else tail.strip()
    if _CURRENT_MESSAGE_MARKER in content:
        return content.split(_CURRENT_MESSAGE_MARKER, 1)[1].strip()
    return content.strip()


def _message_text(item: dict[str, object]) -> str | None:
    detail = item.get("detail")
    if not isinstance(detail, dict):
        return None
    for key in ("content", "body"):
        value = detail.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None
