"""Disposable worker activity fixture for platform browser QA.

Run with PYTHONPATH=. uv run python tests/platform_preview.py --state-dir /tmp/…
No executor, broker or outbound delivery is started. Only activity is simulated;
HTTP responses come from the real worker monitor and SQLite store.
"""

from __future__ import annotations

import argparse
import json
import signal
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import (
    AuditEvent,
    OutboundMessage,
    ReplyChannel,
    RunRecord,
    TaskEnvelope,
)
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor, start_worker_activity_server


def task(index: int, title: str, content: str, at: datetime) -> TaskQueueMessage:
    envelope = TaskEnvelope(
        id=f"platform-fixture:{index}",
        source="im",
        received_at=at,
        actor="preview",
        content=content,
        attachments=[],
        context_refs=[],
        reply_channel=ReplyChannel(
            type="telegram", target="fixture-chat", metadata={"chat_title": title}
        ),
        dedupe_key=f"platform-fixture:{index}",
    )
    return replace(
        TaskQueueMessage.from_envelope(envelope, trace_id=f"preview:{index}"),
        work_item_id=f"item_fixture_{index}",
    )


def finish(store, monitor, message, status, created_at):
    run = RunRecord(
        run_id=f"run:fixture:{message.task_id}",
        envelope_id=message.envelope.id,
        source="im",
        workflow="default",
        latency_ms=42300,
        result_status=status,
        created_at=created_at,
        work_item_id=message.work_item_id,
    )
    store.append_run(run)
    store.append_audit_event(
        AuditEvent(
            run_id=run.run_id,
            envelope_id=run.envelope_id,
            source=run.source,
            workflow=run.workflow,
            result_status=status,
            detail={"task_id": message.task_id, "attempt_count": 1, "reason_codes": []},
            created_at=created_at,
        )
    )
    monitor.record_executor_finished(
        task_message=message,
        run_id=run.run_id,
        result_status=status,
        attempt_count=1,
        reason_codes=[],
        latency_ms=42300,
    )
    with sqlite3.connect(store.db_path) as db:
        db.execute(
            "UPDATE worker_inbox SET state='completed' WHERE task_id=?",
            (message.task_id,),
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=19465)
    parser.add_argument("--empty", action="store_true")
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    path = args.state_dir / "worker.db"
    if path.exists():
        parser.error("use a fresh state directory for each preview")
    store = SQLiteStateStore(str(path))
    monitor = WorkerActivityMonitor(store=store)
    now = datetime.now(timezone.utc)
    live = None
    if not args.empty:
        specs = [
            (
                "Platform interface",
                "Review the new platform interface and check the layout on mobile.",
                "live",
            ),
            (
                "Release notes",
                "Prepare release notes for the latest changes.",
                "success",
            ),
            (
                "Repository maintenance",
                "Check the repository for dependency updates.",
                "error",
            ),
            (
                "Friday planning",
                "Bring together a short plan for Friday’s review.",
                "queued",
            ),
        ]
        for index, (title, content, status) in enumerate(specs):
            at = now - timedelta(minutes=index * 35 + 2)
            message = task(index, title, content, at)
            store.stage_inbox_task(message)
            if status == "queued":
                continue
            monitor.record_task_received(task_message=message)
            monitor.record_executor_started(task_message=message, attempt=1)
            native = [
                {
                    "method": "item/started",
                    "params": {
                        "item": {
                            "type": "commandExecution",
                            "command": "git status --short",
                        }
                    },
                },
                {
                    "method": "item/completed",
                    "params": {
                        "item": {
                            "type": "commandExecution",
                            "command": "git status --short",
                            "exitCode": 0,
                            "aggregatedOutput": "Working tree is clean.",
                        }
                    },
                },
                {
                    "method": "item/completed",
                    "params": {
                        "item": {
                            "type": "agentMessage",
                            "text": "I’ve checked the current state. The layout and core flows are ready for a closer look.",
                        }
                    },
                },
            ]
            output = "\n".join(json.dumps(row) for row in native) + "\n"
            monitor.record_executor_output(
                task_message=message, stream="stdout", content=output[:73]
            )
            monitor.record_executor_output(
                task_message=message, stream="stdout", content=output[73:]
            )
            monitor.record_egress(
                egress_message=EgressQueueMessage(
                    task_id=message.task_id,
                    envelope_id=message.envelope.id,
                    trace_id=message.trace_id,
                    message=OutboundMessage(
                        channel="telegram",
                        target="fixture-chat",
                        body="Fixture delivery: the review is ready.",
                    ),
                    emitted_at=now,
                    event_id=f"fixture-egress:{index}",
                    sequence=0,
                ),
                publish_source="preview",
            )
            if status == "live":
                live = message
                with sqlite3.connect(store.db_path) as db:
                    db.execute(
                        "UPDATE worker_inbox SET state='active' WHERE task_id=?",
                        (message.task_id,),
                    )
            else:
                finish(store, monitor, message, status, at)
    server = start_worker_activity_server(
        host="127.0.0.1", port=args.port, monitor=monitor
    )
    stopped = Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    print(f"Fixture worker: http://127.0.0.1:{args.port}", flush=True)
    try:
        while not stopped.wait(5):
            if live and (args.state_dir / "finish-live").exists():
                finish(store, monitor, live, "success", datetime.now(timezone.utc))
                live = None
            elif live:
                monitor.record_executor_output(
                    task_message=live,
                    stream="stdout",
                    content=json.dumps(
                        {
                            "method": "item/completed",
                            "params": {
                                "item": {
                                    "type": "agentMessage",
                                    "text": f"Preview activity tick · {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC",
                                }
                            },
                        }
                    )
                    + "\n",
                )
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
