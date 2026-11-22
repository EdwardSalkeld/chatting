"""Disposable web-chat simulator. Reads ONLY the explicitly supplied fixture DB.

Pairs with the Go platform testserver's --web-socket. It simulates worker replies,
without a model, broker or Telegram connection. Never point it at live state.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event

from app.broker import TaskQueueMessage
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor, start_worker_activity_server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--handler-fixture-db", required=True, type=Path)
    parser.add_argument("--port", type=int, default=30465)
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    if (args.state_dir / "worker.db").exists():
        parser.error("use a fresh disposable state directory")
    store = SQLiteStateStore(str(args.state_dir / "worker.db"))
    now = datetime.now(timezone.utc)
    for index, title in enumerate(("Technical", "Music")):
        envelope = TaskEnvelope(
            id=f"telegram:preview:{index}",
            source="im",
            received_at=now - timedelta(minutes=10),
            actor="Ed",
            content="Can we keep talking here if Telegram is down?",
            attachments=[],
            context_refs=[],
            dedupe_key=f"preview:{index}",
            reply_channel=ReplyChannel(
                type="telegram",
                target="fixture-chat",
                metadata={
                    "message_id": index * 10 + 1,
                    "message_thread_id": index + 7,
                    "chat_title": title,
                },
            ),
        )
        task = replace(
            TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:preview:{index}"),
            work_item_id=f"item_fixture_{index}",
        )
        store.stage_inbox_task(task)
        store.record_telegram_outbound(
            target="fixture-chat",
            topic_id=index + 7,
            message_id=index * 10 + 2,
            content="Yes. Open this conversation in the web UI and your replies will stay here.",
            attachment=None,
            task_id=task.task_id,
            event_id=f"preview-reply:{index}",
            occurred_at=now - timedelta(minutes=9),
        )
        store.mark_inbox_reply_delivered(parent_task_id=task.task_id)
        store.finish_inbox_task(parent_task_id=task.task_id)
    monitor = WorkerActivityMonitor(store=store)
    server = start_worker_activity_server(
        host="127.0.0.1", port=args.port, monitor=monitor
    )
    pending: dict[str, tuple[TaskQueueMessage, float]] = {}
    stopped = Event()
    print(f"Disposable web worker: http://127.0.0.1:{args.port}", flush=True)
    try:
        while not stopped.wait(0.5):
            with sqlite3.connect(args.handler_fixture_db) as db:
                for eid, raw, item in db.execute(
                    "SELECT w.envelope_id,w.envelope_json,a.work_item_id FROM web_ingress w "
                    "JOIN task_assignments a ON a.task_id='task:'||w.envelope_id WHERE w.published_at IS NULL"
                ).fetchall():
                    envelope = json.loads(raw)
                    task = TaskQueueMessage.from_dict(
                        {
                            "schema_version": "1.0",
                            "message_type": "chatting.task.v1",
                            "task_id": "task:" + eid,
                            "trace_id": "trace:" + eid,
                            "emitted_at": envelope["received_at"],
                            "envelope": envelope,
                            "work_item_id": item,
                        }
                    )
                    store.stage_inbox_task(task)
                    with sqlite3.connect(store.db_path) as worker:
                        worker.execute(
                            "UPDATE worker_inbox SET state='active' WHERE task_id=?",
                            (task.task_id,),
                        )
                    pending[task.task_id] = task, time.monotonic() + 2
                    db.execute(
                        "UPDATE web_ingress SET published_at=? WHERE envelope_id=?",
                        (datetime.now(timezone.utc).isoformat(), eid),
                    )
            for task_id, (task, at) in list(pending.items()):
                if time.monotonic() < at:
                    continue
                store.record_web_outbound(
                    target=task.envelope.reply_channel.target,
                    content="Preview reply: your web message reached the same conversation. This answer was saved without contacting Telegram.",
                    task_id=task_id,
                    event_id=f"preview:{task_id}",
                    occurred_at=datetime.now(timezone.utc),
                )
                store.mark_inbox_reply_delivered(parent_task_id=task_id)
                store.finish_inbox_task(parent_task_id=task_id)
                del pending[task_id]
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
