import json
import tempfile
import unittest
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import OutboundMessage, ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor, start_worker_activity_server


class WorkerActivityTests(unittest.TestCase):
    def _build_task_message(self) -> TaskQueueMessage:
        envelope = TaskEnvelope(
            id="telegram:1",
            source="im",
            received_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
            actor="8605042448:edsalkeld",
            content="hello",
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(type="telegram", target="8605042448"),
            dedupe_key="telegram:1",
        )
        return TaskQueueMessage.from_envelope(envelope, trace_id="trace:telegram:1")

    def test_monitor_snapshot_tracks_executor_state_and_egress(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            monitor = WorkerActivityMonitor(store=store, history_limit=10)
            task_message = self._build_task_message()

            monitor.record_task_received(task_message=task_message)
            monitor.record_executor_started(
                task_message=task_message,
                attempt=1,
                workflow="respond_and_optionally_edit",
            )

            running_snapshot = monitor.snapshot()
            self.assertEqual(running_snapshot["current_executor"]["active"], True)
            self.assertEqual(running_snapshot["current_executor"]["task_id"], task_message.task_id)

            monitor.record_egress(
                egress_message=EgressQueueMessage(
                    task_id=task_message.task_id,
                    envelope_id=task_message.envelope.id,
                    trace_id=task_message.trace_id,
                    event_index=0,
                    event_count=1,
                    message=OutboundMessage(
                        channel="telegram",
                        target="8605042448",
                        body="working on it",
                    ),
                    emitted_at=datetime(2026, 3, 31, 12, 1, tzinfo=timezone.utc),
                    event_id="evt:telegram:1:0",
                    sequence=None,
                    event_kind="incremental",
                    message_type="chatting.egress.v2",
                ),
                publish_source="main_reply",
            )
            monitor.record_executor_finished(
                task_message=task_message,
                run_id="run:telegram:1",
                workflow="respond_and_optionally_edit",
                result_status="success",
                attempt_count=1,
                reason_codes=[],
                latency_ms=42,
            )

            snapshot = monitor.snapshot()
            self.assertEqual(snapshot["current_executor"]["active"], False)
            self.assertEqual(snapshot["recent_activity"][0]["phase"], "task_finished")
            self.assertEqual(snapshot["recent_activity"][1]["phase"], "egress_incremental")

    def test_activity_http_server_serves_json_and_html(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            monitor = WorkerActivityMonitor(store=store, history_limit=5)
            task_message = self._build_task_message()
            monitor.record_task_received(task_message=task_message)
            monitor.record_executor_started(
                task_message=task_message,
                attempt=1,
                workflow="respond_and_optionally_edit",
            )

            server = start_worker_activity_server(host="127.0.0.1", port=0, monitor=monitor)
            port = server.server.server_address[1]
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/activity.json") as response:
                    payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(payload["current_executor"]["active"], True)
                self.assertEqual(payload["recent_activity"][0]["phase"], "executor_started")

                with urllib.request.urlopen(f"http://127.0.0.1:{port}/") as response:
                    html_body = response.read().decode("utf-8")
                self.assertIn("Worker Now", html_body)
                self.assertIn("task_received", html_body)
            finally:
                server.shutdown()


if __name__ == "__main__":
    unittest.main()
