import tempfile
import unittest
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.broker import EgressQueueMessage
from app.models import (
    AttachmentRef,
    AuditEvent,
    OutboundMessage,
    ReplyChannel,
    RunRecord,
    TaskEnvelope,
)
from app.state.sqlite_store import SQLiteStateStore


class SQLiteStateStoreTests(unittest.TestCase):
    def test_append_run_persists_and_lists_run_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            record = RunRecord(
                run_id="run_1",
                envelope_id="evt_1",
                source="cron",
                workflow="respond_and_optionally_edit",
                latency_ms=12,
                result_status="success",
                created_at=datetime(2026, 2, 27, 16, 10, tzinfo=timezone.utc),
            )

            store.append_run(record)

            self.assertEqual(store.list_runs(), [record])

    def test_append_audit_event_persists_and_lists_audit_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            event = AuditEvent(
                run_id="run_1",
                envelope_id="evt_1",
                source="cron",
                workflow="respond_and_optionally_edit",
                result_status="success",
                detail={"approved_action_count": 1, "reason_codes": []},
                created_at=datetime(2026, 2, 27, 16, 10, tzinfo=timezone.utc),
            )

            store.append_audit_event(event)

            self.assertEqual(store.list_audit_events(), [event])

    def test_append_dead_letter_persists_pending_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            envelope = TaskEnvelope(
                id="email:dead-1",
                source="email",
                received_at=datetime(2026, 2, 27, 16, 10, tzinfo=timezone.utc),
                actor="alice@example.com",
                content="Please retry",
                attachments=[
                    AttachmentRef(uri="file:///tmp/photo.jpg", name="photo.jpg")
                ],
                context_refs=["repo:/home/edward/chatting"],
                reply_channel=ReplyChannel(type="email", target="alice@example.com"),
                dedupe_key="email:dead-1",
            )

            dead_letter_id = store.append_dead_letter(
                run_id="run:email:dead-1",
                envelope=envelope,
                reason_codes=["retry_exhausted"],
                last_error="RuntimeError: boom",
                attempt_count=2,
            )
            with sqlite3.connect(db_path) as connection:
                rows = connection.execute(
                    "SELECT dead_letter_id, run_id, reason_codes_json, status FROM dead_letters"
                ).fetchall()
            self.assertEqual(
                rows,
                [
                    (
                        dead_letter_id,
                        "run:email:dead-1",
                        '["retry_exhausted"]',
                        "pending",
                    )
                ],
            )

    def test_egress_outbox_roundtrip_and_replay_listing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            message = EgressQueueMessage(
                task_id="task:email:1",
                envelope_id="email:1",
                trace_id="trace:email:1",
                message=OutboundMessage(
                    channel="email", target="alice@example.com", body="hello"
                ),
                emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:0",
                sequence=0,
                event_kind="message",
            )

            store.queue_egress_outbox_event(message)
            replayable = store.list_replayable_egress_outbox_events()
            self.assertEqual(len(replayable), 1)
            self.assertEqual(replayable[0].event_id, "evt:task:email:1:0")

            store.mark_egress_outbox_event_acked(event_id="evt:task:email:1:0")
            replayable = store.list_replayable_egress_outbox_events()
            self.assertEqual(replayable, [])

    def test_worker_activity_roundtrip_and_internal_filtering(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            store.append_worker_activity(
                occurred_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
                task_id="task:email:1",
                envelope_id="email:1",
                phase="task_received",
                summary="email task received",
                detail={"reply_channel": "email"},
            )
            store.append_worker_activity(
                occurred_at=datetime(2026, 3, 31, 12, 1, tzinfo=timezone.utc),
                task_id="task:internal:heartbeat:1",
                envelope_id="internal:heartbeat:1",
                phase="egress_message",
                summary="heartbeat pong",
                detail={"channel": "log"},
                is_internal=True,
            )

            visible = store.list_recent_worker_activity(limit=10)
            self.assertEqual(len(visible), 1)
            self.assertEqual(visible[0]["phase"], "task_received")
            self.assertIsInstance(visible[0]["activity_id"], int)

            all_events = store.list_recent_worker_activity(
                limit=10, include_internal=True
            )
            self.assertEqual(len(all_events), 2)
            self.assertEqual(all_events[0]["phase"], "egress_message")
            self.assertEqual(all_events[1]["phase"], "task_received")
            self.assertGreater(
                all_events[0]["activity_id"], all_events[1]["activity_id"]
            )

    def test_worker_activity_orders_by_occurred_at_not_insert_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            store.append_worker_activity(
                occurred_at=datetime(2026, 4, 27, 19, 0, tzinfo=timezone.utc),
                task_id="task:im:new",
                envelope_id="im:new",
                source="im",
                phase="task_received",
                summary="newer chat task received",
                detail={"reply_channel": "telegram"},
            )
            store.append_worker_activity(
                occurred_at=datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc),
                task_id="task:cron:old",
                envelope_id="cron:old",
                source="cron",
                phase="egress_incremental",
                summary="old cron replayed later",
                detail={"channel": "telegram"},
            )

            visible = store.list_recent_worker_activity(limit=10)
            self.assertEqual(
                [item["task_id"] for item in visible],
                ["task:im:new", "task:cron:old"],
            )
            self.assertGreater(visible[1]["activity_id"], visible[0]["activity_id"])


if __name__ == "__main__":
    unittest.main()
