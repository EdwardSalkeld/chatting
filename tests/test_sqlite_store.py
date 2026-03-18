import tempfile
import unittest
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.broker import EgressQueueMessage
from app.models import AttachmentRef, AuditEvent, OutboundMessage, ReplyChannel, RunRecord, TaskEnvelope
from app.state.sqlite_store import SQLiteStateStore
class SQLiteStateStoreTests(unittest.TestCase):
    def test_seen_and_mark_seen_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            self.assertFalse(store.seen("email", "provider-abc"))
            store.mark_seen("email", "provider-abc")
            self.assertTrue(store.seen("email", "provider-abc"))

    def test_seen_scopes_dedupe_keys_by_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            store.mark_seen("email", "same-id")
            self.assertTrue(store.seen("email", "same-id"))
            self.assertFalse(store.seen("cron", "same-id"))

    def test_initialization_migrates_legacy_idempotency_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE idempotency_keys (
                    dedupe_key TEXT PRIMARY KEY,
                    seen_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT INTO idempotency_keys (dedupe_key, seen_at) VALUES (?, ?)",
                ("legacy-key", "2026-02-27T16:00:00Z"),
            )
            connection.commit()
            connection.close()

            store = SQLiteStateStore(str(db_path))

            self.assertTrue(store.seen("legacy", "legacy-key"))
            self.assertFalse(store.seen("email", "legacy-key"))
            store.mark_seen("email", "legacy-key")
            self.assertTrue(store.seen("email", "legacy-key"))

    def test_initialization_drops_legacy_admin_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE pending_approvals (approval_id INTEGER PRIMARY KEY)")
            connection.execute("CREATE TABLE current_config (config_path TEXT PRIMARY KEY)")
            connection.execute("CREATE TABLE config_versions (version_id INTEGER PRIMARY KEY)")
            connection.commit()
            connection.close()

            SQLiteStateStore(str(db_path))

            connection = sqlite3.connect(db_path)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            connection.close()

            self.assertNotIn("pending_approvals", tables)
            self.assertNotIn("current_config", tables)
            self.assertNotIn("config_versions", tables)

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

    def test_append_dead_letter_persists_and_marks_replayed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            envelope = TaskEnvelope(
                id="email:dead-1",
                source="email",
                received_at=datetime(2026, 2, 27, 16, 10, tzinfo=timezone.utc),
                actor="alice@example.com",
                content="Please retry",
                attachments=[AttachmentRef(uri="file:///tmp/photo.jpg", name="photo.jpg")],
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
            pending = store.list_dead_letters(status="pending")
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].dead_letter_id, dead_letter_id)
            self.assertEqual(pending[0].envelope.id, envelope.id)
            self.assertEqual(pending[0].envelope.attachments, envelope.attachments)
            self.assertEqual(pending[0].status, "pending")

            store.mark_dead_letter_replayed(dead_letter_id, "run:email:dead-1:replay:1")
            replayed = store.list_dead_letters(status="replayed")
            self.assertEqual(len(replayed), 1)
            self.assertEqual(replayed[0].dead_letter_id, dead_letter_id)
            self.assertEqual(replayed[0].replayed_run_id, "run:email:dead-1:replay:1")

    def test_conversation_turns_roundtrip_scoped_by_channel_and_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            store.append_conversation_turn(
                channel="telegram",
                target="chat-1",
                role="user",
                content="What country is Paris in?",
                run_id="run:telegram:1",
            )
            store.append_conversation_turn(
                channel="telegram",
                target="chat-1",
                role="assistant",
                content="Paris is in France.",
                run_id="run:telegram:1",
            )
            store.append_conversation_turn(
                channel="telegram",
                target="chat-2",
                role="user",
                content="Should not appear in chat-1 history.",
                run_id="run:telegram:2",
            )

            turns = store.list_recent_conversation_turns(
                channel="telegram",
                target="chat-1",
                limit=8,
            )
            self.assertEqual(
                turns,
                [
                    ("user", "What country is Paris in?"),
                    ("assistant", "Paris is in France."),
                ],
            )

    def test_dispatched_event_checkpoint_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            store.mark_dispatched_event(run_id="run:telegram:1", event_index=1)
            store.mark_dispatched_event(run_id="run:telegram:1", event_index=0)
            store.mark_dispatched_event(run_id="run:telegram:1", event_index=1)

            self.assertEqual(
                store.list_dispatched_event_indices(run_id="run:telegram:1"),
                [0, 1],
            )
            self.assertEqual(
                store.list_dispatched_event_indices(run_id="run:telegram:other"),
                [],
            )

    def test_dispatched_event_id_checkpoint_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            self.assertFalse(
                store.has_dispatched_event_id(task_id="task:telegram:1", event_id="evt:1")
            )
            store.mark_dispatched_event_id(task_id="task:telegram:1", event_id="evt:1")
            store.mark_dispatched_event_id(task_id="task:telegram:1", event_id="evt:1")

            self.assertTrue(
                store.has_dispatched_event_id(task_id="task:telegram:1", event_id="evt:1")
            )
            self.assertFalse(
                store.has_dispatched_event_id(task_id="task:telegram:1", event_id="evt:2")
            )

    def test_egress_outbox_roundtrip_and_replay_listing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            message = EgressQueueMessage(
                task_id="task:email:1",
                envelope_id="email:1",
                trace_id="trace:email:1",
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="hello"),
                emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:0",
                sequence=0,
                event_kind="message",
                message_type="chatting.egress.v2",
            )

            store.queue_egress_outbox_event(message)
            replayable = store.list_replayable_egress_outbox_events()
            self.assertEqual(len(replayable), 1)
            self.assertEqual(replayable[0].event_id, "evt:task:email:1:0")

            store.mark_egress_outbox_event_published(event_id="evt:task:email:1:0")
            replayable = store.list_replayable_egress_outbox_events()
            self.assertEqual(replayable, [])

            store.mark_egress_outbox_event_acked(event_id="evt:task:email:1:0")
            replayable = store.list_replayable_egress_outbox_events()
            self.assertEqual(replayable, [])
if __name__ == "__main__":
    unittest.main()
