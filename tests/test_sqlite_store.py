import tempfile
import unittest
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.models import AuditEvent, ReplyChannel, RunRecord, TaskEnvelope
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

    def test_append_run_persists_and_lists_run_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            record = RunRecord(
                run_id="run_1",
                envelope_id="evt_1",
                source="cron",
                workflow="respond_and_optionally_edit",
                policy_profile="default",
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
                policy_profile="default",
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
                attachments=[],
                context_refs=["repo:/home/edward/chatting"],
                policy_profile="default",
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
            self.assertEqual(pending[0].status, "pending")

            store.mark_dead_letter_replayed(dead_letter_id, "run:email:dead-1:replay:1")
            replayed = store.list_dead_letters(status="replayed")
            self.assertEqual(len(replayed), 1)
            self.assertEqual(replayed[0].dead_letter_id, dead_letter_id)
            self.assertEqual(replayed[0].replayed_run_id, "run:email:dead-1:replay:1")

    def test_pending_approval_roundtrip_and_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            approval_id = store.append_pending_approval(
                run_id="run:email:1",
                envelope_id="email:1",
                config_path="secrets.api_key",
                config_value="new-secret",
            )
            pending = store.list_pending_approvals(status="pending")
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].approval_id, approval_id)
            self.assertEqual(pending[0].config_path, "secrets.api_key")
            self.assertEqual(pending[0].status, "pending")

            store.resolve_pending_approval(approval_id, "approved")
            approved = store.list_pending_approvals(status="approved")
            self.assertEqual(len(approved), 1)
            self.assertEqual(approved[0].approval_id, approval_id)
            self.assertEqual(approved[0].status, "approved")

    def test_apply_config_update_and_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            version_id = store.apply_config_update(
                config_path="routing.default_timeout",
                new_value=240,
                source="pending_approval",
                source_ref="approval:1",
            )
            versions = store.list_config_versions()
            self.assertEqual(len(versions), 1)
            self.assertEqual(versions[0].version_id, version_id)
            self.assertEqual(versions[0].old_value, None)
            self.assertEqual(versions[0].new_value, 240)

            rollback_version_id = store.rollback_config_version(version_id)
            versions = store.list_config_versions()
            self.assertEqual(len(versions), 2)
            self.assertEqual(versions[1].version_id, rollback_version_id)
            self.assertEqual(versions[1].source, "rollback")


if __name__ == "__main__":
    unittest.main()
