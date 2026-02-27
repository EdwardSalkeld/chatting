import tempfile
import unittest
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.models import AuditEvent, RunRecord
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


if __name__ == "__main__":
    unittest.main()
