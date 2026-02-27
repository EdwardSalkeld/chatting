import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.models import RunRecord
from app.state.sqlite_store import SQLiteStateStore


class SQLiteStateStoreTests(unittest.TestCase):
    def test_seen_and_mark_seen_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            self.assertFalse(store.seen("email:abc"))
            store.mark_seen("email:abc")
            self.assertTrue(store.seen("email:abc"))

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


if __name__ == "__main__":
    unittest.main()
