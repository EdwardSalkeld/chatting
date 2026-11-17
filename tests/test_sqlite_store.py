import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.state.sqlite_store import SQLiteStateStore


class WorkerActivityStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = SQLiteStateStore(str(Path(tmp.name) / "state.db"))

    def append(self, task_id: str, occurred_at: datetime, **kwargs) -> None:
        self.store.append_worker_activity(
            occurred_at=occurred_at,
            task_id=task_id,
            envelope_id=task_id.removeprefix("task:"),
            phase="task_received",
            summary=task_id,
            detail={},
            **kwargs,
        )

    def test_internal_events_are_hidden_unless_requested(self) -> None:
        self.append("task:email:1", datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc))
        self.append(
            "task:internal:heartbeat:1",
            datetime(2026, 3, 31, 12, 1, tzinfo=timezone.utc),
            is_internal=True,
        )

        visible = self.store.list_recent_worker_activity(limit=10)
        self.assertEqual([item["task_id"] for item in visible], ["task:email:1"])
        all_events = self.store.list_recent_worker_activity(
            limit=10, include_internal=True
        )
        self.assertEqual(
            [item["task_id"] for item in all_events],
            ["task:internal:heartbeat:1", "task:email:1"],
        )

    def test_orders_by_occurred_at_not_insert_order(self) -> None:
        self.append("task:im:new", datetime(2026, 4, 27, 19, 0, tzinfo=timezone.utc))
        self.append("task:cron:old", datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc))

        visible = self.store.list_recent_worker_activity(limit=10)
        self.assertEqual(
            [item["task_id"] for item in visible], ["task:im:new", "task:cron:old"]
        )
        self.assertGreater(visible[1]["activity_id"], visible[0]["activity_id"])
