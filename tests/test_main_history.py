import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.main_history import main
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.main import WORKER_CONFIG_PATH_ENV_VAR


class MainHistoryCliTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "worker.db"
        self.config_path = Path(tmp.name) / "worker.json"
        self.config_path.write_text(json.dumps({"db_path": str(db_path)}))
        self.store = SQLiteStateStore(str(db_path))

    def stage(self, number: int, sender: str = "edward", topic: int | None = None):
        metadata = {"message_id": number}
        if topic is not None:
            metadata["message_thread_id"] = topic
        envelope = TaskEnvelope(
            id=f"telegram:{number}",
            source="im",
            received_at=datetime(2026, 8, 16, 11, number, tzinfo=timezone.utc),
            actor=sender,
            content=f"plan number {number}",
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(
                type="telegram", target="-123", metadata=metadata
            ),
            dedupe_key=f"telegram:{number}",
        )
        self.store.stage_inbox_task(
            TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:{number}")
        )

    def run_cli(self, *args: str) -> dict:
        stdout = io.StringIO()
        argv = ["main_history.py", "--channel", "telegram", "--target", "-123"]
        with (
            patch("sys.stdout", stdout),
            patch.dict(
                "os.environ", {WORKER_CONFIG_PATH_ENV_VAR: str(self.config_path)}
            ),
            patch("sys.argv", [*argv, *args]),
        ):
            self.assertEqual(main(), 0)
        return json.loads(stdout.getvalue())

    def test_returns_ordered_window_around_telegram_message(self) -> None:
        for number in (10, 11, 12):
            self.stage(number)
        payload = self.run_cli(
            "--around-message-id", "11", "--before", "1", "--after", "1"
        )
        self.assertTrue(payload["anchor_found"])
        self.assertEqual(
            [turn["message_id"] for turn in payload["turns"]], [10, 11, 12]
        )
        self.assertTrue(payload["turns"][1]["is_anchor"])

    def test_search_filters_sender_date_and_topic(self) -> None:
        for number, topic, sender in (
            (10, 7, "alice"),
            (11, 8, "alice"),
            (12, 7, "bob"),
            (13, 7, "alice"),
        ):
            self.stage(number, sender, topic)
        payload = self.run_cli(
            "--topic-id",
            "7",
            "--query",
            "plan",
            "--sender",
            "alice",
            "--since",
            "2026-08-16T11:11:00Z",
        )
        self.assertEqual([turn["message_id"] for turn in payload["turns"]], [13])
