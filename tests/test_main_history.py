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


class MainHistoryCliTests(unittest.TestCase):
    def test_returns_ordered_window_around_telegram_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "worker.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"db_path": str(db_path)}), encoding="utf-8"
            )
            store = SQLiteStateStore(str(db_path))
            for number in (10, 11, 12):
                envelope = TaskEnvelope(
                    id=f"telegram:{number}",
                    source="im",
                    received_at=datetime(
                        2026, 8, 16, 11, number, tzinfo=timezone.utc
                    ),
                    actor="edward",
                    content=f"turn {number}",
                    attachments=[],
                    context_refs=[],
                    reply_channel=ReplyChannel(
                        type="telegram",
                        target="-123",
                        metadata={"message_id": number},
                    ),
                    dedupe_key=f"telegram:{number}",
                )
                store.stage_inbox_task(
                    TaskQueueMessage.from_envelope(
                        envelope, trace_id=f"trace:telegram:{number}"
                    )
                )

            stdout = io.StringIO()
            with (
                patch("sys.stdout", stdout),
                patch(
                    "sys.argv",
                    [
                        "main_history.py",
                        "--channel",
                        "telegram",
                        "--target",
                        "-123",
                        "--around-message-id",
                        "11",
                        "--before",
                        "1",
                        "--after",
                        "1",
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

            payload = json.loads(stdout.getvalue())
            self.assertEqual(exit_code, 0)
            self.assertTrue(payload["anchor_found"])
            self.assertEqual(
                [turn["message_id"] for turn in payload["turns"]], [10, 11, 12]
            )
            self.assertTrue(payload["turns"][1]["is_anchor"])


if __name__ == "__main__":
    unittest.main()
