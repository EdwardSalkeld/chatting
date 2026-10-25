import io
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.main_work_items import main
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.main import WORKER_CONFIG_PATH_ENV_VAR


class RegisterPRTests(unittest.TestCase):
    def test_register_pr_reads_settings_from_the_worker_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "worker.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(db_path),
                        "handler_egress_url": "http://127.0.0.1:9999/egress",
                    }
                )
            )
            envelope = TaskEnvelope(
                id="telegram:1",
                source="im",
                received_at=datetime(2026, 8, 16, 10, 0, tzinfo=timezone.utc),
                actor="alice",
                content="open a PR",
                attachments=[],
                context_refs=[],
                reply_channel=ReplyChannel(
                    type="telegram", target="-123", metadata={"message_id": 1}
                ),
                dedupe_key="telegram:1",
            )
            task = replace(
                TaskQueueMessage.from_envelope(envelope, trace_id="trace:1"),
                work_item_id="item_1",
            )
            SQLiteStateStore(str(db_path)).stage_inbox_task(task)

            with (
                patch.dict(
                    "os.environ", {WORKER_CONFIG_PATH_ENV_VAR: str(config_path)}
                ),
                patch(
                    "sys.argv",
                    [
                        "main_work_items.py",
                        "register-pr",
                        task.task_id,
                        "https://github.com/o/r/pull/9",
                    ],
                ),
                patch("app.main_work_items.register_pr") as register,
                patch("sys.stdout", io.StringIO()),
            ):
                self.assertEqual(main(), 0)

        register.assert_called_once_with(
            "http://127.0.0.1:9999",
            task_id=task.task_id,
            pr_url="https://github.com/o/r/pull/9",
        )


if __name__ == "__main__":
    unittest.main()
