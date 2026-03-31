import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.main_reply import main
from app.handler.runtime import TaskLedgerStore
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore


class _FakeBroker:
    def __init__(self, address: str):
        self.address = address
        self.ensured: list[str] = []
        self.published: list[tuple[str, dict[str, object]]] = []

    def ensure_queue(self, queue_name: str) -> None:
        self.ensured.append(queue_name)

    def publish_json(self, queue_name: str, payload: dict[str, object]) -> str:
        self.published.append((queue_name, payload))
        return "guid-1"
class MainReplyCliTests(unittest.TestCase):
    def test_main_reply_publishes_unsequenced_incremental_v2_payload(self) -> None:
        broker = _FakeBroker("127.0.0.1:9876")
        stdout = io.StringIO()
        with (
            patch("app.main_reply.BBMBQueueAdapter", return_value=broker),
            patch("sys.stdout", stdout),
            patch(
                "sys.argv",
                [
                    "main_reply.py",
                    "task:email:53",
                    "--message",
                    "working on it",
                    "--channel",
                    "email",
                    "--target",
                    "alice@example.com",
                    "--event-id",
                    "evt:custom:1",
                ],
            ),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(broker.ensured, ["chatting.egress.v1"])
        self.assertEqual(len(broker.published), 1)
        queue_name, payload = broker.published[0]
        self.assertEqual(queue_name, "chatting.egress.v1")
        self.assertEqual(payload["task_id"], "task:email:53")
        self.assertEqual(payload["envelope_id"], "email:53")
        self.assertEqual(payload["trace_id"], "trace:email:53")
        self.assertEqual(payload["event_id"], "evt:custom:1")
        self.assertEqual(payload["event_kind"], "incremental")
        self.assertEqual(payload["message_type"], "chatting.egress.v2")
        self.assertNotIn("sequence", payload)

        printed = json.loads(stdout.getvalue())
        self.assertEqual(printed["status"], "published")
        self.assertEqual(printed["guid"], "guid-1")
        self.assertIsNone(printed["sequence"])

    def test_main_reply_uses_bbmb_address_from_worker_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(json.dumps({"bbmb_address": "10.0.0.5:9999"}), encoding="utf-8")
            broker = _FakeBroker("placeholder")
            stdout = io.StringIO()
            with (
                patch("app.main_reply.BBMBQueueAdapter", return_value=broker) as broker_ctor,
                patch("sys.stdout", stdout),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "task:email:53",
                        "--message",
                        "working on it",
                        "--channel",
                        "email",
                        "--target",
                        "alice@example.com",
                        "--config",
                        str(config_path),
                    ],
                ),
                ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        broker_ctor.assert_called_once_with(address="10.0.0.5:9999")
        self.assertEqual(broker.ensured, ["chatting.egress.v1"])

    def test_main_reply_requires_envelope_id_for_non_prefixed_task_id(self) -> None:
        with patch(
            "sys.argv",
            [
                "main_reply.py",
                "email:53",
                "--message",
                "working on it",
                "--channel",
                "email",
                "--target",
                "alice@example.com",
            ],
        ):
            with self.assertRaisesRegex(ValueError, "envelope_id is required"):
                main()

    def test_main_reply_publishes_telegram_reaction_using_explicit_message_id(self) -> None:
        broker = _FakeBroker("127.0.0.1:9876")
        with (
            patch("app.main_reply.BBMBQueueAdapter", return_value=broker),
            patch(
                "sys.argv",
                [
                    "main_reply.py",
                    "task:telegram:53",
                    "--channel",
                    "telegram",
                    "--target",
                    "8605042448",
                    "--telegram-reaction",
                    "👍",
                    "--telegram-message-id",
                    "123",
                ],
            ),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        _, payload = broker.published[0]
        self.assertEqual(payload["message"]["channel"], "telegram_reaction")
        self.assertEqual(payload["message"]["body"], "👍")
        self.assertEqual(payload["message"]["metadata"], {"message_id": 123})

    def test_main_reply_publishes_telegram_reaction_using_task_ledger_message_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(json.dumps({"db_path": str(db_path)}), encoding="utf-8")
            ledger = TaskLedgerStore(str(db_path))
            envelope = TaskEnvelope(
                id="telegram:53",
                source="im",
                received_at=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
                actor="8605042448:edsalkeld",
                content="hello",
                attachments=[],
                context_refs=[],
                reply_channel=ReplyChannel(
                    type="telegram",
                    target="8605042448",
                    metadata={"message_id": 456},
                ),
                dedupe_key="telegram:53",
            )
            ledger.record_task(TaskQueueMessage.from_envelope(envelope, trace_id="trace:telegram:53"))

            broker = _FakeBroker("127.0.0.1:9876")
            with (
                patch("app.main_reply.BBMBQueueAdapter", return_value=broker),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "task:telegram:53",
                        "--channel",
                        "telegram",
                        "--target",
                        "8605042448",
                        "--telegram-reaction",
                        "👍",
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        _, payload = broker.published[0]
        self.assertEqual(payload["message"]["metadata"], {"message_id": 456})

    def test_main_reply_publishes_attachment_message(self) -> None:
        broker = _FakeBroker("127.0.0.1:9876")
        with tempfile.TemporaryDirectory() as tmpdir:
            attachment_path = Path(tmpdir) / "menu.pdf"
            attachment_path.write_bytes(b"%PDF-1.4\n")
            with (
                patch("app.main_reply.BBMBQueueAdapter", return_value=broker),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "task:telegram:53",
                        "--channel",
                        "telegram",
                        "--target",
                        "8605042448",
                        "--message",
                        "This week's menu",
                        "--attachment-path",
                        str(attachment_path),
                        "--attachment-name",
                        "menu.pdf",
                    ],
                ),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        _, payload = broker.published[0]
        self.assertEqual(payload["message"]["body"], "This week's menu")
        self.assertEqual(payload["message"]["attachment"]["name"], "menu.pdf")
        self.assertEqual(payload["message"]["attachment"]["uri"], attachment_path.as_uri())

    def test_main_reply_records_worker_activity_when_db_path_is_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(json.dumps({"db_path": str(db_path)}), encoding="utf-8")
            broker = _FakeBroker("127.0.0.1:9876")
            with (
                patch("app.main_reply.BBMBQueueAdapter", return_value=broker),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "task:email:53",
                        "--message",
                        "working on it",
                        "--channel",
                        "email",
                        "--target",
                        "alice@example.com",
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            activity = SQLiteStateStore(str(db_path)).list_recent_worker_activity(limit=5, include_internal=True)
            self.assertEqual(len(activity), 1)
            self.assertEqual(activity[0]["phase"], "egress_incremental")
            self.assertEqual(activity[0]["detail"]["publish_source"], "main_reply")


if __name__ == "__main__":
    unittest.main()
