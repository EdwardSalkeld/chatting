import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.main_reply import (
    DEFAULT_HANDLER_EGRESS_URL,
    EXIT_DROPPED,
    EXIT_TRANSIENT,
    main,
)
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.task_ledger import TaskLedgerStore


class _FakeSubmit:
    """Stand-in for main_reply._submit_egress: records calls, returns a canned
    (status_code, response) so tests don't do real HTTP."""

    def __init__(
        self, status: int = 200, response: dict[str, object] | None = None
    ) -> None:
        self.status = status
        self.response = (
            response if response is not None else {"status": "dispatched", "reason": ""}
        )
        self.calls: list[tuple[str, dict[str, object]]] = []

    def __call__(
        self, url: str, payload: dict[str, object]
    ) -> tuple[int, dict[str, object]]:
        self.calls.append((url, payload))
        return self.status, dict(self.response)


class MainReplyCliTests(unittest.TestCase):
    def test_main_reply_posts_unsequenced_incremental_v2_payload(self) -> None:
        submit = _FakeSubmit()
        stdout = io.StringIO()
        with (
            patch("app.main_reply._submit_egress", submit),
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
        self.assertEqual(len(submit.calls), 1)
        url, payload = submit.calls[0]
        self.assertEqual(url, DEFAULT_HANDLER_EGRESS_URL)
        self.assertEqual(payload["task_id"], "task:email:53")
        self.assertEqual(payload["envelope_id"], "email:53")
        self.assertEqual(payload["trace_id"], "trace:email:53")
        self.assertEqual(payload["event_id"], "evt:custom:1")
        self.assertEqual(payload["event_kind"], "incremental")
        self.assertEqual(payload["message_type"], "chatting.egress.v2")
        self.assertNotIn("sequence", payload)

        printed = json.loads(stdout.getvalue())
        self.assertEqual(printed["status"], "dispatched")
        self.assertEqual(printed["http_status"], 200)
        self.assertIsNone(printed["sequence"])

    def test_main_reply_uses_handler_egress_url_from_worker_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"handler_egress_url": "http://10.0.0.5:9999/egress"}),
                encoding="utf-8",
            )
            submit = _FakeSubmit()
            stdout = io.StringIO()
            with (
                patch("app.main_reply._submit_egress", submit),
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
        self.assertEqual(submit.calls[0][0], "http://10.0.0.5:9999/egress")

    def test_main_reply_returns_dropped_exit_and_stays_quiet_in_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"db_path": str(db_path)}), encoding="utf-8"
            )
            submit = _FakeSubmit(
                status=422,
                response={"status": "dropped", "reason": "telegram_attachment_missing"},
            )
            stderr = io.StringIO()
            with (
                patch("app.main_reply._submit_egress", submit),
                patch("sys.stderr", stderr),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "task:telegram:53",
                        "--message",
                        "here is the screenshot",
                        "--channel",
                        "telegram",
                        "--target",
                        "8605042448",
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

            activity = SQLiteStateStore(str(db_path)).list_recent_worker_activity(
                limit=10,
                include_internal=True,
            )

        self.assertEqual(exit_code, EXIT_DROPPED)
        printed = json.loads(stderr.getvalue())
        self.assertEqual(printed["status"], "dropped")
        self.assertEqual(printed["reason"], "telegram_attachment_missing")
        # A drop is not a delivery, so it must not be recorded as one — the
        # supervised recovery loop keys off these activity rows.
        self.assertEqual(activity, [])

    def test_main_reply_returns_transient_exit_when_handler_unreachable(self) -> None:
        submit = _FakeSubmit(status=0, response={"reason": "egress endpoint unreachable"})
        stderr = io.StringIO()
        with (
            patch("app.main_reply._submit_egress", submit),
            patch("sys.stderr", stderr),
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
                ],
            ),
        ):
            exit_code = main()

        self.assertEqual(exit_code, EXIT_TRANSIENT)

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

    def test_main_reply_posts_telegram_reaction_using_explicit_message_id(
        self,
    ) -> None:
        submit = _FakeSubmit()
        with (
            patch("app.main_reply._submit_egress", submit),
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
        _, payload = submit.calls[0]
        self.assertEqual(payload["message"]["channel"], "telegram_reaction")
        self.assertEqual(payload["message"]["body"], "👍")
        self.assertEqual(payload["message"]["metadata"], {"message_id": 123})

    def test_main_reply_posts_telegram_reaction_using_task_ledger_message_id(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"db_path": str(db_path)}), encoding="utf-8"
            )
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
            ledger.record_task(
                TaskQueueMessage.from_envelope(envelope, trace_id="trace:telegram:53")
            )

            submit = _FakeSubmit()
            with (
                patch("app.main_reply._submit_egress", submit),
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
        _, payload = submit.calls[0]
        self.assertEqual(payload["message"]["metadata"], {"message_id": 456})

    def test_main_reply_posts_attachment_message(self) -> None:
        submit = _FakeSubmit()
        with tempfile.TemporaryDirectory() as tmpdir:
            attachment_path = Path(tmpdir) / "menu.pdf"
            attachment_path.write_bytes(b"%PDF-1.4\n")
            with (
                patch("app.main_reply._submit_egress", submit),
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
        _, payload = submit.calls[0]
        self.assertEqual(payload["message"]["body"], "This week's menu")
        self.assertEqual(payload["message"]["attachment"]["name"], "menu.pdf")
        self.assertEqual(
            payload["message"]["attachment"]["uri"], attachment_path.as_uri()
        )

    def test_main_reply_records_worker_activity_on_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"db_path": str(db_path)}), encoding="utf-8"
            )
            submit = _FakeSubmit()
            with (
                patch("app.main_reply._submit_egress", submit),
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
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

            activity = SQLiteStateStore(str(db_path)).list_recent_worker_activity(
                limit=10,
                include_internal=True,
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(activity), 1)
        self.assertEqual(activity[0]["task_id"], "task:email:53")
        self.assertEqual(activity[0]["phase"], "egress_incremental")
        self.assertEqual(activity[0]["summary"], "incremental egress to email")
        self.assertEqual(activity[0]["detail"]["event_id"], "evt:custom:1")


if __name__ == "__main__":
    unittest.main()
