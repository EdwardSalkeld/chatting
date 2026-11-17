import io
import http.client
import json
import tempfile
import unittest
import urllib.request
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.main_work_items import main
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.main import WORKER_CONFIG_PATH_ENV_VAR
from tests.test_egress_client import READ_TIMEOUT_SECONDS, egress_server


def _staged_task(db_path: Path) -> TaskQueueMessage:
    task = replace(
        TaskQueueMessage.from_envelope(
            TaskEnvelope(
                id="email:1",
                source="email",
                actor="alice",
                received_at=datetime.now(timezone.utc),
                content="Investigate this",
                attachments=[],
                context_refs=[],
                reply_channel=ReplyChannel(type="email", target="alice@example.com"),
                dedupe_key="email:1",
            ),
            trace_id="trace:1",
        ),
        work_item_id="item_1",
    )
    SQLiteStateStore(str(db_path)).stage_inbox_task(task)
    return task


class WorkerConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "worker.db"
        self.config = Path(tmp.name) / "worker.json"
        self.config.write_text(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "handler_egress_url": "http://127.0.0.1:9999/egress",
                }
            )
        )
        self.task = _staged_task(db_path)

    def _main(self, *args: str) -> str:
        output = io.StringIO()
        with (
            patch.dict("os.environ", {WORKER_CONFIG_PATH_ENV_VAR: str(self.config)}),
            patch("sys.argv", ["main_work_items.py", *args]),
            patch("sys.stdout", output),
        ):
            self.assertEqual(main(), 0)
        return output.getvalue()

    def test_ensure_topic_uses_handler_config_and_returns_reusable_destination(self):
        result = {
            "conversation": {
                "type": "telegram",
                "target": "-100",
                "metadata": {"message_thread_id": 42},
            }
        }
        with patch(
            "urllib.request.urlopen",
            return_value=io.BytesIO(json.dumps(result).encode()),
        ) as call:
            output = self._main(
                "ensure-telegram-topic",
                self.task.task_id,
                "--name",
                "Email work",
                "--chat-id",
                "-100",
            )
        request = call.call_args.args[0]
        self.assertEqual(
            request.full_url, "http://127.0.0.1:9999/work-items/telegram-topic"
        )
        self.assertEqual(
            json.loads(request.data),
            {
                "task_id": self.task.task_id,
                "chat_id": "-100",
                "name": "Email work",
                "topic_id": 0,
            },
        )
        self.assertEqual(json.loads(output), result)

    def test_register_pr_reads_settings_from_the_worker_config(self) -> None:
        pr_url = "https://github.com/o/r/pull/9"
        with patch("app.main_work_items.register_pr") as register:
            self._main("register-pr", self.task.task_id, pr_url)
        register.assert_called_once_with(
            "http://127.0.0.1:9999", task_id=self.task.task_id, pr_url=pr_url
        )


class HandlerTransportFailureTests(unittest.TestCase):
    def test_malformed_topic_response_reports_unknown_outcome(self) -> None:
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "worker.db"
            task = _staged_task(db_path)
            response = io.BytesIO(b"not json")
            argv = [
                "main_work_items.py",
                "--db",
                str(db_path),
                "ensure-telegram-topic",
                task.task_id,
                "--name",
                "Work",
            ]
            with (
                patch("sys.argv", argv),
                patch("urllib.request.urlopen", return_value=response),
                patch("sys.stderr", stderr),
                self.assertRaises(SystemExit) as raised,
            ):
                main()
        self.assertEqual(raised.exception.code, 2)
        self.assertIn("creation outcome may be unknown", stderr.getvalue())

    def test_unreadable_http_error_body_does_not_crash_cli(self) -> None:
        class BrokenBody(io.BytesIO):
            def read(self, *args):
                raise http.client.IncompleteRead(b"partial")

        for command in (
            ["ensure-telegram-topic", "--name", "Work"],
            ["register-pr", "https://github.com/o/r/pull/9"],
        ):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as tmp:
                db_path = Path(tmp) / "worker.db"
                task = _staged_task(db_path)
                error = urllib.error.HTTPError(
                    "http://handler", 400, "bad request", {}, BrokenBody()
                )
                argv = [
                    "main_work_items.py",
                    "--db",
                    str(db_path),
                    command[0],
                    task.task_id,
                    *command[1:],
                ]
                stderr = io.StringIO()
                with (
                    patch("sys.argv", argv),
                    patch("urllib.request.urlopen", side_effect=error),
                    patch("sys.stderr", stderr),
                    self.assertRaises(SystemExit) as raised,
                ):
                    main()
                self.assertEqual(raised.exception.code, 2)
                self.assertIn("HTTP 400 (response body unavailable)", stderr.getvalue())

    def _run(self, mode: str, command: list[str]) -> tuple[object, str]:
        real_urlopen = urllib.request.urlopen

        def short_urlopen(request, timeout):
            return real_urlopen(request, timeout=READ_TIMEOUT_SECONDS)

        stderr = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as tmp,
            egress_server(mode) as (url, received),
        ):
            db_path = Path(tmp) / "worker.db"
            task = _staged_task(db_path)
            argv = [
                "main_work_items.py",
                "--db",
                str(db_path),
                "--handler-url",
                url.removesuffix("/egress"),
                command[0],
                task.task_id,
                *command[1:],
            ]
            with (
                patch("sys.argv", argv),
                patch("urllib.request.urlopen", short_urlopen),
                patch("sys.stdout", io.StringIO()),
                patch("sys.stderr", stderr),
                self.assertRaises(SystemExit) as raised,
            ):
                main()
            self.assertEqual(len(received), 1)
        return raised.exception.code, stderr.getvalue()

    def test_ensure_topic_reports_unknown_outcome(self) -> None:
        for mode in ("stall", "close_mid_body"):
            with self.subTest(mode=mode):
                code, stderr = self._run(
                    mode,
                    ["ensure-telegram-topic", "--name", "Work", "--chat-id", "-100"],
                )
                self.assertEqual(code, 2)
                self.assertIn("creation outcome may be unknown", stderr)

    def test_register_reports_handler_unavailable(self) -> None:
        # register only checks the status line, so a truncated body is a
        # success there and is not covered.
        code, stderr = self._run(
            "close_before_response", ["register-pr", "https://github.com/o/r/pull/9"]
        )
        self.assertEqual(code, 2)
        self.assertIn("handler unavailable", stderr)
