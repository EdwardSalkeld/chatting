import json
import subprocess
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.models import AttachmentRef, PromptContext, ReplyChannel, TaskEnvelope
from app.worker.executor import CodexExecutor


def _envelope() -> TaskEnvelope:
    return TaskEnvelope(
        id="evt_1",
        source="email",
        received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
        actor="alice@example.com",
        content="Subject: hello\n\nPlease summarize this thread.",
        attachments=[
            AttachmentRef(uri="file:///tmp/evidence.png", name="evidence.png")
        ],
        context_refs=["repo:/workspace/chatting"],
        reply_channel=ReplyChannel(type="email", target="alice@example.com"),
        dedupe_key="evt_1",
        prompt_context=PromptContext(
            global_instructions=["Keep replies concise."],
            reply_channel_instructions=["Use a short email subject line."],
        ),
    )


class CodexExecutorTests(unittest.TestCase):
    def test_execute_returns_stdout_and_stderr_on_success(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["codex"],
            returncode=0,
            stdout='{"status":"ok"}',
            stderr="operator log",
        )
        with patch("app.worker.executor.codex.subprocess.run", return_value=completed):
            result = CodexExecutor(command=("codex",)).execute(_envelope())

        self.assertEqual(result.errors, [])
        self.assertEqual(result.stdout, '{"status":"ok"}')
        self.assertEqual(result.stderr, "operator log")

    def test_execute_returns_timeout_error(self) -> None:
        with patch(
            "app.worker.executor.codex.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["codex"], timeout=1),
        ):
            result = CodexExecutor(command=("codex",), timeout_seconds=1).execute(
                _envelope()
            )

        self.assertEqual(result.errors, ["executor_timeout"])

    def test_execute_returns_nonzero_error_with_stderr(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["codex"],
            returncode=7,
            stdout="",
            stderr="boom",
        )
        with patch("app.worker.executor.codex.subprocess.run", return_value=completed):
            result = CodexExecutor(command=("codex",)).execute(_envelope())

        self.assertEqual(result.errors, ["executor_exit_nonzero:7:boom"])

    def test_execute_sends_expected_task_payload(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["codex"],
            returncode=0,
            stdout="{}",
            stderr="",
        )
        with patch(
            "app.worker.executor.codex.subprocess.run",
            return_value=completed,
        ) as run_mock:
            executor = CodexExecutor(
                command=("codex", "exec"),
                cwd="/workspace/chatting",
                env={"TOKEN": "secret"},
                timeout_seconds=123,
                now_provider=lambda: datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc),
            )
            executor.execute(_envelope())

        payload = json.loads(run_mock.call_args.kwargs["input"])
        self.assertEqual(payload["schema_version"], "1.0")
        self.assertEqual(payload["task"]["task_id"], "task:evt_1")
        self.assertEqual(payload["task"]["reply_channel"]["type"], "email")
        self.assertEqual(
            payload["reply_contract"]["visible_replies_must_use"],
            "python3 -m app.main_reply",
        )
        self.assertEqual(run_mock.call_args.kwargs["cwd"], "/workspace/chatting")
        self.assertEqual(run_mock.call_args.kwargs["env"], {"TOKEN": "secret"})
        self.assertEqual(run_mock.call_args.kwargs["timeout"], 123)


if __name__ == "__main__":
    unittest.main()
