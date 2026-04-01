import json
import subprocess
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.models import (
    AttachmentRef,
    ExecutionConstraints,
    PromptContext,
    ReplyChannel,
    RoutedTask,
)
from app.worker.executor import CodexExecutor


def _task() -> RoutedTask:
    return RoutedTask(
        task_id="task_1",
        envelope_id="evt_1",
        workflow="respond_and_optionally_edit",
        priority="normal",
        execution_constraints=ExecutionConstraints(timeout_seconds=7, max_tokens=1000),
        event_time=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
        source="email",
        actor="alice@example.com",
        content="Subject: hello\\n\\nPlease summarize this thread.",
        attachments=[
            AttachmentRef(uri="file:///tmp/evidence.png", name="evidence.png")
        ],
        prompt_context=PromptContext(
            global_instructions=["Keep replies concise."],
            reply_channel_instructions=["Use a short email subject line."],
        ),
        reply_channel=ReplyChannel(type="email", target="alice@example.com"),
    )


class CodexExecutorTests(unittest.TestCase):
    @patch("app.worker.executor.codex.subprocess.run")
    def test_execute_returns_timeout_error_when_subprocess_times_out(
        self, run_mock
    ) -> None:
        run_mock.side_effect = subprocess.TimeoutExpired(cmd=["codex"], timeout=7)
        executor = CodexExecutor(command=("codex", "exec", "--json"))

        result = executor.execute(_task())

        self.assertEqual(result.errors, ["executor_timeout"])
        self.assertEqual(result.actions, [])
        self.assertIsNone(result.stdout)
        self.assertIsNone(result.stderr)

    @patch("app.worker.executor.codex.subprocess.run")
    def test_execute_returns_nonzero_exit_error_and_captures_streams(
        self, run_mock
    ) -> None:
        run_mock.return_value = subprocess.CompletedProcess(
            args=["codex", "exec", "--json"],
            returncode=2,
            stdout="operator trace",
            stderr="fatal error",
        )
        executor = CodexExecutor(command=("codex", "exec", "--json"))

        result = executor.execute(_task())

        self.assertEqual(result.errors, ["executor_exit_nonzero:2:fatal error"])
        self.assertEqual(result.stdout, "operator trace")
        self.assertEqual(result.stderr, "fatal error")

    @patch("app.worker.executor.codex.subprocess.run")
    def test_execute_uses_task_timeout_and_captures_transcript(self, run_mock) -> None:
        run_mock.return_value = subprocess.CompletedProcess(
            args=["codex", "exec", "--json"],
            returncode=0,
            stdout='{"type":"message","text":"thinking"}\n',
            stderr="warning stream",
        )
        executor = CodexExecutor(
            command=("codex", "exec", "--json"),
            now_provider=lambda: datetime(2026, 2, 27, 18, 0, tzinfo=timezone.utc),
        )
        task = _task()

        result = executor.execute(task)

        self.assertEqual(result.errors, [])
        self.assertEqual(result.actions, [])
        self.assertEqual(result.stdout, '{"type":"message","text":"thinking"}\n')
        self.assertEqual(result.stderr, "warning stream")
        run_mock.assert_called_once()
        self.assertEqual(
            run_mock.call_args.kwargs["timeout"],
            task.execution_constraints.timeout_seconds,
        )
        payload = json.loads(run_mock.call_args.kwargs["input"])
        self.assertEqual(payload["task"]["event_time"], "2026-02-27T16:00:00Z")
        self.assertEqual(
            payload["task"]["attachments"],
            [{"uri": "file:///tmp/evidence.png", "name": "evidence.png"}],
        )
        self.assertEqual(
            payload["task"]["prompt_context"],
            {
                "global_instructions": ["Keep replies concise."],
                "source_instructions": [],
                "reply_channel_instructions": ["Use a short email subject line."],
                "task_instructions": [],
                "assembled_instructions": [
                    "Keep replies concise.",
                    "Use a short email subject line.",
                ],
            },
        )
        self.assertEqual(payload["current_time"], "2026-02-27T18:00:00Z")
        self.assertEqual(
            payload["reply_contract"]["visible_replies_must_use"],
            "python3 -m app.main_reply",
        )
        self.assertEqual(
            payload["reply_contract"]["executor_exit_status_drives_completion"], True
        )
        self.assertEqual(
            payload["reply_contract"]["executor_stdout_stderr_are_operator_transcript"],
            True,
        )

    @patch("app.worker.executor.codex.subprocess.run")
    def test_execute_passes_configured_working_directory(self, run_mock) -> None:
        run_mock.return_value = subprocess.CompletedProcess(
            args=["codex", "exec", "--json"],
            returncode=0,
            stdout="ok",
            stderr="",
        )
        executor = CodexExecutor(
            command=("codex", "exec", "--json"), cwd="/opt/chatting"
        )

        executor.execute(_task())

        run_mock.assert_called_once()
        self.assertEqual(run_mock.call_args.kwargs["cwd"], "/opt/chatting")


if __name__ == "__main__":
    unittest.main()
