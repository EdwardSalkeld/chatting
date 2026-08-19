import argparse
import json
import subprocess
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.models import PromptContext, ReplyChannel, TaskEnvelope
from app.worker.executor import CodexExecutor, GooseExecutor, UsageReporter
from app.worker.main import _build_executor

_HARNESS_RUN = "app.worker.executor.harness.subprocess.run"


def _envelope() -> TaskEnvelope:
    return TaskEnvelope(
        id="telegram:7",
        source="im",
        received_at=datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc),
        actor="8605042448:edsalkeld",
        content="Summarise today's notes",
        attachments=[],
        context_refs=["repo:/srv/chatting/workspace"],
        reply_channel=ReplyChannel(type="telegram", target="8605042448"),
        dedupe_key="telegram:7",
        prompt_context=PromptContext(global_instructions=["Keep replies concise."]),
    )


def _completed(returncode: int = 0, stdout: str = "", stderr: str = "") -> object:
    return subprocess.CompletedProcess(
        args=["goose"], returncode=returncode, stdout=stdout, stderr=stderr
    )


class GooseExecutorDefaultsTests(unittest.TestCase):
    def test_default_command_reads_the_payload_from_stdin(self) -> None:
        # `-i -` is what makes goose share Codex's stdin transport; without it the
        # payload would have to go via a temp file.
        command = GooseExecutor().command
        self.assertEqual(command[-2:], ("-i", "-"))
        self.assertIn("run", command)
        self.assertIn("--no-session", command)

    def test_default_command_bounds_runaway_loops(self) -> None:
        command = GooseExecutor().command
        self.assertIn("--max-turns", command)
        self.assertIn("--max-tool-repetitions", command)

    def test_default_command_enables_shell_and_file_tools(self) -> None:
        command = GooseExecutor().command
        self.assertIn("--with-builtin", command)
        self.assertIn("developer", command)


class GooseExecutorExecuteTests(unittest.TestCase):
    def test_sends_the_same_chatting_contract_as_codex(self) -> None:
        with patch(_HARNESS_RUN, return_value=_completed(stdout="{}")) as run_mock:
            GooseExecutor(command=("goose", "run")).execute(_envelope())

        payload = json.loads(run_mock.call_args.kwargs["input"])
        self.assertIn(
            "app.main_reply", payload["reply_contract"]["visible_replies_must_use"]
        )
        self.assertEqual(payload["task"]["source"], "im")
        self.assertEqual(payload["task"]["reply_channel"]["target"], "8605042448")

    def test_passes_cwd_and_env_through(self) -> None:
        with patch(_HARNESS_RUN, return_value=_completed()) as run_mock:
            GooseExecutor(
                command=("goose", "run"),
                cwd="/srv/chatting/workspace",
                env={"GOOSE_PROVIDER": "openrouter"},
            ).execute(_envelope())

        self.assertEqual(run_mock.call_args.kwargs["cwd"], "/srv/chatting/workspace")
        self.assertEqual(
            run_mock.call_args.kwargs["env"], {"GOOSE_PROVIDER": "openrouter"}
        )

    def test_success_captures_both_streams_as_transcript(self) -> None:
        with patch(_HARNESS_RUN, return_value=_completed(stdout="out", stderr="err")):
            result = GooseExecutor(command=("goose",)).execute(_envelope())

        self.assertEqual(result.errors, [])
        self.assertEqual(result.stdout, "out")
        self.assertEqual(result.stderr, "err")

    def test_goose_panic_exit_becomes_a_nonzero_executor_error(self) -> None:
        # goose reports failure as a Rust panic exiting 101 rather than a clean
        # error, so the contract rests on the status code alone.
        panic = (
            "thread 'main' panicked at crates/goose-cli/src/session/builder.rs:363:10"
        )
        with patch(_HARNESS_RUN, return_value=_completed(returncode=101, stderr=panic)):
            result = GooseExecutor(command=("goose",)).execute(_envelope())

        self.assertEqual(len(result.errors), 1)
        self.assertTrue(result.errors[0].startswith("executor_exit_nonzero:101:"))
        self.assertIn("panicked", result.errors[0])

    def test_timeout_reports_executor_timeout(self) -> None:
        with patch(
            _HARNESS_RUN,
            side_effect=subprocess.TimeoutExpired(cmd="goose", timeout=1),
        ):
            result = GooseExecutor(command=("goose",), timeout_seconds=1).execute(
                _envelope()
            )

        self.assertEqual(result.errors, ["executor_timeout"])

    def test_does_not_report_usage_yet(self) -> None:
        # /usage degrades to "this executor does not report usage" until the
        # OpenRouter key endpoint is wired up.
        self.assertNotIsInstance(GooseExecutor(), UsageReporter)


def _args(**overrides: object) -> argparse.Namespace:
    defaults: dict[str, object] = {
        "codex_command": None,
        "claude_command": None,
        "goose_command": None,
        "codex_working_dir": None,
        "config": None,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class BuildExecutorSelectionTests(unittest.TestCase):
    def test_goose_command_alone_selects_the_goose_executor(self) -> None:
        executor = _build_executor(
            _args(), {"goose_command": "goose run --no-session -i -"}
        )

        self.assertIsInstance(executor, GooseExecutor)
        assert isinstance(executor, GooseExecutor)
        self.assertEqual(executor.command, ("goose", "run", "--no-session", "-i", "-"))

    def test_codex_command_keeps_precedence_over_goose(self) -> None:
        # A deployment that already sets codex_command must not switch harness by
        # gaining a goose key.
        executor = _build_executor(
            _args(),
            {"codex_command": "codex exec", "goose_command": "goose run"},
        )

        self.assertIsInstance(executor, CodexExecutor)

    def test_claude_command_also_keeps_precedence_over_goose(self) -> None:
        executor = _build_executor(
            _args(),
            {"claude_command": "claude", "goose_command": "goose run"},
        )

        self.assertIsInstance(executor, CodexExecutor)

    def test_goose_working_dir_comes_from_the_shared_setting(self) -> None:
        executor = _build_executor(
            _args(),
            {
                "goose_command": "goose run",
                "codex_working_dir": "/srv/chatting/workspace",
            },
        )

        assert isinstance(executor, GooseExecutor)
        self.assertEqual(executor.cwd, "/srv/chatting/workspace")

    def test_blank_goose_command_is_rejected_by_config_validation(self) -> None:
        # Rejected upstream by _resolve_optional_str, in common with every other
        # string setting; asserted here so the goose path is covered by it too.
        with self.assertRaises(ValueError):
            _build_executor(_args(), {"goose_command": "   "})

    def test_no_command_at_all_still_defaults_to_codex(self) -> None:
        executor = _build_executor(_args(), {})

        self.assertIsInstance(executor, CodexExecutor)


if __name__ == "__main__":
    unittest.main()
