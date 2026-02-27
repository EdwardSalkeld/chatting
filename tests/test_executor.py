import json
import subprocess
import unittest
from unittest.mock import patch

from app.executor import CodexExecutor, parse_execution_result
from app.models import ExecutionConstraints, RoutedTask


def _task() -> RoutedTask:
    return RoutedTask(
        task_id="task_1",
        envelope_id="evt_1",
        workflow="respond_and_optionally_edit",
        priority="normal",
        execution_constraints=ExecutionConstraints(timeout_seconds=7, max_tokens=1000),
        policy_profile="default",
    )


class ParseExecutionResultTests(unittest.TestCase):
    def test_parse_execution_result_accepts_valid_payload(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [
                    {
                        "channel": "email",
                        "target": "alice@example.com",
                        "body": "Done.",
                    }
                ],
                "actions": [{"type": "write_file", "path": "docs/notes.md"}],
                "config_updates": [{"path": "routing.default_timeout", "value": 240}],
                "requires_human_review": False,
                "errors": [],
            }
        )

        result = parse_execution_result(payload)

        self.assertEqual(result.to_dict()["messages"][0]["channel"], "email")
        self.assertEqual(result.to_dict()["actions"][0]["type"], "write_file")
        self.assertEqual(result.to_dict()["config_updates"][0]["path"], "routing.default_timeout")

    def test_parse_execution_result_rejects_unknown_top_level_keys(self) -> None:
        payload = json.dumps(
            {
                "messages": [],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
                "unexpected": "value",
            }
        )

        with self.assertRaisesRegex(ValueError, "unknown_top_level_keys"):
            parse_execution_result(payload)


class CodexExecutorTests(unittest.TestCase):
    @patch("app.executor.codex.subprocess.run")
    def test_execute_returns_timeout_error_when_subprocess_times_out(self, run_mock) -> None:
        run_mock.side_effect = subprocess.TimeoutExpired(cmd=["codex"], timeout=7)
        executor = CodexExecutor(command=("codex", "exec", "--json"))

        result = executor.execute(_task())

        self.assertEqual(result.errors, ["executor_timeout"])
        self.assertEqual(result.messages, [])

    @patch("app.executor.codex.subprocess.run")
    def test_execute_returns_nonzero_exit_error(self, run_mock) -> None:
        run_mock.return_value = subprocess.CompletedProcess(
            args=["codex", "exec", "--json"],
            returncode=2,
            stdout="",
            stderr="fatal error",
        )
        executor = CodexExecutor(command=("codex", "exec", "--json"))

        result = executor.execute(_task())

        self.assertEqual(result.errors, ["executor_exit_nonzero:2:fatal error"])

    @patch("app.executor.codex.subprocess.run")
    def test_execute_uses_task_timeout_and_parses_stdout(self, run_mock) -> None:
        run_mock.return_value = subprocess.CompletedProcess(
            args=["codex", "exec", "--json"],
            returncode=0,
            stdout=json.dumps(
                {
                    "messages": [
                        {
                            "channel": "log",
                            "target": "evt_1",
                            "body": "ok",
                        }
                    ],
                    "actions": [],
                    "config_updates": [],
                    "requires_human_review": False,
                    "errors": [],
                }
            ),
            stderr="",
        )
        executor = CodexExecutor(command=("codex", "exec", "--json"))
        task = _task()

        result = executor.execute(task)

        self.assertEqual(result.errors, [])
        self.assertEqual(result.messages[0].body, "ok")
        run_mock.assert_called_once()
        self.assertEqual(run_mock.call_args.kwargs["timeout"], task.execution_constraints.timeout_seconds)


if __name__ == "__main__":
    unittest.main()
