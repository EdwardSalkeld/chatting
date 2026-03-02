import json
import subprocess
import unittest
from unittest.mock import patch

from app.executor import CodexExecutor, parse_execution_result
from app.models import ExecutionConstraints, ReplyChannel, RoutedTask


def _task() -> RoutedTask:
    return RoutedTask(
        task_id="task_1",
        envelope_id="evt_1",
        workflow="respond_and_optionally_edit",
        priority="normal",
        execution_constraints=ExecutionConstraints(timeout_seconds=7, max_tokens=1000),
        policy_profile="default",
        source="email",
        actor="alice@example.com",
        content="Subject: hello\\n\\nPlease summarize this thread.",
        reply_channel=ReplyChannel(type="email", target="alice@example.com"),
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
                "actions": [
                    {
                        "type": "write_file",
                        "path": "docs/notes.md",
                        "content": "hello",
                    }
                ],
                "config_updates": [{"path": "routing.default_timeout", "value": 240}],
                "requires_human_review": False,
                "errors": [],
            }
        )

        result = parse_execution_result(payload)

        self.assertEqual(result.to_dict()["messages"][0]["channel"], "email")
        self.assertEqual(result.to_dict()["actions"][0]["type"], "write_file")
        self.assertEqual(result.to_dict()["config_updates"][0]["path"], "routing.default_timeout")

    def test_parse_execution_result_recovers_last_json_object_from_mixed_output(self) -> None:
        valid_payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [
                    {
                        "channel": "email",
                        "target": "alice@example.com",
                        "body": "Done.",
                    }
                ],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )
        mixed_output = "starting codex\n" + valid_payload + "\ncompleted\n"

        result = parse_execution_result(mixed_output)

        self.assertEqual(result.to_dict()["messages"][0]["channel"], "email")

    def test_parse_execution_result_rejects_unknown_top_level_keys(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
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

    def test_parse_execution_result_rejects_unknown_message_keys(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [
                    {
                        "channel": "email",
                        "target": "alice@example.com",
                        "body": "Done.",
                        "priority": "high",
                    }
                ],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "unknown_message_keys"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_message_missing_required_field(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [
                    {
                        "channel": "email",
                        "target": "alice@example.com",
                    }
                ],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "message_body_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_message_body_with_only_whitespace(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [
                    {
                        "channel": "email",
                        "target": "alice@example.com",
                        "body": "   ",
                    }
                ],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "message_body_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_unknown_action_keys(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [
                    {
                        "type": "write_file",
                        "path": "docs/notes.md",
                        "mode": "append",
                    }
                ],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "unknown_action_keys"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_action_missing_required_field(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"path": "docs/notes.md"}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "action_type_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_action_type_with_only_whitespace(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "  "}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "action_type_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_write_file_action_missing_path(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "write_file", "content": "hello"}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "write_file_path_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_write_file_action_missing_content(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "write_file", "path": "docs/notes.md"}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "write_file_content_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_write_file_action_with_empty_path(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "write_file", "path": "", "content": "hello"}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "write_file_path_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_write_file_action_with_whitespace_path(
        self,
    ) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "write_file", "path": "   ", "content": "hello"}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "write_file_path_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_write_file_action_with_empty_content(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "write_file", "path": "docs/notes.md", "content": ""}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "write_file_content_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_write_file_action_with_whitespace_content(
        self,
    ) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "write_file", "path": "docs/notes.md", "content": "   "}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "write_file_content_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_non_write_file_action_with_path(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "run_shell", "path": "echo hi"}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "non_write_file_path_forbidden"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_non_write_file_action_with_content(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [{"type": "run_shell", "content": "echo hi"}],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "non_write_file_content_forbidden"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_unknown_config_update_keys(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [],
                "config_updates": [
                    {
                        "path": "routing.default_timeout",
                        "value": 240,
                        "source": "llm",
                    }
                ],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "unknown_config_update_keys"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_config_update_missing_required_path(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [],
                "config_updates": [{"value": 240}],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "config_update_path_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_config_update_path_with_only_whitespace(
        self,
    ) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [],
                "config_updates": [{"path": " ", "value": 240}],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "config_update_path_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_config_update_missing_required_value(
        self,
    ) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [],
                "config_updates": [{"path": "routing.default_timeout"}],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "config_update_value_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_missing_schema_version(self) -> None:
        payload = json.dumps(
            {
                "messages": [],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "missing_top_level_keys:schema_version"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_blank_schema_version(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "",
                "messages": [],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "schema_version_required"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_unsupported_schema_version(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "2.0",
                "messages": [],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [],
            }
        )

        with self.assertRaisesRegex(ValueError, "unsupported_schema_version:2.0"):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_error_item_with_empty_string(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": [""],
            }
        )

        with self.assertRaisesRegex(
            ValueError, "errors_items_must_be_non_empty_strings"
        ):
            parse_execution_result(payload)

    def test_parse_execution_result_rejects_error_item_with_only_whitespace(self) -> None:
        payload = json.dumps(
            {
                "schema_version": "1.0",
                "messages": [],
                "actions": [],
                "config_updates": [],
                "requires_human_review": False,
                "errors": ["   "],
            }
        )

        with self.assertRaisesRegex(
            ValueError, "errors_items_must_be_non_empty_strings"
        ):
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
                    "schema_version": "1.0",
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
