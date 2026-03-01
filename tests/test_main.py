import json
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from app.executor import StubExecutor
from app.models import RoutedTask, TaskEnvelope
from app.main import main, run_bootstrap, run_live
from app.connectors import EmailMessage, FakeEmailConnector
from app.state import SQLiteStateStore


@dataclass
class FlakyExecutor:
    """Fail once for a task ID, then delegate to the default stub executor."""

    fail_task_id: str
    _stub: StubExecutor = field(default_factory=StubExecutor)

    def __post_init__(self) -> None:
        self._attempts: dict[str, int] = {}

    def execute(self, task: RoutedTask):
        seen = self._attempts.get(task.task_id, 0) + 1
        self._attempts[task.task_id] = seen
        if task.task_id == self.fail_task_id and seen == 1:
            raise RuntimeError("transient executor failure")
        return self._stub.execute(task)


@dataclass(frozen=True)
class AlwaysFailExecutor:
    """Executor used to test retry exhaustion and dead-letter recording."""

    error_message: str = "persistent executor failure"

    def execute(self, task: RoutedTask):
        raise RuntimeError(self.error_message)


class MainBootstrapFlowTests(unittest.TestCase):
    def test_run_bootstrap_processes_unique_events_and_records_blocked_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")

            output_buffer = StringIO()
            with redirect_stdout(output_buffer):
                runs = run_bootstrap(db_path)

            self.assertEqual([run.source for run in runs], ["cron", "email", "email", "email"])
            self.assertEqual(
                [run.envelope_id for run in runs],
                [
                    "cron:daily-summary:2026-02-27T09:00:00+00:00",
                    "email:ok-1",
                    "email:blocked-1",
                    "email:ok-1",
                ],
            )
            self.assertEqual(
                [run.result_status for run in runs],
                ["success", "success", "blocked_action", "duplicate_skipped"],
            )

            audit_events = SQLiteStateStore(db_path).list_audit_events()
            self.assertEqual(len(audit_events), len(runs))
            self.assertEqual(
                [event.run_id for event in audit_events],
                [run.run_id for run in runs],
            )
            self.assertEqual(
                [event.result_status for event in audit_events],
                [run.result_status for run in runs],
            )
            first_success_event = next(event for event in audit_events if event.run_id == "run:cron:daily-summary:2026-02-27T09:00:00+00:00")
            self.assertEqual(
                first_success_event.detail["trace_id"],
                "trace:cron:daily-summary:2026-02-27T09:00:00+00:00",
            )
            self.assertEqual(first_success_event.detail["applied_action_count"], 0)
            self.assertEqual(first_success_event.detail["skipped_action_count"], 0)
            self.assertEqual(first_success_event.detail["dispatched_message_count"], 1)
            self.assertEqual(first_success_event.detail["apply_reason_codes"], [])
            self.assertEqual(
                first_success_event.detail["execution_summary"],
                {
                    "message_count": 1,
                    "action_count": 0,
                    "config_update_count": 0,
                    "error_count": 0,
                    "action_types": [],
                    "requires_human_review": False,
                },
            )
            self.assertEqual(
                first_success_event.detail["execution_result"],
                {
                    "schema_version": "1.0",
                    "messages": [
                        {
                            "channel": "log",
                            "target": "daily-summary",
                            "body": "Handled workflow scheduled_automation",
                        }
                    ],
                    "actions": [],
                    "config_updates": [],
                    "requires_human_review": False,
                    "errors": [],
                },
            )
            self.assertEqual(first_success_event.detail["policy_decision"]["approved_actions"], [])
            self.assertEqual(first_success_event.detail["policy_decision"]["blocked_actions"], [])
            self.assertEqual(first_success_event.detail["apply_result"]["applied_actions"], [])
            self.assertEqual(first_success_event.detail["apply_result"]["skipped_actions"], [])
            self.assertEqual(len(first_success_event.detail["apply_result"]["dispatched_messages"]), 1)

            blocked_event = next(event for event in audit_events if event.run_id == "run:email:blocked-1")
            self.assertEqual(blocked_event.detail["applied_action_count"], 0)
            self.assertEqual(blocked_event.detail["skipped_action_count"], 0)
            self.assertEqual(blocked_event.detail["dispatched_message_count"], 1)
            self.assertEqual(blocked_event.detail["apply_reason_codes"], ["policy_blocked_actions_present"])
            self.assertEqual(
                blocked_event.detail["execution_result"]["actions"],
                [{"type": "run_shell", "path": "echo blocked"}],
            )
            self.assertEqual(
                blocked_event.detail["policy_decision"]["blocked_actions"],
                [{"type": "run_shell", "path": "echo blocked"}],
            )
            self.assertEqual(
                blocked_event.detail["apply_result"]["dispatched_messages"][0]["target"],
                "bob@example.com",
            )
            duplicate_event = next(
                event for event in audit_events if event.result_status == "duplicate_skipped"
            )
            self.assertEqual(duplicate_event.envelope_id, "email:ok-1")
            self.assertEqual(duplicate_event.workflow, "duplicate_skip")
            self.assertEqual(duplicate_event.detail["reason_codes"], ["duplicate_dedupe_key"])
            self.assertEqual(duplicate_event.detail["dedupe_key"], "email:ok-1")
            self.assertEqual(duplicate_event.detail["attempt_count"], 0)
            self.assertEqual(duplicate_event.detail["last_error"], None)

            observed_lines = [
                line
                for line in output_buffer.getvalue().splitlines()
                if line.startswith("run_observed ")
            ]
            self.assertEqual(len(observed_lines), 4)
            for line in observed_lines:
                self.assertIn("trace_id=", line)
                self.assertIn("run_id=", line)
                self.assertIn("envelope_id=", line)
                self.assertIn("source=", line)
                self.assertIn("workflow=", line)
                self.assertIn("policy_profile=", line)
                self.assertIn("latency_ms=", line)
                self.assertIn("result_status=", line)

    def test_run_bootstrap_retries_transient_executor_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            output_buffer = StringIO()
            with redirect_stdout(output_buffer):
                runs = run_bootstrap(
                    db_path,
                    executor=FlakyExecutor(fail_task_id="task:email:ok-1"),
                    max_attempts=2,
                )

            self.assertEqual(
                [run.result_status for run in runs],
                ["success", "success", "blocked_action", "duplicate_skipped"],
            )
            self.assertIn(
                "retry_scheduled trace_id=trace:email:ok-1 run_id=run:email:ok-1 attempt=1 next_attempt=2 max_attempts=2",
                output_buffer.getvalue(),
            )

            audit_events = SQLiteStateStore(db_path).list_audit_events()
            target_event = next(event for event in audit_events if event.run_id == "run:email:ok-1")
            self.assertEqual(target_event.detail["trace_id"], "trace:email:ok-1")
            self.assertEqual(target_event.detail["attempt_count"], 2)
            self.assertEqual(target_event.detail["reason_codes"], [])
            self.assertEqual(target_event.detail["applied_action_count"], 0)
            self.assertEqual(target_event.detail["skipped_action_count"], 0)
            self.assertEqual(target_event.detail["dispatched_message_count"], 1)
            self.assertEqual(target_event.detail["apply_reason_codes"], [])
            self.assertEqual(
                target_event.detail["execution_summary"],
                {
                    "message_count": 1,
                    "action_count": 0,
                    "config_update_count": 0,
                    "error_count": 0,
                    "action_types": [],
                    "requires_human_review": False,
                },
            )
            self.assertEqual(target_event.detail["execution_result"]["schema_version"], "1.0")
            self.assertEqual(target_event.detail["policy_decision"]["schema_version"], "1.0")
            self.assertEqual(target_event.detail["apply_result"]["schema_version"], "1.0")
            self.assertIsNotNone(target_event.detail["last_error"])

    def test_run_bootstrap_marks_dead_letter_when_retries_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            envelope = _single_email_envelope()

            output_buffer = StringIO()
            with redirect_stdout(output_buffer):
                runs = run_bootstrap(
                    db_path,
                    envelopes=[envelope],
                    executor=AlwaysFailExecutor(),
                    max_attempts=2,
                )

            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0].result_status, "dead_letter")
            self.assertIn(
                "dead_letter trace_id=trace:email:dlq-1 run_id=run:email:dlq-1 attempts=2 max_attempts=2",
                output_buffer.getvalue(),
            )

            audit_events = SQLiteStateStore(db_path).list_audit_events()
            self.assertEqual(len(audit_events), 1)
            self.assertEqual(audit_events[0].result_status, "dead_letter")
            self.assertEqual(audit_events[0].detail["trace_id"], "trace:email:dlq-1")
            self.assertEqual(audit_events[0].detail["reason_codes"], ["retry_exhausted"])
            self.assertEqual(audit_events[0].detail["attempt_count"], 2)
            self.assertEqual(audit_events[0].detail["applied_action_count"], 0)
            self.assertEqual(audit_events[0].detail["skipped_action_count"], 0)
            self.assertEqual(audit_events[0].detail["dispatched_message_count"], 0)
            self.assertEqual(audit_events[0].detail["apply_reason_codes"], [])
            self.assertEqual(
                audit_events[0].detail["execution_summary"],
                {
                    "message_count": 0,
                    "action_count": 0,
                    "config_update_count": 0,
                    "error_count": 0,
                    "action_types": [],
                    "requires_human_review": False,
                },
            )
            self.assertIsNone(audit_events[0].detail["execution_result"])
            self.assertIsNone(audit_events[0].detail["policy_decision"])
            self.assertIsNone(audit_events[0].detail["apply_result"])
            self.assertIn("RuntimeError", audit_events[0].detail["last_error"])


@dataclass
class OneShotConnector:
    envelopes: list[TaskEnvelope]
    _consumed: bool = False

    def poll(self) -> list[TaskEnvelope]:
        if self._consumed:
            return []
        self._consumed = True
        return list(self.envelopes)


class MainLiveFlowTests(unittest.TestCase):
    def test_run_live_processes_connector_envelopes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            envelope = _single_email_envelope()

            runs = run_live(
                db_path,
                connectors=[OneShotConnector(envelopes=[envelope])],
                executor=StubExecutor(),
                max_loops=1,
                max_attempts=2,
                poll_interval_seconds=0.1,
                base_dir=tmpdir,
            )

            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0].source, "email")
            self.assertEqual(runs[0].result_status, "success")


class MainCliTests(unittest.TestCase):
    @patch("app.main.run_bootstrap")
    def test_main_passes_configured_max_attempts(self, run_bootstrap_mock) -> None:
        run_bootstrap_mock.return_value = []
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            with patch(
                "sys.argv",
                ["app.main", "--db-path", db_path, "--max-attempts", "4"],
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        run_bootstrap_mock.assert_called_once_with(db_path, max_attempts=4)

    @patch("app.main.run_bootstrap")
    def test_main_list_runs_outputs_json_and_skips_bootstrap(self, run_bootstrap_mock) -> None:
        run_bootstrap_mock.return_value = []
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            run_bootstrap(db_path)

            output_buffer = StringIO()
            with (
                patch(
                    "sys.argv",
                    ["app.main", "--db-path", db_path, "--list-runs", "--limit", "2"],
                ),
                redirect_stdout(output_buffer),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        run_bootstrap_mock.assert_not_called()
        listed_runs = json.loads(output_buffer.getvalue().strip())
        self.assertEqual(len(listed_runs), 2)
        self.assertEqual(
            [record["result_status"] for record in listed_runs],
            ["blocked_action", "duplicate_skipped"],
        )

    @patch("app.main.run_bootstrap")
    def test_main_list_audit_events_outputs_json_and_skips_bootstrap(
        self, run_bootstrap_mock
    ) -> None:
        run_bootstrap_mock.return_value = []
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            run_bootstrap(db_path)

            output_buffer = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--list-audit-events",
                        "--limit",
                        "2",
                    ],
                ),
                redirect_stdout(output_buffer),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        run_bootstrap_mock.assert_not_called()
        listed_events = json.loads(output_buffer.getvalue().strip())
        self.assertEqual(len(listed_events), 2)
        self.assertEqual(
            [event["result_status"] for event in listed_events],
            ["blocked_action", "duplicate_skipped"],
        )

    def test_main_list_runs_supports_result_status_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            run_bootstrap(db_path)

            output_buffer = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--list-runs",
                        "--result-status",
                        "duplicate_skipped",
                    ],
                ),
                redirect_stdout(output_buffer),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        listed_runs = json.loads(output_buffer.getvalue().strip())
        self.assertEqual(len(listed_runs), 1)
        self.assertEqual(listed_runs[0]["result_status"], "duplicate_skipped")

    def test_main_list_audit_events_supports_result_status_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            run_bootstrap(db_path)

            output_buffer = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--list-audit-events",
                        "--result-status",
                        "duplicate_skipped",
                    ],
                ),
                redirect_stdout(output_buffer),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        listed_events = json.loads(output_buffer.getvalue().strip())
        self.assertEqual(len(listed_events), 1)
        self.assertEqual(listed_events[0]["result_status"], "duplicate_skipped")

    def test_main_rejects_combined_list_query_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--db-path",
                    db_path,
                    "--list-runs",
                    "--list-audit-events",
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError, "--list-runs cannot be combined with --list-audit-events"
                ):
                    main()

    def test_main_rejects_list_audit_events_with_run_live(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--db-path",
                    db_path,
                    "--list-audit-events",
                    "--run-live",
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "--list-runs/--list-audit-events cannot be combined with --run-live",
                ):
                    main()

    def test_main_rejects_whitespace_only_result_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--db-path",
                    db_path,
                    "--list-runs",
                    "--result-status",
                    "   ",
                ],
            ):
                with self.assertRaisesRegex(ValueError, "result_status must not be empty"):
                    main()

    def test_main_rejects_non_positive_max_attempts(self) -> None:
        with patch("sys.argv", ["app.main", "--max-attempts", "0"]):
            with self.assertRaises(SystemExit) as context:
                main()

        self.assertEqual(context.exception.code, 2)

    def test_main_rejects_whitespace_only_cli_db_path(self) -> None:
        with patch("sys.argv", ["app.main", "--db-path", "   "]):
            with self.assertRaisesRegex(ValueError, "db_path must not be empty"):
                main()

    def test_main_rejects_whitespace_only_config_db_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "bootstrap-config.json"
            config_path.write_text(json.dumps({"db_path": "   "}), encoding="utf-8")
            with patch("sys.argv", ["app.main", "--config", str(config_path)]):
                with self.assertRaisesRegex(ValueError, "config db_path must not be empty"):
                    main()

    def test_main_rejects_unknown_config_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "bootstrap-config.json"
            config_path.write_text(
                json.dumps({"db_path": str(Path(tmpdir) / "state.db"), "unexpected_key": True}),
                encoding="utf-8",
            )
            with patch("sys.argv", ["app.main", "--config", str(config_path)]):
                with self.assertRaisesRegex(
                    ValueError, r"config contains unknown keys: unexpected_key"
                ):
                    main()

    @patch("app.main.run_bootstrap")
    def test_main_reads_config_path_from_environment(self, run_bootstrap_mock) -> None:
        run_bootstrap_mock.return_value = []
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state-from-env.db")
            config_path = Path(tmpdir) / "bootstrap-config.json"
            config_path.write_text(
                json.dumps({"db_path": db_path, "max_attempts": 3}),
                encoding="utf-8",
            )
            with (
                patch.dict("os.environ", {"CHATTING_CONFIG_PATH": str(config_path)}, clear=False),
                patch("sys.argv", ["app.main"]),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        run_bootstrap_mock.assert_called_once_with(db_path, max_attempts=3)

    @patch("app.main.run_bootstrap")
    def test_main_cli_config_overrides_environment_config_path(self, run_bootstrap_mock) -> None:
        run_bootstrap_mock.return_value = []
        with tempfile.TemporaryDirectory() as tmpdir:
            env_config_path = Path(tmpdir) / "env-config.json"
            env_config_path.write_text(
                json.dumps({"db_path": str(Path(tmpdir) / "state-from-env.db"), "max_attempts": 3}),
                encoding="utf-8",
            )
            cli_db_path = str(Path(tmpdir) / "state-from-cli-config.db")
            cli_config_path = Path(tmpdir) / "cli-config.json"
            cli_config_path.write_text(
                json.dumps({"db_path": cli_db_path, "max_attempts": 4}),
                encoding="utf-8",
            )
            with (
                patch.dict("os.environ", {"CHATTING_CONFIG_PATH": str(env_config_path)}, clear=False),
                patch("sys.argv", ["app.main", "--config", str(cli_config_path)]),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        run_bootstrap_mock.assert_called_once_with(cli_db_path, max_attempts=4)

    def test_main_rejects_whitespace_only_environment_config_path(self) -> None:
        with (
            patch.dict("os.environ", {"CHATTING_CONFIG_PATH": "   "}, clear=False),
            patch("sys.argv", ["app.main"]),
        ):
            with self.assertRaisesRegex(ValueError, "CHATTING_CONFIG_PATH must not be empty"):
                main()

    def test_main_rejects_unknown_keys_from_environment_config_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "bootstrap-config.json"
            config_path.write_text(
                json.dumps({"db_path": str(Path(tmpdir) / "state.db"), "bad": "value"}),
                encoding="utf-8",
            )
            with (
                patch.dict("os.environ", {"CHATTING_CONFIG_PATH": str(config_path)}, clear=False),
                patch("sys.argv", ["app.main"]),
            ):
                with self.assertRaisesRegex(ValueError, r"config contains unknown keys: bad"):
                    main()

    def test_main_rejects_boolean_max_attempts_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "bootstrap-config.json"
            config_path.write_text(
                json.dumps({"db_path": str(Path(tmpdir) / "state.db"), "max_attempts": True}),
                encoding="utf-8",
            )
            with patch("sys.argv", ["app.main", "--config", str(config_path)]):
                with self.assertRaisesRegex(ValueError, "config max_attempts must be an integer"):
                    main()

    def test_main_rejects_boolean_poll_interval_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "live-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(Path(tmpdir) / "state.db"),
                        "schedule_file": str(Path(tmpdir) / "schedule.json"),
                        "poll_interval_seconds": True,
                    }
                ),
                encoding="utf-8",
            )
            schedule_file = Path(tmpdir) / "schedule.json"
            schedule_file.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "heartbeat",
                            "content": "ping",
                            "interval_seconds": 60,
                            "context_refs": ["repo:/home/edward/chatting"],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with patch("sys.argv", ["app.main", "--run-live", "--config", str(config_path)]):
                with self.assertRaisesRegex(
                    ValueError, "config poll_interval_seconds must be numeric"
                ):
                    main()

    def test_main_run_live_with_imap_requires_smtp_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--run-live",
                    "--db-path",
                    db_path,
                    "--imap-host",
                    "imap.example.com",
                    "--imap-username",
                    "bot@example.com",
                    "--max-loops",
                    "1",
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError, "--smtp-host is required when --imap-host is set"
                ):
                    main()

    def test_main_run_live_with_config_imap_requires_smtp_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "live-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "imap_host": "imap.example.com",
                        "imap_username": "bot@example.com",
                        "max_loops": 1,
                    }
                ),
                encoding="utf-8",
            )
            with patch("sys.argv", ["app.main", "--run-live", "--config", str(config_path)]):
                with self.assertRaisesRegex(
                    ValueError, "--smtp-host is required when --imap-host is set"
                ):
                    main()

    def test_main_run_live_rejects_blank_context_refs_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "live-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "imap_host": "imap.example.com",
                        "imap_username": "bot@example.com",
                        "smtp_host": "smtp.example.com",
                        "context_refs": ["   "],
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.dict("os.environ", {"CHATTING_IMAP_PASSWORD": "secret"}, clear=False),
                patch("sys.argv", ["app.main", "--run-live", "--config", str(config_path)]),
            ):
                with self.assertRaisesRegex(
                    ValueError, "context_ref/context_refs entries must not be empty"
                ):
                    main()

    @patch("app.main._build_codex_executor")
    @patch("app.main._build_email_sender")
    @patch("app.main._build_live_connectors")
    @patch("app.main.run_live")
    def test_main_run_live_mode_uses_live_flow(
        self,
        run_live_mock,
        connectors_mock,
        email_sender_mock,
        executor_mock,
    ) -> None:
        run_live_mock.return_value = []
        connectors_mock.return_value = []
        email_sender_mock.return_value = None
        executor_mock.return_value = StubExecutor()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--run-live",
                    "--db-path",
                    db_path,
                    "--max-loops",
                    "1",
                ],
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        run_live_mock.assert_called_once()

    @patch("app.main.run_live")
    @patch("app.main._build_live_connectors")
    @patch("app.main._build_email_sender")
    def test_main_run_live_mode_accepts_stub_executor(
        self,
        email_sender_mock,
        connectors_mock,
        run_live_mock,
    ) -> None:
        run_live_mock.return_value = []
        connectors_mock.return_value = []
        email_sender_mock.return_value = None

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--run-live",
                    "--use-stub-executor",
                    "--db-path",
                    db_path,
                    "--max-loops",
                    "1",
                ],
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        executor_arg = run_live_mock.call_args.kwargs["executor"]
        self.assertIsInstance(executor_arg, StubExecutor)

    @patch("app.main._build_codex_executor")
    @patch("app.main._build_email_sender")
    @patch("app.main._build_live_connectors")
    @patch("app.main.run_live")
    def test_main_run_live_mode_reads_defaults_from_config_file(
        self,
        run_live_mock,
        connectors_mock,
        email_sender_mock,
        executor_mock,
    ) -> None:
        run_live_mock.return_value = []
        connectors_mock.return_value = []
        email_sender_mock.return_value = None
        executor_mock.return_value = StubExecutor()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "live-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(Path(tmpdir) / "state-from-config.db"),
                        "max_attempts": 5,
                        "poll_interval_seconds": 5.5,
                        "max_loops": 3,
                        "base_dir": "/tmp/chatting",
                    }
                ),
                encoding="utf-8",
            )

            with patch("sys.argv", ["app.main", "--run-live", "--config", str(config_path)]):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        run_live_mock.assert_called_once_with(
            str(Path(tmpdir) / "state-from-config.db"),
            connectors=[],
            executor=executor_mock.return_value,
            max_attempts=5,
            poll_interval_seconds=5.5,
            max_loops=3,
            base_dir="/tmp/chatting",
            email_sender=None,
        )


def _single_email_envelope() -> TaskEnvelope:
    return FakeEmailConnector(
        messages=[
            EmailMessage(
                provider_message_id="dlq-1",
                from_address="dlq@example.com",
                subject="Fail run",
                body="Trigger retry exhaustion",
                received_at=datetime(2026, 2, 27, 20, 0, tzinfo=timezone.utc),
                context_refs=["repo:/home/edward/chatting"],
            )
        ]
    ).poll()[0]


if __name__ == "__main__":
    unittest.main()
