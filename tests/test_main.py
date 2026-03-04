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
from app.models import ConfigUpdate, ExecutionResult, OutboundMessage, ReplyChannel, RoutedTask, TaskEnvelope
from app.main import main, run_bootstrap, run_live
from app.connectors import EmailMessage, FakeEmailConnector, TelegramConnector
from app.applier import TelegramMessageSender
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


@dataclass(frozen=True)
class PendingReviewExecutor:
    """Executor used to emit one sensitive config update requiring approval."""

    def execute(self, task: RoutedTask):
        return ExecutionResult(
            messages=[],
            actions=[],
            config_updates=[ConfigUpdate(path="secrets.api_key", value="new-secret")],
            requires_human_review=False,
            errors=[],
        )


@dataclass
class RecordingTelegramExecutor:
    """Executor that captures incoming task content and emits Telegram replies."""

    seen_contents: list[str] = field(default_factory=list)

    def execute(self, task: RoutedTask):
        self.seen_contents.append(task.content or "")
        if task.reply_channel is None:
            return ExecutionResult(
                messages=[],
                actions=[],
                config_updates=[],
                requires_human_review=False,
                errors=[],
            )
        return ExecutionResult(
            messages=[
                OutboundMessage(
                    channel=task.reply_channel.type,
                    target=task.reply_channel.target,
                    body=f"ack: {task.envelope_id}",
                )
            ],
            actions=[],
            config_updates=[],
            requires_human_review=False,
            errors=[],
        )


class MainBootstrapFlowTests(unittest.TestCase):
    def test_run_bootstrap_processes_unique_events_and_records_blocked_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")

            with self.assertLogs("app.main", level="INFO") as logs:
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
                for line in logs.output
                if "run_observed " in line
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
            with self.assertLogs("app.main", level="WARNING") as logs:
                runs = run_bootstrap(
                    db_path,
                    executor=FlakyExecutor(fail_task_id="task:email:ok-1"),
                    max_attempts=2,
                )

            self.assertEqual(
                [run.result_status for run in runs],
                ["success", "success", "blocked_action", "duplicate_skipped"],
            )
            retry_lines = [line for line in logs.output if "retry_scheduled " in line]
            self.assertEqual(len(retry_lines), 1)
            self.assertIn("trace_id=trace:email:ok-1", retry_lines[0])
            self.assertIn("run_id=run:email:ok-1", retry_lines[0])
            self.assertIn("attempt=1", retry_lines[0])
            self.assertIn("next_attempt=2", retry_lines[0])
            self.assertIn("max_attempts=2", retry_lines[0])

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

            with self.assertLogs("app.main", level="ERROR") as logs:
                runs = run_bootstrap(
                    db_path,
                    envelopes=[envelope],
                    executor=AlwaysFailExecutor(),
                    max_attempts=2,
                )

            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0].result_status, "dead_letter")
            dead_letter_lines = [line for line in logs.output if "dead_letter " in line]
            self.assertEqual(len(dead_letter_lines), 1)
            self.assertIn("trace_id=trace:email:dlq-1", dead_letter_lines[0])
            self.assertIn("run_id=run:email:dlq-1", dead_letter_lines[0])
            self.assertIn("attempts=2", dead_letter_lines[0])
            self.assertIn("max_attempts=2", dead_letter_lines[0])

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


@dataclass
class RecordingTelegramSender:
    sent: list[tuple[str, str]] = field(default_factory=list)

    def send(self, target: str, body: str) -> None:
        self.sent.append((target, body))


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

    def test_run_live_supports_parallel_worker_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            envelopes = [
                _single_email_envelope(),
                FakeEmailConnector(
                    messages=[
                        EmailMessage(
                            provider_message_id="dlq-2",
                            from_address="two@example.com",
                            subject="Second",
                            body="Second message",
                            received_at=datetime(2026, 2, 27, 20, 1, tzinfo=timezone.utc),
                            context_refs=["repo:/home/edward/chatting"],
                        )
                    ]
                ).poll()[0],
            ]

            runs = run_live(
                db_path,
                connectors=[OneShotConnector(envelopes=envelopes)],
                executor=StubExecutor(),
                max_loops=1,
                max_attempts=2,
                poll_interval_seconds=0.1,
                base_dir=tmpdir,
                worker_count=2,
            )

            self.assertEqual(len(runs), 2)
            self.assertEqual({run.envelope_id for run in runs}, {"email:dlq-1", "email:dlq-2"})

    def test_run_live_includes_recent_telegram_conversation_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            executor = RecordingTelegramExecutor()
            telegram_sender = RecordingTelegramSender()
            envelopes = [
                _telegram_envelope(event_id="telegram:1", content="What country is Paris in?"),
                _telegram_envelope(event_id="telegram:2", content="What continent is that in?"),
            ]

            runs = run_live(
                db_path,
                connectors=[OneShotConnector(envelopes=envelopes)],
                executor=executor,
                max_loops=1,
                max_attempts=2,
                poll_interval_seconds=0.1,
                base_dir=tmpdir,
                telegram_sender=telegram_sender,
            )

            self.assertEqual(len(runs), 2)
            self.assertEqual(
                telegram_sender.sent,
                [("8605042448", "ack: telegram:1"), ("8605042448", "ack: telegram:2")],
            )
            self.assertEqual(len(executor.seen_contents), 2)
            self.assertEqual(executor.seen_contents[0], "What country is Paris in?")
            self.assertIn("Recent conversation context (oldest first):", executor.seen_contents[1])
            self.assertIn("user: What country is Paris in?", executor.seen_contents[1])
            self.assertIn("assistant: ack: telegram:1", executor.seen_contents[1])
            self.assertIn("Current user message:", executor.seen_contents[1])
            self.assertIn("What continent is that in?", executor.seen_contents[1])


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
                    ValueError,
                    "--list-runs/--list-audit-events/--list-dead-letters/--replay-dead-letters/--list-pending-approvals/--approve-pending-approval/--reject-pending-approval/--list-config-versions/--rollback-config-version/--list-metrics/--serve-metrics cannot be combined",
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
                    "--list-runs/--list-audit-events/--list-dead-letters/--replay-dead-letters/--list-pending-approvals/--approve-pending-approval/--reject-pending-approval/--list-config-versions/--rollback-config-version/--list-metrics/--serve-metrics cannot be combined with --run-live",
                ):
                    main()

    def test_main_list_dead_letters_outputs_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            run_bootstrap(
                db_path,
                envelopes=[_single_email_envelope()],
                executor=AlwaysFailExecutor(),
                max_attempts=2,
            )

            output_buffer = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--list-dead-letters",
                        "--result-status",
                        "pending",
                    ],
                ),
                redirect_stdout(output_buffer),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        dead_letters = json.loads(output_buffer.getvalue().strip())
        self.assertEqual(len(dead_letters), 1)
        self.assertEqual(dead_letters[0]["status"], "pending")
        self.assertEqual(dead_letters[0]["reason_codes"], ["retry_exhausted"])

    def test_main_replay_dead_letters_processes_pending_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            run_bootstrap(
                db_path,
                envelopes=[_single_email_envelope()],
                executor=AlwaysFailExecutor(),
                max_attempts=2,
            )

            output_buffer = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--replay-dead-letters",
                        "--use-stub-executor",
                    ],
                ),
                redirect_stdout(output_buffer),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            replayed_runs = json.loads(output_buffer.getvalue().strip())
            self.assertEqual(len(replayed_runs), 1)
            self.assertEqual(replayed_runs[0]["result_status"], "success")
            self.assertIn(":replay:", replayed_runs[0]["run_id"])
            dead_letters = SQLiteStateStore(db_path).list_dead_letters(status="replayed")
            self.assertEqual(len(dead_letters), 1)
            self.assertEqual(dead_letters[0].status, "replayed")

    def test_main_lists_and_resolves_pending_approvals(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            run_bootstrap(
                db_path,
                envelopes=[_single_email_envelope()],
                executor=PendingReviewExecutor(),
                max_attempts=1,
            )

            list_output = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--list-pending-approvals",
                        "--result-status",
                        "pending",
                    ],
                ),
                redirect_stdout(list_output),
            ):
                list_exit_code = main()

            self.assertEqual(list_exit_code, 0)
            pending = json.loads(list_output.getvalue().strip())
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["status"], "pending")
            approval_id = pending[0]["approval_id"]
            self.assertEqual(pending[0]["config_path"], "secrets.api_key")

            approve_output = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--approve-pending-approval",
                        str(approval_id),
                    ],
                ),
                redirect_stdout(approve_output),
            ):
                approve_exit_code = main()

            self.assertEqual(approve_exit_code, 0)
            approval_result = json.loads(approve_output.getvalue().strip())
            self.assertEqual(len(approval_result), 1)
            self.assertEqual(approval_result[0]["approval_id"], approval_id)
            self.assertEqual(approval_result[0]["status"], "approved")
            self.assertIn("version_id", approval_result[0])

            approved = SQLiteStateStore(db_path).list_pending_approvals(status="approved")
            self.assertEqual(len(approved), 1)
            self.assertEqual(approved[0].approval_id, approval_id)
            self.assertEqual(approved[0].status, "approved")

            versions_output = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--list-config-versions",
                    ],
                ),
                redirect_stdout(versions_output),
            ):
                versions_exit_code = main()

            self.assertEqual(versions_exit_code, 0)
            versions = json.loads(versions_output.getvalue().strip())
            self.assertEqual(len(versions), 1)
            self.assertEqual(versions[0]["source"], "pending_approval")
            self.assertEqual(versions[0]["config_path"], "secrets.api_key")

            rollback_output = StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        db_path,
                        "--rollback-config-version",
                        str(versions[0]["version_id"]),
                    ],
                ),
                redirect_stdout(rollback_output),
            ):
                rollback_exit_code = main()

            self.assertEqual(rollback_exit_code, 0)
            rollback_payload = json.loads(rollback_output.getvalue().strip())
            self.assertEqual(len(rollback_payload), 1)
            self.assertEqual(rollback_payload[0]["rolled_back_version_id"], versions[0]["version_id"])
            self.assertIn("rollback_version_id", rollback_payload[0])

    def test_main_list_metrics_outputs_computed_summary(self) -> None:
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
                        "--list-metrics",
                    ],
                ),
                redirect_stdout(output_buffer),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        payload = json.loads(output_buffer.getvalue().strip())
        self.assertEqual(payload["total_runs"], 4)
        self.assertIn("success", payload["by_status"])
        self.assertIn("blocked_action", payload["by_status"])
        self.assertIn("duplicate_skipped", payload["by_status"])

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

    def test_main_rejects_schedule_job_unknown_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_file = Path(tmpdir) / "schedule.json"
            schedule_file.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "heartbeat",
                            "content": "ping",
                            "interval_seconds": 60,
                            "unexpected": True,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--run-live",
                    "--schedule-file",
                    str(schedule_file),
                    "--max-loops",
                    "1",
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "schedule job at index 0 contains unknown keys: unexpected",
                ):
                    main()

    def test_main_rejects_schedule_job_boolean_interval_seconds(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_file = Path(tmpdir) / "schedule.json"
            schedule_file.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "heartbeat",
                            "content": "ping",
                            "interval_seconds": True,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--run-live",
                    "--schedule-file",
                    str(schedule_file),
                    "--max-loops",
                    "1",
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "schedule job at index 0 interval_seconds must be a positive integer",
                ):
                    main()

    def test_main_rejects_schedule_job_blank_context_ref(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_file = Path(tmpdir) / "schedule.json"
            schedule_file.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "heartbeat",
                            "content": "ping",
                            "interval_seconds": 60,
                            "context_refs": ["   "],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--run-live",
                    "--schedule-file",
                    str(schedule_file),
                    "--max-loops",
                    "1",
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "schedule job at index 0 context_refs must be a list of non-empty strings",
                ):
                    main()

    def test_main_rejects_schedule_job_with_only_reply_channel_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_file = Path(tmpdir) / "schedule.json"
            schedule_file.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "heartbeat",
                            "content": "ping",
                            "interval_seconds": 60,
                            "reply_channel_type": "telegram",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--run-live",
                    "--schedule-file",
                    str(schedule_file),
                    "--max-loops",
                    "1",
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "schedule job at index 0 reply_channel_type and reply_channel_target must be provided together",
                ):
                    main()

    def test_main_rejects_schedule_job_with_only_reply_channel_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_file = Path(tmpdir) / "schedule.json"
            schedule_file.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "heartbeat",
                            "content": "ping",
                            "interval_seconds": 60,
                            "reply_channel_target": "8605042448",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with patch(
                "sys.argv",
                [
                    "app.main",
                    "--run-live",
                    "--schedule-file",
                    str(schedule_file),
                    "--max-loops",
                    "1",
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "schedule job at index 0 reply_channel_type and reply_channel_target must be provided together",
                ):
                    main()

    def test_main_run_live_with_imap_requires_smtp_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            with (
                patch.dict("os.environ", {}, clear=True),
                patch(
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
                ),
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
            telegram_sender=None,
            worker_count=1,
        )

    @patch("app.main._build_codex_executor")
    @patch("app.main.run_live")
    def test_main_run_live_with_telegram_config_wires_connector_and_sender(
        self,
        run_live_mock,
        executor_mock,
    ) -> None:
        run_live_mock.return_value = []
        executor_mock.return_value = StubExecutor()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "live-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(Path(tmpdir) / "state.db"),
                        "telegram_enabled": True,
                        "max_loops": 1,
                        "poll_interval_seconds": 1.0,
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.dict("os.environ", {"CHATTING_TELEGRAM_BOT_TOKEN": "token"}, clear=False),
                patch("sys.argv", ["app.main", "--run-live", "--config", str(config_path)]),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        run_live_mock.assert_called_once()
        connectors_arg = run_live_mock.call_args.kwargs["connectors"]
        self.assertEqual(len(connectors_arg), 1)
        self.assertIsInstance(connectors_arg[0], TelegramConnector)
        self.assertIsInstance(
            run_live_mock.call_args.kwargs["telegram_sender"],
            TelegramMessageSender,
        )

    def test_main_run_live_rejects_missing_telegram_token_env_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "live-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "telegram_enabled": True,
                        "max_loops": 1,
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.dict("os.environ", {"CHATTING_TELEGRAM_BOT_TOKEN": ""}, clear=False),
                patch("sys.argv", ["app.main", "--run-live", "--config", str(config_path)]),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "missing Telegram bot token env var: CHATTING_TELEGRAM_BOT_TOKEN",
                ):
                    main()

    def test_main_run_live_rejects_blank_telegram_allowed_chat_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "live-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "telegram_enabled": True,
                        "telegram_allowed_chat_ids": ["   "],
                        "max_loops": 1,
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.dict("os.environ", {"CHATTING_TELEGRAM_BOT_TOKEN": "token"}, clear=False),
                patch("sys.argv", ["app.main", "--run-live", "--config", str(config_path)]),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "telegram_allowed_chat_id\\(s\\) entries must not be empty",
                ):
                    main()


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


def _telegram_envelope(*, event_id: str, content: str) -> TaskEnvelope:
    return TaskEnvelope(
        id=event_id,
        source="im",
        received_at=datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc),
        actor="8605042448:edsalkeld",
        content=content,
        attachments=[],
        context_refs=["repo:/home/edward/chatting"],
        policy_profile="default",
        reply_channel=ReplyChannel(type="telegram", target="8605042448"),
        dedupe_key=event_id,
    )


if __name__ == "__main__":
    unittest.main()
