import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

from app.executor import StubExecutor
from app.models import RoutedTask, TaskEnvelope
from app.main import run_bootstrap
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

            self.assertEqual([run.source for run in runs], ["cron", "email", "email"])
            self.assertEqual(
                [run.envelope_id for run in runs],
                ["cron:daily-summary:2026-02-27T09:00:00+00:00", "email:ok-1", "email:blocked-1"],
            )
            self.assertEqual(
                [run.result_status for run in runs],
                ["success", "success", "blocked_action"],
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

            observed_lines = [
                line
                for line in output_buffer.getvalue().splitlines()
                if line.startswith("run_observed ")
            ]
            self.assertEqual(len(observed_lines), 3)
            for line in observed_lines:
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
                ["success", "success", "blocked_action"],
            )
            self.assertIn(
                "retry_scheduled run_id=run:email:ok-1 attempt=1 next_attempt=2 max_attempts=2",
                output_buffer.getvalue(),
            )

            audit_events = SQLiteStateStore(db_path).list_audit_events()
            target_event = next(event for event in audit_events if event.run_id == "run:email:ok-1")
            self.assertEqual(target_event.detail["attempt_count"], 2)
            self.assertEqual(target_event.detail["reason_codes"], [])
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
                "dead_letter run_id=run:email:dlq-1 attempts=2 max_attempts=2",
                output_buffer.getvalue(),
            )

            audit_events = SQLiteStateStore(db_path).list_audit_events()
            self.assertEqual(len(audit_events), 1)
            self.assertEqual(audit_events[0].result_status, "dead_letter")
            self.assertEqual(audit_events[0].detail["reason_codes"], ["retry_exhausted"])
            self.assertEqual(audit_events[0].detail["attempt_count"], 2)
            self.assertIn("RuntimeError", audit_events[0].detail["last_error"])


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
