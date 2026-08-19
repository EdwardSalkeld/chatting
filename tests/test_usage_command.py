import json
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.broker import TaskQueueMessage
from app.models import (
    ExecutionResult,
    ReplyChannel,
    TaskEnvelope,
    UsageReport,
    UsageWindow,
)
from app.state import SQLiteStateStore
from app.usage_command import (
    format_usage_report,
    is_usage_command_envelope,
)
from app.worker.activity import WorkerActivityMonitor
from app.worker.executor import CodexExecutor
from app.worker.runtime import WorkerProcessResult, process_task_message

_NOW = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)


def _envelope(content: str) -> TaskEnvelope:
    return TaskEnvelope(
        id="telegram:1",
        source="im",
        received_at=_NOW,
        actor="8605042448:edsalkeld",
        content=content,
        attachments=[],
        context_refs=[],
        reply_channel=ReplyChannel(type="telegram", target="8605042448"),
        dedupe_key="telegram:1",
    )


def _task_message(content: str) -> TaskQueueMessage:
    return TaskQueueMessage(
        task_id="task:telegram:1",
        envelope=_envelope(content),
        trace_id="trace-1",
        emitted_at=_NOW,
    )


def _write_rollout(
    directory: Path,
    *,
    name: str,
    rate_limits: object,
    model: str | None = "gpt-5.6-terra",
    timestamp: str = "2026-08-19T10:11:13.540Z",
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    if model is not None:
        lines.append(
            json.dumps(
                {
                    "timestamp": timestamp,
                    "type": "turn_context",
                    "payload": {"model": model, "cwd": "/srv/chatting/workspace"},
                }
            )
        )
    # A realistic transcript line that must not be parsed as usage data.
    lines.append(
        json.dumps(
            {
                "timestamp": timestamp,
                "type": "response_item",
                "payload": {"type": "message", "text": "some transcript content"},
            }
        )
    )
    if rate_limits is not None:
        lines.append(
            json.dumps(
                {
                    "timestamp": timestamp,
                    "type": "event_msg",
                    "payload": {"type": "token_count", "rate_limits": rate_limits},
                }
            )
        )
    (directory / name).write_text("\n".join(lines) + "\n", encoding="utf-8")


_LIVE_RATE_LIMITS = {
    "limit_id": "codex",
    "limit_name": None,
    "primary": {
        "used_percent": 42.0,
        "window_minutes": 10080,
        "resets_at": 1787270400,
    },
    "secondary": None,
    "credits": {"has_credits": True, "unlimited": False, "balance": "250"},
    "plan_type": "plus",
}

_EXHAUSTED_RATE_LIMITS = {
    "limit_id": "premium",
    "primary": None,
    "secondary": None,
    "credits": {"has_credits": False, "unlimited": False, "balance": "0"},
}


class UsageCommandDetectionTests(unittest.TestCase):
    def test_bare_command_matches(self) -> None:
        self.assertTrue(is_usage_command_envelope(_envelope("/usage")))

    def test_surrounding_whitespace_and_case_match(self) -> None:
        self.assertTrue(is_usage_command_envelope(_envelope("  /Usage\n")))

    def test_command_with_extra_words_does_not_match(self) -> None:
        self.assertFalse(is_usage_command_envelope(_envelope("/usage please")))

    def test_command_mentioned_mid_sentence_does_not_match(self) -> None:
        self.assertFalse(
            is_usage_command_envelope(_envelope("what does /usage report?"))
        )

    def test_forum_topic_thread_prefix_still_matches(self) -> None:
        self.assertTrue(is_usage_command_envelope(_envelope("[thread_id=42] /usage")))

    def test_thread_prefix_with_extra_words_does_not_match(self) -> None:
        self.assertFalse(
            is_usage_command_envelope(_envelope("[thread_id=42] /usage please"))
        )


class CodexUsageReportTests(unittest.TestCase):
    def test_reads_newest_snapshot_from_session_rollouts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / ".codex"
            day = codex_home / "sessions" / "2026" / "08" / "19"
            _write_rollout(
                day,
                name="rollout-2026-08-19T07-00-00-aaa.jsonl",
                rate_limits={
                    "limit_id": "codex",
                    "primary": {"used_percent": 10.0, "window_minutes": 10080},
                    "credits": {
                        "has_credits": True,
                        "unlimited": False,
                        "balance": "9",
                    },
                },
                model="gpt-5.6-sol",
            )
            _write_rollout(
                day,
                name="rollout-2026-08-19T10-11-00-bbb.jsonl",
                rate_limits=_LIVE_RATE_LIMITS,
            )
            report = CodexExecutor(env={"CODEX_HOME": str(codex_home)}).usage_report()

        self.assertEqual(report.errors, [])
        self.assertEqual(report.model, "gpt-5.6-terra")
        self.assertEqual(report.plan_type, "plus")
        self.assertEqual(report.limit_id, "codex")
        self.assertIsNotNone(report.primary)
        assert report.primary is not None
        self.assertEqual(report.primary.used_percent, 42.0)
        self.assertEqual(report.primary.window_minutes, 10080)
        self.assertEqual(report.credit_balance, "250")
        self.assertIs(report.has_credits, True)
        self.assertEqual(
            report.observed_at,
            datetime(2026, 8, 19, 10, 11, 13, 540000, tzinfo=timezone.utc),
        )

    def test_resolves_codex_home_from_home_when_unset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            day = Path(tmp) / ".codex" / "sessions" / "2026" / "08" / "19"
            _write_rollout(
                day,
                name="rollout-2026-08-19T10-11-00-bbb.jsonl",
                rate_limits=_LIVE_RATE_LIMITS,
            )
            report = CodexExecutor(env={"HOME": tmp}).usage_report()

        self.assertEqual(report.errors, [])
        self.assertEqual(report.limit_id, "codex")

    def test_skips_rollouts_without_a_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / ".codex"
            day = codex_home / "sessions" / "2026" / "08" / "19"
            _write_rollout(
                day,
                name="rollout-2026-08-19T07-00-00-aaa.jsonl",
                rate_limits=_LIVE_RATE_LIMITS,
            )
            _write_rollout(
                day,
                name="rollout-2026-08-19T11-00-00-ccc.jsonl",
                rate_limits=None,
            )
            report = CodexExecutor(env={"CODEX_HOME": str(codex_home)}).usage_report()

        self.assertEqual(report.errors, [])
        self.assertEqual(report.limit_id, "codex")

    def test_exhausted_plan_reports_no_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / ".codex"
            day = codex_home / "sessions" / "2026" / "08" / "19"
            _write_rollout(
                day,
                name="rollout-2026-08-19T10-11-00-bbb.jsonl",
                rate_limits=_EXHAUSTED_RATE_LIMITS,
            )
            report = CodexExecutor(env={"CODEX_HOME": str(codex_home)}).usage_report()

        self.assertEqual(report.errors, [])
        self.assertEqual(report.limit_id, "premium")
        self.assertIsNone(report.primary)
        self.assertIs(report.has_credits, False)
        self.assertEqual(report.credit_balance, "0")

    def test_missing_sessions_directory_reports_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = CodexExecutor(env={"CODEX_HOME": tmp}).usage_report()
        self.assertEqual(report.errors, ["codex_sessions_missing"])

    def test_unresolvable_codex_home_reports_error(self) -> None:
        report = CodexExecutor(env={}).usage_report()
        self.assertEqual(report.errors, ["codex_home_unresolved"])


class UsageReportFormattingTests(unittest.TestCase):
    def test_formats_window_credits_and_staleness(self) -> None:
        report = UsageReport(
            observed_at=datetime(2026, 8, 19, 10, 0, tzinfo=timezone.utc),
            model="gpt-5.6-terra",
            plan_type="plus",
            limit_id="codex",
            primary=UsageWindow(
                used_percent=42.0,
                window_minutes=10080,
                resets_at=datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
            ),
            credit_balance="250",
            has_credits=True,
        )
        body = format_usage_report(report, now=_NOW)

        self.assertIn("Codex usage as of 19 Aug 10:00 UTC, 2h ago.", body)
        self.assertIn("Model gpt-5.6-terra, plan plus, limit codex", body)
        self.assertIn("Primary window (7d): 42% used", body)
        self.assertIn("resets 20 Aug 12:00 UTC (in 1d)", body)
        self.assertIn("Credits: 250.", body)

    def test_formats_exhausted_plan_without_a_window(self) -> None:
        report = UsageReport(
            observed_at=_NOW,
            limit_id="premium",
            credit_balance="0",
            has_credits=False,
        )
        body = format_usage_report(report, now=_NOW)

        self.assertIn("No window allowance reported", body)
        self.assertIn("Credits: 0, none left to spend.", body)

    def test_reports_absent_snapshot_plainly(self) -> None:
        body = format_usage_report(
            UsageReport(errors=["codex_sessions_empty"]), now=_NOW
        )
        self.assertIn("No usage snapshot available yet", body)


@dataclass(frozen=True)
class _ExplodingUsageExecutor:
    """Fails loudly if the worker runs a task instead of short-circuiting."""

    report: UsageReport

    def execute(self, task) -> ExecutionResult:
        del task
        raise AssertionError("/usage must not invoke the executor")

    def usage_report(self) -> UsageReport:
        return self.report


@dataclass(frozen=True)
class _NoUsageExecutor:
    def execute(self, task) -> ExecutionResult:
        del task
        raise AssertionError("/usage must not invoke the executor")


class UsageCommandWorkerTests(unittest.TestCase):
    def _process(self, executor) -> WorkerProcessResult:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteStateStore(str(Path(tmp) / "worker.db"))
            monitor = WorkerActivityMonitor(store=store)
            return process_task_message(
                store=store,
                task_message=_task_message("/usage"),
                executor_impl=executor,
                max_attempts=2,
                activity_monitor=monitor,
            )

    def test_usage_command_replies_without_running_the_executor(self) -> None:
        report = UsageReport(
            observed_at=datetime(2026, 8, 19, 10, 0, tzinfo=timezone.utc),
            model="gpt-5.6-terra",
            limit_id="codex",
            primary=UsageWindow(used_percent=42.0, window_minutes=10080),
            credit_balance="250",
            has_credits=True,
        )
        result = self._process(_ExplodingUsageExecutor(report=report))

        self.assertEqual(result.reason_codes, ["usage_command"])
        self.assertEqual(result.run_record.result_status, "success")
        self.assertFalse(result.dead_lettered)
        self.assertEqual(len(result.egress_messages), 2)
        visible = result.egress_messages[0]
        self.assertEqual(visible.message.channel, "telegram")
        self.assertEqual(visible.message.target, "8605042448")
        self.assertIn("Primary window (7d): 42% used", visible.message.body or "")
        self.assertEqual(result.egress_messages[1].event_kind, "completion")

    def test_executor_without_usage_support_says_so(self) -> None:
        result = self._process(_NoUsageExecutor())

        self.assertEqual(result.reason_codes, ["usage_command"])
        body = result.egress_messages[0].message.body or ""
        self.assertIn("does not report usage", body)


if __name__ == "__main__":
    unittest.main()
