import tempfile
import time
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.internal_heartbeat import build_internal_heartbeat_envelope
from app.internal_notices import (
    INTERNAL_NOTICE_METADATA_KEY,
    TELEGRAM_CHANNEL_NOT_ENABLED_NOTICE,
)
from app.models import (
    ExecutionResult,
    ReplyChannel,
    TaskEnvelope,
)
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor
from app.worker.runtime import _build_error_summary, process_task_message


@dataclass(frozen=True)
class MultiMessageExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(
            errors=[], stdout="executor stdout", stderr="executor stderr"
        )


@dataclass(frozen=True)
class AlwaysFailExecutor:
    def execute(self, task):
        del task
        raise RuntimeError("executor down")


@dataclass(frozen=True)
class CreditsFailExecutor:
    def execute(self, task):
        del task
        raise RuntimeError("out of credits")


@dataclass(frozen=True)
class ExecutionErrorExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(
            errors=["executor_exit_nonzero:1:insufficient credits"],
        )


@dataclass(frozen=True)
class LongExecutionErrorExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(
            errors=[
                "executor_exit_nonzero:1:"
                + "permission denied\nconfig.toml " * 30
            ],
        )


@dataclass(frozen=True)
class IncrementalReplyExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(
            errors=[],
        )


@dataclass(frozen=True)
class NoMessageExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(errors=[])


@dataclass(frozen=True)
class SleepExecutor:
    sleep_seconds: float

    def execute(self, task):
        del task
        time.sleep(self.sleep_seconds)
        return ExecutionResult(errors=[])


@dataclass(frozen=True)
class FinalAliasExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(errors=[])


@dataclass(frozen=True)
class MainReplyExecutor:
    store: SQLiteStateStore

    def execute(self, task):
        self.store.append_worker_activity(
            occurred_at=datetime.now(timezone.utc),
            task_id=f"task:{task.id}",
            envelope_id=task.id,
            phase="egress_incremental",
            summary="incremental egress to telegram",
            detail={
                "channel": "telegram",
                "target": task.reply_channel.target,
                "event_id": "evt:test:main-reply",
                "event_kind": "incremental",
                "publish_source": "main_reply",
                "sequence": None,
            },
        )
        return ExecutionResult(errors=[])


class WorkerRuntimeTests(unittest.TestCase):
    def _build_monitor(self, store: SQLiteStateStore) -> WorkerActivityMonitor:
        return WorkerActivityMonitor(store=store, history_limit=10)

    def _build_task_message(self) -> TaskQueueMessage:
        envelope = TaskEnvelope(
            id="email:1",
            source="email",
            received_at=datetime(2026, 3, 6, 13, 0, tzinfo=timezone.utc),
            actor="alice@example.com",
            content="hello",
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:1",
        )
        return TaskQueueMessage.from_envelope(envelope, trace_id="trace:email:1")

    def _build_telegram_task_message(self, *, content: str = "hello") -> TaskQueueMessage:
        envelope = TaskEnvelope(
            id="telegram:1",
            source="im",
            received_at=datetime(2026, 3, 6, 13, 0, tzinfo=timezone.utc),
            actor="8605042448:edsalkeld",
            content=content,
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(
                type="telegram",
                target="8605042448",
                metadata={"message_id": 2471},
            ),
            dedupe_key="telegram:1",
        )
        return TaskQueueMessage.from_envelope(envelope, trace_id="trace:telegram:1")

    def _build_internal_heartbeat_task_message(self) -> TaskQueueMessage:
        return TaskQueueMessage.from_envelope(
            build_internal_heartbeat_envelope(
                sequence=1,
                now=datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc),
            ),
            trace_id="trace:internal:heartbeat:1",
        )

    def _build_internal_channel_notice_task_message(self) -> TaskQueueMessage:
        envelope = TaskEnvelope(
            id="telegram-disallowed-channel:2101",
            source="internal",
            received_at=datetime(2026, 6, 25, 8, 0, tzinfo=timezone.utc),
            actor="message-handler",
            content=(
                "Not enabled in channel -100777. "
                "Add this id to telegram_allowed_channel_ids to enable replies here."
            ),
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(
                type="telegram",
                target="-100777",
                metadata={
                    INTERNAL_NOTICE_METADATA_KEY: (
                        TELEGRAM_CHANNEL_NOT_ENABLED_NOTICE
                    ),
                    "message_id": 19,
                },
            ),
            dedupe_key="telegram-disallowed-channel:2101",
        )
        return TaskQueueMessage.from_envelope(
            envelope,
            trace_id="trace:internal:telegram-disallowed-channel:2101",
        )

    def test_process_task_message_emits_completion_only_for_successful_task(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                executor_impl=MultiMessageExecutor(),
                max_attempts=2,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(len(result.egress_messages), 1)
            self.assertEqual(result.egress_messages[0].event_kind, "completion")
            self.assertEqual(result.egress_messages[0].message.channel, "internal")
            audit_event = store.list_audit_events()[0]
            self.assertEqual(
                audit_event.detail["execution_result"]["stdout"], "executor stdout"
            )
            self.assertEqual(
                audit_event.detail["execution_result"]["stderr"], "executor stderr"
            )
            activity = store.list_recent_worker_activity(
                limit=10, include_internal=True
            )
            self.assertEqual(activity[1]["phase"], "executor_stderr")
            self.assertEqual(activity[2]["phase"], "executor_stdout")

    def test_process_task_message_retries_and_dead_letters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                executor_impl=AlwaysFailExecutor(),
                max_attempts=2,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "dead_letter")
            self.assertEqual(result.dead_lettered, True)
            self.assertEqual(result.attempt_count, 2)
            self.assertEqual(result.reason_codes, ["retry_exhausted"])
            self.assertEqual(result.error_summary, "RuntimeError: executor down")
            self.assertEqual(
                store.list_dead_letters()[0].reason_codes, ["retry_exhausted"]
            )

    def test_process_task_message_emits_visible_credit_error_before_dead_letter_completion(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                executor_impl=CreditsFailExecutor(),
                max_attempts=1,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "dead_letter")
            self.assertEqual(len(result.egress_messages), 2)
            visible = result.egress_messages[0]
            completion = result.egress_messages[1]
            self.assertEqual(visible.event_kind, "message")
            self.assertEqual(visible.message.channel, "email")
            self.assertIn("ran out of credits or quota", visible.message.body)
            self.assertEqual(completion.event_kind, "completion")
            self.assertEqual(completion.sequence, 1)

    def test_process_task_message_emits_visible_credit_error_for_execution_error_result(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                executor_impl=ExecutionErrorExecutor(),
                max_attempts=1,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "execution_error")
            self.assertEqual(len(result.egress_messages), 2)
            self.assertEqual(result.reason_codes, ["executor_reported_errors"])
            self.assertEqual(
                result.error_summary, "executor_exit_nonzero:1:insufficient credits"
            )
            visible = result.egress_messages[0]
            self.assertEqual(visible.event_kind, "message")
            self.assertEqual(visible.message.target, "alice@example.com")
            self.assertIn("ran out of credits or quota", visible.message.body)

    def test_process_task_message_builds_bounded_single_line_error_summary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                executor_impl=LongExecutionErrorExecutor(),
                max_attempts=1,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "execution_error")
            self.assertIsNotNone(result.error_summary)
            assert result.error_summary is not None
            self.assertNotIn("\n", result.error_summary)
            self.assertLessEqual(len(result.error_summary), 240)
            self.assertTrue(result.error_summary.endswith("..."))

    def test_process_task_message_keeps_non_telegram_success_without_visible_reply(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                executor_impl=IncrementalReplyExecutor(),
                max_attempts=2,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(len(result.egress_messages), 1)
            self.assertEqual(result.egress_messages[0].event_kind, "completion")
            audit_event = store.list_audit_events()[0]
            self.assertEqual(
                audit_event.detail["incremental_reply_send_requested_count"], 0
            )
            self.assertEqual(
                audit_event.detail["incremental_reply_send_published_count"], 0
            )
            self.assertEqual(
                audit_event.detail["worker_pickup_send_requested_count"], 0
            )
            self.assertEqual(
                audit_event.detail["worker_pickup_send_published_count"], 0
            )

    def test_process_task_message_marks_telegram_success_without_visible_reply_as_execution_error(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_telegram_task_message(),
                executor_impl=IncrementalReplyExecutor(),
                max_attempts=2,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "execution_error")
            self.assertEqual(result.reason_codes, ["missing_visible_reply"])
            self.assertEqual(len(result.egress_messages), 2)
            self.assertEqual(result.egress_messages[0].event_kind, "message")
            self.assertEqual(result.egress_messages[0].message.channel, "telegram")
            audit_event = store.list_audit_events()[0]
            self.assertIn(
                "failed to publish the Telegram reply",
                result.egress_messages[0].message.body,
            )
            self.assertEqual(result.egress_messages[1].event_kind, "completion")
            self.assertEqual(
                audit_event.detail["incremental_reply_send_published_count"], 0
            )
            self.assertEqual(
                audit_event.detail["worker_pickup_send_requested_count"], 0
            )
            self.assertEqual(
                audit_event.detail["worker_pickup_send_published_count"], 0
            )

    def test_process_task_message_handles_internal_heartbeat_without_executor(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_internal_heartbeat_task_message(),
                executor_impl=AlwaysFailExecutor(),
                max_attempts=2,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(result.run_record.workflow, "default")
            self.assertEqual(result.run_record.source, "internal")
            self.assertEqual(len(result.egress_messages), 2)
            visible_egress_message = result.egress_messages[0]
            completion_egress_message = result.egress_messages[1]
            self.assertEqual(visible_egress_message.event_kind, "message")
            self.assertEqual(visible_egress_message.message.channel, "log")
            self.assertEqual(visible_egress_message.message.target, "heartbeat")
            self.assertEqual(
                json.loads(visible_egress_message.message.body)["kind"],
                "heartbeat_pong",
            )
            self.assertEqual(completion_egress_message.event_kind, "completion")
            self.assertEqual(result.reason_codes, ["internal_heartbeat"])
            self.assertIsNone(result.error_summary)
            audit_event = store.list_audit_events()[0]
            self.assertEqual(audit_event.workflow, "default")
            self.assertEqual(audit_event.detail["reason_codes"], ["internal_heartbeat"])
            self.assertEqual(audit_event.detail["heartbeat"]["kind"], "heartbeat_pong")

    def test_process_task_message_emits_internal_completion_when_no_visible_messages(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                executor_impl=NoMessageExecutor(),
                max_attempts=2,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(len(result.egress_messages), 1)
            terminal = result.egress_messages[0]
            self.assertEqual(terminal.event_kind, "completion")
            self.assertEqual(terminal.message.channel, "internal")
            self.assertEqual(terminal.message.target, "task")
            self.assertEqual(terminal.sequence, 0)

    def test_process_task_message_allows_telegram_success_when_main_reply_was_published(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_telegram_task_message(),
                executor_impl=MainReplyExecutor(store=store),
                max_attempts=2,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(len(result.egress_messages), 1)
            self.assertEqual(result.egress_messages[0].event_kind, "completion")
            audit_event = store.list_audit_events()[0]
            self.assertEqual(
                audit_event.detail["incremental_reply_send_published_count"], 1
            )
            self.assertEqual(
                audit_event.detail["worker_pickup_send_published_count"], 0
            )

    def test_process_task_message_sends_delayed_worker_pickup_for_long_telegram_task(
        self,
    ) -> None:
        published: list[EgressQueueMessage] = []
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_telegram_task_message(),
                executor_impl=SleepExecutor(sleep_seconds=0.05),
                max_attempts=1,
                activity_monitor=self._build_monitor(store),
                publish_incremental_egress=published.append,
                telegram_pickup_delay_seconds=0.01,
            )

            self.assertEqual(result.run_record.result_status, "execution_error")
            self.assertEqual(len(published), 1)
            pickup = published[0]
            self.assertEqual(pickup.event_kind, "incremental")
            self.assertEqual(pickup.message.channel, "telegram")
            self.assertEqual(pickup.message.body, "Looking into that now 🤔")
            audit_event = store.list_audit_events()[0]
            self.assertEqual(
                audit_event.detail["worker_pickup_send_requested_count"], 1
            )
            self.assertEqual(
                audit_event.detail["worker_pickup_send_published_count"], 1
            )

    def test_process_task_message_skips_delayed_worker_pickup_for_fast_telegram_task(
        self,
    ) -> None:
        published: list[EgressQueueMessage] = []
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            process_task_message(
                store=store,
                task_message=self._build_telegram_task_message(),
                executor_impl=SleepExecutor(sleep_seconds=0.005),
                max_attempts=1,
                activity_monitor=self._build_monitor(store),
                publish_incremental_egress=published.append,
                telegram_pickup_delay_seconds=0.05,
            )

            self.assertEqual(published, [])
            audit_event = store.list_audit_events()[0]
            self.assertEqual(
                audit_event.detail["worker_pickup_send_requested_count"], 1
            )
            self.assertEqual(
                audit_event.detail["worker_pickup_send_published_count"], 0
            )

    def test_process_task_message_skips_delayed_worker_pickup_when_task_requests_silence(
        self,
    ) -> None:
        published: list[EgressQueueMessage] = []
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            process_task_message(
                store=store,
                task_message=self._build_telegram_task_message(
                    content=(
                        "Run the import now. Only when something changed, "
                        "do not send any Telegram message."
                    )
                ),
                executor_impl=SleepExecutor(sleep_seconds=0.05),
                max_attempts=1,
                activity_monitor=self._build_monitor(store),
                publish_incremental_egress=published.append,
                telegram_pickup_delay_seconds=0.01,
            )

            self.assertEqual(published, [])
            audit_event = store.list_audit_events()[0]
            self.assertEqual(
                audit_event.detail["worker_pickup_send_requested_count"], 0
            )
            self.assertEqual(
                audit_event.detail["worker_pickup_send_published_count"], 0
            )

    def test_process_task_message_handles_internal_channel_notice_without_executor(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_internal_channel_notice_task_message(),
                executor_impl=AlwaysFailExecutor(),
                max_attempts=2,
                activity_monitor=self._build_monitor(store),
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(result.run_record.source, "internal")
            self.assertEqual(len(result.egress_messages), 2)
            visible_egress_message = result.egress_messages[0]
            completion_egress_message = result.egress_messages[1]
            self.assertEqual(visible_egress_message.event_kind, "message")
            self.assertEqual(visible_egress_message.message.channel, "telegram")
            self.assertEqual(visible_egress_message.message.target, "-100777")
            self.assertEqual(
                visible_egress_message.message.body,
                "Not enabled in channel -100777. Add this id to telegram_allowed_channel_ids to enable replies here.",
            )
            self.assertEqual(completion_egress_message.event_kind, "completion")
            self.assertEqual(completion_egress_message.sequence, 1)
            self.assertEqual(result.reason_codes, ["internal_notice"])
            self.assertIsNone(result.error_summary)
            audit_event = store.list_audit_events()[0]
            self.assertEqual(audit_event.detail["reason_codes"], ["internal_notice"])
            self.assertEqual(
                audit_event.detail["internal_notice"],
                "telegram_channel_not_enabled",
            )

    def test_build_error_summary_prefers_execution_error_then_last_error_then_fallback(
        self,
    ) -> None:
        self.assertEqual(
            _build_error_summary(
                execution_errors=["executor_exit_nonzero:1: boom"],
                last_error="RuntimeError: ignored",
            ),
            "executor_exit_nonzero:1: boom",
        )
        self.assertEqual(
            _build_error_summary(execution_errors=[], last_error="RuntimeError:\n boom"),
            "RuntimeError: boom",
        )
        self.assertEqual(
            _build_error_summary(execution_errors=[], last_error=None),
            "unknown_error",
        )


if __name__ == "__main__":
    unittest.main()
