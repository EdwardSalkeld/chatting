import tempfile
import unittest
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
import json
import sqlite3

from tests.test_pr_registration import APP_SERVER_PR_CREATED
from app.broker import TaskQueueMessage
from app.internal_heartbeat import (
    INTERNAL_HEARTBEAT_TARGET,
    INTERNAL_REPLY_CHANNEL_TYPE,
    INTERNAL_SOURCE,
)
from app.internal_notices import (
    INTERNAL_NOTICE_METADATA_KEY,
    TELEGRAM_CHANNEL_NOT_ENABLED_NOTICE,
    TELEGRAM_GROUP_NOT_ENABLED_NOTICE,
)
from app.models import (
    ExecutionResult,
    ReplyChannel,
    TaskEnvelope,
)
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor
from app.worker.runtime import process_task_message

GENERIC_FAILURE = "I failed to process this task. Please try again later."


def _record_main_reply(
    store: SQLiteStateStore,
    task: TaskEnvelope,
    *,
    event_id: str,
    channel: str = "telegram",
    task_id: str | None = None,
) -> None:
    task_id = task_id or f"task:{task.id}"
    store.append_worker_activity(
        occurred_at=datetime.now(timezone.utc),
        task_id=task_id,
        envelope_id=task_id.removeprefix("task:"),
        phase="egress_incremental",
        summary="incremental egress to telegram",
        detail={
            "channel": channel,
            "target": task.reply_channel.target,
            "event_id": event_id,
            "event_kind": "incremental",
            "publish_source": "main_reply",
            "sequence": None,
        },
    )


class StepExecutor:
    """Runs `step(pass_number, task)` on each launch and records the tasks."""

    def __init__(
        self, step: Callable[[int, TaskEnvelope], ExecutionResult] | None = None
    ) -> None:
        self.step = step
        self.calls: list[TaskEnvelope] = []

    def for_workspace(self, *, work_item_id):
        del work_item_id
        return self

    def execute(self, task):
        self.calls.append(task)
        assert self.step is not None, "unexpected executor call"
        return self.step(len(self.calls), task)


def _raises(message: str) -> StepExecutor:
    def step(_pass: int, _task: TaskEnvelope) -> ExecutionResult:
        raise RuntimeError(message)

    return StepExecutor(step)


def _returns(error: str | None = None, stdout: str | None = None) -> StepExecutor:
    return StepExecutor(
        lambda n, _task: ExecutionResult(
            errors=[error] if error else [],
            stdout=stdout.format(n) if stdout else None,
        )
    )


@dataclass(frozen=True)
class StreamingExecutor:
    """Emits `chunks` through the output callback the runtime installs."""

    chunks: tuple[tuple[str, str], ...]
    output_callback: Callable[[str, str], None] | None = None

    def for_workspace(self, *, work_item_id):
        del work_item_id
        return self

    def with_output_callback(self, callback):
        return replace(self, output_callback=callback)

    def execute(self, task):
        del task
        assert self.output_callback is not None
        for stream, text in self.chunks:
            self.output_callback(stream, text)
        return ExecutionResult(
            errors=[],
            stdout="".join(t for s, t in self.chunks if s == "stdout"),
            stderr="".join(t for s, t in self.chunks if s == "stderr"),
        )


def _task(
    envelope_id: str,
    reply_channel: ReplyChannel,
    *,
    source: str = "im",
    content: str = "hello",
    minute: int = 0,
) -> TaskQueueMessage:
    envelope = TaskEnvelope(
        id=envelope_id,
        source=source,
        received_at=datetime(2026, 3, 6, 13, minute, tzinfo=timezone.utc),
        actor="8605042448:edsalkeld",
        content=content,
        attachments=[],
        context_refs=[],
        reply_channel=reply_channel,
        dedupe_key=envelope_id,
    )
    return TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:{envelope_id}")


def _email_task() -> TaskQueueMessage:
    return _task(
        "email:1",
        ReplyChannel(type="email", target="alice@example.com"),
        source="email",
    )


def _telegram_task(number: int = 1, content: str = "hello") -> TaskQueueMessage:
    return _task(
        f"telegram:{number}",
        ReplyChannel(
            type="telegram",
            target="8605042448",
            metadata={"message_id": 2470 + number},
        ),
        content=content,
        minute=number,
    )


class WorkerRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db_path = Path(tmp.name) / "worker.db"
        self.store = SQLiteStateStore(str(self.db_path))

    def _process(self, task_message, executor, *, max_attempts=2, **kwargs):
        return process_task_message(
            store=self.store,
            task_message=task_message,
            executor_impl=executor,
            max_attempts=max_attempts,
            activity_monitor=WorkerActivityMonitor(store=self.store, history_limit=10),
            **kwargs,
        )

    def _audit_detail(self) -> dict[str, object]:
        return self.store.list_audit_events()[0].detail

    def test_database_maintenance_is_builtin_and_never_calls_executor(self):
        task = _task(
            "internal:maintenance",
            ReplyChannel(type="internal", target="database-maintenance"),
            source="internal",
            content=json.dumps({"retention_days": 14}),
        )
        result = self._process(task, StepExecutor())
        self.assertEqual(result.reason_codes, ["database_maintenance"])
        self.assertEqual(result.run_record.result_status, "success")
        # The handler flushes from sequence 0, so a lone completion must take it.
        self.assertEqual(
            [(msg.event_kind, msg.sequence) for msg in result.egress_messages],
            [("completion", 0)],
        )
        detail = self._audit_detail()
        self.assertEqual(detail["egress_message_count"], 1)
        self.assertEqual(detail["maintenance"]["audit_transcripts_removed"], 0)

    def test_successful_task_emits_completion_only_and_streams_output(self) -> None:
        executor = StreamingExecutor(
            (("stdout", "executor stdout"), ("stderr", "executor stderr"))
        )
        result = self._process(_email_task(), executor)

        self.assertEqual(result.run_record.result_status, "success")
        self.assertEqual(len(result.egress_messages), 1)
        terminal = result.egress_messages[0]
        self.assertEqual(terminal.event_kind, "completion")
        self.assertEqual(terminal.message.channel, "internal")
        self.assertEqual(terminal.message.target, "task")
        self.assertEqual(terminal.sequence, 0)
        detail = self._audit_detail()
        self.assertNotIn("stdout", detail["execution_result"])
        self.assertNotIn("stderr", detail["execution_result"])
        self.assertEqual(detail["incremental_reply_send_published_count"], 0)
        activity = self.store.list_recent_worker_activity(
            limit=10, include_internal=True
        )
        self.assertEqual(activity[1]["phase"], "executor_stderr")
        self.assertEqual(activity[2]["phase"], "executor_stdout")
        self.assertEqual(activity[1]["detail"]["content"], "executor stderr")
        self.assertEqual(activity[2]["detail"]["content"], "executor stdout")

    def test_reports_pull_requests_the_executor_creates(self) -> None:
        line = APP_SERVER_PR_CREATED + "\n"
        # Streamed output can split one event across callbacks.
        executor = StreamingExecutor(
            (("stdout", line[:40]), ("stdout", line[40:]), ("stderr", line))
        )
        created: list[str] = []
        self._process(_email_task(), executor, on_pr_created=created.append)
        self.assertEqual(
            created, ["https://github.com/EdwardSalkeld/chatting/pull/308"]
        )

    def test_retries_and_dead_letters(self) -> None:
        result = self._process(_email_task(), _raises("executor down"))

        self.assertEqual(result.run_record.result_status, "dead_letter")
        self.assertEqual(result.dead_lettered, True)
        self.assertEqual(result.attempt_count, 2)
        self.assertEqual(result.reason_codes, ["retry_exhausted"])
        self.assertEqual(result.error_summary, "RuntimeError: executor down")
        with sqlite3.connect(self.db_path) as connection:
            (reason_codes_json,) = connection.execute(
                "SELECT reason_codes_json FROM dead_letters"
            ).fetchone()
        self.assertEqual(json.loads(reason_codes_json), ["retry_exhausted"])
        self.assertEqual(len(result.egress_messages), 2)
        self.assertEqual(result.egress_messages[0].event_kind, "message")
        self.assertEqual(result.egress_messages[0].message.body, GENERIC_FAILURE)

    def test_auth_failure_is_reported_without_exposing_details(self) -> None:
        executor = _returns("executor_exit_nonzero:1:invalidated oauth token: secret")
        result = self._process(_telegram_task(), executor, max_attempts=1)

        self.assertEqual(result.run_record.result_status, "execution_error")
        visible, completion = result.egress_messages
        self.assertEqual(visible.message.channel, "telegram")
        self.assertEqual(visible.message.body, GENERIC_FAILURE)
        self.assertEqual(completion.event_kind, "completion")

    def test_credit_error_is_visible_before_completion(self) -> None:
        executor = _returns("executor_exit_nonzero:1:insufficient credits")
        result = self._process(_email_task(), executor, max_attempts=1)

        self.assertEqual(result.run_record.result_status, "execution_error")
        self.assertEqual(result.reason_codes, ["executor_reported_errors"])
        self.assertEqual(
            result.error_summary, "executor_exit_nonzero:1:insufficient credits"
        )
        visible, completion = result.egress_messages
        self.assertEqual(visible.event_kind, "message")
        self.assertEqual(visible.message.target, "alice@example.com")
        self.assertIn("ran out of credits or quota", visible.message.body)
        self.assertEqual(completion.sequence, 1)

    def test_error_summary_is_bounded_single_line(self) -> None:
        executor = _returns(
            "executor_exit_nonzero:1:" + "permission denied\nconfig.toml " * 30
        )
        summary = self._process(_email_task(), executor, max_attempts=1).error_summary
        assert summary is not None
        self.assertNotIn("\n", summary)
        self.assertLessEqual(len(summary), 240)
        self.assertTrue(summary.endswith("..."))

    def test_telegram_supervised_recovery_without_reply_is_execution_error(
        self,
    ) -> None:
        executor = _returns(stdout="pass {} transcript")
        result = self._process(_telegram_task(), executor)

        self.assertEqual(result.run_record.result_status, "execution_error")
        self.assertEqual(result.reason_codes, ["missing_visible_reply"])
        self.assertEqual(result.attempt_count, 2)
        self.assertEqual([call.content for call in executor.calls], ["hello"] * 2)
        recovery_instructions = executor.calls[1].prompt_context.task_instructions[-1]
        self.assertIn("Do not redo side effects", recovery_instructions)
        self.assertIn("Captured stdout:\npass 1 transcript", recovery_instructions)
        visible, completion = result.egress_messages
        self.assertEqual(visible.message.channel, "telegram")
        self.assertIn("failed to publish the Telegram reply", visible.message.body)
        self.assertEqual(completion.event_kind, "completion")
        detail = self._audit_detail()
        self.assertEqual(detail["executor_launch_count"], 2)
        self.assertEqual(detail["supervised_recovery_used"], True)
        self.assertEqual(detail["incremental_reply_send_published_count"], 0)
        self.assertNotIn("stdout", detail["execution_result"])

    def test_supervised_recovery_can_publish_reply_and_succeed(self) -> None:
        def step(n: int, task: TaskEnvelope) -> ExecutionResult:
            if n == 2:
                _record_main_reply(self.store, task, event_id="evt:recovery")
            return ExecutionResult(errors=[])

        executor = StepExecutor(step)
        result = self._process(_telegram_task(), executor)

        self.assertEqual(result.run_record.result_status, "success")
        self.assertEqual(result.attempt_count, 2)
        self.assertEqual(result.reason_codes, [])
        detail = self._audit_detail()
        self.assertEqual(detail["incremental_reply_send_published_count"], 1)
        self.assertEqual(detail["supervised_recovery_used"], True)

    def test_supervisor_recovers_when_reaction_precedes_claimed_followups(self) -> None:
        parent = _telegram_task(1)
        followup = _telegram_task(2, "one more thing")
        self.store.stage_inbox_task(parent)
        self.store.stage_inbox_task(followup)
        # Exercise the exit-based recovery fallback retained by /finish off.
        assignment = self.store.get_work_assignment(task_id=parent.task_id)
        assert assignment is not None
        self.store.set_work_explicit_finish(
            work_item_id=assignment.work_item_id, enabled=False
        )
        self.store.claim_next_inbox_task()
        self.store.claim_conversation_followups(parent_task_id=parent.task_id)

        def step(n: int, task: TaskEnvelope) -> ExecutionResult:
            channel = "telegram_reaction" if n == 1 else "telegram"
            _record_main_reply(self.store, task, event_id=f"evt:{n}", channel=channel)
            if n == 2:
                self.store.mark_inbox_reply_delivered(parent_task_id=parent.task_id)
            return ExecutionResult(errors=[])

        executor = StepExecutor(step)
        result = self._process(parent, executor)

        self.assertEqual(result.run_record.result_status, "success")
        self.assertEqual(len(executor.calls), 2)
        self.assertIn(
            "claimed newer messages",
            executor.calls[1].prompt_context.task_instructions[-1],
        )

    def test_claimed_followup_reply_counts_as_parent_visible_reply(self) -> None:
        # Do not recover/reply twice after a newer turn receives the reply.
        parent = _telegram_task(1)
        followup = _telegram_task(2, "Oh, you already said.")
        self.store.stage_inbox_task(parent)
        self.store.stage_inbox_task(followup)
        self.store.claim_next_inbox_task()

        def step(_n: int, task: TaskEnvelope) -> ExecutionResult:
            self.store.claim_conversation_followups(parent_task_id=parent.task_id)
            _record_main_reply(
                self.store, task, event_id="evt:followup", task_id=followup.task_id
            )
            self.store.mark_inbox_reply_delivered(parent_task_id=followup.task_id)
            return ExecutionResult(errors=[])

        executor = StepExecutor(step)
        result = self._process(parent, executor)

        self.assertEqual(result.run_record.result_status, "success")
        self.assertEqual(result.reason_codes, [])
        self.assertEqual(len(executor.calls), 1)
        detail = self._audit_detail()
        self.assertEqual(detail["incremental_reply_send_published_count"], 1)
        self.assertEqual(detail["supervised_recovery_used"], False)

    def test_telegram_success_when_main_reply_was_published(self) -> None:
        def step(_n: int, task: TaskEnvelope) -> ExecutionResult:
            _record_main_reply(self.store, task, event_id="evt:main-reply")
            return ExecutionResult(errors=[])

        result = self._process(_telegram_task(), StepExecutor(step))

        self.assertEqual(result.run_record.result_status, "success")
        self.assertEqual(
            [msg.event_kind for msg in result.egress_messages], ["completion"]
        )
        self.assertEqual(
            self._audit_detail()["incremental_reply_send_published_count"], 1
        )

    def test_internal_heartbeat_is_handled_without_executor(self) -> None:
        task = _task(
            "internal-heartbeat:2026-03-09T12:00:00+00:00:1",
            ReplyChannel(
                type=INTERNAL_REPLY_CHANNEL_TYPE, target=INTERNAL_HEARTBEAT_TARGET
            ),
            source=INTERNAL_SOURCE,
            content="internal heartbeat ping",
        )
        result = self._process(task, StepExecutor())

        self.assertEqual(result.run_record.result_status, "success")
        self.assertEqual(result.run_record.source, "internal")
        visible, completion = result.egress_messages
        self.assertEqual(visible.message.channel, "log")
        self.assertEqual(visible.message.target, "heartbeat")
        self.assertEqual(json.loads(visible.message.body)["kind"], "heartbeat_pong")
        self.assertEqual(completion.event_kind, "completion")
        self.assertEqual(result.reason_codes, ["internal_heartbeat"])
        self.assertEqual(self._audit_detail()["heartbeat"]["kind"], "heartbeat_pong")

    def test_internal_notices_are_handled_without_executor(self) -> None:
        for kind, notice, target in (
            ("channel", TELEGRAM_CHANNEL_NOT_ENABLED_NOTICE, "-100777"),
            ("group", TELEGRAM_GROUP_NOT_ENABLED_NOTICE, "-100888"),
        ):
            with self.subTest(notice):
                self.setUp()
                body = f"Not enabled in {kind} {target}."
                task = _task(
                    f"telegram-disallowed-{kind}:1",
                    ReplyChannel(
                        type="telegram",
                        target=target,
                        metadata={
                            INTERNAL_NOTICE_METADATA_KEY: notice,
                            "message_id": 1,
                        },
                    ),
                    source="internal",
                    content=body,
                )
                result = self._process(task, StepExecutor())

                self.assertEqual(result.run_record.result_status, "success")
                visible, completion = result.egress_messages
                self.assertEqual(visible.message.channel, "telegram")
                self.assertEqual(visible.message.target, target)
                self.assertEqual(visible.message.body, body)
                self.assertEqual(completion.sequence, 1)
                self.assertEqual(result.reason_codes, ["internal_notice"])
                self.assertEqual(self._audit_detail()["internal_notice"], notice)
