import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json

from app.broker import TaskQueueMessage
from app.internal_heartbeat import build_internal_heartbeat_envelope
from app.models import (
    ActionProposal,
    ExecutionResult,
    ReplyChannel,
    TaskEnvelope,
)
from app.policy import AllowlistPolicyEngine
from app.router import RuleBasedRouter
from app.state import SQLiteStateStore
from app.worker_runtime import process_task_message
@dataclass(frozen=True)
class MultiMessageExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(actions=[], config_updates=[], requires_human_review=False, errors=[])
@dataclass(frozen=True)
class WriteFileExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(
            actions=[ActionProposal(type="write_file", path="output.txt", content="hello")],
            config_updates=[],
            requires_human_review=False,
            errors=[],
        )
@dataclass(frozen=True)
class AlwaysFailExecutor:
    def execute(self, task):
        del task
        raise RuntimeError("executor down")
@dataclass(frozen=True)
class IncrementalReplyExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(
            actions=[],
            config_updates=[],
            requires_human_review=False,
            errors=[],
        )
@dataclass(frozen=True)
class NoMessageExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(actions=[], config_updates=[], requires_human_review=False, errors=[])
@dataclass(frozen=True)
class FinalAliasExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(actions=[], config_updates=[], requires_human_review=False, errors=[])
class WorkerRuntimeTests(unittest.TestCase):
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

    def _build_internal_heartbeat_task_message(self) -> TaskQueueMessage:
        return TaskQueueMessage.from_envelope(
            build_internal_heartbeat_envelope(
                sequence=1,
                now=datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc),
            ),
            trace_id="trace:internal:heartbeat:1",
        )

    def test_process_task_message_emits_completion_only_for_successful_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                router=RuleBasedRouter(),
                executor_impl=MultiMessageExecutor(),
                policy=AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"})),
                max_attempts=2,
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(len(result.egress_messages), 1)
            self.assertEqual(result.egress_messages[0].event_kind, "completion")
            self.assertEqual(result.egress_messages[0].message.channel, "internal")

    def test_process_task_message_marks_dropped_actions_reason_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                router=RuleBasedRouter(),
                executor_impl=WriteFileExecutor(),
                policy=AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"})),
                max_attempts=2,
            )

            self.assertEqual(result.run_record.result_status, "success")
            audit_event = store.list_audit_events()[0]
            self.assertIn(
                "approved_actions_not_forwarded_to_egress",
                audit_event.detail["reason_codes"],
            )

    def test_process_task_message_retries_and_dead_letters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                router=RuleBasedRouter(),
                executor_impl=AlwaysFailExecutor(),
                policy=AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"})),
                max_attempts=2,
            )

            self.assertEqual(result.run_record.result_status, "dead_letter")
            self.assertEqual(result.dead_lettered, True)
            self.assertEqual(store.list_dead_letters()[0].reason_codes, ["retry_exhausted"])

    def test_process_task_message_emits_completion_even_when_executor_does_no_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                router=RuleBasedRouter(),
                executor_impl=IncrementalReplyExecutor(),
                policy=AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"})),
                max_attempts=2,
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(len(result.egress_messages), 1)
            self.assertEqual(result.egress_messages[0].event_kind, "completion")
            audit_event = store.list_audit_events()[0]
            self.assertEqual(audit_event.detail["incremental_reply_send_requested_count"], 0)
            self.assertEqual(audit_event.detail["incremental_reply_send_published_count"], 0)

    def test_process_task_message_ignores_incremental_reply_policy_when_executor_returns_completion_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                router=RuleBasedRouter(),
                executor_impl=IncrementalReplyExecutor(),
                policy=AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"})),
                max_attempts=2,
            )

            self.assertEqual(len(result.egress_messages), 1)
            self.assertEqual(result.egress_messages[0].event_kind, "completion")
            audit_event = store.list_audit_events()[0]
            self.assertNotIn("incremental_reply_send_not_allowed", audit_event.detail["reason_codes"])

    def test_process_task_message_handles_internal_heartbeat_without_executor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_internal_heartbeat_task_message(),
                router=RuleBasedRouter(),
                executor_impl=AlwaysFailExecutor(),
                policy=AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"})),
                max_attempts=2,
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(result.run_record.workflow, "internal_heartbeat")
            self.assertEqual(result.run_record.source, "internal")
            self.assertEqual(len(result.egress_messages), 2)
            visible_egress_message = result.egress_messages[0]
            completion_egress_message = result.egress_messages[1]
            self.assertEqual(visible_egress_message.event_kind, "message")
            self.assertEqual(visible_egress_message.message.channel, "log")
            self.assertEqual(visible_egress_message.message.target, "heartbeat")
            self.assertEqual(json.loads(visible_egress_message.message.body)["kind"], "heartbeat_pong")
            self.assertEqual(completion_egress_message.event_kind, "completion")
            audit_event = store.list_audit_events()[0]
            self.assertEqual(audit_event.workflow, "internal_heartbeat")
            self.assertEqual(audit_event.detail["reason_codes"], ["internal_heartbeat"])
            self.assertEqual(audit_event.detail["heartbeat"]["kind"], "heartbeat_pong")

    def test_process_task_message_emits_internal_completion_when_no_visible_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                router=RuleBasedRouter(),
                executor_impl=NoMessageExecutor(),
                policy=AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"})),
                max_attempts=2,
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(len(result.egress_messages), 1)
            terminal = result.egress_messages[0]
            self.assertEqual(terminal.event_kind, "completion")
            self.assertEqual(terminal.message.channel, "internal")
            self.assertEqual(terminal.message.target, "task")
            self.assertEqual(terminal.sequence, 0)

if __name__ == "__main__":
    unittest.main()
