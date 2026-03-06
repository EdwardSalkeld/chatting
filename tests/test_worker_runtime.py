import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.broker import TaskQueueMessage
from app.models import (
    ActionProposal,
    ExecutionResult,
    OutboundMessage,
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
        return ExecutionResult(
            messages=[
                OutboundMessage(channel="final", target="ignored", body="one"),
                OutboundMessage(channel="final", target="ignored", body="two"),
                OutboundMessage(channel="email", target="ops@example.com", body="three"),
            ],
            actions=[],
            config_updates=[],
            requires_human_review=False,
            errors=[],
        )


@dataclass(frozen=True)
class WriteFileExecutor:
    def execute(self, task):
        del task
        return ExecutionResult(
            messages=[OutboundMessage(channel="final", target="ignored", body="done")],
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
            policy_profile="default",
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:1",
        )
        return TaskQueueMessage.from_envelope(envelope, trace_id="trace:email:1")

    def test_process_task_message_emits_multiple_egress_messages(self) -> None:
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
            self.assertEqual(len(result.egress_messages), 3)
            self.assertEqual(result.egress_messages[0].event_index, 0)
            self.assertEqual(result.egress_messages[0].event_count, 3)
            self.assertEqual(result.egress_messages[0].message.target, "alice@example.com")
            self.assertEqual(result.egress_messages[1].message.body, "two")
            self.assertEqual(result.egress_messages[2].message.target, "ops@example.com")

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


if __name__ == "__main__":
    unittest.main()
