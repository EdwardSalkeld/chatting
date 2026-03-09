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


@dataclass(frozen=True)
class IncrementalReplyExecutor:
    def execute(self, task, reply_send=None):
        del task
        if reply_send is not None:
            reply_send({"body": "working on it"})
            reply_send({"body": "still working"})
        return ExecutionResult(
            messages=[OutboundMessage(channel="final", target="ignored", body="done")],
            actions=[],
            config_updates=[],
            requires_human_review=False,
            errors=[],
        )


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

    def _build_internal_heartbeat_task_message(self) -> TaskQueueMessage:
        return TaskQueueMessage.from_envelope(
            build_internal_heartbeat_envelope(
                sequence=1,
                now=datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc),
            ),
            trace_id="trace:internal:heartbeat:1",
        )

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

    def test_process_task_message_emits_incremental_and_final_v2_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            policy = AllowlistPolicyEngine(
                allowed_action_types=frozenset({"write_file"}),
                allow_incremental_reply_send=True,
            )
            result = process_task_message(
                store=store,
                task_message=self._build_task_message(),
                router=RuleBasedRouter(),
                executor_impl=IncrementalReplyExecutor(),
                policy=policy,
                max_attempts=2,
            )

            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(len(result.egress_messages), 3)
            self.assertEqual(result.egress_messages[0].event_kind, "incremental")
            self.assertEqual(result.egress_messages[1].event_kind, "incremental")
            self.assertEqual(result.egress_messages[2].event_kind, "final")
            self.assertEqual([item.sequence for item in result.egress_messages], [0, 1, 2])
            self.assertEqual(
                [item.message_type for item in result.egress_messages],
                ["chatting.egress.v2", "chatting.egress.v2", "chatting.egress.v2"],
            )

    def test_process_task_message_blocks_incremental_reply_send_when_policy_disabled(self) -> None:
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
            self.assertEqual(result.egress_messages[0].event_kind, "final")
            audit_event = store.list_audit_events()[0]
            self.assertIn("incremental_reply_send_not_allowed", audit_event.detail["reason_codes"])

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
            self.assertEqual(len(result.egress_messages), 1)
            egress_message = result.egress_messages[0]
            self.assertEqual(egress_message.message.channel, "log")
            self.assertEqual(egress_message.message.target, "heartbeat")
            self.assertEqual(json.loads(egress_message.message.body)["kind"], "heartbeat_pong")
            audit_event = store.list_audit_events()[0]
            self.assertEqual(audit_event.workflow, "internal_heartbeat")
            self.assertEqual(audit_event.detail["reason_codes"], ["internal_heartbeat"])
            self.assertEqual(audit_event.detail["heartbeat"]["kind"], "heartbeat_pong")


if __name__ == "__main__":
    unittest.main()
