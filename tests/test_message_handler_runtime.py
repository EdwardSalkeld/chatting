import tempfile
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from app.main_message_handler import _handle_egress_message
from app.broker import EgressQueueMessage, TaskQueueMessage
from app.message_handler_runtime import TaskLedgerStore
from app.models import ApplyResult, OutboundMessage, ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore


@dataclass
class _RecordingApplier:
    apply_calls: int = 0

    def apply(self, decision, envelope=None):
        del decision, envelope
        self.apply_calls += 1
        return ApplyResult(
            applied_actions=[],
            skipped_actions=[],
            dispatched_messages=[
                OutboundMessage(channel="email", target="alice@example.com", body="ok")
            ],
            reason_codes=[],
        )


class MessageHandlerRuntimeTests(unittest.TestCase):
    def _build_task_message(self) -> TaskQueueMessage:
        envelope = TaskEnvelope(
            id="email:1",
            source="email",
            received_at=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
            actor="alice@example.com",
            content="hello",
            attachments=[],
            context_refs=[],
            policy_profile="default",
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:1",
        )
        return TaskQueueMessage.from_envelope(envelope, trace_id="trace:email:1")

    def test_handle_egress_message_drops_unknown_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            applier = _RecordingApplier()
            acked: list[str] = []

            payload = EgressQueueMessage(
                task_id="task:missing",
                envelope_id="email:missing",
                trace_id="trace:missing",
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-1",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-1"])
            self.assertEqual(applier.apply_calls, 0)

    def test_handle_egress_message_drops_disallowed_channel(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = self._build_task_message()
            ledger.record_task(task_message)
            applier = _RecordingApplier()
            acked: list[str] = []

            payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="telegram", target="123", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-2",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-2"])
            self.assertEqual(applier.apply_calls, 0)

    def test_handle_egress_message_dispatches_and_marks_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = self._build_task_message()
            ledger.record_task(task_message)
            applier = _RecordingApplier()
            acked: list[str] = []

            payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=2,
                message=OutboundMessage(channel="email", target="alice@example.com", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-3",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-3"])
            self.assertEqual(applier.apply_calls, 1)
            self.assertEqual(store.list_dispatched_event_indices(run_id=task_message.task_id), [0])


if __name__ == "__main__":
    unittest.main()
