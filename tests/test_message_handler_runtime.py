import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.main_message_handler import _handle_egress_message, _prepare_ingress_envelope
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

    def _build_telegram_envelope(self, *, envelope_id: str, content: str, target: str = "12345") -> TaskEnvelope:
        return TaskEnvelope(
            id=envelope_id,
            source="im",
            received_at=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
            actor="77:alice",
            content=content,
            attachments=[],
            context_refs=[],
            policy_profile="default",
            reply_channel=ReplyChannel(type="telegram", target=target),
            dedupe_key=envelope_id,
        )

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
            self.assertTrue(
                store.has_dispatched_event_id(
                    task_id=task_message.task_id,
                    event_id="v1:task:email:1:0",
                )
            )

    def test_handle_egress_message_skips_duplicate_event_id(self) -> None:
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
                message=OutboundMessage(channel="email", target="alice@example.com", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:1",
                sequence=0,
                event_kind="incremental",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-4",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )
            _handle_egress_message(
                picked_guid="guid-5",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-4", "guid-5"])
            self.assertEqual(applier.apply_calls, 1)


    def test_handle_egress_message_buffers_until_expected_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = self._build_task_message()
            ledger.record_task(task_message)
            acked: list[str] = []
            applied_bodies: list[str] = []

            @dataclass
            class _OrderAwareApplier:
                def apply(self, decision, envelope=None):
                    del envelope
                    applied_bodies.append(decision.approved_messages[0].body)
                    return ApplyResult(
                        applied_actions=[],
                        skipped_actions=[],
                        dispatched_messages=decision.approved_messages,
                        reason_codes=[],
                    )

            applier = _OrderAwareApplier()
            out_of_order_payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=1,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="second"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:1",
                sequence=1,
                event_kind="incremental",
                message_type="chatting.egress.v2",
            ).to_dict()
            first_payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="first"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:0",
                sequence=0,
                event_kind="incremental",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-7",
                picked_payload=out_of_order_payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )
            self.assertEqual(acked, ["guid-7"])
            self.assertEqual(applied_bodies, [])

            _handle_egress_message(
                picked_guid="guid-8",
                picked_payload=first_payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-7", "guid-8"])
            self.assertEqual(applied_bodies, ["first", "second"])
            self.assertEqual(store.list_dispatched_event_indices(run_id=task_message.task_id), [0, 1])

    def test_prepare_ingress_envelope_enriches_telegram_with_last_30_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            for index in range(35):
                store.append_conversation_turn(
                    channel="telegram",
                    target="12345",
                    role="user",
                    content=f"turn-{index}",
                    run_id=f"run:{index}",
                )

            envelope = self._build_telegram_envelope(envelope_id="telegram:100", content="latest-turn")

            prepared = _prepare_ingress_envelope(
                store=store,
                envelope=envelope,
                run_id="task:telegram:100",
            )

            self.assertIn("Recent conversation context (oldest first):", prepared.content)
            self.assertIn("user: turn-5", prepared.content)
            self.assertIn("user: turn-24", prepared.content)
            self.assertNotIn("user: turn-4", prepared.content)
            self.assertIn("Current user message:\nlatest-turn", prepared.content)
            turns = store.list_recent_conversation_turns(channel="telegram", target="12345", limit=1)
            self.assertEqual(turns, [("user", "latest-turn")])

    def test_handle_egress_message_persists_matching_telegram_dispatch_as_assistant_turn(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = TaskQueueMessage.from_envelope(
                self._build_telegram_envelope(envelope_id="telegram:200", content="hello"),
                trace_id="trace:telegram:200",
            )
            ledger.record_task(task_message)

            @dataclass
            class _TelegramApplier:
                apply_calls: int = 0

                def apply(self, decision, envelope=None):
                    del decision, envelope
                    self.apply_calls += 1
                    return ApplyResult(
                        applied_actions=[],
                        skipped_actions=[],
                        dispatched_messages=[
                            OutboundMessage(channel="telegram", target="12345", body="reply-1"),
                            OutboundMessage(channel="telegram", target="99999", body="reply-2"),
                        ],
                        reason_codes=[],
                    )

            applier = _TelegramApplier()
            acked: list[str] = []

            payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="telegram", target="12345", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-6",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"telegram"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-6"])
            self.assertEqual(applier.apply_calls, 1)
            turns = store.list_recent_conversation_turns(channel="telegram", target="12345", limit=5)
            self.assertEqual(turns, [("assistant", "reply-1")])


if __name__ == "__main__":
    unittest.main()
