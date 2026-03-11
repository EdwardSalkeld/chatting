import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json

from app.main_message_handler import (
    EgressTelemetryRollup,
    HeartbeatTelemetryRollup,
    MessageHandlerMetrics,
    _handle_egress_message,
    _prepare_ingress_envelope,
    _render_prometheus_metrics,
)
from app.broker import EgressQueueMessage, TaskQueueMessage
from app.internal_heartbeat import build_internal_heartbeat_egress, build_internal_heartbeat_envelope
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

    def _build_internal_heartbeat_task_message(self) -> TaskQueueMessage:
        return TaskQueueMessage.from_envelope(
            build_internal_heartbeat_envelope(
                sequence=1,
                now=datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc),
            ),
            trace_id="trace:internal:heartbeat:1",
        )

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
                event_id="evt:task:missing:0",
                sequence=0,
                event_kind="final",
                message_type="chatting.egress.v2",
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

    def test_handle_egress_message_drops_completed_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = self._build_task_message()
            ledger.record_task(task_message)
            ledger.mark_task_completed(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
            )
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
                event_id="evt:task:email:1:0",
                sequence=0,
                event_kind="final",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-completed",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-completed"])
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
                event_id="evt:task:email:1:0",
                sequence=0,
                event_kind="final",
                message_type="chatting.egress.v2",
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
                event_id="evt:task:email:1:0",
                sequence=0,
                event_kind="final",
                message_type="chatting.egress.v2",
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
                    event_id="evt:task:email:1:0",
                )
            )
            self.assertIsNone(ledger.get_task(task_message.task_id))
            self.assertTrue(
                ledger.is_task_completed(
                    task_id=task_message.task_id,
                    envelope_id=task_message.envelope.id,
                )
            )

    def test_handle_egress_message_accepts_terminal_drop_marker(self) -> None:
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
                message=OutboundMessage(
                    channel="drop",
                    target="task",
                    body="Worker completed without final reply; marking task complete.",
                ),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:0:final:drop",
                sequence=0,
                event_kind="final",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-drop-terminal",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-drop-terminal"])
            self.assertEqual(applier.apply_calls, 1)
            self.assertTrue(
                ledger.is_task_completed(
                    task_id=task_message.task_id,
                    envelope_id=task_message.envelope.id,
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

    def test_handle_egress_message_dispatches_unsequenced_incremental_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = self._build_task_message()
            ledger.record_task(task_message)
            acked: list[str] = []
            applied_bodies: list[str] = []

            @dataclass
            class _BodyRecordingApplier:
                def apply(self, decision, envelope=None):
                    del envelope
                    applied_bodies.append(decision.approved_messages[0].body)
                    return ApplyResult(
                        applied_actions=[],
                        skipped_actions=[],
                        dispatched_messages=decision.approved_messages,
                        reason_codes=[],
                    )

            applier = _BodyRecordingApplier()
            payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="working on it"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:adhoc",
                sequence=None,
                event_kind="incremental",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-adhoc-1",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-adhoc-1"])
            self.assertEqual(applied_bodies, ["working on it"])
            self.assertTrue(
                store.has_dispatched_event_id(
                    task_id=task_message.task_id,
                    event_id="evt:task:email:1:adhoc",
                )
            )
            self.assertEqual(store.list_dispatched_event_indices(run_id=task_message.task_id), [])

    def test_handle_egress_message_marks_outbox_event_acked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = self._build_task_message()
            ledger.record_task(task_message)
            applier = _RecordingApplier()
            acked: list[str] = []

            queued = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:outbox",
                sequence=0,
                event_kind="final",
                message_type="chatting.egress.v2",
            )
            store.queue_egress_outbox_event(queued)
            store.mark_egress_outbox_event_published(event_id="evt:task:email:1:outbox")

            _handle_egress_message(
                picked_guid="guid-outbox-1",
                picked_payload=queued.to_dict(),
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-outbox-1"])
            self.assertEqual(applier.apply_calls, 1)
            self.assertEqual(store.list_replayable_egress_outbox_events(), [])

    def test_handle_egress_message_allows_internal_heartbeat_log_channel_and_records_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = self._build_internal_heartbeat_task_message()
            ledger.record_task(task_message)
            heartbeat_telemetry = HeartbeatTelemetryRollup()
            heartbeat_telemetry.record_sent(sent_at=task_message.emitted_at)
            acked: list[str] = []
            applied_messages: list[tuple[str, str, str]] = []

            @dataclass
            class _HeartbeatApplier:
                apply_calls: int = 0

                def apply(self, decision, envelope=None):
                    del envelope
                    self.apply_calls += 1
                    applied_messages.extend(
                        (message.channel, message.target, message.body)
                        for message in decision.approved_messages
                    )
                    return ApplyResult(
                        applied_actions=[],
                        skipped_actions=[],
                        dispatched_messages=decision.approved_messages,
                        reason_codes=[],
                    )

            applier = _HeartbeatApplier()
            queued = build_internal_heartbeat_egress(
                task_message=task_message,
                worker_received_at=task_message.emitted_at,
            )

            _handle_egress_message(
                picked_guid="guid-heartbeat-1",
                picked_payload=queued.to_dict(),
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
                heartbeat_telemetry=heartbeat_telemetry,
            )

            self.assertEqual(acked, ["guid-heartbeat-1"])
            self.assertEqual(applier.apply_calls, 1)
            self.assertEqual(len(applied_messages), 1)
            self.assertEqual(applied_messages[0][0], "log")
            self.assertEqual(applied_messages[0][1], "heartbeat")
            self.assertEqual(json.loads(applied_messages[0][2])["kind"], "heartbeat_pong")
            snapshot = heartbeat_telemetry.snapshot()
            self.assertEqual(snapshot["sent_total"], 1)
            self.assertEqual(snapshot["received_total"], 1)
            self.assertNotEqual(snapshot["latest_sent_at"], "none")
            self.assertNotEqual(snapshot["latest_received_at"], "none")
            self.assertIsInstance(snapshot["latest_latency_ms"], int)
            self.assertGreaterEqual(snapshot["latest_latency_ms"], 0)


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
                event_id="evt:task:telegram:200:0",
                sequence=0,
                event_kind="final",
                message_type="chatting.egress.v2",
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

    def test_egress_telemetry_rollup_reports_dedupe_rate_and_latency(self) -> None:
        telemetry = EgressTelemetryRollup()
        telemetry.record_received()
        telemetry.record_received()
        telemetry.record_dispatched(event_kind="incremental", latency_ms=120)
        telemetry.record_dispatched(event_kind="final", latency_ms=80)
        telemetry.record_deduped()
        telemetry.record_dropped(reason="unknown_task")
        telemetry.record_dropped(reason="disallowed_channel")
        snapshot = telemetry.snapshot()

        self.assertEqual(snapshot["received_total"], 2)
        self.assertEqual(snapshot["dispatched_total"], 2)
        self.assertEqual(snapshot["incremental_dispatched_total"], 1)
        self.assertEqual(snapshot["final_dispatched_total"], 1)
        self.assertEqual(snapshot["deduped_total"], 1)
        self.assertEqual(snapshot["dedupe_hit_rate_pct"], 33.33)
        self.assertEqual(snapshot["dispatch_latency_ms_avg"], 100.0)
        self.assertEqual(snapshot["dispatch_latency_ms_max"], 120)
        self.assertEqual(snapshot["dropped_total"], 2)
        self.assertEqual(snapshot["dropped_unknown_task_total"], 1)
        self.assertEqual(snapshot["dropped_completed_task_total"], 0)
        self.assertEqual(snapshot["dropped_disallowed_channel_total"], 1)
        self.assertEqual(snapshot["dropped_missing_event_id_total"], 0)

    def test_message_handler_metrics_snapshot_rolls_up_loop_stats(self) -> None:
        current_monotonic = 100.0

        def _monotonic() -> float:
            return current_monotonic

        metrics = MessageHandlerMetrics(
            started_at=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
            monotonic_fn=_monotonic,
        )
        telemetry_snapshot = {
            "received_total": 2,
            "dispatched_total": 1,
            "deduped_total": 1,
            "dedupe_hit_rate_pct": 50.0,
            "dropped_total": 1,
            "dropped_unknown_task_total": 1,
            "dropped_completed_task_total": 0,
            "dropped_disallowed_channel_total": 0,
            "dropped_missing_event_id_total": 0,
            "incremental_dispatched_total": 1,
            "final_dispatched_total": 0,
            "dispatch_latency_ms_avg": 120.5,
            "dispatch_latency_ms_max": 121,
        }

        current_monotonic = 160.0
        metrics.record_loop(
            ingress_published=3,
            github_scanned_events=5,
            github_new_events=2,
            github_published=1,
            telemetry_snapshot=telemetry_snapshot,
            completed_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
        )
        snapshot = metrics.snapshot()

        self.assertEqual(snapshot["uptime_seconds"], 60.0)
        self.assertEqual(snapshot["process_start_time_seconds"], 1772798400.0)
        self.assertEqual(snapshot["loops_total"], 1)
        self.assertEqual(snapshot["ingress_published_total"], 3)
        self.assertEqual(snapshot["github_scanned_events_total"], 5)
        self.assertEqual(snapshot["github_new_events_total"], 2)
        self.assertEqual(snapshot["github_published_total"], 1)
        self.assertEqual(snapshot["last_loop_completed_timestamp_seconds"], 1772798460.0)
        self.assertEqual(snapshot["egress_loops_total"], 0)
        self.assertEqual(snapshot["last_egress_loop_completed_timestamp_seconds"], 0.0)
        self.assertEqual(snapshot["received_total"], 2)
        self.assertEqual(snapshot["dispatch_latency_ms_avg"], 120.5)

    def test_render_prometheus_metrics_formats_expected_samples(self) -> None:
        rendered = _render_prometheus_metrics(
            {
                "uptime_seconds": 60.0,
                "process_start_time_seconds": 1772798400.0,
                "loops_total": 2,
                "ingress_published_total": 4,
                "github_scanned_events_total": 5,
                "github_new_events_total": 3,
                "github_published_total": 1,
                "last_loop_completed_timestamp_seconds": 1772798460.0,
                "egress_loops_total": 5,
                "last_egress_loop_completed_timestamp_seconds": 1772798461.0,
                "received_total": 7,
                "dispatched_total": 6,
                "deduped_total": 1,
                "dedupe_hit_rate_pct": 14.29,
                "dropped_total": 2,
                "dropped_unknown_task_total": 1,
                "dropped_completed_task_total": 0,
                "dropped_disallowed_channel_total": 1,
                "dropped_missing_event_id_total": 0,
                "incremental_dispatched_total": 4,
                "final_dispatched_total": 2,
                "dispatch_latency_ms_avg": 99.5,
                "dispatch_latency_ms_max": 140,
            }
        )

        self.assertIn("# TYPE chatting_message_handler_loops_total counter", rendered)
        self.assertIn("chatting_message_handler_uptime_seconds 60.000000", rendered)
        self.assertIn(
            "chatting_message_handler_process_start_time_seconds 1772798400.000000",
            rendered,
        )
        self.assertIn("chatting_message_handler_loops_total 2", rendered)
        self.assertIn("chatting_message_handler_github_published_total 1", rendered)
        self.assertIn("chatting_message_handler_egress_loops_total 5", rendered)
        self.assertIn("chatting_message_handler_egress_received_total 7", rendered)
        self.assertIn(
            "chatting_message_handler_egress_dedupe_hit_rate_pct 14.290000",
            rendered,
        )
        self.assertIn(
            "chatting_message_handler_egress_dispatch_latency_ms_avg 99.500000",
            rendered,
        )


if __name__ == "__main__":
    unittest.main()
