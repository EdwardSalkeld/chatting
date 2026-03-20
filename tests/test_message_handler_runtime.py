import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
from unittest.mock import patch

from app.applier import MessageDispatchError
from app.main_message_handler import (
    EgressTelemetryRollup,
    HeartbeatTelemetryRollup,
    IngressPollFailureLogState,
    MessageHandlerMetrics,
    TelegramAttachmentCleanupSettings,
    _handle_egress_message,
    _log_ingress_poll_failure,
    _prepare_ingress_envelope,
    _resolve_error_email_recipient,
    _render_prometheus_metrics,
)
from app.broker import EgressQueueMessage, TaskQueueMessage
from app.internal_heartbeat import build_internal_heartbeat_egress, build_internal_heartbeat_envelope
from app.message_handler_runtime import (
    TaskLedgerStore,
    TelegramAttachmentStore,
    cleanup_telegram_attachments,
)
from app.models import ApplyResult, AttachmentRef, OutboundMessage, ReplyChannel, TaskEnvelope
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
@dataclass
class _DispatchFailingApplier:
    apply_calls: int = 0

    def apply(self, decision, envelope=None):
        del decision, envelope
        self.apply_calls += 1
        raise MessageDispatchError(
            reason_code="telegram_dispatch_failed",
            dispatched_messages=[],
        )
@dataclass
class _RecordingAlertEmailSender:
    sent: list[tuple[str, str, str | None]]

    def send(self, target: str, body: str, *, subject: str | None = None) -> None:
        self.sent.append((target, body, subject))
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

    def _build_telegram_attachment_task_message(self, attachment_uri: str) -> TaskQueueMessage:
        envelope = TaskEnvelope(
            id="telegram:photo-1",
            source="im",
            received_at=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
            actor="77:alice",
            content="[photo attached]",
            attachments=[AttachmentRef(uri=attachment_uri, name="telegram-photo.jpg")],
            context_refs=[],
            reply_channel=ReplyChannel(type="telegram", target="12345"),
            dedupe_key="telegram:photo-1",
        )
        return TaskQueueMessage.from_envelope(envelope, trace_id="trace:telegram:photo-1")

    def _build_telegram_envelope(self, *, envelope_id: str, content: str, target: str = "12345") -> TaskEnvelope:
        return TaskEnvelope(
            id=envelope_id,
            source="im",
            received_at=datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
            actor="77:alice",
            content=content,
            attachments=[],
            context_refs=[],
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
                event_kind="message",
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
                event_kind="message",
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
                event_kind="message",
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

    def test_handle_egress_message_dispatches_visible_message_without_closing_task(self) -> None:
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
                event_kind="message",
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
            self.assertIsNotNone(ledger.get_task(task_message.task_id))
            self.assertFalse(
                ledger.is_task_completed(
                    task_id=task_message.task_id,
                    envelope_id=task_message.envelope.id,
                )
            )

    def test_handle_egress_message_marks_telegram_attachments_cleanup_eligible_on_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            attachment_dir = Path(tmpdir) / "attachments"
            attachment_dir.mkdir()
            attachment_path = attachment_dir / "telegram-photo.jpg"
            attachment_path.write_bytes(b"photo-bytes")

            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            attachment_store = TelegramAttachmentStore(db_path)
            task_message = self._build_telegram_attachment_task_message(attachment_path.resolve().as_uri())
            ledger.record_task(task_message)
            tracked = attachment_store.record_task_attachments(
                task_message=task_message,
                attachment_root_dir=str(attachment_dir),
            )
            self.assertEqual(tracked, 1)
            applier = _RecordingApplier()
            acked: list[str] = []

            payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="log", target="ops", body="done"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:telegram:photo-1:0",
                sequence=0,
                event_kind="completion",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-telegram-completion",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                attachment_store=attachment_store,
                attachment_cleanup_settings=TelegramAttachmentCleanupSettings(
                    attachment_root_dir=str(attachment_dir),
                    cleanup_grace_seconds=3600,
                    max_age_seconds=86400,
                ),
                allowed_egress_channels={"log"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-telegram-completion"])
            records = attachment_store.list_records()
            self.assertEqual(len(records), 1)
            self.assertIsNotNone(records[0].eligible_after)
            self.assertTrue(attachment_path.exists())

    def test_handle_egress_message_tracks_outbound_attachment_for_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            attachment_dir = Path(tmpdir) / "attachments"
            attachment_dir.mkdir()
            attachment_path = attachment_dir / "menu.pdf"
            attachment_path.write_bytes(b"%PDF")

            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            attachment_store = TelegramAttachmentStore(db_path)
            task_message = self._build_telegram_envelope(
                envelope_id="telegram:outbound-1",
                content="send the menu",
            )
            queued_task = TaskQueueMessage.from_envelope(
                task_message,
                trace_id="trace:telegram:outbound-1",
            )
            ledger.record_task(queued_task)
            acked: list[str] = []

            @dataclass
            class _AttachmentApplier:
                def apply(self, decision, envelope=None):
                    del decision, envelope
                    dispatched = OutboundMessage(
                        channel="telegram",
                        target="12345",
                        body="menu attached",
                        attachment=AttachmentRef(
                            uri=attachment_path.resolve().as_uri(),
                            name="menu.pdf",
                        ),
                    )
                    return ApplyResult(
                        applied_actions=[],
                        skipped_actions=[],
                        dispatched_messages=[dispatched],
                        reason_codes=[],
                    )

            payload = EgressQueueMessage(
                task_id=queued_task.task_id,
                envelope_id=queued_task.envelope.id,
                trace_id=queued_task.trace_id,
                event_index=0,
                event_count=2,
                message=OutboundMessage(channel="telegram", target="12345", body="menu attached"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:telegram:outbound-1:0",
                sequence=0,
                event_kind="message",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-telegram-outbound",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                attachment_store=attachment_store,
                attachment_cleanup_settings=TelegramAttachmentCleanupSettings(
                    attachment_root_dir=str(attachment_dir),
                    cleanup_grace_seconds=3600,
                    max_age_seconds=86400,
                ),
                allowed_egress_channels={"telegram"},
                applier=_AttachmentApplier(),
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-telegram-outbound"])
            records = attachment_store.list_records()
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].attachment_path, str(attachment_path.resolve()))
            self.assertIsNone(records[0].eligible_after)

            completion_payload = EgressQueueMessage(
                task_id=queued_task.task_id,
                envelope_id=queued_task.envelope.id,
                trace_id=queued_task.trace_id,
                event_index=1,
                event_count=2,
                message=OutboundMessage(channel="internal", target="task", body="done"),
                emitted_at=datetime(2026, 3, 6, 12, 2, tzinfo=timezone.utc),
                event_id="evt:task:telegram:outbound-1:1",
                sequence=1,
                event_kind="completion",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-telegram-outbound-completion",
                picked_payload=completion_payload,
                ledger=ledger,
                store=store,
                attachment_store=attachment_store,
                attachment_cleanup_settings=TelegramAttachmentCleanupSettings(
                    attachment_root_dir=str(attachment_dir),
                    cleanup_grace_seconds=3600,
                    max_age_seconds=86400,
                ),
                allowed_egress_channels={"telegram"},
                applier=_AttachmentApplier(),
                ack_callback=acked.append,
            )

            self.assertEqual(
                acked,
                ["guid-telegram-outbound", "guid-telegram-outbound-completion"],
            )
            completion_records = attachment_store.list_records()
            self.assertEqual(len(completion_records), 1)
            self.assertIsNotNone(completion_records[0].eligible_after)

    def test_cleanup_telegram_attachments_deletes_only_after_completion_grace(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            attachment_dir = Path(tmpdir) / "attachments"
            attachment_dir.mkdir()
            attachment_path = attachment_dir / "telegram-photo.jpg"
            attachment_path.write_bytes(b"photo-bytes")

            attachment_store = TelegramAttachmentStore(db_path)
            task_message = self._build_telegram_attachment_task_message(attachment_path.resolve().as_uri())
            attachment_store.record_task_attachments(
                task_message=task_message,
                attachment_root_dir=str(attachment_dir),
            )
            attachment_store.mark_task_attachments_eligible(
                task_id=task_message.task_id,
                eligible_after=datetime(2026, 3, 6, 13, 0, tzinfo=timezone.utc),
            )

            before = cleanup_telegram_attachments(
                attachment_store=attachment_store,
                attachment_root_dir=str(attachment_dir),
                completion_grace_period=timedelta(hours=1),
                max_attachment_age=timedelta(days=30),
                now=datetime(2026, 3, 6, 12, 59, tzinfo=timezone.utc),
            )
            self.assertEqual(before.deleted_count, 0)
            self.assertTrue(attachment_path.exists())

            after = cleanup_telegram_attachments(
                attachment_store=attachment_store,
                attachment_root_dir=str(attachment_dir),
                completion_grace_period=timedelta(hours=1),
                max_attachment_age=timedelta(days=30),
                now=datetime(2026, 3, 6, 13, 1, tzinfo=timezone.utc),
            )

            self.assertEqual(after.deleted_count, 1)
            self.assertFalse(attachment_path.exists())

    def test_cleanup_telegram_attachments_deletes_orphaned_file_after_max_age(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            attachment_dir = Path(tmpdir) / "attachments"
            attachment_dir.mkdir()
            attachment_path = attachment_dir / "telegram-photo.jpg"
            attachment_path.write_bytes(b"photo-bytes")

            attachment_store = TelegramAttachmentStore(db_path)
            task_message = self._build_telegram_attachment_task_message(attachment_path.resolve().as_uri())
            attachment_store.record_task_attachments(
                task_message=task_message,
                attachment_root_dir=str(attachment_dir),
            )

            import sqlite3

            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    "UPDATE telegram_attachment_ledger SET created_at = ? WHERE task_id = ?",
                    ("2026-02-01T00:00:00Z", task_message.task_id),
                )
                connection.commit()

            result = cleanup_telegram_attachments(
                attachment_store=attachment_store,
                attachment_root_dir=str(attachment_dir),
                completion_grace_period=timedelta(days=7),
                max_attachment_age=timedelta(days=30),
                now=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(result.deleted_count, 1)
            self.assertFalse(attachment_path.exists())

    def test_handle_egress_message_applies_completion_without_external_dispatch(self) -> None:
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
                    channel="internal",
                    target="task",
                    body="Worker completed; task closure is internal-only.",
                ),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:0:completion",
                sequence=0,
                event_kind="completion",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-completion",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"email"},
                applier=applier,
                ack_callback=acked.append,
            )

            self.assertEqual(acked, ["guid-completion"])
            self.assertEqual(applier.apply_calls, 0)
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

    def test_handle_egress_message_drops_unsequenced_dispatch_failures_without_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = self._build_telegram_envelope(
                envelope_id="telegram:1",
                content="hello",
            )
            task_record = TaskQueueMessage.from_envelope(task_message, trace_id="trace:telegram:1")
            ledger.record_task(task_record)
            applier = _DispatchFailingApplier()
            acked: list[str] = []
            telemetry = EgressTelemetryRollup()

            payload = EgressQueueMessage(
                task_id=task_record.task_id,
                envelope_id=task_record.envelope.id,
                trace_id=task_record.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="telegram", target="12345", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:telegram:1:adhoc",
                sequence=None,
                event_kind="incremental",
                message_type="chatting.egress.v2",
            ).to_dict()

            with self.assertLogs("app.main_message_handler", level="ERROR") as captured:
                _handle_egress_message(
                    picked_guid="guid-telegram-fail",
                    picked_payload=payload,
                    ledger=ledger,
                    store=store,
                    allowed_egress_channels={"telegram"},
                    applier=applier,
                    ack_callback=acked.append,
                    telemetry=telemetry,
                )

            self.assertEqual(acked, ["guid-telegram-fail"])
            self.assertEqual(applier.apply_calls, 1)
            self.assertEqual(telemetry.dropped_total, 1)
            self.assertFalse(
                store.has_dispatched_event_id(
                    task_id=task_record.task_id,
                    event_id="evt:task:telegram:1:adhoc",
                )
            )
            self.assertIn("egress_drop_dispatch_failed", captured.output[0])

    def test_handle_egress_message_emails_dispatch_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_envelope = self._build_telegram_envelope(
                envelope_id="telegram:alert",
                content="hello",
            )
            task_message = TaskQueueMessage.from_envelope(task_envelope, trace_id="trace:telegram:alert")
            ledger.record_task(task_message)
            applier = _DispatchFailingApplier()
            alert_sender = _RecordingAlertEmailSender(sent=[])
            acked: list[str] = []

            payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="telegram", target="12345", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:telegram:alert:adhoc",
                sequence=None,
                event_kind="incremental",
                message_type="chatting.egress.v2",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-telegram-alert",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"telegram"},
                applier=applier,
                ack_callback=acked.append,
                error_email_sender=alert_sender,
                error_email_recipient="alerts@example.com",
            )

            self.assertEqual(acked, ["guid-telegram-alert"])
            self.assertEqual(len(alert_sender.sent), 1)
            recipient, body, subject = alert_sender.sent[0]
            self.assertEqual(recipient, "alerts@example.com")
            self.assertEqual(subject, "Chatting handler dispatch error: telegram_dispatch_failed")
            self.assertIn("task_id: task:telegram:alert", body)
            self.assertIn("event_id: evt:task:telegram:alert:adhoc", body)
            self.assertIn("reason_code: telegram_dispatch_failed", body)

    def test_handle_egress_message_continues_past_sequenced_dispatch_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_envelope = self._build_telegram_envelope(
                envelope_id="telegram:2",
                content="hello",
            )
            task_message = TaskQueueMessage.from_envelope(task_envelope, trace_id="trace:telegram:2")
            ledger.record_task(task_message)
            applier = _DispatchFailingApplier()
            acked: list[str] = []
            telemetry = EgressTelemetryRollup()

            failed_message = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=2,
                message=OutboundMessage(channel="telegram", target="12345", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:telegram:2:0",
                sequence=0,
                event_kind="message",
                message_type="chatting.egress.v2",
            ).to_dict()
            completion_message = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=1,
                event_count=2,
                message=OutboundMessage(channel="internal", target="task", body="done"),
                emitted_at=datetime(2026, 3, 6, 12, 2, tzinfo=timezone.utc),
                event_id="evt:task:telegram:2:1",
                sequence=1,
                event_kind="completion",
                message_type="chatting.egress.v2",
            ).to_dict()

            with self.assertLogs("app.main_message_handler", level="ERROR") as captured:
                _handle_egress_message(
                    picked_guid="guid-telegram-seq-fail",
                    picked_payload=failed_message,
                    ledger=ledger,
                    store=store,
                    allowed_egress_channels={"telegram"},
                    applier=applier,
                    ack_callback=acked.append,
                    telemetry=telemetry,
                )
                _handle_egress_message(
                    picked_guid="guid-telegram-seq-complete",
                    picked_payload=completion_message,
                    ledger=ledger,
                    store=store,
                    allowed_egress_channels={"telegram"},
                    applier=applier,
                    ack_callback=acked.append,
                    telemetry=telemetry,
                )

            self.assertEqual(
                acked,
                ["guid-telegram-seq-fail", "guid-telegram-seq-complete"],
            )
            self.assertEqual(applier.apply_calls, 1)
            self.assertEqual(telemetry.dropped_total, 1)
            self.assertFalse(
                store.has_dispatched_event_id(
                    task_id=task_message.task_id,
                    event_id="evt:task:telegram:2:0",
                )
            )
            self.assertTrue(
                store.has_dispatched_event_id(
                    task_id=task_message.task_id,
                    event_id="evt:task:telegram:2:1",
                )
            )
            self.assertTrue(
                ledger.is_task_completed(
                    task_id=task_message.task_id,
                    envelope_id=task_message.envelope.id,
                )
            )
            self.assertIn("egress_drop_dispatch_failed", captured.output[0])

    def test_resolve_error_email_recipient_prefers_explicit_value_then_smtp_username(self) -> None:
        explicit_args = type("Args", (), {"error_email_to": "ops@example.com"})()
        config = {"smtp_username": "smtp@example.com", "imap_username": "imap@example.com"}
        self.assertEqual(_resolve_error_email_recipient(explicit_args, config), "ops@example.com")

        default_args = type("Args", (), {"error_email_to": None})()
        self.assertEqual(_resolve_error_email_recipient(default_args, config), "smtp@example.com")
        self.assertEqual(
            _resolve_error_email_recipient(default_args, {"imap_username": "imap@example.com"}),
            "imap@example.com",
        )

    def test_log_ingress_poll_failure_suppresses_repeated_tracebacks(self) -> None:
        state: dict[str, IngressPollFailureLogState] = {}
        with (
            patch("app.main_message_handler.time.monotonic", side_effect=[10.0, 20.0, 30.0, 71.0]),
            patch("app.main_message_handler.LOGGER.exception") as exception_logger,
        ):
            _log_ingress_poll_failure(
                connector_name="TelegramConnector",
                loop_count=1,
                state_by_connector=state,
                interval_seconds=60.0,
            )
            _log_ingress_poll_failure(
                connector_name="TelegramConnector",
                loop_count=2,
                state_by_connector=state,
                interval_seconds=60.0,
            )
            _log_ingress_poll_failure(
                connector_name="TelegramConnector",
                loop_count=3,
                state_by_connector=state,
                interval_seconds=60.0,
            )
            _log_ingress_poll_failure(
                connector_name="TelegramConnector",
                loop_count=4,
                state_by_connector=state,
                interval_seconds=60.0,
            )

        self.assertEqual(exception_logger.call_count, 2)
        self.assertEqual(
            exception_logger.call_args_list[0].args,
            ("ingress_connector_poll_failed connector=%s loop=%s", "TelegramConnector", 1),
        )
        self.assertEqual(
            exception_logger.call_args_list[1].args,
            (
                "ingress_connector_poll_failed connector=%s loop=%s suppressed_repeats=%s",
                "TelegramConnector",
                4,
                2,
            ),
        )

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
                event_kind="message",
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
                event_kind="message",
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
                event_kind="message",
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
                event_kind="message",
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

    def test_handle_egress_message_persists_attachment_only_dispatch_as_assistant_turn(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "handler.db")
            store = SQLiteStateStore(db_path)
            ledger = TaskLedgerStore(db_path)
            task_message = TaskQueueMessage.from_envelope(
                self._build_telegram_envelope(envelope_id="telegram:201", content="hello"),
                trace_id="trace:telegram:201",
            )
            ledger.record_task(task_message)

            @dataclass
            class _TelegramApplier:
                def apply(self, decision, envelope=None):
                    del decision, envelope
                    return ApplyResult(
                        applied_actions=[],
                        skipped_actions=[],
                        dispatched_messages=[
                            OutboundMessage(
                                channel="telegram",
                                target="12345",
                                attachment=AttachmentRef(
                                    uri="file:///tmp/menu.pdf",
                                    name="menu.pdf",
                                ),
                            )
                        ],
                        reason_codes=[],
                    )

            payload = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="telegram", target="12345", body="reply"),
                emitted_at=datetime(2026, 3, 6, 12, 1, tzinfo=timezone.utc),
                event_id="evt:task:telegram:201:0",
                sequence=0,
                event_kind="message",
            ).to_dict()

            _handle_egress_message(
                picked_guid="guid-7",
                picked_payload=payload,
                ledger=ledger,
                store=store,
                allowed_egress_channels={"telegram"},
                applier=_TelegramApplier(),
                ack_callback=lambda _guid: None,
            )

            turns = store.list_recent_conversation_turns(channel="telegram", target="12345", limit=5)
            self.assertEqual(turns, [("assistant", "[Attachment sent: menu.pdf]")])

    def test_egress_telemetry_rollup_reports_dedupe_rate_and_latency(self) -> None:
        telemetry = EgressTelemetryRollup()
        telemetry.record_received()
        telemetry.record_received()
        telemetry.record_dispatched(event_kind="incremental", latency_ms=120)
        telemetry.record_dispatched(event_kind="message", latency_ms=80)
        telemetry.record_dispatched(event_kind="completion", latency_ms=5)
        telemetry.record_deduped()
        telemetry.record_dropped(reason="unknown_task")
        telemetry.record_dropped(reason="disallowed_channel")
        snapshot = telemetry.snapshot()

        self.assertEqual(snapshot["received_total"], 2)
        self.assertEqual(snapshot["dispatched_total"], 3)
        self.assertEqual(snapshot["incremental_dispatched_total"], 1)
        self.assertEqual(snapshot["message_dispatched_total"], 1)
        self.assertEqual(snapshot["completion_applied_total"], 1)
        self.assertEqual(snapshot["deduped_total"], 1)
        self.assertEqual(snapshot["dedupe_hit_rate_pct"], 25.0)
        self.assertEqual(snapshot["dispatch_latency_ms_avg"], 68.33)
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
            "message_dispatched_total": 0,
            "completion_applied_total": 0,
            "dispatch_latency_ms_avg": 120.5,
            "dispatch_latency_ms_max": 121,
        }

        current_monotonic = 160.0
        metrics.record_loop(
            ingress_published=3,
            github_scanned_events=5,
            github_new_events=2,
            github_published=1,
            telegram_attachment_cleanup=None,
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
        self.assertEqual(snapshot["telegram_attachment_cleanup_deleted_total"], 0)
        self.assertEqual(snapshot["telegram_attachment_cleanup_missing_total"], 0)
        self.assertEqual(snapshot["telegram_attachment_cleanup_failed_total"], 0)
        self.assertEqual(snapshot["telegram_attachment_cleanup_reclaimed_bytes_total"], 0)
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
                "telegram_attachment_cleanup_deleted_total": 2,
                "telegram_attachment_cleanup_missing_total": 1,
                "telegram_attachment_cleanup_failed_total": 0,
                "telegram_attachment_cleanup_reclaimed_bytes_total": 4096,
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
                "message_dispatched_total": 2,
                "completion_applied_total": 1,
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
        self.assertIn("chatting_message_handler_telegram_attachment_cleanup_deleted_total 2", rendered)
        self.assertIn(
            "chatting_message_handler_telegram_attachment_cleanup_reclaimed_bytes_total 4096",
            rendered,
        )
        self.assertIn("chatting_message_handler_egress_loops_total 5", rendered)
        self.assertIn("chatting_message_handler_egress_received_total 7", rendered)
        self.assertIn("chatting_message_handler_egress_message_dispatched_total 2", rendered)
        self.assertIn("chatting_message_handler_egress_completion_applied_total 1", rendered)
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
