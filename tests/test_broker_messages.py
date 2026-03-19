import unittest
from datetime import datetime, timezone

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import AttachmentRef, OutboundMessage, PromptContext, ReplyChannel, TaskEnvelope
class TaskQueueMessageTests(unittest.TestCase):
    def test_task_message_round_trip(self) -> None:
        envelope = TaskEnvelope(
            id="email:1",
            source="email",
            received_at=datetime(2026, 3, 6, 11, 0, tzinfo=timezone.utc),
            actor="alice@example.com",
            content="hello",
            attachments=[AttachmentRef(uri="file://inbox/msg1.txt", name="msg1.txt")],
            context_refs=["repo:/home/edward/develop/chatting"],
            reply_channel=ReplyChannel(
                type="email",
                target="alice@example.com",
                metadata={"thread_id": "abc"},
            ),
            dedupe_key="email:1",
            prompt_context=PromptContext(
                global_instructions=["Keep replies concise."],
                reply_channel_instructions=["Use email formatting."],
            ),
        )

        message = TaskQueueMessage.from_envelope(envelope, trace_id="trace:email:1")
        parsed = TaskQueueMessage.from_dict(message.to_dict())

        self.assertEqual(parsed.trace_id, "trace:email:1")
        self.assertEqual(parsed.task_id, "task:email:1")
        self.assertEqual(parsed.envelope.id, envelope.id)
        self.assertEqual(parsed.envelope.attachments[0].uri, "file://inbox/msg1.txt")
        self.assertEqual(parsed.envelope.reply_channel.metadata, {"thread_id": "abc"})
        self.assertEqual(
            parsed.envelope.prompt_context.assembled_instructions(),
            ["Keep replies concise.", "Use email formatting."],
        )

    def test_task_message_rejects_wrong_type(self) -> None:
        with self.assertRaises(ValueError):
            TaskQueueMessage.from_dict({"message_type": "chatting.egress.v1"})
class EgressQueueMessageTests(unittest.TestCase):
    def test_egress_message_round_trip(self) -> None:
        message = EgressQueueMessage(
            task_id="task:email:1",
            envelope_id="email:1",
            trace_id="trace:email:1",
            event_index=0,
            event_count=2,
            message=OutboundMessage(
                channel="email",
                target="alice@example.com",
                body="hello",
                metadata={"thread_id": "abc"},
            ),
            emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
            event_id="evt:task:email:1:0",
            sequence=0,
            event_kind="message",
            message_type="chatting.egress.v2",
        )

        parsed = EgressQueueMessage.from_dict(message.to_dict())

        self.assertEqual(parsed.task_id, message.task_id)
        self.assertEqual(parsed.event_index, 0)
        self.assertEqual(parsed.event_count, 1)
        self.assertEqual(parsed.event_id, "evt:task:email:1:0")
        self.assertEqual(parsed.sequence, 0)
        self.assertEqual(parsed.event_kind, "message")
        self.assertEqual(parsed.message.target, "alice@example.com")
        self.assertEqual(parsed.message.metadata, {"thread_id": "abc"})

    def test_egress_v2_message_round_trip(self) -> None:
        message = EgressQueueMessage(
            task_id="task:email:2",
            envelope_id="email:2",
            trace_id="trace:email:2",
            event_index=0,
            event_count=1,
            message=OutboundMessage(channel="email", target="alice@example.com", body="hello"),
            emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
            event_id="evt:task:email:2:1",
            sequence=1,
            event_kind="incremental",
            message_type="chatting.egress.v2",
        )

        parsed = EgressQueueMessage.from_dict(message.to_dict())

        self.assertEqual(parsed.task_id, message.task_id)
        self.assertEqual(parsed.event_id, "evt:task:email:2:1")
        self.assertEqual(parsed.sequence, 1)
        self.assertEqual(parsed.event_kind, "incremental")
        self.assertEqual(parsed.event_index, 1)

    def test_egress_v2_attachment_message_round_trip(self) -> None:
        message = EgressQueueMessage(
            task_id="task:telegram:2",
            envelope_id="telegram:2",
            trace_id="trace:telegram:2",
            event_index=0,
            event_count=1,
            message=OutboundMessage(
                channel="telegram",
                target="12345",
                body="menu attached",
                attachment=AttachmentRef(uri="file:///tmp/menu.pdf", name="menu.pdf"),
            ),
            emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
            event_id="evt:task:telegram:2:1",
            sequence=0,
            event_kind="message",
            message_type="chatting.egress.v2",
        )

        parsed = EgressQueueMessage.from_dict(message.to_dict())

        self.assertEqual(parsed.message.body, "menu attached")
        self.assertIsNotNone(parsed.message.attachment)
        assert parsed.message.attachment is not None
        self.assertEqual(parsed.message.attachment.uri, "file:///tmp/menu.pdf")
        self.assertEqual(parsed.message.attachment.name, "menu.pdf")

    def test_egress_message_rejects_legacy_message_type(self) -> None:
        with self.assertRaises(ValueError):
            EgressQueueMessage(
                task_id="task:email:1",
                envelope_id="email:1",
                trace_id="trace:email:1",
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="hello"),
                emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:1:0",
                sequence=0,
                event_kind="message",
                message_type="chatting.egress.v1",
            )

    def test_egress_v2_message_requires_event_id(self) -> None:
        with self.assertRaises(ValueError):
            EgressQueueMessage(
                task_id="task:email:2",
                envelope_id="email:2",
                trace_id="trace:email:2",
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="hello"),
                emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
                sequence=0,
                event_kind="message",
                message_type="chatting.egress.v2",
            )

    def test_egress_v2_message_allows_missing_sequence(self) -> None:
        message = EgressQueueMessage(
            task_id="task:email:2",
            envelope_id="email:2",
            trace_id="trace:email:2",
            event_index=0,
            event_count=1,
            message=OutboundMessage(channel="email", target="alice@example.com", body="hello"),
            emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
            event_id="evt:task:email:2:adhoc",
            sequence=None,
            event_kind="incremental",
            message_type="chatting.egress.v2",
        )

        payload = message.to_dict()
        self.assertNotIn("sequence", payload)
        parsed = EgressQueueMessage.from_dict(payload)
        self.assertIsNone(parsed.sequence)
        self.assertEqual(parsed.event_kind, "incremental")

    def test_egress_v2_message_requires_sequence(self) -> None:
        with self.assertRaises(ValueError):
            EgressQueueMessage(
                task_id="task:email:2",
                envelope_id="email:2",
                trace_id="trace:email:2",
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="email", target="alice@example.com", body="hello"),
                emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:2:message",
                sequence=None,
                event_kind="message",
                message_type="chatting.egress.v2",
            )

    def test_egress_v2_completion_requires_sequence(self) -> None:
        with self.assertRaises(ValueError):
            EgressQueueMessage(
                task_id="task:email:2",
                envelope_id="email:2",
                trace_id="trace:email:2",
                event_index=0,
                event_count=1,
                message=OutboundMessage(channel="internal", target="task", body="done"),
                emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
                event_id="evt:task:email:2:completion",
                sequence=None,
                event_kind="completion",
                message_type="chatting.egress.v2",
            )
if __name__ == "__main__":
    unittest.main()
