import unittest
from datetime import datetime, timezone

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import AttachmentRef, OutboundMessage, ReplyChannel, TaskEnvelope


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
            policy_profile="default",
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:1",
        )

        message = TaskQueueMessage.from_envelope(envelope, trace_id="trace:email:1")
        parsed = TaskQueueMessage.from_dict(message.to_dict())

        self.assertEqual(parsed.trace_id, "trace:email:1")
        self.assertEqual(parsed.task_id, "task:email:1")
        self.assertEqual(parsed.envelope.id, envelope.id)
        self.assertEqual(parsed.envelope.attachments[0].uri, "file://inbox/msg1.txt")

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
            message=OutboundMessage(channel="email", target="alice@example.com", body="hello"),
            emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
        )

        parsed = EgressQueueMessage.from_dict(message.to_dict())

        self.assertEqual(parsed.task_id, message.task_id)
        self.assertEqual(parsed.event_index, 0)
        self.assertEqual(parsed.event_count, 2)
        self.assertEqual(parsed.message.target, "alice@example.com")

    def test_egress_message_rejects_event_index_out_of_range(self) -> None:
        with self.assertRaises(ValueError):
            EgressQueueMessage(
                task_id="task:email:1",
                envelope_id="email:1",
                trace_id="trace:email:1",
                event_index=3,
                event_count=2,
                message=OutboundMessage(channel="email", target="alice@example.com", body="hello"),
                emitted_at=datetime(2026, 3, 6, 11, 1, tzinfo=timezone.utc),
            )


if __name__ == "__main__":
    unittest.main()
