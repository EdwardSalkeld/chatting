import unittest
from dataclasses import replace

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import AttachmentRef, OutboundMessage
from tests.test_contract_fixtures import load_fixture


class QueueMessageTests(unittest.TestCase):
    def test_task_factory_and_type_validation(self):
        payload = load_fixture("task.v1.json")
        parsed = TaskQueueMessage.from_dict(payload)
        generated = TaskQueueMessage.from_envelope(
            parsed.envelope, trace_id=parsed.trace_id
        )
        self.assertEqual(generated.task_id, parsed.task_id)
        self.assertEqual(generated.envelope.to_dict(), payload["envelope"])
        payload["message_type"] = "chatting.egress.v2"
        with self.assertRaisesRegex(ValueError, "message_type"):
            TaskQueueMessage.from_dict(payload)

    def test_egress_rejects_invalid_type_and_missing_fields(self):
        for field, value in (
            ("message_type", "chatting.egress.v1"),
            ("event_id", None),
        ):
            with self.subTest(field=field):
                payload = load_fixture("egress.v2.sequenced.json")
                if value is None:
                    del payload[field]
                else:
                    payload[field] = value
                with self.assertRaisesRegex(ValueError, field):
                    EgressQueueMessage.from_dict(payload)
        message = EgressQueueMessage.from_dict(load_fixture("egress.v2.sequenced.json"))
        for kind in ("message", "completion"):
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(ValueError, "sequence"):
                    replace(message, event_kind=kind, sequence=None)

    def test_sequenced_incremental_and_attachment_round_trip(self):
        base = EgressQueueMessage.from_dict(load_fixture("egress.v2.incremental.json"))
        message = replace(
            base,
            sequence=1,
            message=OutboundMessage(
                channel="telegram",
                target="12345",
                body="menu attached",
                attachment=AttachmentRef(uri="file:///menu.pdf", name="menu.pdf"),
            ),
        )
        self.assertEqual(EgressQueueMessage.from_dict(message.to_dict()), message)
