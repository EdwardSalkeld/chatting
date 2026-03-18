import unittest
from datetime import datetime, timezone

from app.models import ReplyChannel, TaskEnvelope
from app.queue import InMemoryQueueBackend
class InMemoryQueueBackendTests(unittest.TestCase):
    def test_enqueue_dequeue_roundtrip(self) -> None:
        queue = InMemoryQueueBackend()
        envelope = TaskEnvelope(
            id="email:1",
            source="email",
            received_at=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
            actor="alice@example.com",
            content="hello",
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:1",
        )

        queue.enqueue(envelope)

        self.assertEqual(queue.size(), 1)
        self.assertEqual(queue.dequeue(), envelope)
        self.assertEqual(queue.dequeue(), None)
        self.assertEqual(queue.size(), 0)
if __name__ == "__main__":
    unittest.main()
