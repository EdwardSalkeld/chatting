import json
import unittest
from pathlib import Path

from app.broker.messages import (
    AuxiliaryIngressQueueMessage,
    EgressQueueMessage,
    TaskQueueMessage,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "contracts"


def load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class ContractFixtureTests(unittest.TestCase):
    def test_task_fixture_parses_and_serializes(self) -> None:
        payload = load_fixture("task.v1.json")

        parsed = TaskQueueMessage.from_dict(payload)

        self.assertEqual(parsed.message_type, "chatting.task.v1")
        self.assertEqual(parsed.task_id, "task:email:fixture:1")
        self.assertEqual(parsed.envelope.id, "email:fixture:1")
        self.assertEqual(parsed.to_dict(), payload)

    def test_sequenced_egress_fixture_parses_and_serializes(self) -> None:
        payload = load_fixture("egress.v2.sequenced.json")

        parsed = EgressQueueMessage.from_dict(payload)

        self.assertEqual(parsed.message_type, "chatting.egress.v2")
        self.assertEqual(parsed.event_kind, "message")
        self.assertEqual(parsed.sequence, 0)
        self.assertEqual(parsed.to_dict(), payload)

    def test_incremental_egress_fixture_parses_and_serializes(self) -> None:
        payload = load_fixture("egress.v2.incremental.json")

        parsed = EgressQueueMessage.from_dict(payload)

        self.assertEqual(parsed.message_type, "chatting.egress.v2")
        self.assertEqual(parsed.event_kind, "incremental")
        self.assertIsNone(parsed.sequence)
        self.assertNotIn("sequence", parsed.to_dict())
        self.assertEqual(parsed.to_dict(), payload)

    def test_auxiliary_ingress_fixture_parses_and_serializes(self) -> None:
        payload = load_fixture("auxiliary_ingress.v1.json")

        parsed = AuxiliaryIngressQueueMessage.from_dict(payload)

        self.assertEqual(parsed.message_type, "chatting.auxiliary_ingress.v1")
        self.assertEqual(parsed.event_id, "aux:fixture:1")
        self.assertEqual(parsed.to_dict(), payload)


if __name__ == "__main__":
    unittest.main()
