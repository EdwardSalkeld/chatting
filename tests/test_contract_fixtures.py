import json
import unittest
from pathlib import Path

from app.broker.messages import (
    EgressQueueMessage,
    TaskQueueMessage,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "contracts"


def load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class ContractFixtureTests(unittest.TestCase):
    def test_shared_wire_fixtures_round_trip(self):
        for name, model in (
            ("task.v1.json", TaskQueueMessage),
            ("egress.v2.sequenced.json", EgressQueueMessage),
            ("egress.v2.incremental.json", EgressQueueMessage),
        ):
            with self.subTest(fixture=name):
                payload = load_fixture(name)
                self.assertEqual(model.from_dict(payload).to_dict(), payload)
