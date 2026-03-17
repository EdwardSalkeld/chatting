import json
import unittest
from dataclasses import dataclass

from app.broker.bbmb_client import BBMBQueueAdapter, BrokerOperationError
class QueueEmptyError(RuntimeError):
    pass
@dataclass
class _FakePickupResult:
    guid: str
    content: str
class _FakeBBMBClient:
    def __init__(self, _address: str):
        self.queues: dict[str, list[_FakePickupResult]] = {}
        self.deleted: list[tuple[str, str]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def ensure_queue(self, queue_name: str) -> None:
        self.queues.setdefault(queue_name, [])

    def add_message(self, queue_name: str, content: str) -> str:
        guid = f"guid-{len(self.queues[queue_name]) + 1}"
        self.queues[queue_name].append(_FakePickupResult(guid=guid, content=content))
        return guid

    def pickup_message(
        self,
        queue_name: str,
        timeout_seconds: int = 30,
        wait_seconds: int = 0,
    ):
        del timeout_seconds, wait_seconds
        queue = self.queues.setdefault(queue_name, [])
        if not queue:
            raise QueueEmptyError("empty")
        return queue.pop(0)

    def delete_message(self, queue_name: str, guid: str) -> None:
        self.deleted.append((queue_name, guid))
class BBMBQueueAdapterTests(unittest.TestCase):
    def test_publish_pickup_ack_roundtrip(self) -> None:
        shared_client = _FakeBBMBClient("ignored")
        adapter = BBMBQueueAdapter(
            address="localhost:9876",
            client_factory=lambda _address: shared_client,
        )

        guid = adapter.publish_json("chatting.tasks.v1", {"hello": "world"})
        picked = adapter.pickup_json("chatting.tasks.v1", timeout_seconds=5)

        self.assertEqual(guid, "guid-1")
        self.assertIsNotNone(picked)
        assert picked is not None
        self.assertEqual(picked.guid, "guid-1")
        self.assertEqual(picked.payload, {"hello": "world"})

        adapter.ack("chatting.tasks.v1", picked.guid)
        self.assertEqual(shared_client.deleted, [("chatting.tasks.v1", "guid-1")])

    def test_pickup_empty_returns_none(self) -> None:
        shared_client = _FakeBBMBClient("ignored")
        adapter = BBMBQueueAdapter(
            address="localhost:9876",
            client_factory=lambda _address: shared_client,
        )

        self.assertEqual(adapter.pickup_json("chatting.tasks.v1"), None)

    def test_pickup_invalid_json_raises(self) -> None:
        shared_client = _FakeBBMBClient("ignored")
        shared_client.ensure_queue("chatting.tasks.v1")
        shared_client.queues["chatting.tasks.v1"].append(
            _FakePickupResult(guid="guid-1", content=json.dumps(["not", "object"]))
        )
        adapter = BBMBQueueAdapter(
            address="localhost:9876",
            client_factory=lambda _address: shared_client,
        )

        with self.assertRaises(BrokerOperationError):
            adapter.pickup_json("chatting.tasks.v1")

    def test_pickup_forwards_wait_seconds(self) -> None:
        recorded: dict[str, int] = {}

        class _RecordingPickupClient(_FakeBBMBClient):
            def pickup_message(
                self,
                queue_name: str,
                timeout_seconds: int = 30,
                wait_seconds: int = 0,
            ):
                recorded["timeout_seconds"] = timeout_seconds
                recorded["wait_seconds"] = wait_seconds
                return super().pickup_message(
                    queue_name,
                    timeout_seconds=timeout_seconds,
                    wait_seconds=wait_seconds,
                )

        shared_client = _RecordingPickupClient("ignored")
        shared_client.ensure_queue("chatting.tasks.v1")
        shared_client.queues["chatting.tasks.v1"].append(
            _FakePickupResult(guid="guid-1", content=json.dumps({"ok": True}))
        )
        adapter = BBMBQueueAdapter(
            address="localhost:9876",
            client_factory=lambda _address: shared_client,
        )

        adapter.pickup_json("chatting.tasks.v1", timeout_seconds=7, wait_seconds=3)
        self.assertEqual(recorded, {"timeout_seconds": 7, "wait_seconds": 3})
if __name__ == "__main__":
    unittest.main()
