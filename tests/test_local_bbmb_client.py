import struct
import unittest

from app.broker.local_bbmb_client import BBMBError, Client, QueueEmptyError


def _encode_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack(">I", len(encoded)) + encoded


def _frame(command_type: int, payload: bytes) -> bytes:
    return struct.pack(">I", len(payload) + 1) + struct.pack("B", command_type) + payload


class _FakeSocket:
    def __init__(self, responses: list[bytes]):
        self._responses = list(responses)
        self._cursor = 0
        self.sent: list[bytes] = []
        self.connected_to: tuple[str, int] | None = None
        self.closed = False

    def connect(self, address: tuple[str, int]) -> None:
        self.connected_to = address

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)

    def recv(self, size: int) -> bytes:
        if self._cursor >= len(self._responses):
            return b""
        data = self._responses[self._cursor]
        self._cursor += 1
        if len(data) > size:
            self._responses.insert(self._cursor, data[size:])
            return data[:size]
        return data

    def close(self) -> None:
        self.closed = True


class LocalBBMBClientTests(unittest.TestCase):
    def test_ensure_queue_add_pickup_delete_round_trip(self) -> None:
        ensure_response = _frame(0x01, struct.pack("B", 0x00))
        add_response = _frame(0x02, struct.pack("B", 0x00) + _encode_string("guid-1"))
        pickup_response = _frame(
            0x03,
            struct.pack("B", 0x00)
            + _encode_string("guid-1")
            + _encode_string('{"hello":"world"}')
            + _encode_string("checksum"),
        )
        delete_response = _frame(0x04, struct.pack("B", 0x00))

        fake_socket = _FakeSocket(
            responses=[
                ensure_response[:3],
                ensure_response[3:],
                add_response,
                pickup_response,
                delete_response,
            ]
        )

        client = Client(
            address="127.0.0.1:9876",
            socket_factory=lambda: fake_socket,
        )
        with client:
            client.ensure_queue("chatting.tasks.v1")
            guid = client.add_message("chatting.tasks.v1", '{"hello":"world"}')
            msg = client.pickup_message("chatting.tasks.v1", timeout_seconds=5)
            client.delete_message("chatting.tasks.v1", "guid-1")

        self.assertEqual(guid, "guid-1")
        self.assertEqual(msg.guid, "guid-1")
        self.assertEqual(msg.content, '{"hello":"world"}')
        self.assertEqual(fake_socket.connected_to, ("127.0.0.1", 9876))
        self.assertEqual(len(fake_socket.sent), 4)
        self.assertEqual(fake_socket.closed, True)

    def test_pickup_empty_queue_raises(self) -> None:
        pickup_empty_response = _frame(0x03, struct.pack("B", 0x01))
        fake_socket = _FakeSocket(responses=[pickup_empty_response])

        client = Client(
            address="127.0.0.1:9876",
            socket_factory=lambda: fake_socket,
        )
        with client:
            with self.assertRaises(QueueEmptyError):
                client.pickup_message("chatting.tasks.v1", timeout_seconds=1)

    def test_pickup_payload_includes_wait_seconds_when_configured(self) -> None:
        pickup_empty_response = _frame(0x03, struct.pack("B", 0x01))
        fake_socket = _FakeSocket(responses=[pickup_empty_response])
        client = Client(
            address="127.0.0.1:9876",
            socket_factory=lambda: fake_socket,
        )

        with client:
            with self.assertRaises(QueueEmptyError):
                client.pickup_message("chatting.tasks.v1", timeout_seconds=5, wait_seconds=4)

        expected_payload = _encode_string("chatting.tasks.v1") + struct.pack(">I", 5) + struct.pack(">I", 4)
        expected_frame = _frame(0x03, expected_payload)
        self.assertEqual(fake_socket.sent, [expected_frame])

    def test_operations_require_connection(self) -> None:
        client = Client(address="127.0.0.1:9876", socket_factory=lambda: _FakeSocket([]))
        with self.assertRaises(BBMBError):
            client.ensure_queue("chatting.tasks.v1")


if __name__ == "__main__":
    unittest.main()
