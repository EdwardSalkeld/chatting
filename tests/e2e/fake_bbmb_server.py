"""Minimal BBMB-compatible test server for E2E runs."""

from __future__ import annotations

import hashlib
import socketserver
import struct
import threading
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass


class _CommandType:
    ENSURE_QUEUE = 0x01
    ADD_MESSAGE = 0x02
    PICKUP_MESSAGE = 0x03
    DELETE_MESSAGE = 0x04


class _StatusCode:
    OK = 0x00
    EMPTY_QUEUE = 0x01
    NOT_FOUND = 0x02
    INVALID_CHECKSUM = 0x03
    INTERNAL_ERROR = 0x05


@dataclass(frozen=True)
class _QueuedMessage:
    guid: str
    content: str
    checksum: str


class _BrokerState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queues: dict[str, deque[_QueuedMessage]] = defaultdict(deque)
        self._inflight: dict[str, dict[str, _QueuedMessage]] = defaultdict(dict)

    def ensure_queue(self, queue_name: str) -> None:
        with self._lock:
            self._queues[queue_name]

    def add_message(
        self,
        queue_name: str,
        content: str,
        checksum: str,
    ) -> tuple[int, bytes]:
        expected_checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if checksum != expected_checksum:
            return _StatusCode.INVALID_CHECKSUM, b""
        with self._lock:
            guid = f"guid:{uuid.uuid4()}"
            self._queues[queue_name].append(
                _QueuedMessage(guid=guid, content=content, checksum=checksum)
            )
        return _StatusCode.OK, _write_string(guid)

    def pickup_message(self, queue_name: str) -> tuple[int, bytes]:
        with self._lock:
            queue = self._queues[queue_name]
            if not queue:
                return _StatusCode.EMPTY_QUEUE, b""
            message = queue.popleft()
            self._inflight[queue_name][message.guid] = message
        payload = (
            _write_string(message.guid)
            + _write_string(message.content)
            + _write_string(message.checksum)
        )
        return _StatusCode.OK, payload

    def delete_message(self, queue_name: str, guid: str) -> int:
        with self._lock:
            if self._inflight[queue_name].pop(guid, None) is None:
                return _StatusCode.NOT_FOUND
        return _StatusCode.OK


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class _BBMBRequestHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        while True:
            length_bytes = self._recv_exactly(4)
            if length_bytes is None:
                return
            length = struct.unpack(">I", length_bytes)[0]
            frame = self._recv_exactly(length)
            if frame is None:
                return
            command_type = frame[0]
            payload = frame[1:]
            response_command, response_payload = self._dispatch(command_type, payload)
            self.request.sendall(
                struct.pack(">I", len(response_payload) + 1)
                + struct.pack("B", response_command)
                + response_payload
            )

    def _dispatch(self, command_type: int, payload: bytes) -> tuple[int, bytes]:
        state: _BrokerState = self.server.broker_state
        if command_type == _CommandType.ENSURE_QUEUE:
            queue_name, _ = _read_string(payload, 0)
            state.ensure_queue(queue_name)
            return command_type, struct.pack("B", _StatusCode.OK)
        if command_type == _CommandType.ADD_MESSAGE:
            queue_name, offset = _read_string(payload, 0)
            content, offset = _read_string(payload, offset)
            checksum, _ = _read_string(payload, offset)
            status, response_payload = state.add_message(queue_name, content, checksum)
            return command_type, struct.pack("B", status) + response_payload
        if command_type == _CommandType.PICKUP_MESSAGE:
            queue_name, _ = _read_string(payload, 0)
            status, response_payload = state.pickup_message(queue_name)
            return command_type, struct.pack("B", status) + response_payload
        if command_type == _CommandType.DELETE_MESSAGE:
            queue_name, offset = _read_string(payload, 0)
            guid, _ = _read_string(payload, offset)
            status = state.delete_message(queue_name, guid)
            return command_type, struct.pack("B", status)
        return command_type, struct.pack("B", _StatusCode.INTERNAL_ERROR)

    def _recv_exactly(self, size: int) -> bytes | None:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self.request.recv(size - len(chunks))
            if not chunk:
                return None if not chunks else bytes(chunks)
            chunks.extend(chunk)
        return bytes(chunks)


class FakeBBMBServer:
    """Context-managed BBMB-compatible TCP server bound to a supplied port."""

    def __init__(self, host: str, port: int) -> None:
        self._server = _ThreadedTCPServer((host, port), _BBMBRequestHandler)
        self._server.broker_state = _BrokerState()
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            daemon=True,
        )

    def __enter__(self) -> "FakeBBMBServer":
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def _write_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack(">I", len(encoded)) + encoded


def _read_string(data: bytes, offset: int) -> tuple[str, int]:
    length = struct.unpack(">I", data[offset : offset + 4])[0]
    offset += 4
    value = data[offset : offset + length].decode("utf-8")
    return value, offset + length
