"""Minimal BBMB TCP client vendored into chatting."""

from __future__ import annotations

import hashlib
import socket
import struct
from dataclasses import dataclass
from typing import Callable


class BBMBError(Exception):
    """Base BBMB client error."""


class QueueEmptyError(BBMBError):
    """Raised when pickup is called on an empty queue."""


class NotFoundError(BBMBError):
    """Raised when delete references an unknown message GUID."""


class InvalidChecksumError(BBMBError):
    """Raised when server rejects a message checksum."""


class MessageTooLargeError(BBMBError):
    """Raised when message exceeds BBMB server max size."""


class ServerError(BBMBError):
    """Raised on BBMB internal server errors."""


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
    MESSAGE_TOO_LARGE = 0x04
    INTERNAL_ERROR = 0x05


MAX_MESSAGE_SIZE = 1024 * 1024  # 1MB


@dataclass(frozen=True)
class Message:
    guid: str
    content: str
    checksum: str


class Client:
    """Blocking BBMB protocol client."""

    def __init__(
        self,
        address: str = "localhost:9876",
        *,
        socket_factory: Callable[[], socket.socket] | None = None,
    ) -> None:
        if not isinstance(address, str) or not address.strip():
            raise ValueError("address is required")
        parts = address.split(":", maxsplit=1)
        self.host = parts[0]
        self.port = int(parts[1]) if len(parts) > 1 else 9876
        self._socket_factory = socket_factory or socket.socket
        self.sock: socket.socket | None = None

    def connect(self) -> None:
        self.sock = self._socket_factory()
        self.sock.connect((self.host, self.port))

    def close(self) -> None:
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def __enter__(self) -> "Client":
        self.connect()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def ensure_queue(self, queue_name: str) -> None:
        payload = _write_string(queue_name)
        self._write_frame(_CommandType.ENSURE_QUEUE, payload)
        _, response_payload = self._read_frame()
        status = struct.unpack("B", response_payload[:1])[0]
        if status != _StatusCode.OK:
            raise BBMBError(f"ensure_queue_failed_status:{status}")

    def add_message(self, queue_name: str, content: str) -> str:
        if len(content) > MAX_MESSAGE_SIZE:
            raise MessageTooLargeError("message exceeds 1MB limit")

        checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
        payload = _write_string(queue_name)
        payload += _write_string(content)
        payload += _write_string(checksum)

        self._write_frame(_CommandType.ADD_MESSAGE, payload)
        _, response_payload = self._read_frame()
        status = struct.unpack("B", response_payload[:1])[0]

        if status == _StatusCode.OK:
            guid, _ = _read_string(response_payload, 1)
            return guid
        if status == _StatusCode.INVALID_CHECKSUM:
            raise InvalidChecksumError("server rejected checksum")
        if status == _StatusCode.MESSAGE_TOO_LARGE:
            raise MessageTooLargeError("message too large")
        if status == _StatusCode.INTERNAL_ERROR:
            raise ServerError("server internal error")
        raise BBMBError(f"add_message_failed_status:{status}")

    def pickup_message(
        self,
        queue_name: str,
        timeout_seconds: int = 30,
        wait_seconds: int = 0,
    ) -> Message:
        payload = _write_string(queue_name)
        payload += struct.pack(">I", timeout_seconds)
        if wait_seconds > 0:
            payload += struct.pack(">I", wait_seconds)

        self._write_frame(_CommandType.PICKUP_MESSAGE, payload)
        _, response_payload = self._read_frame()
        status = struct.unpack("B", response_payload[:1])[0]

        if status == _StatusCode.OK:
            offset = 1
            guid, offset = _read_string(response_payload, offset)
            content, offset = _read_string(response_payload, offset)
            checksum, _ = _read_string(response_payload, offset)
            return Message(guid=guid, content=content, checksum=checksum)
        if status == _StatusCode.EMPTY_QUEUE:
            raise QueueEmptyError("queue is empty")
        if status == _StatusCode.INTERNAL_ERROR:
            raise ServerError("server internal error")
        raise BBMBError(f"pickup_message_failed_status:{status}")

    def delete_message(self, queue_name: str, guid: str) -> None:
        payload = _write_string(queue_name)
        payload += _write_string(guid)

        self._write_frame(_CommandType.DELETE_MESSAGE, payload)
        _, response_payload = self._read_frame()
        status = struct.unpack("B", response_payload[:1])[0]

        if status == _StatusCode.OK:
            return
        if status == _StatusCode.NOT_FOUND:
            raise NotFoundError("message not found")
        if status == _StatusCode.INTERNAL_ERROR:
            raise ServerError("server internal error")
        raise BBMBError(f"delete_message_failed_status:{status}")

    def _write_frame(self, command_type: int, payload: bytes) -> None:
        sock = _require_socket(self.sock)
        length = len(payload) + 1
        frame = struct.pack(">I", length)
        frame += struct.pack("B", command_type)
        frame += payload
        sock.sendall(frame)

    def _read_frame(self) -> tuple[int, bytes]:
        length_bytes = self._recv_exactly(4)
        length = struct.unpack(">I", length_bytes)[0]

        command_type_bytes = self._recv_exactly(1)
        command_type = struct.unpack("B", command_type_bytes)[0]

        payload = self._recv_exactly(length - 1)
        return command_type, payload

    def _recv_exactly(self, size: int) -> bytes:
        sock = _require_socket(self.sock)
        data = b""
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise BBMBError("connection closed by server")
            data += chunk
        return data


def _require_socket(sock: socket.socket | None) -> socket.socket:
    if sock is None:
        raise BBMBError("client is not connected")
    return sock


def _write_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack(">I", len(encoded)) + encoded


def _read_string(data: bytes, offset: int) -> tuple[str, int]:
    if offset + 4 > len(data):
        raise BBMBError("unexpected end of data")

    length = struct.unpack(">I", data[offset : offset + 4])[0]
    offset += 4

    if offset + length > len(data):
        raise BBMBError("unexpected end of data")

    value = data[offset : offset + length].decode("utf-8")
    offset += length
    return value, offset


__all__ = [
    "BBMBError",
    "Client",
    "InvalidChecksumError",
    "Message",
    "MessageTooLargeError",
    "NotFoundError",
    "QueueEmptyError",
    "ServerError",
]
