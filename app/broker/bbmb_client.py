"""Thin BBMB queue adapter with JSON payload helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Protocol


class BrokerOperationError(RuntimeError):
    """Raised when BBMB broker operations fail."""


@dataclass(frozen=True)
class PickedMessage:
    """One message picked up from a BBMB queue."""

    guid: str
    payload: dict[str, Any]


class _BBMBClientProtocol(Protocol):
    def __enter__(self) -> "_BBMBClientProtocol": ...

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None: ...

    def ensure_queue(self, queue_name: str) -> None: ...

    def add_message(self, queue_name: str, content: str) -> str: ...

    def pickup_message(self, queue_name: str, timeout_seconds: int = 30) -> Any: ...

    def delete_message(self, queue_name: str, guid: str) -> None: ...


class BBMBQueueAdapter:
    """JSON-oriented BBMB queue adapter."""

    def __init__(
        self,
        *,
        address: str,
        client_factory: Callable[[str], _BBMBClientProtocol] | None = None,
    ) -> None:
        if not isinstance(address, str) or not address.strip():
            raise ValueError("address is required")
        self._address = address
        self._client_factory = client_factory or _default_client_factory

    def ensure_queue(self, queue_name: str) -> None:
        try:
            with self._client_factory(self._address) as client:
                client.ensure_queue(queue_name)
        except Exception as error:  # noqa: BLE001
            raise BrokerOperationError(f"ensure_queue_failed:{queue_name}:{error}") from error

    def publish_json(self, queue_name: str, payload: dict[str, Any]) -> str:
        encoded = json.dumps(payload, sort_keys=True)
        try:
            with self._client_factory(self._address) as client:
                client.ensure_queue(queue_name)
                return client.add_message(queue_name, encoded)
        except Exception as error:  # noqa: BLE001
            raise BrokerOperationError(f"publish_failed:{queue_name}:{error}") from error

    def pickup_json(self, queue_name: str, *, timeout_seconds: int = 30) -> PickedMessage | None:
        try:
            with self._client_factory(self._address) as client:
                result = client.pickup_message(queue_name, timeout_seconds=timeout_seconds)
        except Exception as error:  # noqa: BLE001
            if error.__class__.__name__ == "QueueEmptyError":
                return None
            raise BrokerOperationError(f"pickup_failed:{queue_name}:{error}") from error

        try:
            payload = json.loads(result.content)
        except Exception as error:  # noqa: BLE001
            raise BrokerOperationError(f"pickup_payload_invalid_json:{queue_name}:{error}") from error
        if not isinstance(payload, dict):
            raise BrokerOperationError(f"pickup_payload_invalid_type:{queue_name}:object_required")
        return PickedMessage(guid=result.guid, payload=payload)

    def ack(self, queue_name: str, guid: str) -> None:
        try:
            with self._client_factory(self._address) as client:
                client.delete_message(queue_name, guid)
        except Exception as error:  # noqa: BLE001
            raise BrokerOperationError(f"ack_failed:{queue_name}:{guid}:{error}") from error


def _default_client_factory(address: str) -> _BBMBClientProtocol:
    from app.broker.local_bbmb_client import Client
    return Client(address=address)


__all__ = [
    "BBMBQueueAdapter",
    "BrokerOperationError",
    "PickedMessage",
]
