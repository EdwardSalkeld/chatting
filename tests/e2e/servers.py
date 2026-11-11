"""Port and broker helpers for tests that start real servers."""

from __future__ import annotations

import os
import socket
import time
import unittest
from pathlib import Path

BBMB_SERVER_ENV = "CHATTING_BBMB_SERVER_BIN"


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        return probe.connect_ex((host, port)) == 0


def wait_for_port(host: str, port: int, timeout_seconds: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if is_port_open(host, port):
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {host}:{port}")


def require_bbmb_server(test: unittest.TestCase) -> Path:
    """Return the BBMB server binary, or skip the test when it is unavailable."""
    raw = os.environ.get(BBMB_SERVER_ENV, "").strip()
    if not raw:
        test.skipTest(f"{BBMB_SERVER_ENV} is not set")
    server_bin = Path(raw)
    if not server_bin.is_file():
        test.skipTest(f"bbmb server binary not found: {server_bin}")
    return server_bin
