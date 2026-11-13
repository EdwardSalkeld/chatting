import json
import socket
import struct
import threading
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.egress_client import submit_egress
from tests.e2e.servers import reserve_port

READ_TIMEOUT_SECONDS = 0.3


@contextmanager
def egress_server(mode: str) -> Iterator[tuple[str, list[dict[str, object]]]]:
    """Serve a real loopback egress endpoint that misbehaves according to mode.

    Yields the endpoint URL and the list of payloads the server received, so a
    test can tell a request that reached the handler from one that did not.
    """
    received: list[dict[str, object]] = []
    release = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            received.append(json.loads(self.rfile.read(length)))
            if mode == "ok":
                self._reply(200, {"status": "dispatched", "metadata": {"id": 7}})
            elif mode == "dropped":
                self._reply(422, {"status": "dropped", "reason": "unknown_task"})
            elif mode == "stall":
                release.wait(timeout=10)
            elif mode == "close_before_response":
                pass
            elif mode == "close_mid_body":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "100")
                self.end_headers()
                self.wfile.write(b'{"status": "disp')
                self.wfile.flush()
            elif mode == "reset":
                # SO_LINGER with a zero timeout makes close() send RST.
                self.connection.setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                )
            else:
                raise AssertionError(f"unknown mode {mode}")

        def _reply(self, status: int, body: dict[str, object]) -> None:
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/egress", received
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class SubmitEgressTests(unittest.TestCase):
    def _submit(self, url: str) -> tuple[int, dict[str, object]]:
        return submit_egress(
            url, {"event_id": "evt:1"}, timeout_seconds=READ_TIMEOUT_SECONDS
        )

    def test_success_returns_status_and_body(self) -> None:
        with egress_server("ok") as (url, received):
            status, body = self._submit(url)
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "dispatched", "metadata": {"id": 7}})
        self.assertEqual(received, [{"event_id": "evt:1"}])

    def test_http_error_returns_its_status_and_body(self) -> None:
        with egress_server("dropped") as (url, _):
            status, body = self._submit(url)
        self.assertEqual(status, 422)
        self.assertEqual(body, {"status": "dropped", "reason": "unknown_task"})

    def test_connection_refused_is_transient(self) -> None:
        status, body = self._submit(f"http://127.0.0.1:{reserve_port()}/egress")
        self.assertEqual(status, 0)
        self.assertIn("unreachable", str(body["reason"]))

    def test_read_timeout_is_transient(self) -> None:
        with egress_server("stall") as (url, received):
            status, body = self._submit(url)
        self.assertEqual(status, 0)
        self.assertIn("transport error", str(body["reason"]))
        # The handler had the request: this is the case where a retry must
        # reuse the event id.
        self.assertEqual(len(received), 1)

    def test_connection_closed_before_response_is_transient(self) -> None:
        with egress_server("close_before_response") as (url, _):
            status, body = self._submit(url)
        self.assertEqual(status, 0)
        self.assertIn("RemoteDisconnected", str(body["reason"]))

    def test_connection_closed_mid_body_is_transient(self) -> None:
        with egress_server("close_mid_body") as (url, _):
            status, body = self._submit(url)
        self.assertEqual(status, 0)
        self.assertIn("IncompleteRead", str(body["reason"]))

    def test_connection_reset_is_transient(self) -> None:
        with egress_server("reset") as (url, _):
            status, _ = self._submit(url)
        self.assertEqual(status, 0)


if __name__ == "__main__":
    unittest.main()
