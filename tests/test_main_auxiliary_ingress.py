import io
import json
import unittest
from datetime import datetime, timezone

from app.main_auxiliary_ingress import _build_handler, _normalize_path, _publish_body


class _RecordingBroker:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish_json(self, queue_name: str, payload: dict[str, object]) -> None:
        self.published.append((queue_name, payload))


class MainAuxiliaryIngressTests(unittest.TestCase):
    def test_publish_body_emits_auxiliary_ingress_message(self) -> None:
        broker = _RecordingBroker()

        message = _publish_body(
            broker=broker,  # type: ignore[arg-type]
            queue_name="chatting.auxiliary-ingress.v1",
            body={"hello": "world"},
            now=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(message.body, {"hello": "world"})
        self.assertEqual(len(broker.published), 1)
        queue_name, payload = broker.published[0]
        self.assertEqual(queue_name, "chatting.auxiliary-ingress.v1")
        self.assertEqual(payload["message_type"], "chatting.auxiliary_ingress.v1")
        self.assertEqual(payload["body"], {"hello": "world"})

    def test_normalize_path_rejects_root(self) -> None:
        with self.assertRaises(ValueError):
            _normalize_path("/")

    def test_handler_accepts_matching_post_and_queues_body(self) -> None:
        broker = _RecordingBroker()
        handler_cls = _build_handler(
            broker=broker,  # type: ignore[arg-type]
            queue_name="chatting.auxiliary-ingress.v1",
            generic_post_path="/secret-path",
        )

        handler = handler_cls.__new__(handler_cls)
        handler.path = "/secret-path"
        handler.headers = {"Content-Length": "17"}
        handler.rfile = io.BytesIO(b'{"hello":"world"}')
        handler.wfile = io.BytesIO()
        responses: list[int] = []
        sent_headers: list[tuple[str, str]] = []
        handler.send_response = responses.append  # type: ignore[method-assign]
        handler.send_header = lambda key, value: sent_headers.append((key, value))  # type: ignore[method-assign]
        handler.end_headers = lambda: None  # type: ignore[method-assign]
        handler.send_error = lambda code, message=None: responses.append(code)  # type: ignore[method-assign]

        handler.do_POST()

        self.assertEqual(responses, [200])
        self.assertEqual(len(broker.published), 1)
        payload = json.loads(handler.wfile.getvalue().decode("utf-8"))
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(broker.published[0][1]["body"], {"hello": "world"})
        self.assertIn(("Content-Type", "application/json"), sent_headers)

    def test_handler_rejects_invalid_json(self) -> None:
        broker = _RecordingBroker()
        handler_cls = _build_handler(
            broker=broker,  # type: ignore[arg-type]
            queue_name="chatting.auxiliary-ingress.v1",
            generic_post_path="/secret-path",
        )

        handler = handler_cls.__new__(handler_cls)
        handler.path = "/secret-path"
        handler.headers = {"Content-Length": "5"}
        handler.rfile = io.BytesIO(b"nope!")
        handler.wfile = io.BytesIO()
        responses: list[tuple[int, str | None]] = []
        handler.send_response = lambda code: None  # type: ignore[method-assign]
        handler.send_header = lambda key, value: None  # type: ignore[method-assign]
        handler.end_headers = lambda: None  # type: ignore[method-assign]
        handler.send_error = lambda code, message=None: responses.append(
            (code, message)
        )  # type: ignore[method-assign]

        handler.do_POST()

        self.assertEqual(responses, [(400, "request body must be valid JSON")])
        self.assertEqual(broker.published, [])
