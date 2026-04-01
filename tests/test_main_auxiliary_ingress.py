import io
import json
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path

from app.main_auxiliary_ingress import (
    _build_handler,
    _load_config,
    _normalize_path,
    _parse_ingress_route,
    _publish_body,
    _resolve_ingress_routes,
)


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
            queue_by_path={"/secret-path": "chatting.auxiliary-ingress.v1"},
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
            queue_by_path={"/secret-path": "chatting.auxiliary-ingress.v1"},
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

    def test_parse_ingress_route_accepts_secret_suffix_without_slash(self) -> None:
        self.assertEqual(
            _parse_ingress_route("generic-post:12334"),
            ("generic-post", "/12334"),
        )

    def test_resolve_ingress_routes_prefers_dynamic_config(self) -> None:
        routes = _resolve_ingress_routes(
            Namespace(ingress_route=[]),
            {"ingress_routes": ["generic-post:12334", "new-service:/secret-two"]},
        )

        self.assertEqual(
            routes,
            {
                "/12334": "generic-post",
                "/secret-two": "new-service",
            },
        )

    def test_resolve_ingress_routes_requires_route_config(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least one ingress route"):
            _resolve_ingress_routes(Namespace(ingress_route=[]), {})

    def test_load_config_accepts_ingress_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "auxiliary-ingress.json"
            config_path.write_text(
                json.dumps({"ingress_routes": ["generic-post:12334"]}),
                encoding="utf-8",
            )

            payload = _load_config(str(config_path))

        self.assertEqual(payload, {"ingress_routes": ["generic-post:12334"]})
