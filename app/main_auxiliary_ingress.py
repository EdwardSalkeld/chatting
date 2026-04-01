"""HTTP ingress that publishes raw JSON bodies to BBMB for handler-side pickup."""

from __future__ import annotations

import argparse
import json
import logging
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from app.broker import (
    AUXILIARY_INGRESS_QUEUE_NAME,
    AuxiliaryIngressQueueMessage,
    BBMBQueueAdapter,
)

LOGGER = logging.getLogger("app.main_auxiliary_ingress")


def _configure_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the auxiliary ingress HTTP listener."
    )
    parser.add_argument("--bbmb-address", help="BBMB address host:port.")
    parser.add_argument("--listen-host", default="127.0.0.1", help="HTTP bind host.")
    parser.add_argument(
        "--listen-port", type=_positive_int, default=9481, help="HTTP bind port."
    )
    parser.add_argument(
        "--generic-post-path",
        required=True,
        help="Exact secret path that accepts JSON POST bodies.",
    )
    parser.add_argument(
        "--queue-name",
        default=AUXILIARY_INGRESS_QUEUE_NAME,
        help="BBMB queue name for auxiliary ingress payloads.",
    )
    return parser.parse_args()


def _normalize_path(value: str) -> str:
    stripped = value.strip()
    if not stripped.startswith("/"):
        raise ValueError("generic_post_path must start with /")
    if stripped == "/":
        raise ValueError("generic_post_path must not be /")
    return stripped.rstrip("/") or stripped


def _read_json_body(handler: BaseHTTPRequestHandler) -> object:
    content_length_header = handler.headers.get("Content-Length", "").strip()
    if not content_length_header:
        raise ValueError("missing Content-Length")
    content_length = int(content_length_header)
    raw_body = handler.rfile.read(content_length)
    try:
        decoded = raw_body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("request body must be UTF-8 JSON") from error
    return json.loads(decoded)


def _publish_body(
    *,
    broker: BBMBQueueAdapter,
    queue_name: str,
    body: object,
    now: datetime | None = None,
) -> AuxiliaryIngressQueueMessage:
    message = AuxiliaryIngressQueueMessage(
        event_id=f"aux:{uuid.uuid4()}",
        received_at=now or datetime.now(timezone.utc),
        body=body,
    )
    broker.publish_json(queue_name, message.to_dict())
    return message


def _build_handler(
    *,
    broker: BBMBQueueAdapter,
    queue_name: str,
    generic_post_path: str,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            try:
                request_path = _normalize_path(self.path)
            except ValueError:
                self.send_error(404)
                return
            if request_path != generic_post_path:
                self.send_error(404)
                return
            try:
                body = _read_json_body(self)
                published = _publish_body(
                    broker=broker,
                    queue_name=queue_name,
                    body=body,
                )
            except json.JSONDecodeError:
                self.send_error(400, "request body must be valid JSON")
                return
            except ValueError as error:
                self.send_error(400, str(error))
                return
            except Exception:  # noqa: BLE001
                LOGGER.exception("auxiliary_ingress_publish_failed")
                self.send_error(500, "failed to publish payload")
                return

            response = json.dumps(
                {"status": "queued", "event_id": published.event_id},
                sort_keys=True,
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, format: str, *args: object) -> None:
            LOGGER.info("http_access " + format, *args)

    return Handler


def main() -> int:
    _configure_logging()
    args = _parse_args()
    broker = BBMBQueueAdapter(address=args.bbmb_address or "127.0.0.1:9876")
    broker.ensure_queue(args.queue_name)
    server = HTTPServer(
        (args.listen_host, args.listen_port),
        _build_handler(
            broker=broker,
            queue_name=args.queue_name,
            generic_post_path=_normalize_path(args.generic_post_path),
        ),
    )
    LOGGER.info(
        "auxiliary_ingress_listening host=%s port=%s path=%s queue=%s",
        args.listen_host,
        args.listen_port,
        args.generic_post_path,
        args.queue_name,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("auxiliary_ingress_stopped")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
