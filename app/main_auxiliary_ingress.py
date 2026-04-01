"""HTTP ingress that publishes raw JSON bodies to BBMB for handler-side pickup."""

from __future__ import annotations

import argparse
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Mapping

from app.broker import (
    AUXILIARY_INGRESS_QUEUE_NAME,
    AuxiliaryIngressQueueMessage,
    BBMBQueueAdapter,
)

MAIN_AUXILIARY_INGRESS_CONFIG_PATH_ENV_VAR = "CHATTING_AUXILIARY_INGRESS_CONFIG_PATH"
LOGGER = logging.getLogger("app.main_auxiliary_ingress")
_ALLOWED_CONFIG_KEYS = frozenset(
    {
        "bbmb_address",
        "listen_host",
        "listen_port",
        "generic_post_path",
        "queue_name",
        "ingress_routes",
    }
)


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
    parser.add_argument("--config", help="Path to JSON config file.")
    parser.add_argument("--bbmb-address", help="BBMB address host:port.")
    parser.add_argument("--listen-host", default="127.0.0.1", help="HTTP bind host.")
    parser.add_argument(
        "--listen-port", type=_positive_int, default=9481, help="HTTP bind port."
    )
    parser.add_argument(
        "--generic-post-path",
        help="Exact secret path that accepts JSON POST bodies.",
    )
    parser.add_argument(
        "--queue-name",
        default=AUXILIARY_INGRESS_QUEUE_NAME,
        help="BBMB queue name for auxiliary ingress payloads.",
    )
    parser.add_argument(
        "--ingress-route",
        action="append",
        default=[],
        help="Ingress route in the form queue_name:path_token or queue_name:/path.",
    )
    return parser.parse_args()


def _normalize_path(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("path must not be empty")
    if not stripped.startswith("/"):
        stripped = "/" + stripped
    if stripped == "/":
        raise ValueError("path must not be /")
    return stripped.rstrip("/") or stripped


def _load_config(
    config_path: str | None, environ: Mapping[str, str] | None = None
) -> dict[str, object]:
    env = os.environ if environ is None else environ
    path = config_path
    if path is None:
        raw = env.get(MAIN_AUXILIARY_INGRESS_CONFIG_PATH_ENV_VAR)
        if raw is not None:
            if not raw.strip():
                raise ValueError(
                    f"{MAIN_AUXILIARY_INGRESS_CONFIG_PATH_ENV_VAR} must not be empty"
                )
            path = raw
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("config file must contain a JSON object")
    unknown_keys = sorted(set(payload.keys()) - _ALLOWED_CONFIG_KEYS)
    if unknown_keys:
        raise ValueError("config contains unknown keys: " + ", ".join(unknown_keys))
    return payload


def _parse_ingress_route(raw_value: str) -> tuple[str, str]:
    raw = raw_value.strip()
    if not raw:
        raise ValueError("ingress route must not be empty")
    queue_name, separator, path_value = raw.partition(":")
    if not separator:
        raise ValueError("ingress route must be queue_name:path")
    queue_name = queue_name.strip()
    if not queue_name:
        raise ValueError("ingress route queue_name must not be empty")
    return queue_name, _normalize_path(path_value)


def _resolve_ingress_routes(
    args: argparse.Namespace, config: dict[str, object]
) -> dict[str, str]:
    route_specs: list[tuple[str, str]] = []
    raw_config_routes = config.get("ingress_routes")
    if raw_config_routes is not None:
        if not isinstance(raw_config_routes, list) or not all(
            isinstance(item, str) for item in raw_config_routes
        ):
            raise ValueError("config ingress_routes must be a list of strings")
        route_specs.extend(_parse_ingress_route(item) for item in raw_config_routes)

    route_specs.extend(_parse_ingress_route(item) for item in args.ingress_route)

    legacy_path = config.get("generic_post_path")
    if legacy_path is None:
        legacy_path = args.generic_post_path
    legacy_queue_name = config.get("queue_name")
    if legacy_queue_name is None:
        legacy_queue_name = args.queue_name
    if legacy_path is not None:
        if legacy_queue_name is not None and not isinstance(legacy_queue_name, str):
            raise ValueError("config queue_name must be a string")
        route_specs.append(
            (
                (legacy_queue_name or AUXILIARY_INGRESS_QUEUE_NAME).strip(),
                _normalize_path(str(legacy_path)),
            )
        )

    if not route_specs:
        raise ValueError("at least one ingress route must be configured")

    queue_by_path: dict[str, str] = {}
    seen_queues: set[str] = set()
    for queue_name, path in route_specs:
        if queue_name in seen_queues:
            raise ValueError(f"duplicate ingress route queue_name: {queue_name}")
        if path in queue_by_path:
            raise ValueError(f"duplicate ingress route path: {path}")
        seen_queues.add(queue_name)
        queue_by_path[path] = queue_name
    return queue_by_path


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
    queue_by_path: dict[str, str],
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            try:
                request_path = _normalize_path(self.path)
            except ValueError:
                self.send_error(404)
                return
            queue_name = queue_by_path.get(request_path)
            if queue_name is None:
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
    config = _load_config(args.config)
    queue_by_path = _resolve_ingress_routes(args, config)
    bbmb_address = args.bbmb_address or config.get("bbmb_address") or "127.0.0.1:9876"
    if not isinstance(bbmb_address, str):
        raise ValueError("config bbmb_address must be a string")
    listen_host = args.listen_host
    if "listen_host" in config and args.listen_host == "127.0.0.1":
        configured_listen_host = config["listen_host"]
        if not isinstance(configured_listen_host, str):
            raise ValueError("config listen_host must be a string")
        listen_host = configured_listen_host
    listen_port = args.listen_port
    if "listen_port" in config and args.listen_port == 9481:
        configured_listen_port = config["listen_port"]
        if not isinstance(configured_listen_port, int) or configured_listen_port <= 0:
            raise ValueError("config listen_port must be a positive integer")
        listen_port = configured_listen_port

    broker = BBMBQueueAdapter(address=bbmb_address)
    for queue_name in queue_by_path.values():
        broker.ensure_queue(queue_name)
    server = HTTPServer(
        (listen_host, listen_port),
        _build_handler(
            broker=broker,
            queue_by_path=queue_by_path,
        ),
    )
    LOGGER.info(
        "auxiliary_ingress_listening host=%s port=%s routes=%s",
        listen_host,
        listen_port,
        ",".join(
            f"{path}->{queue_name}"
            for path, queue_name in sorted(queue_by_path.items())
        ),
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
