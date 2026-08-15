"""Worker entrypoint: consume task queue, execute, publish egress events."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import sys
import tempfile
import time
from pathlib import Path
from typing import Mapping

from app.broker import (
    BBMBQueueAdapter,
    EgressQueueMessage,
    TASK_QUEUE_NAME,
    TaskQueueMessage,
)
from app.egress_client import DEFAULT_HANDLER_EGRESS_URL, submit_egress
from app.worker.activity import (
    DEFAULT_ACTIVITY_HISTORY_LIMIT,
    DEFAULT_ACTIVITY_HOST,
    DEFAULT_ACTIVITY_PORT,
    WorkerActivityMonitor,
    WorkerActivityServer,
    start_worker_activity_server,
)
from app.worker.executor import CodexExecutor, Executor
from app.state import SQLiteStateStore
from app.worker.runtime import process_task_message

WORKER_CONFIG_PATH_ENV_VAR = "CHATTING_WORKER_CONFIG_PATH"
LOGGER = logging.getLogger(__name__)
ALLOWED_WORKER_CONFIG_KEYS = frozenset(
    {
        "activity_port",
        "bbmb_address",
        "claude_command",
        "codex_command",
        "codex_working_dir",
        "db_path",
        "activity_history_limit",
        "handler_egress_url",
        "handler_api_url",
        "max_attempts",
        "max_loops",
        "poll_timeout_seconds",
        "sleep_seconds",
    }
)
BBMB_PICKUP_WAIT_SECONDS = 10


def _log_worker_processed(*, task_id: str, result) -> None:
    if result.run_record.result_status in {"execution_error", "dead_letter"}:
        LOGGER.warning(
            "worker_processed run_id=%s task_id=%s egress_messages=%s result_status=%s "
            "reason_codes=%s attempt_count=%s error_summary=%s",
            result.run_record.run_id,
            task_id,
            len(result.egress_messages),
            result.run_record.result_status,
            ",".join(result.reason_codes) if result.reason_codes else "none",
            result.attempt_count,
            result.error_summary or "unknown_error",
        )
        return
    LOGGER.info(
        "worker_processed run_id=%s task_id=%s egress_messages=%s result_status=%s",
        result.run_record.run_id,
        task_id,
        len(result.egress_messages),
        result.run_record.result_status,
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


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be a non-negative integer")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the chatting worker process.")
    parser.add_argument("--config", help="Path to JSON config file.")
    parser.add_argument("--db-path", help="Path to worker SQLite state DB.")
    parser.add_argument("--bbmb-address", help="BBMB broker address host:port.")
    parser.add_argument(
        "--handler-egress-url",
        help="Handler synchronous egress endpoint URL (default the worker config's handler_egress_url).",
    )
    parser.add_argument(
        "--handler-api-url",
        help="Handler schedule/reminder API base URL (default http://127.0.0.1:9464).",
    )
    parser.add_argument(
        "--max-attempts",
        type=_positive_int,
        help="Maximum execution attempts per task.",
    )
    parser.add_argument(
        "--max-loops", type=_positive_int, help="Optional loop limit for smoke tests."
    )
    parser.add_argument(
        "--poll-timeout-seconds",
        type=_positive_int,
        help="Queue pickup timeout seconds.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=_positive_float,
        help="Sleep duration after empty pickup.",
    )
    parser.add_argument(
        "--codex-command", help="Executor command to launch for Codex runs."
    )
    parser.add_argument(
        "--claude-command", help="Executor command to launch for Claude runs."
    )
    parser.add_argument(
        "--activity-history-limit",
        type=_positive_int,
        help="Maximum recent worker activity events to show in the UI.",
    )
    parser.add_argument(
        "--activity-port",
        type=_non_negative_int,
        help="Worker activity HTTP port. Use 0 to pick an ephemeral port.",
    )
    parser.add_argument(
        "--codex-working-dir",
        help="Working directory used only for launching executor subprocesses.",
    )
    return parser.parse_args()


def _load_config(
    config_path: str | None, environ: Mapping[str, str] | None = None
) -> dict[str, object]:
    env = os.environ if environ is None else environ
    path = config_path
    if path is None:
        raw_env_path = env.get(WORKER_CONFIG_PATH_ENV_VAR)
        if raw_env_path is not None:
            if not raw_env_path.strip():
                raise ValueError(f"{WORKER_CONFIG_PATH_ENV_VAR} must not be empty")
            path = raw_env_path

    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("config file must contain a JSON object")
    unknown_keys = sorted(set(payload.keys()) - ALLOWED_WORKER_CONFIG_KEYS)
    if unknown_keys:
        raise ValueError("config contains unknown keys: " + ", ".join(unknown_keys))
    return payload


def _resolve_str(
    cli_value: str | None,
    config_value: object,
    *,
    default_value: str,
    setting_name: str,
) -> str:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        return default_value
    if not isinstance(config_value, str) or not config_value.strip():
        raise ValueError(f"config {setting_name} must be a non-empty string")
    return config_value


def _resolve_optional_str(
    cli_value: str | None, config_value: object, *, setting_name: str
) -> str | None:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        return None
    if not isinstance(config_value, str) or not config_value.strip():
        raise ValueError(f"config {setting_name} must be a non-empty string")
    return config_value


def _resolve_positive_int(
    cli_value: int | None,
    config_value: object,
    *,
    default_value: int,
    setting_name: str,
) -> int:
    if cli_value is not None:
        return cli_value
    if config_value is None:
        return default_value
    if (
        not isinstance(config_value, int)
        or isinstance(config_value, bool)
        or config_value <= 0
    ):
        raise ValueError(f"config {setting_name} must be a positive integer")
    return config_value


def _resolve_non_negative_int(
    cli_value: int | None,
    config_value: object,
    *,
    default_value: int,
    setting_name: str,
) -> int:
    if cli_value is not None:
        return cli_value
    if config_value is None:
        return default_value
    if (
        not isinstance(config_value, int)
        or isinstance(config_value, bool)
        or config_value < 0
    ):
        raise ValueError(f"config {setting_name} must be a non-negative integer")
    return config_value


def _resolve_positive_float(
    cli_value: float | None,
    config_value: object,
    *,
    default_value: float,
    setting_name: str,
) -> float:
    if cli_value is not None:
        return cli_value
    candidate = default_value if config_value is None else config_value
    if isinstance(candidate, bool) or not isinstance(candidate, (int, float)):
        raise ValueError(f"config {setting_name} must be numeric")
    parsed = float(candidate)
    if parsed <= 0:
        raise ValueError(f"config {setting_name} must be positive")
    return parsed


def _resolve_bool(
    cli_value: bool, config_value: object, *, default_value: bool, setting_name: str
) -> bool:
    if cli_value:
        return True
    if config_value is None:
        return default_value
    if not isinstance(config_value, bool):
        raise ValueError(f"config {setting_name} must be a boolean")
    return config_value


def _build_executor(args: argparse.Namespace, config: dict[str, object]) -> Executor:
    codex_working_dir = _resolve_optional_str(
        args.codex_working_dir,
        config.get("codex_working_dir"),
        setting_name="codex_working_dir",
    )

    # Prefer codex_command if set, fall back to claude_command
    codex_raw = _resolve_optional_str(
        args.codex_command,
        config.get("codex_command"),
        setting_name="codex_command",
    )
    claude_raw = _resolve_optional_str(
        getattr(args, "claude_command", None),
        config.get("claude_command"),
        setting_name="claude_command",
    )

    if codex_raw:
        command = tuple(shlex.split(codex_raw))
    elif claude_raw:
        command = tuple(shlex.split(claude_raw))
    else:
        command = ("codex", "exec", "--json")

    if not command:
        raise ValueError("codex_command or claude_command must be configured")

    executor_env = _build_executor_env(args.config, os.environ)
    handler_api_url = _resolve_str(
        args.handler_api_url,
        config.get("handler_api_url"),
        default_value="http://127.0.0.1:9464",
        setting_name="handler_api_url",
    )
    return CodexExecutor(
        command=command,
        cwd=codex_working_dir,
        env=executor_env,
        handler_api_url=handler_api_url,
    )


def _build_executor_env(
    config_path: str | None, environ: Mapping[str, str]
) -> dict[str, str] | None:
    if config_path is None:
        return None
    env = dict(environ)
    env[WORKER_CONFIG_PATH_ENV_VAR] = str(Path(config_path).resolve())
    return env


def main() -> int:
    _configure_logging()
    args = _parse_args()
    config = _load_config(args.config, os.environ)

    db_path = _resolve_str(
        args.db_path,
        config.get("db_path"),
        default_value=str(Path(tempfile.gettempdir()) / "chatting-worker-state.db"),
        setting_name="db_path",
    )
    bbmb_address = _resolve_str(
        args.bbmb_address,
        config.get("bbmb_address"),
        default_value="127.0.0.1:9876",
        setting_name="bbmb_address",
    )
    handler_egress_url = _resolve_str(
        args.handler_egress_url,
        config.get("handler_egress_url"),
        default_value=DEFAULT_HANDLER_EGRESS_URL,
        setting_name="handler_egress_url",
    )
    max_attempts = _resolve_positive_int(
        args.max_attempts,
        config.get("max_attempts"),
        default_value=2,
        setting_name="max_attempts",
    )
    max_loops = _resolve_positive_int(
        args.max_loops,
        config.get("max_loops"),
        default_value=0,
        setting_name="max_loops",
    )
    poll_timeout_seconds = _resolve_positive_int(
        args.poll_timeout_seconds,
        config.get("poll_timeout_seconds"),
        default_value=20,
        setting_name="poll_timeout_seconds",
    )
    sleep_seconds = _resolve_positive_float(
        args.sleep_seconds,
        config.get("sleep_seconds"),
        default_value=1.0,
        setting_name="sleep_seconds",
    )
    activity_history_limit = _resolve_positive_int(
        args.activity_history_limit,
        config.get("activity_history_limit"),
        default_value=DEFAULT_ACTIVITY_HISTORY_LIMIT,
        setting_name="activity_history_limit",
    )
    activity_port = _resolve_non_negative_int(
        args.activity_port,
        config.get("activity_port"),
        default_value=DEFAULT_ACTIVITY_PORT,
        setting_name="activity_port",
    )

    store = SQLiteStateStore(db_path)
    activity_monitor = WorkerActivityMonitor(
        store=store,
        history_limit=activity_history_limit,
    )
    activity_server: WorkerActivityServer = start_worker_activity_server(
        host=DEFAULT_ACTIVITY_HOST,
        port=activity_port,
        monitor=activity_monitor,
    )
    broker = BBMBQueueAdapter(address=bbmb_address)
    broker.ensure_queue(TASK_QUEUE_NAME)

    executor = _build_executor(args, config)

    try:
        loop_count = 0
        while True:
            loop_count += 1
            # Replay any egress still pending in the outbox (a prior POST that
            # failed transiently, or a crash before ack). Cheap when empty.
            _replay_egress_outbox(
                store=store,
                handler_egress_url=handler_egress_url,
                activity_monitor=activity_monitor,
            )
            picked = broker.pickup_json(
                TASK_QUEUE_NAME,
                timeout_seconds=poll_timeout_seconds,
                wait_seconds=BBMB_PICKUP_WAIT_SECONDS,
            )
            if picked is None:
                LOGGER.info("worker_loop_empty loop=%s", loop_count)
                if max_loops and loop_count >= max_loops:
                    break
                time.sleep(sleep_seconds)
                continue

            try:
                task_message = TaskQueueMessage.from_dict(picked.payload)
                activity_monitor.record_task_received(task_message=task_message)
                result = process_task_message(
                    store=store,
                    task_message=task_message,
                    executor_impl=executor,
                    max_attempts=max_attempts,
                    activity_monitor=activity_monitor,
                )
                for egress_message in result.egress_messages:
                    _publish_egress_with_outbox(
                        store=store,
                        handler_egress_url=handler_egress_url,
                        egress_message=egress_message,
                        activity_monitor=activity_monitor,
                    )
                broker.ack(TASK_QUEUE_NAME, picked.guid)
                _log_worker_processed(task_id=task_message.task_id, result=result)
            except Exception:  # noqa: BLE001
                LOGGER.exception("worker_processing_failed guid=%s", picked.guid)

            if max_loops and loop_count >= max_loops:
                break
        return 0
    finally:
        activity_server.shutdown()


def _publish_egress_with_outbox(
    *,
    store: SQLiteStateStore,
    handler_egress_url: str,
    egress_message: EgressQueueMessage,
    activity_monitor: WorkerActivityMonitor,
) -> None:
    # Persist to the outbox first (write-ahead), then submit to the handler. If
    # the submit fails transiently the event stays pending and is replayed on a
    # later loop; the task is still acked, so we never re-run the executor just
    # because a reply could not be delivered yet.
    store.queue_egress_outbox_event(egress_message)
    _deliver_egress_event(
        store=store,
        handler_egress_url=handler_egress_url,
        egress_message=egress_message,
        activity_monitor=activity_monitor,
        publish_source="worker",
    )


def _replay_egress_outbox(
    *,
    store: SQLiteStateStore,
    handler_egress_url: str,
    activity_monitor: WorkerActivityMonitor,
) -> None:
    replayable = store.list_replayable_egress_outbox_events()
    if not replayable:
        return
    delivered = 0
    for message in replayable:
        if _deliver_egress_event(
            store=store,
            handler_egress_url=handler_egress_url,
            egress_message=message,
            activity_monitor=activity_monitor,
            publish_source="outbox_replay",
        ):
            delivered += 1
    LOGGER.info(
        "worker_egress_outbox_replayed pending=%s delivered=%s",
        len(replayable),
        delivered,
    )


def _deliver_egress_event(
    *,
    store: SQLiteStateStore,
    handler_egress_url: str,
    egress_message: EgressQueueMessage,
    activity_monitor: WorkerActivityMonitor,
    publish_source: str,
) -> bool:
    """Submit one egress event to the handler and settle its outbox row.

    Returns True when the handler accepted it (delivered). 2xx acks the row;
    a 422 is a permanent drop — logged loudly and acked so it is not retried
    forever; anything else (handler unreachable / 5xx) leaves the row pending
    for the next replay.
    """
    status_code, response = submit_egress(handler_egress_url, egress_message.to_dict())
    reason = str(response.get("reason", "")) if isinstance(response, dict) else ""

    if status_code in (200, 202):
        if egress_message.event_id is not None:
            store.mark_egress_outbox_event_acked(event_id=egress_message.event_id)
        activity_monitor.record_egress(
            egress_message=egress_message,
            publish_source=publish_source,
        )
        return True

    if status_code == 422:
        # The handler rejected it for good (bad payload, unknown task,
        # disallowed channel, dispatch failure). Dropping egress is an error, so
        # make it loud; ack the row so it does not replay endlessly.
        LOGGER.error(
            "worker_egress_dropped_by_handler task_id=%s event_id=%s channel=%s reason=%s",
            egress_message.task_id,
            egress_message.event_id,
            egress_message.message.channel,
            reason or "unknown",
        )
        if egress_message.event_id is not None:
            store.mark_egress_outbox_event_acked(event_id=egress_message.event_id)
        return False

    # Handler unreachable or server error: leave the row pending for replay.
    LOGGER.warning(
        "worker_egress_delivery_deferred task_id=%s event_id=%s http_status=%s reason=%s",
        egress_message.task_id,
        egress_message.event_id,
        status_code,
        reason or "unknown",
    )
    return False


if __name__ == "__main__":
    sys.exit(main())
