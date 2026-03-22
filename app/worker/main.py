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

from app.broker import BBMBQueueAdapter, EGRESS_QUEUE_NAME, EgressQueueMessage, TASK_QUEUE_NAME, TaskQueueMessage
from app.worker.executor import EXECUTION_RESULT_JSON_SCHEMA, CodexExecutor, Executor
from app.worker.policy import AllowlistPolicyEngine
from app.worker.router import RuleBasedRouter
from app.state import SQLiteStateStore
from app.worker.runtime import process_task_message

WORKER_CONFIG_PATH_ENV_VAR = "CHATTING_WORKER_CONFIG_PATH"
LOGGER = logging.getLogger(__name__)
ALLOWED_WORKER_CONFIG_KEYS = frozenset(
    {
        "bbmb_address",
        "claude_command",
        "codex_command",
        "codex_working_dir",
        "db_path",
        "max_attempts",
        "max_loops",
        "poll_timeout_seconds",
        "sleep_seconds",
    }
)
BBMB_PICKUP_WAIT_SECONDS = 10


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
    parser.add_argument("--max-attempts", type=_positive_int, help="Maximum execution attempts per task.")
    parser.add_argument("--max-loops", type=_positive_int, help="Optional loop limit for smoke tests.")
    parser.add_argument("--poll-timeout-seconds", type=_positive_int, help="Queue pickup timeout seconds.")
    parser.add_argument("--sleep-seconds", type=_positive_float, help="Sleep duration after empty pickup.")
    parser.add_argument("--codex-command", help="Base Codex command (e.g. 'codex exec'). JSON args appended automatically.")
    parser.add_argument("--claude-command", help="Base Claude command (e.g. 'claude'). Structured output args appended automatically.")
    parser.add_argument(
        "--codex-working-dir",
        help="Working directory used only for launching executor subprocesses.",
    )
    return parser.parse_args()


def _load_config(config_path: str | None, environ: Mapping[str, str] | None = None) -> dict[str, object]:
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


def _resolve_str(cli_value: str | None, config_value: object, *, default_value: str, setting_name: str) -> str:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        return default_value
    if not isinstance(config_value, str) or not config_value.strip():
        raise ValueError(f"config {setting_name} must be a non-empty string")
    return config_value


def _resolve_optional_str(cli_value: str | None, config_value: object, *, setting_name: str) -> str | None:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        return None
    if not isinstance(config_value, str) or not config_value.strip():
        raise ValueError(f"config {setting_name} must be a non-empty string")
    return config_value


def _resolve_positive_int(cli_value: int | None, config_value: object, *, default_value: int, setting_name: str) -> int:
    if cli_value is not None:
        return cli_value
    if config_value is None:
        return default_value
    if not isinstance(config_value, int) or isinstance(config_value, bool) or config_value <= 0:
        raise ValueError(f"config {setting_name} must be a positive integer")
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


def _resolve_bool(cli_value: bool, config_value: object, *, default_value: bool, setting_name: str) -> bool:
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
        command = tuple(shlex.split(codex_raw)) + ("--json",)
    elif claude_raw:
        command = tuple(shlex.split(claude_raw)) + (
            "-p",
            "--output-format", "json",
            "--json-schema", EXECUTION_RESULT_JSON_SCHEMA,
        )
    else:
        command = ("codex", "exec", "--json")

    if not command:
        raise ValueError("codex_command or claude_command must be configured")

    return CodexExecutor(command=command, cwd=codex_working_dir)


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

    store = SQLiteStateStore(db_path)
    broker = BBMBQueueAdapter(address=bbmb_address)
    broker.ensure_queue(TASK_QUEUE_NAME)
    broker.ensure_queue(EGRESS_QUEUE_NAME)

    router = RuleBasedRouter()
    policy = AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"}))
    executor = _build_executor(args, config)
    replay_done = False

    loop_count = 0
    while True:
        loop_count += 1
        if not replay_done:
            _replay_egress_outbox(store=store, broker=broker)
            replay_done = True
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
            result = process_task_message(
                store=store,
                task_message=task_message,
                router=router,
                executor_impl=executor,
                policy=policy,
                max_attempts=max_attempts,
            )
            for egress_message in result.egress_messages:
                _publish_egress_with_outbox(
                    store=store,
                    broker=broker,
                    egress_message=egress_message,
                )
            broker.ack(TASK_QUEUE_NAME, picked.guid)
            LOGGER.info(
                "worker_processed run_id=%s task_id=%s egress_messages=%s result_status=%s",
                result.run_record.run_id,
                task_message.task_id,
                len(result.egress_messages),
                result.run_record.result_status,
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception("worker_processing_failed guid=%s", picked.guid)

        if max_loops and loop_count >= max_loops:
            break

    return 0


def _publish_egress_with_outbox(
    *,
    store: SQLiteStateStore,
    broker: BBMBQueueAdapter,
    egress_message: EgressQueueMessage,
) -> None:
    store.queue_egress_outbox_event(egress_message)
    broker.publish_json(EGRESS_QUEUE_NAME, egress_message.to_dict())
    if egress_message.event_id is None:
        return
    store.mark_egress_outbox_event_published(event_id=egress_message.event_id)


def _replay_egress_outbox(
    *,
    store: SQLiteStateStore,
    broker: BBMBQueueAdapter,
) -> None:
    replayable = store.list_replayable_egress_outbox_events()
    if not replayable:
        return
    for message in replayable:
        broker.publish_json(EGRESS_QUEUE_NAME, message.to_dict())
        if message.event_id is None:
            continue
        store.mark_egress_outbox_event_published(event_id=message.event_id)
    LOGGER.info("worker_egress_outbox_replayed count=%s", len(replayable))


if __name__ == "__main__":
    sys.exit(main())
