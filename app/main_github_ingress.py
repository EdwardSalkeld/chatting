"""GitHub assignment polling ingress entrypoint for split-mode task queue."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Mapping

from app.broker import BBMBQueueAdapter, TASK_QUEUE_NAME
from app.github_ingress_runtime import (
    AssignmentCheckpoint,
    GitHubAssignmentCheckpointStore,
    checkpoint_scope_key,
    default_graphql_runner,
    fetch_assignment_events_for_repository,
    parse_repo_slug,
    publish_assignment_events,
    select_events_after_checkpoint,
)
from app.state import SQLiteStateStore

GITHUB_INGRESS_CONFIG_PATH_ENV_VAR = "CHATTING_GITHUB_INGRESS_CONFIG_PATH"
LOGGER = logging.getLogger(__name__)
_ALLOWED_CONFIG_KEYS = frozenset(
    {
        "bbmb_address",
        "db_path",
        "github_assignee_login",
        "github_context_refs",
        "github_max_issues",
        "github_max_timeline_events",
        "github_policy_profile",
        "github_repositories",
        "github_reply_channel_target",
        "github_reply_channel_type",
        "max_loops",
        "poll_interval_seconds",
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


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run GitHub assignment ingress polling.")
    parser.add_argument("--config", help="Path to JSON config file.")
    parser.add_argument("--db-path", help="Path to ingress SQLite state DB.")
    parser.add_argument("--bbmb-address", help="BBMB address host:port.")
    parser.add_argument("--max-loops", type=_positive_int, help="Optional loop limit for smoke runs.")
    parser.add_argument("--poll-interval-seconds", type=_positive_float, help="Polling interval.")
    parser.add_argument(
        "--github-repository",
        action="append",
        default=[],
        help="GitHub repository in owner/repo format (repeatable).",
    )
    parser.add_argument("--github-assignee-login", help="GitHub login to filter assigned events for.")
    parser.add_argument("--github-reply-channel-type", help="Reply channel type for generated tasks.")
    parser.add_argument("--github-reply-channel-target", help="Reply channel target for generated tasks.")
    parser.add_argument(
        "--github-context-ref",
        action="append",
        default=[],
        help="Context ref to attach to generated tasks (repeatable).",
    )
    parser.add_argument("--github-policy-profile", help="Policy profile for generated tasks.")
    parser.add_argument("--github-max-issues", type=_positive_int, help="Per-repo issue scan limit.")
    parser.add_argument(
        "--github-max-timeline-events",
        type=_positive_int,
        help="Per-issue assigned-event scan limit.",
    )
    return parser.parse_args()


def _load_config(config_path: str | None, environ: Mapping[str, str] | None = None) -> dict[str, object]:
    env = os.environ if environ is None else environ
    path = config_path
    if path is None:
        raw = env.get(GITHUB_INGRESS_CONFIG_PATH_ENV_VAR)
        if raw is not None:
            if not raw.strip():
                raise ValueError(f"{GITHUB_INGRESS_CONFIG_PATH_ENV_VAR} must not be empty")
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


def _resolve_str(cli_value: str | None, config_value: object, *, default_value: str, setting_name: str) -> str:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value.strip()
    if config_value is None:
        return default_value
    if not isinstance(config_value, str) or not config_value.strip():
        raise ValueError(f"config {setting_name} must be a non-empty string")
    return config_value.strip()


def _resolve_required_str(cli_value: str | None, config_value: object, *, setting_name: str) -> str:
    resolved = _resolve_str(
        cli_value,
        config_value,
        default_value="",
        setting_name=setting_name,
    )
    if not resolved:
        raise ValueError(f"{setting_name} is required")
    return resolved


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


def _resolve_repositories(args: argparse.Namespace, config: dict[str, object]) -> list[str]:
    config_values = config.get("github_repositories")
    repositories: list[str] = []
    if config_values is not None:
        if not isinstance(config_values, list) or not all(isinstance(item, str) for item in config_values):
            raise ValueError("config github_repositories must be a list of owner/repo strings")
        repositories.extend(config_values)
    repositories.extend(args.github_repository)
    if not repositories:
        raise ValueError("github_repository is required")

    deduped: list[str] = []
    seen: set[str] = set()
    for repository in repositories:
        owner, name = parse_repo_slug(repository)
        normalized = f"{owner}/{name}"
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _resolve_context_refs(args: argparse.Namespace, config: dict[str, object]) -> list[str]:
    merged: list[str] = []
    config_values = config.get("github_context_refs")
    if config_values is not None:
        if not isinstance(config_values, list) or not all(isinstance(item, str) for item in config_values):
            raise ValueError("config github_context_refs must be a list of strings")
        merged.extend(config_values)
    merged.extend(args.github_context_ref)
    if any(not item.strip() for item in merged):
        raise ValueError("github_context_ref entries must not be empty")
    return [item.strip() for item in merged]


def main() -> int:
    _configure_logging()
    args = _parse_args()
    config = _load_config(args.config, os.environ)

    db_path = _resolve_str(
        args.db_path,
        config.get("db_path"),
        default_value=str(Path(tempfile.gettempdir()) / "chatting-github-ingress-state.db"),
        setting_name="db_path",
    )
    bbmb_address = _resolve_str(
        args.bbmb_address,
        config.get("bbmb_address"),
        default_value="127.0.0.1:9876",
        setting_name="bbmb_address",
    )
    max_loops = _resolve_positive_int(
        args.max_loops,
        config.get("max_loops"),
        default_value=0,
        setting_name="max_loops",
    )
    poll_interval_seconds = _resolve_positive_float(
        args.poll_interval_seconds,
        config.get("poll_interval_seconds"),
        default_value=60.0,
        setting_name="poll_interval_seconds",
    )
    repositories = _resolve_repositories(args, config)
    assignee_login = _resolve_required_str(
        args.github_assignee_login,
        config.get("github_assignee_login"),
        setting_name="github_assignee_login",
    )
    reply_channel_type = _resolve_required_str(
        args.github_reply_channel_type,
        config.get("github_reply_channel_type"),
        setting_name="github_reply_channel_type",
    )
    reply_channel_target = _resolve_required_str(
        args.github_reply_channel_target,
        config.get("github_reply_channel_target"),
        setting_name="github_reply_channel_target",
    )
    context_refs = _resolve_context_refs(args, config)
    policy_profile = _resolve_str(
        args.github_policy_profile,
        config.get("github_policy_profile"),
        default_value="default",
        setting_name="github_policy_profile",
    )
    github_max_issues = _resolve_positive_int(
        args.github_max_issues,
        config.get("github_max_issues"),
        default_value=25,
        setting_name="github_max_issues",
    )
    github_max_timeline_events = _resolve_positive_int(
        args.github_max_timeline_events,
        config.get("github_max_timeline_events"),
        default_value=10,
        setting_name="github_max_timeline_events",
    )

    store = SQLiteStateStore(db_path)
    checkpoint_store = GitHubAssignmentCheckpointStore(db_path)
    broker = BBMBQueueAdapter(address=bbmb_address)
    broker.ensure_queue(TASK_QUEUE_NAME)

    scope_key = checkpoint_scope_key(repositories=repositories, assignee_login=assignee_login)

    loop_count = 0
    while True:
        loop_count += 1
        checkpoint = checkpoint_store.get_checkpoint(scope_key)
        scanned_event_count = 0
        events = []
        for repository in repositories:
            owner, name = parse_repo_slug(repository)
            try:
                repository_events = fetch_assignment_events_for_repository(
                    repo_owner=owner,
                    repo_name=name,
                    assignee_login=assignee_login,
                    issue_limit=github_max_issues,
                    timeline_limit=github_max_timeline_events,
                    graphql_runner=default_graphql_runner,
                )
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "github_ingress_poll_failed repository=%s assignee=%s",
                    repository,
                    assignee_login,
                )
                continue
            scanned_event_count += len(repository_events)
            events.extend(repository_events)

        new_events = select_events_after_checkpoint(events, checkpoint=checkpoint)
        published_count = publish_assignment_events(
            events=new_events,
            store=store,
            broker=broker,
            reply_channel_type=reply_channel_type,
            reply_channel_target=reply_channel_target,
            context_refs=context_refs,
            policy_profile=policy_profile,
        )
        if new_events:
            latest = new_events[-1]
            checkpoint_store.set_checkpoint(
                scope_key,
                checkpoint=AssignmentCheckpoint(
                    event_created_at=latest.event_created_at,
                    event_id=latest.event_id,
                ),
            )

        LOGGER.info(
            (
                "github_ingress_loop_completed loop=%s repos=%s scanned_events=%s "
                "new_events=%s published=%s checkpoint=%s"
            ),
            loop_count,
            len(repositories),
            scanned_event_count,
            len(new_events),
            published_count,
            new_events[-1].event_id if new_events else (checkpoint.event_id if checkpoint else "none"),
        )

        if max_loops and loop_count >= max_loops:
            break
        time.sleep(poll_interval_seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
