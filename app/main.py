"""Bootstrap and live entrypoints for the prototype flow."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from app.applier import IntegratedApplier, NoOpApplier, SmtpEmailSender
from app.connectors import (
    Connector,
    CronTrigger,
    EmailMessage,
    FakeCronConnector,
    FakeEmailConnector,
    ImapEmailConnector,
    IntervalScheduleConnector,
    IntervalScheduleJob,
)
from app.executor import CodexExecutor, Executor, StubExecutor
from app.models import AuditEvent, RunRecord, TaskEnvelope
from app.policy import AllowlistPolicyEngine
from app.router import RuleBasedRouter
from app.state import SQLiteStateStore, StateStore

CONFIG_PATH_ENV_VAR = "CHATTING_CONFIG_PATH"
ALLOWED_RUNTIME_CONFIG_KEYS = frozenset(
    {
        "base_dir",
        "codex_command",
        "context_ref",
        "context_refs",
        "db_path",
        "imap_host",
        "imap_mailbox",
        "imap_password_env",
        "imap_port",
        "imap_search",
        "imap_username",
        "max_attempts",
        "max_loops",
        "poll_interval_seconds",
        "schedule_file",
        "smtp_from",
        "smtp_host",
        "smtp_password_env",
        "smtp_port",
        "smtp_starttls",
        "smtp_username",
        "use_stub_executor",
    }
)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("max_attempts must be a positive integer")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("poll_interval_seconds must be positive")
    return parsed


def run_bootstrap(
    db_path: str,
    *,
    envelopes: Sequence[TaskEnvelope] | None = None,
    executor: Executor | None = None,
    max_attempts: int = 2,
) -> list[RunRecord]:
    """Run one deterministic pass of the bootstrap control flow."""
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")

    store: StateStore = SQLiteStateStore(db_path)
    router = RuleBasedRouter()
    executor_impl = executor if executor is not None else StubExecutor()
    policy = AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"}))
    applier = NoOpApplier()
    pending_envelopes = list(envelopes) if envelopes is not None else _default_envelopes()

    for envelope in pending_envelopes:
        _process_envelope(
            store=store,
            envelope=envelope,
            router=router,
            executor_impl=executor_impl,
            policy=policy,
            applier=applier,
            max_attempts=max_attempts,
        )

    runs = store.list_runs()
    print(f"completed runs={len(runs)} db_path={db_path}")
    return runs


def run_live(
    db_path: str,
    *,
    connectors: Sequence[Connector],
    executor: Executor | None = None,
    max_attempts: int = 2,
    poll_interval_seconds: float = 30.0,
    max_loops: int | None = None,
    base_dir: str = ".",
    email_sender: SmtpEmailSender | None = None,
) -> list[RunRecord]:
    """Run a long-lived polling loop for scheduled and email integrations."""
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")
    if max_loops is not None and max_loops <= 0:
        raise ValueError("max_loops must be positive when provided")

    store: StateStore = SQLiteStateStore(db_path)
    router = RuleBasedRouter()
    executor_impl = executor if executor is not None else CodexExecutor()
    policy = AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"}))
    applier = IntegratedApplier(base_dir=base_dir, email_sender=email_sender)

    loop_count = 0
    while True:
        loop_count += 1
        processed_count = 0
        for connector in connectors:
            for envelope in connector.poll():
                record = _process_envelope(
                    store=store,
                    envelope=envelope,
                    router=router,
                    executor_impl=executor_impl,
                    policy=policy,
                    applier=applier,
                    max_attempts=max_attempts,
                )
                if record is not None:
                    processed_count += 1

        print(f"live_loop_completed loop={loop_count} processed={processed_count}")

        if max_loops is not None and loop_count >= max_loops:
            break
        time.sleep(poll_interval_seconds)

    return store.list_runs()


def _process_envelope(
    *,
    store: StateStore,
    envelope: TaskEnvelope,
    router: RuleBasedRouter,
    executor_impl: Executor,
    policy: AllowlistPolicyEngine,
    applier: NoOpApplier | IntegratedApplier,
    max_attempts: int,
) -> RunRecord | None:
    if store.seen(envelope.source, envelope.dedupe_key):
        print(f"skip duplicate source={envelope.source} dedupe_key={envelope.dedupe_key}")
        return None

    store.mark_seen(envelope.source, envelope.dedupe_key)
    trace_id = f"trace:{envelope.id}"

    started = time.perf_counter()
    task = router.route(envelope)
    reason_codes: list[str] = []
    result_status = "dead_letter"
    approved_action_count = 0
    blocked_action_count = 0
    approved_message_count = 0
    execution_message_count = 0
    execution_action_count = 0
    execution_config_update_count = 0
    execution_error_count = 0
    execution_action_types: list[str] = []
    requires_human_review = False
    applied_action_count = 0
    skipped_action_count = 0
    dispatched_message_count = 0
    apply_reason_codes: list[str] = []
    attempt_count = 0
    last_error: str | None = None

    for attempt in range(1, max_attempts + 1):
        attempt_count = attempt
        try:
            execution_result = executor_impl.execute(task)
            execution_message_count = len(execution_result.messages)
            execution_action_count = len(execution_result.actions)
            execution_config_update_count = len(execution_result.config_updates)
            execution_error_count = len(execution_result.errors)
            execution_action_types = [action.type for action in execution_result.actions]
            requires_human_review = execution_result.requires_human_review
            decision = policy.evaluate(execution_result)
            apply_result = applier.apply(decision)
            reason_codes = decision.reason_codes
            approved_action_count = len(decision.approved_actions)
            blocked_action_count = len(decision.blocked_actions)
            approved_message_count = len(decision.approved_messages)
            applied_action_count = len(apply_result.applied_actions)
            skipped_action_count = len(apply_result.skipped_actions)
            dispatched_message_count = len(apply_result.dispatched_messages)
            apply_reason_codes = apply_result.reason_codes
            result_status = _result_status(reason_codes)
            break
        except Exception as exc:  # noqa: BLE001 - convert failures into retry/DLQ state
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_attempts:
                print(
                    f"retry_scheduled trace_id={trace_id} run_id=run:{envelope.id} attempt={attempt} "
                    f"next_attempt={attempt + 1} max_attempts={max_attempts} error={last_error}"
                )
            else:
                reason_codes = ["retry_exhausted"]
                print(
                    f"dead_letter trace_id={trace_id} run_id=run:{envelope.id} attempts={attempt} "
                    f"max_attempts={max_attempts} error={last_error}"
                )

    latency_ms = int((time.perf_counter() - started) * 1000)

    record = RunRecord(
        run_id=f"run:{envelope.id}",
        envelope_id=envelope.id,
        source=envelope.source,
        workflow=task.workflow,
        policy_profile=task.policy_profile,
        latency_ms=latency_ms,
        result_status=result_status,
        created_at=datetime.now(timezone.utc),
    )
    store.append_run(record)
    store.append_audit_event(
        AuditEvent(
            run_id=record.run_id,
            envelope_id=record.envelope_id,
            source=record.source,
            workflow=record.workflow,
            policy_profile=record.policy_profile,
            result_status=record.result_status,
            detail={
                "trace_id": trace_id,
                "reason_codes": reason_codes,
                "approved_action_count": approved_action_count,
                "blocked_action_count": blocked_action_count,
                "approved_message_count": approved_message_count,
                "execution_summary": {
                    "message_count": execution_message_count,
                    "action_count": execution_action_count,
                    "config_update_count": execution_config_update_count,
                    "error_count": execution_error_count,
                    "action_types": execution_action_types,
                    "requires_human_review": requires_human_review,
                },
                "applied_action_count": applied_action_count,
                "skipped_action_count": skipped_action_count,
                "dispatched_message_count": dispatched_message_count,
                "apply_reason_codes": apply_reason_codes,
                "attempt_count": attempt_count,
                "max_attempts": max_attempts,
                "last_error": last_error,
            },
            created_at=record.created_at,
        )
    )
    print(
        f"run_observed trace_id={trace_id} run_id={record.run_id} envelope_id={record.envelope_id} "
        f"source={record.source} workflow={record.workflow} "
        f"policy_profile={record.policy_profile} latency_ms={record.latency_ms} "
        f"result_status={record.result_status}"
    )
    return record


def _default_envelopes() -> list[TaskEnvelope]:
    cron = FakeCronConnector(
        triggers=[
            CronTrigger(
                job_name="daily-summary",
                content="Generate daily summary",
                scheduled_for=datetime(2026, 2, 27, 9, 0, tzinfo=timezone.utc),
                context_refs=["repo:/home/edward/chatting"],
            )
        ]
    ).poll()

    email = FakeEmailConnector(
        messages=[
            EmailMessage(
                provider_message_id="ok-1",
                from_address="alice@example.com",
                subject="Hello",
                body="Please summarize this thread.",
                received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
                context_refs=["repo:/home/edward/chatting"],
            ),
            EmailMessage(
                provider_message_id="blocked-1",
                from_address="bob@example.com",
                subject="Run command",
                body="Need shell access.",
                received_at=datetime(2026, 2, 27, 16, 1, tzinfo=timezone.utc),
                context_refs=["repo:/home/edward/chatting"],
            ),
            EmailMessage(
                provider_message_id="ok-1",
                from_address="alice@example.com",
                subject="Hello duplicate",
                body="Same provider message ID should dedupe.",
                received_at=datetime(2026, 2, 27, 16, 2, tzinfo=timezone.utc),
                context_refs=["repo:/home/edward/chatting"],
            ),
        ]
    ).poll()

    return [*cron, *email]


def _result_status(reason_codes: list[str]) -> str:
    if "action_not_allowed" in reason_codes:
        return "blocked_action"
    if "executor_reported_errors" in reason_codes:
        return "execution_error"
    return "success"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run bootstrap prototype flow.")
    parser.add_argument(
        "--config",
        help="Path to JSON config file. CLI flags override config values.",
    )
    parser.add_argument(
        "--db-path",
        help="Path to SQLite state database. Uses a temp file when omitted.",
    )
    parser.add_argument(
        "--max-attempts",
        type=_positive_int,
        help="Maximum executor attempts per task before marking dead-letter.",
    )
    parser.add_argument(
        "--run-live",
        action="store_true",
        help="Run long-lived integration mode (schedule + IMAP + SMTP + Codex).",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=_positive_float,
        help="Polling interval between live connector sweeps.",
    )
    parser.add_argument(
        "--max-loops",
        type=_positive_int,
        help="Optional loop limit for live mode (useful for smoke checks).",
    )
    parser.add_argument(
        "--base-dir",
        help="Base directory for write_file actions in live mode.",
    )
    parser.add_argument(
        "--schedule-file",
        help="JSON file defining interval schedule jobs for live cron integration.",
    )
    parser.add_argument("--imap-host", help="IMAP host for live email polling.")
    parser.add_argument(
        "--imap-port",
        type=_positive_int,
        help="IMAP port.",
    )
    parser.add_argument("--imap-username", help="IMAP username.")
    parser.add_argument(
        "--imap-password-env",
        help="Environment variable name containing IMAP password.",
    )
    parser.add_argument(
        "--imap-mailbox",
        help="IMAP mailbox name.",
    )
    parser.add_argument(
        "--imap-search",
        help="IMAP search criterion.",
    )
    parser.add_argument("--smtp-host", help="SMTP host for outbound email dispatch.")
    parser.add_argument(
        "--smtp-port",
        type=_positive_int,
        help="SMTP port.",
    )
    parser.add_argument("--smtp-username", help="SMTP username.")
    parser.add_argument(
        "--smtp-password-env",
        help="Environment variable name containing SMTP password.",
    )
    parser.add_argument(
        "--smtp-from",
        help="From address for outbound SMTP email (defaults to --smtp-username).",
    )
    parser.add_argument(
        "--smtp-starttls",
        action="store_true",
        help="Use STARTTLS (plain SMTP + TLS upgrade) instead of implicit SSL.",
    )
    parser.add_argument(
        "--context-ref",
        action="append",
        default=[],
        help="Context reference added to live connector envelopes (repeatable).",
    )
    parser.add_argument(
        "--codex-command",
        help="Command used for live Codex execution.",
    )
    parser.add_argument(
        "--use-stub-executor",
        action="store_true",
        help="Use deterministic stub executor in live mode (smoke/integration testing).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    config = _load_runtime_config(args.config, os.environ)
    db_path = _resolve_str(
        cli_value=args.db_path,
        config_value=config.get("db_path"),
        default_value=str(Path(tempfile.gettempdir()) / "chatting-bootstrap-state.db"),
        setting_name="db_path",
    )
    max_attempts = _resolve_positive_int(
        cli_value=args.max_attempts,
        config_value=config.get("max_attempts"),
        default_value=2,
        setting_name="max_attempts",
    )

    if args.run_live:
        imap_host = _resolve_optional_str(
            cli_value=args.imap_host,
            config_value=config.get("imap_host"),
            setting_name="imap_host",
        )
        smtp_host = _resolve_optional_str(
            cli_value=args.smtp_host,
            config_value=config.get("smtp_host"),
            setting_name="smtp_host",
        )
        if imap_host and not smtp_host:
            raise ValueError("--smtp-host is required when --imap-host is set in live mode")
        connectors = _build_live_connectors(args, config)
        email_sender = _build_email_sender(args, config)
        executor = _build_codex_executor(args, config)
        poll_interval_seconds = _resolve_positive_float(
            cli_value=args.poll_interval_seconds,
            config_value=config.get("poll_interval_seconds"),
            default_value=30.0,
            setting_name="poll_interval_seconds",
        )
        max_loops = _resolve_optional_positive_int(
            cli_value=args.max_loops,
            config_value=config.get("max_loops"),
            setting_name="max_loops",
        )
        base_dir = _resolve_str(
            cli_value=args.base_dir,
            config_value=config.get("base_dir"),
            default_value=".",
            setting_name="base_dir",
        )
        run_live(
            db_path,
            connectors=connectors,
            executor=executor,
            max_attempts=max_attempts,
            poll_interval_seconds=poll_interval_seconds,
            max_loops=max_loops,
            base_dir=base_dir,
            email_sender=email_sender,
        )
        return 0

    run_bootstrap(db_path, max_attempts=max_attempts)
    return 0


def _build_live_connectors(args: argparse.Namespace, config: dict[str, object]) -> list[Connector]:
    connectors: list[Connector] = []

    schedule_file = _resolve_optional_str(
        cli_value=args.schedule_file,
        config_value=config.get("schedule_file"),
        setting_name="schedule_file",
    )
    if schedule_file:
        jobs = _load_schedule_jobs(schedule_file)
        connectors.append(IntervalScheduleConnector(jobs=jobs))

    imap_host = _resolve_optional_str(
        cli_value=args.imap_host,
        config_value=config.get("imap_host"),
        setting_name="imap_host",
    )
    if imap_host:
        imap_username = _resolve_optional_str(
            cli_value=args.imap_username,
            config_value=config.get("imap_username"),
            setting_name="imap_username",
        )
        if not imap_username:
            raise ValueError("--imap-username is required when --imap-host is set")
        imap_password_env = _resolve_str(
            cli_value=args.imap_password_env,
            config_value=config.get("imap_password_env"),
            default_value="CHATTING_IMAP_PASSWORD",
            setting_name="imap_password_env",
        )
        password = os.environ.get(imap_password_env, "")
        if not password:
            raise ValueError(f"missing IMAP password env var: {imap_password_env}")
        context_refs = _resolve_context_refs(args.context_ref, config)
        connectors.append(
            ImapEmailConnector(
                host=imap_host,
                port=_resolve_positive_int(
                    cli_value=args.imap_port,
                    config_value=config.get("imap_port"),
                    default_value=993,
                    setting_name="imap_port",
                ),
                username=imap_username,
                password=password,
                mailbox=_resolve_str(
                    cli_value=args.imap_mailbox,
                    config_value=config.get("imap_mailbox"),
                    default_value="INBOX",
                    setting_name="imap_mailbox",
                ),
                search_criterion=_resolve_str(
                    cli_value=args.imap_search,
                    config_value=config.get("imap_search"),
                    default_value="UNSEEN",
                    setting_name="imap_search",
                ),
                context_refs=context_refs,
            )
        )

    if not connectors:
        raise ValueError(
            "live mode requires at least one connector (--schedule-file and/or --imap-host)"
    )
    return connectors


def _build_email_sender(args: argparse.Namespace, config: dict[str, object]) -> SmtpEmailSender | None:
    smtp_host = _resolve_optional_str(
        cli_value=args.smtp_host,
        config_value=config.get("smtp_host"),
        setting_name="smtp_host",
    )
    if not smtp_host:
        return None

    smtp_username = _resolve_optional_str(
        cli_value=args.smtp_username,
        config_value=config.get("smtp_username"),
        setting_name="smtp_username",
    )
    from_address = _resolve_optional_str(
        cli_value=args.smtp_from,
        config_value=config.get("smtp_from"),
        setting_name="smtp_from",
    ) or smtp_username
    if not from_address:
        raise ValueError("--smtp-from or --smtp-username is required when --smtp-host is set")

    password = None
    if smtp_username:
        smtp_password_env = _resolve_str(
            cli_value=args.smtp_password_env,
            config_value=config.get("smtp_password_env"),
            default_value="CHATTING_SMTP_PASSWORD",
            setting_name="smtp_password_env",
        )
        password = os.environ.get(smtp_password_env, "")
        if not password:
            raise ValueError(f"missing SMTP password env var: {smtp_password_env}")

    smtp_starttls = _resolve_bool(
        cli_value=args.smtp_starttls,
        config_value=config.get("smtp_starttls"),
        default_value=False,
        setting_name="smtp_starttls",
    )

    return SmtpEmailSender(
        host=smtp_host,
        port=_resolve_positive_int(
            cli_value=args.smtp_port,
            config_value=config.get("smtp_port"),
            default_value=465,
            setting_name="smtp_port",
        ),
        from_address=from_address,
        username=smtp_username,
        password=password,
        use_ssl=not smtp_starttls,
        starttls=smtp_starttls,
    )


def _build_codex_executor(args: argparse.Namespace, config: dict[str, object]) -> Executor:
    use_stub_executor = _resolve_bool(
        cli_value=args.use_stub_executor,
        config_value=config.get("use_stub_executor"),
        default_value=False,
        setting_name="use_stub_executor",
    )
    if use_stub_executor:
        return StubExecutor()
    codex_command = _resolve_str(
        cli_value=args.codex_command,
        config_value=config.get("codex_command"),
        default_value="codex exec --json",
        setting_name="codex_command",
    )
    command = tuple(shlex.split(codex_command))
    if not command:
        raise ValueError("--codex-command must not be empty")
    return CodexExecutor(command=command)


def _load_schedule_jobs(schedule_file: str) -> list[IntervalScheduleJob]:
    payload = json.loads(Path(schedule_file).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("schedule file must contain a JSON array")

    jobs: list[IntervalScheduleJob] = []
    for index, raw_job in enumerate(payload):
        if not isinstance(raw_job, dict):
            raise ValueError(f"schedule job at index {index} must be an object")
        raw_start_at = raw_job.get("start_at")
        start_at = _parse_optional_rfc3339(raw_start_at) if raw_start_at is not None else None
        jobs.append(
            IntervalScheduleJob(
                job_name=str(raw_job["job_name"]),
                content=str(raw_job["content"]),
                interval_seconds=int(raw_job["interval_seconds"]),
                context_refs=list(raw_job.get("context_refs", [])),
                policy_profile=str(raw_job.get("policy_profile", "default")),
                start_at=start_at,
            )
        )
    return jobs


def _parse_optional_rfc3339(value: str) -> datetime:
    parsed_value = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(parsed_value)
    if parsed.tzinfo is None:
        raise ValueError("schedule job start_at must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _load_runtime_config(
    config_path: str | None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    config_source = config_path
    env = os.environ if environ is None else environ

    if config_source is None:
        raw_env_path = env.get(CONFIG_PATH_ENV_VAR)
        if raw_env_path is not None:
            if not raw_env_path.strip():
                raise ValueError(f"{CONFIG_PATH_ENV_VAR} must not be empty")
            config_source = raw_env_path

    if not config_source:
        return {}
    payload = json.loads(Path(config_source).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("config file must contain a JSON object")
    unknown_keys = sorted(set(payload.keys()) - ALLOWED_RUNTIME_CONFIG_KEYS)
    if unknown_keys:
        keys = ", ".join(unknown_keys)
        raise ValueError(f"config contains unknown keys: {keys}")
    return payload


def _resolve_str(
    *,
    cli_value: str | None,
    config_value: object,
    default_value: str,
    setting_name: str,
) -> str:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        if not default_value.strip():
            raise ValueError(f"default {setting_name} must not be empty")
        return default_value
    if not isinstance(config_value, str):
        raise ValueError(f"config {setting_name} must be a string")
    if not config_value.strip():
        raise ValueError(f"config {setting_name} must not be empty")
    return config_value


def _resolve_optional_str(
    *,
    cli_value: str | None,
    config_value: object,
    setting_name: str,
) -> str | None:
    if cli_value is not None:
        if not cli_value.strip():
            raise ValueError(f"{setting_name} must not be empty")
        return cli_value
    if config_value is None:
        return None
    if not isinstance(config_value, str):
        raise ValueError(f"config {setting_name} must be a string")
    if not config_value.strip():
        raise ValueError(f"config {setting_name} must not be empty")
    return config_value


def _resolve_positive_int(
    *,
    cli_value: int | None,
    config_value: object,
    default_value: int,
    setting_name: str,
) -> int:
    if cli_value is not None:
        return cli_value
    candidate = config_value if config_value is not None else default_value
    if not isinstance(candidate, int):
        raise ValueError(f"config {setting_name} must be an integer")
    if candidate <= 0:
        raise ValueError(f"config {setting_name} must be positive")
    return candidate


def _resolve_optional_positive_int(
    *,
    cli_value: int | None,
    config_value: object,
    setting_name: str,
) -> int | None:
    if cli_value is not None:
        return cli_value
    if config_value is None:
        return None
    if not isinstance(config_value, int):
        raise ValueError(f"config {setting_name} must be an integer")
    if config_value <= 0:
        raise ValueError(f"config {setting_name} must be positive")
    return config_value


def _resolve_positive_float(
    *,
    cli_value: float | None,
    config_value: object,
    default_value: float,
    setting_name: str,
) -> float:
    if cli_value is not None:
        return cli_value
    candidate = config_value if config_value is not None else default_value
    if not isinstance(candidate, (int, float)):
        raise ValueError(f"config {setting_name} must be numeric")
    parsed = float(candidate)
    if parsed <= 0:
        raise ValueError(f"config {setting_name} must be positive")
    return parsed


def _resolve_bool(
    *,
    cli_value: bool,
    config_value: object,
    default_value: bool,
    setting_name: str,
) -> bool:
    if cli_value:
        return True
    if config_value is None:
        return default_value
    if not isinstance(config_value, bool):
        raise ValueError(f"config {setting_name} must be a boolean")
    return config_value


def _resolve_context_refs(cli_values: list[str], config: dict[str, object]) -> list[str]:
    raw_config_values = config.get("context_ref")
    if raw_config_values is None:
        raw_config_values = config.get("context_refs")

    if raw_config_values is None:
        config_values: list[str] = []
    else:
        if not isinstance(raw_config_values, list):
            raise ValueError("config context_ref/context_refs must be a list of strings")
        if not all(isinstance(item, str) for item in raw_config_values):
            raise ValueError("config context_ref/context_refs must be a list of strings")
        config_values = list(raw_config_values)

    merged_values = [*config_values, *cli_values]
    if any(not value.strip() for value in merged_values):
        raise ValueError("context_ref/context_refs entries must not be empty")
    return merged_values


if __name__ == "__main__":
    raise SystemExit(main())
