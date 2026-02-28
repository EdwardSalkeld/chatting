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
from typing import Sequence

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

    started = time.perf_counter()
    task = router.route(envelope)
    reason_codes: list[str] = []
    result_status = "dead_letter"
    approved_action_count = 0
    blocked_action_count = 0
    approved_message_count = 0
    attempt_count = 0
    last_error: str | None = None

    for attempt in range(1, max_attempts + 1):
        attempt_count = attempt
        try:
            execution_result = executor_impl.execute(task)
            decision = policy.evaluate(execution_result)
            applier.apply(decision)
            reason_codes = decision.reason_codes
            approved_action_count = len(decision.approved_actions)
            blocked_action_count = len(decision.blocked_actions)
            approved_message_count = len(decision.approved_messages)
            result_status = _result_status(reason_codes)
            break
        except Exception as exc:  # noqa: BLE001 - convert failures into retry/DLQ state
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_attempts:
                print(
                    f"retry_scheduled run_id=run:{envelope.id} attempt={attempt} "
                    f"next_attempt={attempt + 1} max_attempts={max_attempts} error={last_error}"
                )
            else:
                reason_codes = ["retry_exhausted"]
                print(
                    f"dead_letter run_id=run:{envelope.id} attempts={attempt} "
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
                "reason_codes": reason_codes,
                "approved_action_count": approved_action_count,
                "blocked_action_count": blocked_action_count,
                "approved_message_count": approved_message_count,
                "attempt_count": attempt_count,
                "max_attempts": max_attempts,
                "last_error": last_error,
            },
            created_at=record.created_at,
        )
    )
    print(
        f"run_observed run_id={record.run_id} envelope_id={record.envelope_id} "
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
        "--db-path",
        help="Path to SQLite state database. Uses a temp file when omitted.",
    )
    parser.add_argument(
        "--max-attempts",
        type=_positive_int,
        default=2,
        help="Maximum executor attempts per task before marking dead-letter (default: 2).",
    )
    parser.add_argument(
        "--run-live",
        action="store_true",
        help="Run long-lived integration mode (schedule + IMAP + SMTP + Codex).",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=_positive_float,
        default=30.0,
        help="Polling interval between live connector sweeps (default: 30).",
    )
    parser.add_argument(
        "--max-loops",
        type=_positive_int,
        help="Optional loop limit for live mode (useful for smoke checks).",
    )
    parser.add_argument(
        "--base-dir",
        default=".",
        help="Base directory for write_file actions in live mode (default: current directory).",
    )
    parser.add_argument(
        "--schedule-file",
        help="JSON file defining interval schedule jobs for live cron integration.",
    )
    parser.add_argument("--imap-host", help="IMAP host for live email polling.")
    parser.add_argument(
        "--imap-port",
        type=_positive_int,
        default=993,
        help="IMAP port (default: 993).",
    )
    parser.add_argument("--imap-username", help="IMAP username.")
    parser.add_argument(
        "--imap-password-env",
        default="CHATTING_IMAP_PASSWORD",
        help="Environment variable name containing IMAP password.",
    )
    parser.add_argument(
        "--imap-mailbox",
        default="INBOX",
        help="IMAP mailbox name (default: INBOX).",
    )
    parser.add_argument(
        "--imap-search",
        default="UNSEEN",
        help="IMAP search criterion (default: UNSEEN).",
    )
    parser.add_argument("--smtp-host", help="SMTP host for outbound email dispatch.")
    parser.add_argument(
        "--smtp-port",
        type=_positive_int,
        default=465,
        help="SMTP port (default: 465).",
    )
    parser.add_argument("--smtp-username", help="SMTP username.")
    parser.add_argument(
        "--smtp-password-env",
        default="CHATTING_SMTP_PASSWORD",
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
        default="codex exec --json",
        help="Command used for live Codex execution (default: 'codex exec --json').",
    )
    parser.add_argument(
        "--use-stub-executor",
        action="store_true",
        help="Use deterministic stub executor in live mode (smoke/integration testing).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.db_path:
        db_path = args.db_path
    else:
        db_path = str(Path(tempfile.gettempdir()) / "chatting-bootstrap-state.db")

    if args.run_live:
        if args.imap_host and not args.smtp_host:
            raise ValueError("--smtp-host is required when --imap-host is set in live mode")
        connectors = _build_live_connectors(args)
        email_sender = _build_email_sender(args)
        executor = _build_codex_executor(args)
        run_live(
            db_path,
            connectors=connectors,
            executor=executor,
            max_attempts=args.max_attempts,
            poll_interval_seconds=args.poll_interval_seconds,
            max_loops=args.max_loops,
            base_dir=args.base_dir,
            email_sender=email_sender,
        )
        return 0

    run_bootstrap(db_path, max_attempts=args.max_attempts)
    return 0


def _build_live_connectors(args: argparse.Namespace) -> list[Connector]:
    connectors: list[Connector] = []

    if args.schedule_file:
        jobs = _load_schedule_jobs(args.schedule_file)
        connectors.append(IntervalScheduleConnector(jobs=jobs))

    if args.imap_host:
        if not args.imap_username:
            raise ValueError("--imap-username is required when --imap-host is set")
        password = os.environ.get(args.imap_password_env, "")
        if not password:
            raise ValueError(f"missing IMAP password env var: {args.imap_password_env}")
        connectors.append(
            ImapEmailConnector(
                host=args.imap_host,
                port=args.imap_port,
                username=args.imap_username,
                password=password,
                mailbox=args.imap_mailbox,
                search_criterion=args.imap_search,
                context_refs=list(args.context_ref),
            )
        )

    if not connectors:
        raise ValueError(
            "live mode requires at least one connector (--schedule-file and/or --imap-host)"
        )
    return connectors


def _build_email_sender(args: argparse.Namespace) -> SmtpEmailSender | None:
    if not args.smtp_host:
        return None

    from_address = args.smtp_from or args.smtp_username
    if not from_address:
        raise ValueError("--smtp-from or --smtp-username is required when --smtp-host is set")

    password = None
    if args.smtp_username:
        password = os.environ.get(args.smtp_password_env, "")
        if not password:
            raise ValueError(f"missing SMTP password env var: {args.smtp_password_env}")

    return SmtpEmailSender(
        host=args.smtp_host,
        port=args.smtp_port,
        from_address=from_address,
        username=args.smtp_username,
        password=password,
        use_ssl=not args.smtp_starttls,
        starttls=args.smtp_starttls,
    )


def _build_codex_executor(args: argparse.Namespace) -> Executor:
    if args.use_stub_executor:
        return StubExecutor()
    command = tuple(shlex.split(args.codex_command))
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


if __name__ == "__main__":
    raise SystemExit(main())
