"""Bootstrap entrypoint for the milestone-01 prototype flow."""

from __future__ import annotations

import argparse
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from app.applier import NoOpApplier
from app.connectors import (
    CronTrigger,
    EmailMessage,
    FakeCronConnector,
    FakeEmailConnector,
)
from app.executor import Executor, StubExecutor
from app.models import AuditEvent, RunRecord, TaskEnvelope
from app.policy import AllowlistPolicyEngine
from app.router import RuleBasedRouter
from app.state import SQLiteStateStore, StateStore


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("max_attempts must be a positive integer")
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
        if store.seen(envelope.source, envelope.dedupe_key):
            print(f"skip duplicate source={envelope.source} dedupe_key={envelope.dedupe_key}")
            continue

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

    runs = store.list_runs()
    print(f"completed runs={len(runs)} db_path={db_path}")
    return runs


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
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.db_path:
        db_path = args.db_path
    else:
        db_path = str(Path(tempfile.gettempdir()) / "chatting-bootstrap-state.db")
    run_bootstrap(db_path, max_attempts=args.max_attempts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
