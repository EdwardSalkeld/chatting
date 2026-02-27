"""Bootstrap entrypoint for the milestone-01 prototype flow."""

from __future__ import annotations

import argparse
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from app.applier import NoOpApplier
from app.connectors import (
    CronTrigger,
    EmailMessage,
    FakeCronConnector,
    FakeEmailConnector,
)
from app.executor import StubExecutor
from app.models import RunRecord, TaskEnvelope
from app.policy import AllowlistPolicyEngine
from app.router import RuleBasedRouter
from app.state import SQLiteStateStore


def run_bootstrap(db_path: str) -> list[RunRecord]:
    """Run one deterministic pass of the bootstrap control flow."""
    store = SQLiteStateStore(db_path)
    router = RuleBasedRouter()
    executor = StubExecutor()
    policy = AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"}))
    applier = NoOpApplier()

    for envelope in _default_envelopes():
        if store.seen(envelope.dedupe_key):
            print(f"skip duplicate dedupe_key={envelope.dedupe_key}")
            continue

        store.mark_seen(envelope.dedupe_key)

        started = time.perf_counter()
        task = router.route(envelope)
        execution_result = executor.execute(task)
        decision = policy.evaluate(execution_result)
        applier.apply(decision)
        latency_ms = int((time.perf_counter() - started) * 1000)

        record = RunRecord(
            run_id=f"run:{envelope.id}",
            envelope_id=envelope.id,
            source=envelope.source,
            workflow=task.workflow,
            policy_profile=task.policy_profile,
            latency_ms=latency_ms,
            result_status=_result_status(decision.reason_codes),
            created_at=datetime.now(timezone.utc),
        )
        store.append_run(record)
        print(
            f"processed envelope_id={envelope.id} status={record.result_status} "
            f"workflow={task.workflow}"
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
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.db_path:
        db_path = args.db_path
    else:
        db_path = str(Path(tempfile.gettempdir()) / "chatting-bootstrap-state.db")
    run_bootstrap(db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
