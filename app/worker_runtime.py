"""Worker-side task processing for BBMB split architecture."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.executor import Executor
from app.models import AuditEvent, OutboundMessage, PolicyDecision, RunRecord
from app.policy import AllowlistPolicyEngine
from app.router import RuleBasedRouter
from app.state import StateStore


@dataclass(frozen=True)
class WorkerProcessResult:
    """Outcome of one worker task processing run."""

    run_record: RunRecord
    egress_messages: list[EgressQueueMessage]
    dead_lettered: bool


def process_task_message(
    *,
    store: StateStore,
    task_message: TaskQueueMessage,
    router: RuleBasedRouter,
    executor_impl: Executor,
    policy: AllowlistPolicyEngine,
    max_attempts: int,
) -> WorkerProcessResult:
    """Process one task message and persist run/audit records."""
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")

    envelope = task_message.envelope
    run_id = f"run:{task_message.task_id}:{time.time_ns()}"
    task = router.route(envelope)
    started = time.perf_counter()

    reason_codes: list[str] = []
    result_status = "dead_letter"
    attempt_count = 0
    last_error: str | None = None
    last_error_stage: str | None = None
    execution_payload: dict[str, object] | None = None
    policy_payload: dict[str, object] | None = None
    egress_messages: list[EgressQueueMessage] = []

    for attempt in range(1, max_attempts + 1):
        attempt_count = attempt
        error_stage = "executor"
        try:
            execution_result = executor_impl.execute(task)
            execution_payload = execution_result.to_dict()

            error_stage = "policy"
            decision = policy.evaluate(execution_result)
            policy_payload = decision.to_dict()

            dropped_action_count = len(decision.approved_actions)
            reason_codes = list(decision.reason_codes)
            if dropped_action_count > 0:
                reason_codes.append("approved_actions_not_forwarded_to_egress")
            reason_codes = _dedupe(reason_codes)
            result_status = _result_status(reason_codes)

            egress_messages = _build_egress_messages(
                task_message=task_message,
                decision=decision,
            )
            break
        except Exception as exc:  # noqa: BLE001
            last_error = f"{type(exc).__name__}: {exc}"
            last_error_stage = error_stage
            if attempt == max_attempts:
                reason_codes = ["retry_exhausted"]

    latency_ms = int((time.perf_counter() - started) * 1000)
    run_record = RunRecord(
        run_id=run_id,
        envelope_id=envelope.id,
        source=envelope.source,
        workflow=task.workflow,
        policy_profile=task.policy_profile,
        latency_ms=latency_ms,
        result_status=result_status,
        created_at=datetime.now(timezone.utc),
    )
    store.append_run(run_record)

    store.append_audit_event(
        AuditEvent(
            run_id=run_record.run_id,
            envelope_id=run_record.envelope_id,
            source=run_record.source,
            workflow=run_record.workflow,
            policy_profile=run_record.policy_profile,
            result_status=run_record.result_status,
            detail={
                "trace_id": task_message.trace_id,
                "task_id": task_message.task_id,
                "reason_codes": reason_codes,
                "attempt_count": attempt_count,
                "max_attempts": max_attempts,
                "last_error": last_error,
                "last_error_stage": last_error_stage,
                "execution_result": execution_payload,
                "policy_decision": policy_payload,
                "egress_message_count": len(egress_messages),
            },
            created_at=run_record.created_at,
        )
    )

    dead_lettered = False
    if run_record.result_status == "dead_letter":
        store.append_dead_letter(
            run_id=run_record.run_id,
            envelope=envelope,
            reason_codes=reason_codes,
            last_error=last_error,
            attempt_count=attempt_count,
        )
        dead_lettered = True

    return WorkerProcessResult(
        run_record=run_record,
        egress_messages=egress_messages,
        dead_lettered=dead_lettered,
    )


def _build_egress_messages(
    *,
    task_message: TaskQueueMessage,
    decision: PolicyDecision,
) -> list[EgressQueueMessage]:
    normalized_messages = [
        _normalize_message_for_egress(message=message, task_message=task_message)
        for message in decision.approved_messages
    ]

    return [
        EgressQueueMessage(
            task_id=task_message.task_id,
            envelope_id=task_message.envelope.id,
            trace_id=task_message.trace_id,
            event_index=index,
            event_count=len(normalized_messages),
            message=message,
            emitted_at=datetime.now(timezone.utc),
        )
        for index, message in enumerate(normalized_messages)
    ]


def _normalize_message_for_egress(*, message: OutboundMessage, task_message: TaskQueueMessage) -> OutboundMessage:
    if message.channel != "final":
        return message

    return OutboundMessage(
        channel=task_message.envelope.reply_channel.type,
        target=task_message.envelope.reply_channel.target,
        body=message.body,
    )


def _result_status(reason_codes: list[str]) -> str:
    if "action_not_allowed" in reason_codes:
        return "blocked_action"
    if "executor_reported_errors" in reason_codes:
        return "execution_error"
    if "retry_exhausted" in reason_codes:
        return "dead_letter"
    return "success"


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


__all__ = [
    "WorkerProcessResult",
    "process_task_message",
]
