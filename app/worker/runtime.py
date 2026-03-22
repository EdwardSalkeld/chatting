"""Worker-side task processing for BBMB split architecture."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from app.broker import EgressQueueMessage, TaskQueueMessage
from app.executor import Executor
from app.internal_heartbeat import (
    build_internal_completion_egress,
    build_internal_heartbeat_egress,
    is_internal_heartbeat_envelope,
)
from app.models import AuditEvent, OutboundMessage, RunRecord
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
    if is_internal_heartbeat_envelope(envelope):
        return _process_internal_heartbeat(
            store=store,
            task_message=task_message,
        )

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

            terminal_egress_messages = _build_completion_egress_messages(
                task_message=task_message,
                starting_sequence=0,
                visible_error_body=_build_credit_exhausted_visible_error(
                    execution_errors=execution_result.errors,
                    last_error=None,
                ),
            )
            egress_messages = terminal_egress_messages
            break
        except Exception as exc:  # noqa: BLE001
            last_error = f"{type(exc).__name__}: {exc}"
            last_error_stage = error_stage
            if attempt == max_attempts:
                reason_codes = ["retry_exhausted"]
                result_status = "dead_letter"
                egress_messages = _build_completion_egress_messages(
                    task_message=task_message,
                    starting_sequence=0,
                    visible_error_body=_build_credit_exhausted_visible_error(
                        execution_errors=[],
                        last_error=last_error,
                    ),
                )

    latency_ms = int((time.perf_counter() - started) * 1000)
    run_record = RunRecord(
        run_id=run_id,
        envelope_id=envelope.id,
        source=envelope.source,
        workflow=task.workflow,
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
                "incremental_reply_send_requested_count": 0,
                "incremental_reply_send_published_count": 0,
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


def _process_internal_heartbeat(
    *,
    store: StateStore,
    task_message: TaskQueueMessage,
) -> WorkerProcessResult:
    worker_received_at = datetime.now(timezone.utc)
    visible_egress_message = build_internal_heartbeat_egress(
        task_message=task_message,
        worker_received_at=worker_received_at,
    )
    completion_egress_message = build_internal_completion_egress(
        task_message=task_message,
        sequence=1,
        emitted_at=worker_received_at,
    )
    ping_emitted_at = task_message.emitted_at.astimezone(timezone.utc)
    run_record = RunRecord(
        run_id=f"run:{task_message.task_id}:{time.time_ns()}",
        envelope_id=task_message.envelope.id,
        source=task_message.envelope.source,
        workflow="internal_heartbeat",
        latency_ms=max(int((worker_received_at - ping_emitted_at).total_seconds() * 1000), 0),
        result_status="success",
        created_at=worker_received_at,
    )
    store.append_run(run_record)
    store.append_audit_event(
        AuditEvent(
            run_id=run_record.run_id,
            envelope_id=run_record.envelope_id,
            source=run_record.source,
            workflow=run_record.workflow,
            result_status=run_record.result_status,
            detail={
                "trace_id": task_message.trace_id,
                "task_id": task_message.task_id,
                "reason_codes": ["internal_heartbeat"],
                "attempt_count": 1,
                "max_attempts": 1,
                "last_error": None,
                "last_error_stage": None,
                "execution_result": {
                    "actions": [],
                    "errors": [],
                },
                "policy_decision": {
                    "approved_actions": [],
                    "blocked_actions": [],
                    "approved_messages": [visible_egress_message.message.to_dict()],
                    "reason_codes": ["internal_heartbeat"],
                },
                "incremental_reply_send_requested_count": 0,
                "incremental_reply_send_published_count": 0,
                "egress_message_count": 2,
                "heartbeat": json.loads(visible_egress_message.message.body or "{}"),
            },
            created_at=run_record.created_at,
        )
    )
    return WorkerProcessResult(
        run_record=run_record,
        egress_messages=[visible_egress_message, completion_egress_message],
        dead_lettered=False,
    )


def _build_completion_egress_messages(
    *,
    task_message: TaskQueueMessage,
    starting_sequence: int,
    visible_error_body: str | None = None,
) -> list[EgressQueueMessage]:
    emitted_at = datetime.now(timezone.utc)
    messages: list[EgressQueueMessage] = []
    completion_sequence = starting_sequence
    event_count = 1

    if visible_error_body is not None:
        event_count = 2
        messages.append(
            _build_visible_error_egress(
                task_message=task_message,
                sequence=starting_sequence,
                event_count=event_count,
                emitted_at=emitted_at,
                body=visible_error_body,
            )
        )
        completion_sequence += 1

    messages.append(
        _build_completion_egress(
            task_message=task_message,
            sequence=completion_sequence,
            event_count=event_count,
            emitted_at=emitted_at,
        )
    )
    return messages


def _build_visible_error_egress(
    *,
    task_message: TaskQueueMessage,
    sequence: int,
    event_count: int,
    emitted_at: datetime,
    body: str,
) -> EgressQueueMessage:
    return EgressQueueMessage(
        task_id=task_message.task_id,
        envelope_id=task_message.envelope.id,
        trace_id=task_message.trace_id,
        event_index=sequence,
        event_count=event_count,
        message=OutboundMessage(
            channel=task_message.envelope.reply_channel.type,
            target=task_message.envelope.reply_channel.target,
            body=body,
        ),
        emitted_at=emitted_at,
        event_id=_event_id_for_sequence(
            task_id=task_message.task_id,
            sequence=sequence,
            event_kind="message",
            dedupe_key="credits-exhausted",
        ),
        sequence=sequence,
        event_kind="message",
        message_type="chatting.egress.v2",
    )

def _build_completion_egress(
    *,
    task_message: TaskQueueMessage,
    sequence: int,
    event_count: int,
    emitted_at: datetime,
) -> EgressQueueMessage:
    return EgressQueueMessage(
        task_id=task_message.task_id,
        envelope_id=task_message.envelope.id,
        trace_id=task_message.trace_id,
        event_index=sequence,
        event_count=event_count,
        message=OutboundMessage(
            channel="internal",
            target="task",
            body="Worker completed; task closure is internal-only.",
        ),
        emitted_at=emitted_at,
        event_id=_event_id_for_sequence(
            task_id=task_message.task_id,
            sequence=sequence,
            event_kind="completion",
            dedupe_key="internal",
        ),
        sequence=sequence,
        event_kind="completion",
        message_type="chatting.egress.v2",
    )


def _result_status(reason_codes: list[str]) -> str:
    if "action_not_allowed" in reason_codes:
        return "blocked_action"
    if "executor_reported_errors" in reason_codes:
        return "execution_error"
    if "retry_exhausted" in reason_codes:
        return "dead_letter"
    return "success"


def _build_credit_exhausted_visible_error(
    *,
    execution_errors: list[str],
    last_error: str | None,
) -> str | None:
    candidates = [*execution_errors]
    if last_error is not None:
        candidates.append(last_error)
    for candidate in candidates:
        if _looks_like_credit_exhaustion(candidate):
            return "Codex ran out of credits or quota, so this task did not complete."
    return None


def _looks_like_credit_exhaustion(error: str) -> bool:
    lowered = error.lower()
    return any(
        needle in lowered
        for needle in (
            "out of credits",
            "ran out of credits",
            "insufficient credits",
            "credit balance",
            "quota",
            "usage limit",
        )
    )


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _event_id_for_sequence(
    *,
    task_id: str,
    sequence: int,
    event_kind: str,
    dedupe_key: str | None,
) -> str:
    if dedupe_key is None:
        return f"evt:{task_id}:{sequence}:{event_kind}"
    return f"evt:{task_id}:{sequence}:{event_kind}:{dedupe_key}"


__all__ = [
    "WorkerProcessResult",
    "process_task_message",
]
