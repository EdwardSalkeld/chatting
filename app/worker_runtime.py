"""Worker-side task processing for BBMB split architecture."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.executor import Executor
from app.internal_heartbeat import (
    build_internal_heartbeat_egress,
    is_internal_heartbeat_envelope,
)
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
    if is_internal_heartbeat_envelope(envelope):
        return _process_internal_heartbeat(
            store=store,
            task_message=task_message,
        )

    run_id = f"run:{task_message.task_id}:{time.time_ns()}"
    task = router.route(envelope)
    started = time.perf_counter()

    reason_codes: list[str] = []
    incremental_reason_codes: list[str] = []
    result_status = "dead_letter"
    attempt_count = 0
    last_error: str | None = None
    last_error_stage: str | None = None
    execution_payload: dict[str, object] | None = None
    policy_payload: dict[str, object] | None = None
    egress_messages: list[EgressQueueMessage] = []
    incremental_requested_count = 0
    incremental_messages: list[EgressQueueMessage] = []

    for attempt in range(1, max_attempts + 1):
        attempt_count = attempt
        next_sequence = 0
        attempt_incremental_reason_codes: list[str] = []
        attempt_incremental_messages: list[EgressQueueMessage] = []
        attempt_incremental_send_timestamps: list[float] = []
        attempt_incremental_requested_count = 0

        def reply_send(payload: dict[str, Any] | None = None, /, **kwargs: object) -> None:
            nonlocal next_sequence, attempt_incremental_requested_count
            attempt_incremental_requested_count += 1
            merged_payload: dict[str, object] = {}
            if payload is not None:
                if not isinstance(payload, dict):
                    raise ValueError("reply_send payload must be an object")
                merged_payload.update(payload)
            merged_payload.update(kwargs)

            body = merged_payload.get("body")
            if not isinstance(body, str) or not body.strip():
                raise ValueError("reply_send body is required")

            now_monotonic = time.monotonic()
            allowed, deny_reason = policy.can_send_incremental_reply(
                [*attempt_incremental_send_timestamps, now_monotonic]
            )
            if not allowed:
                attempt_incremental_reason_codes.append(deny_reason or "incremental_reply_send_denied")
                return

            channel = merged_payload.get("channel")
            if channel is None:
                channel = task_message.envelope.reply_channel.type
            if not isinstance(channel, str) or not channel.strip():
                raise ValueError("reply_send channel must be a non-empty string")

            target = merged_payload.get("target")
            if target is None:
                target = task_message.envelope.reply_channel.target
            if not isinstance(target, str) or not target.strip():
                raise ValueError("reply_send target must be a non-empty string")

            dedupe_key_value = merged_payload.get("dedupe_key")
            dedupe_key: str | None
            if dedupe_key_value is None:
                dedupe_key = None
            elif isinstance(dedupe_key_value, str) and dedupe_key_value.strip():
                dedupe_key = dedupe_key_value.strip()
            else:
                raise ValueError("reply_send dedupe_key must be a non-empty string")

            attempt_incremental_send_timestamps.append(now_monotonic)
            attempt_incremental_messages.append(
                EgressQueueMessage(
                    task_id=task_message.task_id,
                    envelope_id=task_message.envelope.id,
                    trace_id=task_message.trace_id,
                    event_index=next_sequence,
                    event_count=1,
                    message=OutboundMessage(channel=channel, target=target, body=body),
                    emitted_at=datetime.now(timezone.utc),
                    event_id=_event_id_for_sequence(
                        task_id=task_message.task_id,
                        sequence=next_sequence,
                        event_kind="incremental",
                        dedupe_key=dedupe_key,
                    ),
                    sequence=next_sequence,
                    event_kind="incremental",
                    message_type="chatting.egress.v2",
                )
            )
            next_sequence += 1

        error_stage = "executor"
        try:
            execution_result = _execute_with_optional_reply_send(
                executor_impl=executor_impl,
                task=task,
                reply_send=reply_send,
            )
            execution_payload = execution_result.to_dict()

            error_stage = "policy"
            decision = policy.evaluate(execution_result)
            policy_payload = decision.to_dict()

            dropped_action_count = len(decision.approved_actions)
            incremental_reason_codes = attempt_incremental_reason_codes
            incremental_messages = attempt_incremental_messages
            incremental_requested_count = attempt_incremental_requested_count
            reason_codes = [*decision.reason_codes, *incremental_reason_codes]
            if dropped_action_count > 0:
                reason_codes.append("approved_actions_not_forwarded_to_egress")
            reason_codes = _dedupe(reason_codes)
            result_status = _result_status(reason_codes)

            final_egress_messages = _build_egress_messages(
                task_message=task_message,
                decision=decision,
                starting_sequence=next_sequence,
            )
            egress_messages = [*incremental_messages, *final_egress_messages]
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
                "incremental_reply_send_requested_count": incremental_requested_count,
                "incremental_reply_send_published_count": len(incremental_messages),
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
    egress_message = build_internal_heartbeat_egress(
        task_message=task_message,
        worker_received_at=worker_received_at,
    )
    ping_emitted_at = task_message.emitted_at.astimezone(timezone.utc)
    run_record = RunRecord(
        run_id=f"run:{task_message.task_id}:{time.time_ns()}",
        envelope_id=task_message.envelope.id,
        source=task_message.envelope.source,
        workflow="internal_heartbeat",
        policy_profile=task_message.envelope.policy_profile,
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
            policy_profile=run_record.policy_profile,
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
                    "messages": [egress_message.message.to_dict()],
                    "actions": [],
                    "config_updates": [],
                    "requires_human_review": False,
                    "errors": [],
                },
                "policy_decision": {
                    "approved_actions": [],
                    "blocked_actions": [],
                    "approved_messages": [egress_message.message.to_dict()],
                    "config_updates": {"approved": [], "pending_review": [], "rejected": []},
                    "reason_codes": ["internal_heartbeat"],
                },
                "incremental_reply_send_requested_count": 0,
                "incremental_reply_send_published_count": 0,
                "egress_message_count": 1,
                "heartbeat": json.loads(egress_message.message.body),
            },
            created_at=run_record.created_at,
        )
    )
    return WorkerProcessResult(
        run_record=run_record,
        egress_messages=[egress_message],
        dead_lettered=False,
    )


def _build_egress_messages(
    *,
    task_message: TaskQueueMessage,
    decision: PolicyDecision,
    starting_sequence: int,
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
            event_index=starting_sequence + index,
            event_count=len(normalized_messages),
            message=message,
            emitted_at=datetime.now(timezone.utc),
            event_id=_event_id_for_sequence(
                task_id=task_message.task_id,
                sequence=starting_sequence + index,
                event_kind="final",
                dedupe_key=None,
            ),
            sequence=starting_sequence + index,
            event_kind="final",
            message_type="chatting.egress.v2",
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


def _execute_with_optional_reply_send(
    *,
    executor_impl: Executor,
    task,
    reply_send: Callable[..., None],
):
    try:
        return executor_impl.execute(task, reply_send=reply_send)
    except TypeError as error:
        if "reply_send" not in str(error):
            raise
        return executor_impl.execute(task)


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
