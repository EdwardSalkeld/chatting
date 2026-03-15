# Milestone 04: Real-Time Reply API With Deferred Final Message

## Scope
Planning record for issue #12: define an explicit runtime contract for in-run user-visible sends while preserving a distinct end-of-run final response in split mode (`app.main_worker` + `app.main_message_handler`).

## Status Snapshot (2026-03-09)
Implemented foundations:
- `chatting.egress.v2` payload (`event_id`, `sequence`, `event_kind`) with v1 compatibility parsing.
- Worker-side `reply_send(...)` callback in `process_task_message(...)` that builds `incremental` events.
- Worker outbox persistence/replay (`pending_publish`, `published_unacked`, `acked`).
- Message-handler task-ledger staging and ordered dispatch by `sequence`.
- Message-handler dedupe via `(task_id, event_id)` and telemetry rollup counters.
- Incremental policy gates (`allow_incremental_reply_send`, max count, window rate cap).

Known gap for true staggered replies:
- `CodexExecutor.execute(...)` currently ignores the `reply_send` callback, so real Codex runs still emit at run end only. The runtime contract exists at the worker boundary but is not yet wired to an executor event stream.

## Goals
- Enable explicit immediate sends via runtime API (`reply_send`).
- Keep final assistant response semantics at run completion.
- Maintain ordering and idempotency through retries/failures.
- Preserve strict egress safety checks and channel controls.

## Non-Goals
- Transport-level streaming protocols.
- UI redesign.
- Unrelated architecture refactors.

## Decisions (Issue #12 Open Questions Resolved)
1. API shape and exposure:
- Use executor side-channel API (`reply_send`) at runtime, not `ExecutionResult.actions`.
- Reason: `ExecutionResult` is terminal/batch data, while in-run sends require immediate dispatch semantics.

2. Initial workflow scope:
- Gate initial rollout to `workflow=respond_and_optionally_edit`.
- Reason: this is the highest-volume conversational flow and already has clear final-reply semantics.

3. Final message policy:
- Final message remains required for successful run completion in this workflow, even when incremental sends occur.
- Incremental sends are progress updates only; they never replace completion semantics.

## Runtime Contract Proposal
`reply_send(payload)`

Required:
- `body: str` non-empty

Optional:
- `channel: str` defaults to envelope reply channel type
- `target: str` defaults to envelope reply channel target
- `dedupe_key: str` optional stable suffix for event identity

Validation:
- `payload` must be an object
- `channel`, `target`, `dedupe_key` must be non-empty strings when provided
- policy gate must allow call at that timestamp (cap + rate window)

Error model:
- Invalid payload raises `ValueError` and fails the run attempt.
- Policy denial is non-fatal: no egress event emitted; reason code recorded in audit.
- Broker publish failures are retried through worker-level attempt loop plus outbox replay.

## Execution Model
1. Worker begins run and creates per-attempt sequence counter (`0..n`).
2. Executor invokes `reply_send`; worker immediately creates `incremental` `EgressQueueMessage`.
3. At executor completion, policy-approved terminal assistant message(s) are emitted as `final` events with higher sequences.
4. Worker stores each event in outbox before publish.
5. Message-handler acks broker pickup quickly, stages events, then flushes only the current expected sequence.
6. Final message is dispatched when its sequence is reached.

Ordering/idempotency rules:
- Sequence is monotonic per `task_id`.
- User-visible dispatch is effectively exactly-once per `(task_id, event_id)` despite at-least-once broker delivery.
- Out-of-order delivery is tolerated via task-ledger staging.

## State and Recovery Design
Worker outbox (`egress_outbox`):
- `event_id` PK, `task_id`, `sequence`, `event_kind`, `payload_json`, `publish_state`, timestamps.
- Replay on startup for `pending_publish` and `published_unacked`.

Message-handler checkpoints:
- Durable dedupe table `dispatched_event_ids(task_id, event_id)`.
- Legacy `dispatched_events(run_id, event_index)` retained for compatibility/audit continuity.

Failure behavior:
- Worker crash before publish: replay from outbox.
- Worker crash after publish before message-handler ack: replay causes duplicate broker deliveries, deduped at handler.
- Handler crash after channel dispatch before dedupe mark is still a duplicate risk window; follow-up work should tighten atomicity or include downstream idempotency keys.

## Policy, Security, and Abuse Controls
- `allow_incremental_reply_send` default false.
- `max_incremental_reply_sends` hard cap per task.
- `incremental_reply_window_seconds` + `max_incremental_reply_sends_per_window` rate control.
- Existing message-handler egress channel allowlist remains authoritative.
- Audit detail already captures requested vs published incremental counts and reason codes.

## Observability Requirements
Current:
- Egress telemetry counters include received/dispatched/deduped/dropped plus incremental/final counts and latency aggregates.
- Run audit includes trace/task ids and incremental request/publish counts.

Follow-up additions:
- Explicit audit event taxonomy for each incremental lifecycle transition.
- Metric for "final dispatch delay after last incremental".
- Metric for outbox replay volume and replay age percentile.

## Test Plan
Unit:
- `reply_send` payload/validation/defaults.
- `EgressQueueMessage` v2 shape validation.
- sequence/event-id construction and compatibility behavior.

Integration:
- staged out-of-order egress dispatches in-order only.
- duplicate broker deliveries do not duplicate outbound user messages.
- outbox replay republishes and converges.

Failure injection:
- worker crash after outbox write before publish.
- handler restart during staged sequence backlog.
- policy-denied incremental send remains non-fatal and audited.

## Risks and Rollout Notes
Risks:
- Current executor path cannot yet produce true in-run sends for Codex subprocess.
- Handler crash window around dispatch/mark may still permit rare duplicate outbound sends.
- Final-message-required semantics are policy-level intent, not yet enforced by explicit validator.

Rollout:
1. Add executor event bridge for `reply_send` and keep feature disabled by default.
2. Enable for `respond_and_optionally_edit` in smoke/staging.
3. Monitor dedupe, latency, replay, and incremental send rates.
4. Expand to additional workflows after stability thresholds hold.

## Follow-Up Implementation Chunks (PR-Sized)
- [x] `egress.v2` contract and parser compatibility.
- [x] worker outbox persistence/replay.
- [x] message-handler sequence staging + event-id dedupe.
- [x] policy gates for incremental send usage.
- [ ] Codex executor bridge for runtime `reply_send` events (true immediate sends).
- [ ] Enforce final-message-required invariant for targeted workflows.
- [ ] Add explicit incremental lifecycle audit taxonomy.
- [ ] Add crash-injection integration tests for outbox/handler recovery windows.
