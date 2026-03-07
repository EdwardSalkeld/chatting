# Milestone 04: Real-Time Reply API With Deferred Final Message

## Scope
Define the contract and execution model for user-visible in-run replies while preserving a distinct final message at run completion for split-mode (`app.main_worker` + `app.main_message_handler`).

This is a planning/design milestone only.

## Current Baseline (2026-03-07)
- Worker emits egress only after executor + policy complete (`process_task_message`).
- Message handler dispatches egress with idempotency via `(task_id, event_index)`.
- Multiple messages are supported, but they are emitted as a batch at run end.

## Goals
- Support explicit immediate in-run sends (`reply_send`) from the worker runtime.
- Keep deferred final-message behavior at run completion.
- Ensure ordering and idempotency across retries/crashes.
- Preserve strict egress validation and channel allowlists in message handler.

## Non-Goals
- Transport-specific streaming protocols (SSE/WebSocket).
- UI redesign.
- Refactors unrelated to worker/message-handler dispatch lifecycle.

## Decisions
1. API exposure
- Use a worker-hosted side-channel API (`reply_send`) that the executor can invoke during task execution.
- Do not model `reply_send` as a normal `ActionProposal` in `ExecutionResult` because that shape is batch-oriented and finalized only after execution completes.

2. Workflow rollout
- Initial rollout is limited to `workflow=respond_and_optionally_edit`.
- Other workflows remain deferred until behavior and policy controls are validated.

3. Final-message semantics
- Final message remains required for successful runs.
- In-run `reply_send` messages are additive progress updates, not final-run completion.
- Duplicate text across incremental and final messages is allowed but discouraged; prompt guidance should direct concise progress updates and a distinct final summary.

## Proposed Contracts

### A) In-Run API
`reply_send(payload)`

Required payload fields:
- `body: string` (non-empty)

Optional payload fields:
- `channel: string` (defaults to task reply channel type)
- `target: string` (defaults to task reply channel target)
- `dedupe_key: string` (caller-provided stable key; optional)

Validation:
- `channel` must be in message-handler `allowed_egress_channels`.
- `target` must pass existing channel-specific validation.
- `body` length cap (configurable) to prevent abuse.

### B) Egress Envelope Upgrade
Introduce `chatting.egress.v2` payload with:
- `task_id`
- `envelope_id`
- `trace_id`
- `event_id` (stable unique id for idempotency)
- `sequence` (monotonic per task, 0-based)
- `event_kind` (`incremental` | `final`)
- `message` (`OutboundMessage`)
- `emitted_at`

Compatibility:
- Message handler accepts both `v1` and `v2` during migration.
- New worker publishes `v2` only after handler support is deployed.

## Execution Model
1. Worker starts task and initializes per-task sequence allocator.
2. During execution, `reply_send` calls create `incremental` egress events immediately.
3. At run completion, worker emits one `final` egress event derived from the normal final assistant response.
4. Message handler dispatches events in sequence order when available and checkpoints delivered `event_id`s.

Ordering rules:
- `incremental` messages must preserve call order.
- `final` event sequence must be greater than all incremental events for that task.

## Idempotency And Recovery

### Worker outbox
Persist an outbox row before broker publish:
- `event_id`, `task_id`, `sequence`, `event_kind`, `payload_json`, `publish_state`.

Publish states:
- `pending_publish`
- `published_unacked`
- `acked_by_handler` (optional via reconciliation path)

Worker crash recovery:
- On restart, re-publish `pending_publish` and `published_unacked` events.
- Event replay is safe because message handler dedupes by `event_id`.

### Message-handler checkpoints
- Replace/augment `(run_id, event_index)` checkpointing with `(task_id, event_id)`.
- If event already dispatched, ack and skip.
- If task unknown in ledger, log+drop (existing strict behavior remains).

### Retry semantics
- Broker delivery remains at-least-once.
- User-visible dispatch becomes effectively exactly-once per `event_id`.
- Final event retries are independent from incremental retries.

## Policy, Security, And Abuse Controls
- Add policy toggle: `allow_incremental_reply_send` (default false per profile).
- Add per-task cap: max incremental sends (for example 5).
- Add per-task rate limit window (for example N sends per 30s).
- Continue outbound channel allowlist enforcement in message handler.
- Log and audit blocked immediate-send attempts with explicit reason codes.

## Observability
Add audit events and structured logs for:
- `reply_send_requested`
- `reply_send_published`
- `reply_send_dispatched`
- `reply_send_deduped`
- `final_message_published`
- `final_message_dispatched`

Required audit identifiers:
- `trace_id`, `task_id`, `event_id`, `sequence`, `event_kind`, `channel`, `target`.

Metrics:
- incremental send count / task
- incremental dispatch latency
- dedupe hit rate
- final-message delay after last incremental send

## Test Plan
1. Unit
- `reply_send` payload validation and default channel/target behavior.
- `egress.v2` schema validation and compatibility parsing.
- sequence allocator monotonicity and final-event placement.

2. Integration
- worker emits incremental + final while task is still running.
- message handler dispatch ordering and dedupe with repeated broker delivery.
- task replay after worker crash without duplicate user-visible messages.

3. Failure
- broker publish failure between outbox write and publish.
- message-handler crash after dispatch before ack.
- disallowed channel and policy-denied immediate-send behavior.

## Rollout Plan
1. Ship message-handler support for `egress.v2` + `event_id` checkpoints behind config.
2. Ship worker outbox + `reply_send` side-channel disabled by default.
3. Enable for `respond_and_optionally_edit` only in non-prod/smoke environment.
4. Monitor dedupe/latency metrics and audit coverage.
5. Gradually enable for all eligible workflows.

## Follow-Up Implementation Tasks (PR-Sized)
- Add `egress.v2` models and parsing in `app/broker/messages.py`.
- Add event-id checkpoint storage APIs in `app/state` and migrations in `SQLiteStateStore`.
- Add worker outbox persistence/replay in `app/worker_runtime.py`.
- Add executor side-channel plumbing for `reply_send` in worker runtime/executor wrapper boundary.
- Add message-handler ordered dispatch + `event_id` dedupe in `app/main_message_handler.py`.
- Add policy profile controls for incremental send gating.
- Add audit/metrics instrumentation and regression tests.

