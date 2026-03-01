# 0001: Python Architecture For Private Single-User Automation

## Status
Accepted

## Context

We need a reliable local automation assistant that:
- ingests events from multiple private sources (schedule, email, IM, webhook)
- normalizes all events to one envelope contract
- runs Codex non-interactively with strict parsing and policy gates
- persists full run/audit history for local traceability
- supports safe config proposal/approval/rollback

This system is intentionally scoped for private, single-user operation on one machine.

## Decision

Use a modular, contract-driven single-process architecture:

1. `connectors` produce canonical `TaskEnvelope` entries.
2. `router` maps envelope -> `RoutedTask`.
3. `executor` produces strict `ExecutionResult` payloads.
4. `policy` gates actions/messages/config updates.
5. `applier` executes approved operations and dispatches responses.
6. `state` persists operational history and control-plane records.

## Implementation modules

### `app/connectors/`
- source adapters for cron/email/IM/webhook-style inputs
- every connector emits canonical envelopes

### `app/router/`
- deterministic routing rules and execution constraints

### `app/executor/`
- `StubExecutor` for deterministic smoke/testing
- `CodexExecutor` for subprocess-based real execution
- strict structured-output parser contract

### `app/policy/`
- deny-by-default action policy
- sensitive config updates become pending approvals

### `app/applier/`
- action execution (`write_file`) with path safety
- outbound dispatch support (log/email/telegram)

### `app/state/`
SQLite-backed persistence for:
- idempotency keys
- run records
- audit events
- dead-letter queue
- pending approvals
- config versions + rollback

### `app/queue/`
- in-memory queue abstraction used in live loop
- scoped to local process usage

## Runtime topology

- one process
- local SQLite database
- local queue abstraction
- worker loop tuned for private single-user reliability

No distributed or multi-tenant scaling is planned.

## Safety controls

- strict schema validation
- source-scoped dedupe checks
- bounded retries with dead-letter capture
- full audit payload persistence
- human gate for sensitive config changes
- versioned config updates with rollback command

## Consequences

Pros:
- clear boundaries between modules
- strong local observability and recoverability
- safe-by-default automation behavior

Tradeoffs:
- optimized for one user, one machine
- not designed for distributed workload scaling
