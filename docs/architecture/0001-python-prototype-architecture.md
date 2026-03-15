# 0001: Split-Mode Architecture For Private Single-User Automation

## Status
Accepted

## Context

We need a reliable local automation assistant that:
- ingests events from multiple private sources (schedule, email, IM, webhook)
- normalizes all events to one envelope contract
- runs Codex non-interactively with strict parsing and policy gates
- persists full run/audit history for local traceability
- supports safe config proposal/approval/rollback

This system is intentionally scoped for private, single-user operation with a split runtime.
The integration-facing host and execution-facing host may be the same machine or separate machines.

## Decision

Use a modular, contract-driven split architecture:

1. `message-handler` produces canonical `TaskEnvelope` entries from connectors and publishes
   `TaskQueueMessage` payloads to BBMB.
2. `worker` maps each task envelope to a routed task, executes it, evaluates policy, and emits
   `EgressQueueMessage` payloads.
3. `message-handler` validates egress against the ingress ledger and performs external dispatch.
4. SQLite persists operational history, the ingress ledger, egress replay state, and control-plane records.

## Implementation modules

### `app/connectors/`
- source adapters for schedule, email, IM, heartbeat, and GitHub assignment ingress
- every runtime connector emits canonical envelopes

### `app/broker/`
- BBMB transport adapter plus `chatting.task.v1` and `chatting.egress.v2` payload contracts
- hardcoded transport queues: `chatting.tasks.v1` and `chatting.egress.v1`

### `app/main_message_handler.py`
- owns connector polling, ingress dedupe, task ledger persistence, strict egress validation, and outbound dispatch
- keeps integration secrets and allowlisted external effects on the integration-facing side

### `app/main_worker.py`
- owns task consumption, routing, executor calls, policy evaluation, retries, dead-lettering, and egress publication
- keeps Codex execution isolated from integration credentials

### `app/router/`, `app/executor/`, `app/policy/`
- deterministic routing, strict executor output parsing, and deny-by-default action policy

### `app/state/` and `app/message_handler_runtime.py`
SQLite-backed persistence for:
- idempotency keys
- run records
- audit events
- dead-letter queue
- pending approvals
- config versions + rollback
- worker egress outbox replay state
- ingress task ledger, staged egress, and task-completion markers
- task completion is internal-only; user-visible replies are separate egress events

## Runtime topology

- `message-handler`
- `worker`
- `bbmb-server`
- one SQLite database per runtime role by default

No multi-tenant scaling is planned.

## Safety controls

- strict schema validation
- source-scoped dedupe checks
- bounded retries with dead-letter capture
- full audit payload persistence
- human gate for sensitive config changes
- versioned config updates with rollback command

## Consequences

Pros:
- clear security boundary between integration dispatch and Codex execution
- strong observability and recoverability through SQLite state and egress replay
- safe-by-default automation behavior with strict egress validation

Tradeoffs:
- optimized for one user and a small number of cooperating runtime processes
- more moving parts than the retired single-process bootstrap prototype
