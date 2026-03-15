# 0002: Interface Contracts And BBMB Payload Schemas

## Status
Accepted

## Purpose

Define stable contracts across message-handler, worker, BBMB payloads, and persistence layers so
behavior remains consistent as modules evolve.

## Core Python protocol contracts

```python
from typing import Protocol, Iterable

class Connector(Protocol):
    def poll(self) -> Iterable["TaskEnvelope"]: ...

class Router(Protocol):
    def route(self, envelope: "TaskEnvelope") -> "RoutedTask": ...

class Executor(Protocol):
    def execute(self, task: "RoutedTask") -> "ExecutionResult": ...

class PolicyEngine(Protocol):
    def evaluate(self, result: "ExecutionResult") -> "PolicyDecision": ...

class StateStore(Protocol):
    def seen(self, source: str, dedupe_key: str) -> bool: ...
    def mark_seen(self, source: str, dedupe_key: str) -> None: ...
    def append_run(self, record: "RunRecord") -> None: ...
    def append_audit_event(self, event: "AuditEvent") -> None: ...
```

## Canonical top-level schema objects

- `TaskEnvelope`
- `TaskQueueMessage`
- `EgressQueueMessage`
- `RoutedTask`
- `ExecutionResult`
- `PolicyDecision`
- `RunRecord`
- `AuditEvent`

Operational extensions:
- `DeadLetterRecord`
- `PendingApprovalRecord`
- `ConfigVersionRecord`
- task-ledger and staged-egress records in `app.message_handler_runtime`

The BBMB payload contracts are:
- `chatting.task.v1` on `chatting.tasks.v1`
- `chatting.egress.v2` on `chatting.egress.v1`

All top-level payload objects include `schema_version` and enforce strict required fields.

## Observability contract

Each run emits:
- `trace_id`
- `run_id`
- `envelope_id`
- `source`
- `workflow`
- `policy_profile`
- `latency_ms`
- `result_status`

## Security contract

- no raw secrets in envelopes/prompt payloads
- action execution deny-by-default
- sensitive config updates require explicit approval workflow
- all decisions/audit outcomes persisted in SQLite
- `message-handler` is the only process allowed to dispatch to external systems

## Deployment scope

Contracts are implemented for a private single-user system with a split runtime.
The transport contract is BBMB-backed; there is no multi-tenant orchestration layer.
