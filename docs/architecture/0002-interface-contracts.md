# 0002: Interface Contracts And Data Schemas

## Status
Accepted

## Purpose

Define stable contracts across connectors/router/executor/policy/applier/state so behavior remains consistent as modules evolve.

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

class Applier(Protocol):
    def apply(self, decision: "PolicyDecision") -> "ApplyResult": ...

class QueueBackend(Protocol):
    def enqueue(self, envelope: "TaskEnvelope") -> None: ...
    def dequeue(self) -> "TaskEnvelope | None": ...
    def size(self) -> int: ...

class StateStore(Protocol):
    def seen(self, source: str, dedupe_key: str) -> bool: ...
    def mark_seen(self, source: str, dedupe_key: str) -> None: ...
    def append_run(self, record: "RunRecord") -> None: ...
    def append_audit_event(self, event: "AuditEvent") -> None: ...
```

## Canonical top-level schema objects

- `TaskEnvelope`
- `RoutedTask`
- `ExecutionResult`
- `PolicyDecision`
- `ApplyResult`
- `RunRecord`
- `AuditEvent`

Operational extensions:
- `DeadLetterRecord`
- `PendingApprovalRecord`
- `ConfigVersionRecord`

All top-level objects include `schema_version` and enforce strict required fields.

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

## Deployment scope

Contracts are implemented for a private single-user system.
No distributed orchestration contract is required.
