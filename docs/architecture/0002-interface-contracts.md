# 0002: Interface Contracts and Data Schemas

## Status
Proposed (prototype baseline)

## Purpose
Define stable interfaces so implementation details can evolve in Python and later be replaced in Go without changing system behavior.

## Python Interface Contracts

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

class StateStore(Protocol):
    def seen(self, dedupe_key: str) -> bool: ...
    def mark_seen(self, dedupe_key: str) -> None: ...
    def append_run(self, record: "RunRecord") -> None: ...
```

## JSON Contracts

### TaskEnvelope (normalized input)
```json
{
  "id": "evt_123",
  "source": "email",
  "received_at": "2026-02-27T16:00:00Z",
  "actor": "alice@example.com",
  "content": "Please summarize and reply",
  "attachments": [],
  "context_refs": ["repo:/home/edward/chatting"],
  "policy_profile": "default",
  "reply_channel": {"type": "email", "target": "alice@example.com"},
  "dedupe_key": "email:provider_msg_id"
}
```

### RoutedTask
```json
{
  "task_id": "task_123",
  "envelope_id": "evt_123",
  "workflow": "respond_and_optionally_edit",
  "priority": "normal",
  "execution_constraints": {
    "timeout_seconds": 180,
    "max_tokens": 12000
  },
  "policy_profile": "default"
}
```

### ExecutionResult (from Codex)
```json
{
  "messages": [
    {"channel": "email", "target": "alice@example.com", "body": "..."}
  ],
  "actions": [
    {"type": "write_file", "path": "docs/notes.md", "content": "..."}
  ],
  "config_updates": [
    {"path": "routing.default_timeout", "value": 240}
  ],
  "requires_human_review": false,
  "errors": []
}
```

### PolicyDecision
```json
{
  "approved_actions": [],
  "blocked_actions": [],
  "approved_messages": [],
  "config_updates": {
    "approved": [],
    "pending_review": [],
    "rejected": []
  },
  "reason_codes": []
}
```

## Versioning Strategy
- Add `schema_version` to every top-level object.
- Backward-compatible changes: additive optional fields.
- Breaking changes: bump major schema version and support dual-read during migration.

## Observability Contract
Each run must emit:
- `run_id`
- `envelope_id`
- `source`
- `workflow`
- `policy_profile`
- `latency_ms`
- `result_status`

## Security Contract
- Never place raw secrets in envelopes or prompt payloads.
- Config updates are patch-style and key-scoped.
- Action execution is deny-by-default unless explicitly approved by policy.
