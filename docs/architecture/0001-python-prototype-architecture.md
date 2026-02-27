# 0001: Python Prototype Architecture for Modular Chat-Powered Automation

## Status
Proposed (prototype baseline)

## Context
We need a modular system that:
- accepts multiple input types (scheduler, email, IM/webhooks)
- normalizes incoming events into a single task format
- runs Codex non-interactively with broad capabilities
- returns responses in the correct channel
- can propose and apply settings updates safely

The prototype should optimize for speed of iteration in Python, while preserving boundaries that can be rebuilt later in Go.

## Decision
Use an event-driven worker architecture with explicit contracts:

1. Connectors ingest source events and emit `TaskEnvelope` objects.
2. Router classifies intent, priority, and workflow.
3. Executor runs Codex in a controlled non-interactive step.
4. Policy engine validates proposed actions.
5. Applier performs approved actions and produces responses.
6. State store tracks idempotency, run history, config versions, and audit trail.

This will run as a long-lived worker process with queue semantics. Scheduler events are implemented as another connector so all paths stay unified.

## Modules

### `connectors/`
Responsibilities:
- Poll/subscribe to external systems
- Convert source-specific payloads to `TaskEnvelope`
- Attach dedupe key and source metadata

Initial connectors:
- `cron_connector.py`
- `email_connector.py`

Future connectors:
- `im_slack_connector.py`
- `webhook_connector.py`

### `core/envelope.py`
Canonical input schema:
- `id: str`
- `source: Literal["cron", "email", "im", "webhook"]`
- `received_at: datetime`
- `actor: str | None`
- `content: str`
- `attachments: list[AttachmentRef]`
- `context_refs: list[str]`
- `policy_profile: str`
- `reply_channel: ReplyChannel`
- `dedupe_key: str`

### `router/`
Responsibilities:
- Map envelopes to workflow types
- Set execution constraints (timeouts, budget, policy profile)
- Prioritize queueing

Output contract: `RoutedTask`

### `executor/codex_runner.py`
Responsibilities:
- Build deterministic prompt package from `RoutedTask`
- Launch Codex non-interactively
- Enforce timeout/token budget
- Parse structured output

Required structured output shape:
- `messages`: channel-bound responses
- `actions`: filesystem/command/API actions
- `config_updates`: proposed settings deltas
- `requires_human_review`: bool
- `errors`: list[str]

### `policy/`
Responsibilities:
- Validate action types
- Enforce allow/deny lists
- Validate config update schema and key-level permissions

Core principle: Codex proposes, policy decides.

### `applier/`
Responsibilities:
- Execute approved actions
- Persist applied changes and run outcomes
- Send responses via source adapters

### `state/`
Responsibilities:
- Store envelopes and run records
- Maintain idempotency table by source + dedupe_key
- Versioned settings storage with rollback points

Prototype suggestion:
- SQLite for metadata/state
- File-based versioned settings snapshots

## Runtime Topology
Single process initially:
- Input loop (connectors)
- In-memory queue
- Worker pool
- Shared policy/state interfaces

Scale path:
- External queue (Redis/SQS)
- Multiple worker replicas
- Connector process split from execution workers

## Safety and Reliability
Mandatory controls in prototype:
- Idempotency check before routing
- Retry policy with max attempts and DLQ state
- Global concurrency limits
- Per-run timeout budget
- Full audit log for prompt, output, and applied actions
- Secret injection only via environment/secret manager references

## Settings Self-Update Model
Use proposal/apply workflow:
1. Codex emits `config_updates` proposals.
2. Policy validates allowed keys and schema.
3. Sensitive changes require human approval flag.
4. Approved updates are versioned with rollback metadata.

## Why this supports a Go rewrite
Portability boundaries are explicit:
- Connectors, Router, Executor, Policy, Applier, State are interface-isolated.
- Canonical envelope and routed task schemas become language-neutral contracts.
- Queue and storage backends are abstracted, reducing rewrite risk.

## Consequences
Pros:
- Fast Python prototyping with strong architecture boundaries
- Connector expansion without workflow rewrites
- Clear governance over Codex actions

Tradeoffs:
- Higher upfront contract design cost
- More plumbing than a script-only approach
- Need disciplined schema/version management
