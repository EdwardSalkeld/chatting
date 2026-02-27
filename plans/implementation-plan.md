# Implementation Plan (Python Prototype)

## Objective
Deliver a production-shaped prototype that can ingest scheduled and email events, execute Codex non-interactively, and safely apply/respond with policy controls.

## Phase 0: Foundation
Duration: 2-3 days

Deliverables:
- Repository structure and package layout
- Typed data models for `TaskEnvelope`, `RoutedTask`, `RunRecord` (`TaskEnvelope` and `RoutedTask` completed on 2026-02-27; `RunRecord` pending)
- Config system with environment-specific loading
- Structured logging and trace IDs

Acceptance criteria:
- App starts with a single command
- Config validation fails fast on invalid settings
- Logs include run and envelope IDs consistently

## Phase 1: Ingestion + Queue
Duration: 3-4 days

Deliverables:
- `cron_connector` for scheduled tasks
- `email_connector` with polling adapter abstraction
- In-memory queue and worker loop
- Idempotency persistence in SQLite

Acceptance criteria:
- Duplicate source events are skipped
- Connectors produce valid `TaskEnvelope` schema
- Queue backpressure behavior is observable via logs/metrics

## Phase 2: Routing + Execution
Duration: 3-5 days

Deliverables:
- Rule-based router (workflow + policy profile)
- Codex executor wrapper (non-interactive)
- Structured output parser with strict schema checks
- Timeout/retry enforcement

Acceptance criteria:
- Invalid Codex outputs fail safely with error record
- Execution timeouts do not crash worker loop
- Routed policy profiles map correctly to execution constraints

## Phase 3: Policy + Apply
Duration: 3-4 days

Deliverables:
- Policy engine allow/deny list for action types
- Config update validator with key-level controls
- Action applier and response dispatch adapters
- Human-review gate support

Acceptance criteria:
- Disallowed actions are blocked and audited
- Sensitive config changes move to pending-approval state
- Allowed responses are returned to originating channel

## Phase 4: Reliability + Ops
Duration: 2-4 days

Deliverables:
- Retry policy with dead-letter tracking
- Run history and audit trail querying
- Basic metrics (throughput, failure rate, latency)
- Runbook for incidents and rollback

Acceptance criteria:
- Failed runs are triaged via dead-letter store
- Audit record links input, prompt package, output, and applied actions
- Operators can rollback config to prior version

## Phase 5: Connector Expansion (Optional)
Duration: 3-5 days

Deliverables:
- IM connector (e.g., Slack)
- Webhook connector
- Source-specific response formatting

Acceptance criteria:
- Both connectors use same canonical envelope path
- New connector onboarding does not change router/executor interfaces

## Cross-Cutting Test Plan
- Unit tests: models, router, policy, parser
- Integration tests: connector -> queue -> executor -> applier
- Failure tests: timeout, malformed output, denied actions, duplicate events
- Smoke tests: scheduled task and email roundtrip

## Exit Criteria for Prototype
- Two connectors in production-like flow (cron + email)
- Structured and auditable Codex execution contract
- Safe settings proposal/apply mechanism
- Evidence-backed decision on Go rewrite timing
