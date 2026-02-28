# Implementation Plan (Python Prototype)

## Objective
Deliver a production-shaped prototype that can ingest scheduled and email events, execute Codex non-interactively, and safely apply/respond with policy controls.

## Phase 0: Foundation
Duration: 2-3 days

Deliverables:
- Repository structure and package layout
- Typed data models for `TaskEnvelope`, `RoutedTask`, `ExecutionResult`, `PolicyDecision`, `RunRecord` (all completed on 2026-02-27)
- Config system with environment-specific loading
- Structured logging and trace IDs

Acceptance criteria:
- App starts with a single command
- Config validation fails fast on invalid settings
- Logs include run and envelope IDs consistently

Progress notes:
- 2026-02-27: Hardened bootstrap observability output in `app.main` so each processed run emits `run_id`, `envelope_id`, `source`, `workflow`, `policy_profile`, `latency_ms`, and `result_status`; added test coverage in `tests.test_main`.

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

Progress notes:
- 2026-02-27: Implemented bootstrap `app.connectors` package with connector protocol and fake cron/email connectors that emit canonical `TaskEnvelope` objects (unit-tested).
- 2026-02-27: Implemented baseline `app.router.RuleBasedRouter` that maps canonical envelopes to `RoutedTask` with source-specific workflows/constraints and urgent email prioritization (unit-tested).
- 2026-02-27: Implemented runnable `app.main` bootstrap flow with SQLite idempotency checks in the processing loop; verifies duplicate events are skipped and run records are persisted for cron/email scenarios.
- 2026-02-27: Added explicit `StateStore` protocol contract and SQLite runtime conformance test to reinforce module boundary compatibility for future backend swaps.
- 2026-02-27: Hardened interface boundaries across connectors/router/executor/policy/applier by marking protocols runtime-checkable and adding implementation conformance tests.
- 2026-02-27: Hardened SQLite idempotency to scope dedupe by `(source, dedupe_key)` in the `StateStore` contract, including legacy table migration coverage to avoid cross-source collisions.

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

Progress notes:
- 2026-02-27: Added `app.executor.StubExecutor` for deterministic bootstrap execution results used by `app.main`; full Codex executor wrapper with timeout + structured output parsing remains open.
- 2026-02-27: Implemented `app.executor.CodexExecutor` with subprocess timeout handling and strict JSON schema parsing via `parse_execution_result`, including unit tests for success, timeout, invalid output, and non-zero exit paths.
- 2026-02-27: Tightened `parse_execution_result` schema checks to reject unknown nested keys in `messages`, `actions`, and `config_updates`, reducing output drift risk from executor responses.
- 2026-02-27: Hardened `parse_execution_result` to require explicit top-level `schema_version` from Codex output instead of applying an implicit default; added parser coverage for missing-schema rejection to keep contract versioning strict.
- 2026-02-27: Hardened schema-version contract validation to reject empty `schema_version` values in top-level models and executor parsing, preventing invalid version metadata from entering state/audit records.
- 2026-02-28: Added bounded retry handling in `app.main.run_bootstrap` (configurable `max_attempts`) so transient executor failures retry and exhausted attempts are recorded as `dead_letter` run outcomes with audit detail (`attempt_count`, `last_error`, `reason_codes=["retry_exhausted"]`).
- 2026-02-28: Hardened schema-version compatibility to reject unsupported versions (currently only `1.0` is accepted) in top-level models and executor parsing, preventing silent cross-version contract drift.

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

Progress notes:
- 2026-02-27: Implemented `app.policy.AllowlistPolicyEngine` with deny-by-default action gating, sensitive config pending-review classification, and unit tests.
- 2026-02-27: Implemented `app.applier.NoOpApplier` and `ApplyResult` contract baseline with unit tests to complete the milestone's applier stub requirement without side effects.

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

Progress notes:
- 2026-02-27: Added per-run audit logging baseline via `AuditEvent` model and SQLite persistence; bootstrap flow now emits one audit event for each processed run with policy decision counts and reason codes.

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
