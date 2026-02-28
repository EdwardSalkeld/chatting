# System Backlog

## P0 (Must Have)
- [x] Define canonical `TaskEnvelope`, `RoutedTask`, `ExecutionResult`, and `PolicyDecision` schemas
- [x] Implement cron and email connectors
- [x] Add SQLite-backed idempotency and run records
- [x] Build Codex executor with timeout and structured output parsing
- [x] Implement policy engine and action gating
- [x] Add audit logging for every run
- [x] Formalize `StateStore` protocol boundary and verify SQLite conformance

Progress notes:
- 2026-02-27: Added `app.models` with typed canonical `TaskEnvelope` and `RoutedTask` contracts, including `schema_version`, required-field validation, and serialization tests.
- 2026-02-27: Added typed `ExecutionResult` and `PolicyDecision` model contracts with serialization and validation tests aligned to interface contract examples.
- 2026-02-27: Added `RunRecord` model contract plus `app.state.SQLiteStateStore` for SQLite-backed idempotency keys and run-record persistence, with unit tests.
- 2026-02-27: Added `app.connectors` with `Connector` protocol and fake cron/email connectors that normalize source events to canonical `TaskEnvelope` objects, with unit tests.
- 2026-02-27: Added `app.router.RuleBasedRouter` baseline that produces contract-valid `RoutedTask` objects for cron and email envelopes, with unit tests.
- 2026-02-27: Added `app.policy.AllowlistPolicyEngine` with deny-by-default action gating, config update review buckets, and unit tests.
- 2026-02-27: Added `app.applier.NoOpApplier` plus `ApplyResult` contract for bootstrap-safe apply summaries, with unit tests.
- 2026-02-27: Added runnable `app.main` bootstrap orchestration with deterministic `StubExecutor` integration, duplicate skipping via SQLite idempotency checks, and persisted run statuses including `success` and `blocked_action`.
- 2026-02-27: Added `app.executor.CodexExecutor` subprocess wrapper with per-task timeout enforcement plus strict JSON `ExecutionResult` parsing (`parse_execution_result`) that rejects unknown top-level fields and malformed schemas; covered with unit tests.
- 2026-02-27: Hardened `ExecutionResult` parser strictness by rejecting unknown nested keys in message/action/config-update objects, tightening the P0 schema-drift mitigation.
- 2026-02-27: Added typed `AuditEvent` contract plus `SQLiteStateStore` audit event persistence (`append_audit_event`/`list_audit_events`) and wired `app.main` to write one audit event per processed run with policy decision summary fields.
- 2026-02-27: Added `app.state.StateStore` protocol contract and a runtime conformance test ensuring `SQLiteStateStore` satisfies the state interface boundary defined in architecture docs.
- 2026-02-27: Hardened run-level observability output to emit the full contract fields (`run_id`, `envelope_id`, `source`, `workflow`, `policy_profile`, `latency_ms`, `result_status`) and added bootstrap flow assertions.
- 2026-02-27: Hardened executor output contract enforcement by requiring explicit `schema_version` in parsed `ExecutionResult` payloads; removed implicit defaulting and added parser rejection coverage for missing schema versions.
- 2026-02-27: Hardened protocol contracts for connectors/router/executor/policy/applier with `@runtime_checkable` interfaces plus runtime conformance tests for shipped implementations.
- 2026-02-27: Hardened idempotency storage semantics to use source-scoped dedupe (`source + dedupe_key`) at the `StateStore` boundary, with SQLite legacy-schema migration and collision regression coverage.
- 2026-02-27: Hardened schema-version handling by rejecting blank `schema_version` values in top-level model constructors and executor output parsing.
- 2026-02-28: Added bootstrap retry enforcement in `app.main` with bounded attempts and `dead_letter` terminal status on retry exhaustion, including audit metadata for attempts and terminal error details.
- 2026-02-28: Hardened schema-version compatibility by rejecting unsupported schema versions (non-`1.0`) in model constructors and `parse_execution_result`, tightening cross-module contract enforcement.

## P1 (Should Have)
- Dead-letter queue state and replay utility
- Human approval workflow for sensitive actions
- Metrics endpoint and dashboard starter
- Config versioning + rollback command

## P2 (Nice to Have)
- Slack connector
- Webhook connector
- Queue backend abstraction (Redis/SQS)
- Multi-worker horizontal scaling

## Risks
- Output schema drift from Codex
- Connector-specific API instability
- Over-permissive policy defaults

## Mitigations
- Strict schema validation + reject unknown critical fields
- Connector adapters isolated behind interfaces
- Deny-by-default policy profile with explicit allowlists
