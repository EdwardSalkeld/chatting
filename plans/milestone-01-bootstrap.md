# Milestone 01: Bootstrap Skeleton

## Goal
Create a runnable Python skeleton that proves end-to-end control flow with fake connectors and a stub executor.

## Scope
- Package layout and entrypoint
- Models for envelope/routed/result/decision
- Connector interface + fake cron/email connectors
- Router + stub policy + no-op applier
- SQLite state store (idempotency + run log)

## Done Criteria
- Running `python -m app.main` processes at least one fake cron and one fake email event
- Duplicate event is skipped via dedupe key
- Run log records success and blocked-action scenarios
- Unit tests cover router and policy baseline behavior

## Out of Scope
- Real email provider integration
- Real Codex command invocation
- External queue backends

## Progress Checkpoints
- [x] Bootstrap package layout (`app/`, `tests/`)
- [x] Canonical models implemented for `TaskEnvelope` and `RoutedTask` with unit tests
- [x] Canonical models implemented for `ExecutionResult` and `PolicyDecision` with unit tests
- [x] SQLite state store baseline implemented (`seen`, `mark_seen`, `append_run`) with unit tests
- [x] Connector interface and fake cron/email connectors implemented with unit tests
- [x] Router baseline implemented (`RuleBasedRouter`) with unit tests for source/workflow/priority mapping
- [x] Stub policy baseline implemented (`AllowlistPolicyEngine`) with unit tests for action/config gating
- [x] No-op applier stub implemented (`NoOpApplier`) with unit tests
- [x] Runnable `app.main` implemented with duplicate-skipping and run-log persistence
- [x] State storage protocol boundary (`StateStore`) codified with SQLite conformance test

Notes:
- 2026-02-27: Added `app.connectors` package with `Connector` protocol plus `FakeCronConnector` and `FakeEmailConnector` that emit canonical `TaskEnvelope` records.
- 2026-02-27: Added `app.router.RuleBasedRouter` with deterministic routing rules for cron/email plus urgent-priority escalation.
- 2026-02-27: Added `app.policy.AllowlistPolicyEngine` implementing baseline deny-by-default policy behavior, including sensitive config updates routed to pending review.
- 2026-02-27: Added `app.applier.NoOpApplier` baseline with `ApplyResult` contract so policy-approved operations can be summarized without side effects during bootstrap.
- 2026-02-27: Added runnable `app.main` bootstrap flow wired through fake connectors, rule-based router, stub executor, allowlist policy, no-op applier, and SQLite run logging. The flow includes an intentional duplicate event skip and persists both `success` and `blocked_action` run outcomes.
- 2026-02-27: Post-milestone P0 progress: added `app.executor.CodexExecutor` and strict structured-output parsing to advance Phase 2 executor requirements while preserving Milestone 01 bootstrap defaults.
- 2026-02-27: Post-milestone P0 hardening: tightened executor structured-output parsing to reject unknown nested keys in `messages`, `actions`, and `config_updates`.
- 2026-02-27: Post-milestone P0 progress: added `AuditEvent` persistence and per-run audit logging in `app.main` to complete the remaining P0 backlog requirement.
- 2026-02-27: Post-milestone P0 hardening: added `StateStore` protocol interface and conformance coverage for `SQLiteStateStore` to keep persistence behind an explicit contract.
- 2026-02-27: Post-milestone P0 hardening: aligned bootstrap run logging with the observability contract by emitting run-level fields (`run_id`, `envelope_id`, `source`, `workflow`, `policy_profile`, `latency_ms`, `result_status`) and validating them in `tests.test_main`.
