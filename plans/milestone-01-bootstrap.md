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
- [ ] Remaining in scope: fake connectors, router/policy/applier stubs, runnable `app.main`
