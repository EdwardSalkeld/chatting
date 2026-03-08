# 0003: Current Implementation Overview (Single-User)

## Status
Accepted

## Scope

This document describes the current implementation as of 2026-03-01.
The deployment model is private, single-user, single-machine.

## Runtime flow

1. Connectors emit canonical `TaskEnvelope` objects.
2. `RuleBasedRouter` maps each envelope to `RoutedTask`.
3. Executor (`StubExecutor` or `CodexExecutor`) returns `ExecutionResult`.
4. `AllowlistPolicyEngine` gates actions/config updates/messages.
5. Applier executes approved actions/messages.
6. `SQLiteStateStore` persists idempotency, run history, audit, dead letters, approvals, and config versions.

## Entrypoints

- `app.main_message_handler`: ingress + egress dispatch in split mode
- `app.main_worker`: task execution in split mode
- `app.main`: read/query + admin commands only (`--list-*`, replay dead letters, approvals, rollback, metrics)

## Persistence tables (SQLite)

- `idempotency_keys`
- `run_records`
- `audit_events`
- `dead_letters`
- `pending_approvals`
- `current_config`
- `config_versions`

## Safety controls implemented

- strict schema validation and required-field enforcement
- source-scoped idempotency (`source + dedupe_key`)
- bounded retries with dead-letter terminal state
- deny-by-default action policy
- sensitive config updates sent to human approval workflow
- full run/audit event persistence with trace metadata

## Non-goals

- distributed deployment
- multi-tenant operation
- external queue systems

The implementation remains intentionally private and single-user; distributed scaling is not a project goal.
