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
- [x] Core module interface protocols made runtime-checkable with implementation conformance tests
- [x] Idempotency contract hardened to dedupe by `source + dedupe_key` with SQLite compatibility migration
- [x] Schema-version contract hardened to reject blank `schema_version` values across top-level model payloads
- [x] Schema-version compatibility hardened to reject unsupported versions beyond `1.0` across top-level model payloads and executor parsing
- [x] Bootstrap worker loop retries transient execution failures and records exhausted attempts as `dead_letter`
- [x] Executor parser requires non-empty `write_file.path` and `write_file.content` in action payloads
- [x] Executor parser rejects `path`/`content` fields on non-`write_file` action payloads
- [x] Executor parser rejects whitespace-only values for required message/action/config-update fields
- [x] Executor parser rejects empty/whitespace-only strings in `errors` payload entries
- [x] Top-level models reject empty/whitespace-only entries in `errors`/`reason_codes` string-list fields
- [x] Config-update contracts reject whitespace-only `path` values and require parser-level `value` presence
- [x] Model-layer required-string fields reject whitespace-only values without regressing executor parser error-code contracts
- [x] `TaskEnvelope` enforces runtime attachment/context-ref list contracts (typed attachments + non-empty refs)
- [x] Run audit events include apply-phase outcome fields (`applied_action_count`, `skipped_action_count`, `dispatched_message_count`, `apply_reason_codes`)
- [x] Run audit events include executor output summary fields (`message_count`, `action_count`, `config_update_count`, `error_count`, `action_types`, `requires_human_review`)

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
- 2026-02-27: Post-milestone P0 hardening: required explicit `schema_version` in executor JSON parsing (`parse_execution_result`) so contract versioning is enforced rather than defaulted.
- 2026-02-27: Post-milestone P0 hardening: marked connector/router/executor/policy/applier protocols as runtime-checkable and added interface conformance tests for default implementations.
- 2026-02-27: Post-milestone P0 hardening: updated `StateStore` idempotency methods and SQLite schema to key dedupe on `(source, dedupe_key)`; added regression coverage for cross-source collisions and legacy idempotency-table migration.
- 2026-02-27: Post-milestone P0 hardening: added non-empty `schema_version` validation in top-level contracts and executor parser checks so invalid version metadata is rejected early.
- 2026-02-28: Post-milestone hardening: added bounded retry behavior in `app.main.run_bootstrap` with `dead_letter` terminal status and audit details when attempts are exhausted.
- 2026-02-28: Post-milestone P0 hardening: limited accepted `schema_version` values to `1.0` in top-level models and `parse_execution_result`, with regression tests for unsupported versions.
- 2026-02-28: Post-milestone P0 hardening: exposed retry limits via `app.main --max-attempts` with positive-integer validation and CLI coverage tests.
- 2026-02-28: Post-milestone P0 hardening: tightened executor parser required-field checks so missing nested keys in `messages`, `actions`, and `config_updates` fail with explicit `*_required` errors.
- 2026-02-28: Post-milestone P0 hardening: tightened executor action parsing so `write_file` actions with missing/empty `path` or `content` are rejected before policy evaluation.
- 2026-02-28: Post-milestone P0 hardening: tightened executor action-shape parsing so non-`write_file` actions reject `path` and `content` fields.
- 2026-02-28: Post-milestone P0 hardening: tightened executor required-string checks to reject whitespace-only values for `message.body`, `action.type`, `config_update.path`, and `write_file` path/content fields.
- 2026-02-28: Post-milestone P0 hardening: tightened executor parsing so `errors` entries must be non-empty, non-whitespace strings.
- 2026-02-28: Post-milestone integration progress: added live runtime mode in `app.main` (`--run-live`) plus IMAP/schedule connector wiring and SMTP dispatch wiring so the app can operate beyond fake bootstrap inputs.
- 2026-02-28: Post-milestone integration hardening: added live smoke-run support (`--use-stub-executor`), IMAP+SMTP startup validation, and first-run operator artifacts (`docs/run-live.md`, sample schedule JSON).
- 2026-02-28: Post-milestone integration hardening: added `--config` runtime JSON support and a full example config template so live runs no longer require long argument chains.
- 2026-02-28: Post-milestone P0 hardening: enforced model-level non-empty string-list validation for `ExecutionResult.errors`, `PolicyDecision.reason_codes`, and `ApplyResult.reason_codes` to keep contract integrity consistent beyond executor parsing.
- 2026-02-28: Post-milestone P0 hardening: aligned config-update strictness across layers by rejecting whitespace-only `ConfigUpdate.path` in model constructors and adding parser regression coverage for missing `config_update.value`.
- 2026-02-28: Post-milestone P0 hardening: expanded model-level required-string validation to reject whitespace-only values in reply-channel, envelope/routed-task identifiers/content, action/message fields, and run/audit metadata; updated executor action parsing order so `write_file_*_required` parser errors remain unchanged.
- 2026-02-28: Post-milestone P0 hardening: enforced `TaskEnvelope` runtime list contracts so `attachments` contain only `AttachmentRef` values (with non-empty URI/name fields) and `context_refs` entries are non-empty strings.
- 2026-02-28: Post-milestone P0 hardening: updated bootstrap/live processing to persist apply-phase audit outcomes (`applied_action_count`, `skipped_action_count`, `dispatched_message_count`, `apply_reason_codes`) so audit records reflect what the applier actually executed or skipped.
- 2026-02-28: Post-milestone P0 hardening: updated bootstrap/live processing to persist executor output summaries (`message_count`, `action_count`, `config_update_count`, `error_count`, `action_types`, `requires_human_review`) in audit-event detail for stronger run forensics.
