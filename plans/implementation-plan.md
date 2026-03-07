# Implementation Plan (Python Prototype)

## Objective
Deliver a private single-user automation system that can ingest scheduled and message events, execute Codex non-interactively, and safely apply/respond with policy controls.

## Scope Guardrails
- Deployment model is one user on one machine.
- Reliability and auditability are prioritized over throughput scaling.
- No distributed scaling roadmap is planned for this project.

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
- 2026-02-28: Added per-run `trace_id` observability in `app.main` across retry/dead-letter/run-observed logs and persisted it in audit-event detail, with bootstrap regression coverage in `tests.test_main`.
- 2026-02-28: Hardened `TaskEnvelope` model runtime validation to enforce canonical list contracts for `attachments` and `context_refs` (including non-empty `AttachmentRef.uri`/`name` and non-blank context refs), with regression coverage in `tests.test_models`.
- 2026-02-28: Hardened runtime config-string validation in `app.main` to fail fast on whitespace-only CLI/config values (including live `context_ref(s)` entries), with regression coverage in `tests.test_main`.
- 2026-02-28: Added environment-based runtime config loading in `app.main` via `CHATTING_CONFIG_PATH` (used when `--config` is omitted), with explicit blank-value rejection and CLI-precedence regression coverage in `tests.test_main`.
- 2026-02-28: Hardened runtime JSON config contract in `app.main` to reject unknown keys (for both `--config` and `CHATTING_CONFIG_PATH`), preventing silent misconfiguration drift from typos.
- 2026-02-28: Hardened runtime numeric config validation in `app.main` so JSON booleans are rejected for integer/float settings (for example `max_attempts` and `poll_interval_seconds`), preventing implicit bool-to-number coercion.
- 2026-03-01: Hardened live schedule-file contract parsing in `app.main` so each job now fails fast on unknown keys, missing required keys, bool/non-positive `interval_seconds`, blank required strings, invalid `context_refs`, and non-string `start_at` values, with regression coverage in `tests.test_main`.

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
- 2026-02-28: Added production-integration connector primitives: `IntervalScheduleConnector` (interval-based scheduled events) and `ImapEmailConnector` (IMAP inbox polling to canonical envelopes), both with unit coverage.
- 2026-02-27: Implemented baseline `app.router.RuleBasedRouter` that maps canonical envelopes to `RoutedTask` with source-specific workflows/constraints and urgent email prioritization (unit-tested).
- 2026-02-27: Implemented runnable `app.main` bootstrap flow with SQLite idempotency checks in the processing loop; verifies duplicate events are skipped and run records are persisted for cron/email scenarios.
- 2026-02-27: Added explicit `StateStore` protocol contract and SQLite runtime conformance test to reinforce module boundary compatibility for future backend swaps.
- 2026-02-27: Hardened interface boundaries across connectors/router/executor/policy/applier by marking protocols runtime-checkable and adding implementation conformance tests.
- 2026-02-27: Hardened SQLite idempotency to scope dedupe by `(source, dedupe_key)` in the `StateStore` contract, including legacy table migration coverage to avoid cross-source collisions.
- 2026-03-01: Updated duplicate-event handling in `app.main` so dedupe skips are still persisted as `RunRecord` + `AuditEvent` entries (`result_status="duplicate_skipped"`), improving run-history completeness for idempotency paths without changing connector/router boundaries.
- 2026-03-01: Added explicit queue abstraction (`app.queue.QueueBackend`) and in-memory backend wiring in live mode to keep the control flow modular for private single-user operation.

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
- 2026-02-28: Added bootstrap CLI retry control (`python -m app.main --max-attempts <N>`) with positive-integer validation and tests, so retry/DLQ behavior can be tuned without code edits.
- 2026-02-28: Hardened executor nested required-field validation so missing `message.body`, `action.type`, and `config_update.path` fail with explicit `*_required` parser errors to reduce structured-output ambiguity.
- 2026-02-28: Hardened executor action contract validation so `write_file` proposals must include non-empty `path` and `content`, with parser regression tests for missing/empty values.
- 2026-02-28: Hardened executor action-shape validation so non-`write_file` actions cannot include `path` or `content`, preventing cross-action payload drift in structured outputs.
- 2026-02-28: Hardened executor required-string parsing to reject whitespace-only `messages`/`actions`/`config_updates` fields and whitespace-only `write_file.path`/`write_file.content`, with parser regression coverage.
- 2026-02-28: Hardened executor parser error-list validation to reject empty/whitespace-only entries in `errors`, keeping failure telemetry and audit records semantically useful.
- 2026-02-28: Hardened config-update contract consistency by rejecting whitespace-only `ConfigUpdate.path` values at the model layer and adding parser regression coverage for missing `config_update.value`.
- 2026-02-28: Added live runtime mode in `app.main` (`--run-live`) with long-lived connector polling, retry/DLQ handling, Codex command wiring, and CLI-configurable IMAP/schedule/SMTP integration parameters.
- 2026-02-28: Extended `RoutedTask` context to include source/actor/content/reply-channel metadata and propagated it through routing + stub execution so email-origin tasks can produce dispatchable email responses in live mode.
- 2026-02-28: Added live-mode operability guardrails and startup artifacts: `--use-stub-executor` smoke option, SMTP-required validation for IMAP mode, plus runnable docs and a scheduler JSON example.
- 2026-02-28: Added JSON runtime config-file support (`--config`) for live-mode settings (DB path, scheduler/IMAP/SMTP/Codex wiring, loop controls), with CLI override precedence and test coverage.
- 2026-02-28: Hardened top-level model contracts so `ExecutionResult.errors`, `PolicyDecision.reason_codes`, and `ApplyResult.reason_codes` reject empty/whitespace-only entries, with regression coverage in `tests.test_models`.
- 2026-02-28: Hardened model-level required-string validation (including `ReplyChannel`, `TaskEnvelope`, `RoutedTask`, `OutboundMessage`, `ActionProposal`, `RunRecord`, `AuditEvent`) to reject whitespace-only values, and preserved executor parser-specific `write_file_*_required` error codes by validating action payloads before model construction.
- 2026-02-28: Hardened model typed-collection contracts so `ExecutionResult`, `ConfigUpdateDecision`, `PolicyDecision`, and `ApplyResult` reject invalid list item/object types at construction time; added regression coverage in `tests.test_models`.

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
- 2026-02-28: Added `app.applier.IntegratedApplier` + `SmtpEmailSender` for live `write_file` application and outbound email/log dispatch, including path-safety checks and unit coverage.
- 2026-03-01: Added persisted human-approval workflow for sensitive config updates: pending-review proposals are written to SQLite and exposed via CLI query/decision commands (`--list-pending-approvals`, `--approve-pending-approval`, `--reject-pending-approval`).

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
- 2026-02-28: Extended run audit-event detail to include apply-phase outcomes from `ApplyResult` (`applied_action_count`, `skipped_action_count`, `dispatched_message_count`, `apply_reason_codes`) so operators can correlate policy approvals with actual side effects.
- 2026-02-28: Extended bootstrap/live audit-event detail with executor output summaries (`message_count`, `action_count`, `config_update_count`, `error_count`, `action_types`, `requires_human_review`) so run history captures execution-output shape alongside policy/apply outcomes.
- 2026-03-01: Deepened run-audit fidelity in `app.main` by persisting full structured `execution_result`, `policy_decision`, and `apply_result` payload snapshots alongside existing summary counters, with regression coverage in `tests.test_main`.
- 2026-03-01: Added read-only run-history querying in `app.main` (`--list-runs` with optional `--result-status` and `--limit`) so operators can inspect persisted SQLite run records without executing bootstrap/live processing.
- 2026-03-01: Added read-only audit-event querying in `app.main` (`--list-audit-events` with optional `--result-status` and `--limit`) so operators can inspect persisted audit trails without executing bootstrap/live processing.
- 2026-03-01: Added dead-letter queue persistence + replay tooling: `app.main` now stores dead-letter envelopes in SQLite and exposes read-only dead-letter query (`--list-dead-letters`) plus replay execution (`--replay-dead-letters`) modes.
- 2026-03-01: Added config versioning/rollback primitives in SQLite plus operator commands (`--list-config-versions`, `--rollback-config-version`); approved pending config updates now materialize as versioned changes.
- 2026-03-01: Added metrics reporting surfaces backed by run history (`--list-metrics`, plus `--serve-metrics` HTTP endpoints for `/metrics` JSON and `/dashboard` starter UI).

## Phase 5: Connector Expansion (Optional)
Duration: 3-5 days

Deliverables:
- IM connector (e.g., Slack)
- Webhook connector
- Source-specific response formatting

Acceptance criteria:
- Both connectors use same canonical envelope path
- New connector onboarding does not change router/executor interfaces

Progress notes:
- 2026-02-28: Authored `plans/milestone-02-telegram-integration.md` to define phased Telegram connector + dispatch implementation (contracts, config, tests, rollout) as the first concrete Phase 5 workstream.
- 2026-03-01: Completed Telegram integration baseline (inbound connector + outbound dispatch + live runtime/config wiring + smoke-run docs) via `app.connectors.TelegramConnector`, `TelegramMessageSender`, `app.main` Telegram config branch, and regression coverage in `tests.test_connectors`, `tests.test_applier`, and `tests.test_main`.
- 2026-03-01: Added Phase 5 connector expansion foundations for Slack and webhook sources (`SlackConnector`, `WebhookConnector`) with canonical `TaskEnvelope` normalization and connector protocol conformance tests.

## Phase 6: Service + Process Split Planning (Deferred)
Duration: 2-4 days (design only)

Deliverables:
- `systemd` deployment plan: unit template, runtime user/group, restart strategy, logging strategy, and operational runbook
- Runtime config migration plan for service mode: prefer managed config file; define when `EnvironmentFile` is still appropriate
- Producer/worker split architecture proposal for an SQS-like broker integration
- Queue contract for asynchronous request/response flow (at minimum `task` queue and `result` queue), including ack/retry/dead-letter expectations

Acceptance criteria:
- A local operator can install and run the service via documented `systemd` commands without ad hoc shell setup
- Service-mode configuration can be supplied without relying on interactive shell environment variables
- Broker split proposal defines process boundaries, message envelopes, idempotency keys, and failure-handling rules
- Proposal includes migration path from current in-process queue to broker-backed queues without breaking existing audit/run-history contracts

Progress notes:
- 2026-03-01: Phase added as deferred planning scope only; implementation intentionally not started.
- 2026-03-03: Added initial `systemd` deployment implementation for live mode with `deploy/systemd/chatting-live.service` (runtime user/group, restart policy, journald log routing) and managed `EnvironmentFile` template (`configs/chatting-live.env.example`), plus operator runbook steps in `docs/run-live.md`.
- 2026-03-07: Added `plans/milestone-04-realtime-reply-api.md` design plan for in-run `reply_send` with deferred final-message semantics in split mode, including event-id idempotency, outbox recovery model, rollout constraints, and implementation-slice backlog.

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
