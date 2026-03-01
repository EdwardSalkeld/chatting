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
- 2026-02-28: Added `app.main` CLI support for `--max-attempts` with fail-fast validation so retry/DLQ limits are operator-configurable at runtime.
- 2026-02-28: Hardened executor structured-output parsing to emit explicit missing-field errors for nested required keys (`message_body_required`, `action_type_required`, `config_update_path_required`) to tighten P0 schema-drift controls.
- 2026-02-28: Hardened executor action schema enforcement so `write_file` actions require non-empty `path` and `content`, preventing incomplete file-write proposals from entering policy/apply flow.
- 2026-02-28: Hardened executor action-shape schema so non-`write_file` actions reject `path`/`content` fields, tightening contract isolation between action types.
- 2026-02-28: Hardened executor required-string schema checks to reject whitespace-only required fields in `messages`, `actions`, and `config_updates`, including whitespace-only `write_file.path`/`write_file.content`.
- 2026-02-28: Hardened executor parser `errors` contract to reject empty/whitespace-only error strings, preventing low-signal failure metadata from entering run/audit records.
- 2026-02-28: Hardened config-update schema quality by enforcing non-whitespace `ConfigUpdate.path` validation in top-level models and covering missing `config_update.value` in executor parser tests.
- 2026-02-28: Added integration-ready connector primitives for real sources: interval-based cron scheduling (`IntervalScheduleConnector`) and IMAP polling (`ImapEmailConnector`), with unit tests validating canonical envelope normalization.
- 2026-02-28: Added integration-ready apply path via `IntegratedApplier` and `SmtpEmailSender` so approved `write_file` actions and outbound email/log messages can be executed beyond bootstrap no-op mode.
- 2026-02-28: Added live worker execution mode in `app.main` with connector polling loop (`run_live`), CLI configuration for schedule/IMAP/SMTP/Codex wiring, and tests covering live flow and CLI branch selection.
- 2026-02-28: Added reply-channel propagation in routed task context (router + models + stub executor) so email responses can target sender channels correctly during integrated live runs.
- 2026-02-28: Added live-mode startup safety (`--imap-host` now requires SMTP config), smoke-path execution (`--use-stub-executor`), and runnable artifacts (`docs/run-live.md`, `configs/live-schedule.example.json`) to accelerate deployment validation.
- 2026-02-28: Added live-mode JSON config ingestion (`--config`) plus template config (`configs/live-runtime.example.json`), reducing command-line verbosity while preserving CLI override behavior.
- 2026-02-28: Hardened top-level model string-list contracts so `errors`/`reason_codes` entries must be non-empty, non-whitespace strings across `ExecutionResult`, `PolicyDecision`, and `ApplyResult`, closing a remaining schema-quality gap outside executor parsing.
- 2026-02-28: Hardened model-level required-string contracts to reject whitespace-only values across key message/action/envelope/run/audit fields while keeping executor parser `write_file_path_required`/`write_file_content_required` error semantics stable.
- 2026-02-28: Hardened `TaskEnvelope` runtime contract validation so `attachments` must be `AttachmentRef` objects with non-empty `uri`/optional `name`, and `context_refs` entries must be non-empty strings.
- 2026-02-28: Hardened run audit fidelity by recording apply-phase outcomes from `ApplyResult` (`applied_action_count`, `skipped_action_count`, `dispatched_message_count`, `apply_reason_codes`) alongside policy decision metadata for each processed run.
- 2026-02-28: Hardened run audit fidelity by recording executor output summaries (`message_count`, `action_count`, `config_update_count`, `error_count`, `action_types`, `requires_human_review`) for each processed run, improving post-run forensics without changing policy/apply boundaries.
- 2026-02-28: Extended run observability with per-run `trace_id` emitted on retry/dead-letter/run-observed logs and stored in audit-event detail to improve cross-log run correlation.
- 2026-02-28: Hardened `app.main` runtime config validation to reject whitespace-only string settings at CLI/config boundaries (including `context_ref(s)` entries for live connectors), preserving fail-fast behavior and envelope contract integrity.
- 2026-02-28: Hardened model typed-collection contract enforcement by validating structured-list/object fields in `ExecutionResult`, `ConfigUpdateDecision`, `PolicyDecision`, and `ApplyResult`, preventing invalid payload shapes from bypassing constructor-level schema checks.
- 2026-02-28: Added environment-driven runtime config loading (`CHATTING_CONFIG_PATH`) with strict non-empty validation and explicit `--config` override precedence, advancing the Phase 0 config-system deliverable while preserving current CLI contracts.
- 2026-02-28: Hardened runtime config-file validation to reject unknown JSON keys in `app.main`, so operator typos fail fast instead of being silently ignored.
- 2026-02-28: Hardened runtime numeric config validation in `app.main` to reject boolean values for integer/float settings (including `max_attempts` and `poll_interval_seconds`), closing a JSON type-coercion gap in fail-fast config handling.
- 2026-03-01: Expanded run-audit payload depth by persisting full `execution_result`, `policy_decision`, and `apply_result` snapshots in audit-event detail (not just aggregate counts), improving post-run diagnostics while keeping module boundaries unchanged.

## P1 (Should Have)
- Dead-letter queue state and replay utility
- Human approval workflow for sensitive actions
- Metrics endpoint and dashboard starter
- Config versioning + rollback command

## P2 (Nice to Have)
- Telegram connector + outbound dispatch path
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

Planning notes:
- 2026-02-28: Added scoped delivery plan for Telegram integration in `plans/milestone-02-telegram-integration.md` to de-risk P2 connector expansion with explicit acceptance criteria and rollout stages.
