# Milestone 02: Telegram Integration Plan

## Goal
Add a production-shaped Telegram connector and response path that plugs into the existing envelope -> router -> executor -> policy -> applier pipeline without breaking current email/scheduler behavior.

## Success Criteria
- Telegram updates are ingested into canonical `TaskEnvelope` records with stable dedupe keys.
- Telegram-origin tasks are processed by the existing live worker loop.
- Policy-approved outbound Telegram replies are dispatched back to the correct chat/thread.
- Idempotency, retries, run records, and audit events behave the same as existing connectors.
- Telegram integration can be enabled primarily through runtime config (`--config`) with minimal CLI flags.

## Scope
- New connector module: `app/connectors/telegram_connector.py`
- Telegram message dispatcher adapter (applier-level integration)
- Runtime config extensions for Telegram settings
- Targeted tests + live smoke test instructions
- Plan and runbook updates

## Out of Scope
- Rich Telegram feature set (inline keyboards, polls, media uploads, commands framework)
- Multi-bot orchestration
- End-user identity/ACL model beyond existing policy profiles

## Delivery Phases

## Phase A: Contracts + Config
Duration: 0.5-1 day

Deliverables:
- Define Telegram config shape in runtime config docs/examples.
- Decide ingestion mode for v1: long polling (`getUpdates`) via HTTP.
- Confirm canonical mapping:
  - `source`: `"im"` (or `"webhook"` if later moved to webhook mode)
  - `actor`: Telegram user id/username
  - `reply_channel`: `{type: "telegram", target: "<chat_id>"}` with optional thread metadata embedded in content/context.
- Dedupe key design:
  - Use Telegram `update_id` as primary dedupe key.
  - Include bot identifier if multi-bot support is added later.

Acceptance criteria:
- Config keys and mapping rules documented in this plan and runtime docs.
- No schema-breaking changes required for existing top-level models.

## Phase B: Telegram Connector (Inbound)
Duration: 1-2 days

Deliverables:
- Implement `TelegramConnector` that:
  - Calls Telegram Bot API `getUpdates` with offset handling.
  - Emits normalized `TaskEnvelope` for supported update types (`message`, `edited_message` optional).
  - Skips unsupported update payloads safely.
  - Persists dedupe through existing state store semantics.
- Add connector-specific error handling:
  - Backoff on transient HTTP failures.
  - Safe handling for malformed payloads.

Acceptance criteria:
- Connector unit tests cover:
  - basic normalization
  - dedupe key stability
  - offset progression
  - unsupported updates
  - transient API failures

## Phase C: Telegram Dispatch (Outbound)
Duration: 1 day

Deliverables:
- Add outbound adapter for channel `"telegram"` in integrated applier path.
- Implement sender abstraction (similar to SMTP sender) using Telegram `sendMessage`.
- Preserve existing behavior for `email` and `log`.

Acceptance criteria:
- Policy-approved Telegram messages are dispatched to target chat id.
- Failure to dispatch produces deterministic reason codes and audit traceability.

## Phase D: Live Runtime Wiring
Duration: 0.5-1 day

Deliverables:
- Extend `app.main` live mode:
  - Build Telegram connector from config/CLI.
  - Build Telegram sender from config/CLI.
  - Validate required settings at startup.
- Add template config file for Telegram-enabled live runs.
- Update runbook docs with concise launch commands.

Acceptance criteria:
- `python3 -m app.main --run-live --config <telegram-config.json>` starts with Telegram enabled.
- Config precedence remains: CLI overrides config.

## Phase E: Tests + Verification
Duration: 1 day

Deliverables:
- Unit tests:
  - connector parsing and offset logic
  - applier dispatch for telegram channel
  - main config validation/branch wiring
- Integration-style test:
  - mocked Telegram inbound + stub executor + telegram dispatch adapter
- Smoke validation checklist in docs.

Acceptance criteria:
- Existing test suite remains green.
- New Telegram tests pass in CI/local.

## Proposed Runtime Config Additions

```json
{
  "telegram_enabled": true,
  "telegram_bot_token_env": "CHATTING_TELEGRAM_BOT_TOKEN",
  "telegram_api_base_url": "https://api.telegram.org",
  "telegram_poll_timeout_seconds": 20,
  "telegram_allowed_chat_ids": ["123456789"],
  "telegram_context_refs": ["repo:/home/edward/chatting"]
}
```

Notes:
- Keep bot token in env var, never in JSON config.
- `telegram_allowed_chat_ids` should be optional but recommended for least privilege.

## Security + Safety Requirements
- Token must be read from environment (`*_ENV` indirection pattern).
- Optional allowlist for inbound chat ids.
- Reject or safely ignore oversize/unsupported payloads.
- Preserve deny-by-default policy behavior for actions.
- Full run/audit logging parity with email/scheduler paths.

## Risks
- Telegram API rate limits and long-poll edge cases.
- Message formatting differences (Markdown/HTML escaping).
- Chat/thread targeting mistakes when replies include group/thread metadata.

## Mitigations
- Deterministic sender abstraction with strict input validation.
- Conservative plain-text replies for v1.
- Explicit tests for target-chat/thread mapping.
- Backoff and retry strategy aligned with existing worker retry controls.

## Rollout Strategy
1. Ship behind config flag (`telegram_enabled` false by default).
2. Validate in single test chat with `--use-stub-executor`.
3. Enable Codex executor after inbound/outbound reliability is validated.
4. Expand to additional chats after audit/retry behavior is confirmed.

## Exit Checklist
- [x] Telegram connector implemented and tested
- [x] Telegram outbound dispatch implemented and tested
- [x] Live config + docs updated
- [x] End-to-end smoke run documented and reproducible
- [x] Plan/backlog progress notes updated with concrete completion dates

## Progress Notes
- 2026-03-01: Implemented `app.connectors.TelegramConnector` (Bot API `getUpdates` long polling with offset progression, supported message normalization to canonical `TaskEnvelope`, optional chat allowlist filtering, unsupported-update skipping, and deterministic unit coverage).
- 2026-03-01: Implemented outbound Telegram dispatch in `app.applier.IntegratedApplier` via new `TelegramMessageSender` (`sendMessage` API), including deterministic dispatch error reason codes (`telegram_dispatch_not_configured`/`telegram_dispatch_failed`) and unit coverage.
- 2026-03-01: Wired Telegram into `app.main` live runtime/config (`telegram_enabled`, token-env indirection, API base URL, poll timeout, allowlisted inbound chat IDs, Telegram-specific context refs, and CLI override flags), with live-branch coverage in `tests.test_main`.
- 2026-03-01: Updated operator artifacts for Telegram (`configs/live-runtime.example.json` and `docs/run-live.md`) including Telegram-enabled smoke-run instructions.
