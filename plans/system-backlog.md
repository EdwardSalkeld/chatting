# System Backlog

## P0 (Must Have)
- Define canonical `TaskEnvelope` and `RoutedTask` schemas
- Implement cron and email connectors
- Add SQLite-backed idempotency and run records
- Build Codex executor with timeout and structured output parsing
- Implement policy engine and action gating
- Add audit logging for every run

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
