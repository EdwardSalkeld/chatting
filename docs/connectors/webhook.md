# Connector: Webhook

Module: `app.connectors.webhook_connector`

## Purpose

Drain queued webhook events into canonical webhook envelopes.

## Integration state

- Connector module is implemented and unit-tested.
- It is not wired into the split-mode runtime CLI/config yet.
- Intended for private local integrations where events are pushed in-process.

## Input model

`WebhookEvent` fields:
- `event_id`
- `actor`
- `content`
- `received_at` (timezone-aware datetime)
- `reply_target`
- `context_refs`

## Output mapping

- `source`: `webhook`
- `id` / `dedupe_key`: `webhook:<event_id>`
- `reply_channel`: `webhook:<reply_target>`
