# Connector: Webhook

Modules:
- `app.handler.connectors.webhook_connector`
- `app.handler.connectors.auxiliary_ingress_connector`
- `app.main_auxiliary_ingress`

## Purpose

Accept JSON webhook-style POST bodies on a secret path, push them through BBMB, and normalize them
into canonical webhook envelopes.

## Integration state

- The legacy in-process `WebhookConnector` remains available for tests/private integrations.
- Split mode now supports BBMB-backed auxiliary ingress with:
  - `app.main_auxiliary_ingress` listening on configured secret paths from `ingress_routes`
  - `AuxiliaryIngressConnector` draining every configured auxiliary queue
  - worker-visible task content set to the JSON body only

## Input model

`WebhookEvent` fields:
- `event_id`
- `actor`
- `content`
- `received_at` (timezone-aware datetime)
- `reply_target`
- `context_refs`

Auxiliary ingress queue payload fields:
- `event_id`
- `received_at` (timezone-aware datetime)
- `body` (any valid JSON value)

## Output mapping

- `source`: `webhook`
- `id` / `dedupe_key`: `webhook:<event_id>`
- `reply_channel`: `webhook:<reply_target>` for in-process events, or `webhook:<queue_name>` for
  auxiliary ingress
- `content`: exact JSON body rendered as text, without request-path or header metadata
