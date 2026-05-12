# Connector: Webhook

Modules:
- `app.handler.connectors.auxiliary_ingress_connector`
- `app.main_auxiliary_ingress`

## Purpose

Accept JSON webhook-style POST bodies on a secret path, push them through BBMB, and normalize them
into canonical webhook envelopes.

## Integration state

Split mode supports BBMB-backed auxiliary ingress with:
- `app.main_auxiliary_ingress` listening on configured secret paths from `ingress_routes`
- `AuxiliaryIngressConnector` draining configured handler-side queue names from `auxiliary_ingress_queues`
- worker-visible task content set to the JSON body only

## Queue payload

Auxiliary ingress queue message fields:
- `event_id`
- `received_at` (timezone-aware datetime)
- `body` (any valid JSON value)

## Output mapping

- `source`: `webhook`
- `id` / `dedupe_key`: `webhook:<event_id>`
- `reply_channel`: `webhook:<queue_name>`
- `content`: exact JSON body rendered as text, without request-path or header metadata
