# Connector: Slack

Module: `app.connectors.slack_connector`

## Purpose

Normalize Slack-style message payloads into canonical IM envelopes.

## Integration state

- Connector module is implemented and unit-tested.
- It is not wired into the split-mode runtime CLI/config yet.
- Intended for in-process/private integrations that provide `fetch_messages`.

## Constructor inputs

- `fetch_messages`: callback returning list of message dicts
- `context_refs` (optional)
- `allowed_channel_ids` (optional)

Expected payload fields:
- `id`
- `user`
- `channel`
- `text`
- optional `ts`

## Output mapping

- `source`: `im`
- `id` / `dedupe_key`: `slack:<message_id>`
- `reply_channel`: `slack:<channel_id>`
- `actor`: Slack user id
