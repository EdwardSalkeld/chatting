# Connector: Fake Email

Module: `app.connectors.fake_email_connector`

## Purpose

In-memory email connector used in tests and fixture-style local runs.

## Input model

`EmailMessage` fields:
- `provider_message_id`
- `from_address`
- `subject`
- `body`
- `received_at` (timezone-aware datetime)
- `context_refs`

## Output mapping

- `source`: `email`
- `id` / `dedupe_key`: `email:<provider_message_id>`
- `content`: `Subject: <subject>\n\n<body>`
- `reply_channel`: `email:<from_address>`
- `actor`: `<from_address>`

## Configuration

No CLI/runtime config path. It is created directly in code/tests.
