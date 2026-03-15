# Connector: Telegram

Module: `app.connectors.telegram_connector`

## Purpose

Long-poll Telegram Bot API `getUpdates` and normalize DM/group `message` updates and `channel_post` updates to canonical IM envelopes.

## Runtime config keys

- `telegram_enabled` (bool)
- `telegram_bot_token_env` (default `CHATTING_TELEGRAM_BOT_TOKEN`)
- `telegram_api_base_url` (default `https://api.telegram.org`)
- `telegram_poll_timeout_seconds` (default `20`)
- `telegram_allowed_chat_ids` (optional list)
- `telegram_allowed_channel_ids` (optional list, required to ingest `channel_post`)
- `telegram_context_refs` (optional list)

Matching CLI flags exist (`--telegram-enabled`, `--telegram-bot-token-env`, etc.).

## Output mapping

- `source`: `im`
- `id` / `dedupe_key`: `telegram:<update_id>`
- `reply_channel`: `telegram:<chat_id>`
- `actor`: `<user_id>:<username>` when available
- `content`: text body (thread id is prefixed when present)

## Notes

- Unsupported update types are skipped.
- Empty text messages are skipped.
- `channel_post` updates are ignored unless the channel ID is present in `telegram_allowed_channel_ids`.
- Ignored `channel_post` updates log `update_id`, `channel_id`, and an explicit reason so new channel IDs can be copied from logs.
- Offset is advanced as `highest_update_id + 1` each poll.

## Outbound egress

- Telegram egress supports:
  - plain text replies through `sendMessage`
  - image attachments through `sendPhoto`
  - PDFs and other files through `sendDocument`
- Outbound attachment messages use the normal `OutboundMessage` contract with:
  - optional `body` as the text message or attachment caption
  - optional `attachment` object containing a local `file://` URI and optional `name`
- Attachment selection is inferred from the local file type:
  - image MIME types go to `sendPhoto`
  - everything else goes to `sendDocument`
- Attachment constraints:
  - only local absolute `file://` paths are supported today
  - the referenced file must already exist on disk when dispatch runs
  - upload/API failures surface deterministic Telegram reason codes such as `telegram_attachment_missing` and `telegram_attachment_send_failed`
