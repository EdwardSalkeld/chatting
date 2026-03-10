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
- `reply_channel.metadata.message_id`: original Telegram `message_id` for native reactions
- `actor`: `<user_id>:<username>` when available
- `content`: text body (thread id is prefixed when present)

## Notes

- Unsupported update types are skipped.
- Empty text messages are skipped.
- `channel_post` updates are ignored unless the channel ID is present in `telegram_allowed_channel_ids`.
- Ignored `channel_post` updates log `update_id`, `channel_id`, and an explicit reason so new channel IDs can be copied from logs.
- Offset is advanced as `highest_update_id + 1` each poll.
