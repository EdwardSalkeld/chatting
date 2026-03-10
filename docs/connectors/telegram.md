# Connector: Telegram

Module: `app.connectors.telegram_connector`

## Purpose

Long-poll Telegram Bot API `getUpdates` and normalize DM/group `message` updates and `channel_post` updates to canonical IM envelopes.

## Runtime config keys

- `telegram_enabled` (bool)
- `telegram_bot_token_env` (default `CHATTING_TELEGRAM_BOT_TOKEN`)
- `telegram_api_base_url` (default `https://api.telegram.org`)
- `telegram_poll_timeout_seconds` (default `20`)
- `telegram_attachment_dir` (optional local directory for downloaded photo attachments; defaults to a temp-dir path if omitted)
- `telegram_allowed_chat_ids` (optional list)
- `telegram_allowed_channel_ids` (optional list, required to ingest `channel_post`)
- `telegram_context_refs` (optional list)

Matching CLI flags exist (`--telegram-enabled`, `--telegram-bot-token-env`, etc.).

## Output mapping

- `source`: `im`
- `id` / `dedupe_key`: `telegram:<update_id>`
- `reply_channel`: `telegram:<chat_id>`
- `actor`: `<user_id>:<username>` when available
- `content`: text body or photo caption (thread id is prefixed when present)
- `attachments`: downloaded photo stored as a local `file://` attachment when the update contains a photo

## Notes

- Unsupported update types are skipped.
- Photo-only messages are accepted with synthesized content `[photo attached]`.
- `channel_post` updates are ignored unless the channel ID is present in `telegram_allowed_channel_ids`.
- Ignored `channel_post` updates log `update_id`, `channel_id`, and an explicit reason so new channel IDs can be copied from logs.
- Offset is advanced as `highest_update_id + 1` each poll.
- In production, point `telegram_attachment_dir` at durable storage so Codex can still access files after ingress has completed.
