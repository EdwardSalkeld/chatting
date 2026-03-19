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
- `telegram_attachment_cleanup_grace_seconds` (default `604800`, or 7 days)
- `telegram_attachment_max_age_seconds` (default `2592000`, or 30 days)
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
- `content`: text body or photo caption (thread id is prefixed when present)
- `attachments`: downloaded photo stored as a local `file://` attachment when the update contains a photo

## Notes

- Unsupported update types are skipped.
- Photo-only messages are accepted with synthesized content `[photo attached]`.
- `channel_post` updates are ignored unless the channel ID is present in `telegram_allowed_channel_ids`.
- Ignored `channel_post` updates log `update_id`, `channel_id`, and an explicit reason so new channel IDs can be copied from logs.
- Offset is advanced as `highest_update_id + 1` each poll.
- The message handler tracks downloaded Telegram attachments in its SQLite DB and only makes them cleanup-eligible after the task reaches terminal completion.
- Cleanup uses a hybrid policy:
  terminal tasks wait for `telegram_attachment_cleanup_grace_seconds`, while orphaned/dead-lettered attachments are reclaimed once they exceed `telegram_attachment_max_age_seconds`.
- Cleanup runs in the message-handler loop and logs tracked, eligible, deleted, missing, and failed attachment cleanup events.
- In production, point `telegram_attachment_dir` at durable storage when workers need access beyond ingress; the handler reclaims old files automatically unless you increase the retention settings.

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
  - only local absolute `file://` paths are supported
  - the referenced file must already exist on disk when dispatch runs
  - upload/API failures surface deterministic Telegram reason codes such as `telegram_attachment_missing` and `telegram_attachment_send_failed`
- Outbound attachments under `telegram_attachment_dir` are tracked in the same cleanup ledger as inbound downloads and become cleanup-eligible after task completion.
