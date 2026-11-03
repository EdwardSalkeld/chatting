# Connector: Telegram

Package: `go/handler/internal/connectors/telegram`

## Purpose

Long-poll Telegram Bot API `getUpdates` and normalize DM/group `message` updates and `channel_post` updates to canonical IM envelopes.

## Runtime config keys

- `telegram_enabled` (bool)
- `telegram_bot_token_env` (default `CHATTING_TELEGRAM_BOT_TOKEN`)
- `telegram_api_base_url` (default `https://api.telegram.org`)
- `telegram_poll_timeout_seconds` (default `20`)
- `telegram_attachment_dir` (optional local directory for downloaded photo and document attachments; defaults to a temp-dir path if omitted)
- `telegram_allowed_chat_ids` (optional list)
- `telegram_allowed_channel_ids` (optional list, required to ingest `channel_post`)
- `telegram_context_refs` (optional list; falls back to `context_refs`)

## Output mapping

- `source`: `im`
- `id` / `dedupe_key`: `telegram:<update_id>`
- `reply_channel`: `telegram:<chat_id>`
- `reply_channel.metadata.message_id`: original Telegram `message_id` for native reactions
- `reply_channel.metadata.message_thread_id`: forum topic ID when present; the chat and topic together identify the conversation/workspace
- `reply_channel.metadata.reply_to_message_id`: quoted Telegram message id when the inbound message uses Telegram's native Reply gesture
- `reply_channel.metadata.location`: normalized Telegram location metadata when the inbound update includes a `location`
- `reply_channel.metadata.sender`: human-readable sender label — `@username`, else first/last name, else the numeric id; suffixed with ` (bot)` when the sender is a bot. Falls back to the sending chat (title/username) for channel posts.
- `actor`: `<user_id>:<username>` when available
- `content`: text body, photo caption, a synthesized location block, or a document marker containing its filename (thread id is prefixed when present)
- `attachments`: downloaded photo or document stored as a local `file://` attachment

## Notes

- Conversation history belongs to the worker alone. The handler passes each message through unchanged and keeps no history. The worker records inbound turns when it receives a task and outbound turns when `app.main_reply` delivers a message, by chat, topic, and message ID, including reply anchors and attachments. Each executor run gets up to 30 earlier turns from that history.
- The executor can search older worker turns by words, sender, and date using `app.main_history --query`, then retrieve turns around a returned message ID using `--around-message-id`. Both commands are scoped to the task's chat and topic.
- Unsupported update types are skipped.
- Photo-only messages are accepted with synthesized content `[photo attached]`.
- Document messages are accepted and preserve Telegram's original filename. Their content includes `[document attached: <filename>]`, including when the document has no caption, so subsequent conversation history identifies the file that was sent.
- Documents larger than Telegram's 20 MB bot download limit are not downloaded; the task and conversation history instead contain a clear unavailable-document marker.
- Location-only messages are accepted with synthesized content that includes latitude, longitude, and a map URL.
- When a message includes both text and a Telegram `location`, the location block is appended to the text so the worker can reason over both in one prompt.
- `channel_post` updates from allowlisted channel IDs are ingested normally.
- `channel_post` updates from other channel IDs still update `telegram_chat_registry`, then trigger an automatic Telegram reply that includes the channel ID so it can be copied into `telegram_allowed_channel_ids`.
- `message` updates from non-allowlisted `group` and `supergroup` chats still update `telegram_chat_registry`, then trigger an automatic Telegram reply that includes the chat ID so it can be copied into `telegram_allowed_chat_ids`.
- Each observed `message`, `channel_post`, and `my_chat_member` chat is upserted into the handler SQLite table `telegram_chat_registry` before allowlist filtering. Query that table to discover new DM/group/supergroup/channel IDs, titles, usernames, and the latest retrieval timestamp.
- Offset is advanced as `highest_update_id + 1` each poll.
- The message handler tracks downloaded Telegram attachments in its SQLite DB and only makes them cleanup-eligible after the task reaches terminal completion.
- Cleanup uses a hybrid policy:
  terminal tasks wait 7 days, while orphaned/dead-lettered attachments are reclaimed after 30 days.
- Cleanup runs in the message-handler loop and logs tracked, eligible, deleted, missing, and failed attachment cleanup events.
- `telegram_attachment_dir` must be readable by the worker, which runs as a different user, so the executor can open downloaded files.

## Outbound egress

When choosing a Telegram topic as the destination, include `telegram_topic_id`
in the `app.main_reply` spec alongside the group's `target`:

```json
{
  "task_id": "task:telegram:123",
  "channel": "telegram",
  "target": "-1004372890338",
  "telegram_topic_id": 3,
  "message": "Reply inside the chosen topic"
}
```

Use the chosen conversation's `metadata.message_thread_id`, not its message ID.
Keep the topic on every message part and attachment. Omit the field for General
or chats without topics. Explicit agent sends choose their destination; the
handler does not infer a topic from another conversation or force replies back
to the incoming one. Topic IDs must be positive integers. An invalid or deleted
topic returns a delivery error rather than retrying in General.

The worker maps this field to `OutboundMessage.metadata.message_thread_id`.
Text, photos, documents and formatting retries forward it to Telegram.
Successful sends return the actual `message_id` and, when present,
`message_thread_id` from Telegram; history records that destination topic rather
than guessing from the incoming task. Legacy `final` notices preserve their
event's topic. Native reactions continue to address chat plus message ID.

Existing topic conversations retain their workspace on later messages. Creating
a topic on behalf of a PR and binding it to that PR's existing workspace is a
separate capability; this change implements delivery into existing topics.

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
- Successful send responses retain Telegram's returned `message_id`; `app.main_reply` stores that id in the worker history ledger.
