# Connector: Zulip

Zulip support runs in the Go handler and is disabled by default. It polls the
[message history API](https://zulip.com/api/get-messages), using an API key for
a **generic bot**. No inbound webhook or public callback URL is required.
The same bot sends replies through the [message API](https://zulip.com/api/send-message).

## Configuration

Example handler configuration fragment (replace the example values):

```json
{
  "zulip_enabled": true,
  "zulip_site": "https://chat.example.com",
  "zulip_bot_email": "billy-bot@chat.example.com",
  "zulip_api_key_env": "CHATTING_ZULIP_API_KEY",
  "zulip_allowed_channel_ids": ["12", "34"],
  "zulip_allow_private_messages": false,
  "zulip_context_refs": ["repo:/srv/project"],
  "allowed_egress_channels": ["email", "telegram", "telegram_reaction", "github", "zulip", "log"]
}
```

- `zulip_site` is the server origin, including scheme. HTTPS uses the system
  certificate trust store. HTTP is supported for local development. Credentials
  in the URL, URL paths, query strings and fragments are rejected.
- `zulip_bot_email` identifies the generic bot. Supply its API key to the handler
  through the environment variable named by `zulip_api_key_env` (default
  `CHATTING_ZULIP_API_KEY`). Do not store the key in the JSON configuration.
- Subscribe the bot to the channels it should read. Zulip does not automatically
  subscribe newly created bots. `zulip_allowed_channel_ids` contains numeric
  channel IDs as strings, not channel names. Empty means no channel intake.
- `zulip_allow_private_messages` enables incoming one-to-one and group direct
  messages involving the bot. It defaults to false.
- `zulip_context_refs` falls back to `context_refs` when empty.
- Polling follows the handler's existing `poll_interval_seconds`; the Zulip
  request itself is nonblocking and bounded by a 30-second HTTP timeout.
- If `allowed_egress_channels` is explicitly configured, include `zulip` to allow
  replies. The default list includes it; sending also requires Zulip to be enabled.

Keep hosting, installed URLs, credentials and deployment values in the deployment
repository. Telegram and Zulip can be enabled together.

## Intake and recovery

On the first successful poll for a server/bot pair, establish a cursor at the
latest visible message. Existing history is not replayed as new requests.
Subsequent polls fetch up to 1000 newer messages, in ID order, so additional
backlog drains on later polls. After a restart, polling resumes from the saved
cursor. The cursor advances only after each accepted task has been published to
the broker; normal ingress deduplication handles repeated polling.

The handler ignores the configured bot's own messages, empty messages, messages
outside allowed channels, and private messages when disabled. All other messages
in allowed channels become requests; a mention is not required. Content is fetched
as raw Markdown and passed through unchanged.

The cursor includes the server and bot email. Changing either establishes a new
baseline. Enabling another channel or private messages later does not replay old
messages. Recovery is limited to history the bot can still access; deleted or
retention-limited messages cannot be recovered. Only one handler should poll a
given configuration/database at a time.

## Conversations and replies

Incoming messages use `source: im` and an ID containing a server hash and Zulip
message ID. Reply metadata includes the server, channel ID, topic, message ID and
sender label. The topic's exact name is preserved in the reply address. Workspaces
are scoped by server plus channel/topic, or the other participants in a private
chat. Topic case changes use the same workspace. Renaming a topic or moving it
to another channel currently creates a different conversation; edit/delete/move
events are not ingested. Previous turns remain available in the persistent
app-server conversation; Telegram's separate history-search CLI is not a Zulip
history interface.

Use `app.main_reply --spec-file` as for other transports, with `channel: zulip`.
The `target` is a JSON **string**, copied exactly from the event channel to reply:

```json
{
  "task_id": "task:zulip:example",
  "channel": "zulip",
  "target": "{\"site\":\"https://chat.example.com\",\"channel_id\":12,\"topic\":\"Chatting PR #325\"}",
  "message": "The checks passed."
}
```

For a direct message, use a target such as `{"site":"https://chat.example.com","user_ids":[7]}` encoded as the
`target` string; group direct messages use several user IDs. The bot can send
to a new topic by choosing a new name in an existing channel, subject to Zulip
permissions. Outbound text uses Zulip Markdown without Telegram escape rewriting.
The target's site must match the configured server, preventing a saved address
from being sent to a different installation after a configuration change.
File uploads, reactions and message editing are not implemented; attempts to send
an attachment fail explicitly.

This connector does not yet bind a topic created from a PR task to that PR's
existing workspace. Such a topic gets its own workspace when its first incoming
message arrives. GitHub subjects registered from a Zulip-origin task do return to
that topic's workspace, with the Zulip conversation supplied as context. The agent
chooses where to reply.
