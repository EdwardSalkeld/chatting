# Connector: IMAP Email

Package: `go/handler/internal/connectors/imap`

Polls the bot mailbox over IMAP and turns each message into an email task.
Replies go out over SMTP.

## Config keys

- `imap_host`, `imap_port` (default `993`), `imap_username`
- `imap_password_env` (default `CHATTING_IMAP_PASSWORD`)
- `imap_mailbox` (default `INBOX`)
- `imap_search` (default `UNSEEN`)
- `smtp_host`, `smtp_port` (default `465`), `smtp_username`, `smtp_from`
- `smtp_starttls` (default `false`: implicit TLS; `true` upgrades a plain connection, as on port 587)
- `smtp_password_env` (default `CHATTING_SMTP_PASSWORD`)
- `context_refs`

## Output mapping

- `source`: `email`
- `id` / `dedupe_key`: `email:<imap_uid>`
- `content`: `Subject: <subject>\n\n<body>`
- `actor`: the sender address, when it parses
- `reply_channel`: the sender address, otherwise `imap_username`, with
  `message_id`, `in_reply_to` and `references` in its metadata for threading

A missing or invalid `Date`
header falls back to the time the message was polled.
