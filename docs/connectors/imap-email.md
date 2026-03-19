# Connector: IMAP Email

Module: `app.connectors.imap_email_connector`

## Purpose

Poll an IMAP mailbox and normalize messages to canonical email envelopes.

## Runtime config keys

- `prompt_context` (optional global prompt instructions)
- `email_prompt_context` (optional email-specific prompt instructions)
- `imap_host`
- `imap_port` (default `993`)
- `imap_username`
- `imap_password_env` (default `CHATTING_IMAP_PASSWORD`)
- `imap_mailbox` (default `INBOX`)
- `imap_search` (default `UNSEEN`)
- `context_ref` / `context_refs`

## Output mapping

- `source`: `email`
- `id` / `dedupe_key`: `email:<imap_uid>`
- `content`: `Subject: <subject>\n\n<body>`
- `reply_channel`: sender email if available, otherwise IMAP username
- `actor`: parsed sender email when available

## Notes

- Prompt guidance reaches the worker separately from `context_refs`. The assembled order is:
  global `prompt_context`, then `email_prompt_context`, then the task content itself.
- In live mode, `--smtp-host` is required if IMAP is enabled so replies can be dispatched.
- Invalid/missing email date headers fall back to connector current UTC time.
