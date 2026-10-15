# Personal inbox triage

This is an opt-in workflow alongside the existing bot mailbox. Adding the code
does not connect a personal account or change existing email routing. Configure
one personal account per handler/broker deployment in v1.

The handler polls IMAP, persists messages, and publishes classification requests
to `chatting.personal-inbox.tasks.v1`. A dedicated Python worker asks Jev for a
category and a separate text model for a summary/action/deadline. It returns data
on `chatting.personal-inbox.results.v1`. The handler applies fixed policy and
records every external effect. Personal mail never enters `chatting.tasks.v1` or
the general-purpose Codex executor.

All messages share the logical work item `personal-inbox:<account>`, with durable
individual message records. This is a separate mail processing lane, not a
persistent Codex conversation/workspace or a lane in the existing worker UI.
Its inspection endpoint is described below.

## Policy

| Category | Handler behavior |
| --- | --- |
| `news` | Save summary for a digest; optionally mark read and move to the single configured news folder |
| `order` | Immediately notify the configured orders Telegram destination; leave mail in the inbox |
| `action` | Notify the configured action destination and create a Linear issue in the configured team/project; leave mail in the inbox |
| `review` | Surface in the action destination; leave mail in the inbox; no automatic ticket |

Action/deadline extraction promotes routine classifications to `action`.
Confidence below 0.90 becomes `review` regardless of other fields. This threshold
is a conservative starting policy, not a measured accuracy guarantee.
The Jev version is pinned to `jev-1.13.0`; its chosen probability and reported
confidence must both be at least 0.90. Test with labelled real mail before
allowing automatic filing.

The handler rejects unknown result fields: the model cannot supply a target,
folder, team, shell command, attachment path, or arbitrary mailbox operation.
No SMTP, reply, delete, COPY/EXPUNGE fallback, or generic IMAP command is exposed.
Only a UID from an ingested message can be changed. Mark-read precedes move.
The server must support `UID MOVE` and the configured folder must already exist.

Digests collect saved routine summaries after `digest_interval_seconds` from
ingestion, checked every ten seconds, in bounded two-message Telegram chunks.
They are delayed summaries rather than a calendar-time daily digest. Each
digest's membership and body are committed before sending so replay cannot
regroup previously delivered messages into another digest.

## Ingestion and recovery

Ingestion uses account/server/mailbox/UIDVALIDITY/UID identity, `UID SEARCH` and
`BODY.PEEK[]`. It includes new mail regardless of read status and fetching never
marks it read. On first startup, `backfill: false` saves a UIDNEXT baseline and
starts with subsequent arrivals. `backfill: true` ingests messages currently in
the selected inbox. Changing this flag after initialization does not reset the
cursor. Mail moved out of the selected inbox before polling is outside this
connector's view; v1 does not watch every folder or delivery events.

A message and its cursor update commit together, before publication. Failed
fetches stop cursor advancement at that message. Already-persisted tasks continue
while IMAP is down, and classification requests without a result are republished
after ten minutes. The worker caches model output before publication; an ACK
failure replays that cached response. The handler accepts only the first valid
classification per message. Unknown/invalid messages go to
`chatting.personal-inbox.invalid.v1` for inspection.

The handler database is separate from its ordinary orchestration database, uses
WAL and a five-second busy timeout, and has one runtime owner enforced by a file
lock. No transactions span network requests. Runtime errors are logged and retried
by the independent mail loop, without failing the ordinary handler runner.
Invalid enabled configuration or an unavailable database still fails startup.

Each effect is `pending`, `inflight`, `done`, `uncertain`, or `preview`. The handler
commits `inflight` before calling a provider and saves its receipt afterwards.
Interrupted calls and failures with unknown outcomes become `uncertain`; they
are **not blindly retried**. Other independent effects still proceed. Pending
steps resume, while completed steps do not repeat. A transient storage failure
after delivery also leaves the effect held for reconciliation.

Linear issues use a stable client-supplied UUID, with lookup before creation.
Retrying after a lost creation response can recover the existing issue rather
than creating a second one. Telegram has no equivalent dedupe key: inspect the
destination before explicitly resolving or retrying an uncertain send. The mail
reference in each alert identifies the original processing record.

A UIDVALIDITY/account/mailbox change pauses ingestion and blocks mailbox effects.
Do not delete/reset the database to fix this: preserve the ledger and reconcile
the old and new identities first. Automatic identity reset is intentionally not
implemented in v1.

## Worker boundary

Run `python -m app.main_personal_inbox_worker` as a dedicated user/process using
[the service example](../../configs/personal-inbox-worker.service.example).
It needs `TYPESAFE_API_KEY` and `CHATTING_PERSONAL_SUMMARY_API_KEY`, plus an explicit
`--summary-model` supporting structured outputs. These are model credentials,
not mailbox credentials. The summary request has `tools: []`, `tool_choice: none`,
`store: false`, and no previous conversation. Tool calls/refusals/incomplete
outputs are rejected. Jev or summarizer failures produce a review alert, never
automatic filing. No classification response is executed as code.

The service's environment must exclude IMAP/SMTP, Telegram, Linear, SSH, GitHub
and deployment credentials; its user must not be able to read the handler's
secrets/database or the coding worker's home/workspaces. No provider request
contains local context, history, credentials or handler destinations. Mail is
sent to TypeSafe for classification and OpenAI for summarization; this must be
part of the operator's account rollout decision. Provider calls are stateless
but local message records and cached summaries persist; v1 has no automatic
retention pruning. Protect the databases and backups as personal email data.

The ordinary worker's existing Jev environment is not automatically inherited
by the dedicated mail worker. Provision its model keys separately. The process
uses fixed model endpoints; email content cannot choose a network destination.

## Configuration and rollout

Merge the `personal_inbox` object from
[the example](../../configs/personal-inbox.handler.example.json) into the normal
handler configuration. Defaults are disabled, dry-run, no backfill, and no
mailbox changes. IMAP is TLS-only. The handler alone receives:

- `CHATTING_PERSONAL_IMAP_PASSWORD` (or the configured password environment name).
- Its existing configured Telegram bot token.
- `CHATTING_PERSONAL_LINEAR_KEY` (or the configured environment name), a Linear
  personal key with access to the selected team/project.

Dry-run needs the IMAP password but does not require live Telegram/Linear keys.
All destinations and the Linear team still need explicit configuration so
previews are meaningful. Personal IMAP does not require SMTP. The existing bot
mailbox configuration remains independent.

1. Configure the account, folder and Telegram destinations/Linear team. Provision
   the dedicated worker and model keys. Choose an explicit summary model.
2. Enable the connector with `dry_run: true`. Inspect classifications/previews
   using the loopback API. Read-only IMAP ingestion still reaches the model
   providers; dry-run suppresses external effects, not model requests.
3. Set `dry_run: false` for notifications/tickets on newly classified mail.
   Earlier preview effects remain previews; they are never implicitly replayed.
4. After inspecting real classification quality, enable `move_news` and/or
   `mark_news_read`. These apply to newly planned effects; enabling them does
   not retrospectively modify previously classified mail.

Setting `dry_run: true` pauses pending live effects. Disabling a mailbox flag
also pauses that pending operation. Disabling the entire connector stops its
ingestion, results and effect loop; ordinary email processing continues.

## Inspection and reconciliation

These routes exist only when the connector is enabled, on the existing
**loopback egress/admin server**, not the metrics listener:

- `GET /api/personal-inbox`: first 100 messages, their derived status, pending/
  uncertain effects, effect counts, last successful cycle and last cycle error.
  Pass `?after=<last message id>` to page through messages in ID order.
  `steps` reports the last success and last error of each cycle step
  (`results`, `ingest`, `publish`, `digest`, `perform`) separately. An IMAP
  outage holds back `last_success` for the whole cycle, so read `steps` to tell
  a dead workflow from one that is only failing to ingest.
- `GET /api/personal-inbox/effects`: first 100 effects, including durable receipts
  and previews. Pass `?after=<last effect id>` for subsequent pages.
- `POST /api/personal-inbox/resolve`: body with `id`, `outcome` (`done` or `retry`),
  and a required reconciliation `note`. Only uncertain/interrupted effects can
  be resolved. `done` acknowledges independently verified delivery; `retry`
  explicitly permits another attempt. Save JSON to a file and use
  `curl --data-binary @resolution.json` against the loopback URL.

A message is `processed` only when its required effects (including its digest)
are all done. Other statuses explain why it is unfinished: classification,
processing, digest, review, reconciliation, or preview. Reviewing a classifier's
uncertain decision is currently manual; v1 has no reclassification editor or
automatic correction endpoint. Its original mail stays available in the inbox.

Attachments, encrypted/calendar or unparseable mail, and truncated bodies are
held for review rather than silently filed. v1 reads only bounded MIME text;
it does not open attachments or follow links. A raw message exceeding 10 MiB
gets a durable review placeholder, closes that IMAP session, and allows later
mail to proceed on the next poll. A large message cannot permanently block
ingestion of everything behind it.

## Validation references

- [TypeSafe's typed questions](https://docs.typesafe.ai/introduction)
- [OpenAI structured outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses)
- [Linear GraphQL API](https://linear.app/developers/graphql)
- [Linear's IssueCreateInput schema](https://github.com/linear/linear/blob/master/packages/sdk/src/schema.graphql)
- [IMAP UID MOVE](https://www.rfc-editor.org/rfc/rfc6851)

Automated tests use synthetic mail and fake provider responses. They establish
the local contracts and recovery behavior, not classification accuracy on a
personal inbox or successful live account delivery.
