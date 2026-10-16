# Personal inbox triage

This is an opt-in workflow alongside the existing bot mailbox. Adding the code
does not connect a personal account or change existing email routing. Configure
one personal account per handler/broker deployment in v1.

The handler reads mail over JMAP, persists messages, and publishes
classification requests to `chatting.personal-inbox.tasks.v1`. A dedicated
Python worker asks Jev for a category and a separate text model for a
summary/action/deadline. It returns data on
`chatting.personal-inbox.results.v1`. The handler applies fixed policy and
records every external effect. Personal mail never enters `chatting.tasks.v1` or
the general-purpose Codex executor.

All messages share the logical work item `personal-inbox:<account>`, with durable
individual message records. This is a separate mail processing lane, not a
persistent Codex conversation/workspace or a lane in the existing worker UI.
Its inspection endpoint is described below.

## Policy

| Category | Relayed as | Filed into |
| --- | --- | --- |
| `news` | One daily digest, not an alert per newsletter | `folders.news` |
| `order` | Immediate Telegram to the orders destination | `folders.order` |
| `action` | Immediate Telegram to the action destination, and a Linear issue if ticketing is configured | `folders.action` |
| `review` | Immediate Telegram to the action destination; no ticket | Nothing, by default |

Filing is driven entirely by the `folders` map. A category with no entry, or an
empty one, is left where it is. `review` is deliberately configured that way:
it means *the workflow could not classify this confidently*, so the inbox
becomes the queue of mail it did not understand. An empty inbox then means
nothing needs you, rather than that you have not looked today. Set
`folders.review` if you would rather file it anyway.

Action/deadline extraction promotes routine classifications to `action`.
Confidence below 0.90 becomes `review` regardless of other fields. This
threshold is a conservative starting policy, not a measured accuracy guarantee.
The Jev version is pinned to `jev-1.13.0`; its chosen probability and reported
confidence must both be at least 0.90. Test with labelled real mail before
allowing automatic filing.

The handler rejects unknown result fields: the model cannot supply a target,
folder, team, shell command, attachment path, or arbitrary mailbox operation.
No sending, deleting or arbitrary JMAP method is exposed — the only mailbox
change available is moving one ingested message into one configured folder,
optionally marking it read in the same operation.

## Links back to the message

Every alert ends with a link that opens the message in the mail client, built
from `mail_link_template`. The placeholders are `{thread}`, `{email}`,
`{folder}`, `{account}` (the account id with its leading `u` removed, which is
what Fastmail's web URLs use) and `{account_id}` (the raw id).

For Fastmail the folder segment is decorative — the web app resolves by thread
and message id — so a link keeps working after the handler files the message,
and after you move it again by hand. Set `mail_link_template` to `""` to fall
back to printing the internal message reference instead.

## Ingestion and recovery

Ingestion tracks the JMAP `Email` state string. On first startup with
`backfill: false` the handler records the current state and starts with
subsequent arrivals. With `backfill: true` it ingests what is currently in the
inbox, up to 500 messages, before recording that state. Changing the flag after
initialization does not re-run the baseline.

Each cycle asks for at most 25 changes. Created messages still in the inbox are
stored, and only then does the state advance, so an interruption replays them;
inserts keep the first record of a message, so a replay cannot overwrite a
classification that has already been acted on. Mail delivered straight to
another folder by a server-side rule is outside this connector's view, and v1
does not watch messages later moved *into* the inbox.

If the server can no longer calculate changes from the stored state it reports
`cannotCalculateChanges`. The handler re-baselines to the current state, records
`state_reset_at`, and does not replay the mailbox as new arrivals. Mail that
arrived during the gap is not ingested; inspect the inbox directly.

Already-persisted tasks continue while the mail server is down, and
classification requests without a result are republished after ten minutes. The
worker caches model output before publication; an ACK failure replays that
cached response. The handler accepts only the first valid classification per
message. Unknown/invalid messages go to `chatting.personal-inbox.invalid.v1`.

The handler database is separate from its ordinary orchestration database, uses
WAL and a five-second busy timeout, and has one runtime owner enforced by a file
lock. No transactions span network requests. Runtime errors are logged and
retried by the independent mail loop, without failing the ordinary handler.
Invalid enabled configuration, an unavailable database, or a configured folder
that does not exist or whose name is ambiguous all fail startup.

Each effect is `pending`, `inflight`, `done`, `uncertain`, or `preview`. The
handler commits `inflight` before calling a provider and saves its receipt
afterwards. Interrupted calls and failures with unknown outcomes become
`uncertain`; they are **not blindly retried**. Other independent effects still
proceed. Pending steps resume, while completed steps do not repeat.

Filing is a single atomic `Email/set`: the move and the read flag cannot
half-apply, so there is no ordering to enforce between them. The destination is
re-checked against configuration at the moment of the call, so clearing a folder
stops mail being filed into it even if the effect was planned while it was set.

Linear issues use a stable client-supplied UUID, with lookup before creation.
Retrying after a lost creation response can recover the existing issue rather
than creating a second one. Telegram has no equivalent dedupe key: inspect the
destination before explicitly resolving or retrying an uncertain send.

## Mailbox names

Folder names in `folders` are resolved to mailbox ids at startup and again
whenever a lookup fails, in case you have just created one. Names are not unique
in a real account — an account can hold two mailboxes both called `Archive` — so
an ambiguous name is refused rather than guessed at. Use a full path such as
`Triage/News` to disambiguate. A nested mailbox is also addressable by its leaf
name alone when that name is unambiguous.

## Worker boundary

Classification runs as a thread of the ordinary worker, not a separate service.
It shares that process and its existing `TYPESAFE_API_KEY`, but not its
executor: personal mail has its own queue and never enters `chatting.tasks.v1`.

The thread starts only when both of these are set, so the default is off:

- `TYPESAFE_API_KEY` — the worker already has this for model routing.
- `CHATTING_PERSONAL_CACHE_PATH` — where model output is cached before
  publication, so a failed publish or ACK replays the saved answer rather than
  paying for the call again.

`CHATTING_PERSONAL_SUMMARY_MODEL` is optional and passed to Codex as `-m`;
without it Codex uses its own configured default.

### Why there is no second API key

Jev classifies, and summaries are written by the locally installed Codex CLI
using its existing login. There is no second provider and no second credential.

Codex is an agent and its shell tool cannot be disabled — `tools.shell=false`
is accepted and then ignored. Two things stand in front of it:

1. **Jev screens every email first.** One of its questions asks whether the
   email contains text addressed to an automated system: instructions to ignore
   previous instructions, to run a command, to reveal a file or credential, or
   to change how the mail is handled. Anything but a confident "no" is withheld
   from Codex entirely and surfaced to the owner as a review alert instead.
   A failed or unavailable Jev call fails closed the same way, because Jev is
   the only thing standing between an untrusted email and an agent with a shell.
2. **Codex runs locked down.** `--ephemeral` (no session files on disk),
   `--ignore-user-config`, `--ignore-rules`, `--sandbox read-only`, an empty
   working directory, and an environment with `TYPESAFE_API_KEY` and every
   `CHATTING_*` variable removed. Output is constrained by `--output-schema`
   and re-validated by the handler.

The read-only sandbox also blocks network egress. Measured on the deployment
host as the worker user: an outbound request succeeds unsandboxed and fails to
resolve inside `codex exec --sandbox read-only`. That is a property of the
host's sandbox implementation and differs between platforms, so the screening
above does not depend on it; re-measure if the host or the Codex version
changes, by running a read-only `codex exec` that attempts an outbound request
and comparing it against the same request outside the sandbox.

`--ignore-user-config` also discards the deployment's `codex-config.toml`,
including its model setting, so set `CHATTING_PERSONAL_SUMMARY_MODEL` to the
model that file names rather than letting Codex fall back to its own default.

Routing is never Codex's to decide. Jev sets the category, the handler chooses
the folder and destinations from its own configuration, and the handler
performs every effect itself. A hostile email can at worst affect the wording
of text its own owner reads.

Mail is sent to TypeSafe for classification and to Codex for summarising; this
must be part of the operator's rollout decision. Provider calls are stateless,
but local message records and the cache persist; v1 has no automatic retention
pruning. Protect the databases and backups as personal email data.

## The daily digest

Routine mail is relayed once a day rather than one alert per newsletter. At
`digest_at`, read in `timezone`, the handler freezes that day's routine mail
into one round-up and asks for every line in a single call. The result is one
Telegram message:

```
Inbox digest — 4 since yesterday, 16 Oct

• Namecheap — .com registry prices rising in January
• Saal Digital — 50% off print orders until 31 October
• J.P. Morgan — terms and conditions updated
• Lauren McGuire Mulvihill — Simon on the Streets donation drive
```

Lines come from message bodies, not subjects, because marketing subject lines
hide the thing worth knowing: "A little thank you from us" is a discount, and
only the body says which. Each message contributes its first 2000 characters,
and a digest covers at most 60 messages.

The handler accepts the lines only when they account for exactly the membership
frozen when the digest opened — no dropped, invented or duplicated rows — and
dead-letters the result otherwise. Membership and the rendered body are
committed before sending, so a replay cannot regroup messages into a second
round-up.

A day with no routine mail records its occurrence without sending anything, so
a newsletter arriving after the send time waits for the next one rather than
firing a digest of its own. Routine mail therefore carries no per-message
summary at all: Jev decides the category and whether the recipient must act,
and only mail that needs acting on buys a summary.

## Configuration and rollout

Merge the `personal_inbox` object from
[the example](../../configs/personal-inbox.handler.example.json) into the normal
handler configuration. Defaults are disabled, dry-run, no backfill and no
folders. The handler alone receives:

- `CHATTING_PERSONAL_JMAP_TOKEN` (or the configured name). Create it under
  Settings → Privacy & Security → Connected apps & API tokens, type **JMAP**.
  A read-only token is enough until you enable filing; filing needs the Email
  scope without read-only.
- Its existing configured Telegram bot token.
- `CHATTING_PERSONAL_LINEAR_KEY` (or the configured name), **only if** you want
  action mail to raise tickets. Leave `linear_team_id` empty and ticketing is
  off: action mail is still alerted and filed, there is just no issue. A
  configured team whose key is missing from the environment logs that ticketing
  is disabled and carries on rather than failing startup, so the rest of the
  workflow can run before Linear is set up.

The worker's model credentials are listed under **Worker boundary** above and
are deliberately separate from these: the worker never receives mailbox,
Telegram or Linear credentials.

Dry-run needs the JMAP token but does not require live Telegram/Linear keys.
All destinations and the Linear team still need explicit configuration so
previews are meaningful.

1. Create the folders you intend to file into. Configure the account, folder
   map, digest time and Telegram destinations. Set the worker's cache path.
   Ticketing can be left out entirely at this stage.
2. Enable the connector with `dry_run: true`. Inspect classifications/previews
   using the loopback API. Read-only ingestion still reaches the model
   providers; dry-run suppresses external effects, not model requests.
3. Set `dry_run: false` for notifications/tickets on newly classified mail.
   Earlier preview effects remain previews; they are never implicitly replayed.
4. Add entries to `folders` once you trust the classifications. These apply to
   newly planned effects; adding a folder does not retrospectively file mail
   that was already classified.

Setting `dry_run: true` pauses pending live effects. Clearing a folder also
stops that pending filing. Disabling the entire connector stops its ingestion,
results and effect loop; ordinary email processing continues.

## Inspection and reconciliation

These routes exist only when the connector is enabled, on the existing
**loopback egress/admin server**, not the metrics listener:

- `GET /api/personal-inbox`: first 100 messages, their derived status, pending/
  uncertain effects, effect counts, last successful cycle and last cycle error.
  Pass `?after=<last message id>` to page through messages in ID order.
  `steps` reports the last success and last error of each cycle step
  (`results`, `ingest`, `publish`, `digest`, `perform`) separately. A mail
  server outage holds back `last_success` for the whole cycle, so read `steps`
  to tell a dead workflow from one that is only failing to ingest.
- `GET /api/personal-inbox/effects`: first 100 effects, including durable
  receipts and previews. Pass `?after=<last effect id>` for subsequent pages.
- `POST /api/personal-inbox/resolve`: body with `id`, `outcome` (`done` or
  `retry`), and a required reconciliation `note`. Only uncertain/interrupted
  effects can be resolved. `done` acknowledges independently verified delivery;
  `retry` explicitly permits another attempt. Save JSON to a file and use
  `curl --data-binary @resolution.json` against the loopback URL.

A message is `processed` only when its required effects are all done. Other
statuses explain why it is unfinished: classification, processing, digest,
review, reconciliation, or preview. Reviewing a classifier's uncertain decision
is currently manual; v1 has no reclassification editor.

Mail with attachments, or whose body the server could not return cleanly, is
held for review rather than silently filed. Both signals come from JMAP
directly — `hasAttachment`, and the `isTruncated`/`isEncodingProblem` flags on
the body value — rather than from inspecting raw MIME.

## Validation references

- [TypeSafe's typed questions](https://docs.typesafe.ai/introduction)
- [OpenAI structured outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses)
- [Linear GraphQL API](https://linear.app/developers/graphql)
- [JMAP Mail, RFC 8621](https://www.rfc-editor.org/rfc/rfc8621)
- [Fastmail's API documentation](https://www.fastmail.com/dev/)

Automated tests use synthetic mail and fake provider responses, plus an opt-in
read-only check against a real account (`JMAP_LIVE=1`). They establish the local
contracts and recovery behavior, not classification accuracy on a personal
inbox or successful live account delivery.
