# Personal inbox triage

Triage for one personal mailbox, separate from the bot mailbox the IMAP
connector reads.

The handler reads mail over JMAP, persists messages, and publishes
classification requests to `chatting.personal-inbox.tasks.v1`. A worker thread
asks Jev for a category and Codex for a summary/action/deadline. It returns data on
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
| `action` | Immediate Telegram to the action destination, and a Linear issue if ticketing is configured | Nothing: stays in the inbox until you archive it |
| `review` | Immediate Telegram to the action destination, labelled `review`; no Linear issue | Nothing, by default |

Action mail always stays in the inbox, unread by this workflow, until you
archive it yourself; configuring a folder for it fails startup.

Other filing is driven by the `folders` map. A category with no entry, or an
empty one, is left where it is. `review` is deliberately configured that way:
it means *the workflow could not classify this confidently*. Both action and
review mail alert immediately, keeping unsettled classifications visible so
the rules can be improved. Each newly classified review email gets one alert,
using the same delivery and reconciliation behaviour as action alerts; this
does not add recurring reminders or replay alerts for historical review mail.
Set `folders.review` if you would rather file it anyway.

Jev's confident category is authoritative. Below `confidence_threshold`, there
is no settled decision and the message goes to `review`. Extracted actions or
deadlines, incomplete bodies, attachments and summary failures cannot change a
confident category. Jev sees the completeness flag and is instructed to choose
`review` when uncertain. Review is notified to the action destination and stays
in the inbox by default. If Jev is unavailable or its answer is invalid, there
is no decision to use: the worker returns `review`
without calling the summariser. Suspicious mail is withheld from both summary
paths, while retaining Jev's category. Summary failures produce a notice to read
the original, retaining the category and confidence.
The Jev version is pinned to `jev-1.13.0`, and the confidence it reports is
the lower of its reported confidence and the probability of the category it
chose. The handler compares it with `confidence_threshold` to distinguish a
decision from an unsettled reading.

Classification uses the first 2,000 body characters, together with the sender,
subject, date and completeness flag. The per-message summariser sees the same
excerpt that Jev screened; the daily digest already uses at most 2,000 body
characters per email. The handler can retain a longer body for its record, but
it is not sent to Jev. Deliberate excerpting does not itself force Review.
This applies to future processing; saved decisions are not reclassified.

The handler rejects unknown result fields: the model cannot supply a target,
folder, team, shell command, attachment path, or arbitrary mailbox operation.
No sending, deleting or arbitrary JMAP method is exposed — the only mailbox
change available is moving one ingested message into one configured folder,
optionally marking it read in the same operation.

Injection screening distinguishes instructions to an automated reader from
ordinary customer instructions, including sharing referral codes or redeeming
vouchers. A positive injection answer or a negative answer below 90% confidence
still withholds the summary. The notice distinguishes a detected instruction
from an inability to confidently rule one out; neither is a quoted finding.

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
another folder by a server-side rule is outside this connector's view, and it
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

### Personal rules as data

Set `CHATTING_PERSONAL_RULES_REPO` to a local clone of the owner's memory repo
to enable runtime feedback. Leave it unset for the existing generic policy.
The worker fetches `origin/main` before each uncached classification and reads
`personal-inbox/rules.json` from the fetched commit, without changing the shared
checkout or overwriting local edits. The repository, branch and path cannot be
chosen by an email. Git must be available with noninteractive read credentials.

The JSON document has exactly `schema_version: 1` and `rules`, a list of objects
with exactly `id` and `instruction`. IDs are unique, at most 80 characters,
lowercase letters/digits/hyphens/underscores, starting with a letter or digit.
Instructions are nonempty plain English. Limit: 30 rules and 8,000 bytes total.
Use narrow scopes with exceptions; an empty list removes all personal guidance.
Personal rule contents belong in that data repository, never in Chatting.

Jev's category/action/deadline questions and the per-message summariser receive
the same immutable snapshot as trusted instructions, separately from untrusted
mail. Injection screening remains unchanged. The trace persists `rules_revision`
and `supplied_rule_ids`: these identify all context supplied, not a claim that a
particular rule matched. Cached publish/ACK retries reuse the saved answer and
revision without fetching again. Previously processed mail is not replayed.
Digest generation is unchanged: it summarises already classified news and
does not extract required actions.

If configured rules cannot be fetched, are missing, or fail validation, the
task remains unacknowledged for retry. No fresh classification is published
using silently stale or absent rules. Fix the data or repository access to
resume processing. Feedback supplied in a normal chat can be saved by updating,
committing and pushing the rules file; the next fresh classification sees it
without a code change, restart or deployment.

### Why there is no second API key

Jev classifies, and summaries are written by the locally installed Codex CLI
using its existing login. There is no second provider and no second credential.

Codex is an agent and its shell tool cannot be disabled — `tools.shell=false`
is accepted and then ignored. Two things stand in front of it:

1. **Jev screens the email excerpt first.** One of its questions asks whether the
   excerpt contains text addressed to an automated system: instructions to ignore
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

Network restrictions of the read-only sandbox depend on the platform and Codex
version, so the screening above does not depend on network isolation. Verify
the sandbox's behavior on the target platform by comparing an outbound request
inside a read-only `codex exec` with the same request outside the sandbox.

`--ignore-user-config` also discards user configuration, including its model
setting. Set `CHATTING_PERSONAL_SUMMARY_MODEL` explicitly to choose the summary
model rather than letting Codex fall back to its own default.

Routing is never Codex's to decide. Jev sets the category, the handler chooses
the folder and destinations from its own configuration, and the handler
performs every effect itself. A hostile email can at worst affect the wording
of text its own owner reads.

Mail is sent to TypeSafe for classification and to Codex for summarising.
Provider calls are stateless, but local message records and the cache persist,
with no automatic retention pruning. Protect the databases and backups as personal email data.

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

## Configuration

The `personal_inbox` object in the handler config enables the workflow. Its
keys are defined in
[`personalinbox/config.go`](../../go/handler/internal/personalinbox/config.go).
Defaults are disabled, dry-run, no backfill and no folders. Folders in
the `folders` map must exist before startup.

The handler alone receives:

- `CHATTING_PERSONAL_JMAP_TOKEN` (or the configured name): a Fastmail JMAP
  token with the Email scope, not read-only, since filing writes.
- Its existing Telegram bot token.
- `CHATTING_PERSONAL_LINEAR_KEY` (or the configured name), only for ticketing.
  With `linear_team_id` empty, ticketing is off: action mail is still alerted
  and left in the inbox. A configured team whose key is missing logs that
  ticketing is disabled rather than failing startup.

The worker never receives mailbox, Telegram or Linear credentials; its own are
listed under **Worker boundary** above.

`dry_run: true` records effects as previews instead of performing them, but
still sends mail to the model providers. Setting it pauses pending live
effects, and previews are never replayed when it is cleared. Clearing a folder
stops pending filing into it, and adding one does not file mail that was
already classified. Disabling the connector stops its ingestion, results and
effect loop; the rest of the handler carries on.

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
is currently manual; there is no reclassification editor.

Mail with attachments, or whose body the server could not render, is held for
review rather than silently filed. Both come from JMAP directly:
`hasAttachment` and `isEncodingProblem`. A body longer than the fetch limit is
**not** held back — that is our limit, not a fault in the mail — but it is
recorded as `body_truncated`.

HTML is converted to text before anything sees it. Markup reaches the model as
noise otherwise, and inflates the body enough to hit the fetch limit, so the
raw part is fetched generously and the text capped afterwards rather than the
other way round.

## Diagnosing a decision

Paid bookings and reservations belong in `order` when they require no separate
outstanding task. Travel/event times, finding a seat and showing or downloading
issued tickets describe using the purchase; they do not create an action or
deadline. Explicit required check-in, a failed payment, an unpaid balance or
missing details needed to complete the booking still belong in `action`.
Both Jev's questions and the summary prompt use this distinction. The summary
adds descriptive text; only Jev decides the route.

Each classification carries a `trace`: the category and confidence Jev gave,
whether it judged an action, a deadline or an injection attempt, whether a
summary was bought, and a `reason`: `accepted`, `confidence_below_threshold`,
`injection_suspected`, or a triage failure reason. These distinguish
`triage_input_too_large`, `triage_request_rejected`, `triage_rate_limited`,
`triage_invalid_response` and `triage_unavailable`. Failed calls also record
`triage_error` and `triage_http_status` (zero without an HTTP response).
Worker warnings contain only this fixed error kind and HTTP status, never
provider response text or email content. Legacy failures without these fields
retain `triage_unavailable`. Alongside it each
message records `body_html`, `body_chars` and `body_truncated`.

Body extraction preserves useful plain text but compares the rendered HTML
alternative when the text is a browser-view placeholder, mostly tracking URLs,
degraded, or substantially shorter than the HTML content. HTML is parsed locally:
visible text and image alt text are retained, while scripts, styles, comments,
hidden preheaders and invisible padding are excluded. No linked pages, images or
other remote resources are fetched. Image-only content without descriptive alt
text cannot be recovered by this text extraction. Digest lines use concrete
facts from the subject when those are missing from the extracted body. The same
extracted body reaches classification and the digest; existing excerpt limits
and suspicious-mail withholding still apply. This affects newly ingested mail;
stored classifications and already delivered digests are not replayed.

That is enough to answer "why is this in review" from the record, without
going back to the mailbox to guess.

`GET /personal-inbox` on the handler's metrics/admin listener lists the same thing as a
page: when each message arrived, who sent it, its subject linked back to the
mailbox, the decision, the score and the rule that settled it, newest first.
Mail still waiting for a classification is shown too, and the header carries
the running mode, the confidence threshold, the counts per category and anything
rejected. That server binds loopback and is reached through the internal TLS
front end, which matters because the page shows mail subjects.

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
