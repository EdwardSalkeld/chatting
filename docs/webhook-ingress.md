# Webhook transit probe

The handler starts an isolated IPv4 loopback listener on port `9468` by default.
Set `webhook_http_port` to `0` to disable it. The transit probe exposes
`POST /webhooks/test`, accepts any content type without credentials, and never
creates tasks. Keep the control, UI, and metrics listeners private.

When exposing webhooks, configure TLS termination and forward only the exact
`/webhooks/test` and `/webhooks/github` paths to this listener. Enforce request
size and rate limits at the public edge. The handler limits bodies to 64 KiB
and applies read/write/header timeouts.

A 200 JSON response contains `status: received` and a server-generated
`request_id`, also returned in `X-Request-ID`. Find the same ID in the handler's
`webhook_received` journal entry. Logs contain body size and at most the first
4096 bytes of body, JSON escaped, with a truncation flag. Headers and query
strings are omitted; the body is public, untrusted input and may contain personal
data. Normal journal retention applies. This is a receipt probe, not durable
delivery or a promise of processing. The test probe remains unauthenticated.

To verify the receiver locally:

```sh
curl --fail-with-body -i http://127.0.0.1:9468/webhooks/test \
  -H 'Content-Type: application/json' -d '{"probe":"transit"}'
```

Match the response request ID with the handler's `webhook_received` log.
For public ingress, also verify transit from outside the private network and
confirm unrelated app endpoints are inaccessible through the proxy.

## GitHub App receipt logging

`POST /webhooks/github` uses the same listener and bounded receipt logs, but
requires a valid `X-Hub-Signature-256` HMAC-SHA256 over the exact request body.
The key comes from the environment variable named by `github_webhook_secret_env`
(default `CHATTING_GITHUB_WEBHOOK_SECRET`). Supply that variable in the
handler process environment. Missing configuration returns 503; missing or
invalid signatures return 401 without logging the payload. Signed invalid JSON
returns 400. By default this endpoint only logs and records ownership; delivery
requires `github_webhook_delivery_enabled: true`. It makes no GitHub API calls.

Accepted requests log `github_webhook_received`, the server request ID, up to
4096 bytes of JSON-escaped body, and bounded GitHub event and delivery headers.
Those two routing headers are not authenticated by GitHub's body signature.
Signatures and other headers are not logged. All receipts, including drops and
retries, are logged with a `filter` decision (`keep`, `drop`, `needs_context`)
and a reason.

With delivery disabled, each accepted GitHub receipt includes `mode: log_only`; **only keeps** include a structured
`task_preview` using the existing `chatting.task.v1` BBMB ingress contract
(`TaskQueueMessage`, with a `TaskEnvelope`). This is a preview only: the public
receiver has no broker dependency. Keeps also include a compact
`notification_preview`, assigned to the owning work item when ownership resolves.
Assignment may create an item/subject association, but logging-only mode never
writes the task ledger, queues a task or launches a worker. Deploying this change
keeps logging only because the delivery switch defaults to false.

The adapter is event- and repository-independent, so additional subscriptions
and installations need no adapter changes. It records the event, action,
repository, sender, receipt time, and payload preview. The payload remains
limited to the first 4096 bytes with explicit size and truncation evidence;
extracted string fields and routing headers are limited to 256 bytes. Preview
content is untrusted external data. Large deliveries remain subject to the
existing 64 KiB request limit; increasing App subscriptions does not raise it.

Envelope, task, trace and dedupe identities derive from `X-GitHub-Delivery`;
redeliveries retain those identities but are logged again without a second preview. If that header is
missing, identity falls back to a hash of event type and body. Header-derived
identities are correlation hints, not authenticated identities. Replies default
to the existing `log` channel rather than assuming an issue comment destination.
Live notifications use an event/body hash for task and envelope identity;
delivery headers remain correlation metadata. They contain only the event,
action, repository/subject and specific comment/review/thread/run/release IDs,
plus run attempt and commit when available. They do not include a payload body or
context bundle. The worker resumes its item session and fetches the details.

## Ownership and optional delivery

Ownership reuses #314's `work_item_artifacts` registry. A registered PR's events
return to its originating item. An unowned watched
issue/PR gets one dedicated item whose channel is that GitHub subject. Later
events reuse it, including after restart. Several related subjects can share an
item through explicit registration; conflicts are rejected, not reassigned.

Workspace ownership does not choose where the agent speaks. The handler preserves
the event's own channel and adds the item's originating conversation as context.
The agent receives `event_channel` and `associated_conversations`, and chooses
whether to comment on the issue/PR, reply inside a review thread, message the
associated Telegram conversation, use several destinations, or stay silent.
Every send explicitly selects its channel and target; no preferred reply address
replaces the event's origin. The legacy database column `preferred_reply_json`
is retained for compatibility, solely as origin context.

For GitHub sends, an issue/PR URL creates an ordinary comment. A PR review
comment URL ending in `#discussion_r<comment-id>` replies inside that thread.
Both use `app.main_reply` with channel `github`. Inline destinations must identify
a review **comment**, not the overall review ID.

The worker automatically registers PRs opened through `gh pr create`. Register
other related subjects against a task already owned by the desired item:

```sh
python3 -P -m app.main_work_items register-subject task:telegram:123 https://github.com/owner/repo/issues/45
python3 -P -m app.main_work_items register-pr task:telegram:123 https://github.com/owner/repo/pull/46
python3 -P -m app.main_work_items register-repository task:telegram:123 owner/repo
```

These call `/work-items/register-subject` (`task_id`, `subject_url`),
`/work-items/register-pr` (`task_id`, `pr_url`) and
`/work-items/register-repository` (`task_id`, `repository`) on the handler's
existing loopback API. Issues and PRs share GitHub's repository/number namespace.
The historical artifact kind `github_pr` stores either kind of numbered subject;
repository destinations use `github_repository`.

Releases require both the repository filter opt-in and a registered repository
destination. Missing destinations, event IDs or ambiguous workflow subjects are
`needs_context`, never General. Their full bounded event is retained in
`github_webhook_held` with a reason. There is no automatic enrichment or held-event
replay in this version: inspect the row, supply the missing registration/context
and redeliver the event through GitHub. A successful redelivery clears the held
record. Workflows linked to several PRs are held rather than guessing an owner.

With `github_webhook_delivery_enabled: true`, filtering/watch/CI/dedupe state and
the compact pending notification commit in one SQLite transaction. A failure
rolls everything back so redelivery is safe. `github_webhook_pending` survives
restart; the ordinary ingress loop assigns the item, records the ledger and
publishes to `chatting.tasks.v1`, acknowledging the pending row after publication.
Broker retries retain task identity and worker inbox dedupe prevents a second
execution. Turning delivery off pauses pending notifications; it does not delete
them or replay events previously received in logging-only mode.

The worker uses the work-item ID as its conversation/session ID and workspace
key. See [session migration](conversation-executor.md). New dedicated GitHub
items, like all new handler-assigned work items, start on app-server; existing
items keep their chosen backend. Each item
queues events behind its active task. Webhook turns may complete silently when
the worker finds nothing to do. Semantic interpretation, review/comment grouping
and deciding whether to act belong to the worker. The handler does not infer
relationships from issue text or summarize reviews.

The deployed configuration lives in Lab's
`nixos/hosts/magpie/chatting-config.nix`; this PR does not enable delivery there.
Before enabling it, turn off the older GitHub assignment/review polling inputs
for the same repositories to avoid receiving the same feedback from two sources.

## Tag-to-watch filtering

`github_webhook_login` defaults to `BillyAcachofa`. An external actor can start
watching an issue or PR by assigning it to that login (`assigned_to_us`), or by
directly `@mention`ing it in a new/edited issue or PR title/body, issue comment,
inline review comment, or review. Assignment matches the top-level `assignee`
changed by an `assigned` event, case-insensitively. Assignments to other users,
old assignee lists copied into parent objects, issue authorship and reviewer
status do not start a watch. Mentions match case-insensitively with boundaries,
so a longer login does not match; they use plain text matching, not a Markdown
parser. Own actions remain suppressed.

PRs authored by that login or a configured `github_webhook_self_logins` identity
are watched automatically, without a tag. The opening hook records the watch
even when it is a suppressed own action. Later PR/review hooks or ordinary PR
comments can also establish the watch when they include PR author metadata.
Other people's PRs still require an external tag.

The handler stores watches in `github_webhook_watches` in its existing SQLite
DB, scoped by login, repository and issue/PR number. Watches survive restarts
and remain after close/merge, so reopen and late feedback are observed. There
is currently no unwatch command or expiry. Watches start from newly delivered
tagging or own-PR events; historical conversations are not fetched or backfilled.

Every subsequent related hook is inspected. Candidate task previews follow
these rules (an initial tag itself is always kept):

| Event | Keep on a watched subject |
| --- | --- |
| Issues | Open, reopen, close, assign/unassign; title/body edits that change more than whitespace |
| Issue comments | Created comments; body edits that change more than whitespace |
| Pull requests | Open/reopen/close, synchronize, ready/draft transitions, assignment/review request changes, meaningful title/body edits |
| PR reviews | Submitted, dismissed, meaningful body edits |
| PR review comments | Created, meaningful body edits |
| PR review threads | Resolved/unresolved |
| Releases | Published, non-draft, non-prerelease in explicitly opted-in repositories |
| Workflow runs | Completed failures/timeouts/action-required/startup failures for current watched PR commits; success clearing a recorded failure |

Other actions (including labels, milestones and comment deletion) are receipt
logs only. Draft PR events carry `draft_tracking_only`; an explicit initial
tag still carries `tagged_in`. This is a preview filter, not permission to
execute work or post a reply.

Events whose sender is Billy or an additional `github_webhook_self_logins`
identity (default: `billybotapp[bot]`) are dropped as `self_action`, while a
watch's PR head is still updated. Own mentions cannot subscribe unrelated
subjects; PR authorship can create a watch independently of the actor. Other
bots are evaluated normally. Workflow runs are the exception: relevance comes
from the watched PR and head SHA, regardless of who triggered the run.

`github_webhook_release_repositories` is an exact `owner/repo` list, empty by
default. Release opt-in is independent of personal watches. Workflow state in
`github_webhook_workflows` records outcomes per login/repository/workflow/commit;
older run numbers/attempts are ignored. Routine successes, cancellations and
progress hooks produce no previews. Missing PR links/head context or incomplete
edits produce `needs_context`, without GitHub API calls. Explicit branch/workflow
waits and canceled-run waits have no state source yet and are not inferred.

Delivery IDs and event/body hashes suppress repeated previews using the existing
SQLite idempotency table. Changing a delivery header does not defeat body-hash
deduplication. Missing-context events remain retryable. These records and watches
are committed atomically with pending notifications when delivery is enabled.
Logging-only previews still depend on journal delivery. State errors return
500 rather than acknowledging a decision. Signature verification precedes all
filter/state access. Payload previews still cap at 4096 bytes, but filtering reads
the entire accepted body up to the existing 64 KiB request limit.

## GitHub App registration

Register an App with the subscriptions and repository permissions required
by the events you intend to receive. Set its webhook URL to the installation's
public `/webhooks/github` endpoint, and supply the same signing secret to the
App and handler. Keep installation identities, selected repositories and secret
storage in the deployment configuration.

An unsigned request should return 401 when the receiver is configured. Send a
signed ping and match its request ID in the handler logs, then use GitHub's App
delivery page to verify a real delivery receives HTTP 200. A synthetic ping
alone does not prove that an App is registered or installed.
