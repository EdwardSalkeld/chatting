# Webhook transit probe

The handler starts an isolated IPv4 loopback listener on port `9468` by default.
Set `webhook_http_port` to `0` to disable it. The transit probe exposes
`POST /webhooks/test`, accepts any content type without credentials, and never
creates tasks. Keep the control, UI, and metrics listeners private.

The deployment path is public TLS at `hooks.alcachofa.faith` on Falcon, verified
HTTPS over Tailscale to Magpie Nginx (SNI and Host `hooks.alcachofa.faith`), then
HTTP to `127.0.0.1:9468`. Both proxies forward only the exact probe and GitHub paths.
Requests are limited to 64 KiB and the public proxy enforces a global 10 requests
per second budget with a burst of 20. The handler has read/write/header timeouts.

A 200 JSON response contains `status: received` and a server-generated
`request_id`, also returned in `X-Request-ID`. Find the same ID in the handler's
`webhook_received` journal entry. Logs contain body size and at most the first
4096 bytes of body, JSON escaped, with a truncation flag. Headers and query
strings are omitted; the body is public, untrusted input and may contain personal
data. Normal journal retention applies. This is a receipt probe, not durable
delivery or a promise of processing. The test probe remains unauthenticated.

Chatting, Webstack DNS and Lab can land independently. Until the receiver is
deployed, the proxy returns a gateway error. No handler configuration change or
receiver-specific Chatting input pin is needed in Lab. A subsequent normal
Chatting package update brings the listener into service.

After deployment, POST from outside the Tailnet:

```sh
curl --fail-with-body -i https://hooks.alcachofa.faith/webhooks/test \
  -H 'Content-Type: application/json' -d '{"probe":"external-transit"}'
```

Match the returned ID in `journalctl -u chatting-handler`. Verify GET gives 405,
`/`, `/metrics`, `/egress`, and `/api/schedules` give 404, an oversized body gives
413, and an unavailable backend returns a gateway error rather than 200. A probe
to the public hostname from a Tailnet host alone does not prove external transit.

## GitHub App receipt logging

`POST /webhooks/github` uses the same listener and bounded receipt logs, but
requires a valid `X-Hub-Signature-256` HMAC-SHA256 over the exact request body.
The key comes from the environment variable named by `github_webhook_secret_env`
(default `CHATTING_GITHUB_WEBHOOK_SECRET`); Lab supplies it to the handler using
a SOPS-rendered environment file. Missing configuration returns 503; missing or
invalid signatures return 401 without logging the payload. Signed invalid JSON
returns 400. No requests on this endpoint create tasks or call the GitHub API.

Accepted requests log `github_webhook_received`, the server request ID, up to
4096 bytes of JSON-escaped body, and bounded GitHub event and delivery headers.
Those two routing headers are not authenticated by GitHub's body signature.
Signatures and other headers are not logged. All receipts, including drops and
retries, are logged with a `filter` decision (`keep`, `drop`, `needs_context`)
and a reason.

Each accepted GitHub receipt includes `mode: log_only`; **only keeps** include a structured
`task_preview` using the existing `chatting.task.v1` BBMB ingress contract
(`TaskQueueMessage`, with a `TaskEnvelope`). This is a preview only: the public
receiver has no broker dependency, does not write the task ledger or assign work items, and
never queues or executes tasks. Deploying this change keeps logging only.

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
Before enabling real ingress, implement reply routing, payload completeness,
transactional delivery/outbox handling, work item assignment, and burst/review
coalescing with the existing polling connector. No jobs are currently emitted.

## Tag-to-watch filtering

`github_webhook_login` defaults to `BillyAcachofa`. An external actor must
directly `@mention` that login in a new/edited issue or PR title/body, issue
comment, inline review comment, or review to start watching. Author, assignee,
reviewer status and old mentions copied into parent objects do not subscribe
an issue. Usernames match case-insensitively with boundaries, so a longer login
does not match. Mentions are plain text matching, not a Markdown parser.

The handler stores watches in `github_webhook_watches` in its existing SQLite
DB, scoped by login, repository and issue/PR number. Watches survive restarts
and remain after close/merge, so reopen and late feedback are observed. There
is currently no unwatch command or expiry. Only newly delivered tagging events
start watches; historical conversations are not fetched or backfilled.

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
identity (default: `billybotapp[bot]`) are dropped as `self_action`, while an existing
watch's PR head is still updated. Own comments cannot start a watch. Other
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
are single-process filtering state, not a transactional task-delivery guarantee;
a crash between state updates and logging can lose a preview. State errors return
500 rather than acknowledging a decision. Signature verification precedes all
filter/state access. Payload previews still cap at 4096 bytes, but filtering reads
the entire accepted body up to the existing 64 KiB request limit.

The initial App subscribes only to `release` with read-only Contents permission.
Install it on one selected repository to limit traffic. GitHub may also send
ping and installation lifecycle events; these are authenticated and logged in
the same way. The subscription covers release actions, not just publication.

[github-app-manifest.json](github-app-manifest.json) contains the registration
settings. `public: true` allows the BillyAcachofa-owned app to be installed on
Edward's repository; publication to Marketplace is not needed. Choose selected
repositories when installing.

GitHub does not offer standalone App registration using a personal access token.
The [manifest flow](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest)
starts with a signed-in browser POST to `https://github.com/settings/apps/new`,
then exchanges its temporary code through
`POST /app-manifests/{code}/conversions`. That flow generates its own webhook
secret, which must replace the receiver's secret before enabling delivery.
Alternatively, register manually using these settings and set the webhook
secret to the existing receiver secret. Do not paste secrets into issues or logs.

This [prefilled registration form](https://github.com/settings/apps/new?name=Billy%20Chatting%20Webhook&url=https%3A%2F%2Fgithub.com%2FEdwardSalkeld%2Fchatting&description=Logging-only%20GitHub%20release%20webhook%20for%20Chatting&public=true&contents=read&webhook_active=true&webhook_url=https%3A%2F%2Fhooks.alcachofa.faith%2Fwebhooks%2Fgithub&events%5B%5D=release&request_oauth_on_install=false)
can be used while signed into BillyAcachofa. GitHub requires entering the secret
in the form itself; it cannot be supplied through URL parameters. The encrypted
receiver secret is also stored in Billy's versioned memory secrets as
`github/chatting-webhook` for secure retrieval during registration.

After merging and deploying Chatting and Lab, check an unsigned request returns
401. Send a signed ping, match its returned request ID in the handler journal,
then use GitHub's App delivery page to confirm a real delivery received HTTP 200.
Finally trigger a release in the selected repository and verify its delivery ID
in `github_webhook_received` logs. A synthetic signed ping alone does not prove
that an App was registered or installed successfully.
