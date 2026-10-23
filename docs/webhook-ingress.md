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
Signatures and other headers are not logged. Retries are logged again; there is
no durable delivery or deduplication promise at this stage.

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
