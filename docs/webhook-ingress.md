# Webhook transit probe

The handler starts an isolated IPv4 loopback listener on port `9468` by default.
Set `webhook_http_port` to `0` to disable it. This listener exposes only
`POST /webhooks/test`, accepts any content type without credentials, and never
creates tasks. Keep the control, UI, and metrics listeners private.

The deployment path is public TLS at `hooks.alcachofa.faith` on Falcon, verified
HTTPS over Tailscale to Magpie Nginx (SNI and Host `hooks.alcachofa.faith`), then
HTTP to `127.0.0.1:9468`. Both proxies forward only the exact probe path.
Requests are limited to 64 KiB and the public proxy enforces a global 10 requests
per second budget with a burst of 20. The handler has read/write/header timeouts.

A 200 JSON response contains `status: received` and a server-generated
`request_id`, also returned in `X-Request-ID`. Find the same ID in the handler's
`webhook_received` journal entry. Logs contain body size and at most the first
4096 bytes of body, JSON escaped, with a truncation flag. Headers and query
strings are omitted; the body is public, untrusted input and may contain personal
data. Normal journal retention applies. This is a receipt probe, not durable
delivery or a promise of processing. Sender authentication, signatures and
deduplication belong to future connectors.

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
