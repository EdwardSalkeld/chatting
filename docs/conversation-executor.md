# Persistent conversation executor

The default executor remains `codex exec`. Opt an individual conversation into
the separate Codex app-server executor with `/executor app-server`. `/executor`
reports its setting and `/executor exec` switches it back. Commands in Telegram
forum topics affect that topic, not every topic in the group. The selection is
stored against the work-item ID. One item has one channel, workspace and current
app-server session. The inbox's `conversation_id` is that same item ID for
handler-assigned tasks; transport addresses do not select another session.
Old broker messages without a handler assignment retain their legacy routes.
New GitHub-only items use app-server automatically. Existing items retain their
executor choice, including an explicit choice of `exec`.

Operators can also select it using an existing task from the conversation:

```bash
python3 -P -m app.main_work_items --db /var/lib/worker/chatting-worker.db executor task:telegram:123 app-server
```

It runs `codex app-server` with stdio transport, separately from `codex_command`. App-server inherits the installed Codex
configuration and authentication; it does not inherit flags from `codex_command`.
The executor always sets `sandbox: danger-full-access` and `approvalPolicy: never`
when creating and resuming threads: full filesystem and network access with no
approval prompts. This also repairs saved threads that previously ran read-only.
Reply delivery needs write access to both workspace files and the worker database.
Client approval or interactive input requests
fail visibly rather than hanging an unattended worker. Per-work-item model
selection, automatic routing and low-to-high escalation still apply.

Sessions live in `<absolute-worker-db-path>.sessions`, which must be on
persistent storage, private to the worker user, with a short path suitable for
Unix sockets. It contains a hashed directory per work item, with the saved
Codex thread ID, request journals, a private socket and a session log. The actual
Codex rollouts remain under the usual persistent `CODEX_HOME`; both must survive
restarts. Processes start lazily and expire after five minutes idle, while the
thread survives. Threads resume after process expiry or restart. A configuration
generation change retires the previous owner before the next attempt starts one
with the new environment.

On upgrade, the worker canonicalizes existing inbox IDs and copies executor
settings from each item's original conversation. It retains that old ID as a
migration hint. On first use, it retires the old session owner through the normal
generation-change protocol after any active turn, then copies the saved thread,
request journals and pending-crash marker into the item's session directory.
It never starts a fresh thread to bypass an uncertain old request. A migration
marker prevents two items from adopting the same old thread; ambiguous legacy
ownership fails visibly for operator reconciliation.
All historical conversation associations are retained in
`work_item_session_aliases`. If an old broad item (for example General) has more
than one saved app-server thread, none is adopted: the item starts a fresh thread
and the original directories remain intact. If any of them holds a request with
no recorded outcome, migration stops instead, because a fresh thread could repeat
that request's effects. Reconcile those before resuming the item. Once an item has
its own thread, old sessions are never consulted again.

The existing supervisors and inbox still own tasks and delivery. An opted-in
item runs one task at a time. Each turn
receives the current task ID, reply channel and workspace; its `cwd` and model are
set explicitly. Only a new thread receives the recent-history bootstrap. Follow-up
messages use the existing reply-time claiming protocol: `main_reply` returns the
new input and withholds a stale draft. This first version does not push messages
through `turn/steer` while a tool is running.
An `/executor` command arriving during work stays queued as a command; it and
later messages are not absorbed as model follow-ups to the old turn.

Successful `turn/completed` ends a task without killing the conversation process.
The exec backend's `/finish` setting remains saved but does not apply to this
backend. Visible replies still go through `main_reply`; an app-server turn that
omits its reply receives the existing recovery continuation and delivery checks.
GitHub webhook turns can finish silently when the worker finds no action is
needed, even when their item's reply channel is Telegram.

Before starting a model turn the owner fsyncs a request journal. Completed
requests return their cached result on transport retries, so a dropped connection
cannot repeat their side effects. Failed/interrupted turns are errors, not
successful completion. A crash leaving a `submitted` journal blocks subsequent
turns in that session: inspect the Codex rollout, session log and delivered replies
before reconciling it. After determining what ran, remove that request journal and
its `pending.json` marker to allow an intentional retry. Do not delete the session directory as routine retry
handling. Switching back to `exec` is available for rollback; it does not replay
old turns or delete the saved thread.

Protocol reference: [official OpenAI App Server documentation](https://developers.openai.com/codex/app-server).

Run the deterministic integration tests with
`python -m unittest tests.test_app_server_executor`. An opt-in live check starts a
real turn, restarts the server and verifies remembered context after resume:
`CHATTING_CODEX_APP_SERVER_BIN=/path/to/codex python -m unittest tests.test_app_server_live`.
The live check uses the configured Codex authentication and two small model turns.
