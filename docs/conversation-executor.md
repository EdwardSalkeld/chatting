# Persistent conversation executor

The default executor remains `codex exec`. Opt an individual conversation into
the separate Codex app-server executor with `/executor app-server`. `/executor`
reports its setting and `/executor exec` switches it back. Commands in Telegram
forum topics affect that topic, not every topic in the group. The selection is
stored in the worker database against its existing opaque conversation ID; it
applies to subsequent tasks, including different work items in that conversation.

Operators can also select it using an existing task from the conversation:

```bash
python3 -P -m app.main_work_items --db /var/lib/worker/chatting-worker.db executor task:telegram:123 app-server
```

Configure `app_server_command` separately from `codex_command`. Its default is
`codex app-server`, with stdio transport. App-server inherits the installed Codex
configuration and authentication; it does not inherit flags from `codex_command`.
For an existing deployment that deliberately bypasses sandboxing, configure that
policy explicitly, for example `codex app-server -c sandbox_mode='"danger-full-access"'`.
Turns use approval policy `never`, so client approval or interactive input requests
fail visibly rather than hanging an unattended worker. Per-work-item model
selection, automatic routing and low-to-high escalation still apply.

`app_server_dir` defaults to `<absolute-worker-db-path>.sessions`. Keep it on
persistent storage, private to the worker user, with a short path suitable for
Unix sockets. It contains a hashed directory per conversation, with the saved
Codex thread ID, request journals, a private socket and a session log. The actual
Codex rollouts remain under the usual persistent `CODEX_HOME`; both must survive
restarts. Processes start lazily and expire after five minutes idle, while the
thread survives. Threads resume after process expiry or restart. A configuration
generation change retires the previous owner before the next attempt starts one
with the new environment.

The existing supervisors and inbox still own tasks and delivery. An opted-in
conversation runs one task at a time, even across different work items. Each turn
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
