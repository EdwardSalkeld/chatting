# Persistent lane routing prototype

Status: implementation branch targeting `roadmap/chatting-upgrades`. This is
identity, record keeping, and persistent workspace directories. Executors are
still serialized.

## Ownership and policy

The **handler** owns the authoritative router and SQLite mapping. It sees all
ingress before publishing tasks, owns the task ledger, and later handles outbound
delivery. Its `routing.Router` chooses the route policy, while the SQLite store
persists lane identities. The task queue message carries `work_item_id` to the
worker. That same ID names the persistent workspace directory. The worker records
the handler assignment for its own records and future execution leases. Older
messages without an assignment use one fixed legacy lane.

- A direct Telegram message maps to one durable lane per chat and topic. All
  messages in that chat/topic use its work item ID, including
  replies and unrelated new requests. The lane never closes or expires.
- Non-Telegram ingress maps to one durable general lane unless a trusted GitHub
  PR URL matches an artifact registered by an earlier task. A linked notification
  then uses that task's lane. If that lane originated in Telegram, the handler
  changes the task's reply route to the originating chat/topic before publishing,
  so the worker's reply returns there.
- An unmatched or conflicting PR reference stays in the general lane. A
  notification's sender or subject alone is never evidence for another lane.
- All assignments are durable and idempotent by task ID. The worker creates a
  persistent directory for the assigned work item ID before launching Codex.

When an agent creates a PR it must register the PR URL against the originating
task, for example:

    python3 -m app.main_work_items --db /path/to/worker.db register-pr task:telegram:123 https://github.com/owner/repo/pull/50

The CLI posts to the handler's loopback API. Set `--handler-url` when that API
uses a non-default address. The worker does not access the handler database.
`show <task-id>` returns the lane, reason, and preferred reply route.
The handler records the lane ID in its task ledger and related delivery records.
The worker records the lane ID on inbox, run, audit, dead letter, activity,
conversation turn, Telegram history, egress outbox, and dispatch rows where the
originating task/run is known. Existing historical rows remain nullable after migration.

## Workspace directories and next steps

The worker launches Codex from `<workspace_root>/<work_item_id>`, where the
default root is `.chatting-workspaces` inside `codex_working_dir`. The optional
`workspace_root` worker setting selects another absolute path. Directories are
created on first use and are not automatically deleted. The executor task payload
includes the work item ID and the selected working directory.

Repo references in the incoming task still point to existing checkouts; this
change does not copy or mount them into the lane directory. Before starting
concurrent execution, add per-lane repo worktrees, lane execution locks, and
locks for shared resources such as deployments. PR registration currently needs
an explicit agent/integration call to the CLI; automatic capture at PR creation
is still needed. The general lane's preferred reply route reflects its first
event and is not a global outbound destination.
