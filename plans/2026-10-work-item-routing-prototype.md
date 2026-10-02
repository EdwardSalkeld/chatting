# Persistent lane routing prototype

Status: implementation branch targeting `roadmap/chatting-upgrades`. This is
identity, record keeping, and persistent workspace directories. Executors are
still serialized.

## Ownership and policy

The **handler** owns the authoritative router and SQLite mapping. It sees all
ingress before publishing tasks, owns the task ledger, and later handles outbound
delivery. Its `routing.Router` chooses the route policy, while the SQLite store
persists lane identities. The task queue message carries `work_item_id` and
`workspace_id` to the worker. The worker mirrors assignments for its own records
and future execution leases. Its local router remains as a compatibility fallback
for older task messages without handler-assigned IDs.

- A direct Telegram message maps to one durable lane per chat and topic. All
  messages in that chat/topic use its work item and workspace ID, including
  replies and unrelated new requests. The lane never closes or expires.
- Non-Telegram ingress maps to one durable general lane unless a trusted GitHub
  PR URL matches an artifact registered by an earlier task. A linked notification
  then uses that task's lane. If that lane originated in Telegram, the handler
  changes the task's reply route to the originating chat/topic before publishing,
  so the worker's reply returns there.
- An unmatched or conflicting PR reference stays in the general lane. A
  notification's sender or subject alone is never evidence for another lane.
- All assignments are durable and idempotent by task ID. The worker creates a
  persistent directory for the assigned workspace ID before launching Codex.

When an agent creates a PR it must register the PR URL against the originating
task, for example:

    python3 -m app.main_work_items --db /path/to/worker.db --handler-db /path/to/handler.db register-pr task:telegram:123 https://github.com/owner/repo/pull/50

`show <task-id>` returns the lane, workspace, reason, and preferred reply route.
The handler records the lane ID in its task ledger and related delivery records.
The worker records the lane ID on inbox, run, audit, dead letter, activity,
conversation turn, Telegram history, egress outbox, and dispatch rows where the
originating task/run is known. Existing historical rows remain nullable after migration.

## Workspace directories and next steps

The worker launches Codex from `<workspace_root>/<workspace_id>`, where the
default root is `.chatting-workspaces` inside `codex_working_dir`. The optional
`workspace_root` worker setting selects another absolute path. Directories are
created on first use and are not automatically deleted. The executor task payload
includes both IDs and the selected working directory.

Repo references in the incoming task still point to existing checkouts; this
change does not copy or mount them into the lane directory. Before starting
concurrent execution, add per-lane repo worktrees, lane execution locks, and
locks for shared resources such as deployments. PR registration currently needs
an explicit agent/integration call to the CLI; automatic capture at PR creation
is still needed. The general lane's preferred reply route reflects its first
event and is not a global outbound destination.
