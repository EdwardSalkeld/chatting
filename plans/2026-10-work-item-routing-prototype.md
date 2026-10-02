# Work item routing prototype

Status: prototype on a child branch of `roadmap/chatting-upgrades`. This records
identity and routing evidence in the worker SQLite database. It does not start
parallel executors or move their working directories yet.

## What happens without user-visible IDs

Every inbound event is staged with an internal work item and workspace ID. The
user continues to write ordinary messages. A Telegram reply to an earlier user
or assistant message joins that message's item. An unthreaded Telegram message
joins the sole open item in its chat/topic. If several items are open and the
message has no clear parent, it remains unassigned with candidate items for a
later classifier or a natural-language clarification. A new email gets a new
item unless its `In-Reply-To` or `References` header matches a recorded email
message ID. Scheduled firings remain separate by default.

When an agent creates a PR, it registers the PR URL against the originating
task. For example:

    python3 -m app.main_work_items --db /path/to/worker.db register-pr task:telegram:123 https://github.com/owner/repo/pull/50

A later notification from GitHub can be matched by that PR URL, even though the
email has no workspace ID. The item keeps its original preferred reply route,
which can be inspected with:

    python3 -m app.main_work_items --db /path/to/worker.db show task:email:456

The PR association is made by the agent or PR creation integration, not by the
user. An unknown notification gets a separate triage item. The artifact table
rejects assigning one PR to two items.

## Boundaries before concurrent execution

- The single-open-item rule is a provisional shortcut. Distinguishing a new
  objective from a follow-up in the same unthreaded chat needs a classifier
  with an abstain path; an internal ID must never become required user syntax.
- The worker still runs in its existing shared directory. Workspace IDs are
  durable identities, not directories yet. Add private worktrees, workspace
  lifecycle, and per-item leases before starting parallel executors.
- The preferred reply route is recorded and shown by the prototype; outbound
  delivery still follows the ingress envelope. Wire delivery through the item
  once cross-channel notification handling is enabled on the test VM.
- This prototype does not modify the live worker database. Only a deployment
  of this branch would run its additive SQLite tables and connector metadata.
