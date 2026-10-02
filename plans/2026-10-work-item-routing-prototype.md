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

**This does not yet solve ordinary Telegram conversation routing.** In particular,
the sole-open-item shortcut cannot tell "another thought about this task" from
"separately, start a new task". It would silently attach the second request to
the first item. Telegram chat/topic is a pool of possible items, not itself an
item. The current shortcut is a prototype fixture, not an acceptable final
assignment rule for concurrent work.

The next routing step must decide among *continue an existing item*, *create a
new item*, and *ask which one*. First use strong evidence, such as a Telegram
reply to a recorded message or a tracked PR. For an ordinary unthreaded message,
compare its meaning with compact summaries of active/recent items in that
chat/topic and include "new objective" as a candidate. A classifier (Jev may
be worth testing here) can rank those outcomes, but it needs calibrated
confidence and an abstain path. A clear new objective creates an item even when
one is already open; a clear follow-up joins the matching item even when several
are open. If the evidence is weak, ask in natural language (for example, "Is
this about the concurrency prototype or a new task?") and retain the message
pending until answered. User corrections should update the assignment and its
future routing evidence. No user-visible item syntax is required.

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

- The single-open-item rule is a provisional shortcut and may misassign a new
  objective. Do not use it to select a writable workspace for parallel runs.
  Implement and evaluate the three-way routing decision above first.
- The worker still runs in its existing shared directory. Workspace IDs are
  durable identities, not directories yet. Add private worktrees, workspace
  lifecycle, and per-item leases before starting parallel executors.
- The preferred reply route is recorded and shown by the prototype; outbound
  delivery still follows the ingress envelope. Wire delivery through the item
  once cross-channel notification handling is enabled on the test VM.
- This prototype does not modify the live worker database. Only a deployment
  of this branch would run its additive SQLite tables and connector metadata.
