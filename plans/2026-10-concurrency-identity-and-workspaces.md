# Concurrency foundations: identity, workspaces, and ingress

Status: proposed design for discussion, 2026-10-02. This settles the concepts to test before choosing a concurrent worker implementation. All work stays on `roadmap/chatting-upgrades` and uses an isolated test deployment.

## The four identities

Keep these separate in the task record:

| Identity | Meaning | Lifetime |
| --- | --- | --- |
| Ingress event | One Telegram update, email, scheduled firing, reminder, or notification. Its source ID deduplicates delivery. | Permanent audit record. |
| Conversation | Where dialogue happens: Telegram chat plus topic, email thread, or another transport thread. | Retained history; may contain several work items. |
| Work item | One continuing objective and its later follow-ups or external signals. This is the unit that gets an execution lane and a writable workspace. | From first task until explicitly closed or archived; can be reopened. |
| Resource | A repo, deployment, remote service, or other thing work may modify. | Independent of work items. |

The reply destination is a fifth, separate property. A CI email may belong to an existing work item whose preferred progress destination is Telegram. A Telegram group can contain more than one work item. Neither the email sender nor the Telegram chat ID is a sufficient workspace key.

## Workspace proposal

A workspace is the writable filesystem view for a work item, with its own directory, scratch files, and per-repo checkout or Git worktree. Assign a stable `workspace_id` when the work item is created. Subsequent runs for that item return to it. Preserve the work item and its workspace while the work is active or awaiting external signals; archive it when the item is closed, retaining metadata and committed branches so it can be restored. Do not use a fixed time-to-live as the identity rule. A completed run does not imply a completed work item.

Separate the durable work item from an execution lease. Only one executor may write a work item's workspace at a time. Different work items can run at once, even if they concern the same repo, because they get different checkouts. A new item can start from an explicitly chosen branch or commit; continuation keeps the existing branch. Reuse the same workspace only for a proven continuation or an explicit user choice. An uncertain match gets a new item or a clarification, never an automatic join to an active writer.

The initial isolation boundary should include a separate working directory, writable scratch directory, temporary directory, and executor session state for each active run. Mount or expose only the resources that item needs where practical; keep common instructions and reference data read-only. This is protection against accidental file collisions, not a security boundary against a malicious executor: network access and credentials still require separate policy. Outbound attachment paths must remain visible to the handler through a designated per-run export directory.

Filesystem separation alone does not protect shared resources. An operation against a live deployment, the memory repo's main branch, a shared SQLite file, or another external service needs a named resource lease or transactional conflict check. Git worktrees isolate working files, but sharing one Git object store still allows coordinated fetches, refs, and branch names; give each work item a unique branch and define how updates are integrated. The test VM must use separate state, secrets, and transport routes from production.

## Ingress assignment

Route an event in this order: (1) explicit work item reference or trusted parent ID; (2) verified correlation to an existing item, such as a stored PR or CI run ID, email `Message-ID`/`References`, or a reply to a known bot message; (3) an ingress-specific conversation mapping; (4) a new work item. Record the evidence and allow an override. A classifier may rank ambiguous candidates later but must be able to abstain; it cannot silently turn a weak match into shared writable state.

| Ingress | Default conversation | Work item rule |
| --- | --- | --- |
| Telegram | Chat ID plus topic/thread ID when present. | Follow-up to a known item or explicit reference continues it. A new request starts a new item. In a chat with one active item, a normal follow-up may continue that item; concurrent ambiguous items need an explicit selection or clarification. |
| Email | RFC message thread using `Message-ID`, `In-Reply-To`, and `References`. | Known thread or tracked artifact continues its item. Unrelated mail, including mail from the same sender, starts a new item. Subject and sender are only hints. |
| Schedule | Stable schedule ID, independent of its reply destination. | Each firing gets its own item by default. A schedule can opt into a continuing item when it truly maintains ongoing state. Overlapping firings of that same continuing item serialize. |
| Reminder | Its `created_from_task_id` lineage when present. | Resume the originating item if it is still valid, otherwise start a linked item. The copied reply channel only controls delivery. |
| CI/GitHub notification | Provider event and artifact IDs. | Join the item that registered the PR, commit, or CI run; unmatched events become triage items. Preserve the original progress destination. |

Workspace selection follows the work item, not the arrival channel. `context_refs` name candidate resources and starting context, but do not automatically grant a shared writable checkout. A workspace can contain several repo worktrees when a task spans repos. Explicit user direction takes precedence over automatic assignment. Show the selected work item, workspace, and correlation reason in task records so routing errors are repairable.

## Current code gaps this design exposes

- `app/state/sqlite_store.py` currently keys `conversation_routes` by reply channel type, target, and optional thread metadata. That groups all mail to one sender and scheduled work to one reply destination, even when they are unrelated. `claim_next_inbox_task` also lacks a per-conversation or per-work-item active exclusion, so adding executors directly would admit two writers to a lane.
- The Telegram connector puts `message_thread_id` into content but not reply metadata, so the worker's existing route function cannot distinguish Telegram topics. This needs to be corrected before using conversation identity as a lock key.
- The IMAP connector does not retain email thread headers in the envelope. Scheduled envelopes contain a firing ID, while schedule ID is not a first-class route field. Reminder records carry `created_from_task_id`, but the reminder envelope does not expose it to the worker.
- The executor has one `codex_working_dir`, so it cannot yet launch into a selected work item workspace. Existing `context_refs` are prompt context, not a workspace allocation contract.

## Decisions needed before executor concurrency

1. Add explicit `conversation_id`, `work_item_id`, `workspace_id`, `resource_refs`, correlation evidence, and preferred reply route to the durable task model. Define migration from existing `conversation_routes` without treating old coarse routes as proven work items.
2. Decide the initial work item join rule for ordinary Telegram messages when a chat or topic has multiple active items. The safe default is explicit reply/reference or ask; a single clearly active item can be joined automatically.
3. Define workspace creation, archive, restore, branch naming, and conflict handling, including shared memory writes and deploy/resource leases. Set a retention policy for uncommitted scratch separately from work item identity.
4. Specify a small set of routing fixtures before implementation: two Telegram topics; two independent tasks in one chat; two unrelated emails from one sender; a reply-chain email; two schedules reporting to one chat; a reminder; and a CI email linked to a Telegram-origin PR. Each fixture should assert conversation, item, workspace, and reply route.

Only after those identities and fixtures are agreed should the worker's concurrency mechanism and capacity be chosen.
