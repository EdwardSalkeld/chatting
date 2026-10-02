# Chatting upgrades: exploration roadmap

Status: working exploration and build branch, 2026-10-02. This is a map for discussion and experiments, not a committed delivery schedule.

## Working agreement

- `roadmap/chatting-upgrades` is the home for this work: designs, experiments, and incremental implementation can accumulate here.
- Keep this work off `main` while the current Chatting deployment is in use. Do not merge the roadmap branch into `main` as part of ordinary iteration.
- Explore a separate test VM and deployment path before running disruptive changes. The test environment should have its own configuration, secrets, state, and inbound/outbound routing so experiments cannot consume production messages or send production replies. Record the exact isolation and cutover checks before deploying it.
- No urgency ranking or fixed implementation order is needed. Follow dependencies revealed by the work and revisit the plan as experiments teach us more.

## Aim

Make Chatting feel responsive and intelligible while several conversations and background events are in flight. Preserve the link from an original request through later signals, actions, and replies. Spend frontier model time where it is useful and use cheaper decisions for routine work.

## Current seams to investigate

- The Go handler adds the latest 30 Telegram conversation turns to each new envelope (`telegramMemoryTurnLimit` in `go/handler/internal/runtime/runtime.go`). This is a fixed turn count, regardless of relevance or size. The worker separately stores Telegram history and offers `app.main_history` around a message ID, but the executor contract advertises that lookup only for a reply quote.
- `app/worker/main.py` processes one claimed inbox task through one executor before taking the next task. Its collector keeps ingesting messages during execution, and `app.main_reply` can attach newer turns in the same conversation. That solves some follow-up races but leaves unrelated conversations waiting.
- `app/worker/activity.py` persists activity and exposes a read-only page/JSON feed. Its live state represents one active executor. The page is a useful starting surface, but it cannot yet show several active runs or explain why a queued task is waiting.
- The worker has one configured executor command (`codex_command`, with a legacy fallback), so per-task model selection would need a routing decision and execution metadata.
- The worker's `conversation_routes` table maps reply channel and target to an opaque conversation ID. The ID shape can support later cross-input links, but no explicit relation from an email or CI signal to the originating task is established by that mapping alone.

## Explorations

### 1. Visibility of work in flight

Draw the operator journey first: new input, queued, classified, waiting for a slot, running, replying, finished, and failed. Show *which conversation*, elapsed time, last meaningful activity, selected model, and why it is waiting. Keep an event timeline for each task and a fleet view of active and queued work. Prototype from the existing worker activity data and identify missing events before redesigning the page.

Questions: What should Edward see in a glance? Which progress events can the executor emit reliably? How much transcript detail is useful without exposing secrets in a shared UI?

First experiment: a read-only mock or page backed by captured activity for one long task, a same-conversation follow-up, and two unrelated queued tasks. Check that it answers “what is happening?” without opening logs.

### 2. Parallel conversations

Define the scheduling unit as a conversation or task lineage, not an ingress channel alone. Different Telegram channels, email, and scheduled work may run concurrently; turns in one conversation must keep a coherent order and late follow-ups must still be incorporated. Add a bounded global capacity and explicit per-conversation exclusion before increasing worker count. Make crash recovery, SQLite claims, retries, and reply-time follow-up claiming safe with multiple active executors.

Questions: Should the first version use several worker processes or a supervisor within one process? How many concurrent frontier runs can the host and API budget sustain? What priority should alerts and short requests receive?

First experiment: two long tasks in separate conversations plus a follow-up to one; verify parallel progress and one correct final reply per conversation after a worker restart.

### 3. Model selection and cheap classifiers

Separate a small, bounded routing decision from task execution. Candidate outcomes: deterministic handling, cheap model, frontier model, or escalation. Use deterministic rules where they are clear, and evaluate Jev for decisions that are awkward to specify as rules but too routine to spend an LLM call on. Collect labeled examples and compare Jev with rules and a cheap model before putting it on the live path. Log the chosen route, confidence or rule, cost, latency, and eventual correction/escalation. Keep a direct path to a frontier model for uncertain or consequential decisions.

Jev candidates to test:

| Decision | Useful input | Output and safeguard |
| --- | --- | --- |
| Workspace selection | Message, channel, referenced repo/path, recent task lineage | Ranked workspace candidates; preserve explicit workspace instructions and escalate ambiguity. |
| Model selection | Request shape, expected tool use, complexity, risk, attachments | Cheap or frontier route with reason and confidence; allow escalation when the cheap route stalls or the task grows. |
| Follow-up or new task | Message, conversation ID, recent open work | Candidate lineage; avoid silently attaching weak matches. |
| Alert and notification triage | Source, stable event IDs, subject, related work | Classify duplicate, informational, or investigation candidate; retain correlation evidence. |
| Context retrieval gate | Request and available history metadata | Decide whether to fetch older context; use source-backed retrieval when needed. |

These are hypotheses, not assignments to Jev. Measure classification accuracy on real examples, abstention behavior, added latency, and cost avoided. A workspace or model misroute can be expensive even if the classifier call is cheap, so evaluate the whole task outcome and provide a way to override a route.

Questions: What is Jev's interface and operational footprint? Which models/providers are eligible? What spend and latency targets should define success? Which classes of mistake must force escalation?

First experiment: replay a sample of past tasks through a proposed router without changing live execution, then inspect wrong cheap-route decisions and estimated savings.

### 4. Conversation context and durable memory

Replace the fixed injected turn window with a small recent window plus on-demand retrieval. Extend the existing worker history lookup beyond reply quotes: search by conversation, time, message ID, and text, returning source links and bounded excerpts. Keep raw transport history distinct from curated, durable memory notes. Summarize settled decisions and long-running project context into the memory repo with pointers back to source messages; record when a summary was made and allow corrections.

Questions: What history is already available and for how long? Should cross-channel retrieval require an explicit linked lineage? What privacy and retention rules apply to group chats and email? When should memory be written automatically versus reviewed?

First experiment: answer a question referring to a discussion from last week using retrieved messages, then compare accuracy, context size, and cost against the fixed-window baseline.

### 5. Cross-input linking and reply routing

Introduce an explicit work or lineage ID that can link an original chat request, repo/PR, CI run, notification email, agent action, and status reply. Treat a new signal as evidence about existing work only when correlation is strong; otherwise ask or surface a candidate link. Persist the originating conversation and its reply policy so a CI failure can trigger appropriate diagnosis and send progress back to the channel where Edward requested the work. Deduplicate repeat notifications and make action/reply state visible in the work timeline.

Questions: Which stable IDs are available in GitHub notifications, CI emails, and Chatting task records? Which events should prompt automatic investigation, and which should only notify? How should a work item move if Edward continues it in another channel?

First experiment: create a controlled task → PR → failing CI email chain and verify that the email is linked, investigated once, and reported in the originating chat.

## Shared foundations to explore

- A task/conversation event model can serve the work-in-flight view, scheduling, routing, and cross-input links. Capture queue delay, run time, cost, failures, and reasons for waits or route changes.
- A test VM should make it possible to try several active executors, routing changes, and new history behavior without touching the working app. Define how fixtures or replayed traffic will exercise inbound signals and replies before exposing the VM to live inputs.
- Keep experiments measurable and reversible. Shadow routing and replay are useful for classifier evaluation; restart and duplicate-delivery scenarios are useful for concurrency and cross-input links.

## Open design questions

- What VM resources and deployment wiring are available for an isolated test instance?
- What Jev API or runtime is available, and which cheap models/providers should be compared with it?
- Which routing errors are most costly, and what abstention or escalation thresholds are acceptable?
