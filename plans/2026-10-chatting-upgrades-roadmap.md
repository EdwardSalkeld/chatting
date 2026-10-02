# Chatting upgrades: exploration roadmap

Status: exploratory, 2026-10-02. This is a working map for discussion, not a committed delivery schedule or implementation design.

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

Separate a small, bounded routing decision from task execution. Candidate outcomes: deterministic handling, cheap model, frontier model, or escalation. Start with obvious rules and collect labeled examples before using a classifier. Log the chosen route, confidence or rule, cost, latency, and eventual correction/escalation. Keep a direct path to a frontier model for ambiguous, high-impact, or long-running tasks.

Potential cheap decisions: source and intent classification, alert deduplication, whether a message is a follow-up, retrieval query selection, and deciding whether a short factual answer needs the full tool-capable executor. Explore Jev's role here after defining its interface and actual latency/cost envelope; do not assume it should own a decision that needs tools or rich context.

Questions: Which models/providers are eligible? What is Jev exactly in this stack? What spend and latency targets should define success? Which classes of mistake must force escalation?

First experiment: replay a sample of past tasks through a proposed router without changing live execution, then inspect wrong cheap-route decisions and estimated savings.

### 4. Conversation context and durable memory

Replace the fixed injected turn window with a small recent window plus on-demand retrieval. Extend the existing worker history lookup beyond reply quotes: search by conversation, time, message ID, and text, returning source links and bounded excerpts. Keep raw transport history distinct from curated, durable memory notes. Summarize settled decisions and long-running project context into the memory repo with pointers back to source messages; record when a summary was made and allow corrections.

Questions: What history is already available and for how long? Should cross-channel retrieval require an explicit linked lineage? What privacy and retention rules apply to group chats and email? When should memory be written automatically versus reviewed?

First experiment: answer a question referring to a discussion from last week using retrieved messages, then compare accuracy, context size, and cost against the fixed-window baseline.

### 5. Cross-input linking and reply routing

Introduce an explicit work or lineage ID that can link an original chat request, repo/PR, CI run, notification email, agent action, and status reply. Treat a new signal as evidence about existing work only when correlation is strong; otherwise ask or surface a candidate link. Persist the originating conversation and its reply policy so a CI failure can trigger appropriate diagnosis and send progress back to the channel where Edward requested the work. Deduplicate repeat notifications and make action/reply state visible in the work timeline.

Questions: Which stable IDs are available in GitHub notifications, CI emails, and Chatting task records? Which events should prompt automatic investigation, and which should only notify? How should a work item move if Edward continues it in another channel?

First experiment: create a controlled task → PR → failing CI email chain and verify that the email is linked, investigated once, and reported in the originating chat.

## Suggested order

1. Establish a task/conversation event model and capture the baseline for queue delay, run time, cost, and failures. Use it to improve the work-in-flight view immediately.
2. Add reliable conversation-scoped scheduling and bounded parallel execution. The view should expose both active runs and waiting reasons.
3. Expose on-demand history retrieval and source-backed memory. This improves both human continuity and the data available to routing.
4. Run model/classifier routing in shadow mode, evaluate mistakes, then enable narrow cheap paths with escalation.
5. Link external signals to original work and route progress replies to the originating conversation, using the same event model and concurrency controls.

These tracks can be researched in parallel; implementation order should change if the experiments expose a simpler seam or a dependency.

## Decisions to make with Edward

- Which pain is most urgent: seeing current work, removing the queue, or preserving long-term context?
- What does Jev refer to here, and which cheap models/providers should be considered?
- Is this roadmap branch purely exploratory until a reviewed design, or should it become the home for small prototypes as well?
