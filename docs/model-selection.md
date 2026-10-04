# Work item model selection

Each work item starts on **high** (Sol). Send `/set low` to use Luna for later
tasks in that work item, `/set high` to switch back, or `/model` to check the
current setting. The setting is stored in the worker database and survives
worker restarts. Commands sent in a Telegram topic are accepted with the
connector's thread prefix, and Telegram bot command mentions are accepted.

A Luna executor can request a handoff by writing the task ID and its findings
to the request path in `escalation_contract`. The worker accepts a request only
when the Luna process finishes successfully and has not published a visible
answer. It then runs the same task once on Sol with Luna's findings in the
prompt. The work item's setting remains low for the next task. The worker
records the initial and executed tier plus the handoff reason in its audit
event. This is an optional request by Luna, not a guarantee that it will
recognize every task that needs Sol.
