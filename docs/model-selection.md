# Work item model selection

Each new work item starts on **auto**. Jev selects Luna or Sol for each task.
Send `/set low` to use Luna, `/set high` to use Sol, or `/model` to check the
current setting. The setting is stored in the worker database and survives
worker restarts. Commands sent in a Telegram topic are accepted with the
connector's thread prefix, and Telegram bot command mentions are accepted.

Send `/set auto` to have Jev select Luna or Sol for each new task. The worker
sends the current request and up to 30 recent turns from the same Telegram chat
and topic to TypeSafe's Jev API. Set `TYPESAFE_API_KEY` in the worker service
environment; the key is withheld from Codex subprocesses. A missing key, API
error, invalid response, or uncertain low choice selects Sol. Luna requires at
least 0.80 confidence and 0.80 low probability. The audit event records the
selected tier, probabilities, model, and fallback reason. Manual high/low modes
do not call Jev.

Auto selection pins `jev-1.14.0` (the stable release behind `jev-latest` when
this pin was introduced); inbox triage independently retains its `jev-1.13.0`
pin. Update pins deliberately after evaluating their respective workloads.
Shared response validation rejects invalid types, non-normalized distributions,
non-maximal choices and confidence inconsistent with the documented formula
(allowing rounding). For this binary Choice, confidence 0.80 corresponds to
low probability 0.90; the extra 0.80 probability condition is conservative but
redundant for a valid response. Responses are size-bounded and redirects are
refused. Auto routing keeps its five-second request budget and does not retry;
only inbox triage uses the small 429 retry allowance.

A Luna executor can request a handoff by writing the task ID and its findings
to the request path in `escalation_contract`. The worker accepts a request only
when the Luna process finishes successfully and has not published a visible
answer. It then runs the same task once on Sol with Luna's findings in the
prompt. The work item's setting remains low for the next task. The worker
records the initial and executed tier plus the handoff reason in its audit
event. This is an optional request by Luna, not a guarantee that it will
recognize every task that needs Sol.
