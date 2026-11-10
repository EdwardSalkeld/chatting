# Work item model selection

Each new work item starts on **auto**. Jev selects the low or high tier for each task.
Send `/set low` or `/set high` to select a tier, or `/model` to check the
current setting. The setting is stored in the worker database and survives
worker restarts. Commands sent in a Telegram topic are accepted with the
connector's thread prefix, and Telegram bot command mentions are accepted.

Set model IDs in the worker service environment:

| Variable | Default when unset |
| --- | --- |
| `CHATTING_MODEL_HIGH` | `gpt-6.1-sol` |
| `CHATTING_MODEL_LOW` | `gpt-6-luna` |

For example, a deployment can set `CHATTING_MODEL_HIGH=gpt-6.1-terra` and
`CHATTING_MODEL_LOW=gpt-6-luna`. These are read once at process startup; restart
the worker after changing them. Blank values or IDs containing whitespace fail
startup rather than silently selecting another model. Availability and account
access still depend on the configured Codex client and login.

The same mapping controls both executor backends, `/model`, submitted model
metadata, usage events, routing descriptions and low-to-high handoffs. Persistent
app-server owners inherit the settings and retire when the environment changes.
New turns on existing threads use the configured model. Running tasks retain
their existing process configuration. Personal-mail summaries keep their separate
`CHATTING_PERSONAL_SUMMARY_MODEL` setting.

Send `/set auto` to have Jev select the low or high tier for each new task. The worker
sends the current request and up to 30 recent turns from the same Telegram chat
and topic to TypeSafe's Jev API. Set `TYPESAFE_API_KEY` in the worker service
environment; the key is withheld from Codex subprocesses. A missing key, API
error, invalid response, or uncertain low choice selects high. The low tier requires at
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

A low-tier executor can request a handoff by writing the task ID and its findings
to the request path in `escalation_contract`. The worker accepts a request only
when the low-tier process finishes successfully and has not published a visible
answer. It then runs the same task once on the high tier with its findings in the
prompt. The work item's setting remains low for the next task. The worker
records the initial and executed tier plus the handoff reason in its audit
event. This is an optional request by the low-tier model, not a guarantee that it will
recognize every task that needs the high tier.

Scheduled occurrences route independently through auto even when their
workspace's interactive setting is high or low; they do not change that setting.
See [background usage](background-usage.md) for scheduled-thread isolation.
