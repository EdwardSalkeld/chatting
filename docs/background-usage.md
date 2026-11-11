# Background model usage

Scheduled (`source=cron`) tasks are routed per occurrence through the existing
bounded TypeSafe model router. Only a confident routine classification selects
the configured low tier; complex work, ambiguity, missing credentials and routing errors select high.
Task instructions are included alongside the request so a short schedule title
cannot hide substantive work. The low tier retains its existing handoff to high before a
visible reply. The workspace's interactive `/set` choice is unchanged and still
governs interactive work and its GitHub follow-ups.

On app-server, each scheduled occurrence gets its own thread and retry journal.
It uses the same workspace, files, repository references and task instructions,
without replaying previous occurrences' transcripts. A retry of the same task
reuses its journal; an uncertain request remains blocked from automatic replay.
Interactive sessions and GitHub implementation follow-ups keep their history.
Schedules that need previous findings should retain them in workspace files.
The exec backend already starts each occurrence without a persistent thread.

Personal-mail summaries and digests use the deployment's
`CHATTING_PERSONAL_SUMMARY_MODEL`; choosing Luna there does not change the main
worker's model mapping. Their isolated CLI usage is described in
[token usage](token-usage.md). The native GitHub completion collection is described
in [webhook ingress](webhook-ingress.md).

These changes reduce repeated context and unnecessary starts. Actual allowance
savings depend on workload and must be measured after rollout; raw input-token
shares do not equal subscription-limit shares.
