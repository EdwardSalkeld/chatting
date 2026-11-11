# Documentation

- [Operations](operations.md): runtime requirements, process interfaces and the worker's supervisors
- [Database maintenance](database-maintenance.md): duplicate transcript cleanup and configurable diagnostic retention
- [Message flow](bbmb-message-flow.md): how tasks and replies move between handler and worker
- [Testing](testing.md)
- [Connectors](connectors/README.md)
- [Schedules](schedules.md) and [one-off reminders](reminders.md)
- [Explicit executor termination](explicit-finish.md)
- [Work item model selection](model-selection.md)
- [Background model usage](background-usage.md): scheduled-task context, model routing and personal-mail metering
- [Model token usage](token-usage.md): observed counters and their limitations
- [Persistent conversation executor](conversation-executor.md): Codex app-server defaults and per-item executor selection
- [GitHub webhook ingress](webhook-ingress.md): filtering, subject ownership and optional event delivery

## Architecture decisions

- [0004 Separate visible egress from task completion](architecture/0004-egress-completion-semantics.md)
- [0005 Synchronous egress transport](architecture/0005-synchronous-egress-transport.md)
