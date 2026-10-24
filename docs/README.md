# Documentation

- [Operations](operations.md): runtime requirements, process interfaces and the worker's supervisors
- [Message flow](bbmb-message-flow.md): how tasks and replies move between handler and worker
- [Testing](testing.md)
- [Connectors](connectors/README.md)
- [Schedules](schedules.md) and [one-off reminders](reminders.md)
- [Explicit executor termination](explicit-finish.md)
- [Work item model selection](model-selection.md)
- [Persistent conversation executor](conversation-executor.md): opt individual conversations into Codex app-server

## Architecture decisions

- [0004 Separate visible egress from task completion](architecture/0004-egress-completion-semantics.md)
- [0005 Synchronous egress transport](architecture/0005-synchronous-egress-transport.md)
