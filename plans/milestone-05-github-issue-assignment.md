# Milestone 05: GitHub Issue Assignment Ingress (Polling)

## Goal

Allow assigning a GitHub issue to Billy and have `chatting` pick it up automatically as a normal task,
without requiring an internet-facing webhook endpoint.

## Scope (MVP)

- Poll GitHub using `gh api graphql` for `AssignedEvent` timeline items.
- Filter to configured repositories and target assignee login.
- Normalize matching assignments into canonical `TaskEnvelope` payloads.
- Publish resulting task messages to BBMB `chatting.tasks.v1`.
- Persist idempotency + poll checkpoint in SQLite for restart safety.

## Delivered Slice A (implemented)

- Integrated GitHub assignment polling into `app.main_message_handler` loop.
- Added runtime helpers in `app.github_ingress_runtime`:
  - GraphQL query + parsing.
  - Assignment event normalization.
  - SQLite checkpoint store (`github_assignment_checkpoints`).
  - Event ordering/filtering past checkpoint and queue publish helper.
- Added tests:
  - `tests/test_github_ingress_runtime.py`
  - `tests/test_main_github_ingress.py`
- Added runtime/docs updates:
  - `configs/message-handler-runtime.example.json`
  - `docs/connectors/github-issue-assignments.md`
  - `docs/run-split-bbmb.md`

## Follow-up Slices

1. Add GitHub comment egress (optional) so replies can be posted back into issue threads.
2. Add label-triggered workflows and review-comment ingress using the same polling/checkpoint core.
3. Add metrics/reporting for poll lag, new-event rate, publish errors, and duplicate suppression.
