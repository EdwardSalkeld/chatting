# CI Workflow Plan

Owner: Codex
Branch: `feat/ci-workflow`
Last updated: 2026-03-05

## Goal
Add a GitHub Actions CI workflow that runs the project test suite on pull requests and pushes to `main`, with clear pass/fail feedback.

## Checklist
- [x] Create dedicated CI branch from latest `origin/main`.
- [x] Add this resumable plan doc.
- [x] Define CI scope and triggers.
- [x] Add `.github/workflows/ci.yml` for Python unit tests.
- [x] Run local validation of the same test command used in CI.
- [x] Update docs with CI usage/expectations.
- [ ] Push branch updates in small commits.
- [ ] Open/refresh PR and post current status.

## Progress Log
- 2026-03-04: Created `feat/ci-workflow` from `origin/main`.
- 2026-03-04: Added initial plan/checklist for resumable execution.
- 2026-03-05: Added `.github/workflows/ci.yml` with push/PR to `main` triggers and unittest discovery execution on Python 3.11.
- 2026-03-05: Fixed env-sensitive Telegram CLI test to be deterministic when `CHATTING_TELEGRAM_BOT_TOKEN` is set locally.
- 2026-03-05: Added CI notes to `docs/debug-and-test.md` and linked workflow in `docs/README.md`.

## Resume Notes
- Next action: run local validation command once more and push docs/test plan updates as separate small commits.
- Keep commits small: deterministic test fix, docs/plan refresh, then push.
