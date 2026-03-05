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
- [ ] Run local validation of the same test command used in CI.
- [ ] Update docs with CI usage/expectations.
- [ ] Push branch and open PR.
- [ ] Post PR link and current status.

## Progress Log
- 2026-03-04: Created `feat/ci-workflow` from `origin/main`.
- 2026-03-04: Added initial plan/checklist for resumable execution.
- 2026-03-05: Added `.github/workflows/ci.yml` with push/PR to `main` triggers and unittest discovery execution on Python 3.11.

## Resume Notes
- Next action: implement `.github/workflows/ci.yml` with explicit Python setup + `python3 -m unittest discover -s tests`.
- Keep commits small: plan, workflow, docs, then push.
