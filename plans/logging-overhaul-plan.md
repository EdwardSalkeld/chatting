# Logging Overhaul Plan

## Goal
Replace runtime `print(...)` usage with structured `logging` calls, with timestamped output format configured in the app entrypoint.

## Branch
- `feat/logging-overhaul`

## Checklist
- [x] Restore prior WIP changes from stash for logging overhaul files.
- [x] Replace `print(...)` in `app/main.py` with level-appropriate logger calls.
- [x] Replace `print(...)` in `app/applier/integrated.py` with level-appropriate logger calls.
- [x] Add centralized logging configuration in `app/main.py` with timestamped format.
- [x] Preserve JSON CLI outputs via `stdout` for machine-readable commands.
- [x] Update tests that asserted stdout logs to assert logging output instead.
- [x] Run targeted tests (`tests.test_main.MainBootstrapFlowTests`, `tests.test_applier`).
- [ ] Commit changes.
- [ ] Push branch.
- [ ] Open PR.

## Resume Notes
- If interrupted, continue from the first unchecked item in the checklist.
- Avoid including `AGENTS.md` changes in this PR.
