# Logging Overhaul Plan

## Goal
Replace `print(...)` usage with the Python `logging` module, with consistent timestamped formatting, while preserving machine-readable CLI JSON output behavior.

## Scope
- `app/main.py`
- `app/applier/integrated.py`
- `tests/test_main.py` (and any other tests impacted by log capture changes)

## Checklist
- [x] Create working branch (`feat/logging-overhaul`)
- [x] Inventory all `print(...)` statements and current log expectations in tests
- [ ] Add centralized logging configuration (timestamp + level + logger name)
- [ ] Replace `print(...)` in runtime paths with structured logging calls
- [ ] Keep CLI query output (`--list-*`, replay/approval/config/metrics payload output) on stdout as JSON
- [ ] Update tests to capture/assert logging output instead of stdout where appropriate
- [ ] Run targeted tests for `main` and applier behavior
- [ ] Run full test suite (`python3 -m unittest discover -s tests`)
- [ ] Commit changes
- [ ] Push branch and open PR

## Resume Notes
- Most sensitive area: `tests/test_main.py` currently captures `stdout` for bootstrap/runtime log lines.
- Do not convert machine-oriented CLI JSON output to logger output; keep it as direct stdout.
- If time is tight, prioritize logging conversion + targeted tests + commit, then follow up with full suite in a second pass.
