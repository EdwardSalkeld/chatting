# Test suite audit — 16 November 2026

Baseline: `9563546`, verified against current `origin/main` before editing.
Production code and CI configuration are unchanged.

## Inventory and reduction

| Suite | Before | After | Removed |
| --- | ---: | ---: | ---: |
| Python collected cases | 355 | 298 | 57 |
| Go top-level tests | 295 | 282 | 13 |
| Node release-policy tests | 7 | 7 | 0 |
| **Total test entries** | **657** | **587** | **70 (10.7%)** |
| Python test-file lines | 10,345 | 9,626 | 719 |
| Go test-file lines | 11,534 | 11,255 | 279 |
| Node test-file lines | 97 | 97 | 0 |
| **Total test-file lines** | **21,976** | **20,978** | **998 (4.5%)** |

Counts include skipped tests. Python counts include pytest parametrization;
Go counts come from top-level `run` events in `go test -json`. Lines include
blanks, comments and helpers inside the 100 tracked test files, excluding
standalone fixture/support files and this report.

Consolidation accounts for much of the entry reduction. Nested Python subtests
went from 145 to 190, and nested Go runs from 180 to 195. They are excluded
from the entry totals above to avoid counting both a group and its children.
This is not a claim that 70 distinct behaviors were removed.

## Findings and cuts

- **Repeated contract examples:** Python model, broker and fixture tests fell
  from 36 functions to nine; Go contract tests fell from six to two. Shared
  golden fixtures retain exact wire-shape checks in both languages. Optional
  attachment behavior, factory IDs and rejection cases remain explicit.
- **Rejections that could pass for the wrong reason:** wrong-message-type
  tests previously supplied otherwise incomplete objects. They now change one
  field of a valid fixture; Python also checks the relevant error text.
- **Repeated validation setup:** model versions, timestamps, required strings,
  collection validation and transcript types now use named subtests with valid
  base objects. This retains the boundaries without copying entire objects.
- **Unnecessary Cartesian product:** new-item backend migration tested six
  sources against three old backend states, although backend initialization
  does not branch on source (`app/state/work_items.py`). Six representative
  pairs retain every source and backend state instead of 18 database rebuilds.
- **Repeated transport matrix:** timeout, disconnect, truncated body and reset
  remain at the egress-client boundary. Reply CLI and outbox replay each retain
  a real truncated-response check for their own retry-ID/persistence behavior,
  removing six duplicate HTTP scenarios across these callers.
- **Overlapping state tests:** deleted the simple inbox ack-replay test in
  favor of the stronger recovered-429-plus-ack-replay test; retained the real
  broker/restart test. Removed overlapping app-server settings/claim coverage
  already exercised by work-item session and inbox tests. Folded explicit-
  finish default and non-Telegram no-reply assertions into existing tests.
- **Boilerplate-heavy cases:** combined command recognition, legacy routing,
  worker logs/config boundaries, Go startup errors, GitHub availability/targets,
  dispatcher rendering/failure reasons and malformed broker payloads. Removed
  unused fixture construction and direct-script unittest launchers; pytest and
  `python -m unittest` discovery still work.

## What survived and verification

Concurrency, leases, process ownership, crash journals, idempotency, restart
recovery, cross-language fixtures, authentication, attachment path boundaries,
secret handling and delivery ordering retain their dedicated tests. The seven
release-policy tests remain: each protects a distinct publication failure mode.

Baseline and post-cull full runs passed with loopback sockets/subprocesses
enabled and a short macOS `TMPDIR`. Final four-worker results: Python 293
passed, five skipped, plus 190 passing subtests; Go 281 top-level passed, one skipped, plus 195 passing nested
runs; Node seven passed. Skips require an external bbmb server, live Codex or
JMAP access, or the email e2e services. Ruff lint/format, Pyright and `go vet`
also pass.

A subsequent `-n auto` run exposed intermittent macOS `PermissionError` from
`os.killpg` during cleanup in two explicit-finish tests. Both passed in the
earlier full run, all 11 surviving explicit-finish tests passed together on
an isolated rerun, and the final full four-worker run passed. These tests were
retained; the host/process-signalling flake was not hidden by skipping or
deleting them.

This audit used test inventory, targeted source review and before/after suite
runs. It did not measure mutation scores or prove unchanged branch coverage.
External-service/live tests were not exercised locally; Nix packaging was not
rerun because application and package inputs did not change.
